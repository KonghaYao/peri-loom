use super::SimpleServices;
use crate::{
    error::{ApiError, ApiResult},
    execution::{DataStream, DatabaseExecutor, ExecutionGuard},
    router::StreamTarget,
    state::SessionBinding,
};
use async_trait::async_trait;
use chrono::Utc;
use database_host::ExecutionFrame;
use domain::{error::ErrorCode, DatabaseId};
use futures::StreamExt;
use protocol::data as wire;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::OwnedRwLockReadGuard;

pub struct LocalExecutor(pub Arc<SimpleServices>);
fn session_id(binding: &SessionBinding) -> ApiResult<uuid::Uuid> {
    binding
        .session_id
        .parse()
        .map_err(|_| ApiError::new(ErrorCode::SessionLost, "本地会话 ID 非法"))
}
fn column(value: domain::value::ColumnMeta) -> wire::ColumnMeta {
    wire::ColumnMeta {
        name: value.name,
        type_name: value.type_name,
        nullable: value.nullable,
    }
}

#[async_trait]
impl DatabaseExecutor for LocalExecutor {
    async fn open_stream(
        &self,
        db: DatabaseId,
        target: StreamTarget,
        _request_id: &str,
        deadline: Option<Instant>,
    ) -> ApiResult<DataStream> {
        let permit = self.0.gate.clone().read_owned().await;
        self.0.ensure_available(db).await?;
        self.open_stream_with_permit(db, target, deadline, permit)
            .await
    }
    async fn describe(
        &self,
        db: DatabaseId,
        session: Option<&SessionBinding>,
        sql: &str,
        _request_id: &str,
    ) -> ApiResult<wire::DescribeResponse> {
        let _permit = self.0.gate.read().await;
        self.0.ensure_available(db).await?;
        let result = self
            .0
            .host
            .describe(db, session.map(session_id).transpose()?, sql.to_owned())
            .await?;
        Ok(wire::DescribeResponse {
            error: None,
            params: result
                .param_names
                .into_iter()
                .map(|name| wire::DescribeParam { name })
                .collect(),
            cols: result.columns.into_iter().map(column).collect(),
            is_explain: result.is_explain,
            is_readonly: result.is_readonly,
        })
    }
    async fn open_session(
        &self,
        db: DatabaseId,
        _request_id: &str,
        idle_timeout_ms: u32,
    ) -> ApiResult<SessionBinding> {
        let _permit = self.0.gate.read().await;
        self.0.ensure_available(db).await?;
        let id = self.0.host.open_session(db).await?;
        let now = Utc::now();
        Ok(SessionBinding {
            session_id: id.to_string(),
            database_id: db,
            worker_id: None,
            owner_epoch: None,
            created_at: now,
            expires_at: now + chrono::Duration::milliseconds(i64::from(idle_timeout_ms)),
        })
    }
    async fn validate_session(&self, binding: &SessionBinding) -> ApiResult<()> {
        let _permit = self.0.gate.read().await;
        if binding.worker_id.is_some() {
            return Err(ApiError::new(ErrorCode::SessionLost, "会话部署模式不一致"));
        }
        self.0.catalog.get_database(binding.database_id).await?;
        if !self
            .0
            .host
            .has_session(binding.database_id, session_id(binding)?)
            .await
        {
            return Err(ApiError::new(
                ErrorCode::SessionLost,
                "本地会话已关闭或失效",
            ));
        }
        Ok(())
    }
    async fn close_session(&self, binding: &SessionBinding) -> ApiResult<()> {
        let _permit = self.0.gate.read().await;
        self.0
            .host
            .close_session(binding.database_id, session_id(binding)?)
            .await?;
        Ok(())
    }
    async fn batch(
        &self,
        db: DatabaseId,
        statements: Vec<(String, Vec<wire::Value>)>,
        atomic: bool,
        request_id: &str,
    ) -> ApiResult<wire::ExecuteBatchResponse> {
        let binding = self.open_session(db, request_id, 60_000).await?;
        let host = self.0.host.clone();
        let session = session_id(&binding)?;
        let mut cleanup = ExecutionGuard::new(move || {
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    let _ = host.close_session(db, session).await;
                });
            }
        });
        let started = Instant::now();
        let outcome = async {
            if atomic {
                self.collect(db, &binding.session_id, "BEGIN".into(), vec![], request_id)
                    .await?;
            }
            let mut results = Vec::new();
            let mut total_bytes = 0usize;
            for (sql, params) in statements {
                let (result, bytes) = self
                    .collect(db, &binding.session_id, sql, params, request_id)
                    .await?;
                total_bytes += bytes;
                if total_bytes > 16 * 1024 * 1024 {
                    return Err(ApiError::new(
                        ErrorCode::ResultTooLarge,
                        "batch 结果超过 16 MiB",
                    ));
                }
                results.push(result);
            }
            if atomic {
                self.collect(db, &binding.session_id, "COMMIT".into(), vec![], request_id)
                    .await?;
            }
            Ok(wire::ExecuteBatchResponse {
                error: None,
                results,
                wal_lsn: 0,
                elapsed_micros: started.elapsed().as_micros() as u64,
            })
        }
        .await;
        // 任意错误（包括结果超限）都通过关闭私有会话回滚未提交事务。
        let closed = self.close_session(&binding).await;
        cleanup.disarm();
        match outcome {
            Ok(result) => {
                closed?;
                Ok(result)
            }
            Err(error) => Err(error),
        }
    }
    fn remote_lsn(&self) -> bool {
        false
    }
}
impl LocalExecutor {
    pub(crate) async fn open_stream_with_permit(
        &self,
        db: DatabaseId,
        target: StreamTarget,
        deadline: Option<Instant>,
        permit: OwnedRwLockReadGuard<()>,
    ) -> ApiResult<DataStream> {
        let (session, sql, params) = match target {
            StreamTarget::Stateless { sql, params } => (None, sql, params),
            StreamTarget::Session {
                session_id,
                sql,
                params,
            } => (
                Some(
                    session_id
                        .parse()
                        .map_err(|_| ApiError::new(ErrorCode::SessionLost, "本地会话 ID 非法"))?,
                ),
                sql,
                params,
            ),
        };
        let budget = deadline
            .map(|d| d.saturating_duration_since(Instant::now()))
            .unwrap_or(Duration::from_secs(30));
        let mut stream = self
            .0
            .host
            .execute(
                db,
                session,
                sql,
                params.into_iter().map(Into::into).collect(),
                budget,
            )
            .await?;
        let started = Instant::now();
        let frames = async_stream::stream! {
            // 持有读许可直到响应被消费或丢弃；恢复不能替换一个仍有执行句柄的库。
            let _permit = permit;
            while let Some(result) = stream.frames.recv().await {
                match result {
                    Err(error) => { yield Err(ApiError::from(error)); return; },
                    Ok(frame) => {
                        let frame = match frame {
                            ExecutionFrame::Columns(columns) => wire::stream_frame::Frame::Header(wire::StreamHeader { columns: columns.into_iter().map(column).collect(), row_count_estimate: 0 }),
                            ExecutionFrame::Rows(rows) => wire::stream_frame::Frame::Rows(wire::RowBatch { rows: rows.into_iter().map(|values| wire::Row { values: values.into_iter().map(Into::into).collect() }).collect() }),
                            ExecutionFrame::End { is_autocommit, last_insert_rowid, affected_rows, .. } => wire::stream_frame::Frame::Trailer(wire::StreamTrailer { affected_rows, wal_lsn: 0, elapsed_micros: started.elapsed().as_micros() as u64, is_autocommit: Some(is_autocommit), last_insert_rowid: Some(last_insert_rowid) }),
                        };
                        yield Ok(wire::StreamFrame { error: None, frame: Some(frame) });
                    }
                }
            }
        };
        Ok(DataStream {
            frames: Box::pin(frames),
            guard: ExecutionGuard::new(|| {}),
            remote_lsn: false,
        })
    }
    async fn collect(
        &self,
        db: DatabaseId,
        session_id: &str,
        sql: String,
        params: Vec<wire::Value>,
        request: &str,
    ) -> ApiResult<(wire::ResultSet, usize)> {
        let mut stream = self
            .open_stream(
                db,
                StreamTarget::Session {
                    session_id: session_id.into(),
                    sql,
                    params,
                },
                request,
                None,
            )
            .await?;
        let mut result = wire::ResultSet::default();
        let mut bytes = 0usize;
        let mut finished = false;
        while let Some(frame) = stream.frames.next().await {
            match frame?.frame {
                Some(wire::stream_frame::Frame::Header(header)) => result.columns = header.columns,
                Some(wire::stream_frame::Frame::Rows(rows)) => {
                    for row in rows.rows {
                        bytes += crate::api::data::stream::approximate_bytes(
                            &protocol::convert::row_from_proto(row.clone()),
                        );
                        if bytes > 16 * 1024 * 1024 {
                            return Err(ApiError::new(
                                ErrorCode::ResultTooLarge,
                                "batch 结果超过 16 MiB",
                            ));
                        }
                        result.rows.push(row);
                    }
                }
                Some(wire::stream_frame::Frame::Trailer(trailer)) => {
                    result.affected_rows = trailer.affected_rows;
                    finished = true;
                }
                None => {}
            }
        }
        if !finished {
            return Err(ApiError::internal("执行流未返回完成帧"));
        }
        Ok((result, bytes))
    }
}
