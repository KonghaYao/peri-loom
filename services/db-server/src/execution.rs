//! 数据执行切面：HTTP 保留协议编码，部署 Adapter 隐藏路由与传输机制。
use crate::{
    clients::{self, ChannelPool},
    error::{ApiError, ApiResult},
    router::{DbRouter, StreamTarget},
    state::SessionBinding,
};
use async_trait::async_trait;
use chrono::{Duration, Utc};
use domain::{error::ErrorCode, ids::DatabaseId};
use futures::{Stream, StreamExt};
use std::{pin::Pin, sync::Arc, time::Instant};

pub type FrameStream = Pin<Box<dyn Stream<Item = ApiResult<protocol::data::StreamFrame>> + Send>>;
pub struct ExecutionGuard(Option<Box<dyn FnOnce() + Send>>);
impl ExecutionGuard {
    pub fn new(cancel: impl FnOnce() + Send + 'static) -> Self {
        Self(Some(Box::new(cancel)))
    }
    pub fn disarm(&mut self) {
        self.0 = None;
    }
}
impl Drop for ExecutionGuard {
    fn drop(&mut self) {
        if let Some(cancel) = self.0.take() {
            cancel();
        }
    }
}

pub struct DataStream {
    pub frames: FrameStream,
    pub guard: ExecutionGuard,
    pub remote_lsn: bool,
}

#[async_trait]
pub trait DatabaseExecutor: Send + Sync {
    async fn open_stream(
        &self,
        db: DatabaseId,
        target: StreamTarget,
        request_id: &str,
        deadline: Option<Instant>,
    ) -> ApiResult<DataStream>;
    async fn describe(
        &self,
        db: DatabaseId,
        session: Option<&SessionBinding>,
        sql: &str,
        request_id: &str,
    ) -> ApiResult<protocol::data::DescribeResponse>;
    async fn open_session(
        &self,
        db: DatabaseId,
        request_id: &str,
        idle_timeout_ms: u32,
    ) -> ApiResult<SessionBinding>;
    async fn validate_session(&self, binding: &SessionBinding) -> ApiResult<()>;
    async fn close_session(&self, binding: &SessionBinding) -> ApiResult<()>;
    async fn batch(
        &self,
        db: DatabaseId,
        statements: Vec<(String, Vec<protocol::data::Value>)>,
        atomic: bool,
        request_id: &str,
    ) -> ApiResult<protocol::data::ExecuteBatchResponse>;
    fn remote_lsn(&self) -> bool;
}

pub struct DistributedExecutor {
    pub router: Arc<DbRouter>,
    pub channels: Arc<ChannelPool>,
}

#[async_trait]
impl DatabaseExecutor for DistributedExecutor {
    async fn open_stream(
        &self,
        db: DatabaseId,
        target: StreamTarget,
        request_id: &str,
        deadline: Option<Instant>,
    ) -> ApiResult<DataStream> {
        let (route, frames, guard) = self
            .router
            .open_stream(db, target, request_id, deadline)
            .await?;
        let worker = route.worker_id.to_string();
        // 取消守卫随流存活；正常消费结束由原守卫的协议语义收尾。
        let shared = Arc::new(std::sync::Mutex::new(Some(guard)));
        let cancelled = shared.clone();
        let frames = frames
            .map(move |item| item.map_err(|error| clients::status_to_api_error(error, &worker)));
        let frames = async_stream::stream! {
            let _keep_guard = shared;
            futures::pin_mut!(frames);
            while let Some(frame) = frames.next().await { yield frame; }
            if let Some(mut guard) = _keep_guard.lock().unwrap().take() { guard.disarm(); };
        };
        Ok(DataStream {
            frames: Box::pin(frames),
            guard: ExecutionGuard::new(move || {
                cancelled.lock().unwrap().take();
            }),
            remote_lsn: true,
        })
    }
    async fn describe(
        &self,
        db: DatabaseId,
        session: Option<&SessionBinding>,
        sql: &str,
        request_id: &str,
    ) -> ApiResult<protocol::data::DescribeResponse> {
        self.router.describe(db, session, sql, request_id).await
    }
    async fn open_session(
        &self,
        db: DatabaseId,
        request_id: &str,
        idle_timeout_ms: u32,
    ) -> ApiResult<SessionBinding> {
        let channels = self.channels.clone();
        let request_id = request_id.to_owned();
        let (response, route) = self
            .router
            .call_data(db, None, move |route| {
                let channels = channels.clone();
                let request_id = request_id.clone();
                async move {
                    let mut client = channels
                        .data(&route.worker_endpoint)
                        .await
                        .map_err(|e| tonic::Status::unavailable(e.to_string()))?;
                    let context = clients::data_request_context(
                        &request_id,
                        db,
                        &route.worker_id,
                        route.owner_epoch,
                        None,
                        0,
                    );
                    let response = client
                        .open_session(clients::open_session_request(context, idle_timeout_ms))
                        .await?
                        .into_inner();
                    if let Some(error) =
                        crate::api::data::stream::proto_error(response.error.as_ref())
                    {
                        return Ok(Err(error));
                    }
                    Ok(Ok((response, route)))
                }
            })
            .await?;
        if response.session_id.is_empty() {
            return Err(ApiError::internal("Worker 返回空会话"));
        }
        let now = Utc::now();
        Ok(SessionBinding {
            session_id: response.session_id,
            database_id: db,
            worker_id: Some(route.worker_id),
            owner_epoch: Some(route.owner_epoch),
            created_at: now,
            expires_at: chrono::DateTime::from_timestamp_millis(response.expires_at_unix_ms as i64)
                .unwrap_or(now + Duration::milliseconds(i64::from(idle_timeout_ms))),
        })
    }
    async fn validate_session(&self, binding: &SessionBinding) -> ApiResult<()> {
        let route = self
            .router
            .resolve_target(binding.database_id, None)
            .await?;
        if binding.worker_id.as_ref() != Some(&route.worker_id)
            || binding.owner_epoch != Some(route.owner_epoch)
        {
            return Err(ApiError::new(
                ErrorCode::SessionLost,
                "会话所属数据库已发生 failover，会话不可恢复",
            ));
        }
        Ok(())
    }
    async fn close_session(&self, binding: &SessionBinding) -> ApiResult<()> {
        let worker = binding
            .worker_id
            .as_ref()
            .ok_or_else(|| ApiError::new(ErrorCode::SessionLost, "非分布式会话"))?;
        self.router
            .close_session(&binding.database_id, worker, &binding.session_id)
            .await
    }
    async fn batch(
        &self,
        db: DatabaseId,
        statements: Vec<(String, Vec<protocol::data::Value>)>,
        atomic: bool,
        request_id: &str,
    ) -> ApiResult<protocol::data::ExecuteBatchResponse> {
        let channels = self.channels.clone();
        let request_id = request_id.to_owned();
        self.router
            .call_data(db, None, move |route| {
                let channels = channels.clone();
                let request_id = request_id.clone();
                let statements = statements.clone();
                async move {
                    let mut client = channels
                        .data(&route.worker_endpoint)
                        .await
                        .map_err(|e| tonic::Status::unavailable(e.to_string()))?;
                    let context = clients::data_request_context(
                        &request_id,
                        db,
                        &route.worker_id,
                        route.owner_epoch,
                        None,
                        0,
                    );
                    let response = client
                        .execute_batch(clients::execute_batch_request(context, statements, atomic))
                        .await?
                        .into_inner();
                    if let Some(error) =
                        crate::api::data::stream::proto_error(response.error.as_ref())
                    {
                        return Ok(Err(error));
                    }
                    Ok(Ok(response))
                }
            })
            .await
    }
    fn remote_lsn(&self) -> bool {
        true
    }
}
