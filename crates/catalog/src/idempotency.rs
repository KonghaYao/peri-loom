//! Idempotency：管理面副作用请求的幂等控制（架构 §16 Public HTTP Contract）。
//!
//! 契约：相同 Idempotency-Key 重复提交 100 次只能创建 1 个 Operation。
//! 实现方式：`INSERT ... ON CONFLICT DO NOTHING` + 同事务内 `SELECT`，
//! 由 `idempotency_keys.key` 主键保证只有一个事务能成功插入。

use domain::error::{ErrorCode, Result};
use domain::ids::OperationId;
use domain::records::OperationRecord;
use uuid::Uuid;

use crate::error::{map_sqlx_error, platform_error, ConflictAs, NotFoundAs};
use crate::operations::NewOperation;
use crate::pg::invalid_argument;
use crate::Catalog;

/// 幂等请求的判定结果。
#[derive(Debug, Clone, PartialEq)]
pub enum IdempotencyOutcome {
    /// 首次请求：调用方应使用该 operation_id 创建 Operation 并推进状态
    First { operation_id: OperationId },
    /// 重复请求：直接回放首次请求的结果
    Replay {
        operation_id: OperationId,
        status: i32,
        body: serde_json::Value,
    },
    /// 同一 key 携带了不同的请求体：必须拒绝（避免不同请求互相「撞」出错误结果）
    Conflict,
}

impl IdempotencyOutcome {
    pub fn is_first(&self) -> bool {
        matches!(self, IdempotencyOutcome::First { .. })
    }

    pub fn is_replay(&self) -> bool {
        matches!(self, IdempotencyOutcome::Replay { .. })
    }

    pub fn is_conflict(&self) -> bool {
        matches!(self, IdempotencyOutcome::Conflict)
    }

    /// 关联的 operation_id（Conflict 时没有）。
    pub fn operation_id(&self) -> Option<OperationId> {
        match self {
            IdempotencyOutcome::First { operation_id }
            | IdempotencyOutcome::Replay { operation_id, .. } => Some(*operation_id),
            IdempotencyOutcome::Conflict => None,
        }
    }
}

/// 已存在的幂等记录（读模型）。
#[derive(Debug, Clone, PartialEq)]
struct ExistingIdempotency {
    request_hash: String,
    operation_id: Option<OperationId>,
    response_status: i32,
    response_body: serde_json::Value,
}

/// [`Catalog::begin_idempotent_operation`] 的结果：幂等去重与 Operation 创建是同一次
/// 事务的产物，因此这里不需要「先拿到 First 再去建 Operation」这类中间态。
///
/// 形状与 [`IdempotencyOutcome`] 保持一致（`First` 只带 id）：
/// 调用方拿到 `First` 时，该 operation 行**已经提交**，可以直接
/// [`Catalog::get_operation`] 读到它，而不是再自己创建一遍。
#[derive(Debug, Clone, PartialEq)]
pub enum IdempotencyOperationOutcome {
    /// 首次请求：幂等记录与本条 Operation 已在同一事务内提交。
    ///
    /// 自愈路径（历史脏数据）也会返回 `First`：那种情况下 Operation 是**刚刚补建**的，
    /// 调用方拿到的是一个真实存在、可以继续推进的 operation_id。
    First { operation_id: OperationId },
    /// 重复请求：直接回放首次请求的结果。
    Replay {
        operation_id: OperationId,
        status: i32,
        body: serde_json::Value,
    },
    /// 同一 key 携带了不同的请求体：必须拒绝（避免不同请求互相「撞」出错误结果）。
    Conflict,
}

impl IdempotencyOperationOutcome {
    pub fn is_first(&self) -> bool {
        matches!(self, IdempotencyOperationOutcome::First { .. })
    }

    pub fn is_replay(&self) -> bool {
        matches!(self, IdempotencyOperationOutcome::Replay { .. })
    }

    pub fn is_conflict(&self) -> bool {
        matches!(self, IdempotencyOperationOutcome::Conflict)
    }

    /// 关联的 operation_id（Conflict 时没有）。
    pub fn operation_id(&self) -> Option<OperationId> {
        match self {
            IdempotencyOperationOutcome::First { operation_id }
            | IdempotencyOperationOutcome::Replay { operation_id, .. } => Some(*operation_id),
            IdempotencyOperationOutcome::Conflict => None,
        }
    }
}

/// 已存在记录的纯逻辑判定结果。
#[derive(Debug, Clone, PartialEq)]
enum ExistingDecision {
    Conflict,
    /// 记录里没有 operation_id（异常数据）：先回填再重放，保证重放结果稳定
    NeedOperationId,
    Replay(IdempotencyOutcome),
}

fn classify_existing(request_hash: &str, existing: ExistingIdempotency) -> ExistingDecision {
    if existing.request_hash != request_hash {
        return ExistingDecision::Conflict;
    }
    match existing.operation_id {
        Some(operation_id) => ExistingDecision::Replay(IdempotencyOutcome::Replay {
            operation_id,
            status: existing.response_status,
            body: existing.response_body,
        }),
        None => ExistingDecision::NeedOperationId,
    }
}

/// 在调用方事务内确保 `operations` 行存在，返回 `(记录, 是否新建)`。
///
/// 插入走 `ON CONFLICT DO NOTHING`：`operations_idempotency_idx` 保证同 `idempotency_key`
/// 只能有 1 个 Operation。冲突时不报错而是回读并**采用**已有的那一行 ——
/// 这样「幂等记录指向一个不存在的 Operation」这种脏数据只会被修好一次，
/// 而不是每次重试都撞一次唯一索引、永久失败。
async fn ensure_operation_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    operation_id: OperationId,
    operation: &NewOperation,
    key: &str,
) -> Result<(OperationRecord, bool)> {
    let created =
        crate::operations::insert_operation(&mut **tx, operation_id, operation, Some(key)).await?;
    if let Some(record) = created {
        return Ok((record, true));
    }

    let existing = crate::operations::find_operation_by_idempotency_key(&mut **tx, key)
        .await?
        .ok_or_else(|| {
            platform_error(
                ErrorCode::InternalError,
                format!("operations 与 idempotency_key '{key}' 冲突但回读不到已有行"),
            )
        })?;
    Ok((existing, false))
}

/// 把幂等记录重新指向实际使用的 Operation，维持
/// 「`idempotency_keys.operation_id` 一定指向真实存在的 `operations.id`」这条不变量。
async fn point_idempotency_key_at(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    key: &str,
    operation_id: OperationId,
) -> Result<()> {
    sqlx::query("UPDATE idempotency_keys SET operation_id = $2::uuid WHERE key = $1::text")
        .bind(key)
        .bind(crate::pg::id_to_uuid(&operation_id)?)
        .execute(&mut **tx)
        .await
        .map_err(|e| map_sqlx_error(e, NotFoundAs::Operation, ConflictAs::Idempotency))?;
    Ok(())
}

impl Catalog {
    /// 开始一次幂等请求（**只写幂等记录**）。
    ///
    /// 全过程在一个事务内：先尝试插入（首次请求），冲突后读取已有记录判定
    /// Replay / Conflict。两个并发请求只会有一个拿到 `First`。
    ///
    /// 注意：本方法不创建 Operation。调用方若走「先 `begin_idempotent` 再
    /// [`Catalog::create_operation_with_id`]」两步，两步之间崩溃会留下
    /// 「幂等命中但 Operation 不存在」的死记录（见 FIX-B）。
    /// 需要创建 Operation 的路径请直接用
    /// [`Catalog::begin_idempotent_operation`]：幂等记录与 Operation 同事务提交。
    #[tracing::instrument(skip(self, request_hash), fields(key))]
    pub async fn begin_idempotent(
        &self,
        key: &str,
        request_hash: &str,
    ) -> Result<IdempotencyOutcome> {
        if key.trim().is_empty() {
            return Err(invalid_argument("Idempotency-Key 不能为空"));
        }
        if request_hash.trim().is_empty() {
            return Err(invalid_argument("idempotency request_hash 不能为空"));
        }

        // 预分配 operation_id：即便首次请求尚未创建 Operation，重放也能拿到稳定 id
        let operation_id = OperationId::new_v7();
        let mut tx = self
            .pool()
            .begin()
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Operation, ConflictAs::Idempotency))?;

        let inserted: Option<Uuid> = sqlx::query_scalar(
            "INSERT INTO idempotency_keys (key, request_hash, operation_id, response_status, response_body)
             VALUES ($1::text, $2::text, $3::uuid, 202, '{}'::jsonb)
             ON CONFLICT (key) DO NOTHING
             RETURNING operation_id",
        )
        .bind(key)
        .bind(request_hash)
        .bind(crate::pg::id_to_uuid(&operation_id)?)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| map_sqlx_error(e, NotFoundAs::Operation, ConflictAs::Idempotency))?;

        if inserted.is_some() {
            tx.commit()
                .await
                .map_err(|e| map_sqlx_error(e, NotFoundAs::Operation, ConflictAs::Idempotency))?;
            return Ok(IdempotencyOutcome::First { operation_id });
        }

        let row: Option<(String, Option<Uuid>, i32, serde_json::Value)> = sqlx::query_as(
            "SELECT request_hash, operation_id, response_status, response_body
             FROM idempotency_keys WHERE key = $1::text",
        )
        .bind(key)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| map_sqlx_error(e, NotFoundAs::Operation, ConflictAs::Idempotency))?;

        let existing = match row {
            Some((request_hash, operation_id, response_status, response_body)) => {
                ExistingIdempotency {
                    request_hash,
                    operation_id: match operation_id {
                        Some(v) => Some(
                            crate::pg::decode_uuid_id::<OperationId>(v, "operation_id").map_err(
                                |e| {
                                    platform_error(
                                        ErrorCode::InternalError,
                                        format!("decode operation id: {e}"),
                                    )
                                },
                            )?,
                        ),
                        None => None,
                    },
                    response_status,
                    response_body,
                }
            }
            None => {
                // 插入冲突但记录已消失（并发 purge）：无法安全重放
                return Err(platform_error(
                    ErrorCode::IdempotencyConflict,
                    format!("idempotency key '{key}' 冲突记录已不可见（并发删除或过期）"),
                ));
            }
        };

        let decision = match classify_existing(request_hash, existing) {
            ExistingDecision::Conflict => {
                tx.rollback().await.ok();
                return Ok(IdempotencyOutcome::Conflict);
            }
            ExistingDecision::Replay(outcome) => outcome,
            ExistingDecision::NeedOperationId => {
                let new_id = OperationId::new_v7();
                let updated: Option<Uuid> = sqlx::query_scalar(
                    "UPDATE idempotency_keys SET operation_id = $2::uuid
                     WHERE key = $1::text AND operation_id IS NULL
                     RETURNING operation_id",
                )
                .bind(key)
                .bind(crate::pg::id_to_uuid(&new_id)?)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| map_sqlx_error(e, NotFoundAs::Operation, ConflictAs::Idempotency))?;

                match updated {
                    Some(v) => IdempotencyOutcome::Replay {
                        operation_id: crate::pg::decode_uuid_id::<OperationId>(v, "operation_id")
                            .map_err(|e| {
                            platform_error(
                                ErrorCode::InternalError,
                                format!("decode operation id: {e}"),
                            )
                        })?,
                        status: 202,
                        body: serde_json::json!({}),
                    },
                    // 并发补写：直接回放当前可见值
                    None => IdempotencyOutcome::Replay {
                        operation_id: new_id,
                        status: 202,
                        body: serde_json::json!({}),
                    },
                }
            }
        };

        tx.commit()
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Operation, ConflictAs::Idempotency))?;
        Ok(decision)
    }

    /// 组合入口（FIX-B）：**在同一个事务里**同时写入幂等记录与 Operation。
    ///
    /// # 修复的问题
    ///
    /// [`Catalog::begin_idempotent`] 与 [`Catalog::create_operation_with_id`] 是两次独立提交：
    /// 进程若在两者之间崩溃（或客户端拿到 202 后控制面被 kill），`idempotency_keys` 里会留下
    /// 一条「命中」的记录，而 `operations` 里没有对应行。此后所有携带同一个 `Idempotency-Key`
    /// 的重试都会重放一个**根本不存在的 operation_id**，调用方永远等不到终态 —— 幂等保护
    /// 反而把这个 key 永久锁死。
    ///
    /// 现在的语义：
    /// - 首次请求：`INSERT idempotency_keys ... ON CONFLICT DO NOTHING` 命中插入后，
    ///   在**同一个事务**内用**同一个** `operation_id` 写入 `operations`，一起提交；
    /// - 重复请求：读回已有记录的 `request_hash` / `operation_id`，
    ///   同 hash 返回 `Replay`、不同 hash 返回 `Conflict`（与 [`Catalog::begin_idempotent`] 一致）；
    /// - 自愈：`Replay` 分支若发现对应的 `operations` 行不存在（历史脏数据），
    ///   会在同一事务内按已有 `operation_id` 把该行补建出来并返回 `First`，同时打 warning。
    ///   缺失的 Operation 必须被建出来，否则这个 key 会永久只能重放一个悬空 id。
    ///
    /// # Errors
    ///
    /// key / request_hash 为空、operation.kind 不在白名单内时返回 `INVALID_ARGUMENT`。
    #[tracing::instrument(skip(self, request_hash, operation), fields(key, kind = %operation.kind))]
    pub async fn begin_idempotent_operation(
        &self,
        key: &str,
        request_hash: &str,
        operation: NewOperation,
    ) -> Result<IdempotencyOperationOutcome> {
        if key.trim().is_empty() {
            return Err(invalid_argument("Idempotency-Key 不能为空"));
        }
        if request_hash.trim().is_empty() {
            return Err(invalid_argument("idempotency request_hash 不能为空"));
        }
        // 参数校验放在开事务前：非法 kind 不该占用一次数据库事务
        operation.validate()?;

        // 预分配 operation_id：幂等记录与 Operation 共用它，重放时能直接定位到那一行
        let operation_id = OperationId::new_v7();
        let mut tx = self
            .pool()
            .begin()
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Operation, ConflictAs::Idempotency))?;

        let inserted: Option<Uuid> = sqlx::query_scalar(
            "INSERT INTO idempotency_keys (key, request_hash, operation_id, response_status, response_body)
             VALUES ($1::text, $2::text, $3::uuid, 202, '{}'::jsonb)
             ON CONFLICT (key) DO NOTHING
             RETURNING operation_id",
        )
        .bind(key)
        .bind(request_hash)
        .bind(crate::pg::id_to_uuid(&operation_id)?)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| map_sqlx_error(e, NotFoundAs::Operation, ConflictAs::Idempotency))?;

        if inserted.is_some() {
            // First：幂等键与 Operation 同事务提交。任一步失败都整事务回滚，
            // 不可能再留下「幂等命中但 Operation 不存在」的记录。
            let (record, created) =
                ensure_operation_in_tx(&mut tx, operation_id, &operation, key).await?;
            if !created {
                // 幂等记录曾被 purge、但同 key 的 Operation 还在：
                // 唯一索引挡下了第二条，这里采用已存在的那条，并把幂等记录重新指过去
                tracing::warn!(
                    key,
                    operation_id = %record.id,
                    "同 key 的 Operation 已存在（幂等记录曾被清理），采用已有 Operation"
                );
                point_idempotency_key_at(&mut tx, key, record.id).await?;
            }
            tx.commit()
                .await
                .map_err(|e| map_sqlx_error(e, NotFoundAs::Operation, ConflictAs::Idempotency))?;
            return Ok(IdempotencyOperationOutcome::First {
                operation_id: record.id,
            });
        }

        let row: Option<(String, Option<Uuid>, i32, serde_json::Value)> = sqlx::query_as(
            "SELECT request_hash, operation_id, response_status, response_body
             FROM idempotency_keys WHERE key = $1::text",
        )
        .bind(key)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| map_sqlx_error(e, NotFoundAs::Operation, ConflictAs::Idempotency))?;

        let existing = match row {
            Some((request_hash, operation_id, response_status, response_body)) => {
                ExistingIdempotency {
                    request_hash,
                    operation_id: match operation_id {
                        Some(v) => Some(
                            crate::pg::decode_uuid_id::<OperationId>(v, "operation_id").map_err(
                                |e| {
                                    platform_error(
                                        ErrorCode::InternalError,
                                        format!("decode operation id: {e}"),
                                    )
                                },
                            )?,
                        ),
                        None => None,
                    },
                    response_status,
                    response_body,
                }
            }
            None => {
                // 插入冲突但记录已不可见（并发 purge）：无法安全重放
                return Err(platform_error(
                    ErrorCode::IdempotencyConflict,
                    format!("idempotency key '{key}' 冲突记录已不可见（并发删除或过期）"),
                ));
            }
        };

        match classify_existing(request_hash, existing) {
            ExistingDecision::Conflict => {
                tx.rollback().await.ok();
                return Ok(IdempotencyOperationOutcome::Conflict);
            }
            ExistingDecision::Replay(IdempotencyOutcome::Replay {
                operation_id,
                status,
                body,
            }) => {
                // 自愈：Replay 指向的 Operation 必须真实存在。
                // 只信任 idempotency_keys 一行是不够的 —— 历史脏数据（先提交幂等键、
                // 还没建 Operation 就崩溃）会让重放永久吐出一个查不到的 operation_id。
                let alive: Option<Uuid> =
                    sqlx::query_scalar("SELECT id FROM operations WHERE id = $1::uuid")
                        .bind(crate::pg::id_to_uuid(&operation_id)?)
                        .fetch_optional(&mut *tx)
                        .await
                        .map_err(|e| {
                            map_sqlx_error(e, NotFoundAs::Operation, ConflictAs::Idempotency)
                        })?;

                if alive.is_some() {
                    tx.commit().await.map_err(|e| {
                        map_sqlx_error(e, NotFoundAs::Operation, ConflictAs::Idempotency)
                    })?;
                    return Ok(IdempotencyOperationOutcome::Replay {
                        operation_id,
                        status,
                        body,
                    });
                }

                tracing::warn!(
                    key,
                    operation_id = %operation_id,
                    "幂等记录指向的 Operation 不存在（历史脏数据），回退为 First 并在本事务内补建"
                );
                let (record, _) =
                    ensure_operation_in_tx(&mut tx, operation_id, &operation, key).await?;
                if record.id != operation_id {
                    point_idempotency_key_at(&mut tx, key, record.id).await?;
                }
                tx.commit().await.map_err(|e| {
                    map_sqlx_error(e, NotFoundAs::Operation, ConflictAs::Idempotency)
                })?;
                return Ok(IdempotencyOperationOutcome::First {
                    operation_id: record.id,
                });
            }
            ExistingDecision::NeedOperationId => {
                // 历史数据里 operation_id 为空：回填一个稳定 id，并把它对应的 Operation 一并建出来
                let new_id = OperationId::new_v7();
                let updated: Option<Uuid> = sqlx::query_scalar(
                    "UPDATE idempotency_keys SET operation_id = $2::uuid
                     WHERE key = $1::text AND operation_id IS NULL
                     RETURNING operation_id",
                )
                .bind(key)
                .bind(crate::pg::id_to_uuid(&new_id)?)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| map_sqlx_error(e, NotFoundAs::Operation, ConflictAs::Idempotency))?;

                let op_id = match updated {
                    Some(v) => crate::pg::decode_uuid_id::<OperationId>(v, "operation_id")
                        .map_err(|e| {
                            platform_error(
                                ErrorCode::InternalError,
                                format!("decode operation id: {e}"),
                            )
                        })?,
                    None => {
                        // 并发的重试刚补写过：以库里当前值为准，
                        // 否则会把 Operation 挂到一个没人引用的 id 上
                        let current: Option<Option<Uuid>> = sqlx::query_scalar(
                            "SELECT operation_id FROM idempotency_keys WHERE key = $1::text",
                        )
                        .bind(key)
                        .fetch_optional(&mut *tx)
                        .await
                        .map_err(|e| {
                            map_sqlx_error(e, NotFoundAs::Operation, ConflictAs::Idempotency)
                        })?;
                        match current.flatten() {
                            Some(v) => crate::pg::decode_uuid_id::<OperationId>(v, "operation_id")
                                .map_err(|e| {
                                    platform_error(
                                        ErrorCode::InternalError,
                                        format!("decode operation id: {e}"),
                                    )
                                })?,
                            None => new_id,
                        }
                    }
                };

                let (record, _) = ensure_operation_in_tx(&mut tx, op_id, &operation, key).await?;
                if record.id != op_id {
                    point_idempotency_key_at(&mut tx, key, record.id).await?;
                }
                tx.commit().await.map_err(|e| {
                    map_sqlx_error(e, NotFoundAs::Operation, ConflictAs::Idempotency)
                })?;
                Ok(IdempotencyOperationOutcome::First {
                    operation_id: record.id,
                })
            }
            // `classify_existing` 只会产出上面两种形态（Replay / NeedOperationId），
            // 这里显式兜底：返回内部错误而不是 `unreachable!()`，
            // 避免未来改动把 panic 带进请求路径。
            other => Err(platform_error(
                ErrorCode::InternalError,
                format!("幂等分类结果异常: {other:?}"),
            )),
        }
    }

    /// 把首次请求创建的 Operation 关联到幂等记录上（重放时可直接定位 operation）。
    pub async fn attach_idempotent_operation(
        &self,
        key: &str,
        operation_id: OperationId,
    ) -> Result<bool> {
        let updated: Option<String> = sqlx::query_scalar(
            "UPDATE idempotency_keys SET operation_id = $2::uuid
             WHERE key = $1::text RETURNING key",
        )
        .bind(key)
        .bind(crate::pg::id_to_uuid(&operation_id)?)
        .fetch_optional(self.pool())
        .await
        .map_err(|e| map_sqlx_error(e, NotFoundAs::Operation, ConflictAs::Idempotency))?;
        Ok(updated.is_some())
    }

    /// 记录首次请求的响应，供后续重放（同 key 的重复提交不会再创建 Operation）。
    pub async fn complete_idempotent(
        &self,
        key: &str,
        status: u16,
        body: serde_json::Value,
    ) -> Result<bool> {
        let updated: Option<String> = sqlx::query_scalar(
            "UPDATE idempotency_keys SET response_status = $2::int, response_body = $3::jsonb
             WHERE key = $1::text RETURNING key",
        )
        .bind(key)
        .bind(i32::from(status))
        .bind(body)
        .fetch_optional(self.pool())
        .await
        .map_err(|e| map_sqlx_error(e, NotFoundAs::Operation, ConflictAs::Idempotency))?;
        Ok(updated.is_some())
    }

    /// 清理过期幂等记录（`idempotency_expiry_idx` 支撑），返回删除行数。
    pub async fn purge_expired_idempotency(&self) -> Result<u64> {
        let result = sqlx::query("DELETE FROM idempotency_keys WHERE expires_at < now()")
            .execute(self.pool())
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Operation, ConflictAs::Idempotency))?;
        Ok(result.rows_affected())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn existing(hash: &str, op: Option<OperationId>, status: i32) -> ExistingIdempotency {
        ExistingIdempotency {
            request_hash: hash.to_string(),
            operation_id: op,
            response_status: status,
            response_body: json!({"operation_id": "x"}),
        }
    }

    #[test]
    fn same_hash_and_operation_replays_stored_response() {
        let op = OperationId::new_v7();
        let decision = classify_existing("h1", existing("h1", Some(op), 202));
        match decision {
            ExistingDecision::Replay(IdempotencyOutcome::Replay {
                operation_id,
                status,
                body,
            }) => {
                assert_eq!(operation_id, op);
                assert_eq!(status, 202);
                assert_eq!(body, json!({"operation_id": "x"}));
            }
            other => panic!("expected replay, got {other:?}"),
        }
    }

    #[test]
    fn different_hash_is_a_conflict() {
        let op = OperationId::new_v7();
        assert_eq!(
            classify_existing("h2", existing("h1", Some(op), 202)),
            ExistingDecision::Conflict
        );
    }

    #[test]
    fn missing_operation_id_triggers_backfill() {
        assert_eq!(
            classify_existing("h1", existing("h1", None, 202)),
            ExistingDecision::NeedOperationId
        );
    }

    #[test]
    fn outcome_helpers() {
        let op = OperationId::new_v7();
        let first = IdempotencyOutcome::First { operation_id: op };
        assert!(first.is_first());
        assert_eq!(first.operation_id(), Some(op));

        let replay = IdempotencyOutcome::Replay {
            operation_id: op,
            status: 200,
            body: json!({}),
        };
        assert!(replay.is_replay());
        assert_eq!(replay.operation_id(), Some(op));

        assert!(IdempotencyOutcome::Conflict.is_conflict());
        assert_eq!(IdempotencyOutcome::Conflict.operation_id(), None);
    }
}
