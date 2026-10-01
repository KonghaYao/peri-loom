//! 部署信息只在装配和能力检查处选择，业务调用不依赖集群地址。
use crate::{error::ApiError, router::DbRouter};
use serde::Serialize;
use std::sync::Arc;

#[derive(Clone)]
pub enum Deployment {
    Distributed(Arc<DistributedServices>),
    Simple(Arc<crate::simple::SimpleServices>),
}

pub struct DistributedServices {
    pub catalog: catalog::Catalog,
    pub router: Arc<DbRouter>,
}

#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct DeploymentInfo {
    pub mode: &'static str,
    pub contract_version: u32,
    pub durability: &'static str,
    pub capabilities: Capabilities,
}
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct Capabilities {
    pub workers: bool,
    pub database_move: bool,
    pub remote_durability_lsn: bool,
    pub local_backup: bool,
    pub per_database_hard_isolation: bool,
}
impl Deployment {
    pub fn info(&self) -> DeploymentInfo {
        let distributed = matches!(self, Self::Distributed(_));
        DeploymentInfo {
            mode: if distributed { "distributed" } else { "simple" },
            contract_version: 1,
            durability: if distributed {
                "remote_quorum"
            } else {
                "local_fsync"
            },
            capabilities: Capabilities {
                workers: distributed,
                database_move: distributed,
                remote_durability_lsn: distributed,
                local_backup: !distributed,
                per_database_hard_isolation: distributed,
            },
        }
    }
    pub fn require_cluster(&self) -> Result<(), ApiError> {
        if matches!(self, Self::Distributed(_)) {
            Ok(())
        } else {
            Err(unsupported())
        }
    }
}

pub fn unsupported() -> ApiError {
    // 沿用既有错误码，detail 提供可稳定识别的部署原因，不改变内部 proto 枚举。
    ApiError::new(
        domain::error::ErrorCode::NotImplemented,
        "UNSUPPORTED_IN_DEPLOYMENT_MODE",
    )
    .with_detail(serde_json::json!({"reason": "UNSUPPORTED_IN_DEPLOYMENT_MODE", "mode": "simple"}))
}
