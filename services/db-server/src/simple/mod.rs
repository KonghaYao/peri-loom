//! Simple 装配：本地元数据、宿主和对象存储共用一个实例生命周期。
pub mod config;
mod execution;
pub mod export;
mod instance;
mod jobs;
mod web;
use crate::{
    error::{ApiError, ApiResult},
    state::{AppState, HttpConfig, Readiness, SessionRegistry},
};
use anyhow::{Context, Result};
use catalog::SqliteCatalog;
use config::SimpleConfig;
use database_host::{LocalHost, LocalHostConfig};
use domain::{error::ErrorCode, DatabaseId, LifecycleState};
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{Mutex, RwLock};

pub struct SimpleServices {
    pub catalog: SqliteCatalog,
    pub host: LocalHost,
    pub objects: Arc<objectstore::LocalObjectStore>,
    pub root: PathBuf,
    pub gate: Arc<RwLock<()>>,
    starting: Mutex<()>,
    pub stopping: AtomicBool,
}
impl SimpleServices {
    async fn ensure_available(&self, id: DatabaseId) -> ApiResult<()> {
        if self.stopping.load(Ordering::Acquire) {
            return Err(ApiError::new(ErrorCode::AdmissionDenied, "实例正在停止"));
        }
        if self.catalog.has_pending_mutation(id).await? {
            return Err(ApiError::new(
                ErrorCode::AdmissionDenied,
                "数据库有未完成的删除或恢复作业",
            ));
        }
        let _start = self.starting.lock().await;
        let record = self.catalog.get_database(id).await?;
        if matches!(
            record.state,
            LifecycleState::Stopping | LifecycleState::Draining | LifecycleState::Failed
        ) {
            return Err(ApiError::new(
                ErrorCode::AdmissionDenied,
                "数据库正在维护或已隔离；失败库需显式启动或恢复",
            ));
        }
        if !record.state.is_serving() {
            self.catalog
                .set_lifecycle_state(id, LifecycleState::Starting, None)
                .await?;
            if let Err(error) = self.host.describe(id, None, "SELECT 1".into()).await {
                let _ = self
                    .catalog
                    .set_lifecycle_state(
                        id,
                        if matches!(
                            error.code,
                            ErrorCode::AdmissionDenied | ErrorCode::ResourceExhausted
                        ) {
                            LifecycleState::Cold
                        } else {
                            LifecycleState::Failed
                        },
                        None,
                    )
                    .await;
                return Err(error.into());
            }
            self.catalog
                .set_lifecycle_state(id, LifecycleState::Warm, None)
                .await?;
        }
        Ok(())
    }
    #[allow(clippy::too_many_arguments)] // 与共享长操作受理输入保持一致，事务封装在 Catalog。
    pub async fn submit(
        &self,
        principal: &crate::auth::Principal,
        headers: &axum::http::HeaderMap,
        method: &axum::http::Method,
        path: &str,
        body: &[u8],
        spec: crate::api::LongOperationSpec,
        create: Option<catalog::CreateDatabaseParams>,
    ) -> ApiResult<crate::api::dto::OperationAccepted> {
        if self.stopping.load(Ordering::Acquire) {
            return Err(ApiError::new(ErrorCode::AdmissionDenied, "实例正在停止"));
        }
        if spec.worker_id.is_some() || matches!(spec.job_kind, "DB_MOVE" | "WORKER_DRAIN") {
            return Err(crate::deployment::unsupported());
        }
        let _permit = self.gate.read().await;
        let key = crate::idempotency::extract_idempotency_key(headers)
            .map_err(ApiError::invalid_argument)?;
        let hash = key
            .as_ref()
            .map(|_| crate::idempotency::request_fingerprint(method, path, body));
        let accepted = self
            .catalog
            .submit_operation_job(catalog::LocalSubmission {
                operation: catalog::NewOperation {
                    kind: spec.kind.into(),
                    database_id: spec.database_id,
                    worker_id: None,
                    requested_by: Some(principal.user_id),
                },
                create_database: create,
                job_kind: spec.job_kind.into(),
                job_payload: spec.payload,
                priority: spec.priority,
                idempotency_key: key,
                request_hash: hash,
            })
            .await?;
        Ok(crate::api::dto::OperationAccepted {
            operation_id: Some(accepted.operation.id.to_string()),
            database_id: accepted.operation.database_id.map(|id| id.to_string()),
            state: "PENDING".into(),
            replayed: accepted.replayed,
        })
    }
}

pub async fn run(config: SimpleConfig) -> Result<()> {
    if web::ASSETS.is_empty() {
        anyhow::bail!(
            "此二进制未内嵌 Web：请先构建 web/dist，再重新编译 peri-loom；或使用 Simple 发布镜像"
        );
    }
    if config.tls_cert.is_some() != config.tls_key.is_some() {
        anyhow::bail!("TLS 证书和私钥必须同时配置");
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter(&config.log_level)
        .try_init();
    let instance = instance::Instance::open(&config.data_dir)?;
    // 持有实例锁后清理崩溃遗留暂存物；持久作业会从已发布对象重建暂存内容。
    for entry in std::fs::read_dir(instance.root.join("tmp"))? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            std::fs::remove_dir_all(entry.path())?;
        } else {
            std::fs::remove_file(entry.path())?;
        }
    }
    let catalog = SqliteCatalog::connect(instance.root.join("catalog/metadata.db")).await?;
    catalog.recover_local_jobs().await?;
    let secret = instance.jwt_secret()?;
    if catalog.list_users(1, 0).await?.is_empty() {
        let password = match &config.admin_password_file {
            Some(path) => std::fs::read_to_string(path)
                .context("读取初始化密码文件失败")?
                .trim()
                .to_owned(),
            None => {
                let password = hex::encode(rand::random::<[u8; 24]>());
                instance::atomic_write(
                    &instance.root.join("secrets"),
                    "initial-admin.json",
                    &serde_json::to_vec_pretty(
                        &serde_json::json!({ "username": config.admin_user, "password": password }),
                    )?,
                )?;
                password
            }
        };
        if password.is_empty() {
            anyhow::bail!("初始化密码不能为空");
        }
        crate::auth::bootstrap_admin(&catalog, &config.admin_user, &password).await?;
        tracing::info!(
            "管理员已初始化；随机凭据位于 data-dir/secrets/initial-admin.json，首次登录后删除"
        );
    }
    let services = Arc::new(SimpleServices {
        host: LocalHost::new(
            instance.root.clone(),
            LocalHostConfig {
                max_open_databases: config.max_open_databases,
                max_sessions_per_database: config.max_sessions_per_database,
                queue_capacity: config.queue_capacity,
                max_result_frame_bytes: config.max_result_frame_bytes,
                ..Default::default()
            },
        )?,
        objects: Arc::new(objectstore::LocalObjectStore::new(
            &instance.root.join("objects"),
            &instance.root.join("tmp"),
        )?),
        catalog: catalog.clone(),
        root: instance.root.clone(),
        gate: Arc::new(RwLock::new(())),
        starting: Mutex::new(()),
        stopping: AtomicBool::new(false),
    });
    let metadata: Arc<dyn catalog::Metadata> = Arc::new(catalog);
    let state = AppState {
        config: Arc::new(HttpConfig {
            inline_result_limit_bytes: config.inline_result_limit_bytes,
            session_idle_timeout_ms: 60_000,
        }),
        catalog: metadata.clone(),
        execution: Arc::new(execution::LocalExecutor(services.clone())),
        sessions: Arc::new(SessionRegistry::new()),
        jobs: Arc::new(crate::background::JobQueue::new(metadata)),
        readiness: Arc::new(Readiness::new()),
        jwt: Some(Arc::new(crate::auth::JwtVerifier::hs256(
            &secret,
            "peri-loom",
        ))),
        jwt_issuer: Some(Arc::new(crate::auth::JwtIssuer::hs256(
            &secret,
            "peri-loom",
            3600,
        ))),
        metrics: crate::app::install_prometheus(),
        deployment: crate::deployment::Deployment::Simple(services.clone()),
    };
    // 未完成作业先恢复再接流量，避免删除或恢复中断后打开旧文件。
    jobs::recover(&state, &services)
        .await
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    let router = web::router(crate::api::build_router(state.clone()));
    let listener = tokio::net::TcpListener::bind(config.listen).await?;
    tracing::info!(addr = %listener.local_addr()?, "Simple HTTP 已启动");
    state.readiness.mark_local_ready();
    let runner = jobs::spawn(state.clone(), services.clone());
    let sweeper = crate::background::spawn_session_sweeper(state.clone());
    let outcome = serve(listener, router, &config, state.clone(), services.clone()).await;
    services.stopping.store(true, Ordering::Release);
    state.readiness.mark_local_stopping();
    runner.abort();
    sweeper.abort();
    tokio::time::timeout(Duration::from_secs(35), services.host.shutdown())
        .await
        .context("关闭宿主超时，下次启动将恢复")??;
    services.catalog.clone().close().await?;
    drop(instance);
    outcome
}

async fn serve(
    listener: tokio::net::TcpListener,
    router: axum::Router,
    config: &SimpleConfig,
    state: AppState,
    services: Arc<SimpleServices>,
) -> Result<()> {
    if config.tls_cert.is_some() {
        return web::serve_tls(listener, router, config, state, services).await;
    }
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server = axum::serve(listener, router).with_graceful_shutdown(async move {
        let _ = shutdown_rx.await;
    });
    let server = std::future::IntoFuture::into_future(server);
    tokio::pin!(server);
    tokio::select! {
        result = &mut server => { result?; },
        _ = crate::app::wait_for_shutdown_signal() => {
            services.stopping.store(true, Ordering::Release); state.readiness.mark_local_stopping();
            let _ = shutdown_tx.send(());
            match tokio::time::timeout(Duration::from_secs(35), &mut server).await {
                Ok(result) => result?,
                Err(_) => tracing::warn!("HTTP 排空超时，取消剩余连接并恢复宿主状态"),
            }
        }
    }
    Ok(())
}
