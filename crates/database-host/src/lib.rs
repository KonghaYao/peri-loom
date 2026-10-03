//! 单进程数据库宿主。每个库只有一个执行线程，连接不越过线程边界。
#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use domain::error::{ErrorCode, PlatformError, Result};
use domain::value::{ColumnMeta, SqlValue};
use domain::DatabaseId;
use engine_adapter::{EngineAdapter, EngineConnection, EngineOpenConfig};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

fn stage_metrics_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var_os("PERI_LOOM_SIMPLE_STAGE_METRICS").as_deref()
            == Some(std::ffi::OsStr::new("1"))
    })
}

fn record_stage(stage: &'static str, started: Option<Instant>) {
    if let Some(started) = started {
        metrics::histogram!("simple_stage_micros", "stage" => stage)
            .record(started.elapsed().as_secs_f64() * 1_000_000.0);
    }
}

#[derive(Clone)]
pub struct LocalHostConfig {
    pub max_open_databases: usize,
    pub max_sessions_per_database: usize,
    pub queue_capacity: usize,
    pub row_batch_size: usize,
    pub max_result_frame_bytes: usize,
    pub session_idle_timeout: Duration,
    pub transaction_timeout: Duration,
}

impl Default for LocalHostConfig {
    fn default() -> Self {
        Self {
            max_open_databases: 64,
            max_sessions_per_database: 128,
            queue_capacity: 64,
            row_batch_size: 64,
            max_result_frame_bytes: 256 * 1024,
            session_idle_timeout: Duration::from_secs(60),
            transaction_timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExecutionFrame {
    Columns(Vec<ColumnMeta>),
    Rows(Vec<Vec<SqlValue>>),
    End {
        is_autocommit: bool,
        last_insert_rowid: i64,
        affected_rows: u64,
        durability: Durability,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    Local,
}

pub struct ExecutionStream {
    pub frames: mpsc::Receiver<Result<ExecutionFrame>>,
    cancel: Arc<AtomicBool>,
}

impl ExecutionStream {
    pub fn cancel(&self) {
        self.cancel.store(true, Ordering::SeqCst);
    }
}
impl Drop for ExecutionStream {
    fn drop(&mut self) {
        self.cancel();
    }
}

#[derive(Clone)]
pub struct LocalHost {
    root: PathBuf,
    config: LocalHostConfig,
    workers: Arc<Mutex<HashMap<DatabaseId, mpsc::Sender<Command>>>>,
    closing: Arc<Mutex<std::collections::HashSet<DatabaseId>>>,
    stopped: Arc<AtomicBool>,
}

enum Command {
    Execute {
        dispatched_at: Option<Instant>,
        session: Option<Uuid>,
        sql: String,
        params: Vec<SqlValue>,
        deadline: Instant,
        cancel: Arc<AtomicBool>,
        output: mpsc::Sender<Result<ExecutionFrame>>,
    },
    Describe {
        session: Option<Uuid>,
        sql: String,
        answer: oneshot::Sender<Result<engine_adapter::engine::DescribeOutcome>>,
    },
    OpenSession {
        answer: oneshot::Sender<Result<Uuid>>,
    },
    CloseSession {
        session: Uuid,
        answer: oneshot::Sender<Result<()>>,
    },
    HasSession {
        session: Uuid,
        answer: oneshot::Sender<bool>,
    },
    Snapshot {
        destination: PathBuf,
        answer: oneshot::Sender<Result<()>>,
    },
    Stop {
        answer: oneshot::Sender<Result<()>>,
    },
}

struct Session {
    connection: EngineConnection,
    last_used: Instant,
    transaction_started: Option<Instant>,
}

impl LocalHost {
    pub fn new(root: PathBuf, config: LocalHostConfig) -> Result<Self> {
        if config.max_open_databases == 0
            || config.queue_capacity == 0
            || config.row_batch_size == 0
            || config.max_result_frame_bytes == 0
        {
            return Err(PlatformError::invalid_argument("宿主容量必须大于零"));
        }
        fs::create_dir_all(root.join("databases")).map_err(io_error)?;
        Ok(Self {
            root,
            config,
            workers: Arc::new(Mutex::new(HashMap::new())),
            closing: Arc::new(Mutex::new(std::collections::HashSet::new())),
            stopped: Arc::new(AtomicBool::new(false)),
        })
    }

    pub async fn execute(
        &self,
        db: DatabaseId,
        session: Option<Uuid>,
        sql: String,
        params: Vec<SqlValue>,
        timeout: Duration,
    ) -> Result<ExecutionStream> {
        let worker = self.worker(db)?;
        // 点查通常产生 Columns、Rows、End 三帧。容纳这三帧可避免宿主线程
        // 因客户端任务暂未调度而进入 send_frame 的 5 ms 满队列轮询。
        let (output, frames) = mpsc::channel(3);
        let cancel = Arc::new(AtomicBool::new(false));
        let dispatch_started = stage_metrics_enabled().then(Instant::now);
        tokio::time::timeout(
            timeout,
            worker.send(Command::Execute {
                dispatched_at: dispatch_started,
                session,
                sql,
                params,
                deadline: Instant::now() + timeout,
                cancel: cancel.clone(),
                output,
            }),
        )
        .await
        .map_err(|_| timeout_error())?
        .map_err(|_| worker_lost())?;
        record_stage("host_enqueue", dispatch_started);
        Ok(ExecutionStream { frames, cancel })
    }

    pub async fn describe(
        &self,
        db: DatabaseId,
        session: Option<Uuid>,
        sql: String,
    ) -> Result<engine_adapter::engine::DescribeOutcome> {
        let (answer, rx) = oneshot::channel();
        tokio::time::timeout(
            Duration::from_secs(5),
            self.worker(db)?.send(Command::Describe {
                session,
                sql,
                answer,
            }),
        )
        .await
        .map_err(|_| timeout_error())?
        .map_err(|_| worker_lost())?;
        tokio::time::timeout(Duration::from_secs(5), rx)
            .await
            .map_err(|_| timeout_error())?
            .map_err(|_| worker_lost())?
    }

    pub async fn open_session(&self, db: DatabaseId) -> Result<Uuid> {
        let (answer, rx) = oneshot::channel();
        tokio::time::timeout(
            Duration::from_secs(5),
            self.worker(db)?.send(Command::OpenSession { answer }),
        )
        .await
        .map_err(|_| timeout_error())?
        .map_err(|_| worker_lost())?;
        tokio::time::timeout(Duration::from_secs(5), rx)
            .await
            .map_err(|_| timeout_error())?
            .map_err(|_| worker_lost())?
    }

    pub async fn close_session(&self, db: DatabaseId, session: Uuid) -> Result<()> {
        let (answer, rx) = oneshot::channel();
        let worker = self
            .workers
            .lock()
            .unwrap()
            .get(&db)
            .cloned()
            .ok_or_else(|| PlatformError::session_lost("数据库会话不存在"))?;
        tokio::time::timeout(
            Duration::from_secs(5),
            worker.send(Command::CloseSession { session, answer }),
        )
        .await
        .map_err(|_| timeout_error())?
        .map_err(|_| worker_lost())?;
        tokio::time::timeout(Duration::from_secs(5), rx)
            .await
            .map_err(|_| timeout_error())?
            .map_err(|_| worker_lost())?
    }

    pub async fn has_session(&self, db: DatabaseId, session: Uuid) -> bool {
        let worker = self.workers.lock().unwrap().get(&db).cloned();
        let Some(worker) = worker else { return false };
        let (answer, rx) = oneshot::channel();
        if tokio::time::timeout(
            Duration::from_secs(5),
            worker.send(Command::HasSession { session, answer }),
        )
        .await
        .is_err()
        {
            return false;
        }
        tokio::time::timeout(Duration::from_secs(5), rx)
            .await
            .ok()
            .and_then(|v| v.ok())
            .unwrap_or(false)
    }

    pub fn cancel(&self, stream: &ExecutionStream) {
        stream.cancel();
    }

    pub async fn snapshot(&self, db: DatabaseId, destination: PathBuf) -> Result<()> {
        let (answer, rx) = oneshot::channel();
        self.worker(db)?
            .send(Command::Snapshot {
                destination,
                answer,
            })
            .await
            .map_err(|_| worker_lost())?;
        rx.await.map_err(|_| worker_lost())?
    }

    pub async fn close_db(&self, db: DatabaseId) -> Result<()> {
        {
            let mut closing = self.closing.lock().unwrap();
            if !closing.insert(db) {
                return Err(PlatformError::unavailable("数据库正在关闭"));
            }
        }
        let result = self.stop_worker(db).await;
        self.closing.lock().unwrap().remove(&db);
        result
    }

    async fn stop_worker(&self, db: DatabaseId) -> Result<()> {
        let worker = self.workers.lock().unwrap().get(&db).cloned();
        if let Some(worker) = worker {
            let (answer, rx) = oneshot::channel();
            let result = worker
                .send(Command::Stop { answer })
                .await
                .map_err(|_| worker_lost())
                .map(|_| ());
            let result = match result {
                Ok(()) => rx.await.map_err(|_| worker_lost())?,
                Err(e) => Err(e),
            };
            self.workers.lock().unwrap().remove(&db);
            result
        } else {
            Ok(())
        }
    }

    /// 替换已校验的暂存快照，旧会话在关闭目标库时失效。
    pub async fn restore(&self, db: DatabaseId, staged: PathBuf) -> Result<()> {
        {
            let mut closing = self.closing.lock().unwrap();
            if !closing.insert(db) {
                return Err(PlatformError::unavailable("数据库正在关闭"));
            }
        }
        let result = async {
            if !staged.join("main.db").is_file() {
                return Err(PlatformError::invalid_argument("快照缺少数据库主文件"));
            }
            self.stop_worker(db).await?;
            let target = self.root.join("databases").join(db.to_string());
            let backup = self
                .root
                .join("databases")
                .join(format!(".{db}.restore-backup"));
            if backup.exists() {
                // 两个 rename 之间崩溃：旧库仍在 backup，先回退到已知可用状态。
                if !target.exists() {
                    fs::rename(&backup, &target).map_err(io_error)?;
                    sync_dir(target.parent().unwrap())?;
                } else {
                    // 第二个 rename 已发布：暂存快照仍由作业重建；保留旧库备份，
                    // 丢弃这份待确认的新目录后重新发布已校验的暂存内容。
                    fs::remove_dir_all(&target).map_err(io_error)?;
                    sync_dir(target.parent().unwrap())?;
                    fs::rename(&backup, &target).map_err(io_error)?;
                    sync_dir(target.parent().unwrap())?;
                }
            }
            if target.exists() {
                fs::rename(&target, &backup).map_err(io_error)?;
            }
            if let Err(error) = fs::rename(&staged, &target) {
                if backup.exists() {
                    let _ = fs::rename(&backup, &target);
                }
                return Err(io_error(error));
            }
            sync_dir(target.parent().unwrap())?;
            if backup.exists() {
                fs::remove_dir_all(&backup).map_err(io_error)?;
                sync_dir(target.parent().unwrap())?;
            }
            Ok(())
        }
        .await;
        self.closing.lock().unwrap().remove(&db);
        result
    }

    pub async fn shutdown(&self) -> Result<()> {
        {
            let _closing = self.closing.lock().unwrap();
            self.stopped.store(true, Ordering::SeqCst);
        }
        let dbs: Vec<_> = self.workers.lock().unwrap().keys().copied().collect();
        for db in dbs {
            self.close_db(db).await?;
        }
        Ok(())
    }

    fn worker(&self, db: DatabaseId) -> Result<mpsc::Sender<Command>> {
        let closing = self.closing.lock().unwrap();
        if self.stopped.load(Ordering::SeqCst) || closing.contains(&db) {
            return Err(PlatformError::unavailable("宿主或数据库正在关闭"));
        }
        let mut workers = self.workers.lock().unwrap();
        if let Some(worker) = workers.get(&db) {
            if !worker.is_closed() {
                return Ok(worker.clone());
            }
            workers.remove(&db);
        }
        if workers.len() >= self.config.max_open_databases {
            return Err(PlatformError::new(
                ErrorCode::ResourceExhausted,
                "打开数据库数量已达上限",
            ));
        }
        let path = self.root.join("databases").join(db.to_string());
        if self
            .root
            .join("databases")
            .join(format!(".{db}.restore-backup"))
            .exists()
        {
            return Err(PlatformError::unavailable("数据库恢复仍有未完成的目录替换"));
        }
        fs::create_dir_all(&path).map_err(io_error)?;
        sync_dir(path.parent().unwrap())?;
        let (sender, receiver) = mpsc::channel(self.config.queue_capacity);
        let config = self.config.clone();
        std::thread::Builder::new()
            .name(format!("local-db-{db}"))
            .spawn(move || run_worker(path, config, receiver))
            .map_err(io_error)?;
        workers.insert(db, sender.clone());
        Ok(sender)
    }
}

fn run_worker(path: PathBuf, config: LocalHostConfig, mut commands: mpsc::Receiver<Command>) {
    let adapter = (|| -> Result<Arc<EngineAdapter>> {
        let io =
            Arc::new(turso_core::UnixIO::new().map_err(|e| engine_adapter::map_engine_error(&e))?);
        let db_path = path.join("main.db");
        let adapter = EngineAdapter::open(
            io,
            EngineOpenConfig {
                db_path,
                durable_io: None,
                owner_epoch: 0,
            },
        )?;
        sync_dir(&path)?;
        Ok(adapter)
    })();
    if let Err(error) = &adapter {
        if let Some(command) = commands.blocking_recv() {
            reject(command, error.clone());
        }
        return;
    }
    let mut sessions = HashMap::<Uuid, Session>::new();
    let mut transaction_owner = None::<Uuid>;
    let mut deferred = std::collections::VecDeque::<Command>::new();
    let mut shutdown_answer = None;
    loop {
        expire_sessions(&mut sessions, &mut transaction_owner, &config);
        deferred.retain(|command| {
            if let Command::Execute {
                deadline,
                cancel,
                output,
                ..
            } = command
            {
                if Instant::now() >= *deadline || cancel.load(Ordering::Relaxed) {
                    let _ = output.try_send(Err(timeout_error()));
                    return false;
                }
            }
            true
        });
        let command = if transaction_owner.is_none() {
            deferred.pop_front()
        } else {
            None
        };
        let command = match command {
            Some(command) => Some(command),
            // 没有会话和待处理命令时直接等待通知。固定 10 ms 轮询会让
            // stateless 热库请求平白等待半个轮询周期，低并发点查尤其明显。
            None if sessions.is_empty() && transaction_owner.is_none() => commands.blocking_recv(),
            // 有会话时仍定期醒来，让闲置会话/事务过期并清理 deferred 请求。
            None => match commands.try_recv() {
                Ok(command) => Some(command),
                Err(mpsc::error::TryRecvError::Empty) => {
                    std::thread::sleep(Duration::from_millis(10));
                    continue;
                }
                Err(mpsc::error::TryRecvError::Disconnected) => None,
            },
        };
        let Some(command) = command else { break };
        let Ok(adapter) = &adapter else {
            reject(command, adapter.as_ref().err().unwrap().clone());
            continue;
        };
        match command {
            Command::Execute {
                dispatched_at,
                session,
                sql,
                params,
                deadline,
                cancel,
                output,
            } => {
                if transaction_owner.is_some() && transaction_owner != session {
                    if Instant::now() >= deadline {
                        let _ = output.try_send(Err(timeout_error()));
                    } else if deferred.len() < config.queue_capacity {
                        deferred.push_back(Command::Execute {
                            dispatched_at,
                            session,
                            sql,
                            params,
                            deadline,
                            cancel,
                            output,
                        });
                    } else {
                        let _ = output.try_send(Err(PlatformError::new(
                            ErrorCode::ResourceExhausted,
                            "事务等待队列已满",
                        )));
                    }
                    continue;
                }
                record_stage("host_dispatch_to_worker", dispatched_at);
                let request = ExecuteRequest {
                    session,
                    sql,
                    params,
                    deadline,
                    cancel,
                    output: output.clone(),
                };
                let result = execute_command(adapter, &mut sessions, request, &config);
                if let Err(error) = result {
                    let _ = output.try_send(Err(error));
                }
                transaction_owner = sessions
                    .iter()
                    .find(|(_, s)| s.transaction_started.is_some())
                    .map(|(id, _)| *id);
            }
            Command::Describe {
                session,
                sql,
                answer,
            } => {
                let result = if let Some(id) = session {
                    sessions
                        .get_mut(&id)
                        .ok_or_else(|| PlatformError::session_lost("会话不存在"))
                        .and_then(|s| s.connection.describe(&sql))
                } else {
                    adapter.connect().and_then(|c| c.describe(&sql))
                };
                let _ = answer.send(result);
            }
            Command::OpenSession { answer } => {
                let result = if sessions.len() >= config.max_sessions_per_database {
                    Err(PlatformError::new(
                        ErrorCode::ResourceExhausted,
                        "会话数量已达上限",
                    ))
                } else {
                    adapter.connect().map(|connection| {
                        connection.enforce_full_sync();
                        let id = Uuid::new_v4();
                        sessions.insert(
                            id,
                            Session {
                                connection,
                                last_used: Instant::now(),
                                transaction_started: None,
                            },
                        );
                        id
                    })
                };
                let _ = answer.send(result);
            }
            Command::CloseSession { session, answer } => {
                if let Some(s) = sessions.remove(&session) {
                    if s.transaction_started.is_some() {
                        let _ = s.connection.rollback();
                    }
                }
                if transaction_owner == Some(session) {
                    transaction_owner = None;
                }
                let _ = answer.send(Ok(()));
            }
            Command::HasSession { session, answer } => {
                let _ = answer.send(sessions.contains_key(&session));
            }
            Command::Snapshot {
                destination,
                answer,
            } => {
                let result = if transaction_owner.is_some() {
                    Err(PlatformError::new(
                        ErrorCode::TransactionLost,
                        "事务活动期间不能快照",
                    ))
                } else {
                    adapter.connect().and_then(|c| {
                        c.enforce_full_sync();
                        c.checkpoint_full()?;
                        sync_dir(&path)?;
                        snapshot_files(&path, &destination)
                    })
                };
                let _ = answer.send(result);
            }
            Command::Stop { answer } => {
                for (_, s) in sessions.drain() {
                    if s.transaction_started.is_some() {
                        let _ = s.connection.rollback();
                    }
                }
                shutdown_answer = Some(answer);
                break;
            }
        }
    }
    drop(sessions);
    drop(adapter);
    if let Some(answer) = shutdown_answer {
        let _ = answer.send(Ok(()));
    }
}

struct ExecuteRequest {
    session: Option<Uuid>,
    sql: String,
    params: Vec<SqlValue>,
    deadline: Instant,
    cancel: Arc<AtomicBool>,
    output: mpsc::Sender<Result<ExecutionFrame>>,
}

fn execute_command(
    adapter: &Arc<EngineAdapter>,
    sessions: &mut HashMap<Uuid, Session>,
    request: ExecuteRequest,
    config: &LocalHostConfig,
) -> Result<()> {
    let ExecuteRequest {
        session,
        sql,
        params,
        deadline,
        cancel,
        output,
    } = request;
    if Instant::now() >= deadline {
        return Err(timeout_error());
    }
    validate_sql(&sql)?;
    if let Some(id) = session {
        let s = sessions
            .get_mut(&id)
            .ok_or_else(|| PlatformError::session_lost("会话不存在或已超时"))?;
        s.last_used = Instant::now();
        let result = execute_on_connection(
            adapter,
            &s.connection,
            &sql,
            &params,
            deadline,
            &cancel,
            &output,
            config,
            false,
        );
        if result.is_err()
            && (cancel.load(Ordering::Relaxed) || Instant::now() >= deadline || output.is_closed())
            && !s.connection.is_autocommit()
        {
            s.connection.rollback()?;
        }
        s.transaction_started = if s.connection.is_autocommit() {
            None
        } else {
            Some(s.transaction_started.unwrap_or_else(Instant::now))
        };
        return result;
    }
    let connect_started = stage_metrics_enabled().then(Instant::now);
    let ephemeral = adapter.connect()?;
    ephemeral.enforce_full_sync();
    record_stage("host_stateless_connect", connect_started);
    let result = execute_on_connection(
        adapter, &ephemeral, &sql, &params, deadline, &cancel, &output, config, true,
    );
    if !ephemeral.is_autocommit() {
        ephemeral.rollback()?;
        return result.and(Err(PlatformError::invalid_argument("显式事务需要会话")));
    }
    result
}

#[allow(clippy::too_many_arguments)]
fn execute_on_connection(
    adapter: &Arc<EngineAdapter>,
    connection: &EngineConnection,
    sql: &str,
    params: &[SqlValue],
    deadline: Instant,
    cancel: &Arc<AtomicBool>,
    output: &mpsc::Sender<Result<ExecutionFrame>>,
    config: &LocalHostConfig,
    require_autocommit: bool,
) -> Result<()> {
    connection.set_cancel_flag(cancel.clone());
    connection.set_query_timeout(deadline.saturating_duration_since(Instant::now()));
    let mut batch = Vec::with_capacity(config.row_batch_size);
    let mut batch_bytes = 0usize;
    let engine_started = stage_metrics_enabled().then(Instant::now);
    let result = connection.stream_with_params(
        sql,
        params,
        |columns| {
            let bytes = columns
                .iter()
                .map(|column| {
                    column
                        .name
                        .len()
                        .saturating_add(column.type_name.len())
                        .saturating_add(16)
                })
                .sum::<usize>();
            if bytes > config.max_result_frame_bytes {
                return Err(result_too_large());
            }
            send_frame(output, ExecutionFrame::Columns(columns), cancel, deadline)
        },
        |row| {
            if cancel.load(Ordering::Relaxed) || Instant::now() >= deadline {
                return Err(timeout_error());
            }
            let row_bytes = row
                .iter()
                .map(|value| value.estimated_size().saturating_add(16))
                .sum::<usize>();
            if row_bytes > config.max_result_frame_bytes {
                return Err(result_too_large());
            }
            if !batch.is_empty()
                && batch_bytes.saturating_add(row_bytes) > config.max_result_frame_bytes
            {
                send_frame(
                    output,
                    ExecutionFrame::Rows(std::mem::take(&mut batch)),
                    cancel,
                    deadline,
                )?;
                batch_bytes = 0;
            }
            batch_bytes = batch_bytes.saturating_add(row_bytes);
            batch.push(row);
            if batch.len() >= config.row_batch_size {
                send_frame(
                    output,
                    ExecutionFrame::Rows(std::mem::take(&mut batch)),
                    cancel,
                    deadline,
                )?;
                batch_bytes = 0;
            }
            Ok(())
        },
    );
    record_stage("host_sql_stream", engine_started);
    let (affected, is_readonly) = result?;
    if !batch.is_empty() {
        send_frame(output, ExecutionFrame::Rows(batch), cancel, deadline)?;
    }
    let autocommit = connection.is_autocommit();
    if require_autocommit && !autocommit {
        return Err(PlatformError::invalid_argument("显式事务需要会话"));
    }
    let last_insert_rowid = connection.last_insert_rowid();
    // 只读、无状态且自动提交的语句不会创建新的 WAL 目录项。
    // 会话事务与写语句仍同步目录，保留提交时的文件名持久性保证。
    if !is_readonly || !require_autocommit || !autocommit {
        let sync_started = stage_metrics_enabled().then(Instant::now);
        let result = sync_dir(adapter.db_path().parent().expect("数据库文件有父目录"));
        record_stage("host_directory_sync", sync_started);
        result?;
    }
    send_frame(
        output,
        ExecutionFrame::End {
            is_autocommit: autocommit,
            last_insert_rowid,
            affected_rows: affected,
            durability: Durability::Local,
        },
        cancel,
        deadline,
    )?;
    Ok(())
}

fn expire_sessions(
    sessions: &mut HashMap<Uuid, Session>,
    owner: &mut Option<Uuid>,
    config: &LocalHostConfig,
) {
    let now = Instant::now();
    sessions.retain(|id, s| {
        let expired = now.duration_since(s.last_used) >= config.session_idle_timeout
            || s.transaction_started
                .is_some_and(|start| now.duration_since(start) >= config.transaction_timeout);
        if expired {
            if s.transaction_started.is_some() {
                let _ = s.connection.rollback();
            }
            if *owner == Some(*id) {
                *owner = None;
            }
        }
        !expired
    });
}

fn validate_sql(sql: &str) -> Result<()> {
    let tokens = sql_tokens(sql);
    for (index, token) in tokens.iter().enumerate() {
        if token == "ATTACH" || token == "DETACH" {
            return Err(PlatformError::invalid_argument(
                "本地持久模式不允许附加数据库",
            ));
        }
        if token == "PRAGMA" {
            let name = if tokens.get(index + 2).is_some_and(|part| part == ".") {
                tokens.get(index + 3)
            } else {
                tokens.get(index + 1)
            };
            // 只允许已知的纯读取 schema introspection；未知 PRAGMA 按写入处理。
            if !name.is_some_and(|name| {
                matches!(
                    name.as_str(),
                    "TABLE_INFO"
                        | "TABLE_XINFO"
                        | "INDEX_LIST"
                        | "INDEX_INFO"
                        | "INDEX_XINFO"
                        | "FOREIGN_KEY_LIST"
                        | "DATABASE_LIST"
                        | "COMPILE_OPTIONS"
                        | "USER_VERSION"
                        | "APPLICATION_ID"
                )
            }) {
                return Err(PlatformError::invalid_argument(
                    "本地持久模式不允许此 PRAGMA",
                ));
            }
        }
    }
    Ok(())
}

fn sql_tokens(sql: &str) -> Vec<String> {
    let bytes = sql.as_bytes();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i].is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if bytes[i] == b'-' && bytes.get(i + 1) == Some(&b'-') {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if bytes[i] == b'/' && bytes.get(i + 1) == Some(&b'*') {
            i += 2;
            while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                i += 1;
            }
            i = (i + 2).min(bytes.len());
            continue;
        }
        if bytes[i] == b'\'' {
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    i += 1;
                    if bytes.get(i) != Some(&b'\'') {
                        break;
                    }
                }
                i += 1;
            }
            tokens.push("<STRING>".into());
            continue;
        }
        if matches!(bytes[i], b'"' | b'`' | b'[') {
            let close = if bytes[i] == b'[' { b']' } else { bytes[i] };
            i += 1;
            let start = i;
            while i < bytes.len() && bytes[i] != close {
                i += 1;
            }
            tokens.push(String::from_utf8_lossy(&bytes[start..i]).to_ascii_uppercase());
            i = (i + 1).min(bytes.len());
            continue;
        }
        if bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_' {
            let start = i;
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                i += 1;
            }
            tokens.push(String::from_utf8_lossy(&bytes[start..i]).to_ascii_uppercase());
            continue;
        }
        tokens.push((bytes[i] as char).to_string());
        i += 1;
    }
    tokens
}

fn snapshot_files(source: &Path, destination: &Path) -> Result<()> {
    fs::create_dir_all(destination).map_err(io_error)?;
    for entry in fs::read_dir(source).map_err(io_error)? {
        let entry = entry.map_err(io_error)?;
        if entry.file_type().map_err(io_error)?.is_file() {
            fs::copy(entry.path(), destination.join(entry.file_name())).map_err(io_error)?;
            fs::File::open(destination.join(entry.file_name()))
                .and_then(|f| f.sync_all())
                .map_err(io_error)?;
        }
    }
    sync_dir(destination)
}

fn sync_dir(path: &Path) -> Result<()> {
    fs::File::open(path)
        .and_then(|f| f.sync_all())
        .map_err(io_error)
}

fn send_frame(
    output: &mpsc::Sender<Result<ExecutionFrame>>,
    frame: ExecutionFrame,
    cancel: &AtomicBool,
    deadline: Instant,
) -> Result<()> {
    let mut frame = frame;
    let mut backpressure_started = None;
    loop {
        if cancel.load(Ordering::Relaxed) || output.is_closed() {
            record_stage("host_result_backpressure", backpressure_started);
            return Err(PlatformError::session_lost("响应已关闭"));
        }
        if Instant::now() >= deadline {
            record_stage("host_result_backpressure", backpressure_started);
            return Err(timeout_error());
        }
        match output.try_send(Ok(frame)) {
            Ok(()) => {
                record_stage("host_result_backpressure", backpressure_started);
                return Ok(());
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                record_stage("host_result_backpressure", backpressure_started);
                return Err(PlatformError::session_lost("响应已关闭"));
            }
            Err(mpsc::error::TrySendError::Full(Ok(unsent))) => {
                if backpressure_started.is_none() && stage_metrics_enabled() {
                    backpressure_started = Some(Instant::now());
                }
                frame = unsent;
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(mpsc::error::TrySendError::Full(Err(_))) => unreachable!(),
        }
    }
}
fn io_error(error: std::io::Error) -> PlatformError {
    PlatformError::new(ErrorCode::StorageUnavailable, error.to_string())
}
fn timeout_error() -> PlatformError {
    PlatformError::new(ErrorCode::TransactionMaxLifetimeExceeded, "请求超时")
}
fn result_too_large() -> PlatformError {
    PlatformError::new(ErrorCode::ResultTooLarge, "结果帧超过本地内存上限")
}
fn worker_lost() -> PlatformError {
    PlatformError::unavailable("数据库执行线程已停止")
}
fn reject(command: Command, error: PlatformError) {
    match command {
        Command::Execute { output, .. } => {
            let _ = output.try_send(Err(error));
        }
        Command::Describe { answer, .. } => {
            let _ = answer.send(Err(error));
        }
        Command::OpenSession { answer } => {
            let _ = answer.send(Err(error));
        }
        Command::CloseSession { answer, .. }
        | Command::Snapshot { answer, .. }
        | Command::Stop { answer } => {
            let _ = answer.send(Err(error));
        }
        Command::HasSession { answer, .. } => {
            let _ = answer.send(false);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn collect(stream: &mut ExecutionStream) -> Result<Vec<ExecutionFrame>> {
        let mut frames = Vec::new();
        while let Some(frame) = stream.frames.recv().await {
            frames.push(frame?);
        }
        Ok(frames)
    }

    #[tokio::test]
    async fn committed_rows_survive_reopen_and_rollback_does_not() {
        let dir = tempfile::tempdir().unwrap();
        let db = DatabaseId::new_v7();
        let host = LocalHost::new(dir.path().to_path_buf(), LocalHostConfig::default()).unwrap();
        collect(
            &mut host
                .execute(
                    db,
                    None,
                    "CREATE TABLE t(v INTEGER)".into(),
                    vec![],
                    Duration::from_secs(5),
                )
                .await
                .unwrap(),
        )
        .await
        .unwrap();
        collect(
            &mut host
                .execute(
                    db,
                    None,
                    "INSERT INTO t VALUES (1)".into(),
                    vec![],
                    Duration::from_secs(5),
                )
                .await
                .unwrap(),
        )
        .await
        .unwrap();
        let session = host.open_session(db).await.unwrap();
        collect(
            &mut host
                .execute(
                    db,
                    Some(session),
                    "BEGIN".into(),
                    vec![],
                    Duration::from_secs(5),
                )
                .await
                .unwrap(),
        )
        .await
        .unwrap();
        collect(
            &mut host
                .execute(
                    db,
                    Some(session),
                    "INSERT INTO t VALUES (2)".into(),
                    vec![],
                    Duration::from_secs(5),
                )
                .await
                .unwrap(),
        )
        .await
        .unwrap();
        collect(
            &mut host
                .execute(
                    db,
                    Some(session),
                    "COMMIT".into(),
                    vec![],
                    Duration::from_secs(5),
                )
                .await
                .unwrap(),
        )
        .await
        .unwrap();
        collect(
            &mut host
                .execute(
                    db,
                    Some(session),
                    "BEGIN".into(),
                    vec![],
                    Duration::from_secs(5),
                )
                .await
                .unwrap(),
        )
        .await
        .unwrap();
        collect(
            &mut host
                .execute(
                    db,
                    Some(session),
                    "INSERT INTO t VALUES (3)".into(),
                    vec![],
                    Duration::from_secs(5),
                )
                .await
                .unwrap(),
        )
        .await
        .unwrap();
        host.close_session(db, session).await.unwrap();
        host.shutdown().await.unwrap();
        drop(host);
        let reopened =
            LocalHost::new(dir.path().to_path_buf(), LocalHostConfig::default()).unwrap();
        let frames = collect(
            &mut reopened
                .execute(
                    db,
                    None,
                    "SELECT v FROM t ORDER BY v".into(),
                    vec![],
                    Duration::from_secs(5),
                )
                .await
                .unwrap(),
        )
        .await
        .unwrap();
        let values: Vec<_> = frames
            .iter()
            .filter_map(|frame| match frame {
                ExecutionFrame::Rows(rows) => Some(rows.clone()),
                _ => None,
            })
            .flatten()
            .collect();
        assert_eq!(
            values,
            vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(2)]]
        );
        reopened.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn blocks_durability_pragmas_and_attach() {
        let dir = tempfile::tempdir().unwrap();
        let db = DatabaseId::new_v7();
        let host = LocalHost::new(dir.path().to_path_buf(), LocalHostConfig::default()).unwrap();
        for sql in [
            "PRAGMA synchronous=OFF",
            "PRAGMA main.synchronous=OFF",
            "PRAGMA main./* bypass */synchronous=OFF",
            "PRAGMA /* a */ main /* b */ . /* c */ \"synchronous\"=OFF",
            "PRAGMA journal_mode=DELETE",
            "PRAGMA data_sync_retry=OFF",
            "ATTACH DATABASE 'x' AS y",
        ] {
            let mut stream = host
                .execute(db, None, sql.into(), vec![], Duration::from_secs(5))
                .await
                .unwrap();
            assert!(collect(&mut stream).await.is_err(), "{sql}");
        }
        host.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn transaction_expires_and_waiting_writer_proceeds_without_new_requests() {
        let dir = tempfile::tempdir().unwrap();
        let db = DatabaseId::new_v7();
        let config = LocalHostConfig {
            transaction_timeout: Duration::from_millis(100),
            ..Default::default()
        };
        let host = LocalHost::new(dir.path().to_path_buf(), config).unwrap();
        let mut create = host
            .execute(
                db,
                None,
                "CREATE TABLE t(v INTEGER)".into(),
                vec![],
                Duration::from_secs(2),
            )
            .await
            .unwrap();
        collect(&mut create).await.unwrap();
        let owner = host.open_session(db).await.unwrap();
        let mut begin = host
            .execute(
                db,
                Some(owner),
                "BEGIN".into(),
                vec![],
                Duration::from_secs(2),
            )
            .await
            .unwrap();
        collect(&mut begin).await.unwrap();
        let mut waiting = host
            .execute(
                db,
                None,
                "INSERT INTO t VALUES (7)".into(),
                vec![],
                Duration::from_secs(2),
            )
            .await
            .unwrap();
        let frames = tokio::time::timeout(Duration::from_secs(1), collect(&mut waiting))
            .await
            .unwrap()
            .unwrap();
        assert!(frames
            .iter()
            .any(|frame| matches!(frame, ExecutionFrame::End { .. })));
        assert!(!host.has_session(db, owner).await);
        host.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn unread_stream_does_not_pin_worker_after_deadline() {
        let dir = tempfile::tempdir().unwrap();
        let db = DatabaseId::new_v7();
        let config = LocalHostConfig {
            row_batch_size: 1,
            ..Default::default()
        };
        let host = LocalHost::new(dir.path().to_path_buf(), config).unwrap();
        let mut create = host
            .execute(
                db,
                None,
                "CREATE TABLE t(v INTEGER)".into(),
                vec![],
                Duration::from_secs(2),
            )
            .await
            .unwrap();
        collect(&mut create).await.unwrap();
        let mut insert = host.execute(db, None, "WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<100) INSERT INTO t SELECT x FROM n".into(), vec![], Duration::from_secs(2)).await.unwrap();
        collect(&mut insert).await.unwrap();
        let _unread = host
            .execute(
                db,
                None,
                "SELECT v FROM t".into(),
                vec![],
                Duration::from_millis(100),
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), host.close_db(db))
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn snapshot_restore_replaces_files_and_invalidates_sessions() {
        let dir = tempfile::tempdir().unwrap();
        let db = DatabaseId::new_v7();
        let host = LocalHost::new(dir.path().to_path_buf(), LocalHostConfig::default()).unwrap();
        for sql in ["CREATE TABLE t(v INTEGER)", "INSERT INTO t VALUES (1)"] {
            let mut stream = host
                .execute(db, None, sql.into(), vec![], Duration::from_secs(2))
                .await
                .unwrap();
            collect(&mut stream).await.unwrap();
        }
        let session = host.open_session(db).await.unwrap();
        let staged = dir.path().join("snapshot-staged");
        host.snapshot(db, staged.clone()).await.unwrap();
        let mut second = host
            .execute(
                db,
                None,
                "INSERT INTO t VALUES (2)".into(),
                vec![],
                Duration::from_secs(2),
            )
            .await
            .unwrap();
        collect(&mut second).await.unwrap();
        host.restore(db, staged).await.unwrap();
        assert!(!host.has_session(db, session).await);
        let mut read = host
            .execute(
                db,
                None,
                "SELECT v FROM t ORDER BY v".into(),
                vec![],
                Duration::from_secs(2),
            )
            .await
            .unwrap();
        let frames = collect(&mut read).await.unwrap();
        let values: Vec<_> = frames
            .into_iter()
            .filter_map(|frame| match frame {
                ExecutionFrame::Rows(rows) => Some(rows),
                _ => None,
            })
            .flatten()
            .collect();
        assert_eq!(values, vec![vec![SqlValue::Integer(1)]]);
        host.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn waiting_request_deadline_does_not_end_owner_transaction() {
        let dir = tempfile::tempdir().unwrap();
        let db = DatabaseId::new_v7();
        let host = LocalHost::new(dir.path().to_path_buf(), LocalHostConfig::default()).unwrap();
        let mut create = host
            .execute(
                db,
                None,
                "CREATE TABLE t(v INTEGER)".into(),
                vec![],
                Duration::from_secs(2),
            )
            .await
            .unwrap();
        collect(&mut create).await.unwrap();
        let owner = host.open_session(db).await.unwrap();
        let mut begin = host
            .execute(
                db,
                Some(owner),
                "BEGIN".into(),
                vec![],
                Duration::from_secs(2),
            )
            .await
            .unwrap();
        collect(&mut begin).await.unwrap();
        let mut waiting = host
            .execute(
                db,
                None,
                "INSERT INTO t VALUES (9)".into(),
                vec![],
                Duration::from_millis(50),
            )
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(1), collect(&mut waiting))
                .await
                .unwrap()
                .is_err()
        );
        assert!(host.has_session(db, owner).await);
        let mut rollback = host
            .execute(
                db,
                Some(owner),
                "ROLLBACK".into(),
                vec![],
                Duration::from_secs(2),
            )
            .await
            .unwrap();
        collect(&mut rollback).await.unwrap();
        host.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn cancelled_begin_cannot_leave_unowned_transaction() {
        let dir = tempfile::tempdir().unwrap();
        let db = DatabaseId::new_v7();
        let host = LocalHost::new(dir.path().to_path_buf(), LocalHostConfig::default()).unwrap();
        let mut create = host
            .execute(
                db,
                None,
                "CREATE TABLE t(v INTEGER)".into(),
                vec![],
                Duration::from_secs(2),
            )
            .await
            .unwrap();
        collect(&mut create).await.unwrap();
        let owner = host.open_session(db).await.unwrap();
        let mut begin = host
            .execute(
                db,
                Some(owner),
                "BEGIN".into(),
                vec![],
                Duration::from_secs(2),
            )
            .await
            .unwrap();
        let _ = begin.frames.recv().await;
        begin.cancel();
        drop(begin);
        let mut write = host
            .execute(
                db,
                None,
                "INSERT INTO t VALUES (5)".into(),
                vec![],
                Duration::from_secs(2),
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), collect(&mut write))
            .await
            .unwrap()
            .unwrap();
        host.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn close_and_open_race_never_reopens_during_close() {
        let dir = tempfile::tempdir().unwrap();
        let db = DatabaseId::new_v7();
        let host = LocalHost::new(dir.path().to_path_buf(), LocalHostConfig::default()).unwrap();
        for _ in 0..20 {
            let mut read = host
                .execute(db, None, "SELECT 1".into(), vec![], Duration::from_secs(2))
                .await
                .unwrap();
            collect(&mut read).await.unwrap();
            let closing = host.clone();
            let opening = host.clone();
            let (close, attempt) =
                tokio::join!(async move { closing.close_db(db).await }, async move {
                    opening
                        .execute(db, None, "SELECT 1".into(), vec![], Duration::from_secs(2))
                        .await
                });
            close.unwrap();
            if let Ok(mut stream) = attempt {
                let _ = collect(&mut stream).await;
            }
        }
        let mut final_read = host
            .execute(db, None, "SELECT 1".into(), vec![], Duration::from_secs(2))
            .await
            .unwrap();
        collect(&mut final_read).await.unwrap();
        host.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn failed_open_can_retry_after_filesystem_is_repaired() {
        let dir = tempfile::tempdir().unwrap();
        let db = DatabaseId::new_v7();
        let db_dir = dir.path().join("databases").join(db.to_string());
        std::fs::create_dir_all(db_dir.join("main.db")).unwrap();
        let host = LocalHost::new(dir.path().to_path_buf(), LocalHostConfig::default()).unwrap();
        assert!(host.open_session(db).await.is_err());
        std::fs::remove_dir(db_dir.join("main.db")).unwrap();
        assert!(host.open_session(db).await.is_ok());
        host.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn stateless_begin_has_no_success_trailer_and_large_row_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let db = DatabaseId::new_v7();
        let config = LocalHostConfig {
            max_result_frame_bytes: 64,
            ..Default::default()
        };
        let host = LocalHost::new(dir.path().to_path_buf(), config).unwrap();
        let mut begin = host
            .execute(db, None, "BEGIN".into(), vec![], Duration::from_secs(2))
            .await
            .unwrap();
        let mut frames = Vec::new();
        while let Some(frame) = begin.frames.recv().await {
            frames.push(frame);
        }
        assert!(frames.iter().any(Result::is_err));
        assert!(!frames
            .iter()
            .any(|frame| matches!(frame, Ok(ExecutionFrame::End { .. }))));
        let mut oversized = host
            .execute(
                db,
                None,
                "SELECT ?1".into(),
                vec![SqlValue::Text("x".repeat(1024))],
                Duration::from_secs(2),
            )
            .await
            .unwrap();
        assert!(collect(&mut oversized).await.is_err());
        host.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn restore_retries_both_directory_rename_crash_windows() {
        let dir = tempfile::tempdir().unwrap();
        let db = DatabaseId::new_v7();
        let host = LocalHost::new(dir.path().to_path_buf(), LocalHostConfig::default()).unwrap();
        for sql in ["CREATE TABLE t(v INTEGER)", "INSERT INTO t VALUES (8)"] {
            let mut stream = host
                .execute(db, None, sql.into(), vec![], Duration::from_secs(2))
                .await
                .unwrap();
            collect(&mut stream).await.unwrap();
        }
        let target = dir.path().join("databases").join(db.to_string());
        let backup = dir
            .path()
            .join("databases")
            .join(format!(".{db}.restore-backup"));
        let stage_one = dir.path().join("stage-one");
        host.snapshot(db, stage_one.clone()).await.unwrap();
        host.close_db(db).await.unwrap();
        std::fs::rename(&target, &backup).unwrap();
        assert!(host.open_session(db).await.is_err());
        assert!(
            !target.exists(),
            "lazy open created an empty DB during restore recovery"
        );
        host.restore(db, stage_one).await.unwrap();
        let stage_two = dir.path().join("stage-two");
        host.snapshot(db, stage_two.clone()).await.unwrap();
        host.close_db(db).await.unwrap();
        std::fs::rename(&target, &backup).unwrap();
        std::fs::rename(&stage_two, &target).unwrap();
        std::fs::create_dir(&stage_two).unwrap();
        for entry in std::fs::read_dir(&target).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_file() {
                std::fs::copy(entry.path(), stage_two.join(entry.file_name())).unwrap();
            }
        }
        assert!(host.open_session(db).await.is_err());
        host.restore(db, stage_two).await.unwrap();
        let mut read = host
            .execute(
                db,
                None,
                "SELECT v FROM t".into(),
                vec![],
                Duration::from_secs(2),
            )
            .await
            .unwrap();
        let frames = collect(&mut read).await.unwrap();
        assert!(frames.iter().any(|frame| matches!(frame, ExecutionFrame::Rows(rows) if rows == &vec![vec![SqlValue::Integer(8)]])));
        host.shutdown().await.unwrap();
    }
}
