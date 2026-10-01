pub mod api;
/// 装配层：`main` 只负责 CLI，启动顺序与依赖构造都在这里，便于集成测试复用。
pub mod app;
pub mod auth;
pub mod background;
pub mod clients;
pub mod config;
pub mod error;
pub mod idempotency;
pub mod ingress;
pub mod middleware;
pub mod ndjson;
pub mod router;
pub mod state;
