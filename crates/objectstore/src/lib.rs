//! # objectstore —— S3-compatible 对象存储抽象 + Snapshot / Backup artifact
//!
//! 架构 §17.9：Snapshot / Backup 走 S3-compatible Object Storage
//! （AWS S3 / RustFS / Ceph RGW），**平台代码只依赖 [`ObjectStore`] interface**，
//! 不直接依赖具体厂商 SDK；对象存储不进入普通 SQL read/write hot path。
//!
//! 架构 §11.4：Snapshot 异步进行，artifact 必须携带 `base_lsn` / `owner_epoch` /
//! DB engine version / schema version / checksum manifest，恢复时从 `base_lsn`
//! 继续 replay Remote WAL（§11.2）。
//!
//! ## 模块
//!
//! - [`store`]：[`ObjectStore`] trait 与 [`ObjectMeta`]；
//! - [`s3`]：[`S3ObjectStore`]（aws-sdk-s3）+ [`S3Config`]（env / docker secret）；
//! - [`memory`]：[`InMemoryObjectStore`]，单元测试与无 S3 环境的本地开发使用；
//! - [`snapshot`]：Snapshot manifest 与 upload / download / verify；
//! - [`error`]：[`StorageError`] 到 [`domain::error::ErrorCode`] 的映射。
//!
//! ## 错误码契约（架构 §17.9 + domain 错误码）
//!
//! | 场景 | ErrorCode |
//! | --- | --- |
//! | 对象不存在 | `SNAPSHOT_UNAVAILABLE` |
//! | artifact 损坏 / 校验和（含解压）不一致 | `CHECKSUM_MISMATCH` |
//! | 网络 / 连接 / 5xx / 瞬时 IO 故障 | `STORAGE_UNAVAILABLE` |
//! | 凭证或权限被拒绝 | `PERMISSION_DENIED` |
//! | 调用方参数非法（路径穿越、空 id、未知压缩算法） | `INVALID_ARGUMENT` |

#![forbid(unsafe_code)]

pub mod error;
pub mod memory;
pub mod s3;
pub mod snapshot;
pub mod store;

pub use error::StorageError;
pub use memory::InMemoryObjectStore;
pub use s3::{S3Config, S3ObjectStore};
pub use snapshot::{
    compute_manifest_checksum, compute_sha256, download_snapshot, load_manifest, object_prefix,
    upload_snapshot, verify_snapshot, SnapshotFileEntry, SnapshotManifest, MANIFEST_SUFFIX,
    ZSTD_LEVEL,
};
pub use store::{ObjectMeta, ObjectStore};
