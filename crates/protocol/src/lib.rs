//! 平台内部协议层。
//!
//! 本 crate 是 proto 定义（`proto/platform/*.proto`）的唯一 Rust 绑定来源，
//! 同时承载本地 Unix Domain Socket 的 length-delimited framing 编解码。
//!
//! 协议边界（架构 §15.6，冻结）：
//! - Server -> Worker：gRPC / HTTP2 Streaming
//! - Worker -> DB Process：Unix Domain Socket + length-delimited protobuf（非 gRPC）
//!
//! 注意模块结构：prost 生成的跨包类型引用使用 `super::super::<pkg>::v1::...`
//! 形式的相对路径，因此 Rust 模块层级必须与 proto package 层级严格同构。

/// 平台内部 gRPC 服务定义。
///
/// - [`platform::control`]：Server -> Worker Control Path（Heartbeat / Start / Stop / Move / Drain）
/// - [`platform::data`]：Server -> Worker Data Path（Execute / Streaming / Session / Transaction / Cancel）
/// - [`platform::wal`]：Remote WAL Service（Append / ReadRange / Fence / Trim / Health）
/// - [`platform::runtime`]：Worker Data Dispatcher -> DB Process 本地帧定义
pub mod platform {
    /// 公共类型：错误码、请求上下文、资源预算、生命周期枚举。
    pub mod common {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/platform.common.v1.rs"));
        }
    }

    /// Server -> Worker Control Path。
    pub mod control {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/platform.control.v1.rs"));
        }
    }

    /// Server -> Worker Data Path。
    pub mod data {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/platform.data.v1.rs"));
        }
    }

    /// Remote WAL Service。
    pub mod wal {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/platform.wal.v1.rs"));
        }
    }

    /// 本地 UDS 协议（Dispatcher <-> DB Process）。
    pub mod runtime {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/platform.runtime.v1.rs"));
        }
    }
}

/// 便捷别名：`common::PlatformError` 等价于 `platform::common::v1::PlatformError`。
pub use platform::common::v1 as common;
/// 便捷别名：Control Path 类型与服务。
pub use platform::control::v1 as control;
/// 便捷别名：Data Path 类型与服务。
pub use platform::data::v1 as data;
/// 便捷别名：DB Process 本地帧类型。
pub use platform::runtime::v1 as runtime_local;
/// 便捷别名：Remote WAL 类型与服务。
pub use platform::wal::v1 as wal;

/// 本地 UDS length-delimited framing 编解码。
pub mod framing;

/// 领域类型 <-> proto 类型转换。
pub mod convert;
