//! Protobuf / gRPC 代码生成入口。
//!
//! 生成的代码同时包含 client 与 server：Server Plane 是 Worker Control/Data 的 client，
//! Worker 同时是 Control 的 client 与 Data 的 server。
//!
//! proto 根目录为仓库根下的 `proto/`，import 路径形如 `platform/common.proto`。

use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
    let proto_root = manifest_dir
        .join("../../proto")
        .canonicalize()
        .expect("proto 目录不存在：期望仓库根下的 proto/");

    let protos = [
        proto_root.join("platform/common.proto"),
        proto_root.join("platform/data.proto"),
        proto_root.join("platform/control.proto"),
        proto_root.join("platform/wal.proto"),
        proto_root.join("platform/runtime_local.proto"),
    ];

    // include 路径与 proto 路径同类型（P = PathBuf），用 slice 借用避免克隆
    let includes: [PathBuf; 1] = [proto_root.clone()];

    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .emit_rerun_if_changed(true)
        .compile_protos(&protos, &includes)?;

    println!("cargo:rerun-if-changed={}", proto_root.display());
    Ok(())
}
