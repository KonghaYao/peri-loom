//! 公共 API 契约测试：只用 crate 外部可见的路径调用，保证其他 crate
//! （db-server / db-worker）能按 README 描述的方式使用 objectstore。

use bytes::Bytes;
use domain::error::ErrorCode;
use objectstore::snapshot::{
    compute_sha256, download_snapshot, object_prefix, upload_snapshot, verify_snapshot,
    SnapshotManifest, MANIFEST_SUFFIX, ZSTD_LEVEL,
};
use objectstore::{InMemoryObjectStore, ObjectMeta, ObjectStore, S3Config, StorageError};

#[tokio::test]
async fn object_store_trait_is_usable_as_dyn() {
    let store = InMemoryObjectStore::new();
    // 平台代码以 &dyn ObjectStore 传递实现（不依赖具体厂商）。
    let dyn_store: &dyn ObjectStore = &store;

    dyn_store
        .put("snapshots/db-1/s1/a.db.zst", Bytes::from_static(b"payload"))
        .await
        .unwrap();

    let meta: ObjectMeta = dyn_store
        .head("snapshots/db-1/s1/a.db.zst")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(meta.size, 7);
    assert!(meta.etag.is_some());
    assert!(meta.last_modified.is_some());

    let data = dyn_store.get("snapshots/db-1/s1/a.db.zst").await.unwrap();
    assert_eq!(data, Bytes::from_static(b"payload"));

    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("a.db.zst");
    assert_eq!(
        dyn_store
            .get_to_file("snapshots/db-1/s1/a.db.zst", &out)
            .await
            .unwrap(),
        7
    );
    assert_eq!(tokio::fs::read(&out).await.unwrap(), b"payload");

    // 大文件接口返回写入字节数。
    let big = dir.path().join("big.bin");
    tokio::fs::write(&big, vec![1u8; 9 * 1024 * 1024])
        .await
        .unwrap();
    assert_eq!(
        dyn_store.put_file("k/big", &big).await.unwrap(),
        9 * 1024 * 1024
    );

    let listed = dyn_store.list("snapshots/db-1/s1/").await.unwrap();
    assert_eq!(listed.len(), 1);

    dyn_store
        .delete("snapshots/db-1/s1/a.db.zst")
        .await
        .unwrap();
    let err = dyn_store
        .get("snapshots/db-1/s1/a.db.zst")
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::SnapshotUnavailable);
}

#[tokio::test]
async fn snapshot_flow_through_public_api() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("page1.db");
    let content = b"turso page data".to_vec();
    tokio::fs::write(&source, &content).await.unwrap();

    let store: &dyn ObjectStore = &InMemoryObjectStore::new();
    let sources = vec![("data/page1.db".to_string(), source)];
    let manifest = upload_snapshot(
        store,
        "db-1",
        "snap-1",
        8192,
        3,
        "turso-v0.8.1",
        1,
        &sources,
    )
    .await
    .unwrap();

    assert_eq!(object_prefix("db-1", "snap-1"), "snapshots/db-1/snap-1/");
    assert!(manifest.manifest_key().ends_with(MANIFEST_SUFFIX));
    assert_eq!(manifest.files[0].checksum, compute_sha256(&content));
    assert_eq!(ZSTD_LEVEL, 3);

    // manifest 可序列化后跨进程传递（catalog 就存这份 JSON）。
    let json = manifest.to_json_bytes().unwrap();
    let restored_manifest = SnapshotManifest::from_json_bytes(&json).unwrap();
    assert_eq!(restored_manifest, manifest);

    verify_snapshot(store, &manifest).await.unwrap();

    let dest = dir.path().join("restore");
    download_snapshot(store, &manifest, &dest).await.unwrap();
    assert_eq!(
        tokio::fs::read(dest.join("data/page1.db")).await.unwrap(),
        content
    );
}

#[test]
fn storage_error_exposes_proto_codes() {
    // 库 crate 错误统一映射到 domain 的 ErrorCode（proto 契约）。
    let missing = StorageError::NotFound { key: "k".into() };
    assert_eq!(missing.code(), ErrorCode::SnapshotUnavailable);
    assert_eq!(
        StorageError::Unavailable("net".into()).code(),
        ErrorCode::StorageUnavailable
    );
    assert_eq!(
        StorageError::ChecksumMismatch {
            key: "k".into(),
            expected: "a".into(),
            actual: "b".into(),
        }
        .code(),
        ErrorCode::ChecksumMismatch
    );

    // S3Config 可用于本地 RustFS：显式 endpoint + 凭证（Debug 已脱敏）。
    let config = S3Config {
        endpoint: Some("http://127.0.0.1:9000".into()),
        region: "us-east-1".into(),
        bucket: "peri-loom-snapshots".into(),
        access_key_id: Some("minioadmin".into()),
        secret_access_key: Some("minioadmin".into()),
        force_path_style: true,
    };
    assert!(!format!("{config:?}").contains("minioadmin"));
}
