use super::*;
use std::sync::Arc;

#[test]
fn cached_directory_never_resurrects_an_unlinked_alias() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = LocalObjectStore::open_packed(directory.path())?;
    let writes: Vec<_> = (0..4).map(|index| page(index, 256)).collect();
    store.put_group(&writes)?;
    for _ in 0..3 {
        store.get(&writes[0].key)?.unwrap();
    }
    assert!(
        store.local_storage_metrics()["directory_cache"]["hits"]
            .as_u64()
            .unwrap()
            > 0
    );
    store.delete(&writes[0].key, None)?;
    assert!(store.get(&writes[0].key)?.is_none());
    assert!(!store.list("")?.contains(&writes[0].key));
    assert_eq!(store.get(&writes[1].key)?.unwrap().bytes, writes[1].bytes);
    Ok(())
}

#[test]
fn cached_directory_observes_replacement_by_an_independent_store_handle() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let first = LocalObjectStore::open_packed(directory.path())?;
    let key = "tenant/db/authority/root";
    let old = first.put(key, b"old value", PutCondition::Absent)?;
    for _ in 0..3 {
        assert_eq!(first.get(key)?.unwrap().etag, old);
    }
    let second = LocalObjectStore::open(directory.path())?;
    let new = second.put(key, b"new value", PutCondition::Match(old))?;
    assert_eq!(first.get(key)?.unwrap().etag, new);
    assert_eq!(first.get(key)?.unwrap().bytes, b"new value");
    Ok(())
}

#[test]
fn cached_directory_is_invalidated_when_a_pack_is_corrupted() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = LocalObjectStore::open_packed(directory.path())?;
    let key = "tenant/db/authority/root";
    store.put(key, b"payload", PutCondition::Absent)?;
    for _ in 0..3 {
        store.get(key)?.unwrap();
    }
    let mut file = OpenOptions::new().write(true).open(store.path(key))?;
    file.seek(SeekFrom::Start(codec::HEADER_LEN as u64))?;
    file.write_all(b"!")?;
    file.sync_all()?;
    assert!(store.get(key).is_err());
    Ok(())
}

#[test]
fn same_inode_with_replaced_header_cannot_reuse_an_old_directory() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = LocalObjectStore::open_packed(directory.path())?;
    let key = "tenant/db/authority/root";
    store.put(key, b"old value", PutCondition::Absent)?;
    store.get(key)?.unwrap();
    let mut file = OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(store.path(key))?;
    codec::write(&mut file, &[(key, b"new value", "replacement-etag")])?;
    file.sync_all()?;
    assert_eq!(store.get(key)?.unwrap().etag, "replacement-etag");
    assert_eq!(store.get(key)?.unwrap().bytes, b"new value");
    Ok(())
}

#[test]
fn cached_pack_survives_compaction_without_reviving_removed_keys() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let first = LocalObjectStore::open_packed(directory.path())?;
    let writes: Vec<_> = (0..8).map(|index| page(index, 256)).collect();
    let etags = first.put_group(&writes)?;
    first.get(&writes[7].key)?.unwrap();
    let second = LocalObjectStore::open(directory.path())?;
    for write in &writes[..7] {
        second.delete(&write.key, None)?;
    }
    assert!(counter(&second, "compactions") > 0);
    assert_eq!(first.get(&writes[7].key)?.unwrap().etag, etags[7]);
    assert_eq!(first.list("")?, vec![writes[7].key.clone()]);
    Ok(())
}

#[test]
fn directory_cache_bounds_count_and_bytes_and_declines_oversized_values() -> Result<()> {
    use super::super::cache::{DirectoryCache, Key};
    let directory = tempfile::tempdir()?;
    let file = File::create(directory.path().join("identity"))?;
    let metadata = file.metadata()?;
    let cache = DirectoryCache::default();
    let make = |key_len: usize| {
        Arc::new(codec::Directory {
            version: 1,
            entries: vec![codec::Entry {
                key: "k".repeat(key_len),
                etag: "e".into(),
                checksum: "0".repeat(64),
                offset: 0,
                length: 0,
            }],
        })
    };
    for index in 0..200_u32 {
        let mut header = [0; codec::HEADER_LEN];
        header[..4].copy_from_slice(&index.to_le_bytes());
        cache.insert(Key::new(&metadata, &header), make(32))?;
    }
    assert_eq!(cache.snapshot()["entries"], 128);
    for index in 200..400_u32 {
        let mut header = [0; codec::HEADER_LEN];
        header[..4].copy_from_slice(&index.to_le_bytes());
        cache.insert(Key::new(&metadata, &header), make(32 * 1024))?;
    }
    assert!(cache.snapshot()["estimated_bytes"].as_u64().unwrap() <= 2 * 1024 * 1024);
    assert!(cache.snapshot()["entries"].as_u64().unwrap() < 128);
    let before = cache.snapshot()["entries"].clone();
    cache.insert(
        Key::new(&metadata, &[255; codec::HEADER_LEN]),
        make(3 * 1024 * 1024),
    )?;
    assert_eq!(cache.snapshot()["entries"], before);
    Ok(())
}

#[test]
fn preexisting_legacy_handle_cannot_write_after_empty_store_format_admission() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let legacy = LocalObjectStore::open(directory.path())?;
    let packed = LocalObjectStore::open_packed(directory.path())?;
    assert!(legacy
        .put("tenant/db/authority/root", b"legacy", PutCondition::Absent)
        .is_err());
    assert!(legacy
        .put_group(&group(0, 2, b"legacy", PutCondition::Absent))
        .is_err());
    assert_eq!(packed.list("")?, Vec::<String>::new());
    Ok(())
}
