use super::*;
use crate::object_store::GroupWrite;
use std::io::{Read, Seek, SeekFrom};
use std::process::Command;
use std::time::{Duration, Instant};

mod cache;
mod parallel;

fn page(index: usize, bytes: usize) -> GroupWrite {
    GroupWrite::new(
        format!("tenant/db/authority/index-pages/page-{index}"),
        vec![(index % 251) as u8; bytes],
        PutCondition::Any,
    )
}

fn group(begin: usize, count: usize, root: &[u8], condition: PutCondition) -> Vec<GroupWrite> {
    let mut writes: Vec<_> = (begin..begin + count)
        .map(|index| page(index, 1024))
        .collect();
    writes.push(GroupWrite::new(
        "tenant/db/authority/root",
        root.to_vec(),
        condition,
    ));
    writes
}

fn counter(store: &LocalObjectStore, name: &str) -> u64 {
    store.local_storage_metrics()[name].as_u64().unwrap()
}

fn assert_empty_staging(store: &LocalObjectStore) {
    assert_eq!(fs::read_dir(store.root.join(STAGING)).unwrap().count(), 0);
}

#[test]
fn grouped_pages_share_bounded_files_and_cut_physical_syncs() -> Result<()> {
    use std::os::unix::fs::MetadataExt;
    let directory = tempfile::tempdir()?;
    let packed = LocalObjectStore::open_packed(directory.path().join("packed"))?;
    let loose = LocalObjectStore::open(directory.path().join("loose"))?;
    let writes = group(0, 28, b"root", PutCondition::Absent);
    let before = counter(&packed, "body_sync_calls");
    let dirs = counter(&packed, "directory_sync_calls");
    let etags = packed.put_group(&writes)?;
    loose.put_group(&writes)?;
    assert_eq!(counter(&packed, "body_sync_calls") - before, 2);
    assert_eq!(counter(&packed, "directory_sync_calls") - dirs, 2);
    assert_eq!(counter(&loose, "body_sync_calls"), 29);
    let inode = fs::metadata(packed.path(&writes[0].key))?.ino();
    for (write, etag) in writes.iter().zip(etags) {
        let object = packed.get(&write.key)?.unwrap();
        assert_eq!(object.bytes, write.bytes);
        assert_eq!(object.etag, etag);
    }
    assert_eq!(fs::metadata(packed.path(&writes[20].key))?.ino(), inode);
    assert_ne!(fs::metadata(packed.path(&writes[28].key))?.ino(), inode);
    assert_empty_staging(&packed);
    let reopened = LocalObjectStore::open(directory.path().join("packed"))?;
    assert_eq!(reopened.local_storage_metrics()["format"], "pack-v1");
    assert_eq!(reopened.list("tenant/db/")?.len(), 29);
    Ok(())
}

#[test]
fn absent_match_and_any_adoption_keep_distinct_contracts() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = LocalObjectStore::open_packed(directory.path())?;
    let write = page(1, 64);
    let etag = store.put_group(std::slice::from_ref(&write))?.remove(0);
    let before = counter(&store, "body_sync_calls");
    assert_eq!(store.put_group(std::slice::from_ref(&write))?[0], etag);
    assert_eq!(counter(&store, "body_sync_calls"), before);
    assert!(store
        .put(&write.key, &write.bytes, PutCondition::Absent)
        .unwrap_err()
        .is::<PreconditionFailed>());
    assert!(store
        .put(
            &write.key,
            &write.bytes,
            PutCondition::Match("stale".into())
        )
        .unwrap_err()
        .is::<PreconditionFailed>());
    let next = store.put(&write.key, &write.bytes, PutCondition::Match(etag.clone()))?;
    assert_ne!(next, etag);
    store.put(&write.key, b"overwrite", PutCondition::Any)?;
    assert_eq!(store.get(&write.key)?.unwrap().bytes, b"overwrite");
    assert_empty_staging(&store);
    Ok(())
}

#[test]
fn duplicate_key_groups_keep_order_and_reject_conflicting_immutable_bytes() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = LocalObjectStore::open_packed(directory.path())?;
    let write = page(1, 64);
    let etags = store.put_group(&[write.clone(), write.clone()])?;
    assert_eq!(etags[0], etags[1]);
    let mut bad = write.clone();
    bad.bytes.push(9);
    assert!(store.put_group(&[write.clone(), bad]).is_err());
    let current = store.get(&write.key)?.unwrap();
    let replacements = [
        GroupWrite::new(
            &write.key,
            b"first".to_vec(),
            PutCondition::Match(current.etag.clone()),
        ),
        GroupWrite::new(
            &write.key,
            b"last".to_vec(),
            PutCondition::Match(current.etag),
        ),
    ];
    let etags = store.put_group(&replacements)?;
    assert_eq!(store.get(&write.key)?.unwrap().bytes, b"last");
    assert_eq!(store.get(&write.key)?.unwrap().etag, etags[1]);
    Ok(())
}

#[test]
fn stale_cas_rejects_every_new_alias_after_concurrent_staging() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = LocalObjectStore::open_packed(directory.path())?;
    let old = store.put("tenant/db/authority/root", b"old", PutCondition::Absent)?;
    let writes = group(0, 4, b"loser", PutCondition::Match(old.clone()));
    let staged = stage_group(&store, &writes, &mut GroupPhase::default())?;
    let winner = store.put(
        "tenant/db/authority/root",
        b"winner",
        PutCondition::Match(old),
    )?;
    assert!(store
        .publish_staged(&writes, staged)
        .unwrap_err()
        .is::<PreconditionFailed>());
    assert_eq!(store.get("tenant/db/authority/root")?.unwrap().etag, winner);
    assert_eq!(
        store.list("tenant/db/authority/index-pages/")?,
        Vec::<String>::new()
    );
    assert_empty_staging(&store);
    Ok(())
}

#[test]
fn deleting_a_keeps_b_and_never_resurrects_dead_pack_entries() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = LocalObjectStore::open_packed(directory.path())?;
    let writes: Vec<_> = (0..8).map(|index| page(index, 2048)).collect();
    let etags = store.put_group(&writes)?;
    for index in 0..7 {
        store.delete(&writes[index].key, Some(&etags[index]))?;
        assert!(store.get(&writes[index].key)?.is_none());
        assert!(!store.list("tenant/db/")?.contains(&writes[index].key));
    }
    assert!(counter(&store, "compactions") > 0);
    assert_eq!(store.get(&writes[7].key)?.unwrap().etag, etags[7]);
    let reopened = LocalObjectStore::open(directory.path())?;
    assert_eq!(reopened.list("tenant/db/")?, vec![writes[7].key.clone()]);
    assert_eq!(
        reopened.get(&writes[7].key)?.unwrap().bytes,
        writes[7].bytes
    );
    assert_empty_staging(&store);
    Ok(())
}

#[test]
fn overwriting_packed_aliases_reclaims_half_live_packs_without_changing_peers() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = LocalObjectStore::open_packed(directory.path())?;
    let writes: Vec<_> = (0..8).map(|index| page(index, 1024)).collect();
    let etags = store.put_group(&writes)?;
    for write in writes.iter().take(7) {
        store.put(&write.key, b"new value", PutCondition::Any)?;
    }
    assert!(counter(&store, "compactions") > 0);
    assert_eq!(store.get(&writes[7].key)?.unwrap().etag, etags[7]);
    for write in &writes[..7] {
        assert_eq!(store.get(&write.key)?.unwrap().bytes, b"new value");
    }
    assert_eq!(store.list("tenant/db/")?.len(), 8);
    Ok(())
}

#[test]
fn unconditional_put_repairs_corrupt_legacy_and_pack_objects() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = LocalObjectStore::open_packed(directory.path())?;
    let legacy_key = "tenant/db/segments/corrupt-legacy";
    store.put(
        legacy_key,
        &vec![7; MAX_SMALL_OBJECT + 1],
        PutCondition::Absent,
    )?;
    // A truncated JSON value is a data-format error, including serde's EOF case.
    fs::write(store.path(legacy_key), b"{\"key\":")?;
    assert!(store
        .get(legacy_key)
        .unwrap_err()
        .is::<codec::CorruptObject>());
    let repaired_etag = store.put(legacy_key, b"repaired legacy", PutCondition::Any)?;
    assert_eq!(store.get(legacy_key)?.unwrap().etag, repaired_etag);

    let pack_key = "tenant/db/authority/index-pages/corrupt-directory";
    store.put(pack_key, b"old pack", PutCondition::Absent)?;
    let mut file = OpenOptions::new().write(true).open(store.path(pack_key))?;
    file.seek(SeekFrom::Start(codec::HEADER_LEN as u64))?;
    file.write_all(b"!")?;
    assert!(store
        .get(pack_key)
        .unwrap_err()
        .is::<codec::CorruptObject>());
    store.put(pack_key, b"repaired pack", PutCondition::Any)?;
    assert_eq!(store.get(pack_key)?.unwrap().bytes, b"repaired pack");
    assert_eq!(counter(&store, "compaction_skipped_corrupt"), 2);
    assert_empty_staging(&store);
    Ok(())
}

#[test]
fn corrupt_peer_does_not_block_healthy_alias_delete_or_overwrite() -> Result<()> {
    use std::os::unix::fs::MetadataExt;
    for delete in [true, false] {
        let directory = tempfile::tempdir()?;
        let store = LocalObjectStore::open_packed(directory.path())?;
        let writes = [page(0, 1024), page(1, 64), page(2, 64)];
        let etags = store.put_group(&writes)?;
        let retained_inode = fs::metadata(store.path(&writes[2].key))?.ino();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(store.path(&writes[1].key))?;
        let mut header = [0; codec::HEADER_LEN];
        file.read_exact(&mut header)?;
        let directory_len = u32::from_le_bytes(header[8..12].try_into()?) as u64;
        file.seek(SeekFrom::Start(
            codec::HEADER_LEN as u64 + directory_len + 1024,
        ))?;
        file.write_all(&[99])?;
        assert!(store.get(&writes[1].key).is_err());
        if delete {
            store.delete(&writes[0].key, Some(&etags[0]))?;
            assert!(store.get(&writes[0].key)?.is_none());
        } else {
            store.put(&writes[0].key, b"replacement", PutCondition::Any)?;
            assert_eq!(store.get(&writes[0].key)?.unwrap().bytes, b"replacement");
        }
        let retained = store.get(&writes[2].key)?.unwrap();
        assert_eq!(retained.bytes, writes[2].bytes);
        assert_eq!(retained.etag, etags[2]);
        assert_eq!(
            fs::metadata(store.path(&writes[2].key))?.ino(),
            retained_inode
        );
        assert!(store.get(&writes[1].key).is_err());
        assert_eq!(counter(&store, "compaction_skipped_corrupt"), 1);
        assert_eq!(counter(&store, "compactions"), 0);
        assert_empty_staging(&store);
    }
    Ok(())
}

#[test]
fn optional_compaction_does_not_hide_nonregular_file_errors() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = LocalObjectStore::open_packed(directory.path())?;
    let key = "tenant/db/authority/index-pages/nonregular";
    fs::create_dir(store.path(key))?;
    let error = store.put(key, b"new", PutCondition::Any).unwrap_err();
    assert!(!error.is::<codec::CorruptObject>());
    assert_eq!(counter(&store, "compaction_skipped_corrupt"), 0);
    assert!(store.path(key).is_dir());
    assert_empty_staging(&store);
    Ok(())
}

#[test]
fn large_objects_and_lifetime_families_do_not_share_page_packs() -> Result<()> {
    use std::os::unix::fs::MetadataExt;
    let directory = tempfile::tempdir()?;
    let store = LocalObjectStore::open_packed(directory.path())?;
    let mut writes: Vec<_> = (0..40).map(|index| page(index, 16 * 1024)).collect();
    writes.push(GroupWrite::new(
        "tenant/db/wal/receipt-pages/retained",
        vec![1; 100],
        PutCondition::Any,
    ));
    writes.push(GroupWrite::new(
        "tenant/db/authority/blobs/opaque",
        vec![2; 100],
        PutCondition::Any,
    ));
    writes.push(GroupWrite::new(
        "tenant/db/segments/large",
        vec![3; MAX_SMALL_OBJECT + 1],
        PutCondition::Any,
    ));
    store.put_group(&writes)?;
    assert_ne!(
        fs::metadata(store.path(&writes[0].key))?.ino(),
        fs::metadata(store.path(&writes[40].key))?.ino()
    );
    for write in &writes {
        let mut reader = codec::Reader::open(&store.path(&write.key))?.unwrap();
        if let Some(directory) = &reader.directory {
            assert!(directory.entries.len() <= MAX_ENTRIES);
            assert!(reader.payload_len <= MAX_PAYLOAD);
        }
        assert_eq!(reader.read_key(&write.key)?.bytes, write.bytes);
    }
    assert_eq!(store.list("tenant/db/")?.len(), writes.len());
    Ok(())
}

#[test]
fn cutoff_and_large_legacy_reads_use_bounded_bulk_io_calls() -> Result<()> {
    struct CountingRead {
        file: File,
        calls: usize,
        bytes: usize,
    }
    impl Read for CountingRead {
        fn read(&mut self, destination: &mut [u8]) -> std::io::Result<usize> {
            self.calls += 1;
            let read = self.file.read(destination)?;
            self.bytes += read;
            Ok(read)
        }
    }
    let directory = tempfile::tempdir()?;
    let store = LocalObjectStore::open_packed(directory.path())?;
    for length in [
        MAX_SMALL_OBJECT - 1,
        MAX_SMALL_OBJECT,
        MAX_SMALL_OBJECT + 1,
        MAX_PAYLOAD + 1,
    ] {
        let key = format!("tenant/db/authority/size-boundary-{length}");
        let bytes = vec![37; length];
        let etag = store.put(&key, &bytes, PutCondition::Absent)?;
        let path = store.path(&key);
        let reader = codec::Reader::open(&path)?.unwrap();
        assert_eq!(reader.directory.is_some(), length <= MAX_SMALL_OBJECT);
        let value = store.get(&key)?.unwrap();
        assert_eq!(value.bytes, bytes);
        assert_eq!(value.etag, etag);
        if length > MAX_SMALL_OBJECT {
            let mut reader = CountingRead {
                file: File::open(&path)?,
                calls: 0,
                bytes: 0,
            };
            let value = codec::read_legacy(&mut reader)?;
            assert_eq!(value.bytes, bytes);
            assert_eq!(value.etag, etag);
            assert_eq!(reader.bytes as u64, fs::metadata(&path)?.len());
            // Count actual raw-File reads, not elapsed time: this catches
            // serde_json's unbuffered per-byte IoRead even on a fast filesystem.
            assert!(
                reader.calls <= 32,
                "{length}-byte payload used {} reads",
                reader.calls
            );
            assert!(reader.bytes / reader.calls > 1024);
        }
    }
    Ok(())
}

#[test]
fn point_reads_fetch_one_payload_not_the_entire_pack() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = LocalObjectStore::open_packed(directory.path())?;
    let writes: Vec<_> = (0..16).map(|index| page(index, 8192)).collect();
    store.put_group(&writes)?;
    let before = counter(&store, "pack_payload_bytes_read");
    store.get(&writes[3].key)?.unwrap();
    assert_eq!(counter(&store, "pack_payload_bytes_read") - before, 8192);
    Ok(())
}

#[test]
fn format_admission_rejects_existing_legacy_data_unknown_versions_and_symlinks() -> Result<()> {
    use std::os::unix::fs::symlink;
    let directory = tempfile::tempdir()?;
    let legacy = LocalObjectStore::open(directory.path().join("legacy"))?;
    legacy.put("tenant/db/root", b"data", PutCondition::Absent)?;
    assert!(LocalObjectStore::open_packed(directory.path().join("legacy")).is_err());
    assert!(!legacy.root.join(MARKER).exists());
    let unknown = directory.path().join("unknown");
    fs::create_dir(&unknown)?;
    fs::write(unknown.join(MARKER), b"pack-v999\n")?;
    assert!(LocalObjectStore::open(&unknown).is_err());
    let linked = directory.path().join("linked");
    fs::create_dir(&linked)?;
    symlink(directory.path(), linked.join(STAGING))?;
    assert!(LocalObjectStore::open_packed(&linked).is_err());
    Ok(())
}

#[test]
fn orphan_cleanup_respects_active_staging_locks() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = LocalObjectStore::open_packed(directory.path())?;
    let writes: Vec<_> = (0..4).map(|index| page(index, 64)).collect();
    let staged = stage_group(&store, &writes, &mut GroupPhase::default())?;
    maintenance::cleanup_staging(&store)?;
    assert_eq!(fs::read_dir(store.root.join(STAGING))?.count(), 4);
    drop(staged); // Model the live attempt disappearing without cleanup.
    maintenance::cleanup_staging(&store)?;
    assert_empty_staging(&store);
    assert_eq!(counter(&store, "orphan_names_removed"), 4);
    Ok(())
}

fn with_fault<T>(name: &'static str, action: impl FnOnce() -> T) -> T {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            FAULT.with(|fault| fault.set(None));
        }
    }
    FAULT.with(|fault| fault.set(Some(name)));
    let _reset = Reset;
    action()
}

#[test]
fn dependency_barrier_failure_does_not_publish_new_root() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = LocalObjectStore::open_packed(directory.path())?;
    let etag = store.put("tenant/db/authority/root", b"old", PutCondition::Absent)?;
    let writes = group(0, 4, b"new", PutCondition::Match(etag.clone()));
    assert!(with_fault("after_dependencies_sync", || store.put_group(&writes)).is_err());
    let reopened = LocalObjectStore::open(directory.path())?;
    assert_eq!(
        reopened.get("tenant/db/authority/root")?.unwrap().etag,
        etag
    );
    // Unreferenced durable bodies are legal on an uncertain publication failure.
    for write in &writes[..4] {
        assert_eq!(reopened.get(&write.key)?.unwrap().bytes, write.bytes);
    }
    assert_empty_staging(&reopened);
    Ok(())
}

#[test]
fn adopted_retry_dependencies_are_synced_before_root_publication() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = LocalObjectStore::open_packed(directory.path())?;
    let etag = store.put("tenant/db/authority/root", b"old", PutCondition::Absent)?;
    let writes = group(0, 4, b"new", PutCondition::Match(etag.clone()));
    // Model the dependencies being visible, but the first directory durability
    // barrier failing. Reopening would itself sync the directory, so retry on
    // this same handle to exercise the otherwise unsafe all-adopt path.
    assert!(with_fault("before_dependencies_sync", || store.put_group(&writes)).is_err());
    for write in &writes[..4] {
        assert_eq!(store.get(&write.key)?.unwrap().bytes, write.bytes);
    }
    let body_syncs = counter(&store, "body_sync_calls");
    let directory_syncs = counter(&store, "directory_sync_calls");
    assert!(with_fault("after_dependencies_sync", || store.put_group(&writes)).is_err());
    // All immutable dependencies were adopted; only the new root needed a body
    // sync, but the dependency directory barrier must still have run first.
    assert_eq!(counter(&store, "body_sync_calls"), body_syncs + 1);
    assert_eq!(counter(&store, "directory_sync_calls"), directory_syncs + 1);
    assert_eq!(store.get("tenant/db/authority/root")?.unwrap().etag, etag);
    store.put_group(&writes)?;
    assert_eq!(counter(&store, "directory_sync_calls"), directory_syncs + 3);
    assert_eq!(
        store.get("tenant/db/authority/root")?.unwrap().bytes,
        b"new"
    );
    assert_empty_staging(&store);
    Ok(())
}

#[test]
fn entirely_adopted_group_retry_completes_directory_durability() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = LocalObjectStore::open_packed(directory.path())?;
    let writes: Vec<_> = (0..4).map(|index| page(index, 128)).collect();
    assert!(with_fault("after_root_rename", || store.put_group(&writes)).is_err());
    let existing: Vec<_> = writes
        .iter()
        .map(|write| store.get(&write.key).unwrap().unwrap().etag)
        .collect();
    let body_syncs = counter(&store, "body_sync_calls");
    let directory_syncs = counter(&store, "directory_sync_calls");
    let barriers = barrier_count();
    assert_eq!(store.put_group(&writes)?, existing);
    assert_eq!(counter(&store, "body_sync_calls"), body_syncs);
    assert_eq!(counter(&store, "directory_sync_calls"), directory_syncs + 1);
    assert_eq!(barrier_count(), barriers + 1);
    let reopened = LocalObjectStore::open(directory.path())?;
    for write in &writes {
        assert_eq!(reopened.get(&write.key)?.unwrap().bytes, write.bytes);
    }
    assert_empty_staging(&store);
    Ok(())
}

#[test]
fn absent_delete_retry_completes_directory_durability() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = LocalObjectStore::open_packed(directory.path())?;
    let key = "tenant/db/authority/index-pages/deleted";
    let etag = store.put(key, b"value", PutCondition::Absent)?;
    assert!(with_fault("after_alias_delete", || store.delete(key, Some(&etag))).is_err());
    assert!(store.get(key)?.is_none());
    let directories = counter(&store, "directory_sync_calls");
    let barriers = barrier_count();
    store.delete(key, Some(&etag))?;
    assert_eq!(counter(&store, "directory_sync_calls"), directories + 1);
    assert_eq!(barrier_count(), barriers + 1);
    assert!(LocalObjectStore::open(directory.path())?
        .get(key)?
        .is_none());
    Ok(())
}

#[test]
fn root_publication_failure_is_uncertain_and_never_loses_referenced_bodies() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = LocalObjectStore::open_packed(directory.path())?;
    let etag = store.put("tenant/db/authority/root", b"old", PutCondition::Absent)?;
    let writes = group(0, 4, b"new", PutCondition::Match(etag));
    let failure = with_fault("after_root_rename", || store.put_group(&writes)).unwrap_err();
    assert!(!failure.is::<PreconditionFailed>());
    let reopened = LocalObjectStore::open(directory.path())?;
    assert_eq!(
        reopened.get("tenant/db/authority/root")?.unwrap().bytes,
        b"new"
    );
    for write in &writes[..4] {
        assert_eq!(reopened.get(&write.key)?.unwrap().bytes, write.bytes);
    }
    assert_empty_staging(&reopened);
    Ok(())
}

#[test]
fn body_sync_failure_leaves_no_final_alias() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = LocalObjectStore::open_packed(directory.path())?;
    assert!(with_fault("before_body_sync", || store.put_group(&group(
        0,
        4,
        b"new",
        PutCondition::Absent
    )))
    .is_err());
    assert_eq!(store.list("")?, Vec::<String>::new());
    assert_empty_staging(&store);
    Ok(())
}

#[test]
fn directory_payload_and_extent_corruption_fail_closed() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = LocalObjectStore::open_packed(directory.path())?;
    let key = "tenant/db/authority/index-pages/corrupt";
    store.put(key, &[7; 1024], PutCondition::Absent)?;
    let path = store.path(key);
    let mut file = OpenOptions::new().read(true).write(true).open(&path)?;
    let mut header = [0; codec::HEADER_LEN];
    file.read_exact(&mut header)?;
    let directory_len = u32::from_le_bytes(header[8..12].try_into()?) as u64;
    file.seek(SeekFrom::Start(
        codec::HEADER_LEN as u64 + directory_len + 17,
    ))?;
    file.write_all(&[99])?;
    assert!(store.get(key).is_err());
    // A corrupt directory is detected before any offset can drive allocation.
    file.seek(SeekFrom::Start(8))?;
    file.write_all(&u32::MAX.to_le_bytes())?;
    assert!(store.get(key).is_err());
    assert!(store.list("").is_err());
    Ok(())
}

#[test]
fn packed_cross_process_cas_child() -> Result<()> {
    let Ok(path) = std::env::var("PERI_PACK_CAS_CHILD") else {
        return Ok(());
    };
    let root = PathBuf::from(path);
    let id = std::env::var("PERI_PACK_CAS_ID")?;
    let expected = std::env::var("PERI_PACK_CAS_ETAG")?;
    let store = LocalObjectStore::open(root.join("objects"))?;
    fs::write(root.join(format!("ready-{id}")), b"ready")?;
    let deadline = Instant::now() + Duration::from_secs(10);
    while !root.join("start").exists() {
        ensure!(Instant::now() < deadline, "CAS child timed out");
        std::thread::sleep(Duration::from_millis(2));
    }
    let writes = group(
        id.parse::<usize>()? * 10,
        2,
        id.as_bytes(),
        PutCondition::Match(expected),
    );
    let outcome = match store.put_group(&writes) {
        Ok(_) => "ok",
        Err(error) if error.is::<PreconditionFailed>() => "rejected",
        Err(error) => return Err(error),
    };
    fs::write(root.join(format!("result-{id}")), outcome)?;
    Ok(())
}

#[test]
fn actual_store_lock_serializes_two_independent_processes() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = LocalObjectStore::open_packed(directory.path().join("objects"))?;
    let etag = store.put("tenant/db/authority/root", b"base", PutCondition::Absent)?;
    let executable = std::env::current_exe()?;
    let name = "object_store::local::pack::tests::packed_cross_process_cas_child";
    let mut children = Vec::new();
    for id in ["1", "2"] {
        children.push(
            Command::new(&executable)
                .args(["--exact", name, "--nocapture"])
                .env("PERI_PACK_CAS_CHILD", directory.path())
                .env("PERI_PACK_CAS_ID", id)
                .env("PERI_PACK_CAS_ETAG", &etag)
                .spawn()?,
        );
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    while !directory.path().join("ready-1").exists() || !directory.path().join("ready-2").exists() {
        if Instant::now() >= deadline {
            for child in &mut children {
                let _ = child.kill();
            }
            anyhow::bail!("CAS subprocess startup timed out");
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    fs::write(directory.path().join("start"), b"go")?;
    for child in &mut children {
        ensure!(child.wait()?.success(), "CAS subprocess failed");
    }
    let outcomes = [
        fs::read_to_string(directory.path().join("result-1"))?,
        fs::read_to_string(directory.path().join("result-2"))?,
    ];
    assert_eq!(
        outcomes.iter().filter(|outcome| *outcome == "ok").count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| *outcome == "rejected")
            .count(),
        1
    );
    let winner = String::from_utf8(store.get("tenant/db/authority/root")?.unwrap().bytes)?;
    for id in ["1", "2"] {
        for offset in 0..2 {
            let key = page(id.parse::<usize>()? * 10 + offset, 1024).key;
            assert_eq!(store.get(&key)?.is_some(), id == winner);
        }
    }
    Ok(())
}

#[test]
#[ignore = "diagnostic local filesystem microbenchmark; not the Hrana acceptance gate"]
fn packed_publication_microbenchmark() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let mut results = Vec::new();
    for packed in [false, true, true, false] {
        let path = directory.path().join(format!("run-{}", results.len()));
        let store = if packed {
            LocalObjectStore::open_packed(path)?
        } else {
            LocalObjectStore::open(path)?
        };
        let mut etag = store.put("tenant/db/authority/root", b"initial", PutCondition::Absent)?;
        let start_metrics = store.local_storage_metrics();
        let mut durations = Vec::new();
        for run in 0..120 {
            let writes = group(run * 24, 24, &run.to_le_bytes(), PutCondition::Match(etag));
            let started = Instant::now();
            let etags = store.put_group(&writes)?;
            let elapsed = started.elapsed().as_nanos() as u64;
            etag = etags.last().unwrap().clone();
            if run >= 20 {
                durations.push(elapsed);
            }
        }
        durations.sort_unstable();
        let read_started = Instant::now();
        for index in 0..3000 {
            store.get(&page(index % (120 * 24), 1024).key)?.unwrap();
        }
        let point_read_ns = read_started.elapsed().as_nanos() as u64 / 3000;
        let end = store.local_storage_metrics();
        results.push(serde_json::json!({
            "packed": packed, "samples": durations.len(), "p50_ns": durations[49],
            "p99_ns": durations[98], "mean_point_read_ns": point_read_ns,
            "body_sync_calls": end["body_sync_calls"].as_u64().unwrap() - start_metrics["body_sync_calls"].as_u64().unwrap(),
            "directory_sync_calls": end["directory_sync_calls"].as_u64().unwrap() - start_metrics["directory_sync_calls"].as_u64().unwrap(),
            "maintenance": end
        }));
    }
    println!("PACK_MICROBENCH {}", serde_json::to_string(&results)?);
    Ok(())
}
