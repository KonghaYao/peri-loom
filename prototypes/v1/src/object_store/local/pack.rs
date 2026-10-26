//! Bounded immutable packs retain the existing per-key filename/CAS interface.
//! This is a physical layout, not a new key/value index or a Worker WAL.
mod cache;
mod codec;
mod maintenance;
mod staging;
#[cfg(test)]
mod tests;

use super::*;
use std::sync::atomic::AtomicU64;

pub(super) fn prepare_replacement(store: &LocalObjectStore, path: &Path) -> Result<()> {
    maintenance::prepare_replacement(store, path)
}

pub(super) fn stage_group(
    store: &LocalObjectStore,
    writes: &[super::super::GroupWrite],
    phase: &mut GroupPhase,
) -> Result<Vec<StagedBody>> {
    staging::stage_group(store, writes, phase)
}

const MARKER: &str = ".peri-local-object-format";
const FORMAT: &[u8] = b"pack-v1\n";
const STAGING: &str = ".pack-staging";
const MAX_ENTRIES: usize = 32;
const MAX_PAYLOAD: usize = 256 * 1024;
const MAX_SMALL_OBJECT: usize = 32 * 1024;
const MAX_DIRECTORY: usize = 64 * 1024;

#[derive(Default)]
pub(super) struct Counters {
    pub(super) body_syncs: AtomicU64,
    pub(super) directory_syncs: AtomicU64,
    pack_files: AtomicU64,
    pack_entries: AtomicU64,
    read_directory_bytes: AtomicU64,
    read_payload_bytes: AtomicU64,
    compactions: AtomicU64,
    compaction_skipped_corrupt: AtomicU64,
    compacted_bytes: AtomicU64,
    orphan_names_removed: AtomicU64,
    directory_cache: cache::DirectoryCache,
}

impl Counters {
    pub(super) fn snapshot(&self, packed: bool) -> serde_json::Value {
        let count = |value: &AtomicU64| value.load(Ordering::Relaxed);
        serde_json::json!({
            "format": if packed { "pack-v1" } else { "legacy-json" },
            "body_sync_calls": count(&self.body_syncs),
            "directory_sync_calls": count(&self.directory_syncs),
            "pack_files_created": count(&self.pack_files),
            "pack_entries_created": count(&self.pack_entries),
            "pack_directory_bytes_read": count(&self.read_directory_bytes),
            "pack_payload_bytes_read": count(&self.read_payload_bytes),
            "compactions": count(&self.compactions),
            "compaction_skipped_corrupt": count(&self.compaction_skipped_corrupt),
            "compacted_payload_bytes": count(&self.compacted_bytes),
            "orphan_names_removed": count(&self.orphan_names_removed),
            "max_pack_entries": MAX_ENTRIES,
            "max_pack_payload_bytes": MAX_PAYLOAD,
            "max_pack_directory_bytes": MAX_DIRECTORY,
            "directory_cache": self.directory_cache.snapshot()
        })
    }
}

/// The caller holds the actual store lock. Existing volumes are never silently
/// migrated: new-format admission is only legal before the first object exists.
pub(super) fn open(store: &LocalObjectStore, requested: bool) -> Result<bool> {
    let marker = store.root.join(MARKER);
    reject_symlink(&marker)?;
    match fs::read(&marker) {
        Ok(bytes) => {
            ensure!(bytes == FORMAT, "unsupported local object store format");
            ensure!(
                cfg!(unix),
                "packed local objects require a verified Unix filesystem"
            );
            prepare_staging(store)?;
            maintenance::cleanup_staging_locked(store)?;
            return Ok(true);
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    if !requested {
        return Ok(false);
    }
    ensure!(
        cfg!(unix),
        "packed local objects require a verified Unix filesystem"
    );
    ensure!(
        !fs::read_dir(&store.root)?.any(|entry| {
            entry.map_or(true, |entry| {
                entry.path().extension().is_some_and(|ext| ext == "obj")
            })
        }),
        "packed local objects require a new empty store; offline migration is required"
    );
    prepare_staging(store)?;
    maintenance::cleanup_staging_locked(store)?;
    // Exercise the actual volume, not a host-OS assumption. A failed probe leaves
    // the format marker absent and cannot admit a partially capable volume.
    let probe = store.root.join(STAGING).join("probe");
    let alias = store.root.join(STAGING).join("probe-link");
    let renamed = store.root.join(STAGING).join("probe-renamed");
    let result = (|| -> Result<()> {
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&probe)?;
        file.sync_all()?;
        fs::hard_link(&probe, &alias)?;
        fs::rename(&alias, &renamed)?;
        flush_directory(&store.root.join(STAGING))?;
        fs::remove_file(&renamed)?;
        fs::remove_file(&probe)?;
        flush_directory(&store.root.join(STAGING))?;
        Ok(())
    })();
    for path in [&probe, &alias, &renamed] {
        match fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    result.context("local object pack filesystem capability probe failed")?;
    let temporary = store.root.join(STAGING).join("format.tmp");
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)?;
    file.write_all(FORMAT)?;
    file.sync_all()?;
    fs::rename(&temporary, &marker)?;
    store.flush_objects_directory()?;
    publish_barrier(&file)?;
    Ok(true)
}

fn prepare_staging(store: &LocalObjectStore) -> Result<()> {
    let path = store.root.join(STAGING);
    match fs::symlink_metadata(&path) {
        Ok(metadata) => ensure!(
            metadata.is_dir() && !metadata.file_type().is_symlink(),
            "local pack staging path must be a real directory"
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    fs::create_dir_all(&path)?;
    ensure!(path.is_dir(), "local pack staging path is not a directory");
    store.flush_objects_directory()?;
    Ok(())
}

/// A handle opened before another process admitted the empty volume to pack-v1
/// must not continue publishing the previous layout after that transition.
pub(super) fn validate_layout(store: &LocalObjectStore) -> Result<()> {
    match fs::read(store.root.join(MARKER)) {
        Ok(bytes) => ensure!(
            store.packed && bytes == FORMAT,
            "local object format changed; reopen the store"
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            ensure!(!store.packed, "packed local object format marker missing");
        }
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

pub(super) fn read(
    store: &LocalObjectStore,
    path: &Path,
    key: &str,
) -> Result<Option<StoredObject>> {
    let Some(mut reader) = codec::Reader::open_cached(store, path)? else {
        return Ok(None);
    };
    let value = reader.read_key(key)?;
    if reader.directory.is_some() {
        store
            .physical
            .read_directory_bytes
            .fetch_add(reader.directory_bytes as u64, Ordering::Relaxed);
        store
            .physical
            .read_payload_bytes
            .fetch_add(value.bytes.len() as u64, Ordering::Relaxed);
    }
    Ok(Some(value))
}

pub(super) fn key_for_alias(store: &LocalObjectStore, path: &Path) -> Result<String> {
    let mut reader =
        codec::Reader::open_cached(store, path)?.context("local object disappeared during list")?;
    let key = reader.key_for_alias(path)?;
    // Directory checks alone must not cause list to bless a corrupt payload.
    let value = reader.read_key(&key)?;
    if reader.directory.is_some() {
        store
            .physical
            .read_directory_bytes
            .fetch_add(reader.directory_bytes as u64, Ordering::Relaxed);
        store
            .physical
            .read_payload_bytes
            .fetch_add(value.bytes.len() as u64, Ordering::Relaxed);
    }
    Ok(key)
}

pub(super) fn put(
    store: &LocalObjectStore,
    key: &str,
    bytes: &[u8],
    condition: PutCondition,
) -> Result<String> {
    validate_key(key)?;
    maintenance::cleanup_staging(store)?;
    let write = super::super::GroupWrite::new(key, bytes.to_vec(), condition);
    let mut phase = GroupPhase::start(std::slice::from_ref(&write));
    // Single put(Any) retains its overwrite contract. Group Any means immutable
    // adoption; these two operations must not accidentally share that rule.
    let body = staging::stage_single(store, &write, None, &mut phase)?;
    let result = (|| {
        let _guard = store.lock()?;
        validate_layout(store)?;
        if !matches!(write.condition, PutCondition::Any) {
            check_condition(store.read(key)?.as_ref(), &write.condition)?;
        }
        maintenance::prepare_replacement(store, &body.target)?;
        fs::rename(
            body.temporary.as_ref().context("staged body missing")?,
            &body.target,
        )?;
        store.flush_objects_directory()?;
        publish_barrier(body.file.as_ref().context("staged file missing")?)?;
        Ok(body.etag.clone())
    })();
    if result.is_err() {
        remove_temporaries(std::slice::from_ref(&body));
    }
    phase.emit();
    result
}

#[cfg(test)]
thread_local! {
    static FAULT: std::cell::Cell<Option<&'static str>> = const { std::cell::Cell::new(None) };
}

pub(super) fn fault(point: &'static str) -> Result<()> {
    #[cfg(test)]
    if FAULT.with(|fault| fault.get() == Some(point)) {
        anyhow::bail!("injected pack publication failure: {point}");
    }
    let _ = point;
    Ok(())
}
