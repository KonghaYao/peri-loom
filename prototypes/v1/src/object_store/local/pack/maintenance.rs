use super::*;

/// A small dedicated staging directory avoids scanning millions of live logical
/// object names during request-side housekeeping. Active attempts keep their
/// inode locked, including all of its temporary hard-link aliases.
pub(super) fn cleanup_staging(store: &LocalObjectStore) -> Result<()> {
    let _guard = store.lock()?;
    cleanup_staging_locked(store)
}

pub(super) fn cleanup_staging_locked(store: &LocalObjectStore) -> Result<()> {
    let directory = store.root.join(STAGING);
    let mut changed = false;
    for entry in fs::read_dir(&directory)?.take(128) {
        let path = entry?.path();
        reject_symlink(&path)?;
        ensure!(path.is_file(), "unexpected directory in pack staging");
        let file = match OpenOptions::new().read(true).write(true).open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        match file.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => continue,
            Err(std::fs::TryLockError::Error(error)) => return Err(error.into()),
        }
        match fs::remove_file(&path) {
            Ok(()) => {
                changed = true;
                store
                    .physical
                    .orphan_names_removed
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    if changed {
        store
            .physical
            .directory_syncs
            .fetch_add(1, Ordering::Relaxed);
        flush_directory(&directory)?;
        publish_directory_barrier(&directory)?;
    }
    Ok(())
}

/// Called with the STORE lock before an alias is removed/replaced. When losing
/// this alias would leave a mostly-dead pack, relocate only its remaining live
/// aliases first. Equal bytes/etags make a crash between alias renames harmless:
/// every surviving name still identifies either the old or new durable bytes.
///
/// No global index/maintenance queue is needed: one pack is at most 32 entries
/// and 256 KiB, and every deletion/replacement visits its own bounded directory.
#[cfg(unix)]
pub(super) fn prepare_replacement(store: &LocalObjectStore, target: &Path) -> Result<()> {
    use std::os::unix::fs::MetadataExt;

    let mut reader = match codec::Reader::open(target) {
        Ok(Some(reader)) => reader,
        Ok(None) => return Ok(()),
        Err(error) if error.is::<codec::CorruptObject>() => {
            store
                .physical
                .compaction_skipped_corrupt
                .fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        Err(error) => return Err(error),
    };
    let Some(directory) = &reader.directory else {
        return Ok(());
    };
    if directory.entries.len() < 2 {
        return Ok(());
    }
    let entries = directory.entries.clone();
    let original = reader.file.metadata()?;
    let mut live = Vec::new();
    let mut live_bytes = 0_u64;
    for entry in &entries {
        let alias = store.path(&entry.key);
        if alias == target {
            continue;
        }
        reject_symlink(&alias)?;
        let metadata = match fs::metadata(&alias) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        if metadata.dev() == original.dev() && metadata.ino() == original.ino() {
            live_bytes += entry.length;
            live.push(entry.clone());
        }
    }
    if live.is_empty()
        || (live.len() * 2 >= entries.len() && live_bytes * 2 >= reader.payload_len as u64)
    {
        return Ok(());
    }
    let objects: Vec<_> = match live
        .iter()
        .map(|entry| reader.read_key(&entry.key))
        .collect::<Result<_>>()
    {
        Ok(objects) => objects,
        // Compaction is a physical optimization, not a new condition on the
        // logical key operation. A corrupt peer stays corrupt and visible; do
        // not rewrite it, resurrect anything, or block removal of a healthy key.
        Err(error) if error.is::<codec::CorruptObject>() => {
            store
                .physical
                .compaction_skipped_corrupt
                .fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        Err(error) => return Err(error),
    };
    let items: Vec<_> = live
        .iter()
        .zip(&objects)
        .map(|(entry, object)| {
            (
                entry.key.as_str(),
                object.bytes.as_slice(),
                object.etag.as_str(),
            )
        })
        .collect();
    let mut phase = GroupPhase::default();
    let bodies = staging::stage_pack(store, &items, &mut phase, true)?;
    let result = (|| -> Result<()> {
        for body in &bodies {
            // The same actual STORE lock spans inspection, copy, and publication.
            // Rechecking the inode also makes the relocation precondition explicit.
            let current = fs::metadata(&body.target)?;
            ensure!(
                current.dev() == original.dev() && current.ino() == original.ino(),
                "pack alias changed during locked compaction"
            );
            fs::rename(
                body.temporary
                    .as_ref()
                    .context("compaction temporary missing")?,
                &body.target,
            )?;
        }
        store.flush_objects_directory()?;
        publish_barrier(bodies[0].file.as_ref().context("compaction file missing")?)?;
        Ok(())
    })();
    if result.is_err() {
        remove_temporaries(&bodies);
    }
    result?;
    store.physical.compactions.fetch_add(1, Ordering::Relaxed);
    store
        .physical
        .compacted_bytes
        .fetch_add(live_bytes, Ordering::Relaxed);
    Ok(())
}

#[cfg(not(unix))]
pub(super) fn prepare_replacement(_store: &LocalObjectStore, _target: &Path) -> Result<()> {
    anyhow::bail!("packed local object compaction requires Unix")
}
