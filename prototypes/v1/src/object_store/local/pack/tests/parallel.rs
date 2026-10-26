use super::*;
use std::sync::{Condvar, Mutex};

struct Rendezvous {
    arrived: Mutex<usize>,
    ready: Condvar,
    participants: usize,
}

impl Rendezvous {
    fn new(participants: usize) -> Self {
        Self {
            arrived: Mutex::new(0),
            ready: Condvar::new(),
            participants,
        }
    }

    fn enter(&self) -> Result<()> {
        let mut arrived = self.arrived.lock().unwrap();
        *arrived += 1;
        self.ready.notify_all();
        let (arrived, _) = self
            .ready
            .wait_timeout_while(arrived, Duration::from_secs(5), |count| {
                *count < self.participants
            })
            .unwrap();
        ensure!(
            *arrived == self.participants,
            "staging workers did not overlap"
        );
        Ok(())
    }
}

fn stage_test_job(store: &LocalObjectStore, index: usize) -> Result<staging::StagedBatch> {
    let key = format!("tenant/db/authority/index-pages/parallel-{index}");
    let etag = fresh_etag(&[index as u8; 64]);
    let bodies = staging::stage_pack(
        store,
        &[(&key, &[index as u8; 64], &etag)],
        &mut GroupPhase::default(),
        false,
    )?;
    Ok(staging::StagedBatch {
        bodies: bodies.into_iter().map(|body| (index, body)).collect(),
    })
}

#[test]
fn workers_overlap_and_staged_batch_drop_cleans_every_job() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = LocalObjectStore::open_packed(directory.path())?;
    let rendezvous = Rendezvous::new(4);
    let (batches, _) = staging::run_jobs(8, 4, false, |index| {
        let batch = stage_test_job(&store, index)?;
        if index < 4 {
            rendezvous.enter()?;
        }
        Ok(batch)
    })?;
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.bodies.len())
            .sum::<usize>(),
        8
    );
    assert_eq!(fs::read_dir(store.root.join(STAGING))?.count(), 8);
    drop(batches);
    assert_empty_staging(&store);
    assert!(store.list("")?.is_empty());
    assert!(staging::run_jobs(8, 5, false, |_| unreachable!()).is_err());
    Ok(())
}

#[test]
fn worker_error_or_panic_joins_and_cleans_other_workers() -> Result<()> {
    for panic in [false, true] {
        let directory = tempfile::tempdir()?;
        let store = LocalObjectStore::open_packed(directory.path())?;
        let rendezvous = Rendezvous::new(4);
        let result = staging::run_jobs(8, 4, false, |index| {
            let batch = stage_test_job(&store, index)?;
            if index < 4 {
                rendezvous.enter()?;
            }
            if index == 4 {
                assert!(!panic, "injected parallel stage panic");
                anyhow::bail!("injected parallel stage error");
            }
            Ok(batch)
        });
        assert!(result.is_err());
        assert_empty_staging(&store);
        assert!(store.list("")?.is_empty());
    }
    Ok(())
}

#[test]
fn parallel_mixed_jobs_preserve_order_and_account_each_body_once() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = LocalObjectStore::open_packed(directory.path())?;
    let mut writes: Vec<_> = (0..12).map(|index| page(index, 128)).collect();
    writes.push(GroupWrite::new(
        "tenant/db/authority/blobs/one",
        vec![8; 96],
        PutCondition::Any,
    ));
    writes.push(GroupWrite::new(
        "tenant/db/authority/blobs/two",
        vec![9; 96],
        PutCondition::Any,
    ));
    writes.push(GroupWrite::new(
        "tenant/db/segments/large",
        vec![3; MAX_SMALL_OBJECT + 1],
        PutCondition::Any,
    ));
    writes.push(GroupWrite::new(
        "tenant/db/authority/root",
        b"root".to_vec(),
        PutCondition::Absent,
    ));
    let mut phase = GroupPhase {
        timed: true,
        ..GroupPhase::default()
    };
    let before = counter(&store, "body_sync_calls");
    let bodies = stage_group(&store, &writes, &mut phase)?;
    assert_eq!(phase.workers, 5.min(stage_worker_count(8).min(4)));
    assert_eq!(counter(&store, "body_sync_calls") - before, 5);
    assert_eq!(
        phase.body_fsync_ns,
        bodies.iter().map(|body| body.timing.fsync_ns).sum::<u64>()
    );
    assert_eq!(
        phase.body_write_ns,
        bodies.iter().map(|body| body.timing.write_ns).sum::<u64>()
    );
    let etags = store.publish_staged(&writes, bodies)?;
    for (write, etag) in writes.iter().zip(etags) {
        let stored = store.get(&write.key)?.unwrap();
        assert_eq!(stored.bytes, write.bytes);
        assert_eq!(stored.etag, etag);
    }
    assert_empty_staging(&store);
    Ok(())
}

#[test]
fn parallel_workers_inherit_fault_and_clean_just_linked_aliases() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = LocalObjectStore::open_packed(directory.path())?;
    let writes = group(0, 8, b"root", PutCondition::Absent);
    assert!(with_fault("after_alias_link", || store.put_group(&writes)).is_err());
    assert_empty_staging(&store);
    assert!(store.list("")?.is_empty());
    Ok(())
}
