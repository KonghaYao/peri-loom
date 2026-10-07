use std::thread::JoinHandle;

use anyhow::{Error, Result};

#[derive(Default)]
pub(super) struct WorkerThreads {
    pending: Vec<JoinHandle<()>>,
    failure: Option<Error>,
}

impl WorkerThreads {
    pub fn push(&mut self, worker: JoinHandle<()>) {
        self.pending.push(worker);
    }

    pub fn reap_finished(&mut self) -> bool {
        let mut index = 0;
        while index < self.pending.len() {
            if self.pending[index].is_finished() {
                let worker = self.pending.swap_remove(index);
                self.join(worker, "worker RPC thread panicked");
            } else {
                index += 1;
            }
        }
        self.failure.is_some()
    }

    fn join(&mut self, worker: JoinHandle<()>, message: &'static str) {
        if worker.join().is_err() && self.failure.is_none() {
            self.failure = Some(anyhow::anyhow!(message));
        }
    }

    pub fn finish(
        mut self,
        watchdog: JoinHandle<Result<()>>,
        result: Result<()>,
        clear: impl FnOnce() -> Result<()>,
    ) -> Result<()> {
        while let Some(worker) = self.pending.pop() {
            self.join(worker, "worker RPC thread panicked");
        }
        match watchdog.join() {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                self.failure.get_or_insert(error);
            }
            Err(_) => {
                self.failure
                    .get_or_insert_with(|| anyhow::anyhow!("worker watchdog thread panicked"));
            }
        }
        clear()?;
        result?;
        match self.failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{mpsc, Arc};
    use std::thread;
    use std::time::{Duration, Instant};

    use anyhow::Result;

    use super::WorkerThreads;

    fn wait_finished(worker: &thread::JoinHandle<()>) {
        let deadline = Instant::now() + Duration::from_secs(2);
        while !worker.is_finished() {
            assert!(Instant::now() < deadline, "fixture thread failed to finish");
            thread::yield_now();
        }
    }

    #[test]
    fn finished_worker_panic_is_joined_and_reported_after_cleanup() {
        let worker = thread::spawn(|| panic!("synthetic worker panic"));
        wait_finished(&worker);
        let mut workers = WorkerThreads::default();
        workers.push(worker);
        assert!(workers.reap_finished());
        assert!(workers.pending.is_empty());
        let clears = AtomicUsize::new(0);
        let error = workers
            .finish(thread::spawn(|| Ok(())), Ok(()), || {
                clears.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .unwrap_err();
        assert_eq!(clears.load(Ordering::SeqCst), 1);
        assert_eq!(error.to_string(), "worker RPC thread panicked");
    }

    #[test]
    fn reaping_finished_threads_keeps_live_handles_for_final_join() -> Result<()> {
        let (release, released) = mpsc::channel();
        let live = thread::spawn(move || released.recv().unwrap());
        let finished = thread::spawn(|| {});
        wait_finished(&finished);
        let mut workers = WorkerThreads::default();
        workers.push(live);
        workers.push(finished);
        assert!(!workers.reap_finished());
        assert_eq!(workers.pending.len(), 1);
        release.send(())?;
        workers.finish(thread::spawn(|| Ok(())), Ok(()), || Ok(()))
    }

    #[test]
    fn remaining_workers_and_watchdog_all_finish_before_session_clear() -> Result<()> {
        let completed = Arc::new(AtomicUsize::new(0));
        let mut workers = WorkerThreads::default();
        for _ in 0..2 {
            let completed = completed.clone();
            workers.push(thread::spawn(move || {
                completed.fetch_add(1, Ordering::SeqCst);
            }));
        }
        let watchdog_completed = completed.clone();
        let watchdog = thread::spawn(move || {
            watchdog_completed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        });
        workers.finish(watchdog, Ok(()), || {
            assert_eq!(completed.load(Ordering::SeqCst), 3);
            Ok(())
        })
    }

    #[test]
    fn worker_and_watchdog_panics_do_not_skip_other_joins_or_session_clear() {
        let completed = Arc::new(AtomicUsize::new(0));
        let mut workers = WorkerThreads::default();
        let normal_completed = completed.clone();
        workers.push(thread::spawn(move || {
            normal_completed.fetch_add(1, Ordering::SeqCst);
        }));
        workers.push(thread::spawn(|| panic!("synthetic worker panic")));
        let watchdog = thread::spawn(|| panic!("synthetic watchdog panic"));
        let error = workers
            .finish(watchdog, Ok(()), || {
                assert_eq!(completed.load(Ordering::SeqCst), 1);
                completed.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .unwrap_err();
        assert_eq!(completed.load(Ordering::SeqCst), 2);
        assert_eq!(error.to_string(), "worker RPC thread panicked");
    }

    #[test]
    fn watchdog_panic_is_propagated_after_session_clear() {
        let clears = AtomicUsize::new(0);
        let error = WorkerThreads::default()
            .finish(
                thread::spawn(|| panic!("synthetic watchdog panic")),
                Ok(()),
                || {
                    clears.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                },
            )
            .unwrap_err();
        assert_eq!(clears.load(Ordering::SeqCst), 1);
        assert_eq!(error.to_string(), "worker watchdog thread panicked");
    }

    #[test]
    fn original_clear_then_run_error_priority_is_preserved_over_thread_panics() {
        for clear_fails in [false, true] {
            let mut workers = WorkerThreads::default();
            workers.push(thread::spawn(|| panic!("synthetic worker panic")));
            let clears = AtomicUsize::new(0);
            let error = workers
                .finish(
                    thread::spawn(|| Ok(())),
                    Err(anyhow::anyhow!("run loop failed")),
                    || {
                        clears.fetch_add(1, Ordering::SeqCst);
                        if clear_fails {
                            Err(anyhow::anyhow!("session clear failed"))
                        } else {
                            Ok(())
                        }
                    },
                )
                .unwrap_err();
            assert_eq!(clears.load(Ordering::SeqCst), 1);
            assert_eq!(
                error.to_string(),
                if clear_fails {
                    "session clear failed"
                } else {
                    "run loop failed"
                }
            );
        }
    }

    #[test]
    fn watchdog_unsettled_error_stays_typed_after_all_joins_and_clear() {
        let clears = AtomicUsize::new(0);
        let watchdog = thread::spawn(|| {
            Err(
                anyhow::Error::new(crate::session::AdmissionError::Quarantine)
                    .context(crate::session::UnsettledSqlOutcome),
            )
        });
        let error = WorkerThreads::default()
            .finish(watchdog, Ok(()), || {
                clears.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .unwrap_err();
        assert_eq!(clears.load(Ordering::SeqCst), 1);
        assert!(error
            .downcast_ref::<crate::session::UnsettledSqlOutcome>()
            .is_some());
    }
}
