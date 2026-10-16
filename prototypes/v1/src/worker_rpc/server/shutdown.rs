use std::thread::JoinHandle;

use anyhow::{Error, Result};
use serde_json::Value;

/// Reports the retirement counters that still prove work outstanding, or `None`
/// for a sample that proves none. An unavailable sample is unproven rather than
/// clean, and a missing counter is treated the same way.
pub(super) fn retirement_unproven(health: &Value) -> Option<String> {
    if health["available"] != true {
        return Some("retirement sample unavailable".to_owned());
    }
    let mut outstanding = Vec::new();
    for key in [
        "quarantined_databases",
        "retiring_databases",
        "pending_statements",
        "pending_io",
        "retained_engines",
        "retained_connections",
    ] {
        let Some(count) = health[key].as_u64() else {
            return Some(format!("retirement counter unavailable: {key}"));
        };
        if count != 0 {
            outstanding.push(format!("{key}={count}"));
        }
    }
    (!outstanding.is_empty()).then(|| outstanding.join(","))
}

/// Names the counters an operator needs when a shutdown is unclean, keeping the
/// numeric fields ahead of the free-text reason.
pub(super) fn unclean_log_line(reason: &str, health: &Value) -> String {
    format!(
        "level=error event=worker_shutdown outcome=unclean quarantined_databases={} retiring_databases={} pending_statements={} pending_io={} retained_engines={} retained_connections={} reason={reason}",
        health["quarantined_databases"],
        health["retiring_databases"],
        health["pending_statements"],
        health["pending_io"],
        health["retained_engines"],
        health["retained_connections"],
    )
}

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
        // Every stage contributes to one unclean summary: an operator needs the
        // session ledger, the run loop and the watchdog together rather than
        // whichever failed first, and no stage may skip the ones after it.
        let clear = clear();
        let mut failure = self.failure;
        for (label, later) in [("session cleanup", clear.err()), ("run loop", result.err())] {
            let Some(later) = later else { continue };
            failure = Some(match failure {
                Some(previous) => previous.context(format!("{label}: {later:#}")),
                None => later.context(label),
            });
        }
        match failure {
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
    fn cleanup_and_run_failures_are_reported_alongside_thread_panics() {
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
            let reported = format!("{error:#}");
            assert!(
                reported.contains("worker RPC thread panicked"),
                "{reported}"
            );
            assert!(reported.contains("run loop failed"), "{reported}");
            assert_eq!(
                reported.contains("session clear failed"),
                clear_fails,
                "{reported}"
            );
        }
    }

    #[test]
    fn every_unclean_stage_is_reported_together_instead_of_the_first_one() {
        let error = WorkerThreads::default()
            .finish(
                thread::spawn(|| Err(anyhow::anyhow!("watchdog sweep failed"))),
                Err(anyhow::anyhow!("run loop failed")),
                || Err(anyhow::anyhow!("2 session cleanups failed")),
            )
            .unwrap_err();
        let reported = format!("{error:#}");
        for expected in [
            "watchdog sweep failed",
            "run loop failed",
            "2 session cleanups failed",
        ] {
            assert!(reported.contains(expected), "{reported}");
        }
    }

    #[test]
    fn retirement_counters_decide_whether_an_outstanding_work_claim_holds() {
        let clean = serde_json::json!({"available": true, "quarantined_databases": 0, "retiring_databases": 0,
            "pending_statements": 0, "pending_io": 0, "retained_engines": 0, "retained_connections": 0});
        assert_eq!(super::retirement_unproven(&clean), None);
        let quarantined = serde_json::json!({"available": true, "quarantined_databases": 2, "retiring_databases": 1,
            "pending_statements": 0, "pending_io": 0, "retained_engines": 3, "retained_connections": 0});
        assert_eq!(
            super::retirement_unproven(&quarantined).unwrap(),
            "quarantined_databases=2,retiring_databases=1,retained_engines=3"
        );
        // An unavailable sample is unproven, never a release.
        let unavailable = serde_json::json!({"available": false, "quarantined_databases": null});
        assert!(super::retirement_unproven(&unavailable).is_some());
        let truncated = serde_json::json!({"available": true, "quarantined_databases": 0});
        assert!(super::retirement_unproven(&truncated).is_some());
    }

    #[test]
    fn unclean_log_line_keeps_the_counters_ahead_of_the_reason() {
        let health = serde_json::json!({"available": true, "quarantined_databases": 2, "retiring_databases": null,
            "pending_statements": 1, "pending_io": 0, "retained_engines": 3, "retained_connections": 0});
        let line = super::unclean_log_line("1 session cleanups failed", &health);
        assert!(line.starts_with("level=error event=worker_shutdown outcome=unclean"));
        assert!(line.contains("quarantined_databases=2"));
        assert!(line.contains("retained_engines=3"));
        assert!(line.contains("pending_statements=1"));
        assert!(line.ends_with("reason=1 session cleanups failed"));
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
