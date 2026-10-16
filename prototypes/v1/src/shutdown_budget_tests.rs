use super::ShutdownBudget;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

fn budget_armed_at(deadline: Instant, complete: bool) -> ShutdownBudget {
    ShutdownBudget {
        duration: Duration::from_secs(5),
        state: Arc::new((Mutex::new(Some((deadline, complete))), Condvar::new())),
    }
}

fn complete(budget: &ShutdownBudget) -> bool {
    budget.state.0.lock().unwrap().unwrap().1
}

#[test]
fn shutdown_deadline_only_shortens_and_success_disarms() -> anyhow::Result<()> {
    let budget = ShutdownBudget {
        duration: Duration::from_secs(5),
        state: Arc::new((Mutex::new(None), Condvar::new())),
    };
    let _returned = budget.cancel_on_return();
    budget.start_with(Duration::from_secs(2));
    let first = budget.state.0.lock().unwrap().unwrap().0;
    budget.start();
    assert_eq!(budget.state.0.lock().unwrap().unwrap().0, first);
    budget.start_with(Duration::from_secs(1));
    assert!(budget.state.0.lock().unwrap().unwrap().0 < first);
    budget.finish();
    Ok(())
}

#[test]
fn only_an_in_budget_return_may_disarm_the_watchdog() {
    // No watchdog thread runs in these cases: the guard's own decision is the
    // assertion, so an expired budget must stay armed for the watchdog to report.
    let expired = budget_armed_at(Instant::now() - Duration::from_millis(1), false);
    drop(expired.cancel_on_return());
    assert!(!complete(&expired), "expired budget was disarmed");

    let live = budget_armed_at(Instant::now() + Duration::from_secs(5), false);
    drop(live.cancel_on_return());
    assert!(complete(&live), "in-budget return left the watchdog armed");
}

#[test]
fn stalled_shutdown_exits_nonzero_with_explicit_unclean_result() -> anyhow::Result<()> {
    if std::env::var_os("PERI_SHUTDOWN_TEST_CHILD").is_some() {
        let budget = ShutdownBudget::configured()?;
        budget.start();
        std::thread::sleep(Duration::from_secs(30));
        panic!("deadline did not terminate stalled work");
    }
    let start = Instant::now();
    let output = child_output(
        "shutdown_budget::tests::stalled_shutdown_exits_nonzero_with_explicit_unclean_result",
        [("PERI_SHUTDOWN_BUDGET_MS", "100")],
    )?;
    assert_eq!(output.status.code(), Some(70));
    assert!(start.elapsed() < Duration::from_secs(5));
    assert!(String::from_utf8_lossy(&output.stderr).contains("outcome=unclean"));
    Ok(())
}

#[test]
fn expired_run_handover_exits_unclean_instead_of_a_generic_failure() -> anyhow::Result<()> {
    // The run() shape: work returns early, `finish` hands over, and the guard drops
    // on the return path. Manufacturing clean evidence anywhere in that path would
    // let the process leave through a generic exit without the unclean line.
    if std::env::var_os("PERI_SHUTDOWN_TEST_CHILD").is_some() {
        let budget = ShutdownBudget::configured()?;
        let returned = budget.cancel_on_return();
        budget.start();
        // A real shutdown can observe the deadline here, between the watchdog
        // waking and its exit, without waiting out the whole budget.
        *budget.state.0.lock().unwrap().as_mut().unwrap() =
            (Instant::now() - Duration::from_millis(1), false);
        budget.finish();
        drop(returned);
        panic!("expired shutdown finished normally");
    }
    let start = Instant::now();
    let output = child_output(
        "shutdown_budget::tests::expired_run_handover_exits_unclean_instead_of_a_generic_failure",
        [],
    )?;
    assert_eq!(output.status.code(), Some(70));
    assert!(start.elapsed() < Duration::from_secs(5));
    assert!(String::from_utf8_lossy(&output.stderr).contains("outcome=unclean"));
    Ok(())
}

#[test]
fn expired_return_guard_leaves_the_watchdog_to_report_unclean() -> anyhow::Result<()> {
    if std::env::var_os("PERI_SHUTDOWN_TEST_CHILD").is_some() {
        let budget = ShutdownBudget::configured()?;
        let returned = budget.cancel_on_return();
        budget.start();
        *budget.state.0.lock().unwrap().as_mut().unwrap() =
            (Instant::now() - Duration::from_millis(1), false);
        budget.state.1.notify_all();
        drop(returned);
        std::thread::sleep(Duration::from_secs(1));
        panic!("expired shutdown was disarmed by the return guard");
    }
    let output = child_output(
        "shutdown_budget::tests::expired_return_guard_leaves_the_watchdog_to_report_unclean",
        [],
    )?;
    assert_eq!(output.status.code(), Some(70));
    assert!(String::from_utf8_lossy(&output.stderr).contains("reason=deadline_exceeded"));
    Ok(())
}

fn child_output(
    name: &str,
    env: impl IntoIterator<Item = (&'static str, &'static str)>,
) -> anyhow::Result<std::process::Output> {
    let mut command = std::process::Command::new(std::env::current_exe()?);
    command.args(["--exact", name, "--nocapture"]).envs(env);
    Ok(command.env("PERI_SHUTDOWN_TEST_CHILD", "1").output()?)
}
