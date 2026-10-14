use anyhow::{Context, Result};

use super::types::LeaseClock;

pub struct SystemLeaseClock;

impl LeaseClock for SystemLeaseClock {
    fn now_ms(&self) -> Result<u64> {
        continuous_ms()
    }
}

#[cfg(target_os = "linux")]
fn continuous_ms() -> Result<u64> {
    let mut time = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    let result = unsafe { libc::clock_gettime(libc::CLOCK_BOOTTIME, &mut time) };
    anyhow::ensure!(
        result == 0,
        "CLOCK_BOOTTIME failed: {}",
        std::io::Error::last_os_error()
    );
    u64::try_from(time.tv_sec)?
        .checked_mul(1000)
        .and_then(|seconds| seconds.checked_add(time.tv_nsec as u64 / 1_000_000))
        .context("continuous clock overflow")
}

#[cfg(target_os = "macos")]
fn continuous_ms() -> Result<u64> {
    #[repr(C)]
    struct Timebase {
        numerator: u32,
        denominator: u32,
    }
    unsafe extern "C" {
        fn mach_continuous_time() -> u64;
        fn mach_timebase_info(info: *mut Timebase) -> i32;
    }
    let mut info = Timebase {
        numerator: 0,
        denominator: 0,
    };
    let result = unsafe { mach_timebase_info(&mut info) };
    anyhow::ensure!(
        result == 0 && info.denominator != 0,
        "mach timebase unavailable"
    );
    let ticks = unsafe { mach_continuous_time() };
    u64::try_from(
        u128::from(ticks) * u128::from(info.numerator) / u128::from(info.denominator) / 1_000_000,
    )
    .context("continuous clock overflow")
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn continuous_ms() -> Result<u64> {
    anyhow::bail!("suspend-inclusive lease clock unsupported on this platform")
}
