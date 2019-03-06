use core::store::RateLimiter;

use error::Result;

use std::cell::Cell;
use std::f64;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, SystemTime};

/// This is the {@link RateLimiter} that {@link IndexWriter} assigns to each
/// running merge, to  give {@link MergeScheduler}s ionice like control.
///
/// This is similar to {@link SimpleRateLimiter}, except it's merge-private,
/// it will wake up if its rate changes while it's paused, it tracks how
/// much time it spent stopped and paused, and it supports aborting.
pub struct MergeRateLimiter {
    total_bytes_written: AtomicU64,
    mb_per_sec: Cell<f64>,
    last_time: Cell<SystemTime>,
    min_pause_check_bytes: Cell<u64>,
    abort: AtomicBool,
    total_paused_dur: Cell<Duration>,
    total_stopped_dur: Cell<Duration>,
    // merge: OneMerge,
    lock: Arc<Mutex<()>>,
    cond: Condvar,
}

unsafe impl Send for MergeRateLimiter {}
unsafe impl Sync for MergeRateLimiter {}

#[derive(Debug, Eq, PartialEq)]
enum PauseResult {
    No,
    Stopped,
    Paused,
}

impl MergeRateLimiter {
    pub fn new() -> Self {
        let limiter = MergeRateLimiter {
            total_bytes_written: AtomicU64::new(0),
            mb_per_sec: Cell::new(0.0),
            last_time: Cell::new(SystemTime::now()),
            min_pause_check_bytes: Cell::new(0),
            abort: AtomicBool::new(false),
            total_paused_dur: Cell::new(Duration::default()),
            total_stopped_dur: Cell::new(Duration::default()),
            lock: Arc::new(Mutex::new(())),
            cond: Condvar::new(),
        };
        limiter.set_mb_per_sec(f64::INFINITY);
        limiter
    }

    fn maybe_pause(&self, bytes: u64, cur_ns: SystemTime) -> Result<PauseResult> {
        let l = self.lock.lock()?;
        // Now is a good time to abort the merge:
        self.check_abort()?;

        let mb_per_sec = self.mb_per_sec.get();
        assert!(mb_per_sec > 0.0);
        let seconds_to_pause = bytes as f64 / 1024.0 / 1024.0 / mb_per_sec;
        // Time we should sleep until; this is purely instantaneous
        // rate (just adds seconds onto the last time we had paused to);
        // maybe we should also offer decayed recent history one?
        let target_time =
            self.last_time.get() + Duration::from_nanos((seconds_to_pause * 1000_000_000.0) as u64);

        let mut cur_pause_dur = target_time.duration_since(cur_ns).unwrap();

        // NOTE: except maybe on real-time JVMs, minimum realistic
        // wait/sleep time is 1 msec; if you pass just 1 nsec the impl
        // rounds up to 1 msec, so we don't bother unless it's > 2 msec:
        if cur_pause_dur <= Duration::from_nanos(2_000_000) {
            return Ok(PauseResult::No);
        }

        // Defensive: sleep for at most 250 msec; the loop above will call us
        // again if we should keep sleeping:
        if cur_pause_dur > Duration::from_nanos(250_000_000) {
            cur_pause_dur = Duration::from_nanos(250_000_000);
        }

        // CMS can wake us up here if it changes our target rate:
        self.cond.wait_timeout(l, cur_pause_dur)?;

        let rate = self.mb_per_sec.get();
        if rate == 0.0 {
            Ok(PauseResult::Stopped)
        } else {
            Ok(PauseResult::Paused)
        }
    }

    pub fn check_abort(&self) -> Result<()> {
        let _l = self.lock.lock().unwrap();
        if self.abort.load(Ordering::Acquire) {
            bail!("merge is aborted");
        }
        Ok(())
    }

    pub fn set_abort(&self) {
        let lock = Arc::clone(&self.lock);
        let _l = lock.lock().unwrap();
        self.abort.store(true, Ordering::Release);
        self.cond.notify_one();
    }

    pub fn aborted(&self) -> bool {
        self.abort.load(Ordering::Acquire)
    }
}

const MIN_PAUSE_CHECK_MSEC: i32 = 25;

impl RateLimiter for MergeRateLimiter {
    fn set_mb_per_sec(&self, mb_per_sec: f64) {
        let lock = Arc::clone(&self.lock);
        let _g = lock.lock().unwrap();
        // 0.0 is allowed: it means the merge is paused
        if mb_per_sec < 0.0 {
            panic!("mb_per_sec must be position; got: {}", mb_per_sec);
        }

        self.mb_per_sec.set(mb_per_sec);
        // NOTE: java Double.POSITIVE_INFINITY cast to long is long.MAX_VALUE,
        // rust f64::INFINITY cast to u64 is 0.
        let check_value = MIN_PAUSE_CHECK_MSEC as f64 / 1000.0 * mb_per_sec * 1024.0 * 1024.0;
        debug_assert!(check_value >= 0.0);
        let check_bytes = if f64::is_infinite(check_value) {
            u64::max_value()
        } else {
            check_value as u64
        };
        self.min_pause_check_bytes
            .set(::std::cmp::min(1024 * 1024, check_bytes));
        self.cond.notify_one();
    }

    fn mb_per_sec(&self) -> f64 {
        self.mb_per_sec.get()
    }

    fn pause(&self, bytes: u64) -> Result<Duration> {
        self.total_bytes_written.fetch_add(bytes, Ordering::AcqRel);

        let mut start = SystemTime::now();
        let mut cur_time = start;
        // loop because:
        // 1) Thread.wait doesn't always sleep long enough
        // 2) we wake up and check again when our rate limit is
        //    changed while we were pausing:
        let mut paused = Duration::default();
        loop {
            let result = self.maybe_pause(bytes, cur_time)?;
            if result == PauseResult::No {
                // Set to curNS, not targetNS, to enforce the instant rate, not
                // the "averaaged over all history" rate:
                self.last_time.set(cur_time);
                break;
            }
            cur_time = SystemTime::now();
            let dur = cur_time.duration_since(start).unwrap();
            start = cur_time;

            // Separately track when merge was stopped vs rate limited:
            if result == PauseResult::Stopped {
                let stopped_dur = self.total_stopped_dur.get();
                self.total_stopped_dur.set(stopped_dur + dur);
            } else {
                debug_assert_eq!(result, PauseResult::Paused);
                let total_paused_dur = self.total_stopped_dur.get();
                self.total_paused_dur.set(total_paused_dur + dur);
            }
            paused += dur;
        }
        Ok(paused)
    }

    fn min_pause_check_bytes(&self) -> u64 {
        self.min_pause_check_bytes.get()
    }
}
