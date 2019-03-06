use error::Result;

use std::time::{Duration, SystemTime};

/// Abstract base class to rate limit IO.  Typically implementations are
/// shared across multiple IndexInputs or IndexOutputs (for example
/// those involved all merging).  Those IndexInputs and
/// IndexOutputs would call {@link #pause} whenever the have read
/// or written more than {@link #getMinPauseCheckBytes} bytes.

pub trait RateLimiter: Sync + Send {
    /// Sets an updated MB per second rate limit.
    fn set_mb_per_sec(&self, mb_per_sec: f64);

    /// The current MB per second rate limit.
    fn mb_per_sec(&self) -> f64;

    /// Pauses, if necessary, to keep the instantaneous IO rate
    /// at or below the target
    ///
    /// Note: the implementation is thread-safe
    fn pause(&self, bytes: u64) -> Result<Duration>;

    /// how many bytes caller should add up isself before invoking `#pause`
    fn min_pause_check_bytes(&self) -> u64;
}

const MIN_PAUSE_CHECK_MSEC: i32 = 5;

/// Simple class to rate limit IO.
pub struct SimpleRateLimiter {
    mb_per_sec: f64,
    min_pause_check_bytes: u64,
    last_ns: SystemTime,
}

impl SimpleRateLimiter {
    pub fn new(mb_per_sec: f64) -> Self {
        SimpleRateLimiter {
            mb_per_sec,
            min_pause_check_bytes: 0,
            last_ns: SystemTime::now(),
        }
    }
}

impl RateLimiter for SimpleRateLimiter {
    fn set_mb_per_sec(&self, mb_per_sec: f64) {
        unimplemented!()
    }

    fn mb_per_sec(&self) -> f64 {
        unimplemented!()
    }

    fn pause(&self, bytes: u64) -> Result<Duration> {
        unimplemented!()
    }

    fn min_pause_check_bytes(&self) -> u64 {
        unimplemented!()
    }
}
