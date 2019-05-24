// Copyright 2019 Zhizhesihai (Beijing) Technology Limited.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use error::Result;

use std::sync::Arc;
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

impl RateLimiter for Arc<RateLimiter> {
    fn set_mb_per_sec(&self, mb_per_sec: f64) {
        (**self).set_mb_per_sec(mb_per_sec);
    }

    fn mb_per_sec(&self) -> f64 {
        (**self).mb_per_sec()
    }

    fn pause(&self, bytes: u64) -> Result<Duration> {
        (**self).pause(bytes)
    }

    fn min_pause_check_bytes(&self) -> u64 {
        (**self).min_pause_check_bytes()
    }
}

/// Simple class to rate limit IO.
pub struct SimpleRateLimiter {
    _mb_per_sec: f64,
    _min_pause_check_bytes: u64,
    _last_ns: SystemTime,
}

impl SimpleRateLimiter {
    pub fn new(mb_per_sec: f64) -> Self {
        SimpleRateLimiter {
            _mb_per_sec: mb_per_sec,
            _min_pause_check_bytes: 0,
            _last_ns: SystemTime::now(),
        }
    }
}

impl RateLimiter for SimpleRateLimiter {
    fn set_mb_per_sec(&self, _mb_per_sec: f64) {
        unimplemented!()
    }

    fn mb_per_sec(&self) -> f64 {
        unimplemented!()
    }

    fn pause(&self, _bytes: u64) -> Result<Duration> {
        unimplemented!()
    }

    fn min_pause_check_bytes(&self) -> u64 {
        unimplemented!()
    }
}
