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

use core::store::io::DataOutput;
use core::store::RateLimiter;

use error::Result;
use std::io;
use std::sync::Arc;

/// Trait for output to a file in a Directory.
///
/// A random-access output stream.  Used for all Lucene index output operations.
pub trait IndexOutput: DataOutput {
    fn name(&self) -> &str;
    fn file_pointer(&self) -> i64;
    fn checksum(&self) -> Result<i64>;
}

pub struct IndexOutputRef<T: IndexOutput> {
    // TODO: we need GAT for the lifetime declaration
    // so, currently directly use raw pointer instead
    output: *mut T,
}

impl<T: IndexOutput> IndexOutputRef<T> {
    pub fn new(output: &mut T) -> Self {
        Self { output }
    }
}

impl<T: IndexOutput> IndexOutput for IndexOutputRef<T> {
    fn name(&self) -> &str {
        unsafe { (*self.output).name() }
    }

    fn file_pointer(&self) -> i64 {
        unsafe { (*self.output).file_pointer() }
    }

    fn checksum(&self) -> Result<i64> {
        unsafe { (*self.output).checksum() }
    }
}

impl<T: IndexOutput> DataOutput for IndexOutputRef<T> {}

impl<T: IndexOutput> io::Write for IndexOutputRef<T> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        unsafe { (*self.output).write(buf) }
    }

    fn flush(&mut self) -> io::Result<()> {
        unsafe { (*self.output).flush() }
    }
}

pub struct InvalidIndexOutput {}

impl io::Write for InvalidIndexOutput {
    fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
        unreachable!()
    }

    fn flush(&mut self) -> io::Result<()> {
        unreachable!()
    }
}

impl DataOutput for InvalidIndexOutput {}

impl IndexOutput for InvalidIndexOutput {
    fn name(&self) -> &str {
        "invalid"
    }

    fn file_pointer(&self) -> i64 {
        -1
    }

    fn checksum(&self) -> Result<i64> {
        unreachable!()
    }
}

/// a rate limiting `IndexOutput`
pub struct RateLimitIndexOutput<O: IndexOutput, RL: RateLimiter + ?Sized> {
    delegate: O,
    rate_limiter: Arc<RL>,
    /// How many bytes we've written since we last called rateLimiter.pause.
    bytes_since_last_pause: usize,
    /// Cached here not not always have to call RateLimiter#getMinPauseCheckBytes()
    /// which does volatile read
    current_min_pause_check_bytes: usize,
}

impl<O: IndexOutput, RL: RateLimiter + ?Sized> RateLimitIndexOutput<O, RL> {
    pub fn new(rate_limiter: Arc<RL>, delegate: O) -> Self {
        let current_min_pause_check_bytes = rate_limiter.min_pause_check_bytes() as usize;
        RateLimitIndexOutput {
            delegate,
            rate_limiter,
            bytes_since_last_pause: 0,
            current_min_pause_check_bytes,
        }
    }

    fn check_rate(&mut self) -> Result<()> {
        if self.bytes_since_last_pause > self.current_min_pause_check_bytes {
            self.rate_limiter
                .pause(self.bytes_since_last_pause as u64)?;
            self.bytes_since_last_pause = 0;
            self.current_min_pause_check_bytes = self.rate_limiter.min_pause_check_bytes() as usize;
        }
        Ok(())
    }
}

impl<O: IndexOutput, RL: RateLimiter + ?Sized> IndexOutput for RateLimitIndexOutput<O, RL> {
    fn name(&self) -> &str {
        self.delegate.name()
    }

    fn file_pointer(&self) -> i64 {
        self.delegate.file_pointer()
    }

    fn checksum(&self) -> Result<i64> {
        self.delegate.checksum()
    }
}

impl<O: IndexOutput, RL: RateLimiter + ?Sized> DataOutput for RateLimitIndexOutput<O, RL> {}

impl<O: IndexOutput, RL: RateLimiter + ?Sized> io::Write for RateLimitIndexOutput<O, RL> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.bytes_since_last_pause += buf.len();
        if let Err(_e) = self.check_rate() {
            return Err(io::Error::from(io::ErrorKind::WouldBlock));
        }
        self.delegate.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.delegate.flush()
    }
}
