use core::store::{DataOutput, RateLimiter};
use error::Result;
use std::io;
use std::sync::Arc;

pub trait IndexOutput: DataOutput + Drop {
    fn name(&self) -> &str;
    fn file_pointer(&self) -> i64;
    fn checksum(&self) -> Result<i64>;

    fn as_data_output(&mut self) -> &mut DataOutput;
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

    fn as_data_output(&mut self) -> &mut DataOutput {
        unimplemented!()
    }
}

impl Drop for InvalidIndexOutput {
    fn drop(&mut self) {}
}

pub struct RateLimitIndexOutput {
    delegate: Box<IndexOutput>,
    rate_limiter: Arc<RateLimiter>,
    /// How many bytes we've written since we last called rateLimiter.pause.
    bytes_since_last_pause: usize,
    /// Cached here not not always have to call RateLimiter#getMinPauseCheckBytes()
    /// which does volatile read
    current_min_pause_check_bytes: usize,
}

impl RateLimitIndexOutput {
    pub fn new(rate_limiter: Arc<RateLimiter>, delegate: Box<IndexOutput>) -> Self {
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

impl IndexOutput for RateLimitIndexOutput {
    fn name(&self) -> &str {
        self.delegate.name()
    }

    fn file_pointer(&self) -> i64 {
        self.delegate.file_pointer()
    }

    fn checksum(&self) -> Result<i64> {
        self.delegate.checksum()
    }

    fn as_data_output(&mut self) -> &mut DataOutput {
        self
    }
}

impl DataOutput for RateLimitIndexOutput {}

impl io::Write for RateLimitIndexOutput {
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

impl Drop for RateLimitIndexOutput {
    fn drop(&mut self) {}
}
