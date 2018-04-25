use core::index::LeafReader;
use core::search::collector;
use core::search::collector::Collector;
use core::search::Scorer;
use error::*;
use std::time::{Duration, SystemTime};

pub struct TimeoutCollector {
    timeout_duration: Duration,
    start_time: SystemTime,
    pub timeout: bool,
}

impl TimeoutCollector {
    pub fn new(timeout_duration: Duration, start_time: SystemTime) -> TimeoutCollector {
        TimeoutCollector {
            timeout_duration,
            start_time,
            timeout: false,
        }
    }
}

impl Collector for TimeoutCollector {
    fn set_next_reader(&mut self, _reader_ord: usize, _reader: &LeafReader) -> Result<()> {
        Ok(())
    }

    fn collect(&mut self, _doc: i32, _scorer: &mut Scorer) -> Result<()> {
        let now = SystemTime::now();
        if self.start_time < now && now.duration_since(self.start_time)? >= self.timeout_duration {
            self.timeout = true;
            bail!(ErrorKind::Collector(
                collector::ErrorKind::CollectionTerminated,
            ))
        }
        Ok(())
    }

    fn needs_scores(&self) -> bool {
        true
    }
}
