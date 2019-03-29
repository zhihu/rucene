use core::index::LeafReaderContext;
use core::search::collector;
use core::search::collector::{Collector, LeafCollector, SearchCollector};
use core::search::Scorer;
use core::util::DocId;
use error::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

pub struct TimeoutCollector {
    timeout_duration: Duration,
    start_time: SystemTime,
    pub timeout: Arc<AtomicBool>,
}

impl TimeoutCollector {
    pub fn new(timeout_duration: Duration, start_time: SystemTime) -> TimeoutCollector {
        TimeoutCollector {
            timeout_duration,
            start_time,
            timeout: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl SearchCollector for TimeoutCollector {
    fn set_next_reader(&mut self, _reader: &LeafReaderContext) -> Result<()> {
        Ok(())
    }

    fn support_parallel(&self) -> bool {
        true
    }

    fn leaf_collector(&mut self, _reader: &LeafReaderContext) -> Result<Box<LeafCollector>> {
        Ok(Box::new(TimeoutLeafCollector::new(
            self.timeout_duration,
            self.start_time,
            Arc::clone(&self.timeout),
        )))
    }

    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

impl Collector for TimeoutCollector {
    fn needs_scores(&self) -> bool {
        false
    }

    fn collect(&mut self, _doc: DocId, _scorer: &mut Scorer) -> Result<()> {
        let now = SystemTime::now();
        if self.start_time < now && now.duration_since(self.start_time)? >= self.timeout_duration {
            self.timeout.store(true, Ordering::Release);
            bail!(ErrorKind::Collector(
                collector::ErrorKind::CollectionTerminated,
            ))
        }
        Ok(())
    }
}

struct TimeoutLeafCollector {
    timeout_duration: Duration,
    start_time: SystemTime,
    timeout: Arc<AtomicBool>,
}

impl TimeoutLeafCollector {
    pub fn new(
        timeout_duration: Duration,
        start_time: SystemTime,
        timeout: Arc<AtomicBool>,
    ) -> TimeoutLeafCollector {
        TimeoutLeafCollector {
            timeout_duration,
            start_time,
            timeout,
        }
    }
}

impl Collector for TimeoutLeafCollector {
    fn needs_scores(&self) -> bool {
        false
    }

    fn collect(&mut self, _doc: i32, _scorer: &mut Scorer) -> Result<()> {
        let now = SystemTime::now();
        if self.start_time < now && now.duration_since(self.start_time)? >= self.timeout_duration {
            self.timeout.store(true, Ordering::Release);
            bail!(ErrorKind::Collector(
                collector::ErrorKind::CollectionTerminated,
            ))
        }
        Ok(())
    }
}

impl LeafCollector for TimeoutLeafCollector {
    fn finish_leaf(&mut self) -> Result<()> {
        Ok(())
    }
}
