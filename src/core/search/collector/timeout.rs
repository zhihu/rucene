use core::index::LeafReader;
use core::search::collector;
use core::search::collector::{SearchCollector, Collector, LeafCollector};
use core::search::Scorer;
use core::util::DocId;
use error::*;
use std::time::{Duration, SystemTime};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub struct TimeoutCollector {
    timeout_duration: Duration,
    start_time: SystemTime,
    pub timeout: Arc<AtomicBool>,
    collector: Box<SearchCollector>
}

impl TimeoutCollector {
    pub fn new(
        timeout_duration: Duration,
        start_time: SystemTime,
        collector: Box<SearchCollector>
    ) -> TimeoutCollector {
        TimeoutCollector {
            timeout_duration,
            start_time,
            timeout: Arc::new(AtomicBool::new(false)),
            collector
        }
    }
}

impl SearchCollector for TimeoutCollector {
    fn set_next_reader(&mut self, _reader_ord: usize, _reader: &LeafReader) -> Result<()> {
        Ok(())
    }

    fn support_parallel(&self) -> bool {
        self.collector.support_parallel()
    }

    fn leaf_collector(&mut self, reader: &LeafReader) -> Box<LeafCollector> {
        Box::new(TimeoutLeafCollector::new(
            self.timeout_duration,
            self.start_time,
            self.collector.leaf_collector(reader),
            Arc::clone(&self.timeout)
        ))
    }

    fn finish(&mut self) -> Result<()> {
        self.collector.finish()
    }
}

impl Collector for TimeoutCollector {
    fn needs_scores(&self) -> bool {
        self.collector.needs_scores()
    }

    fn collect(&mut self, doc: DocId, scorer: &mut Scorer) -> Result<()> {
        let now = SystemTime::now();
        if self.start_time < now && now.duration_since(self.start_time)? >= self.timeout_duration {
            self.timeout.swap(true, Ordering::Relaxed);
            bail!(ErrorKind::Collector(
                collector::ErrorKind::CollectionTerminated,
            ))
        }
        self.collector.collect(doc, scorer)
    }
}

struct TimeoutLeafCollector {
    timeout_duration: Duration,
    start_time: SystemTime,
    collector: Box<LeafCollector>,
    timeout: Arc<AtomicBool>
}

impl TimeoutLeafCollector {
    pub fn new(
        timeout_duration: Duration,
        start_time: SystemTime,
        collector: Box<LeafCollector>,
        timeout: Arc<AtomicBool>
    ) -> TimeoutLeafCollector {
        TimeoutLeafCollector {
            timeout_duration, start_time, collector, timeout
        }
    }
}

impl Collector for TimeoutLeafCollector {
    fn needs_scores(&self) -> bool {
        self.collector.needs_scores()
    }

    fn collect(&mut self, doc: i32, scorer: &mut Scorer) -> Result<()> {
        let now = SystemTime::now();
        if self.start_time < now && now.duration_since(self.start_time)? >= self.timeout_duration {
            self.timeout.swap(true, Ordering::Relaxed);
            bail!(ErrorKind::Collector(
                collector::ErrorKind::CollectionTerminated,
            ))
        }
        self.collector.collect(doc, scorer)
    }
}

impl LeafCollector for TimeoutLeafCollector {
    fn finish_leaf(&mut self) -> Result<()> {
        self.collector.finish_leaf()
    }
}
