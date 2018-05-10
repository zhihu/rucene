use core::index::LeafReader;
use core::search::collector;
use core::search::collector::{SearchCollector, Collector, LeafCollector};
use core::search::Scorer;
use core::util::DocId;
use error::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub struct EarlyTerminatingSortingCollector<'a> {
    pub early_terminated: Arc<AtomicBool>,
    num_docs_to_collect_per_reader: usize,
    num_docs_collected_per_reader: usize,
    collector: &'a mut SearchCollector,
}

impl<'a> EarlyTerminatingSortingCollector<'a> {
    pub fn new(
        collector: &'a mut SearchCollector,
        num_docs_to_collect_per_reader: usize,
    ) -> EarlyTerminatingSortingCollector {
        assert!(
            num_docs_to_collect_per_reader > 0,
            format!(
                "num_docs_to_collect_per_reader must always be > 0, got {}",
                num_docs_to_collect_per_reader
            )
        );

        EarlyTerminatingSortingCollector {
            early_terminated: Arc::new(AtomicBool::new(false)),
            num_docs_to_collect_per_reader,
            num_docs_collected_per_reader: 0,
            collector,
        }
    }
}

impl<'a> SearchCollector for EarlyTerminatingSortingCollector<'a> {
    fn set_next_reader(&mut self, reader_ord: usize, reader: &LeafReader) -> Result<()> {
        self.num_docs_collected_per_reader = 0;
        self.collector.set_next_reader(reader_ord, reader)
    }

    fn support_parallel(&self) -> bool {
        self.collector.support_parallel()
    }

    fn leaf_collector(&mut self, reader: &LeafReader) -> Box<LeafCollector> {
        assert!(self.support_parallel());
        Box::new(EarlyTerminatingLeafCollector::new(
            self.num_docs_to_collect_per_reader,
            self.collector.leaf_collector(reader),
            Arc::clone(&self.early_terminated)
        ))
    }

    fn finish(&mut self) -> Result<()> {
        self.collector.finish()
    }
}

impl<'a> Collector for EarlyTerminatingSortingCollector<'a> {
    fn needs_scores(&self) -> bool {
        self.collector.needs_scores()
    }

    fn collect(&mut self, doc: DocId, scorer: &mut Scorer) -> Result<()> {
        self.num_docs_collected_per_reader += 1;

        if self.num_docs_collected_per_reader > self.num_docs_to_collect_per_reader {
            self.early_terminated.swap(true, Ordering::Relaxed);
            bail!(ErrorKind::Collector(
                collector::ErrorKind::LeafCollectionTerminated,
            ))
        } else {
            self.collector.collect(doc, scorer)
        }
    }
}

pub struct EarlyTerminatingLeafCollector {
    pub early_terminated: Arc<AtomicBool>,
    num_docs_to_collect: usize,
    num_docs_collected: usize,
    collector: Box<LeafCollector>
}

impl EarlyTerminatingLeafCollector {
    pub fn new(
        num_docs_to_collect: usize,
        collector: Box<LeafCollector>,
        early_terminated: Arc<AtomicBool>
    ) -> EarlyTerminatingLeafCollector {
        EarlyTerminatingLeafCollector {
            early_terminated,
            num_docs_to_collect,
            num_docs_collected: 0,
            collector
        }
    }
}

impl LeafCollector for EarlyTerminatingLeafCollector {
    fn finish_leaf(&mut self) -> Result<()> {
        self.collector.finish_leaf()
    }
}

impl Collector for EarlyTerminatingLeafCollector {
    fn needs_scores(&self) -> bool {
        self.collector.needs_scores()
    }

    fn collect(&mut self, doc: i32, scorer: &mut Scorer) -> Result<()> {
        self.num_docs_collected += 1;

        if self.num_docs_collected > self.num_docs_to_collect {
            self.early_terminated.swap(true, Ordering::Relaxed);
            bail!(ErrorKind::Collector(
                collector::ErrorKind::LeafCollectionTerminated,
            ))
        } else {
            self.collector.collect(doc, scorer)
        }
    }
}