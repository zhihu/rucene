use core::codec::Codec;
use core::index::LeafReaderContext;
use core::search::collector;
use core::search::collector::{Collector, ParallelLeafCollector, SearchCollector};
use core::search::Scorer;
use core::util::DocId;
use error::{ErrorKind, Result};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub struct EarlyTerminatingSortingCollector {
    pub early_terminated: Arc<AtomicBool>,
    num_docs_to_collect_per_reader: usize,
    num_docs_collected_per_reader: usize,
}

impl EarlyTerminatingSortingCollector {
    pub fn new(num_docs_to_collect_per_reader: usize) -> EarlyTerminatingSortingCollector {
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
        }
    }
}

impl SearchCollector for EarlyTerminatingSortingCollector {
    type LC = EarlyTerminatingLeafCollector;
    fn set_next_reader<C: Codec>(&mut self, _reader: &LeafReaderContext<'_, C>) -> Result<()> {
        self.num_docs_collected_per_reader = 0;
        Ok(())
    }

    fn support_parallel(&self) -> bool {
        true
    }

    fn leaf_collector<C: Codec>(&mut self, _reader: &LeafReaderContext<'_, C>) -> Result<Self::LC> {
        assert!(self.support_parallel());
        Ok(EarlyTerminatingLeafCollector::new(
            self.num_docs_to_collect_per_reader,
            Arc::clone(&self.early_terminated),
        ))
    }

    fn finish_parallel(&mut self) -> Result<()> {
        Ok(())
    }
}

impl Collector for EarlyTerminatingSortingCollector {
    fn needs_scores(&self) -> bool {
        false
    }

    fn collect<S: Scorer + ?Sized>(&mut self, _doc: DocId, _scorer: &mut S) -> Result<()> {
        self.num_docs_collected_per_reader += 1;

        if self.num_docs_collected_per_reader > self.num_docs_to_collect_per_reader {
            self.early_terminated.store(true, Ordering::Release);
            bail!(ErrorKind::Collector(
                collector::ErrorKind::LeafCollectionTerminated,
            ))
        }
        Ok(())
    }
}

pub struct EarlyTerminatingLeafCollector {
    pub early_terminated: Arc<AtomicBool>,
    num_docs_to_collect: usize,
    num_docs_collected: usize,
}

impl EarlyTerminatingLeafCollector {
    pub fn new(
        num_docs_to_collect: usize,
        early_terminated: Arc<AtomicBool>,
    ) -> EarlyTerminatingLeafCollector {
        EarlyTerminatingLeafCollector {
            early_terminated,
            num_docs_to_collect,
            num_docs_collected: 0,
        }
    }
}

impl ParallelLeafCollector for EarlyTerminatingLeafCollector {
    fn finish_leaf(&mut self) -> Result<()> {
        Ok(())
    }
}

impl Collector for EarlyTerminatingLeafCollector {
    fn needs_scores(&self) -> bool {
        false
    }

    fn collect<S: Scorer + ?Sized>(&mut self, _doc: i32, _scorer: &mut S) -> Result<()> {
        self.num_docs_collected += 1;

        if self.num_docs_collected > self.num_docs_to_collect {
            self.early_terminated.swap(true, Ordering::AcqRel);
            bail!(ErrorKind::Collector(
                collector::ErrorKind::LeafCollectionTerminated,
            ))
        }
        Ok(())
    }
}
