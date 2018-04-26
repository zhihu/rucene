use core::index::LeafReader;
use core::search::collector;
use core::search::collector::Collector;
use core::search::Scorer;
use core::util::DocId;
use error::*;

pub struct EarlyTerminatingSortingCollector {
    pub early_terminated: bool,
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
            early_terminated: false,
            num_docs_to_collect_per_reader,
            num_docs_collected_per_reader: 0,
        }
    }
}

impl Collector for EarlyTerminatingSortingCollector {
    fn set_next_reader(&mut self, _reader_ord: usize, _reader: &LeafReader) -> Result<()> {
        self.num_docs_collected_per_reader = 0;

        Ok(())
    }

    fn collect(&mut self, _doc: DocId, _scorer: &mut Scorer) -> Result<()> {
        self.num_docs_collected_per_reader += 1;

        if self.num_docs_collected_per_reader >= self.num_docs_to_collect_per_reader {
            self.early_terminated = true;
            bail!(ErrorKind::Collector(
                collector::ErrorKind::LeafCollectionTerminated,
            ))
        } else {
            Ok(())
        }
    }

    // NOTE: this collector won't be used alone, so return true will be ok
    fn needs_scores(&self) -> bool {
        true
    }
}
