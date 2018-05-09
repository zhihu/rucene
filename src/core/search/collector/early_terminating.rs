use core::index::LeafReader;
use core::search::collector;
use core::search::collector::Collector;
use core::search::Scorer;
use core::util::DocId;
use error::*;

pub struct EarlyTerminatingSortingCollector<'a> {
    pub early_terminated: bool,
    num_docs_to_collect_per_reader: usize,
    num_docs_collected_per_reader: usize,
    collector: &'a mut Collector,
}

impl<'a> EarlyTerminatingSortingCollector<'a> {
    pub fn new(
        collector: &'a mut Collector,
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
            early_terminated: false,
            num_docs_to_collect_per_reader,
            num_docs_collected_per_reader: 0,
            collector,
        }
    }
}

impl<'a> Collector for EarlyTerminatingSortingCollector<'a> {
    fn set_next_reader(&mut self, reader_ord: usize, reader: &LeafReader) -> Result<()> {
        self.num_docs_collected_per_reader = 0;
        self.collector.set_next_reader(reader_ord, reader)
    }

    fn collect(&mut self, doc: DocId, scorer: &mut Scorer) -> Result<()> {
        self.num_docs_collected_per_reader += 1;

        if self.num_docs_collected_per_reader > self.num_docs_to_collect_per_reader {
            self.early_terminated = true;
            bail!(ErrorKind::Collector(
                collector::ErrorKind::LeafCollectionTerminated,
            ))
        } else {
            self.collector.collect(doc, scorer)
        }
    }

    // NOTE: this collector won't be used alone, so return true will be ok
    fn needs_scores(&self) -> bool {
        self.collector.needs_scores()
    }
}
