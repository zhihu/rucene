use core::index::LeafReader;
use core::search::collector::{Collector, LeafCollector, SearchCollector};
use core::search::Scorer;
use core::util::DocId;
use error::*;

/// ChainCollector makes it possible to collect on more than one collector in sequence.
pub struct ChainedCollector<'a> {
    collectors: Vec<&'a mut SearchCollector>,
}

impl<'a> ChainedCollector<'a> {
    /// Constructor
    pub fn new(collectors: Vec<&'a mut SearchCollector>) -> ChainedCollector<'a> {
        ChainedCollector { collectors }
    }
}

impl<'a> SearchCollector for ChainedCollector<'a> {
    fn set_next_reader(&mut self, reader_ord: usize, reader: &LeafReader) -> Result<()> {
        for collector in &mut self.collectors {
            collector.set_next_reader(reader_ord, reader)?;
        }

        Ok(())
    }

    fn support_parallel(&self) -> bool {
        self.collectors.iter().all(|it| it.support_parallel())
    }

    fn leaf_collector(&mut self, reader: &LeafReader) -> Result<Box<LeafCollector>> {
        let mut leaf_collectors = Vec::with_capacity(self.collectors.len());
        for c in &mut self.collectors {
            leaf_collectors.push(c.leaf_collector(reader)?);
        }
        Ok(Box::new(ChainedLeafCollector {
            collectors: leaf_collectors,
        }))
    }

    fn finish(&mut self) -> Result<()> {
        for collector in &mut self.collectors {
            collector.finish()?;
        }
        Ok(())
    }
}

impl<'a> Collector for ChainedCollector<'a> {
    fn needs_scores(&self) -> bool {
        self.collectors.iter().any(|it| it.needs_scores())
    }

    fn collect(&mut self, doc: DocId, scorer: &mut Scorer) -> Result<()> {
        for collector in &mut self.collectors {
            collector.collect(doc, scorer)?;
        }

        Ok(())
    }
}

struct ChainedLeafCollector {
    collectors: Vec<Box<LeafCollector>>,
}

impl Collector for ChainedLeafCollector {
    fn needs_scores(&self) -> bool {
        self.collectors.iter().any(|c| c.needs_scores())
    }

    fn collect(&mut self, doc: i32, scorer: &mut Scorer) -> Result<()> {
        for collector in &mut self.collectors {
            collector.collect(doc, scorer)?;
        }

        Ok(())
    }
}

impl LeafCollector for ChainedLeafCollector {
    fn finish_leaf(&mut self) -> Result<()> {
        for collector in &mut self.collectors {
            collector.as_mut().finish_leaf()?;
        }

        Ok(())
    }
}
