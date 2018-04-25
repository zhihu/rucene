use core::index::LeafReader;
use core::search::collector::Collector;
use core::search::Scorer;
use core::util::DocId;
use error::*;

/// ChainCollector makes it possible to collect on more than one collector in sequence.
///
pub struct ChainedCollector<'a> {
    collectors: Vec<&'a mut Collector>,
}

impl<'a> ChainedCollector<'a> {
    /// Constructor
    pub fn new(collectors: Vec<&'a mut Collector>) -> ChainedCollector<'a> {
        ChainedCollector { collectors }
    }
}

impl<'a> Collector for ChainedCollector<'a> {
    fn set_next_reader(&mut self, reader_ord: usize, reader: &LeafReader) -> Result<()> {
        for collector in &mut self.collectors {
            collector.set_next_reader(reader_ord, reader)?;
        }

        Ok(())
    }

    fn collect(&mut self, doc: DocId, scorer: &mut Scorer) -> Result<()> {
        for collector in &mut self.collectors {
            collector.collect(doc, scorer)?;
        }

        Ok(())
    }

    fn needs_scores(&self) -> bool {
        self.collectors.iter().any(|it| it.needs_scores())
    }
}
