use core::codec::Codec;
use core::index::LeafReaderContext;
use core::search::collector::{Collector, ParallelLeafCollector, SearchCollector};
use core::search::Scorer;
use core::util::DocId;
use error::Result;

/// ChainCollector makes it possible to collect on more than one collector in sequence.
pub struct ChainedCollector<A, B> {
    first: A,
    second: B,
}

impl<A, B> ChainedCollector<A, B> {
    /// Constructor
    pub fn new(first: A, second: B) -> ChainedCollector<A, B> {
        ChainedCollector { first, second }
    }
}

impl<A, B> SearchCollector for ChainedCollector<A, B>
where
    A: SearchCollector,
    B: SearchCollector,
{
    type LC = ChainedCollector<A::LC, B::LC>;

    fn set_next_reader<C: Codec>(&mut self, reader: &LeafReaderContext<'_, C>) -> Result<()> {
        self.first.set_next_reader(reader)?;
        self.second.set_next_reader(reader)
    }

    fn support_parallel(&self) -> bool {
        self.first.support_parallel() && self.second.support_parallel()
    }

    fn leaf_collector<C: Codec>(
        &mut self,
        reader: &LeafReaderContext<'_, C>,
    ) -> Result<ChainedCollector<A::LC, B::LC>> {
        Ok(ChainedCollector {
            first: self.first.leaf_collector(reader)?,
            second: self.second.leaf_collector(reader)?,
        })
    }

    fn finish_parallel(&mut self) -> Result<()> {
        // reverse order for finish
        self.second.finish_parallel()?;
        self.first.finish_parallel()
    }
}

impl<A, B> Collector for ChainedCollector<A, B>
where
    A: Collector,
    B: Collector,
{
    fn needs_scores(&self) -> bool {
        self.first.needs_scores() || self.second.needs_scores()
    }

    fn collect<S: Scorer + ?Sized>(&mut self, doc: DocId, scorer: &mut S) -> Result<()> {
        self.first.collect(doc, scorer)?;
        self.second.collect(doc, scorer)
    }
}

impl<A, B> ParallelLeafCollector for ChainedCollector<A, B>
where
    A: ParallelLeafCollector,
    B: ParallelLeafCollector,
{
    fn finish_leaf(&mut self) -> Result<()> {
        // reverse order for finish
        self.second.finish_leaf()?;
        self.first.finish_leaf()
    }
}
