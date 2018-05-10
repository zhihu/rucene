use error::*;

use core::index::LeafReader;
use core::search::Scorer;
use core::util::DocId;

pub mod top_docs;

pub use self::top_docs::*;

pub mod early_terminating;

pub use self::early_terminating::*;

mod timeout;

pub use self::timeout::*;

pub mod chain;

pub use self::chain::*;

error_chain! {
    types {
        Error, ErrorKind, ResultExt;
    }
    errors {
        LeafCollectionTerminated {
            description("Leaf collection terminated")
        }
        CollectionTerminated {
            description("Collection terminated")
        }
        CollectionFailed {
            description("Collection failed")
        }
    }
}

/// Expert: Collectors are primarily meant to be used to
/// gather raw results from a search, and implement sorting
/// or custom result filtering, collation, etc.
///
/// `Collector` decouples the score from the collected doc:
/// the score computation is skipped entirely if it's not
/// needed. If your collector may request the
/// score for a single hit multiple times, you should use
/// `ScoreCachingWrappingScorer`.
///
/// *NOTE:* The doc that is passed to the collect
/// method is relative to the current reader. If your
/// collector needs to resolve this to the docID space of the
/// Multi*Reader, you must re-base it by recording the
/// docBase from the most recent setNextReader call.
///
/// Not all collectors will need to rebase the docID.  For
/// example, a collector that simply counts the total number
/// of hits would skip it.
///
pub trait SearchCollector: Collector {
    /// This method is called before collecting on a new leaf.
    ///
    fn set_next_reader(&mut self, reader_ord: usize, reader: &LeafReader) -> Result<()>;

    /// iff this collector support parallel collect
    fn support_parallel(&self) -> bool;

    /// segment collector for parallel search
    fn leaf_collector(&mut self, reader: &LeafReader) -> Box<LeafCollector>;

    fn finish(&mut self) -> Result<()>;
}

pub trait Collector {
    /// Indicates if document scores are needed by this collector.
    /// return `true` if scores are needed.
    fn needs_scores(&self) -> bool;

    /// Called once for every document matching a query, with the unbased document
    /// number.
    /// Note: The collection of the current segment can be terminated by throwing
    /// a `ErrorKind::LeafCollectionTerminated`. In this case, the last docs of the
    /// current `LeafReader` will be skipped and `IndexSearcher`
    /// will swallow the exception and continue collection with the next leaf.
    ///
    /// Note: This is called in an inner search loop. For good search performance,
    /// implementations of this method should not call `IndexSearcher::doc(DocId)` on every hit.
    /// Doing so can slow searches by an order of magnitude or more.
    ///
    fn collect(&mut self, doc: DocId, scorer: &mut Scorer) -> Result<()>;
}

pub trait LeafCollector: Collector + Send + Sync {
    fn finish_leaf(&mut self) -> Result<()>;
}


