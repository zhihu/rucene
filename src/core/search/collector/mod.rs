// Copyright 2019 Zhizhesihai (Beijing) Technology Limited.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

mod top_docs;

pub use self::top_docs::*;

mod early_terminating;

pub use self::early_terminating::*;

mod timeout;

pub use self::timeout::*;

mod chain;

pub use self::chain::*;

use error::Result;

use core::codec::Codec;
use core::index::reader::LeafReaderContext;
use core::search::scorer::Scorer;
use core::util::DocId;

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
        CollectionTimeout {
            description("Collection timeout")
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
pub trait SearchCollector: Collector {
    type LC: ParallelLeafCollector;
    /// This method is called before collecting on a new leaf.
    fn set_next_reader<C: Codec>(&mut self, reader: &LeafReaderContext<'_, C>) -> Result<()>;

    /// iff this collector support parallel collect
    fn support_parallel(&self) -> bool;
    fn init_parallel(&mut self) {}

    /// segment collector for parallel search
    fn leaf_collector<C: Codec>(&self, reader: &LeafReaderContext<'_, C>) -> Result<Self::LC>;

    fn finish_parallel(&mut self) -> Result<()>;
}

impl<'a, T: SearchCollector + 'a> SearchCollector for &'a mut T {
    type LC = T::LC;

    fn set_next_reader<C: Codec>(&mut self, reader: &LeafReaderContext<'_, C>) -> Result<()> {
        (**self).set_next_reader(reader)
    }

    fn support_parallel(&self) -> bool {
        (**self).support_parallel()
    }

    fn init_parallel(&mut self) {
        (**self).init_parallel()
    }

    fn leaf_collector<C: Codec>(&self, reader: &LeafReaderContext<'_, C>) -> Result<Self::LC> {
        (**self).leaf_collector(reader)
    }

    fn finish_parallel(&mut self) -> Result<()> {
        (**self).finish_parallel()
    }
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
    fn collect<S: Scorer + ?Sized>(&mut self, doc: DocId, scorer: &mut S) -> Result<()>;
}

impl<'a, T: Collector + 'a> Collector for &'a mut T {
    fn needs_scores(&self) -> bool {
        (**self).needs_scores()
    }

    fn collect<S: Scorer + ?Sized>(&mut self, doc: i32, scorer: &mut S) -> Result<()> {
        (**self).collect(doc, scorer)
    }
}

/// `Collector` that collect parallel for a single segment.
///
/// once finished, the `finish_leaf` method must be
/// called to notify to main thread.
pub trait ParallelLeafCollector: Collector + Send + 'static {
    fn finish_leaf(&mut self) -> Result<()>;
}
