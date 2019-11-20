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

pub mod cache;
pub mod collector;
pub mod query;
pub mod scorer;
pub mod similarity;
pub mod sort_field;

mod searcher;

pub use self::searcher::*;

mod explanation;

pub use self::explanation::*;

mod statistics;

pub use self::statistics::*;

mod search_manager;

pub use self::search_manager::*;

use std::i32;

use core::util::DocId;

use error::Result;

error_chain! {
    types {
        Error, ErrorKind, ResultExt;
    }

    errors {
        SearchFailed {
            description("Search failed")
        }
    }
}

pub type Payload = Vec<u8>;

/// When returned by `next()`, `advance(DocId)` and
/// `doc_id()` it means there are no more docs in the iterator.
pub const NO_MORE_DOCS: DocId = i32::MAX;

/// This trait defines methods to iterate over a set of non-decreasing
/// doc ids. Note that this class assumes it iterates on doc Ids, and therefore
/// `NO_MORE_DOCS` is set to `NO_MORE_DOCS` in order to be used as
/// a sentinel object. Implementations of this class are expected to consider
/// `std:i32:MAX` as an invalid value.
pub trait DocIterator: Send {
    /// Creates a `TermIterator` over current doc.
    ///
    /// TODO: Uncomment after implementing all the `DocIterator`s and `Scorer`s
    ///
    /// fn create_term_iterator(&self) -> TermIterator;

    /// Returns the following:
    ///
    /// * `-1` if `next()` or `advance(DocId)` were not called yet.
    /// * `NO_MORE_DOCS` if the iterator has exhausted.
    /// * Otherwise it should return the doc ID it is currently on.
    fn doc_id(&self) -> DocId;

    /// Advances to the next document in the set and returns the doc it is
    /// currently on, or `NO_MORE_DOCS` if there are no more docs in the
    /// set.
    ///
    /// *NOTE:* after the iterator has exhausted you should not call this
    /// method, as it may result in unpredicted behavior.
    fn next(&mut self) -> Result<DocId>;

    /// Advances to the first beyond the current whose document number is greater
    /// than or equal to _target_, and returns the document number itself.
    /// Exhausts the iterator and returns `NO_MORE_DOCS` if _target_
    /// is greater than the highest document number in the set.
    ///
    /// The behavior of this method is *undefined* when called with
    /// `target <= current`, or after the iterator has exhausted.
    /// Both cases may result in unpredicted behavior.
    ///
    /// Some implementations are considerably more efficient than that.
    ///
    /// *NOTE:* this method may be called with `NO_MORE_DOCS` for
    /// efficiency by some Scorers. If your implementation cannot efficiently
    /// determine that it should exhaust, it is recommended that you check for that
    /// value in each call to this method.
    fn advance(&mut self, target: DocId) -> Result<DocId>;

    /// Slow (linear) implementation of {@link #advance} relying on
    /// `next()` to advance beyond the target position.
    fn slow_advance(&mut self, target: DocId) -> Result<DocId> {
        debug_assert!(self.doc_id() < target);
        let mut doc = self.doc_id();
        while doc < target {
            doc = self.next()?;
        }
        Ok(doc)
    }

    /// Returns the estimated cost of this `DocIterator`.
    ///
    /// This is generally an upper bound of the number of documents this iterator
    /// might match, but may be a rough heuristic, hardcoded value, or otherwise
    /// completely inaccurate.
    fn cost(&self) -> usize;

    /// Return whether the current doc ID that `approximation()` is on matches. This
    /// method should only be called when the iterator is positioned -- ie. not
    /// when `DocIterator#doc_id()` is `-1` or
    /// `NO_MORE_DOCS` -- and at most once.
    fn matches(&mut self) -> Result<bool> {
        Ok(true)
    }

    /// An estimate of the expected cost to determine that a single document `#matches()`.
    /// This can be called before iterating the documents of `approximation()`.
    /// Returns an expected cost in number of simple operations like addition, multiplication,
    /// comparing two numbers and indexing an array.
    /// The returned value must be positive.
    fn match_cost(&self) -> f32 {
        0f32
    }

    /// whether this iterator support *two phase iterator*, default to false
    fn support_two_phase(&self) -> bool {
        false
    }

    /// advance to the next approximate match doc, this works the same as Lucene's
    /// `TwoPhaseIterator#next`
    fn approximate_next(&mut self) -> Result<DocId> {
        self.next()
    }

    /// Advances to the first approximate doc beyond the current doc, this works the
    /// same as Lucene's `TwoPhaseIterator#advance`
    fn approximate_advance(&mut self, target: DocId) -> Result<DocId> {
        self.advance(target)
    }
}

impl Eq for dyn DocIterator {}

impl PartialEq for dyn DocIterator {
    fn eq(&self, other: &Self) -> bool {
        self == other
    }
}

/// a `DocIterator` that means no matching doc is available
#[derive(Clone)]
pub struct EmptyDocIterator {
    doc_id: DocId,
}

impl Default for EmptyDocIterator {
    fn default() -> Self {
        EmptyDocIterator { doc_id: -1 }
    }
}

impl DocIterator for EmptyDocIterator {
    fn doc_id(&self) -> DocId {
        self.doc_id
    }

    fn next(&mut self) -> Result<DocId> {
        self.doc_id = NO_MORE_DOCS;
        Ok(NO_MORE_DOCS)
    }

    fn advance(&mut self, _target: DocId) -> Result<DocId> {
        self.doc_id = NO_MORE_DOCS;
        Ok(NO_MORE_DOCS)
    }

    fn cost(&self) -> usize {
        0usize
    }
}

/// A DocIdSet contains a set of doc ids. Implementing classes must
/// only implement *#iterator* to provide access to the set.
pub trait DocIdSet: Send + Sync {
    type Iter: DocIterator;
    /// Provides a `DocIdSetIterator` to access the set.
    /// This implementation can return None if there
    /// are no docs that match.
    fn iterator(&self) -> Result<Option<Self::Iter>>;
}

#[cfg(test)]
pub mod tests {
    use super::query::Weight;
    use super::scorer::Scorer;
    use super::*;
    use core::codec::Codec;
    use core::index::reader::LeafReaderContext;
    use std::fmt;

    pub struct MockDocIterator {
        doc_ids: Vec<DocId>,
        current_doc_id: DocId,
        offset: i32,
    }

    impl MockDocIterator {
        pub fn new(ids: Vec<DocId>) -> MockDocIterator {
            MockDocIterator {
                doc_ids: ids,
                current_doc_id: -1,
                offset: -1,
            }
        }
    }

    impl DocIterator for MockDocIterator {
        fn doc_id(&self) -> DocId {
            self.current_doc_id
        }

        fn next(&mut self) -> Result<DocId> {
            self.offset += 1;

            if (self.offset as usize) >= self.doc_ids.len() {
                self.current_doc_id = NO_MORE_DOCS;
            } else {
                self.current_doc_id = self.doc_ids[self.offset as usize];
            }

            Ok(self.doc_id())
        }

        fn advance(&mut self, target: DocId) -> Result<DocId> {
            loop {
                let doc_id = self.next()?;
                if doc_id >= target {
                    return Ok(doc_id);
                }
            }
        }

        fn cost(&self) -> usize {
            self.doc_ids.len()
        }
    }

    pub struct MockSimpleScorer<T: DocIterator> {
        iterator: T,
    }

    impl<T: DocIterator> MockSimpleScorer<T> {
        pub fn new(iterator: T) -> Self {
            MockSimpleScorer { iterator }
        }
    }

    impl<T: DocIterator> Scorer for MockSimpleScorer<T> {
        fn score(&mut self) -> Result<f32> {
            Ok(self.doc_id() as f32)
        }
    }

    impl<T: DocIterator> DocIterator for MockSimpleScorer<T> {
        fn doc_id(&self) -> DocId {
            self.iterator.doc_id()
        }

        fn next(&mut self) -> Result<DocId> {
            self.iterator.next()
        }

        fn advance(&mut self, target: DocId) -> Result<DocId> {
            self.iterator.advance(target)
        }

        fn cost(&self) -> usize {
            self.iterator.cost()
        }
        fn matches(&mut self) -> Result<bool> {
            self.iterator.matches()
        }

        fn match_cost(&self) -> f32 {
            self.iterator.match_cost()
        }

        fn approximate_next(&mut self) -> Result<DocId> {
            self.iterator.approximate_next()
        }

        fn approximate_advance(&mut self, target: DocId) -> Result<DocId> {
            self.iterator.approximate_advance(target)
        }
    }

    pub struct MockSimpleWeight {
        docs: Vec<DocId>,
    }

    impl MockSimpleWeight {
        pub fn new(docs: Vec<DocId>) -> MockSimpleWeight {
            MockSimpleWeight { docs }
        }
    }

    impl<C: Codec> Weight<C> for MockSimpleWeight {
        fn create_scorer(
            &self,
            _reader: &LeafReaderContext<'_, C>,
        ) -> Result<Option<Box<dyn Scorer>>> {
            Ok(Some(Box::new(create_mock_scorer(self.docs.clone()))))
        }

        fn query_type(&self) -> &'static str {
            "mock"
        }

        fn normalize(&mut self, _norm: f32, _boost: f32) {}

        fn value_for_normalization(&self) -> f32 {
            0.0
        }

        fn needs_scores(&self) -> bool {
            false
        }

        fn explain(&self, _reader: &LeafReaderContext<'_, C>, _doc: DocId) -> Result<Explanation> {
            unimplemented!()
        }
    }

    impl fmt::Display for MockSimpleWeight {
        fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
            unimplemented!()
        }
    }

    pub fn create_mock_scorer(docs: Vec<DocId>) -> MockSimpleScorer<MockDocIterator> {
        MockSimpleScorer::new(MockDocIterator::new(docs))
    }

    pub fn create_mock_weight(docs: Vec<DocId>) -> MockSimpleWeight {
        MockSimpleWeight::new(docs)
    }

    pub fn create_mock_doc_iterator(docs: Vec<DocId>) -> MockDocIterator {
        MockDocIterator::new(docs)
    }
}
