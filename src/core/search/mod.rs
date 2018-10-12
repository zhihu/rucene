use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::fmt::Display;
use std::hash::{Hash, Hasher};
use std::i32;
use std::collections::HashMap;

use core::index::LeafReader;
use core::search::explanation::Explanation;
use core::search::searcher::IndexSearcher;
use core::search::statistics::CollectionStatistics;
use core::search::statistics::TermStatistics;
use core::search::term_query::TermQuery;
use core::search::top_docs::TopDocs;
use core::util::bit_set::ImmutableBitSetRef;
use core::util::{DocId, IndexedContext, KeyedContext, VariantValue};
use error::*;

pub mod collector;
pub mod conjunction;
pub mod disjunction;
pub mod match_all;
pub mod min_score;
pub mod point_range;
pub mod posting_iterator;
pub mod spans;

pub mod bulk_scorer;
pub mod disi;
pub mod field_comparator;
pub mod req_opt;
pub mod rescorer;
pub mod search_group;
pub mod sort;
pub mod sort_field;
pub mod top_docs;
pub mod util;

// Queries
pub mod boolean_query;
pub mod boost;
pub mod phrase_query;
pub mod query_string;
pub mod term_query;

// Scorers
pub mod term_scorer;

// Similarities
pub mod bm25_similarity;

// IndexSearcher
pub mod searcher;

// Statistics
pub mod cache_policy;
pub mod explanation;
pub mod lru_cache;
pub mod lru_query_cache;
pub mod statistics;

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
///
pub const NO_MORE_DOCS: DocId = i32::MAX;

/// This trait defines methods to iterate over a set of non-decreasing
/// doc ids. Note that this class assumes it iterates on doc Ids, and therefore
/// `NO_MORE_DOCS` is set to `NO_MORE_DOCS` in order to be used as
/// a sentinel object. Implementations of this class are expected to consider
/// `std:i32:MAX` as an invalid value.
///
pub trait DocIterator: Send + Sync {
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
    ///
    fn doc_id(&self) -> DocId;

    /// Advances to the next document in the set and returns the doc it is
    /// currently on, or `NO_MORE_DOCS` if there are no more docs in the
    /// set.
    ///
    /// *NOTE:* after the iterator has exhausted you should not call this
    /// method, as it may result in unpredicted behavior.
    ///
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
    ///
    fn advance(&mut self, target: DocId) -> Result<DocId>;

    /// Slow (linear) implementation of {@link #advance} relying on
    /// `next()` to advance beyond the target position.
    ///
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
    ///
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

    /// advance to the next approximate match doc
    fn approximate_next(&mut self) -> Result<DocId> {
        self.next()
    }

    /// Advances to the first approximate doc beyond the current doc
    fn approximate_advance(&mut self, target: DocId) -> Result<DocId> {
        self.advance(target)
    }
}

impl Eq for DocIterator {}

impl PartialEq for DocIterator {
    fn eq(&self, other: &Self) -> bool {
        self == other
    }
}

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

/// Common scoring functionality for different types of queries.
pub trait Scorer: DocIterator + Send + Sync {
    /// Returns the score of the current document matching the query.
    /// Initially invalid, until `DocIterator::next()` or
    /// `DocIterator::advance(DocId)` is called on the `iterator()`
    /// the first time, or when called from within `LeafCollector::collect`.
    ///
    fn score(&mut self) -> Result<f32>;

    /// whether this scorer support *two phase iterator*, default to false
    ///
    fn support_two_phase(&self) -> bool {
        false
    }

    fn score_context(&mut self) -> Result<IndexedContext> {
        unimplemented!()
    }

    fn score_feature(&mut self) -> Result<Vec<FeatureResult>> {
        unimplemented!()
    }
}

// helper function for doc iterator support two phase
pub fn two_phase_next(scorer: &mut Scorer) -> Result<DocId> {
    let mut doc = scorer.doc_id();
    loop {
        if doc == NO_MORE_DOCS {
            return Ok(NO_MORE_DOCS);
        } else if scorer.matches()? {
            return Ok(doc);
        }
        doc = scorer.approximate_next()?;
    }
}

impl Eq for Scorer {}

impl PartialEq for Scorer {
    fn eq(&self, other: &Self) -> bool {
        self == other
    }
}

// TODO mayte interface return this should return `Option<Scorer>`
pub struct MatchNoDocScorer {
    iterator: EmptyDocIterator,
}

impl Default for MatchNoDocScorer {
    fn default() -> Self {
        MatchNoDocScorer {
            iterator: EmptyDocIterator::default(),
        }
    }
}

impl DocIterator for MatchNoDocScorer {
    fn doc_id(&self) -> DocId {
        self.iterator.doc_id
    }

    fn next(&mut self) -> Result<DocId> {
        self.iterator.next()
    }

    fn advance(&mut self, target: DocId) -> Result<DocId> {
        self.iterator.advance(target)
    }

    fn cost(&self) -> usize {
        0usize
    }
}

impl Scorer for MatchNoDocScorer {
    fn score(&mut self) -> Result<f32> {
        unreachable!()
    }
}

/// The abstract base class for queries.
pub trait Query: Display {
    /// Create new `Scorer` based on query.
    fn create_weight(&self, searcher: &IndexSearcher, needs_scores: bool) -> Result<Box<Weight>>;

    /// For highlight use.
    fn extract_terms(&self) -> Vec<TermQuery>;

    fn query_type(&self) -> &'static str;
}

pub trait Weight: Display {
    fn create_scorer(&self, reader: &LeafReader) -> Result<Box<Scorer>>;

    fn hash_code(&self) -> u32 {
        let key = format!("{}", self);
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish() as u32
    }

    fn query_type(&self) -> &'static str;

    /// return the actual query type for the weight
    /// it is useful when self is a wrapped weight such as `CachingWrapperWeight`
    fn actual_query_type(&self) -> &'static str {
        self.query_type()
    }

    /// Assigns the query normalization factor and boost to this.
    fn normalize(&mut self, norm: f32, boost: f32);

    /// The value for normalization of contained query clauses (e.g. sum of squared weights).
    fn value_for_normalization(&self) -> f32;

    fn needs_scores(&self) -> bool;

    fn create_batch_scorer(&self) -> Option<Box<BatchScorer>> {
        None
    }

    /// An explanation of the score computation for the named document.
    fn explain(&self, reader: &LeafReader, doc: DocId) -> Result<Explanation>;
}

pub trait BatchScorer: Send + Sync {
    fn scores(&self, _score_context: Vec<&IndexedContext>) -> Result<Vec<f32>> {
        unimplemented!()
    }
}

/// Similarity defines the components of Lucene scoring.
///
/// Expert: Scoring API.
///
/// This is a low-level API, you should only extend this API if you want to implement
/// an information retrieval *model*.  If you are instead looking for a convenient way
/// to alter Lucene's scoring, consider extending a higher-level implementation
/// such as `TFIDFSimilarity`, which implements the vector space model with this API, or
/// just tweaking the default implementation: `BM25Similarity`.
///
/// Similarity determines how Lucene weights terms, and Lucene interacts with
/// this class at both `index-time` and
///
///
/// `Indexing Time`
/// At indexing time, the indexer calls `computeNorm(FieldInvertState)`, allowing
/// the Similarity implementation to set a per-document value for the field that will
/// be later accessible via `org.apache.lucene.index.LeafReader#getNormValues(String)`.  Lucene
/// makes no assumption about what is in this norm, but it is most useful for encoding length
/// normalization information.
///
/// Implementations should carefully consider how the normalization is encoded: while
/// Lucene's `BM25Similarity` encodes a combination of index-time boost
/// and length normalization information with `SmallFloat` into a single byte, this
/// might not be suitable for all purposes.
///
/// Many formulas require the use of average document length, which can be computed via a
/// combination of `CollectionStatistics#sumTotalTermFreq()` and
/// `CollectionStatistics#maxDoc()` or `CollectionStatistics#docCount()`,
/// depending upon whether the average should reflect field sparsity.
///
/// Additional scoring factors can be stored in named
/// `NumericDocValuesField`s and accessed
/// at query-time with {@link org.apache.lucene.index.LeafReader#getNumericDocValues(String)}.
///
/// Finally, using index-time boosts (either via folding into the normalization byte or
/// via DocValues), is an inefficient way to boost the scores of different fields if the
/// boost will be the same for every document, instead the Similarity can simply take a constant
/// boost parameter *C*, and `PerFieldSimilarityWrapper` can return different
/// instances with different boosts depending upon field name.
///
/// `Query time`
/// At query-time, Queries interact with the Similarity via these steps:
/// - The {@link #computeWeight(CollectionStatistics, TermStatistics...)} method is called a
/// single time, allowing the implementation to compute any statistics (such as IDF, average
/// document length, etc) across <i>the entire collection</i>. The {@link TermStatistics} and
/// {@link CollectionStatistics} passed in already contain all of the raw statistics
/// involved, so a Similarity can freely use any combination of statistics without causing
/// any additional I/O. Lucene makes no assumption about what is stored in the returned
/// {@link Similarity.SimWeight} object. - The query normalization process occurs a single
/// time: {@link Similarity.SimWeight#getValueForNormalization()} is called for each query
/// leaf node, {@link Similarity#queryNorm(float)} is called for the top-level query, and
/// finally {@link Similarity.SimWeight#normalize(float, float)} passes down the normalization value
///       and any top-level boosts (e.g. from enclosing {@link BooleanQuery}s).
/// - For each segment in the index, the Query creates a {@link #simScorer(SimWeight,
/// org.apache.lucene.index.LeafReaderContext)} The score() method is called for each
/// matching document.
///
/// `Explanations`
/// When {@link IndexSearcher#explain(org.apache.lucene.search.Query, int)} is called, queries
/// consult the Similarity's DocScorer for an explanation of how it computed its score. The query
/// passes in a the document id and an explanation of how the frequency was computed.
///
///

pub trait Similarity: Display {
    /// Compute any collection-level weight (e.g. IDF, average document length, etc)
    /// needed for scoring a query.
    fn compute_weight(
        &self,
        collection_stats: &CollectionStatistics,
        term_stats: &[TermStatistics],
        context: Option<&KeyedContext>,
        boost: f32,
    ) -> Box<SimWeight>;

    /// Computes the normalization value for a query given the sum of the
    /// normalized weights `SimWeight#getValueForNormalization()` of
    /// each of the query terms.  This value is passed back to the
    /// weight (`SimWeight#normalize(float, float)` of each query
    /// term, to provide a hook to attempt to make scores from different
    /// queries comparable.
    /// <p>
    /// By default this is disabled (returns 1), but some
    /// implementations such as `TFIDFSimilarity` override this.
    ///
    /// @param valueForNormalization the sum of the term normalization values
    /// @return a normalization factor for query weights
    ///
    fn query_norm(&self, _value_for_normalization: f32, _context: Option<&KeyedContext>) -> f32 {
        1.0f32
    }
}

pub trait SimScorer: Send + Sync {
    /// Score a single document
    /// @param doc document id within the inverted index segment
    /// @param freq sloppy term frequency
    /// @return document's score
    fn score(&mut self, doc: DocId, freq: f32) -> Result<f32>;

    /// Computes the amount of a sloppy phrase match, based on an edit distance.
    fn compute_slop_factor(&self, distance: i32) -> f32;

    // Calculate a scoring factor based on the data in the payload.
    // fn compute_payload_factor(&self, doc: DocId, start: i32, end: i32, payload: &Payload);
}

pub trait SimWeight {
    ///  The value for normalization of contained query clauses (e.g. sum of squared weights).
    ///
    /// NOTE: a Similarity implementation might not use any query normalization at all,
    /// it's not required. However, if it wants to participate in query normalization,
    /// it can return a value here.
    ///
    fn get_value_for_normalization(&self) -> f32;

    fn normalize(&mut self, query_norm: f32, boost: f32);

    fn sim_scorer(&self, reader: &LeafReader) -> Result<Box<SimScorer>>;

    /// Explain the score for a single document
    fn explain(&self, reader: &LeafReader, doc: DocId, freq: Explanation) -> Result<Explanation> {
        Ok(Explanation::new(
            true,
            self.sim_scorer(reader)?.score(doc, freq.value())?,
            format!("score(doc={},freq={}), with freq of:", doc, freq.value()),
            vec![freq],
        ))
    }
}

pub trait SimilarityProducer: Send + Sync {
    fn create(&self, field: &str) -> Box<Similarity>;
}

/// A query rescorer interface used to re-rank the Top-K results of a previously
/// executed search.
///
pub trait Rescorer {
    /// Modifies the result of the previously executed search `TopDocs`
    /// in place based on the given `RescorerContext`
    ///
    fn rescore(
        &self,
        searcher: &IndexSearcher,
        rescore_ctx: &RescoreRequest,
        top_docs: &mut TopDocs,
    ) -> Result<()>;

    fn rescore_features(
        &self,
        searcher: &IndexSearcher,
        rescore_ctx: &RescoreRequest,
        top_docs: &mut TopDocs,
    ) -> Result<Vec<HashMap<String, VariantValue>>>;

    /// Explains how the score for the specified document was computed.
    fn explain(
        &self,
        searcher: &IndexSearcher,
        req: &RescoreRequest,
        first: Explanation,
        doc: DocId,
    ) -> Result<Explanation>;
}

pub struct RescoreRequest {
    query: Box<Query>,
    query_weight: f32,
    rescore_weight: f32,
    rescore_mode: RescoreMode,
    pub window_size: usize,
    pub rescore_movedout: bool,
}

impl RescoreRequest {
    pub fn new(
        query: Box<Query>,
        query_weight: f32,
        rescore_weight: f32,
        rescore_mode: RescoreMode,
        window_size: usize,
        rescore_movedout: bool,
    ) -> RescoreRequest {
        RescoreRequest {
            query,
            query_weight,
            rescore_weight,
            rescore_mode,
            window_size,
            rescore_movedout,
        }
    }
}

#[derive(Debug, Clone)]
pub enum RescoreMode {
    Avg,
    Max,
    Min,
    Total,
    Multiply,
}

impl RescoreMode {
    pub fn combine(&self, primary: f32, secondary: f32) -> f32 {
        match *self {
            RescoreMode::Avg => (primary + secondary) / 2.0f32,
            RescoreMode::Max => primary.max(secondary),
            RescoreMode::Min => primary.min(secondary),
            RescoreMode::Total => primary + secondary,
            RescoreMode::Multiply => primary * secondary,
        }
    }
}

impl fmt::Display for RescoreMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            RescoreMode::Avg => write!(f, "avg"),
            RescoreMode::Max => write!(f, "max"),
            RescoreMode::Min => write!(f, "min"),
            RescoreMode::Total => write!(f, "sum"),
            RescoreMode::Multiply => write!(f, "product"),
        }
    }
}

pub struct FeatureResult {
    pub extra_params: HashMap<String, VariantValue>,
}

impl FeatureResult {
    pub fn new(params: HashMap<String, VariantValue>) -> FeatureResult {
        FeatureResult {
            extra_params: params,
        }
    }
}

/// A DocIdSet contains a set of doc ids. Implementing classes must
/// only implement *#iterator* to provide access to the set.
pub trait DocIdSet: Send + Sync {
    /// Provides a `DocIdSetIterator` to access the set.
    /// This implementation can return None if there
    /// are no docs that match.
    fn iterator(&self) -> Result<Option<Box<DocIterator>>>;

    /// Optionally provides `Bits` interface for random access
    /// to matching documents.
    /// None, if this `DocIdSet` does not support random access.
    /// In contrast to #iterator(), a return value of None
    /// *does not* imply that no documents match the filter!
    /// The default implementation does not provide random access, so you
    /// only need to implement this method if your DocIdSet can
    /// guarantee random access to every docid in O(1) time without
    /// external disk access (as `Bits` interface cannot return
    /// IOError. This is generally true for bit sets
    /// like `FixedBitSet`, which return
    /// itself if they are used as `DocIdSet`.
    ///
    fn bits(&self) -> Result<Option<ImmutableBitSetRef>>;
}

#[cfg(test)]
pub mod tests {
    use super::*;

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

    pub struct MockSimpleScorer {
        iterator: Box<DocIterator>,
    }

    impl MockSimpleScorer {
        pub fn new(iterator: Box<DocIterator>) -> MockSimpleScorer {
            MockSimpleScorer { iterator }
        }
    }

    impl Scorer for MockSimpleScorer {
        fn score(&mut self) -> Result<f32> {
            Ok(self.doc_id() as f32)
        }
    }

    impl DocIterator for MockSimpleScorer {
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

    impl Weight for MockSimpleWeight {
        fn create_scorer(&self, _reader: &LeafReader) -> Result<Box<Scorer>> {
            Ok(create_mock_scorer(self.docs.clone()))
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

        fn explain(&self, _reader: &LeafReader, _doc: DocId) -> Result<Explanation> {
            unimplemented!()
        }
    }

    impl fmt::Display for MockSimpleWeight {
        fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
            unimplemented!()
        }
    }

    pub fn create_mock_scorer(docs: Vec<DocId>) -> Box<MockSimpleScorer> {
        Box::new(MockSimpleScorer::new(Box::new(MockDocIterator::new(docs))))
    }

    pub fn create_mock_weight(docs: Vec<DocId>) -> Box<MockSimpleWeight> {
        Box::new(MockSimpleWeight::new(docs))
    }

    pub fn create_mock_doc_iterator(docs: Vec<DocId>) -> Box<DocIterator> {
        Box::new(MockDocIterator::new(docs))
    }

    pub struct MockTwoPhaseScorer {
        all_doc_ids: Vec<DocId>,
        invalid_doc_ids: Vec<DocId>,
        current_doc_id: DocId,
        offset: i32,
    }

    impl Scorer for MockTwoPhaseScorer {
        fn score(&mut self) -> Result<f32> {
            Ok(self.doc_id() as f32)
        }

        fn support_two_phase(&self) -> bool {
            true
        }
    }

    impl DocIterator for MockTwoPhaseScorer {
        fn doc_id(&self) -> DocId {
            self.current_doc_id
        }

        fn next(&mut self) -> Result<DocId> {
            self.approximate_next()?;
            two_phase_next(self)
        }

        fn advance(&mut self, target: DocId) -> Result<DocId> {
            self.approximate_advance(target)?;
            two_phase_next(self)
        }

        fn cost(&self) -> usize {
            self.all_doc_ids.len()
        }

        fn matches(&mut self) -> Result<bool> {
            Ok(self.offset >= 0 && self.current_doc_id != NO_MORE_DOCS
                && !self.invalid_doc_ids.contains(&self.current_doc_id))
        }

        fn match_cost(&self) -> f32 {
            1f32
        }

        fn approximate_next(&mut self) -> Result<DocId> {
            self.offset += 1;

            if (self.offset as usize) >= self.all_doc_ids.len() {
                self.current_doc_id = NO_MORE_DOCS;
            } else {
                self.current_doc_id = self.all_doc_ids[self.offset as usize];
            }

            Ok(self.doc_id())
        }

        fn approximate_advance(&mut self, target: DocId) -> Result<DocId> {
            loop {
                let doc_id = self.approximate_next()?;
                if doc_id >= target {
                    return Ok(doc_id);
                }
            }
        }
    }

    impl MockTwoPhaseScorer {
        pub fn new(all_docs: Vec<DocId>, invalid_docs: Vec<DocId>) -> MockTwoPhaseScorer {
            MockTwoPhaseScorer {
                all_doc_ids: all_docs,
                invalid_doc_ids: invalid_docs,
                current_doc_id: -1,
                offset: -1,
            }
        }
    }

    pub fn create_mock_two_phase_scorer(
        all_docs: Vec<DocId>,
        invalid_docs: Vec<DocId>,
    ) -> Box<MockTwoPhaseScorer> {
        Box::new(MockTwoPhaseScorer::new(all_docs, invalid_docs))
    }

    #[test]
    fn test_mock_two_phase_scorer() {
        let mut scorer =
            create_mock_two_phase_scorer(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10], vec![2, 4, 5, 7, 9]);
        assert_eq!(scorer.approximate_next().unwrap(), 1);
        assert!(scorer.matches().unwrap());

        assert_eq!(scorer.approximate_next().unwrap(), 2);
        assert!(!scorer.matches().unwrap());

        assert_eq!(scorer.next().unwrap(), 3);
        assert_eq!(scorer.next().unwrap(), 6);
        assert!(scorer.matches().unwrap());

        assert_eq!(scorer.approximate_advance(7).unwrap(), 7);
        assert!(!scorer.matches().unwrap());

        assert_eq!(scorer.advance(9).unwrap(), 10);
        assert!(scorer.matches().unwrap());
    }
}
