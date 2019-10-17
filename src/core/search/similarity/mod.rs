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

mod bm25_similarity;

pub use self::bm25_similarity::*;

use core::util::{DocId, KeyedContext};

use core::codec::Codec;
use core::index::reader::SearchLeafReader;
use core::search::explanation::Explanation;
use core::search::statistics::{CollectionStatistics, TermStatistics};
use error::Result;
use std::fmt::Display;

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

pub trait Similarity<C: Codec>: Display {
    /// Compute any collection-level weight (e.g. IDF, average document length, etc)
    /// needed for scoring a query.
    fn compute_weight(
        &self,
        collection_stats: &CollectionStatistics,
        term_stats: &[TermStatistics],
        context: Option<&KeyedContext>,
        boost: f32,
    ) -> Box<dyn SimWeight<C>>;

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
    fn query_norm(&self, _value_for_normalization: f32, _context: Option<&KeyedContext>) -> f32 {
        1.0f32
    }
}

/// Per-field similarity provider.
pub trait SimilarityProducer<C> {
    fn create(&self, field: &str) -> Box<dyn Similarity<C>>;
}

impl<C: Codec> SimilarityProducer<C> for Box<dyn SimilarityProducer<C>> {
    fn create(&self, field: &str) -> Box<dyn Similarity<C>> {
        (**self).create(field)
    }
}

/// API for scoring "sloppy" queries such as `TermQuery`, `SpanQuery`, `PhraseQuery`.
///
/// Frequencies are floating-point values: an approximate within-document
/// frequency adjusted for "sloppiness" by `SimScorer::compute_slop_factor`
pub trait SimScorer: Send {
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

/// Stores the weight for a query across the indexed collection.
pub trait SimWeight<C: Codec> {
    ///  The value for normalization of contained query clauses (e.g. sum of squared weights).
    ///
    /// NOTE: a Similarity implementation might not use any query normalization at all,
    /// it's not required. However, if it wants to participate in query normalization,
    /// it can return a value here.
    fn get_value_for_normalization(&self) -> f32;

    fn normalize(&mut self, query_norm: f32, boost: f32);

    fn sim_scorer(&self, reader: &SearchLeafReader<C>) -> Result<Box<dyn SimScorer>>;

    /// Explain the score for a single document
    fn explain(
        &self,
        reader: &SearchLeafReader<C>,
        doc: DocId,
        freq: Explanation,
    ) -> Result<Explanation> {
        Ok(Explanation::new(
            true,
            self.sim_scorer(reader)?.score(doc, freq.value())?,
            format!("score(doc={},freq={}), with freq of:", doc, freq.value()),
            vec![freq],
        ))
    }
}
