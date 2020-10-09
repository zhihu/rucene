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

#[macro_use]
pub mod spans;

mod boolean_query;

pub use self::boolean_query::*;

mod boost_query;

pub use self::boost_query::*;

mod filter_query;

pub use self::filter_query::*;

mod match_all_query;

pub use self::match_all_query::*;

mod phrase_query;

pub use self::phrase_query::*;

mod point_range_query;

pub use self::point_range_query::*;

mod query_string;

pub use self::query_string::*;

mod term_query;

pub use self::term_query::*;

mod disjunction_max_query;

pub use self::disjunction_max_query::*;

mod boosting_query;

pub use self::boosting_query::*;

mod exists_query;

pub use self::exists_query::*;

use core::codec::Codec;
use core::index::reader::LeafReaderContext;
use core::search::explanation::Explanation;
use core::search::scorer::{BatchScorer, Scorer};
use core::search::searcher::SearchPlanBuilder;
use core::util::DocId;

use error::Result;
use std::any::Any;
use std::collections::hash_map::DefaultHasher;
use std::fmt::Display;
use std::hash::{Hash, Hasher};

/// The abstract base class for queries.
///
/// *NOTE:* the generic `C: Codec` here is used for `create_weight` methods. Actually
/// it should be placed as a method generic parameter, but this will restrict the `Query`
/// type be put into trait object. It's common for use to define custom `Query` type and
/// put it into composite `Query`s like [`BooleanQuery`], so we must ensure that `Query` could
/// be boxed. So for almost every case you just impl it like this:
/// ```rust,ignore
/// use rucene::core::search::query::Query;
/// use rucene::core::codec::Codec;
///
/// struct MyQuery;
///
/// impl<C: Codec> Query<C> for MyQuery {
///     ...
/// }
/// ```
///
/// here is the list of [`Query`]s that we already implemented:
/// * [`TermQuery`]
/// * [`BooleanQuery`]
/// * [`BoostQuery`]
/// * [`PhraseQuery`]
/// * [`PointRangeQuery`](point_range/struct.PointRangeQuery.html)
/// * [`ConstantScoreQuery`](match_all/struct.ConstantScoreQuery.html)
/// * [`DisjunctionMaxQuery`](disjunction/struct.DisjunctionMaxQuery.html)
/// * [`MatchAllDocsQuery`](match_all/struct.MatchAllDocsQuery.html)
///
/// See also the family of [`Span Queries`](spans/index.html)
pub trait Query<C: Codec>: Display {
    /// Create new `Scorer` based on query.
    fn create_weight(
        &self,
        searcher: &dyn SearchPlanBuilder<C>,
        needs_scores: bool,
    ) -> Result<Box<dyn Weight<C>>>;

    /// For highlight use.
    fn extract_terms(&self) -> Vec<TermQuery>;

    fn as_any(&self) -> &dyn Any;
}

/// Expert: Calculate query weights and build query scorers.
///
/// The purpose of [`Weight`] is to ensure searching does not modify a
/// [`Query`], so that a [`Query`] instance can be reused.
/// [`IndexSearcher`] dependent state of the query should reside in the [`Weight`]
///
/// `LeafReader` dependent state should reside in the [`Scorer`].
///
/// Since [`Weight`] creates [`Scorer`] instances for a given `LeafReaderContext`
/// callers must maintain the relationship between the searcher's `IndexReader`
/// and the context used to create a [`Scorer`].
///
/// A `Weight` is used in the following way:
/// - A `Weight` is constructed by a top-level query, given a `IndexSearcher`
/// `Query#crate_weight()`.
/// - The `get_value_for_normalization()` method is called on the `Weight`
/// to compute the query normalization factor `Similarity#query_norm()` of the
/// query clauses contained in the query.
/// - The query normalization factor is passed to `normalize()`. At this point the weighting is
///   complete.
/// - A `Scorer` is constructed by `create_scorer()`
pub trait Weight<C: Codec>: Display {
    fn create_scorer(&self, reader: &LeafReaderContext<'_, C>) -> Result<Option<Box<dyn Scorer>>>;

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

    fn create_batch_scorer(&self) -> Option<Box<dyn BatchScorer>> {
        None
    }

    /// An explanation of the score computation for the named document.
    fn explain(&self, reader: &LeafReaderContext<'_, C>, doc: DocId) -> Result<Explanation>;
}
