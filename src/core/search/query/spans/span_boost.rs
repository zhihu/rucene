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

use core::codec::{Codec, CodecPostingIterator};
use core::doc::Term;
use core::index::reader::{LeafReaderContext, SearchLeafReader};
use core::search::explanation::Explanation;
use core::search::query::spans::{
    build_sim_weight, PostingsFlag, SpanGapQuery, SpanGapWeight, SpanNearQuery, SpanNearWeight,
    SpanOrQuery, SpanOrWeight, SpanQuery, SpanQueryEnum, SpanTermQuery, SpanTermWeight, SpanWeight,
    SpanWeightEnum, SpansEnum,
};
use core::search::searcher::SearchPlanBuilder;
use core::search::{
    query::Query, query::TermQuery, query::Weight, scorer::Scorer, similarity::SimScorer,
    similarity::SimWeight,
};
use core::util::{DocId, KeyedContext};

use error::Result;

use std::any::Any;
use std::f32;
use std::fmt;

const SPAN_BOOST_QUERY: &str = "span_boost";

/// counterpart of `BoostQuery` for spans
pub struct SpanBoostQuery {
    pub query: SpanBoostQueryEnum,
    boost: f32,
}

impl SpanBoostQuery {
    pub fn new(query: SpanBoostQueryEnum, boost: f32) -> Self {
        assert!((boost - 1.0f32).abs() > f32::EPSILON);
        SpanBoostQuery { query, boost }
    }

    fn span_boost_weight<C: Codec>(
        &self,
        searcher: &dyn SearchPlanBuilder<C>,
    ) -> Result<SpanBoostWeight<C>> {
        SpanBoostWeight::new(self, searcher, true)
    }

    pub fn boost(&self) -> f32 {
        self.boost
    }
}

impl<C: Codec> SpanQuery<C> for SpanBoostQuery {
    type Weight = SpanWeightEnum<C>;

    fn span_weight(
        &self,
        searcher: &dyn SearchPlanBuilder<C>,
        needs_scores: bool,
    ) -> Result<Self::Weight> {
        if !needs_scores {
            Ok(SpanWeightEnum::from(
                self.query.span_weight(searcher, needs_scores)?,
            ))
        } else {
            Ok(SpanWeightEnum::Boost(self.span_boost_weight(searcher)?))
        }
    }

    fn field(&self) -> &str {
        SpanQuery::<C>::field(&self.query)
    }

    fn ctx(&self) -> Option<KeyedContext> {
        SpanQuery::<C>::ctx(&self.query)
    }
}

impl<C: Codec> Query<C> for SpanBoostQuery {
    fn create_weight(
        &self,
        searcher: &dyn SearchPlanBuilder<C>,
        needs_scores: bool,
    ) -> Result<Box<dyn Weight<C>>> {
        if !needs_scores {
            self.query.create_weight(searcher, needs_scores)
        } else {
            let weight = self.span_boost_weight(searcher)?;
            Ok(Box::new(weight))
        }
    }

    fn extract_terms(&self) -> Vec<TermQuery> {
        Query::<C>::extract_terms(&self.query)
    }

    fn as_any(&self) -> &dyn (::std::any::Any) {
        self
    }
}

impl fmt::Display for SpanBoostQuery {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SpanBoostQuery(query: {}, boost: {})",
            &self.query, self.boost
        )
    }
}

pub enum SpanBoostQueryEnum {
    Term(SpanTermQuery),
    Gap(SpanGapQuery),
    Or(SpanOrQuery),
    Near(SpanNearQuery),
}

impl SpanBoostQueryEnum {
    pub fn into_span_query(self) -> SpanQueryEnum {
        match self {
            SpanBoostQueryEnum::Term(q) => SpanQueryEnum::Term(q),
            SpanBoostQueryEnum::Gap(q) => SpanQueryEnum::Gap(q),
            SpanBoostQueryEnum::Or(q) => SpanQueryEnum::Or(q),
            SpanBoostQueryEnum::Near(q) => SpanQueryEnum::Near(q),
        }
    }
}

impl<C: Codec> SpanQuery<C> for SpanBoostQueryEnum {
    type Weight = SpanBoostWeightEnum<C>;

    fn span_weight(
        &self,
        searcher: &dyn SearchPlanBuilder<C>,
        needs_scores: bool,
    ) -> Result<Self::Weight> {
        let weight = match self {
            SpanBoostQueryEnum::Term(q) => {
                SpanBoostWeightEnum::Term(q.span_weight(searcher, needs_scores)?)
            }
            SpanBoostQueryEnum::Gap(q) => {
                SpanBoostWeightEnum::Gap(q.span_weight(searcher, needs_scores)?)
            }
            SpanBoostQueryEnum::Or(q) => {
                SpanBoostWeightEnum::Or(q.span_weight(searcher, needs_scores)?)
            }
            SpanBoostQueryEnum::Near(q) => {
                SpanBoostWeightEnum::Near(q.span_weight(searcher, needs_scores)?)
            }
        };
        Ok(weight)
    }

    fn field(&self) -> &str {
        match self {
            SpanBoostQueryEnum::Term(q) => SpanQuery::<C>::field(q),
            SpanBoostQueryEnum::Gap(q) => SpanQuery::<C>::field(q),
            SpanBoostQueryEnum::Or(q) => SpanQuery::<C>::field(q),
            SpanBoostQueryEnum::Near(q) => SpanQuery::<C>::field(q),
        }
    }

    fn ctx(&self) -> Option<KeyedContext> {
        match self {
            SpanBoostQueryEnum::Term(q) => SpanQuery::<C>::ctx(q),
            SpanBoostQueryEnum::Gap(q) => SpanQuery::<C>::ctx(q),
            SpanBoostQueryEnum::Or(q) => SpanQuery::<C>::ctx(q),
            SpanBoostQueryEnum::Near(q) => SpanQuery::<C>::ctx(q),
        }
    }
}

impl<C: Codec> Query<C> for SpanBoostQueryEnum {
    fn create_weight(
        &self,
        searcher: &dyn SearchPlanBuilder<C>,
        needs_scores: bool,
    ) -> Result<Box<dyn Weight<C>>> {
        match self {
            SpanBoostQueryEnum::Term(q) => q.create_weight(searcher, needs_scores),
            SpanBoostQueryEnum::Gap(q) => q.create_weight(searcher, needs_scores),
            SpanBoostQueryEnum::Or(q) => q.create_weight(searcher, needs_scores),
            SpanBoostQueryEnum::Near(q) => q.create_weight(searcher, needs_scores),
        }
    }

    fn extract_terms(&self) -> Vec<TermQuery> {
        match self {
            SpanBoostQueryEnum::Term(q) => Query::<C>::extract_terms(q),
            SpanBoostQueryEnum::Gap(q) => Query::<C>::extract_terms(q),
            SpanBoostQueryEnum::Or(q) => Query::<C>::extract_terms(q),
            SpanBoostQueryEnum::Near(q) => Query::<C>::extract_terms(q),
        }
    }

    fn as_any(&self) -> &dyn Any {
        match self {
            SpanBoostQueryEnum::Term(q) => Query::<C>::as_any(q),
            SpanBoostQueryEnum::Gap(q) => Query::<C>::as_any(q),
            SpanBoostQueryEnum::Or(q) => Query::<C>::as_any(q),
            SpanBoostQueryEnum::Near(q) => Query::<C>::as_any(q),
        }
    }
}

impl fmt::Display for SpanBoostQueryEnum {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SpanBoostQueryEnum::Term(q) => write!(f, "SpanBoostQueryEnum({})", q),
            SpanBoostQueryEnum::Gap(q) => write!(f, "SpanBoostQueryEnum({})", q),
            SpanBoostQueryEnum::Or(q) => write!(f, "SpanBoostQueryEnum({})", q),
            SpanBoostQueryEnum::Near(q) => write!(f, "SpanBoostQueryEnum({})", q),
        }
    }
}

pub struct SpanBoostWeight<C: Codec> {
    sim_weight: Option<Box<dyn SimWeight<C>>>,
    weight: SpanBoostWeightEnum<C>,
    boost: f32,
}

impl<C: Codec> SpanBoostWeight<C> {
    pub fn new(
        query: &SpanBoostQuery,
        searcher: &dyn SearchPlanBuilder<C>,
        needs_scores: bool,
    ) -> Result<Self> {
        let mut weight = query.query.span_weight(searcher, needs_scores)?;
        let mut terms = Vec::new();
        weight.extract_term_keys(&mut terms);
        let sim_weight = build_sim_weight(SpanQuery::<C>::field(query), searcher, terms, None)?;
        weight.do_normalize(1.0, query.boost);
        Ok(SpanBoostWeight {
            sim_weight,
            weight,
            boost: query.boost,
        })
    }
}

impl<C: Codec> SpanWeight<C> for SpanBoostWeight<C> {
    fn sim_weight(&self) -> Option<&dyn SimWeight<C>> {
        self.sim_weight.as_ref().map(|x| &**x)
    }

    fn sim_weight_mut(&mut self) -> Option<&mut dyn SimWeight<C>> {
        if let Some(ref mut sim_weight) = self.sim_weight {
            Some(sim_weight.as_mut())
        } else {
            None
        }
    }

    fn get_spans(
        &self,
        reader: &LeafReaderContext<'_, C>,
        required_postings: &PostingsFlag,
    ) -> Result<Option<SpansEnum<CodecPostingIterator<C>>>> {
        self.weight.get_spans(reader, required_postings)
    }

    fn extract_term_keys(&self, terms: &mut Vec<Term>) {
        self.weight.extract_term_keys(terms)
    }
}

impl<C: Codec> Weight<C> for SpanBoostWeight<C> {
    fn create_scorer(
        &self,
        leaf_ctx: &LeafReaderContext<'_, C>,
    ) -> Result<Option<Box<dyn Scorer>>> {
        self.do_create_scorer(leaf_ctx)
    }

    fn query_type(&self) -> &'static str {
        SPAN_BOOST_QUERY
    }

    fn normalize(&mut self, norm: f32, boost: f32) {
        let b = boost * self.boost;
        self.do_normalize(norm, b)
    }

    fn value_for_normalization(&self) -> f32 {
        self.do_value_for_normalization()
    }

    fn needs_scores(&self) -> bool {
        true
    }

    fn explain(&self, reader: &LeafReaderContext<'_, C>, doc: DocId) -> Result<Explanation> {
        self.weight.explain_span(reader, doc)
    }
}

impl<C: Codec> fmt::Display for SpanBoostWeight<C> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SpanBoostWeight(weight: {}, boost: {})",
            &self.weight, self.boost
        )
    }
}

pub enum SpanBoostWeightEnum<C: Codec> {
    Term(SpanTermWeight<C>),
    Gap(SpanGapWeight<C>),
    Or(SpanOrWeight<C>),
    Near(SpanNearWeight<C>),
}

impl<C: Codec> SpanWeight<C> for SpanBoostWeightEnum<C> {
    fn sim_weight(&self) -> Option<&dyn SimWeight<C>> {
        match self {
            SpanBoostWeightEnum::Term(w) => w.sim_weight(),
            SpanBoostWeightEnum::Gap(w) => w.sim_weight(),
            SpanBoostWeightEnum::Or(w) => w.sim_weight(),
            SpanBoostWeightEnum::Near(w) => w.sim_weight(),
        }
    }

    fn sim_weight_mut(&mut self) -> Option<&mut dyn SimWeight<C>> {
        match self {
            SpanBoostWeightEnum::Term(w) => w.sim_weight_mut(),
            SpanBoostWeightEnum::Gap(w) => w.sim_weight_mut(),
            SpanBoostWeightEnum::Or(w) => w.sim_weight_mut(),
            SpanBoostWeightEnum::Near(w) => w.sim_weight_mut(),
        }
    }

    fn get_spans(
        &self,
        reader: &LeafReaderContext<'_, C>,
        required_postings: &PostingsFlag,
    ) -> Result<Option<SpansEnum<CodecPostingIterator<C>>>> {
        match self {
            SpanBoostWeightEnum::Term(w) => w.get_spans(reader, required_postings),
            SpanBoostWeightEnum::Gap(w) => w.get_spans(reader, required_postings),
            SpanBoostWeightEnum::Or(w) => w.get_spans(reader, required_postings),
            SpanBoostWeightEnum::Near(w) => w.get_spans(reader, required_postings),
        }
    }

    fn extract_term_keys(&self, terms: &mut Vec<Term>) {
        match self {
            SpanBoostWeightEnum::Term(w) => w.extract_term_keys(terms),
            SpanBoostWeightEnum::Gap(w) => w.extract_term_keys(terms),
            SpanBoostWeightEnum::Or(w) => w.extract_term_keys(terms),
            SpanBoostWeightEnum::Near(w) => w.extract_term_keys(terms),
        }
    }

    fn do_create_scorer(&self, ctx: &LeafReaderContext<'_, C>) -> Result<Option<Box<dyn Scorer>>> {
        match self {
            SpanBoostWeightEnum::Term(w) => w.do_create_scorer(ctx),
            SpanBoostWeightEnum::Gap(w) => w.do_create_scorer(ctx),
            SpanBoostWeightEnum::Or(w) => w.do_create_scorer(ctx),
            SpanBoostWeightEnum::Near(w) => w.do_create_scorer(ctx),
        }
    }

    fn do_value_for_normalization(&self) -> f32 {
        match self {
            SpanBoostWeightEnum::Term(w) => w.do_value_for_normalization(),
            SpanBoostWeightEnum::Gap(w) => w.do_value_for_normalization(),
            SpanBoostWeightEnum::Or(w) => w.do_value_for_normalization(),
            SpanBoostWeightEnum::Near(w) => w.do_value_for_normalization(),
        }
    }

    fn do_normalize(&mut self, query_norm: f32, boost: f32) {
        match self {
            SpanBoostWeightEnum::Term(w) => w.do_normalize(query_norm, boost),
            SpanBoostWeightEnum::Gap(w) => w.do_normalize(query_norm, boost),
            SpanBoostWeightEnum::Or(w) => w.do_normalize(query_norm, boost),
            SpanBoostWeightEnum::Near(w) => w.do_normalize(query_norm, boost),
        }
    }

    fn sim_scorer(&self, reader: &SearchLeafReader<C>) -> Result<Option<Box<dyn SimScorer>>> {
        match self {
            SpanBoostWeightEnum::Term(w) => w.sim_scorer(reader),
            SpanBoostWeightEnum::Gap(w) => w.sim_scorer(reader),
            SpanBoostWeightEnum::Or(w) => w.sim_scorer(reader),
            SpanBoostWeightEnum::Near(w) => w.sim_scorer(reader),
        }
    }

    fn explain_span(&self, reader: &LeafReaderContext<'_, C>, doc: i32) -> Result<Explanation> {
        match self {
            SpanBoostWeightEnum::Term(w) => w.explain_span(reader, doc),
            SpanBoostWeightEnum::Gap(w) => w.explain_span(reader, doc),
            SpanBoostWeightEnum::Or(w) => w.explain_span(reader, doc),
            SpanBoostWeightEnum::Near(w) => w.explain_span(reader, doc),
        }
    }
}

impl<C: Codec> Weight<C> for SpanBoostWeightEnum<C> {
    fn create_scorer(
        &self,
        leaf_reader: &LeafReaderContext<'_, C>,
    ) -> Result<Option<Box<dyn Scorer>>> {
        match self {
            SpanBoostWeightEnum::Term(w) => w.create_scorer(leaf_reader),
            SpanBoostWeightEnum::Gap(w) => w.create_scorer(leaf_reader),
            SpanBoostWeightEnum::Or(w) => w.create_scorer(leaf_reader),
            SpanBoostWeightEnum::Near(w) => w.create_scorer(leaf_reader),
        }
    }

    fn hash_code(&self) -> u32 {
        match self {
            SpanBoostWeightEnum::Term(w) => w.hash_code(),
            SpanBoostWeightEnum::Gap(w) => w.hash_code(),
            SpanBoostWeightEnum::Or(w) => w.hash_code(),
            SpanBoostWeightEnum::Near(w) => w.hash_code(),
        }
    }

    fn query_type(&self) -> &'static str {
        match self {
            SpanBoostWeightEnum::Term(w) => w.query_type(),
            SpanBoostWeightEnum::Gap(w) => w.query_type(),
            SpanBoostWeightEnum::Or(w) => w.query_type(),
            SpanBoostWeightEnum::Near(w) => w.query_type(),
        }
    }

    fn actual_query_type(&self) -> &'static str {
        match self {
            SpanBoostWeightEnum::Term(w) => w.actual_query_type(),
            SpanBoostWeightEnum::Gap(w) => w.actual_query_type(),
            SpanBoostWeightEnum::Or(w) => w.actual_query_type(),
            SpanBoostWeightEnum::Near(w) => w.actual_query_type(),
        }
    }

    fn normalize(&mut self, norm: f32, boost: f32) {
        match self {
            SpanBoostWeightEnum::Term(w) => w.normalize(norm, boost),
            SpanBoostWeightEnum::Gap(w) => w.normalize(norm, boost),
            SpanBoostWeightEnum::Or(w) => w.normalize(norm, boost),
            SpanBoostWeightEnum::Near(w) => w.normalize(norm, boost),
        }
    }

    fn value_for_normalization(&self) -> f32 {
        match self {
            SpanBoostWeightEnum::Term(w) => w.value_for_normalization(),
            SpanBoostWeightEnum::Gap(w) => w.value_for_normalization(),
            SpanBoostWeightEnum::Or(w) => w.value_for_normalization(),
            SpanBoostWeightEnum::Near(w) => w.value_for_normalization(),
        }
    }

    fn needs_scores(&self) -> bool {
        match self {
            SpanBoostWeightEnum::Term(w) => w.needs_scores(),
            SpanBoostWeightEnum::Gap(w) => w.needs_scores(),
            SpanBoostWeightEnum::Or(w) => w.needs_scores(),
            SpanBoostWeightEnum::Near(w) => w.needs_scores(),
        }
    }

    fn explain(&self, reader: &LeafReaderContext<'_, C>, doc: DocId) -> Result<Explanation> {
        match self {
            SpanBoostWeightEnum::Term(w) => w.explain(reader, doc),
            SpanBoostWeightEnum::Gap(w) => w.explain(reader, doc),
            SpanBoostWeightEnum::Or(w) => w.explain(reader, doc),
            SpanBoostWeightEnum::Near(w) => w.explain(reader, doc),
        }
    }
}

impl<C: Codec> fmt::Display for SpanBoostWeightEnum<C> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SpanBoostWeightEnum::Term(w) => write!(f, "SpanBoostWeightEnum({})", w),
            SpanBoostWeightEnum::Gap(w) => write!(f, "SpanBoostWeightEnum({})", w),
            SpanBoostWeightEnum::Or(w) => write!(f, "SpanBoostWeightEnum({})", w),
            SpanBoostWeightEnum::Near(w) => write!(f, "SpanBoostWeightEnum({})", w),
        }
    }
}
