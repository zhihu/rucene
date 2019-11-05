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

use core::codec::Codec;
use core::index::reader::LeafReaderContext;
use core::search::explanation::Explanation;
use core::search::query::{Query, TermQuery, Weight};
use core::search::scorer::{DisjunctionMaxScorer, Scorer};
use core::search::searcher::SearchPlanBuilder;
use core::util::DocId;

use error::ErrorKind::IllegalArgument;
use error::Result;

use std::f32;
use std::fmt;

/// A query that generates the union of documents produced by its subqueries, and that scores each
/// document with the maximum score for that document as produced by any subquery, plus a tie
/// breaking increment for any additional matching subqueries.
///
/// This is useful when searching for a word in multiple fields with different boost factors (so
/// that the fields cannot be combined equivalently into a single search field).  We want the
/// primary score to be the one associated with the highest boost, not the sum of the field scores
/// (as BooleanQuery would give). If the query is "albino elephant" this ensures that "albino"
/// matching one field and "elephant" matching another gets a higher score than "albino" matching
/// both fields. To get this result, use both BooleanQuery and DisjunctionMaxQuery:  for each term a
/// DisjunctionMaxQuery searches for it in each field, while the set of these DisjunctionMaxQuery's
/// is combined into a BooleanQuery. The tie breaker capability allows results that include the
/// same term in multiple fields to be judged better than results that include this term in only
/// the best of those multiple fields, without confusing this with the better case of two different
/// terms in the multiple fields.
pub struct DisjunctionMaxQuery<C: Codec> {
    pub disjuncts: Vec<Box<dyn Query<C>>>,
    /// Multiple of the non-max disjunction scores added into our final score.
    /// Non-zero values support tie-breaking.
    pub tie_breaker_multiplier: f32,
}

impl<C: Codec> DisjunctionMaxQuery<C> {
    pub fn build(
        disjuncts: Vec<Box<dyn Query<C>>>,
        tie_breaker_multiplier: f32,
    ) -> Result<Box<dyn Query<C>>> {
        let mut disjuncts = disjuncts;
        if disjuncts.is_empty() {
            bail!(IllegalArgument(
                "DisjunctionMaxQuery: sub query should not be empty!".into()
            ))
        } else if disjuncts.len() == 1 {
            Ok(disjuncts.remove(0))
        } else {
            Ok(Box::new(DisjunctionMaxQuery {
                disjuncts,
                tie_breaker_multiplier,
            }))
        }
    }
}

pub const DISJUNCTION_MAX: &str = "dismax";

impl<C: Codec> Query<C> for DisjunctionMaxQuery<C> {
    fn create_weight(
        &self,
        searcher: &dyn SearchPlanBuilder<C>,
        needs_scores: bool,
    ) -> Result<Box<dyn Weight<C>>> {
        let mut weights = Vec::with_capacity(self.disjuncts.len());
        for q in &self.disjuncts {
            weights.push(q.create_weight(searcher, needs_scores)?);
        }

        Ok(Box::new(DisjunctionMaxWeight::new(
            weights,
            self.tie_breaker_multiplier,
            needs_scores,
        )))
    }

    fn extract_terms(&self) -> Vec<TermQuery> {
        let mut queries = Vec::new();
        for q in &self.disjuncts {
            queries.extend(q.extract_terms());
        }
        queries
    }

    fn as_any(&self) -> &dyn (::std::any::Any) {
        self
    }
}

impl<C: Codec> fmt::Display for DisjunctionMaxQuery<C> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let queries: Vec<String> = self.disjuncts.iter().map(|q| format!("{}", q)).collect();
        write!(
            f,
            "DisjunctionMaxQuery(disjunctions: {}, tie_breaker_multiplier: {})",
            queries.join(", "),
            self.tie_breaker_multiplier
        )
    }
}

/// Expert: the Weight for DisjunctionMaxQuery, used to
/// normalize, score and explain these queries.
///
/// <p>NOTE: this API and implementation is subject to
/// change suddenly in the next release.</p>
pub struct DisjunctionMaxWeight<C: Codec> {
    weights: Vec<Box<dyn Weight<C>>>,
    tie_breaker_multiplier: f32,
    needs_scores: bool,
}

impl<C: Codec> DisjunctionMaxWeight<C> {
    pub fn new(
        weights: Vec<Box<dyn Weight<C>>>,
        tie_breaker_multiplier: f32,
        needs_scores: bool,
    ) -> DisjunctionMaxWeight<C> {
        DisjunctionMaxWeight {
            weights,
            tie_breaker_multiplier,
            needs_scores,
        }
    }
}

impl<C: Codec> Weight<C> for DisjunctionMaxWeight<C> {
    fn create_scorer(
        &self,
        reader_context: &LeafReaderContext<'_, C>,
    ) -> Result<Option<Box<dyn Scorer>>> {
        let mut scorers = Vec::with_capacity(self.weights.len());
        for w in &self.weights {
            if let Some(scorer) = w.create_scorer(reader_context)? {
                scorers.push(scorer);
            }
        }
        match scorers.len() {
            0 => Ok(None),
            1 => Ok(scorers.pop()),
            _ => Ok(Some(Box::new(DisjunctionMaxScorer::new(
                scorers,
                self.tie_breaker_multiplier,
                self.needs_scores,
            )))),
        }
    }

    fn query_type(&self) -> &'static str {
        DISJUNCTION_MAX
    }

    fn normalize(&mut self, norm: f32, boost: f32) {
        for weight in &mut self.weights {
            weight.normalize(norm, boost)
        }
    }

    fn value_for_normalization(&self) -> f32 {
        let mut max_value = 0f32;
        let mut sum = 0f32;
        for weight in &self.weights {
            let sub = weight.value_for_normalization();
            sum += sub;
            max_value = max_value.max(sub);
        }
        max_value + (sum - max_value) * self.tie_breaker_multiplier
    }

    fn needs_scores(&self) -> bool {
        self.needs_scores
    }

    fn explain(&self, reader: &LeafReaderContext<'_, C>, doc: DocId) -> Result<Explanation> {
        let mut matched = false;
        let mut max = f32::NEG_INFINITY;
        let mut sum = 0.0f32;

        let mut subs: Vec<Explanation> = vec![];
        for w in &self.weights {
            let e = w.explain(reader, doc)?;
            if e.is_match() {
                matched = true;
                sum += e.value();
                max = e.value().max(max);
                subs.push(e);
            }
        }

        if matched {
            let score = max + (sum - max) * self.tie_breaker_multiplier;
            let desc = if self.tie_breaker_multiplier == 0.0f32 {
                "max of:"
            } else {
                "max plus "
            };

            Ok(Explanation::new(
                true,
                score,
                format!("{} {} times others of:", desc, self.tie_breaker_multiplier),
                subs,
            ))
        } else {
            Ok(Explanation::new(
                false,
                0.0f32,
                "No matching clause".to_string(),
                vec![],
            ))
        }
    }
}

impl<C: Codec> fmt::Display for DisjunctionMaxWeight<C> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let weights: Vec<String> = self.weights.iter().map(|q| format!("{}", q)).collect();
        write!(
            f,
            "DisjunctionMaxWeight(weights:{}, tie_breaker_multiplier:{}, needs_scores:{})",
            weights.join(", "),
            self.tie_breaker_multiplier,
            self.needs_scores
        )
    }
}
