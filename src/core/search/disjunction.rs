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
use core::index::LeafReaderContext;
use core::search::disi::*;
use core::search::explanation::Explanation;
use core::search::searcher::SearchPlanBuilder;
use core::search::TermQuery;
use core::search::{two_phase_next, DocIterator, Query, Scorer, Weight};
use core::util::DocId;
use error::ErrorKind::IllegalArgument;
use error::Result;

use std::f32;
use std::fmt;

pub struct DisjunctionSumScorer<T: Scorer> {
    sub_scorers: DisiPriorityQueue<T>,
    cost: usize,
    support_two_phase: bool,
    two_phase_match_cost: f32,
}

impl<T: Scorer> DisjunctionSumScorer<T> {
    pub fn new(children: Vec<T>) -> DisjunctionSumScorer<T> {
        assert!(children.len() > 1);

        let cost = children.iter().map(|w| w.cost()).sum();
        let support_two_phase = children.iter().any(|s| s.support_two_phase());

        let two_phase_match_cost = if support_two_phase {
            children.iter().map(|s| s.match_cost()).sum()
        } else {
            0f32
        };
        DisjunctionSumScorer {
            sub_scorers: DisiPriorityQueue::new(children),
            cost,
            support_two_phase,
            two_phase_match_cost,
        }
    }
}

impl<T: Scorer> DisjunctionScorer for DisjunctionSumScorer<T> {
    type Scorer = T;
    fn sub_scorers(&self) -> &DisiPriorityQueue<T> {
        &self.sub_scorers
    }

    fn sub_scorers_mut(&mut self) -> &mut DisiPriorityQueue<T> {
        &mut self.sub_scorers
    }

    fn two_phase_match_cost(&self) -> f32 {
        self.two_phase_match_cost
    }

    fn get_cost(&self) -> usize {
        self.cost
    }

    fn support_two_phase_iter(&self) -> bool {
        self.support_two_phase
    }
}

impl<T: Scorer> Scorer for DisjunctionSumScorer<T> {
    fn score(&mut self) -> Result<f32> {
        let mut score: f32 = 0.0;
        self.foreach_top_scorer(|scorer| {
            if scorer.matches()? {
                score += scorer.inner_mut().score()?;
            }
            Ok(true)
        })?;
        Ok(score)
    }
}

pub trait DisjunctionScorer {
    type Scorer: Scorer;
    fn sub_scorers(&self) -> &DisiPriorityQueue<Self::Scorer>;

    fn sub_scorers_mut(&mut self) -> &mut DisiPriorityQueue<Self::Scorer>;

    fn two_phase_match_cost(&self) -> f32;

    fn get_cost(&self) -> usize;

    fn support_two_phase_iter(&self) -> bool;

    /// for each of the list of scorers which are on the current doc.
    fn foreach_top_scorer<F>(&mut self, mut f: F) -> Result<()>
    where
        F: FnMut(&mut DisiWrapper<Self::Scorer>) -> Result<bool>,
    {
        let mut disi = Some(self.sub_scorers().top_list());
        while let Some(scorer) = disi {
            if !f(scorer)? {
                break;
            }
            disi = scorer.next_scorer();
        }
        Ok(())
    }
}

impl<T, S> DocIterator for T
where
    T: DisjunctionScorer<Scorer = S> + Scorer,
    S: Scorer,
{
    fn doc_id(&self) -> DocId {
        self.sub_scorers().peek().doc()
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
        self.get_cost()
    }

    fn matches(&mut self) -> Result<bool> {
        if self.support_two_phase_iter() {
            let mut matches = false;
            self.foreach_top_scorer(|scorer| {
                Ok(if scorer.matches()? {
                    matches = true;
                    false
                } else {
                    true
                })
            })?;
            Ok(matches)
        } else {
            Ok(true)
        }
    }

    fn match_cost(&self) -> f32 {
        self.two_phase_match_cost()
    }

    fn approximate_next(&mut self) -> Result<DocId> {
        let sub_scorers = self.sub_scorers_mut();
        let doc = sub_scorers.peek().doc();

        loop {
            sub_scorers.peek_mut().approximate_next()?;
            if sub_scorers.peek().doc() != doc {
                break;
            }
        }

        Ok(sub_scorers.peek().doc())
    }

    fn approximate_advance(&mut self, target: DocId) -> Result<DocId> {
        let sub_scorers = self.sub_scorers_mut();
        loop {
            sub_scorers.peek_mut().approximate_advance(target)?;
            if sub_scorers.peek().doc() >= target {
                break;
            }
        }

        Ok(sub_scorers.peek().doc())
    }
}

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

    fn as_any(&self) -> &::std::any::Any {
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

/// The Scorer for DisjunctionMaxQuery.  The union of all documents generated by the the subquery
/// scorers is generated in document number order.  The score for each document is the maximum of
/// the scores computed by the subquery scorers that generate that document, plus
/// tieBreakerMultiplier times the sum of the scores for the other subqueries that generate the
/// document.
struct DisjunctionMaxScorer<T: Scorer> {
    sub_scorers: DisiPriorityQueue<T>,
    cost: usize,
    support_two_phase: bool,
    two_phase_match_cost: f32,
    tie_breaker_multiplier: f32,
}

impl<T: Scorer> DisjunctionMaxScorer<T> {
    pub fn new(children: Vec<T>, tie_breaker_multiplier: f32) -> DisjunctionMaxScorer<T> {
        assert!(children.len() > 1);

        let cost = children.iter().map(|w| w.cost()).sum();
        let support_two_phase = children.iter().any(|s| s.support_two_phase());

        let two_phase_match_cost = if support_two_phase {
            children.iter().map(|s| s.match_cost()).sum()
        } else {
            0f32
        };
        DisjunctionMaxScorer {
            sub_scorers: DisiPriorityQueue::new(children),
            cost,
            support_two_phase,
            two_phase_match_cost,
            tie_breaker_multiplier,
        }
    }
}

impl<T: Scorer> Scorer for DisjunctionMaxScorer<T> {
    fn score(&mut self) -> Result<f32> {
        let mut score_sum = 0.0f32;
        let mut score_max = f32::NEG_INFINITY;
        self.foreach_top_scorer(|scorer| {
            if scorer.matches()? {
                let sub_score = scorer.inner_mut().score()?;
                score_sum += sub_score;
                if sub_score > score_max {
                    score_max = sub_score;
                }
            }
            Ok(true)
        })?;
        Ok(score_max + (score_sum - score_max) * self.tie_breaker_multiplier)
    }
}

impl<T: Scorer> DisjunctionScorer for DisjunctionMaxScorer<T> {
    type Scorer = T;
    fn sub_scorers(&self) -> &DisiPriorityQueue<T> {
        &self.sub_scorers
    }

    fn sub_scorers_mut(&mut self) -> &mut DisiPriorityQueue<T> {
        &mut self.sub_scorers
    }

    fn two_phase_match_cost(&self) -> f32 {
        self.two_phase_match_cost
    }

    fn get_cost(&self) -> usize {
        self.cost
    }

    fn support_two_phase_iter(&self) -> bool {
        self.support_two_phase
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::search::tests::*;
    use core::search::NO_MORE_DOCS;

    #[test]
    fn test_disjunction_iterator() {
        let mut scorer = create_disjunction_scorer();

        assert_eq!(scorer.doc_id(), -1);

        assert_eq!(scorer.next().unwrap(), 1);
        assert_eq!(scorer.doc_id(), 1);
        assert!((scorer.score().unwrap() - 1.0) < ::std::f32::EPSILON);

        assert_eq!(scorer.next().unwrap(), 2);
        assert_eq!(scorer.doc_id(), 2);
        assert!((scorer.score().unwrap() - 6.0) < ::std::f32::EPSILON);

        assert_eq!(scorer.advance(4).unwrap(), 4);
        assert_eq!(scorer.doc_id(), 4);
        assert!((scorer.score().unwrap() - 8.0) < ::std::f32::EPSILON);

        assert_eq!(scorer.next().unwrap(), 5);
        assert_eq!(scorer.doc_id(), 5);
        assert!((scorer.score().unwrap() - 15.0) < ::std::f32::EPSILON);

        assert_eq!(scorer.advance(7).unwrap(), NO_MORE_DOCS);
        assert_eq!(scorer.doc_id(), NO_MORE_DOCS);

        let mut two_phase = create_disjunction_two_phase_scorer();
        assert_eq!(two_phase.doc_id(), -1);

        assert_eq!(two_phase.next().unwrap(), 1);
        assert_eq!(two_phase.doc_id(), 1);
        assert!((two_phase.score().unwrap() - 2.0) < ::std::f32::EPSILON);

        assert_eq!(two_phase.next().unwrap(), 2);
        assert!((two_phase.score().unwrap() - 4.0) < ::std::f32::EPSILON);

        assert_eq!(two_phase.next().unwrap(), 3);
        assert!((two_phase.score().unwrap() - 12.0) < ::std::f32::EPSILON);

        assert_eq!(two_phase.next().unwrap(), 5);
        assert_eq!(two_phase.doc_id(), 5);
        assert!((two_phase.score().unwrap() - 15.0) < ::std::f32::EPSILON);
    }

    fn create_disjunction_scorer() -> DisjunctionSumScorer<MockSimpleScorer<MockDocIterator>> {
        let s1 = create_mock_scorer(vec![1, 2, 3, 4, 5]);
        let s2 = create_mock_scorer(vec![2, 5]);
        let s3 = create_mock_scorer(vec![2, 3, 4, 5]);

        let scorers = vec![s1, s2, s3];
        DisjunctionSumScorer::new(scorers)
    }

    fn create_disjunction_two_phase_scorer() -> DisjunctionSumScorer<Box<dyn Scorer>> {
        let s1 = create_mock_scorer(vec![1, 2, 3, 5, 6, 7, 8]);
        let s2 = create_mock_scorer(vec![2, 3, 5, 7, 8]);
        let s3 = create_mock_two_phase_scorer(vec![1, 2, 3, 4, 5, 6, 7], vec![1, 2, 4, 5]);
        let s4 = create_mock_two_phase_scorer(vec![1, 2, 3, 4, 5, 6, 7], vec![2, 4]);

        let scorers: Vec<Box<dyn Scorer>> =
            vec![Box::new(s1), Box::new(s2), Box::new(s3), Box::new(s4)];
        DisjunctionSumScorer::new(scorers)
    }
}
