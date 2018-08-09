use core::index::LeafReader;
use core::search::disi::*;
use core::search::explanation::Explanation;
use core::search::searcher::IndexSearcher;
use core::search::term_query::TermQuery;
use core::search::{two_phase_next, DocIterator, Query, Scorer, Weight};
use core::util::DocId;
use error::ErrorKind::IllegalArgument;
use error::Result;

use std::f32;
use std::fmt;

pub struct DisjunctionSumScorer {
    sub_scorers: DisiPriorityQueue<Scorer>,
    cost: usize,
    support_two_phase: bool,
    two_phase_match_cost: f32,
}

impl DisjunctionSumScorer {
    pub fn new(children: Vec<Box<Scorer>>) -> DisjunctionSumScorer {
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

impl DisjunctionScorer for DisjunctionSumScorer {
    fn sub_scorers(&self) -> &DisiPriorityQueue<Scorer> {
        &self.sub_scorers
    }

    fn sub_scorers_mut(&mut self) -> &mut DisiPriorityQueue<Scorer> {
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

impl Scorer for DisjunctionSumScorer {
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
    fn sub_scorers(&self) -> &DisiPriorityQueue<Scorer>;

    fn sub_scorers_mut(&mut self) -> &mut DisiPriorityQueue<Scorer>;

    fn two_phase_match_cost(&self) -> f32;

    fn get_cost(&self) -> usize;

    fn support_two_phase_iter(&self) -> bool;

    /// for each of the list of scorers which are on the current doc.
    fn foreach_top_scorer<F>(&mut self, mut f: F) -> Result<()>
    where
        F: FnMut(&mut DisiWrapper<Scorer>) -> Result<bool>,
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

impl<T> DocIterator for T
where
    T: DisjunctionScorer + Scorer,
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
/// breaking increment for any additional matching subqueries. This is useful when searching for a
/// word in multiple fields with different boost factors (so that the fields cannot be
/// combined equivalently into a single search field).  We want the primary score to be the one
/// associated with the highest boost, not the sum of the field scores (as BooleanQuery would give).
/// If the query is "albino elephant" this ensures that "albino" matching one field and "elephant"
/// matching another gets a higher score than "albino" matching both fields.
/// To get this result, use both BooleanQuery and DisjunctionMaxQuery:  for each term a
/// DisjunctionMaxQuery searches for it in each field, while the set of these DisjunctionMaxQuery's
/// is combined into a BooleanQuery. The tie breaker capability allows results that include the
/// same term in multiple fields to be judged better than results that include this term in only
/// the best of those multiple fields, without confusing this with the better case of two different
/// terms in the multiple fields.
///

pub struct DisjunctionMaxQuery {
    pub disjuncts: Vec<Box<Query>>,
    /// Multiple of the non-max disjunction scores added into our final score.
    /// Non-zero values support tie-breaking.
    pub tie_breaker_multiplier: f32,
}

impl DisjunctionMaxQuery {
    pub fn build(disjuncts: Vec<Box<Query>>, tie_breaker_multiplier: f32) -> Result<Box<Query>> {
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

impl Query for DisjunctionMaxQuery {
    fn create_weight(&self, searcher: &IndexSearcher, needs_scores: bool) -> Result<Box<Weight>> {
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

    fn query_type(&self) -> &'static str {
        DISJUNCTION_MAX
    }
}

impl fmt::Display for DisjunctionMaxQuery {
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
///
pub struct DisjunctionMaxWeight {
    weights: Vec<Box<Weight>>,
    tie_breaker_multiplier: f32,
    needs_scores: bool,
}

impl DisjunctionMaxWeight {
    pub fn new(
        weights: Vec<Box<Weight>>,
        tie_breaker_multiplier: f32,
        needs_scores: bool,
    ) -> DisjunctionMaxWeight {
        DisjunctionMaxWeight {
            weights,
            tie_breaker_multiplier,
            needs_scores,
        }
    }
}

impl Weight for DisjunctionMaxWeight {
    fn create_scorer(&self, leaf_reader: &LeafReader) -> Result<Box<Scorer>> {
        let mut scorers = Vec::with_capacity(self.weights.len());
        for w in &self.weights {
            scorers.push(w.create_scorer(leaf_reader)?);
        }
        Ok(Box::new(DisjunctionMaxScorer::new(
            scorers,
            self.tie_breaker_multiplier,
        )))
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

    fn explain(&self, reader: &LeafReader, doc: DocId) -> Result<Explanation> {
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

impl fmt::Display for DisjunctionMaxWeight {
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

pub struct DisjunctionMaxScorer {
    sub_scorers: DisiPriorityQueue<Scorer>,
    cost: usize,
    support_two_phase: bool,
    two_phase_match_cost: f32,
    tie_breaker_multiplier: f32,
}

impl DisjunctionMaxScorer {
    pub fn new(children: Vec<Box<Scorer>>, tie_breaker_multiplier: f32) -> DisjunctionMaxScorer {
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

impl Scorer for DisjunctionMaxScorer {
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

impl DisjunctionScorer for DisjunctionMaxScorer {
    fn sub_scorers(&self) -> &DisiPriorityQueue<Scorer> {
        &self.sub_scorers
    }

    fn sub_scorers_mut(&mut self) -> &mut DisiPriorityQueue<Scorer> {
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

    fn create_disjunction_scorer() -> DisjunctionSumScorer {
        let s1 = create_mock_scorer(vec![1, 2, 3, 4, 5]);
        let s2 = create_mock_scorer(vec![2, 5]);
        let s3 = create_mock_scorer(vec![2, 3, 4, 5]);

        let scorers: Vec<Box<Scorer>> = vec![s1, s2, s3];

        DisjunctionSumScorer::new(scorers)
    }

    fn create_disjunction_two_phase_scorer() -> DisjunctionSumScorer {
        let s1 = create_mock_scorer(vec![1, 2, 3, 5, 6, 7, 8]);
        let s2 = create_mock_scorer(vec![2, 3, 5, 7, 8]);
        let s3 = create_mock_two_phase_scorer(vec![1, 2, 3, 4, 5, 6, 7], vec![1, 2, 4, 5]);
        let s4 = create_mock_two_phase_scorer(vec![1, 2, 3, 4, 5, 6, 7], vec![2, 4]);

        DisjunctionSumScorer::new(vec![s1, s2, s3, s4])
    }
}
