use core::search::{two_phase_next, DocIterator, Scorer, NO_MORE_DOCS};
use core::util::DocId;
use error::Result;

use std::cell::RefCell;
use std::cmp::{Ord, Ordering};
use std::collections::binary_heap::Iter;
use std::collections::BinaryHeap;

#[derive(Eq)]
struct ScorerWrapper {
    pub scorer: RefCell<Box<Scorer>>,
    pub doc: DocId,
    pub matches: Option<bool>,
}

impl ScorerWrapper {
    fn new(scorer: Box<Scorer>) -> ScorerWrapper {
        ScorerWrapper {
            scorer: RefCell::new(scorer),
            doc: -1,
            matches: None,
        }
    }

    #[allow(dead_code)]
    fn cost(&self) -> usize {
        self.scorer.borrow().cost()
    }

    fn scorer(&mut self) -> &mut Scorer {
        self.scorer.get_mut().as_mut()
    }

    fn set_doc(&mut self, doc: DocId) {
        if self.doc != doc {
            self.matches = None;
        }
        self.doc = doc;
    }
}

impl Scorer for ScorerWrapper {
    fn score(&mut self) -> Result<f32> {
        self.scorer().score()
    }

    fn support_two_phase(&self) -> bool {
        self.scorer.borrow().support_two_phase()
    }
}

impl DocIterator for ScorerWrapper {
    fn doc_id(&self) -> DocId {
        self.scorer.borrow().doc_id()
    }

    fn next(&mut self) -> Result<DocId> {
        let doc_id = self.scorer().next()?;
        self.set_doc(doc_id);
        Ok(doc_id)
    }

    fn advance(&mut self, target: DocId) -> Result<DocId> {
        let doc_id = self.scorer().advance(target)?;
        self.set_doc(doc_id);
        Ok(doc_id)
    }

    fn matches(&mut self) -> Result<bool> {
        if self.matches.is_none() {
            self.matches = Some(self.scorer().matches()?);
        }

        Ok(self.matches.unwrap())
    }

    fn match_cost(&self) -> f32 {
        self.scorer.borrow().match_cost()
    }

    fn approximate_next(&mut self) -> Result<DocId> {
        let doc_id = self.scorer().approximate_next()?;
        self.set_doc(doc_id);
        Ok(doc_id)
    }

    fn approximate_advance(&mut self, target: DocId) -> Result<DocId> {
        let doc_id = self.scorer().approximate_advance(target)?;
        self.set_doc(doc_id);
        Ok(doc_id)
    }

    fn cost(&self) -> usize {
        self.scorer.borrow().cost()
    }
}

impl Ord for ScorerWrapper {
    fn cmp(&self, other: &Self) -> Ordering {
        self.scorer
            .borrow()
            .doc_id()
            .cmp(&other.scorer.borrow().doc_id())
            .reverse()
    }
}

impl PartialEq for ScorerWrapper {
    fn eq(&self, other: &Self) -> bool {
        self.scorer.borrow().doc_id() == other.scorer.borrow().doc_id()
    }
}

impl PartialOrd for ScorerWrapper {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

struct ScorerPriorityQueue(BinaryHeap<ScorerWrapper>);

impl ScorerPriorityQueue {
    fn new() -> ScorerPriorityQueue {
        ScorerPriorityQueue(BinaryHeap::new())
    }

    fn pop(&mut self) -> ScorerWrapper {
        self.0.pop().unwrap()
    }

    fn push(&mut self, wrapper: ScorerWrapper) {
        self.0.push(wrapper);
    }

    fn push_all(&mut self, wrapper: Vec<ScorerWrapper>) {
        for w in wrapper {
            self.push(w);
        }
    }

    fn peek(&self) -> &ScorerWrapper {
        self.0.peek().unwrap()
    }

    #[allow(dead_code)]
    fn iter(&self) -> Iter<ScorerWrapper> {
        self.0.iter()
    }

    #[allow(dead_code)]
    fn len(&self) -> usize {
        self.0.len()
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

pub struct DisjunctionScorer {
    sub_scorers: ScorerPriorityQueue,
    cost: usize,
    support_two_phase: bool,
    two_phase_match_cost: f32,
}

impl DisjunctionScorer {
    pub fn new(children: Vec<Box<Scorer>>) -> DisjunctionScorer {
        assert!(children.len() > 1);

        let cost = children.iter().map(|w| w.cost()).sum();
        let mut support_two_phase = false;
        // let mut sum_match_cost = 0f32;
        // let mut sum_approx_cost = 0;
        for scorer in &children {
            // let cost_weight = scorer.cost().max(1);
            // sum_approx_cost += cost_weight;
            if scorer.support_two_phase() {
                support_two_phase = true;
                // sum_match_cost += scorer.match_cost() * cost_weight as f32;
            }
        }

        // let match_cost = sum_match_cost / (sum_approx_cost as f32);

        let two_phase_match_cost = if support_two_phase {
            children.iter().map(|s| s.match_cost()).sum()
        } else {
            0f32
        };
        let mut sub_scorers = ScorerPriorityQueue::new();
        for scorer in children {
            let wrapper = ScorerWrapper::new(scorer);
            sub_scorers.push(wrapper);
        }

        DisjunctionScorer {
            sub_scorers,
            cost,
            support_two_phase,
            two_phase_match_cost,
        }
    }

    /// Get the list of scorers which are on the current doc.
    fn pop_top_scorers(&mut self) -> Vec<ScorerWrapper> {
        let current_doc = self.sub_scorers.peek().doc;
        debug_assert_ne!(
            current_doc, -1,
            "You should call iterator::next() first before scoring"
        );
        debug_assert_ne!(
            current_doc, NO_MORE_DOCS,
            "You should check remain docs before scoring"
        );

        let mut top_scorers: Vec<ScorerWrapper> = Vec::new();

        while !self.sub_scorers.is_empty() {
            let scorer = self.sub_scorers.pop();
            if scorer.doc == current_doc {
                top_scorers.push(scorer);
            } else {
                self.sub_scorers.push(scorer);
                break;
            }
        }
        top_scorers
    }
}

impl Scorer for DisjunctionScorer {
    fn score(&mut self) -> Result<f32> {
        let mut top_scorers = self.pop_top_scorers();
        let mut score: f32 = 0.0;

        for scorer in &mut top_scorers {
            if scorer.matches()? {
                score += scorer.score()?;
            }
        }

        self.sub_scorers.push_all(top_scorers);

        Ok(score)
    }
}

impl DocIterator for DisjunctionScorer {
    fn doc_id(&self) -> DocId {
        self.sub_scorers.peek().doc
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
        self.cost
    }

    fn matches(&mut self) -> Result<bool> {
        if self.support_two_phase {
            let mut matches = false;
            let mut top_scorers = self.pop_top_scorers();
            for scorer in &mut top_scorers {
                if scorer.matches()? {
                    matches = true;
                    break;
                }
            }

            self.sub_scorers.push_all(top_scorers);
            Ok(matches)
        } else {
            Ok(true)
        }
    }

    fn match_cost(&self) -> f32 {
        self.two_phase_match_cost
    }

    fn approximate_next(&mut self) -> Result<DocId> {
        let mut top = self.sub_scorers.pop();
        let doc = top.doc;

        loop {
            let next_doc = top.approximate_next()?;
            top.set_doc(next_doc);
            // Reinsert top to the queue
            self.sub_scorers.push(top);

            top = self.sub_scorers.pop();
            if top.doc != doc {
                break;
            }
        }

        let current_doc = top.doc;
        self.sub_scorers.push(top);

        Ok(current_doc)
    }

    fn approximate_advance(&mut self, target: DocId) -> Result<DocId> {
        let mut top = self.sub_scorers.pop();

        loop {
            top.doc = top.approximate_advance(target)?;
            self.sub_scorers.push(top);

            top = self.sub_scorers.pop();
            if top.doc >= target {
                break;
            }
        }

        let current_doc = top.doc;
        self.sub_scorers.push(top);

        Ok(current_doc)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::search::tests::*;

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

    fn create_disjunction_scorer() -> DisjunctionScorer {
        let s1 = create_mock_scorer(vec![1, 2, 3, 4, 5]);
        let s2 = create_mock_scorer(vec![2, 5]);
        let s3 = create_mock_scorer(vec![2, 3, 4, 5]);

        let scorers: Vec<Box<Scorer>> = vec![s1, s2, s3];

        DisjunctionScorer::new(scorers)
    }

    fn create_disjunction_two_phase_scorer() -> DisjunctionScorer {
        let s1 = create_mock_scorer(vec![1, 2, 3, 5, 6, 7, 8]);
        let s2 = create_mock_scorer(vec![2, 3, 5, 7, 8]);
        let s3 = create_mock_two_phase_scorer(vec![1, 2, 3, 4, 5, 6, 7], vec![1, 2, 4, 5]);
        let s4 = create_mock_two_phase_scorer(vec![1, 2, 3, 4, 5, 6, 7], vec![2, 4]);

        DisjunctionScorer::new(vec![s1, s2, s3, s4])
    }
}
