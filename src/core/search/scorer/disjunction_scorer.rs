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

use core::search::scorer::{two_phase_next, Scorer};
use core::search::DocIterator;
use core::util::external::BinaryHeapPub;
use core::util::DocId;

use error::Result;

use core::search::scorer::disi::{DisiPriorityQueue, DisiWrapper};
use std::cmp::Ordering;
use std::f32;
use std::ptr;

pub trait DisjunctionScorer {
    type Scorer: Scorer;
    fn sub_scorers(&self) -> &DisiPriorityQueue<Self::Scorer>;

    fn sub_scorers_mut(&mut self) -> &mut DisiPriorityQueue<Self::Scorer>;

    fn two_phase_mut(
        &mut self,
    ) -> (
        &mut DisiTwoPhase<Self::Scorer>,
        &mut DisiPriorityQueue<Self::Scorer>,
    );

    fn two_phase_match_cost(&self) -> f32;

    fn get_cost(&self) -> usize;

    fn support_two_phase_iter(&self) -> bool;

    /// for each of the list of scorers which are on the current doc.
    fn foreach_sub_matches<F>(&mut self, mut f: F) -> Result<()>
    where
        F: FnMut(&mut DisiWrapper<Self::Scorer>) -> Result<()>,
    {
        let mut disi = if self.support_two_phase_iter() {
            let (two_phase, _) = self.two_phase_mut();
            unsafe { two_phase.get_sub_matches()? }
        } else {
            self.sub_scorers_mut().top_list()
        };

        loop {
            f(disi)?;
            if disi.next.is_null() {
                break;
            } else {
                unsafe { disi = &mut *disi.next };
            }
        }

        Ok(())
    }
}

macro_rules! impl_doc_iter_for_disjunction_scorer {
    ($ty:ident < $( $N:ident $(: $b0:ident $(+$b:ident)* )* ),* >) => {
       impl< $( $N $(: $b0 $(+$b)* ),* ),* > DocIterator for $ty< $( $N ),* > {
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
                    let (two_phase, queue) = self.two_phase_mut();
                    unsafe {two_phase.matches(queue)}
                } else {
                    Ok(true)
                }
            }

            fn match_cost(&self) -> f32 {
                self.two_phase_match_cost()
            }

            fn support_two_phase(&self) -> bool {
                self.support_two_phase_iter()
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
    }
}

struct DisiWrapperRefByCost<T: DocIterator> {
    disi: *mut DisiWrapper<T>,
}

impl<T: DocIterator> DisiWrapperRefByCost<T> {
    fn new(disi: *mut DisiWrapper<T>) -> Self {
        Self { disi }
    }
}

impl<T: DocIterator> Default for DisiWrapperRefByCost<T> {
    fn default() -> Self {
        Self {
            disi: ptr::null_mut(),
        }
    }
}

impl<T: DocIterator> Clone for DisiWrapperRefByCost<T> {
    fn clone(&self) -> Self {
        Self { disi: self.disi }
    }
}

impl<T: DocIterator> Ord for DisiWrapperRefByCost<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl<T: DocIterator> PartialOrd for DisiWrapperRefByCost<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // reverse order
        unsafe {
            (*other.disi)
                .match_cost()
                .partial_cmp(&(*self.disi).match_cost())
        }
    }
}

impl<T: DocIterator> Eq for DisiWrapperRefByCost<T> {}

impl<T: DocIterator> PartialEq for DisiWrapperRefByCost<T> {
    fn eq(&self, other: &Self) -> bool {
        self.disi == other.disi
    }
}

pub struct DisiTwoPhase<T: DocIterator> {
    // list of verified matches on the current doc
    verified_matches: *mut DisiWrapper<T>,
    // priority queue of approximations on the current doc that have not been verified yet
    unverified_matches: BinaryHeapPub<DisiWrapperRefByCost<T>>,

    need_scores: bool,
}

unsafe impl<T: DocIterator> Send for DisiTwoPhase<T> {}

unsafe impl<T: DocIterator> Sync for DisiTwoPhase<T> {}

impl<T: DocIterator> DisiTwoPhase<T> {
    fn new(cap: usize, need_scores: bool) -> Self {
        Self {
            verified_matches: ptr::null_mut(),
            unverified_matches: BinaryHeapPub::with_capacity(cap),
            need_scores,
        }
    }

    unsafe fn get_sub_matches(&mut self) -> Result<&mut DisiWrapper<T>> {
        for w in self.unverified_matches.inner_mut() {
            if (*w.disi).inner_mut().matches()? {
                (*w.disi).next = self.verified_matches;
                self.verified_matches = w.disi;
            }
        }
        self.unverified_matches.clear();
        Ok(&mut *self.verified_matches)
    }

    unsafe fn matches(&mut self, queue: &mut DisiPriorityQueue<T>) -> Result<bool> {
        self.verified_matches = ptr::null_mut();
        self.unverified_matches.clear();

        let mut w = queue.top_list();
        loop {
            let n = w.next;
            if !w.inner().support_two_phase() {
                // implicitly verified, move it to verified_matches
                w.next = self.verified_matches;
                self.verified_matches = w;
                if !self.need_scores {
                    return Ok(true);
                }
            } else {
                self.unverified_matches.push(DisiWrapperRefByCost::new(w));
            }
            if n.is_null() {
                break;
            } else {
                w = &mut *n;
            }
        }

        if !self.verified_matches.is_null() {
            return Ok(true);
        }

        // verify subs that have an two-phase iterator
        // least-costly ones first
        while let Some(w) = self.unverified_matches.pop() {
            if (*w.disi).matches()? {
                (*w.disi).next = ptr::null_mut();
                self.verified_matches = w.disi;
                return Ok(true);
            }
        }

        Ok(false)
    }
}

/// A Scorer for OR like queries, counterpart of `ConjunctionScorer`.
pub struct DisjunctionSumScorer<T: Scorer> {
    sub_scorers: DisiPriorityQueue<T>,
    two_phase: DisiTwoPhase<T>,
    cost: usize,
    support_two_phase: bool,
    two_phase_match_cost: f32,
}

impl<T: Scorer> DisjunctionSumScorer<T> {
    pub fn new(children: Vec<T>, needs_scores: bool) -> DisjunctionSumScorer<T> {
        assert!(children.len() > 1);

        let cost = children.iter().map(|w| w.cost()).sum();
        let support_two_phase = children.iter().any(|s| s.support_two_phase());

        let two_phase_match_cost = if support_two_phase {
            children.iter().map(|s| s.match_cost()).sum()
        } else {
            0f32
        };
        let two_phase = DisiTwoPhase::new(children.len(), needs_scores);
        DisjunctionSumScorer {
            sub_scorers: DisiPriorityQueue::new(children),
            two_phase,
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

    fn two_phase_mut(
        &mut self,
    ) -> (
        &mut DisiTwoPhase<Self::Scorer>,
        &mut DisiPriorityQueue<Self::Scorer>,
    ) {
        (&mut self.two_phase, &mut self.sub_scorers)
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

        self.foreach_sub_matches(|scorer| {
            score += scorer.inner_mut().score()?;
            Ok(())
        })?;
        Ok(score)
    }
}

impl_doc_iter_for_disjunction_scorer!(DisjunctionSumScorer<T: Scorer>);

/// The Scorer for DisjunctionMaxQuery.  The union of all documents generated by the the subquery
/// scorers is generated in document number order.  The score for each document is the maximum of
/// the scores computed by the subquery scorers that generate that document, plus
/// tieBreakerMultiplier times the sum of the scores for the other subqueries that generate the
/// document.
pub struct DisjunctionMaxScorer<T: Scorer> {
    sub_scorers: DisiPriorityQueue<T>,
    two_phase: DisiTwoPhase<T>,
    cost: usize,
    support_two_phase: bool,
    two_phase_match_cost: f32,
    tie_breaker_multiplier: f32,
}

impl<T: Scorer> DisjunctionMaxScorer<T> {
    pub fn new(
        children: Vec<T>,
        tie_breaker_multiplier: f32,
        needs_score: bool,
    ) -> DisjunctionMaxScorer<T> {
        assert!(children.len() > 1);

        let cost = children.iter().map(|w| w.cost()).sum();
        let support_two_phase = children.iter().any(|s| s.support_two_phase());

        let two_phase_match_cost = if support_two_phase {
            children.iter().map(|s| s.match_cost()).sum()
        } else {
            0f32
        };
        let two_phase = DisiTwoPhase::new(children.len(), needs_score);
        DisjunctionMaxScorer {
            sub_scorers: DisiPriorityQueue::new(children),
            two_phase,
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

        self.foreach_sub_matches(|s| {
            let sub_score = s.inner_mut().score()?;
            score_sum += sub_score;
            if sub_score > score_max {
                score_max = sub_score;
            }
            Ok(())
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

    fn two_phase_mut(
        &mut self,
    ) -> (
        &mut DisiTwoPhase<Self::Scorer>,
        &mut DisiPriorityQueue<Self::Scorer>,
    ) {
        (&mut self.two_phase, &mut self.sub_scorers)
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

impl_doc_iter_for_disjunction_scorer!(DisjunctionMaxScorer<T: Scorer>);
