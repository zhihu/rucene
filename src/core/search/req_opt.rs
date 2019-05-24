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

use core::search::{DocIterator, Scorer};
use core::util::DocId;
use error::Result;

/// A Scorer for queries with a required part and an optional part.
/// Delays `advance()` on the optional part until a `score()` is needed.
pub struct ReqOptScorer {
    req_scorer: Box<dyn Scorer>,
    opt_scorer: Box<dyn Scorer>,
}

impl ReqOptScorer {
    pub fn new(req_scorer: Box<dyn Scorer>, opt_scorer: Box<dyn Scorer>) -> ReqOptScorer {
        ReqOptScorer {
            req_scorer,
            opt_scorer,
        }
    }
}

impl Scorer for ReqOptScorer {
    fn score(&mut self) -> Result<f32> {
        let current_doc = self.req_scorer.doc_id();
        let mut score = self.req_scorer.score()?;

        let mut opt_doc = self.opt_scorer.doc_id();
        if opt_doc < current_doc {
            opt_doc = self.opt_scorer.advance(current_doc)?;
        }

        if opt_doc == current_doc {
            score += self.opt_scorer.score()?;
        }

        Ok(score)
    }

    fn support_two_phase(&self) -> bool {
        self.req_scorer.support_two_phase()
    }
}

impl DocIterator for ReqOptScorer {
    fn doc_id(&self) -> DocId {
        self.req_scorer.doc_id()
    }

    fn next(&mut self) -> Result<DocId> {
        self.req_scorer.next()
    }

    fn advance(&mut self, target: DocId) -> Result<DocId> {
        self.req_scorer.advance(target)
    }

    fn cost(&self) -> usize {
        self.req_scorer.cost()
    }
    fn matches(&mut self) -> Result<bool> {
        self.req_scorer.matches()
    }

    fn match_cost(&self) -> f32 {
        self.req_scorer.match_cost()
    }

    fn approximate_next(&mut self) -> Result<DocId> {
        self.req_scorer.approximate_next()
    }

    fn approximate_advance(&mut self, target: DocId) -> Result<DocId> {
        self.req_scorer.approximate_advance(target)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::search::conjunction::*;
    use core::search::disjunction::*;
    use core::search::tests::*;
    use core::search::*;

    #[test]
    fn test_score() {
        let s1 = create_mock_scorer(vec![1, 2, 3, 4, 5]);
        let s2 = create_mock_scorer(vec![2, 3, 5]);
        let s3 = create_mock_scorer(vec![2, 5]);
        let s4 = create_mock_scorer(vec![3, 4, 5]);

        let conjunction_scorer: Box<dyn Scorer> = Box::new(ConjunctionScorer::new(vec![s1, s2]));
        let disjunction_scorer: Box<dyn Scorer> = Box::new(DisjunctionSumScorer::new(vec![s3, s4]));
        let mut scorer = ReqOptScorer::new(conjunction_scorer, disjunction_scorer);

        assert_eq!(scorer.doc_id(), -1);

        assert_eq!(scorer.next().unwrap(), 2);
        assert!((scorer.score().unwrap() - 6.0) < ::std::f32::EPSILON);

        assert_eq!(scorer.next().unwrap(), 3);
        assert!((scorer.score().unwrap() - 9.0) < ::std::f32::EPSILON);

        assert_eq!(scorer.next().unwrap(), 5);
        assert!((scorer.score().unwrap() - 20.0) < ::std::f32::EPSILON);

        assert_eq!(scorer.next().unwrap(), NO_MORE_DOCS);
    }
}
