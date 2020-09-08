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

use core::search::scorer::Scorer;
use core::search::{DocIterator, NO_MORE_DOCS};
use core::util::DocId;
use error::Result;
use std::intrinsics::{likely, unlikely};

/// A Scorer for queries with not queries.
pub struct ReqNotScorer {
    req_scorer: Box<dyn Scorer>,
    not_scorer: Box<dyn Scorer>,
}

impl ReqNotScorer {
    pub fn new(req_scorer: Box<dyn Scorer>, not_scorer: Box<dyn Scorer>) -> Self {
        Self {
            req_scorer,
            not_scorer,
        }
    }
}

impl Scorer for ReqNotScorer {
    fn score(&mut self) -> Result<f32> {
        self.req_scorer.score()
    }
}

impl DocIterator for ReqNotScorer {
    fn doc_id(&self) -> DocId {
        self.req_scorer.doc_id()
    }

    fn next(&mut self) -> Result<DocId> {
        while let Ok(doc) = self.req_scorer.next() {
            if unlikely(doc == NO_MORE_DOCS) {
                break;
            }
            if doc == self.not_scorer.doc_id() {
                continue;
            } else if doc < self.not_scorer.doc_id() {
                return Ok(doc);
            }
            let not_doc = self.not_scorer.advance(doc)?;
            if doc < not_doc {
                return Ok(doc);
            }
        }
        Ok(NO_MORE_DOCS)
    }

    fn advance(&mut self, target: DocId) -> Result<DocId> {
        let doc = self.req_scorer.advance(target)?;
        if likely(doc < NO_MORE_DOCS) {
            loop {
                if doc == self.not_scorer.doc_id() {
                    return self.next();
                } else if doc < self.not_scorer.doc_id() {
                    return Ok(doc);
                }
                self.not_scorer.advance(doc)?;
            }
        }
        Ok(NO_MORE_DOCS)
    }

    fn cost(&self) -> usize {
        self.req_scorer.cost()
    }

    fn matches(&mut self) -> Result<bool> {
        self.req_scorer.matches()
    }

    fn approximate_next(&mut self) -> Result<DocId> {
        while let Ok(doc) = self.req_scorer.approximate_next() {
            if unlikely(doc == NO_MORE_DOCS) {
                break;
            }
            if doc == self.not_scorer.doc_id() {
                continue;
            } else if doc < self.not_scorer.doc_id() {
                return Ok(doc);
            }
            let not_doc = self.not_scorer.approximate_advance(doc)?;
            if doc < not_doc {
                return Ok(doc);
            }
        }
        Ok(NO_MORE_DOCS)
    }

    fn approximate_advance(&mut self, target: DocId) -> Result<DocId> {
        let doc = self.req_scorer.approximate_advance(target)?;
        if likely(doc < NO_MORE_DOCS) {
            loop {
                if doc == self.not_scorer.doc_id() {
                    return self.approximate_next();
                } else if doc < self.not_scorer.doc_id() {
                    return Ok(doc);
                }
                self.not_scorer.approximate_advance(doc)?;
            }
        }
        Ok(NO_MORE_DOCS)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::search::scorer::*;
    use core::search::tests::*;

    #[test]
    fn test_next() {
        let s1 = create_mock_scorer(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
        let s2 = create_mock_scorer(vec![2, 3, 5, 7, 9, 10]);
        let s3 = create_mock_scorer(vec![2, 5]);
        let s4 = create_mock_scorer(vec![1, 4, 5]);

        let conjunction_scorer: Box<dyn Scorer> = Box::new(ConjunctionScorer::new(vec![s1, s2]));
        let disjunction_scorer: Box<dyn Scorer> =
            Box::new(DisjunctionSumScorer::new(vec![s3, s4], true, 0));
        let mut scorer = ReqNotScorer::new(conjunction_scorer, disjunction_scorer);

        assert_eq!(scorer.doc_id(), -1);

        assert_eq!(scorer.next().unwrap(), 3);
        assert_eq!(scorer.next().unwrap(), 7);
        assert_eq!(scorer.next().unwrap(), 9);

        assert_eq!(scorer.next().unwrap(), NO_MORE_DOCS);
    }

    #[test]
    fn test_advance() {
        let s1 = create_mock_scorer(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
        let s2 = create_mock_scorer(vec![2, 3, 5, 7, 9, 10]);
        let s3 = create_mock_scorer(vec![2, 5]);
        let s4 = create_mock_scorer(vec![1, 4, 5]);

        let conjunction_scorer: Box<dyn Scorer> = Box::new(ConjunctionScorer::new(vec![s1, s2]));
        let disjunction_scorer: Box<dyn Scorer> =
            Box::new(DisjunctionSumScorer::new(vec![s3, s4], true, 0));
        let mut scorer = ReqNotScorer::new(conjunction_scorer, disjunction_scorer);

        // 2, 3, 5, 7, 9
        // 1, 2, 4, 5
        assert_eq!(scorer.advance(1).unwrap(), 3);
        assert_eq!(scorer.advance(4).unwrap(), 7);
        assert_eq!(scorer.advance(8).unwrap(), 9);
        assert_eq!(scorer.advance(10).unwrap(), NO_MORE_DOCS);
    }
}
