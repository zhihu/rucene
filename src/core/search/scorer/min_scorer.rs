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

use error::Result;

use core::search::scorer::Scorer;
use core::search::DocIterator;
use core::util::DocId;

// currently directory merge `ScoreCachingWrappingScorer` into this class
pub struct MinScoreScorer<S: Scorer> {
    origin: S,
    min_score: f32,
    // cache these two fields to avoid calculate score twice
    cur_doc: DocId,
    cur_score: f32,
}

impl<S: Scorer> MinScoreScorer<S> {
    pub fn new(origin: S, min_score: f32) -> Self {
        MinScoreScorer {
            origin,
            min_score,
            cur_doc: -1,
            cur_score: 0f32,
        }
    }
}

impl<S: Scorer> Scorer for MinScoreScorer<S> {
    fn score(&mut self) -> Result<f32> {
        let doc = self.origin.doc_id();
        if doc != self.cur_doc {
            self.cur_score = self.origin.score()?;
            self.cur_doc = doc;
        }
        Ok(self.cur_score)
    }
}

impl<S: Scorer> DocIterator for MinScoreScorer<S> {
    fn doc_id(&self) -> DocId {
        self.origin.doc_id()
    }

    fn next(&mut self) -> Result<DocId> {
        self.approximate_next()
    }

    fn advance(&mut self, target: DocId) -> Result<DocId> {
        self.approximate_advance(target)
    }

    fn cost(&self) -> usize {
        self.origin.cost()
    }

    fn matches(&mut self) -> Result<bool> {
        Ok(self.origin.matches()? && self.score()? > self.min_score)
    }

    fn approximate_next(&mut self) -> Result<DocId> {
        self.origin.approximate_next()
    }

    fn approximate_advance(&mut self, target: DocId) -> Result<DocId> {
        self.origin.approximate_advance(target)
    }
}
