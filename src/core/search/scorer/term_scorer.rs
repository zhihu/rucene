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

use core::codec::PostingIterator;
use core::search::scorer::Scorer;
use core::search::similarity::SimScorer;
use core::search::DocIterator;
use core::util::DocId;
use error::Result;

pub struct TermScorer<T: PostingIterator> {
    sim_scorer: Box<dyn SimScorer>,
    postings_iterator: T,
}

impl<T: PostingIterator> TermScorer<T> {
    pub fn new(sim_scorer: Box<dyn SimScorer>, postings_iterator: T) -> Self {
        TermScorer {
            sim_scorer,
            postings_iterator,
        }
    }

    fn freq(&self) -> i32 {
        if let Ok(f) = self.postings_iterator.freq() {
            f
        } else {
            1
        }
    }
}

impl<T: PostingIterator> Scorer for TermScorer<T> {
    fn score(&mut self) -> Result<f32> {
        let doc_id = self.doc_id();
        let freq = self.freq();
        Ok(self.sim_scorer.score(doc_id, freq as f32)?)
    }
}

impl<T: PostingIterator> DocIterator for TermScorer<T> {
    fn doc_id(&self) -> DocId {
        self.postings_iterator.doc_id()
    }

    fn next(&mut self) -> Result<DocId> {
        self.postings_iterator.next()
    }

    fn advance(&mut self, target: DocId) -> Result<DocId> {
        self.postings_iterator.advance(target)
    }

    fn cost(&self) -> usize {
        self.postings_iterator.cost()
    }
}
