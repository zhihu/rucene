use error::Result;

use core::search::{two_phase_next, DocIterator, Scorer};
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
    fn support_two_phase(&self) -> bool {
        true
    }
}

impl<S: Scorer> DocIterator for MinScoreScorer<S> {
    fn doc_id(&self) -> DocId {
        self.origin.doc_id()
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
        self.origin.cost()
    }

    fn matches(&mut self) -> Result<bool> {
        Ok(self.origin.matches()? && self.score()? > self.min_score)
    }

    fn match_cost(&self) -> f32 {
        // 1000 for random constant for the score computation
        1000f32 + self.origin.match_cost()
    }

    fn approximate_next(&mut self) -> Result<DocId> {
        self.origin.approximate_next()
    }

    fn approximate_advance(&mut self, target: DocId) -> Result<DocId> {
        self.origin.approximate_advance(target)
    }
}
