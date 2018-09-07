use core::search::posting_iterator::PostingIterator;
use core::search::DocIterator;
use core::search::Scorer;
use core::search::SimScorer;
use core::util::DocId;
use error::*;

pub struct TermScorer {
    sim_scorer: Box<SimScorer>,
    postings_iterator: Box<PostingIterator>,
    boost: f32,
}

impl TermScorer {
    pub fn new(
        sim_scorer: Box<SimScorer>,
        postings_iterator: Box<PostingIterator>,
        boost: f32,
    ) -> TermScorer {
        TermScorer {
            sim_scorer,
            postings_iterator,
            boost,
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

impl Scorer for TermScorer {
    fn score(&mut self) -> Result<f32> {
        let doc_id = self.doc_id();
        let freq = self.freq();
        self.boost;
        Ok(self.sim_scorer.score(doc_id, freq as f32)?)
    }
}

impl DocIterator for TermScorer {
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
