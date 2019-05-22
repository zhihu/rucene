use core::search::posting_iterator::PostingIterator;
use core::search::DocIterator;
use core::search::Scorer;
use core::search::SimScorer;
use core::util::DocId;
use error::Result;

pub struct TermScorer<T: PostingIterator> {
    sim_scorer: Box<dyn SimScorer>,
    postings_iterator: T,
    boost: f32,
}

impl<T: PostingIterator> TermScorer<T> {
    pub fn new(sim_scorer: Box<dyn SimScorer>, postings_iterator: T, boost: f32) -> Self {
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

impl<T: PostingIterator> Scorer for TermScorer<T> {
    fn score(&mut self) -> Result<f32> {
        let doc_id = self.doc_id();
        let freq = self.freq();
        self.boost;
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
