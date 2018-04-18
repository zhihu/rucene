use core::search::DocIterator;
use core::search::Scorer;
use core::search::SimScorer;
use core::util::DocId;
use error::*;

pub struct TermScorer {
    sim_scorer: Box<SimScorer>,
    doc_iterator: Box<DocIterator>,
    boost: f32,
}

impl TermScorer {
    pub fn new(
        sim_scorer: Box<SimScorer>,
        doc_iterator: Box<DocIterator>,
        boost: f32,
    ) -> TermScorer {
        TermScorer {
            sim_scorer,
            doc_iterator,
            boost,
        }
    }

    fn freq(&self) -> f32 {
        1.0
    }
}

impl Scorer for TermScorer {
    fn score(&mut self) -> Result<f32> {
        let doc_id = self.doc_id();
        let freq = self.freq();
        Ok(self.boost * self.sim_scorer.score(doc_id, freq)?)
    }
}

impl DocIterator for TermScorer {
    fn doc_id(&self) -> DocId {
        self.doc_iterator.doc_id()
    }

    fn next(&mut self) -> Result<DocId> {
        self.doc_iterator.next()
    }

    fn advance(&mut self, target: DocId) -> Result<DocId> {
        self.doc_iterator.advance(target)
    }

    fn cost(&self) -> usize {
        self.doc_iterator.cost()
    }
}
