use core::search::match_all::{AllDocsIterator, ConstantScoreScorer};
use core::search::Scorer;
use core::util::DocId;

use error::Result;

#[allow(dead_code)]
pub(crate) fn scorer_as_bits(max_doc: i32, scorer: Box<Scorer>) -> DocIteratorAsBits {
    DocIteratorAsBits::new(max_doc, scorer)
}

pub struct DocIteratorAsBits {
    iterator: Box<Scorer>,
    previous: DocId,
    previous_matched: bool,
    max_doc: i32,
}

impl DocIteratorAsBits {
    pub fn new(max_doc: i32, scorer: Box<Scorer>) -> DocIteratorAsBits {
        DocIteratorAsBits {
            iterator: scorer,
            previous: -1,
            previous_matched: false,
            max_doc,
        }
    }

    pub fn all_doc(max_doc: i32) -> DocIteratorAsBits {
        let scorer = Box::new(ConstantScoreScorer::new(
            1f32,
            Box::new(AllDocsIterator::new(max_doc)),
            max_doc as usize,
        ));
        DocIteratorAsBits {
            iterator: scorer,
            previous: -1,
            previous_matched: false,
            max_doc,
        }
    }

    pub fn get(&mut self, index: usize) -> Result<bool> {
        let index = index as i32;
        if index < 0 || index >= self.max_doc {
            bail!("{} is out of bounds: [0-{}]", index, self.max_doc);
        }

        if index < self.previous {
            bail!(
                "This Bits instance can only be consumed in order. Got called on [{}] while \
                 previously called on [{}].",
                index,
                self.previous
            );
        }

        if index == self.previous {
            return Ok(self.previous_matched);
        }

        self.previous = index;
        let mut doc = self.iterator.doc_id();
        if doc < index {
            doc = self.iterator.advance(index)?;
        }

        self.previous_matched = index == doc;
        Ok(self.previous_matched)
    }

    pub fn len(&self) -> usize {
        self.max_doc as usize
    }

    pub fn is_empty(&self) -> bool {
        self.max_doc <= 0
    }
}
