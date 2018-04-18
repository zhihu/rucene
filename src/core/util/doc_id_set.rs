use error::*;

use core::search::{DocIdSet, DocIterator, NO_MORE_DOCS};
use core::util::bit_set::{BitSet, BitSetRef};
use core::util::DocId;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

pub struct BitDocIdSet {
    set: BitSetRef,
    cost: usize,
}

impl BitDocIdSet {
    pub fn new(set: Box<BitSet>, cost: usize) -> BitDocIdSet {
        BitDocIdSet {
            cost,
            set: Arc::new(Mutex::new(set)),
        }
    }

    pub fn with_bits(set: Box<BitSet>) -> BitDocIdSet {
        let cost = set.approximate_cardinality();
        BitDocIdSet {
            cost,
            set: Arc::new(Mutex::new(set)),
        }
    }
}

impl DocIdSet for BitDocIdSet {
    fn iterator(&self) -> Result<Option<Box<DocIterator>>> {
        Ok(Some(Box::new(BitSetIterator::new(
            Arc::clone(&self.set),
            self.cost,
        )?)))
    }

    fn bits(&self) -> Result<Option<BitSetRef>> {
        Ok(Some(Arc::clone(&self.set)))
    }
}

pub struct BitSetIterator {
    bits: BitSetRef,
    length: usize,
    cost: usize,
    doc: DocId,
}

impl BitSetIterator {
    pub fn new(bits: BitSetRef, cost: usize) -> Result<BitSetIterator> {
        let length = bits.lock()?.len();
        Ok(BitSetIterator {
            bits,
            length,
            cost,
            doc: -1,
        })
    }

    pub fn set_doc(&mut self, doc_id: DocId) {
        self.doc = doc_id;
    }
}

impl DocIterator for BitSetIterator {
    fn doc_id(&self) -> DocId {
        self.doc
    }

    fn next(&mut self) -> Result<DocId> {
        let next = self.doc + 1;
        self.advance(next)
    }

    fn advance(&mut self, target: DocId) -> Result<DocId> {
        if target >= self.length as i32 {
            self.doc = NO_MORE_DOCS;
        } else {
            self.doc = self.bits.lock()?.next_set_bit(target as usize);
        }
        Ok(self.doc)
    }

    fn cost(&self) -> usize {
        self.cost
    }
}

pub struct IntArrayDocIdSet {
    docs: Rc<Vec<i32>>,
    length: usize,
}

impl IntArrayDocIdSet {
    pub fn new(docs: Vec<i32>, length: usize) -> IntArrayDocIdSet {
        assert_eq!(docs[length], NO_MORE_DOCS);

        IntArrayDocIdSet {
            docs: Rc::new(docs),
            length,
        }
    }
}

impl DocIdSet for IntArrayDocIdSet {
    fn iterator(&self) -> Result<Option<Box<DocIterator>>> {
        Ok(Some(Box::new(IntArrayDocIterator::new(
            Rc::clone(&self.docs),
            self.length,
        ))))
    }

    fn bits(&self) -> Result<Option<BitSetRef>> {
        Ok(None)
    }
}

pub struct IntArrayDocIterator {
    docs: Rc<Vec<i32>>,
    length: usize,
    i: i32,
    doc: DocId,
}

impl IntArrayDocIterator {
    pub fn new(docs: Rc<Vec<i32>>, length: usize) -> IntArrayDocIterator {
        IntArrayDocIterator {
            docs,
            length,
            i: -1,
            doc: -1,
        }
    }
}

impl DocIterator for IntArrayDocIterator {
    fn doc_id(&self) -> DocId {
        self.doc
    }

    fn next(&mut self) -> Result<DocId> {
        self.i += 1;
        self.doc = self.docs[self.i as usize];
        Ok(self.doc)
    }

    fn advance(&mut self, target: DocId) -> Result<DocId> {
        let adv = match self.docs[(self.i + 1) as usize..self.length].binary_search(&target) {
            Ok(x) => x,
            Err(e) => e,
        };
        self.i += (adv + 1) as i32;
        self.doc = self.docs[self.i as usize];
        Ok(self.doc)
    }

    fn cost(&self) -> usize {
        self.length
    }
}
