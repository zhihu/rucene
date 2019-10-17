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

use core::search::{DocIdSet, DocIterator, NO_MORE_DOCS};
use core::util::bit_set::{FixedBitSet, ImmutableBitSet};
use core::util::DocId;
use std::sync::Arc;

pub struct BitDocIdSet<T: ImmutableBitSet> {
    set: Arc<T>,
    cost: usize,
}

impl<T: ImmutableBitSet> BitDocIdSet<T> {
    pub fn new(set: Arc<T>, cost: usize) -> BitDocIdSet<T> {
        BitDocIdSet { cost, set }
    }

    pub fn with_bits(set: Arc<T>) -> BitDocIdSet<T> {
        let cost = set.approximate_cardinality();
        BitDocIdSet { cost, set }
    }
}

impl<T: ImmutableBitSet + 'static> DocIdSet for BitDocIdSet<T> {
    type Iter = BitSetDocIterator<T>;
    fn iterator(&self) -> Result<Option<Self::Iter>> {
        Ok(Some(BitSetDocIterator::new(
            Arc::clone(&self.set),
            self.cost,
        )?))
    }

    //    fn bits(&self) -> Result<Option<ImmutableBitSetRef>> {
    //        Ok(Some(Arc::clone(&self.set)))
    //    }
}

pub struct BitSetDocIterator<T: ImmutableBitSet> {
    bits: Arc<T>,
    length: usize,
    cost: usize,
    doc: DocId,
}

impl<T: ImmutableBitSet> BitSetDocIterator<T> {
    pub fn new(bits: Arc<T>, cost: usize) -> Result<Self> {
        let length = bits.len();
        Ok(BitSetDocIterator {
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

impl<T: ImmutableBitSet> DocIterator for BitSetDocIterator<T> {
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
            self.doc = self.bits.next_set_bit(target as usize);
        }
        Ok(self.doc)
    }

    fn cost(&self) -> usize {
        self.cost
    }
}

pub struct IntArrayDocIdSet {
    docs: Arc<Vec<i32>>,
    length: usize,
}

impl IntArrayDocIdSet {
    pub fn new(docs: Vec<i32>, length: usize) -> IntArrayDocIdSet {
        assert_eq!(docs[length], NO_MORE_DOCS);

        IntArrayDocIdSet {
            docs: Arc::new(docs),
            length,
        }
    }
}

impl DocIdSet for IntArrayDocIdSet {
    type Iter = IntArrayDocIterator;
    fn iterator(&self) -> Result<Option<Self::Iter>> {
        Ok(Some(IntArrayDocIterator::new(
            Arc::clone(&self.docs),
            self.length,
        )))
    }

    //    fn bits(&self) -> Result<Option<ImmutableBitSetRef>> {
    //        Ok(None)
    //    }
}

pub struct IntArrayDocIterator {
    docs: Arc<Vec<i32>>,
    length: usize,
    i: i32,
    doc: DocId,
}

impl IntArrayDocIterator {
    pub fn new(docs: Arc<Vec<i32>>, length: usize) -> IntArrayDocIterator {
        IntArrayDocIterator {
            docs,
            length,
            i: -1,
            doc: -1,
        }
    }

    // used as stub
    pub fn empty() -> Self {
        Self::new(Arc::new(vec![NO_MORE_DOCS]), 0)
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

pub enum DocIdSetEnum {
    ShortArray(ShortArrayDocIdSet),
    IntArray(IntArrayDocIdSet),
    NotDocId(NotDocIdSet<ShortArrayDocIdSet>),
    BitDocId(BitDocIdSet<FixedBitSet>),
}

impl DocIdSet for DocIdSetEnum {
    type Iter = DocIdSetDocIterEnum;
    fn iterator(&self) -> Result<Option<Self::Iter>> {
        match self {
            DocIdSetEnum::ShortArray(s) => Ok(s.iterator()?.map(DocIdSetDocIterEnum::ShortArray)),
            DocIdSetEnum::IntArray(s) => Ok(s.iterator()?.map(DocIdSetDocIterEnum::IntArray)),
            DocIdSetEnum::NotDocId(s) => Ok(s.iterator()?.map(DocIdSetDocIterEnum::NotDocId)),
            DocIdSetEnum::BitDocId(s) => Ok(s.iterator()?.map(DocIdSetDocIterEnum::BitDocId)),
        }
    }
}

pub struct ShortArrayDocIdSet {
    docs: Arc<Vec<u16>>,
    length: usize,
}

impl ShortArrayDocIdSet {
    pub fn new(docs: Vec<u16>, length: usize) -> ShortArrayDocIdSet {
        ShortArrayDocIdSet {
            docs: Arc::new(docs),
            length,
        }
    }
}

impl DocIdSet for ShortArrayDocIdSet {
    type Iter = ShortArrayDocIterator;
    fn iterator(&self) -> Result<Option<Self::Iter>> {
        Ok(Some(ShortArrayDocIterator::new(
            self.docs.clone(),
            self.length,
        )))
    }
}

pub struct ShortArrayDocIterator {
    docs: Arc<Vec<u16>>,
    length: usize,
    i: i32,
    doc: DocId,
}

impl ShortArrayDocIterator {
    pub fn new(docs: Arc<Vec<u16>>, length: usize) -> ShortArrayDocIterator {
        ShortArrayDocIterator {
            docs,
            length,
            i: -1,
            doc: -1,
        }
    }
}

impl DocIterator for ShortArrayDocIterator {
    fn doc_id(&self) -> DocId {
        self.doc
    }

    fn next(&mut self) -> Result<DocId> {
        self.i += 1;
        if self.i as usize >= self.length {
            Ok(NO_MORE_DOCS)
        } else {
            self.doc = i32::from(unsafe { *self.docs.as_ptr().offset(self.i as isize) });
            Ok(self.doc)
        }
    }

    fn advance(&mut self, target: DocId) -> Result<DocId> {
        self.doc = if (self.i + 1) as usize >= self.length {
            NO_MORE_DOCS
        } else {
            let adv = match self.docs[(self.i + 1) as usize..self.length]
                .binary_search(&(target as u16))
            {
                Ok(x) => x,
                Err(e) => e,
            };
            self.i += (adv + 1) as i32;
            if self.i < self.length as i32 {
                i32::from(unsafe { *self.docs.as_ptr().offset(self.i as isize) })
            } else {
                NO_MORE_DOCS
            }
        };

        Ok(self.doc)
    }

    fn cost(&self) -> usize {
        self.length
    }
}

pub struct NotDocIdSet<T: DocIdSet> {
    set: T,
    max_doc: i32,
}

impl<T: DocIdSet> NotDocIdSet<T> {
    pub fn new(set: T, max_doc: i32) -> NotDocIdSet<T> {
        NotDocIdSet { set, max_doc }
    }
}

impl<T: DocIdSet> DocIdSet for NotDocIdSet<T> {
    type Iter = NotDocIterator<T::Iter>;
    fn iterator(&self) -> Result<Option<Self::Iter>> {
        match self.set.iterator()? {
            Some(iter) => Ok(Some(NotDocIterator::new(iter, self.max_doc))),
            _ => Ok(None),
        }
    }
    //    fn bits(&self) -> Result<Option<ImmutableBitSetRef>> {
    //        self.set.bits()
    //    }
}

pub struct NotDocIterator<DI: DocIterator> {
    max_doc: i32,
    doc: DocId,
    next_skipped_doc: i32,
    iterator: DI,
}

impl<DI: DocIterator> NotDocIterator<DI> {
    pub fn new(iterator: DI, max_doc: i32) -> Self {
        NotDocIterator {
            max_doc,
            doc: -1,
            next_skipped_doc: -1,
            iterator,
        }
    }
}

impl<DI: DocIterator> DocIterator for NotDocIterator<DI> {
    fn doc_id(&self) -> DocId {
        self.doc
    }

    fn next(&mut self) -> Result<DocId> {
        let adv = self.doc + 1;
        self.advance(adv)
    }

    fn advance(&mut self, target: DocId) -> Result<DocId> {
        self.doc = target;

        if self.doc > self.next_skipped_doc {
            self.next_skipped_doc = self.iterator.advance(self.doc)?;
        }

        loop {
            if self.doc >= self.max_doc {
                self.doc = NO_MORE_DOCS;
                return Ok(self.doc);
            }

            debug_assert!(self.doc <= self.next_skipped_doc);
            if self.doc != self.next_skipped_doc {
                return Ok(self.doc);
            }

            self.doc += 1;
            self.next_skipped_doc = self.iterator.next()?;
        }
    }

    fn cost(&self) -> usize {
        self.max_doc as usize
    }
}

pub enum DocIdSetDocIterEnum {
    ShortArray(ShortArrayDocIterator),
    IntArray(IntArrayDocIterator),
    NotDocId(NotDocIterator<ShortArrayDocIterator>),
    BitDocId(BitSetDocIterator<FixedBitSet>),
}

// used for empty stub
impl Default for DocIdSetDocIterEnum {
    fn default() -> Self {
        DocIdSetDocIterEnum::IntArray(IntArrayDocIterator::empty())
    }
}

impl DocIterator for DocIdSetDocIterEnum {
    fn doc_id(&self) -> DocId {
        match self {
            DocIdSetDocIterEnum::ShortArray(i) => i.doc_id(),
            DocIdSetDocIterEnum::IntArray(i) => i.doc_id(),
            DocIdSetDocIterEnum::NotDocId(i) => i.doc_id(),
            DocIdSetDocIterEnum::BitDocId(i) => i.doc_id(),
        }
    }

    fn next(&mut self) -> Result<DocId> {
        match self {
            DocIdSetDocIterEnum::ShortArray(i) => i.next(),
            DocIdSetDocIterEnum::IntArray(i) => i.next(),
            DocIdSetDocIterEnum::NotDocId(i) => i.next(),
            DocIdSetDocIterEnum::BitDocId(i) => i.next(),
        }
    }

    fn advance(&mut self, target: i32) -> Result<DocId> {
        match self {
            DocIdSetDocIterEnum::ShortArray(i) => i.advance(target),
            DocIdSetDocIterEnum::IntArray(i) => i.advance(target),
            DocIdSetDocIterEnum::NotDocId(i) => i.advance(target),
            DocIdSetDocIterEnum::BitDocId(i) => i.advance(target),
        }
    }

    fn slow_advance(&mut self, target: i32) -> Result<DocId> {
        match self {
            DocIdSetDocIterEnum::ShortArray(i) => i.slow_advance(target),
            DocIdSetDocIterEnum::IntArray(i) => i.slow_advance(target),
            DocIdSetDocIterEnum::NotDocId(i) => i.slow_advance(target),
            DocIdSetDocIterEnum::BitDocId(i) => i.slow_advance(target),
        }
    }

    fn cost(&self) -> usize {
        match self {
            DocIdSetDocIterEnum::ShortArray(i) => i.cost(),
            DocIdSetDocIterEnum::IntArray(i) => i.cost(),
            DocIdSetDocIterEnum::NotDocId(i) => i.cost(),
            DocIdSetDocIterEnum::BitDocId(i) => i.cost(),
        }
    }

    fn matches(&mut self) -> Result<bool> {
        match self {
            DocIdSetDocIterEnum::ShortArray(i) => i.matches(),
            DocIdSetDocIterEnum::IntArray(i) => i.matches(),
            DocIdSetDocIterEnum::NotDocId(i) => i.matches(),
            DocIdSetDocIterEnum::BitDocId(i) => i.matches(),
        }
    }

    fn match_cost(&self) -> f32 {
        match self {
            DocIdSetDocIterEnum::ShortArray(i) => i.match_cost(),
            DocIdSetDocIterEnum::IntArray(i) => i.match_cost(),
            DocIdSetDocIterEnum::NotDocId(i) => i.match_cost(),
            DocIdSetDocIterEnum::BitDocId(i) => i.match_cost(),
        }
    }

    fn approximate_next(&mut self) -> Result<DocId> {
        match self {
            DocIdSetDocIterEnum::ShortArray(i) => i.approximate_next(),
            DocIdSetDocIterEnum::IntArray(i) => i.approximate_next(),
            DocIdSetDocIterEnum::NotDocId(i) => i.approximate_next(),
            DocIdSetDocIterEnum::BitDocId(i) => i.approximate_next(),
        }
    }

    fn approximate_advance(&mut self, target: i32) -> Result<DocId> {
        match self {
            DocIdSetDocIterEnum::ShortArray(i) => i.approximate_advance(target),
            DocIdSetDocIterEnum::IntArray(i) => i.approximate_advance(target),
            DocIdSetDocIterEnum::NotDocId(i) => i.approximate_advance(target),
            DocIdSetDocIterEnum::BitDocId(i) => i.approximate_advance(target),
        }
    }
}
