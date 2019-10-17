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

use core::codec::points::PointValues;
use core::codec::Terms;
use core::search::{DocIterator, NO_MORE_DOCS};
use core::util::bit_set::{BitSet, FixedBitSet};
use core::util::bit_util::{BitsRequired, UnsignedShift};
use core::util::doc_id_set::{BitDocIdSet, DocIdSetEnum, IntArrayDocIdSet};
use core::util::sorter::LSBRadixSorter;
use core::util::DocId;

use error::Result;

use std::cmp::{max, min};
use std::mem;
use std::sync::Arc;

/// A builder of {@link DocIdSet}s.  At first it uses a sparse structure to gather
/// documents, and then upgrades to a non-sparse bit set once enough hits match.
///
/// To add documents, you first need to call {@link #grow} in order to reserve
/// space, and then call `BulkAdder#add(int)` on the returned
/// `BulkAdder`.
pub struct DocIdSetBuilder {
    max_doc: DocId,
    threshold: usize,
    // pkg-private for testing
    multivalued: bool,
    num_values_per_doc: f64,
    buffers: Vec<Buffer>,
    total_allocated: usize,
    bit_set: Option<FixedBitSet>,
    counter: i64,
    adder: BulkAddr,
}

impl DocIdSetBuilder {
    pub fn with_max_doc(max_doc: DocId) -> DocIdSetBuilder {
        DocIdSetBuilder::new(max_doc, -1, -1)
    }

    pub fn from_terms(max_doc: DocId, terms: &impl Terms) -> Result<DocIdSetBuilder> {
        Ok(DocIdSetBuilder::new(
            max_doc,
            terms.doc_count()?,
            terms.sum_doc_freq()?,
        ))
    }

    pub fn from_values(
        max_doc: DocId,
        values: &impl PointValues,
        field: &str,
    ) -> Result<DocIdSetBuilder> {
        Ok(DocIdSetBuilder::new(
            max_doc,
            values.doc_count(field)?,
            values.size(field)?,
        ))
    }

    fn new(max_doc: DocId, doc_count: i32, value_count: i64) -> DocIdSetBuilder {
        let num_values_per_doc = if doc_count <= 0 || value_count <= 0 {
            1f64
        } else {
            value_count as f64 / f64::from(doc_count)
        };
        assert!(num_values_per_doc >= 1f64);
        let threshold = max_doc.unsigned_shift(7usize) as usize;
        let multivalued = doc_count < 0 || i64::from(doc_count) != value_count;

        DocIdSetBuilder {
            max_doc,
            threshold,
            multivalued,
            num_values_per_doc,
            buffers: Vec::new(),
            total_allocated: 0,
            bit_set: None,
            counter: -1,
            adder: BulkAddr::Buffers,
        }
    }

    /// Add the content of the provided `DocIterator` to this builder.
    /// NOTE: if you need to build a `DocIdSet` out of a single
    /// `DocIterator`, you should rather use `RoaringDocIdSet.Builder`.
    pub fn add(&mut self, iter: &mut dyn DocIterator) -> Result<()> {
        if let Some(ref mut bit_set) = self.bit_set {
            bit_set.or(iter)?;
        } else {
            let cost = min(i32::max_value() as usize, iter.cost());
            self.grow(cost);
            for _ in 0..cost {
                let doc = iter.next()?;
                if doc == NO_MORE_DOCS {
                    return Ok(());
                }
                self.add_doc(doc);
            }
        }
        loop {
            let doc = iter.next()?;
            if doc == NO_MORE_DOCS {
                return Ok(());
            }
            self.grow(1usize);
            self.add_doc(doc);
        }
    }

    pub fn add_doc(&mut self, doc: DocId) {
        match self.adder {
            BulkAddr::Buffers => {
                assert!(!self.buffers.is_empty());
                let idx = self.buffers.len() - 1;
                let length = self.buffers[idx].length;
                self.buffers[idx].array[length] = doc;
                self.buffers[idx].length += 1;
            }
            BulkAddr::FixedBitSet => {
                assert!(!self.bit_set.is_none());
                self.bit_set.as_mut().unwrap().set(doc as usize);
            }
        }
    }

    /// Reserve space and return a `BulkAdder` object that can be used to
    /// add up to *numDocs* documents.
    pub fn grow(&mut self, num_docs: usize) {
        if self.bit_set.is_none() {
            if self.total_allocated + num_docs <= self.threshold {
                self.ensure_buffer_capacity(num_docs);
            } else {
                self.upgrade2bit_set();
                self.counter += num_docs as i64;
            }
        } else {
            self.counter += num_docs as i64;
        }
    }

    fn ensure_buffer_capacity(&mut self, num_docs: usize) {
        if self.buffers.is_empty() {
            let cap = self.additional_capacity(num_docs);
            self.add_buffer(cap);
            return;
        }
        let current_idx = self.buffers.len() - 1;
        if self.buffers[current_idx].length >= num_docs + self.buffers[current_idx].array.len() {
            // current buffer is large enough
            return;
        }
        if self.buffers[current_idx].length
            < (self.buffers[current_idx].array.len() - self.buffers[current_idx].array.len())
                >> 3usize
        {
            let cap = self.additional_capacity(num_docs);
            self.grow_buffer(current_idx, cap);
        } else {
            let cap = self.additional_capacity(num_docs);
            self.add_buffer(cap);
        }
    }

    fn additional_capacity(&mut self, num_docs: usize) -> usize {
        // exponential growth: the new array has a size equal to the sum of what
        // has been allocated so far
        let mut c = self.total_allocated;
        // but is also >= numDocs + 1 so that we can store the next batch of docs
        // (plus an empty slot so that we are more likely to reuse the array in build())
        c = max(num_docs + 1, c);
        // avoid cold starts
        c = max(32usize, c);
        // do not go beyond the threshold
        c = min(self.threshold - self.total_allocated, c);
        c
    }

    fn add_buffer(&mut self, len: usize) {
        let buffer = Buffer::with_length(len);
        self.total_allocated += buffer.array.len();
        self.buffers.push(buffer);
        self.adder = BulkAddr::Buffers;
    }

    fn grow_buffer(&mut self, buf_idx: usize, additional_capacity: usize) {
        let size = self.buffers[buf_idx].array.len();
        self.buffers[buf_idx]
            .array
            .resize(size + additional_capacity, 0i32);
        self.total_allocated += additional_capacity;
    }

    fn upgrade2bit_set(&mut self) {
        assert!(self.bit_set.is_none());
        assert!(self.max_doc > 0);
        let mut bit_set = FixedBitSet::new(self.max_doc as usize);
        let mut counter = 0i64;
        for buffer in &self.buffers {
            let length = buffer.length;
            counter += length as i64;
            for i in buffer.array.iter().take(length) {
                bit_set.set(*i as usize);
            }
        }
        self.bit_set = Some(bit_set);
        self.counter = counter;
        self.buffers = Vec::new();
        self.adder = BulkAddr::FixedBitSet;
    }

    pub fn build(&mut self) -> DocIdSetEnum {
        if self.bit_set.is_some() {
            assert!(self.counter >= 0);
            let cost = (self.counter as f64 / self.num_values_per_doc).round() as usize;
            self.buffers = Vec::new();
            DocIdSetEnum::BitDocId(BitDocIdSet::new(
                Arc::new(self.bit_set.take().unwrap()),
                cost,
            ))
        } else {
            let mut concatenated = DocIdSetBuilder::concat(&mut self.buffers);
            let mut sorter = LSBRadixSorter::default();
            sorter.sort(
                (self.max_doc - 1).bits_required() as usize,
                &mut concatenated.array,
                concatenated.length,
            );
            let l = if self.multivalued {
                self.dedup(&mut concatenated.array, concatenated.length)
            } else {
                assert!(self.no_dups(&concatenated.array, concatenated.length));
                concatenated.length
            };
            assert!(l <= concatenated.length);
            concatenated.array[l] = NO_MORE_DOCS;
            self.buffers = Vec::new();
            let array = mem::replace(&mut concatenated.array, Vec::new());
            DocIdSetEnum::IntArray(IntArrayDocIdSet::new(array, l))
        }
    }

    /// Concatenate the buffers in any order, leaving at least one empty slot in the end
    /// NOTE: this method might reuse one of the arrays
    fn concat(buffers: &mut [Buffer]) -> Buffer {
        if buffers.is_empty() {
            return Buffer::with_length(1);
        }

        let mut total_length = 0usize;
        let mut largest_buffer = Buffer::with_length(0);
        let mut largest_idx = 0usize;
        let mut largest_buf_len = 0usize;
        for (idx, buffer) in buffers.iter().enumerate() {
            total_length += buffer.length;
            if buffer.array.len() > largest_buf_len {
                largest_buf_len = buffer.array.len();
                largest_idx = idx;
            }
        }
        largest_buffer = mem::replace(&mut buffers[largest_idx], largest_buffer);
        if largest_buffer.array.len() < total_length + 1 {
            largest_buffer.array.resize(total_length + 1, 0i32);
        }
        total_length = largest_buffer.length;
        for buffer in buffers {
            if buffer.length > 0 {
                largest_buffer.array[total_length..total_length + buffer.length]
                    .copy_from_slice(&buffer.array[0..buffer.length]);
                total_length += buffer.length;
            }
        }
        largest_buffer.length = total_length;
        largest_buffer
    }

    fn dedup(&self, arr: &mut [i32], length: usize) -> usize {
        if length == 0 {
            return 0;
        }
        let mut l = 1usize;
        let mut previous = arr[0];
        for i in 1..length {
            let value = arr[i];
            assert!(value >= previous);
            if value != previous {
                arr[l] = value;
                l += 1;
                previous = value;
            }
        }
        l
    }

    fn no_dups(&self, a: &[i32], len: usize) -> bool {
        for i in 1..len {
            assert!(a[i - 1] < a[i])
        }
        true
    }
}

#[derive(Copy, Clone)]
enum BulkAddr {
    FixedBitSet,
    Buffers,
}

struct Buffer {
    pub array: Vec<i32>,
    pub length: usize,
}

impl Buffer {
    pub fn with_length(length: usize) -> Buffer {
        let array = vec![0i32; length];
        Buffer { length: 0, array }
    }

    #[allow(dead_code)]
    pub fn new(array: Vec<i32>, length: usize) -> Buffer {
        Buffer { array, length }
    }
}
