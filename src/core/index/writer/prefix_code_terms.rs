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

use core::doc::Term;
use core::store::io::{DataInput, DataOutput, IndexInput, RAMOutputStream};
use core::util::fst::{BytesStore, StoreBytesReader};
use core::util::{BytesRef, BytesRefBuilder};

use error::Result;

use std::cmp::{min, Ordering};
use std::collections::BinaryHeap;
use std::io::Write;
use std::mem;

use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;

pub struct PrefixCodedTerms {
    buffer: BytesStore,
    pub size: usize,
    del_gen: AtomicU64,
}

// TODO this is used as a stub for mem::replace,
// other use should use a builder to build
impl Default for PrefixCodedTerms {
    fn default() -> Self {
        PrefixCodedTerms {
            buffer: BytesStore::with_block_bits(0),
            size: 0,
            del_gen: AtomicU64::new(0),
        }
    }
}

impl PrefixCodedTerms {
    fn new(buffer: BytesStore, size: usize) -> Self {
        PrefixCodedTerms {
            buffer,
            size,
            del_gen: AtomicU64::new(0),
        }
    }

    /// Records del gen for this packet.
    pub fn set_del_gen(&self, del_gen: u64) {
        self.del_gen.store(del_gen, AtomicOrdering::Release);
    }

    pub fn iterator(&self) -> PrefixCodedTermsIterator {
        PrefixCodedTermsIterator::new(
            self.del_gen.load(AtomicOrdering::Acquire) as i64,
            &self.buffer,
        )
    }
}

pub struct PrefixCodedTermsBuilder {
    output: RAMOutputStream,
    last_term: Term,
    size: usize,
}

impl Default for PrefixCodedTermsBuilder {
    fn default() -> Self {
        PrefixCodedTermsBuilder {
            output: RAMOutputStream::with_chunk_size(1024, false),
            last_term: Term::new("".into(), vec![]),
            size: 0,
        }
    }
}

impl PrefixCodedTermsBuilder {
    pub fn add_term(&mut self, term: Term) {
        self.add(term.field, &term.bytes).unwrap();
    }

    pub fn add(&mut self, field: String, term: &[u8]) -> Result<()> {
        debug_assert!(self.last_term.is_empty() || self.compare(&field, term) == Ordering::Less);

        let prefix = self.shared_prefix_len(&self.last_term.bytes, term);
        let suffix = term.len() - prefix;

        if field == self.last_term.field {
            self.output.write_vint((prefix << 1) as i32)?;
        } else {
            self.output.write_vint((prefix << 1 | 1) as i32)?;
            self.output.write_string(&field)?;
            self.last_term.field = field;
        }
        self.output.write_vint(suffix as i32)?;
        self.output.write_bytes(term, prefix, suffix)?;
        self.last_term.bytes = term.to_vec();
        self.size += 1;
        Ok(())
    }

    pub fn finish(&mut self) -> PrefixCodedTerms {
        self.output.flush().unwrap();
        let store = mem::replace(&mut self.output.store, BytesStore::with_block_bits(1));
        PrefixCodedTerms::new(store, self.size)
    }

    fn shared_prefix_len(&self, term1: &[u8], term2: &[u8]) -> usize {
        let end = min(term1.len(), term2.len());
        for i in 0..end {
            if term1[i] != term2[i] {
                return i;
            }
        }
        end
    }

    fn compare(&self, field: &str, term: &[u8]) -> Ordering {
        let cmp = self.last_term.field.as_str().cmp(field);
        if cmp == Ordering::Equal {
            (self.last_term.bytes.as_slice()).cmp(term)
        } else {
            cmp
        }
    }
}

/// Iterates over terms in across multiple fields.  The caller must
/// check `#field` after each `#next` to see if the field
/// changed, but `==` can be used since the iterator
/// implementation ensures it will use the same String instance for
/// a given field.
pub trait FieldTermIter {
    /// Returns current field.  This method should not be called
    /// after iteration is done. Note that you may use == to
    /// detect a change in field.
    fn field(&self) -> &str;

    /// Del gen of the current term.
    // TODO: this is really per-iterator not per term, but when we use
    // MergedPrefixCodedTermsIterator we need to know which iterator we are on
    fn del_gen(&self) -> i64;

    /// Increments the iteration to the next {@link BytesRef} in the iterator.
    /// Returns the resulting {@link BytesRef} or <code>null</code> if the end of
    /// the iterator is reached. The returned BytesRef may be re-used across calls
    /// to next. After this method returns null, do not call it again: the results
    /// are undefined.
    /// NOTE: we don't use the `Iterator` trait in in std, because we must not
    /// ignore errors
    fn next(&mut self) -> Result<Option<BytesRef>>;
}

pub struct PrefixCodedTermsIterator {
    reader: StoreBytesReader,
    builder: BytesRefBuilder,
    end: usize,
    pub del_gen: i64,
    pub field: String,
}

impl PrefixCodedTermsIterator {
    fn new(del_gen: i64, store: &BytesStore) -> Self {
        let reader = store.get_forward_reader();
        let end = reader.len() as usize;
        let builder = BytesRefBuilder::new();
        PrefixCodedTermsIterator {
            reader,
            builder,
            end,
            del_gen,
            field: String::new(),
        }
    }

    fn read_term_bytes(&mut self, prefix: usize, suffix: usize) -> Result<()> {
        self.builder.grow(prefix + suffix);
        self.reader
            .read_bytes(self.builder.bytes_mut(), prefix, suffix)?;
        self.builder.length = prefix + suffix;
        Ok(())
    }

    fn bytes(&self) -> BytesRef {
        self.builder.get()
    }
}

impl FieldTermIter for PrefixCodedTermsIterator {
    fn field(&self) -> &str {
        &self.field
    }

    fn del_gen(&self) -> i64 {
        self.del_gen
    }

    fn next(&mut self) -> Result<Option<BytesRef>> {
        if (self.reader.file_pointer() as usize) < self.end {
            let code = self.reader.read_vint()?;
            let new_field = code & 1 != 0;
            if new_field {
                self.field = self.reader.read_string()?;
            }
            let prefix = code >> 1;
            let suffix = self.reader.read_vint()?;
            self.read_term_bytes(prefix as usize, suffix as usize)?;
            Ok(Some(self.builder.get()))
        } else {
            self.field.clear();
            Ok(None)
        }
    }
}

struct TermsIteratorByTerm {
    iter: PrefixCodedTermsIterator,
}

impl TermsIteratorByTerm {
    fn into_field(self) -> TermsIteratorByField {
        TermsIteratorByField { iter: self.iter }
    }
}

impl Ord for TermsIteratorByTerm {
    fn cmp(&self, other: &Self) -> Ordering {
        // reversed ordering for BinaryHeap
        let cmp = other.iter.bytes().cmp(&self.iter.bytes());
        if cmp != Ordering::Equal {
            cmp
        } else {
            other.iter.del_gen.cmp(&self.iter.del_gen)
        }
    }
}

impl PartialOrd for TermsIteratorByTerm {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for TermsIteratorByTerm {}

impl PartialEq for TermsIteratorByTerm {
    fn eq(&self, other: &Self) -> bool {
        self.iter.bytes() == other.iter.bytes()
    }
}

struct TermsIteratorByField {
    iter: PrefixCodedTermsIterator,
}

impl TermsIteratorByField {
    fn into_term(self) -> TermsIteratorByTerm {
        TermsIteratorByTerm { iter: self.iter }
    }
}

impl Ord for TermsIteratorByField {
    fn cmp(&self, other: &Self) -> Ordering {
        // reversed ordering for BinaryHeap
        other.iter.field.cmp(&self.iter.field)
    }
}

impl PartialOrd for TermsIteratorByField {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for TermsIteratorByField {}

impl PartialEq for TermsIteratorByField {
    fn eq(&self, other: &Self) -> bool {
        self.iter.field == other.iter.field
    }
}

pub struct MergedPrefixCodedTermsIterator {
    term_queue: BinaryHeap<TermsIteratorByTerm>,
    field_queue: BinaryHeap<TermsIteratorByField>,
    field: String,
}

impl MergedPrefixCodedTermsIterator {
    fn new(terms_list: &[Arc<PrefixCodedTerms>]) -> Result<Self> {
        debug_assert!(terms_list.len() > 1);
        let mut field_queue = BinaryHeap::with_capacity(terms_list.len());
        for terms in terms_list {
            let mut iter = terms.iterator();
            iter.next()?;
            if !iter.field().is_empty() {
                field_queue.push(TermsIteratorByField { iter });
            }
        }

        let term_queue = BinaryHeap::with_capacity(terms_list.len());

        Ok(MergedPrefixCodedTermsIterator {
            term_queue,
            field_queue,
            field: String::with_capacity(0),
        })
    }
}

impl FieldTermIter for MergedPrefixCodedTermsIterator {
    fn field(&self) -> &str {
        &self.field
    }

    fn del_gen(&self) -> i64 {
        debug_assert!(!self.term_queue.is_empty());
        self.term_queue.peek().unwrap().iter.del_gen()
    }

    fn next(&mut self) -> Result<Option<BytesRef>> {
        if self.term_queue.is_empty() {
            // No more terms in current field:
            if self.field_queue.is_empty() {
                self.field = String::with_capacity(0);
                return Ok(None);
            }

            // Transfer all iterators on the next field into the term queue:
            let top = self.field_queue.pop().unwrap();
            self.field = top.iter.field.clone();
            debug_assert!(!self.field.is_empty());
            self.term_queue.push(top.into_term());

            while !self.field_queue.is_empty()
                && self.field_queue.peek().unwrap().iter.field == self.field
            {
                let iter = self.field_queue.pop().unwrap();
                self.term_queue.push(iter.into_term());
            }
            Ok(Some(self.term_queue.peek().unwrap().iter.bytes()))
        } else {
            if self.term_queue.peek_mut().unwrap().iter.next()?.is_none() {
                self.term_queue.pop();
            } else if self.term_queue.peek().unwrap().iter.field != self.field {
                let top = self.term_queue.pop().unwrap();
                self.field_queue.push(top.into_field());
            }
            if self.term_queue.is_empty() {
                // Recurse (just once) to go to next field:
                self.next()
            } else {
                // Still terms left in this field
                Ok(Some(self.term_queue.peek().unwrap().iter.bytes()))
            }
        }
    }
}

pub enum FieldTermIterator {
    Single(PrefixCodedTermsIterator),
    Merged(MergedPrefixCodedTermsIterator),
}

impl FieldTermIterator {
    pub fn build(terms_list: &[Arc<PrefixCodedTerms>]) -> Result<Self> {
        debug_assert!(!terms_list.is_empty());
        if terms_list.len() == 1 {
            Ok(FieldTermIterator::Single(terms_list[0].iterator()))
        } else {
            let iter = MergedPrefixCodedTermsIterator::new(terms_list)?;
            Ok(FieldTermIterator::Merged(iter))
        }
    }
}

impl FieldTermIter for FieldTermIterator {
    fn field(&self) -> &str {
        match self {
            FieldTermIterator::Single(s) => s.field(),
            FieldTermIterator::Merged(m) => m.field(),
        }
    }

    fn del_gen(&self) -> i64 {
        match self {
            FieldTermIterator::Single(s) => s.del_gen(),
            FieldTermIterator::Merged(m) => m.del_gen(),
        }
    }

    fn next(&mut self) -> Result<Option<BytesRef>> {
        match self {
            FieldTermIterator::Single(s) => s.next(),
            FieldTermIterator::Merged(m) => m.next(),
        }
    }
}
