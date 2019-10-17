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

use core::codec::{EmptyPostingIterator, PostingIterator};
use core::codec::{
    EmptyTermIterator, IterWithSlice, MultiPostingsIterator, SeekStatus, TermIterator, Terms,
};
use core::index::reader::ReaderSlice;
use core::search::{DocIterator, Payload};
use core::util::external::BinaryHeapPub;
use core::util::DocId;

use error::ErrorKind::UnsupportedOperation;
use error::Result;

use std::cmp::Ordering;
use std::mem;

pub struct MultiTerms<T: Terms> {
    subs: Vec<T>,
    #[allow(dead_code)]
    sub_slices: Vec<ReaderSlice>,
    has_freqs: bool,
    has_offsets: bool,
    has_positions: bool,
    has_payloads: bool,
}

impl<T: Terms> MultiTerms<T> {
    pub fn new(subs: Vec<T>, sub_slices: Vec<ReaderSlice>) -> Result<MultiTerms<T>> {
        let mut has_freqs = true;
        let mut has_offsets = true;
        let mut has_positions = true;
        let mut has_payloads = true;

        for i in &subs {
            has_freqs &= i.has_freqs()?;
            has_offsets &= i.has_offsets()?;
            has_positions &= i.has_positions()?;
            has_payloads |= i.has_payloads()?;
        }

        Ok(MultiTerms {
            subs,
            sub_slices,
            has_freqs,
            has_offsets,
            has_positions,
            has_payloads,
        })
    }
}

impl<T: Terms> Terms for MultiTerms<T> {
    type Iterator = MultiTermIteratorEnum<T::Iterator>;
    fn iterator(&self) -> Result<Self::Iterator> {
        let mut terms_iters = vec![];
        let mut i = 0;
        for sub in &self.subs {
            let iterator = sub.iterator()?;
            if !iterator.is_empty() {
                terms_iters.push(TermIteratorIndex::new(iterator, i));
            }
            i += 1;
        }

        if terms_iters.is_empty() {
            Ok(MultiTermIteratorEnum::Empty(EmptyTermIterator::default()))
        } else {
            let mut res = MultiTermIterator::new(self.sub_slices.clone());
            res.reset(terms_iters)?;
            if res.queue.queue.is_empty() {
                Ok(MultiTermIteratorEnum::Empty(EmptyTermIterator::default()))
            } else {
                Ok(MultiTermIteratorEnum::Multi(res))
            }
        }
    }

    fn size(&self) -> Result<i64> {
        Ok(-1)
    }

    fn sum_total_term_freq(&self) -> Result<i64> {
        let mut sum = 0i64;
        for terms in &self.subs {
            let v = terms.sum_total_term_freq()?;
            if v == -1 {
                return Ok(-1i64);
            }
            sum += v
        }
        Ok(sum)
    }

    fn sum_doc_freq(&self) -> Result<i64> {
        let mut sum = 0i64;
        for terms in &self.subs {
            let v = terms.sum_doc_freq()?;
            if v == -1 {
                return Ok(-1i64);
            }
            sum += v
        }
        Ok(sum)
    }

    fn doc_count(&self) -> Result<i32> {
        let mut sum = 0;
        for terms in &self.subs {
            let v = terms.doc_count()?;
            if v == -1 {
                return Ok(-1);
            }
            sum += v
        }
        Ok(sum)
    }

    fn has_freqs(&self) -> Result<bool> {
        Ok(self.has_freqs)
    }

    fn has_offsets(&self) -> Result<bool> {
        Ok(self.has_offsets)
    }

    fn has_positions(&self) -> Result<bool> {
        Ok(self.has_positions)
    }

    fn has_payloads(&self) -> Result<bool> {
        Ok(self.has_payloads)
    }

    fn min(&self) -> Result<Option<Vec<u8>>> {
        unimplemented!();
    }

    fn max(&self) -> Result<Option<Vec<u8>>> {
        unimplemented!();
    }

    fn stats(&self) -> Result<String> {
        unimplemented!();
    }
}

pub struct TermIteratorIndex<T: TermIterator> {
    pub sub_index: usize,
    pub terms: Option<T>,
}

impl<T: TermIterator> TermIteratorIndex<T> {
    pub fn new(terms: T, sub_index: usize) -> Self {
        TermIteratorIndex {
            sub_index,
            terms: Some(terms),
        }
    }

    pub fn take_terms(&mut self) -> T {
        debug_assert!(self.terms.is_some());
        self.terms.take().unwrap()
    }
}

/// Exposes `TermsEnum` API, merged from `TermsEnum` API of sub-segments.
/// This does a merge sort, by term text, of the sub-readers
pub struct MultiTermIterator<T: TermIterator> {
    queue: TermMergeQueue<T>,
    pub subs: Vec<TermIteratorWithSlice<T>>,
    current_subs_indexes: Vec<usize>,
    pub top_indexes: Vec<usize>,
    // sub_docs: Vec<IterWithSlice>,
    last_seek: Vec<u8>,
    last_seek_exact: bool,
    pub num_top: usize,
    num_subs: usize,
    current: Vec<u8>,
    current_is_none: bool,
}

impl<T: TermIterator> MultiTermIterator<T> {
    pub fn new(slices: Vec<ReaderSlice>) -> MultiTermIterator<T> {
        let len = slices.len();
        let queue = TermMergeQueue::new(len);
        let top_indexes = vec![0; len];
        // let mut sub_docs = Vec::with_capacity(len);
        let mut subs = Vec::with_capacity(len);
        let mut i = 0;
        for s in slices {
            subs.push(TermIteratorWithSlice::new(i, s));
            // sub_docs.push(IterWithSlice::new(None, s));
            i += 1;
        }
        MultiTermIterator {
            queue,
            subs,
            current_subs_indexes: vec![0; len],
            top_indexes,
            last_seek: Vec::with_capacity(0),
            last_seek_exact: false,
            num_top: 0,
            num_subs: 0,
            current: vec![],
            current_is_none: true,
        }
    }

    /// The terms array must be newly created TermIterator,
    pub fn reset(&mut self, term_iter_index: Vec<TermIteratorIndex<T>>) -> Result<()> {
        debug_assert!(term_iter_index.len() <= self.top_indexes.len());
        self.num_subs = 0;
        self.num_top = 0;
        self.queue.queue.clear();
        for mut term_index in term_iter_index {
            debug_assert!(term_index.terms.is_some());
            if let Some(term) = term_index.terms.as_mut().unwrap().next()? {
                let iter = term_index.take_terms();
                self.subs[term_index.sub_index].reset(iter, term);
                self.queue.queue.push(TIWSRefWithIndex::new(
                    &mut self.subs[term_index.sub_index],
                    term_index.sub_index,
                ));
                self.current_subs_indexes[self.num_subs] = term_index.sub_index;
                self.num_subs += 1;
            } else {
                // field has no terms
            }
        }
        //        if self.queue.queue.is_empty() {
        //            Ok(MultiTermIteratorEnum::Empty(EmptyTermIterator::default()))
        //        } else {
        //            Ok(MultiTermIteratorEnum::Multi(self))
        //        }
        Ok(())
    }

    fn pull_top(&mut self) {
        // extract all subs from the queue that have the same top term
        debug_assert_eq!(self.num_top, 0);
        let num = self.queue.fill_top(&mut self.top_indexes);
        self.num_top = num;
        debug_assert!(num > 0);
        self.current = self.subs[self.top_indexes[0]].current.clone();
        self.current_is_none = false;
    }

    fn push_top(&mut self) -> Result<()> {
        // call next() on each top, and reorder queue
        for _ in 0..self.num_top {
            let should_pop;
            {
                let top = self.queue.queue.peek_mut().unwrap();
                if let Some(cur) = top.slice().terms.as_mut().unwrap().next()? {
                    should_pop = false;
                    top.slice().current = cur;
                } else {
                    should_pop = true;
                    top.slice().current.clear();
                }
            }
            if should_pop {
                self.queue.queue.pop();
            }
        }
        self.num_top = 0;
        Ok(())
    }
}

impl<T: TermIterator> TermIterator for MultiTermIterator<T> {
    type Postings = MultiPostingsIterator<T::Postings>;
    type TermState = T::TermState;
    fn next(&mut self) -> Result<Option<Vec<u8>>> {
        if self.last_seek_exact {
            // Must seekCeil at this point, so those subs that
            // didn't have the term can find the following term.
            // NOTE: we could save some CPU by only seekCeil the
            // subs that didn't match the last exact seek... but
            // most impls short-circuit if you seekCeil to term
            // they are already on.
            let current = mem::replace(&mut self.current, Vec::with_capacity(0));
            let status = self.seek_ceil(&current)?;
            debug_assert_eq!(status, SeekStatus::Found);
            self.last_seek_exact = false;
        }
        self.last_seek.clear();

        // restore queue
        self.push_top()?;

        // gather equal top fields
        if !self.queue.queue.is_empty() {
            self.pull_top();
        } else {
            self.current.clear();
            self.current_is_none = true;
        }

        if self.current_is_none {
            Ok(None)
        } else {
            Ok(Some(self.current.clone()))
        }
    }

    fn seek_exact(&mut self, text: &[u8]) -> Result<bool> {
        self.queue.queue.clear();
        self.num_top = 0;

        let seek_opt = !self.last_seek.is_empty() && self.last_seek.as_slice() <= text;
        self.last_seek.clear();
        self.last_seek_exact = true;

        for i in 0..self.num_subs {
            // LUCENE-2130: if we had just seek'd already, prior
            // to this seek, and the new seek term is after the
            // previous one, don't try to re-seek this sub if its
            // current term is already beyond this new seek term.
            // Doing so is a waste because this sub will simply
            // seek to the same spot.
            let cur_index = self.current_subs_indexes[i];
            let status = if seek_opt {
                if !self.subs[cur_index].current.is_empty() {
                    match text.cmp(&self.subs[cur_index].current) {
                        Ordering::Equal => true,
                        Ordering::Less => false,
                        Ordering::Greater => self.subs[cur_index]
                            .terms
                            .as_mut()
                            .unwrap()
                            .seek_exact(text)?,
                    }
                } else {
                    false
                }
            } else {
                self.subs[cur_index]
                    .terms
                    .as_mut()
                    .unwrap()
                    .seek_exact(text)?
            };

            if status {
                self.top_indexes[self.num_top] = cur_index;
                self.num_top += 1;
                let term = self.subs[cur_index]
                    .terms
                    .as_mut()
                    .unwrap()
                    .term()?
                    .to_vec();
                self.subs[cur_index].current = term.clone();
                self.current = term;
                self.current_is_none = false;
            }
        }
        // if at least one sub had exact match to the requested
        // term then we found match
        Ok(self.num_top > 0)
    }

    fn seek_ceil(&mut self, text: &[u8]) -> Result<SeekStatus> {
        self.queue.queue.clear();
        self.num_top = 0;
        self.last_seek_exact = false;

        let seek_opt = !self.last_seek.is_empty() && self.last_seek.as_slice() <= text;

        self.last_seek.resize(text.len(), 0u8);
        self.last_seek.copy_from_slice(text);

        for i in 0..self.num_subs {
            // LUCENE-2130: if we had just seek'd already, prior
            // to this seek, and the new seek term is after the
            // previous one, don't try to re-seek this sub if its
            // current term is already beyond this new seek term.
            // Doing so is a waste because this sub will simply
            // seek to the same spot.
            let cur_index = self.current_subs_indexes[i];
            let status = if seek_opt {
                if !self.subs[cur_index].current.is_empty() {
                    match text.cmp(&self.subs[cur_index].current) {
                        Ordering::Equal => SeekStatus::Found,
                        Ordering::Less => SeekStatus::NotFound,
                        Ordering::Greater => self.subs[cur_index]
                            .terms
                            .as_mut()
                            .unwrap()
                            .seek_ceil(text)?,
                    }
                } else {
                    SeekStatus::End
                }
            } else {
                self.subs[cur_index]
                    .terms
                    .as_mut()
                    .unwrap()
                    .seek_ceil(text)?
            };

            if status == SeekStatus::Found {
                self.top_indexes[self.num_top] = cur_index;
                self.num_top += 1;
                let term = self.subs[cur_index]
                    .terms
                    .as_mut()
                    .unwrap()
                    .term()?
                    .to_vec();
                self.subs[cur_index].current = term.clone();
                self.current = term;
                self.current_is_none = false;
                self.queue
                    .queue
                    .push(TIWSRefWithIndex::new(&mut self.subs[cur_index], cur_index));
            } else if status == SeekStatus::NotFound {
                let term = self.subs[cur_index]
                    .terms
                    .as_mut()
                    .unwrap()
                    .term()?
                    .to_vec();
                debug_assert!(!term.is_empty());
                self.subs[cur_index].current = term;
                self.queue
                    .queue
                    .push(TIWSRefWithIndex::new(&mut self.subs[cur_index], cur_index));
            } else {
                debug_assert_eq!(status, SeekStatus::End);
                // enum exhausted
                self.subs[cur_index].current.clear();
            }
        }

        if self.num_top > 0 {
            // at least one sub had exact match to the requested term
            Ok(SeekStatus::Found)
        } else if !self.queue.queue.is_empty() {
            // no sub had exact match, but at least one sub found
            // a term after the requested term -- advance to that
            // next term:
            self.pull_top();
            Ok(SeekStatus::NotFound)
        } else {
            Ok(SeekStatus::End)
        }
    }

    fn seek_exact_ord(&mut self, _ord: i64) -> Result<()> {
        bail!(UnsupportedOperation("".into()))
    }

    fn term(&self) -> Result<&[u8]> {
        Ok(&self.current)
    }

    fn ord(&self) -> Result<i64> {
        bail!(UnsupportedOperation("".into()))
    }

    fn doc_freq(&mut self) -> Result<i32> {
        let mut sum = 0;
        for i in 0..self.num_top {
            sum += self.subs[self.top_indexes[i]]
                .terms
                .as_mut()
                .unwrap()
                .doc_freq()?;
        }
        Ok(sum)
    }

    fn total_term_freq(&mut self) -> Result<i64> {
        let mut sum = 0;
        for i in 0..self.num_top {
            let v = self.subs[self.top_indexes[i]]
                .terms
                .as_mut()
                .unwrap()
                .total_term_freq()?;
            if v == -1 {
                return Ok(v);
            }
            sum += v;
        }
        Ok(sum)
    }

    fn postings_with_flags(&mut self, flags: u16) -> Result<Self::Postings> {
        let mut docs_iter = MultiPostingsIterator::new(self.subs.len());

        let mut upto = 0;
        let sub_indexes: Vec<usize> = self.subs.iter().map(|s| s.index).collect();
        self.top_indexes[0..self.num_top].sort_by(|i, j| sub_indexes[*i].cmp(&sub_indexes[*j]));

        let mut sub_docs = Vec::with_capacity(self.num_top);
        for i in 0..self.num_top {
            let index = self.top_indexes[i];

            debug_assert!(self.subs[index].index < self.subs.len());
            let sub_posting = self.subs[index]
                .terms
                .as_mut()
                .unwrap()
                .postings_with_flags(flags)?;
            sub_docs.push(IterWithSlice::new(sub_posting, self.subs[index].sub_slice));
            upto += 1;
        }

        docs_iter.reset(sub_docs, upto);
        Ok(docs_iter)
    }
}

struct TermMergeQueue<T: TermIterator> {
    queue: BinaryHeapPub<TIWSRefWithIndex<T>>,
    stack: Vec<usize>,
}

impl<T: TermIterator> TermMergeQueue<T> {
    fn new(size: usize) -> Self {
        let queue = BinaryHeapPub::with_capacity(size);
        let stack = vec![0; size];
        TermMergeQueue { queue, stack }
    }

    fn fill_top(&mut self, tops: &mut [usize]) -> usize {
        let len = self.queue.len();
        if len == 0 {
            return 0;
        }

        tops[0] = self.queue.peek().unwrap().index;
        let mut num_top = 1;
        self.stack[0] = 0;
        let mut stack_len = 1;

        while stack_len > 0 {
            stack_len -= 1;
            let index = self.stack[stack_len];
            let left_child = (index << 1) + 1;
            let data = self.queue.inner();
            for child in left_child..len.min(left_child + 2) {
                if data[child].slice().current == data[0].slice().current {
                    tops[num_top] = data[child].index;
                    num_top += 1;
                    self.stack[stack_len] = child;
                    stack_len += 1;
                }
            }
        }
        num_top
    }
}

// pointer to `TermIteratorWithSlice` and index to subs
struct TIWSRefWithIndex<T: TermIterator> {
    term_slice: *mut TermIteratorWithSlice<T>,
    index: usize,
}

impl<T: TermIterator> TIWSRefWithIndex<T> {
    fn new(term_slice: &mut TermIteratorWithSlice<T>, index: usize) -> Self {
        TIWSRefWithIndex { term_slice, index }
    }

    #[allow(clippy::mut_from_ref)]
    fn slice(&self) -> &mut TermIteratorWithSlice<T> {
        unsafe { &mut *self.term_slice }
    }
}

impl<T: TermIterator> Eq for TIWSRefWithIndex<T> {}

impl<T: TermIterator> PartialEq for TIWSRefWithIndex<T> {
    fn eq(&self, other: &TIWSRefWithIndex<T>) -> bool {
        self.slice().current == other.slice().current
    }
}

impl<T: TermIterator> Ord for TIWSRefWithIndex<T> {
    // reverse ord for binary heap
    fn cmp(&self, other: &Self) -> Ordering {
        unsafe { (*other.term_slice).current.cmp(&(*self.term_slice).current) }
    }
}

impl<T: TermIterator> PartialOrd for TIWSRefWithIndex<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub struct TermIteratorWithSlice<T: TermIterator> {
    sub_slice: ReaderSlice,
    pub terms: Option<T>,
    current: Vec<u8>,
    pub index: usize,
}

impl<T: TermIterator> TermIteratorWithSlice<T> {
    fn new(index: usize, sub_slice: ReaderSlice) -> Self {
        TermIteratorWithSlice {
            sub_slice,
            index,
            terms: None,
            current: Vec::with_capacity(0),
        }
    }

    fn reset(&mut self, terms: T, term: Vec<u8>) {
        self.terms = Some(terms);
        self.current = term;
    }
}

pub enum MultiTermIteratorEnum<T: TermIterator> {
    Multi(MultiTermIterator<T>),
    Raw(T),
    Empty(EmptyTermIterator),
}

impl<T: TermIterator> TermIterator for MultiTermIteratorEnum<T> {
    type Postings = MultiPostingIterEnum<T::Postings>;
    type TermState = T::TermState;
    fn next(&mut self) -> Result<Option<Vec<u8>>> {
        match self {
            MultiTermIteratorEnum::Multi(t) => t.next(),
            MultiTermIteratorEnum::Raw(t) => t.next(),
            MultiTermIteratorEnum::Empty(t) => t.next(),
        }
    }

    fn seek_exact(&mut self, text: &[u8]) -> Result<bool> {
        match self {
            MultiTermIteratorEnum::Multi(t) => t.seek_exact(text),
            MultiTermIteratorEnum::Raw(t) => t.seek_exact(text),
            MultiTermIteratorEnum::Empty(t) => t.seek_exact(text),
        }
    }

    fn seek_ceil(&mut self, text: &[u8]) -> Result<SeekStatus> {
        match self {
            MultiTermIteratorEnum::Multi(t) => t.seek_ceil(text),
            MultiTermIteratorEnum::Raw(t) => t.seek_ceil(text),
            MultiTermIteratorEnum::Empty(t) => t.seek_ceil(text),
        }
    }

    fn seek_exact_ord(&mut self, ord: i64) -> Result<()> {
        match self {
            MultiTermIteratorEnum::Multi(t) => t.seek_exact_ord(ord),
            MultiTermIteratorEnum::Raw(t) => t.seek_exact_ord(ord),
            MultiTermIteratorEnum::Empty(t) => t.seek_exact_ord(ord),
        }
    }

    fn seek_exact_state(&mut self, text: &[u8], state: &Self::TermState) -> Result<()> {
        match self {
            MultiTermIteratorEnum::Multi(t) => t.seek_exact_state(text, state),
            MultiTermIteratorEnum::Raw(t) => t.seek_exact_state(text, state),
            MultiTermIteratorEnum::Empty(_) => unreachable!(),
        }
    }

    fn term(&self) -> Result<&[u8]> {
        match self {
            MultiTermIteratorEnum::Multi(t) => t.term(),
            MultiTermIteratorEnum::Raw(t) => t.term(),
            MultiTermIteratorEnum::Empty(t) => t.term(),
        }
    }

    fn ord(&self) -> Result<i64> {
        match self {
            MultiTermIteratorEnum::Multi(t) => t.ord(),
            MultiTermIteratorEnum::Raw(t) => t.ord(),
            MultiTermIteratorEnum::Empty(t) => t.ord(),
        }
    }

    fn doc_freq(&mut self) -> Result<i32> {
        match self {
            MultiTermIteratorEnum::Multi(t) => t.doc_freq(),
            MultiTermIteratorEnum::Raw(t) => t.doc_freq(),
            MultiTermIteratorEnum::Empty(t) => t.doc_freq(),
        }
    }

    fn total_term_freq(&mut self) -> Result<i64> {
        match self {
            MultiTermIteratorEnum::Multi(t) => t.total_term_freq(),
            MultiTermIteratorEnum::Raw(t) => t.total_term_freq(),
            MultiTermIteratorEnum::Empty(t) => t.total_term_freq(),
        }
    }

    fn postings(&mut self) -> Result<Self::Postings> {
        match self {
            MultiTermIteratorEnum::Multi(t) => Ok(MultiPostingIterEnum::Multi(t.postings()?)),
            MultiTermIteratorEnum::Raw(t) => Ok(MultiPostingIterEnum::Raw(t.postings()?)),
            MultiTermIteratorEnum::Empty(t) => Ok(MultiPostingIterEnum::Empty(t.postings()?)),
        }
    }

    fn postings_with_flags(&mut self, flags: u16) -> Result<Self::Postings> {
        match self {
            MultiTermIteratorEnum::Multi(t) => {
                Ok(MultiPostingIterEnum::Multi(t.postings_with_flags(flags)?))
            }
            MultiTermIteratorEnum::Raw(t) => {
                Ok(MultiPostingIterEnum::Raw(t.postings_with_flags(flags)?))
            }
            MultiTermIteratorEnum::Empty(t) => {
                Ok(MultiPostingIterEnum::Empty(t.postings_with_flags(flags)?))
            }
        }
    }

    fn term_state(&mut self) -> Result<Self::TermState> {
        match self {
            MultiTermIteratorEnum::Multi(t) => t.term_state(),
            MultiTermIteratorEnum::Raw(t) => t.term_state(),
            MultiTermIteratorEnum::Empty(_) => unreachable!(),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            MultiTermIteratorEnum::Multi(t) => t.is_empty(),
            MultiTermIteratorEnum::Raw(t) => t.is_empty(),
            MultiTermIteratorEnum::Empty(_) => true,
        }
    }
}

pub enum MultiPostingIterEnum<T: PostingIterator> {
    Multi(MultiPostingsIterator<T>),
    Raw(T),
    Empty(EmptyPostingIterator),
}

impl<T: PostingIterator> MultiPostingIterEnum<T> {
    pub fn is_multi(&self) -> bool {
        match self {
            MultiPostingIterEnum::Multi(_) => true,
            _ => false,
        }
    }
}

impl<T: PostingIterator> PostingIterator for MultiPostingIterEnum<T> {
    fn freq(&self) -> Result<i32> {
        match self {
            MultiPostingIterEnum::Multi(i) => i.freq(),
            MultiPostingIterEnum::Raw(i) => i.freq(),
            MultiPostingIterEnum::Empty(i) => i.freq(),
        }
    }

    fn next_position(&mut self) -> Result<i32> {
        match self {
            MultiPostingIterEnum::Multi(i) => i.next_position(),
            MultiPostingIterEnum::Raw(i) => i.next_position(),
            MultiPostingIterEnum::Empty(i) => i.next_position(),
        }
    }

    fn start_offset(&self) -> Result<i32> {
        match self {
            MultiPostingIterEnum::Multi(i) => i.start_offset(),
            MultiPostingIterEnum::Raw(i) => i.start_offset(),
            MultiPostingIterEnum::Empty(i) => i.start_offset(),
        }
    }

    fn end_offset(&self) -> Result<i32> {
        match self {
            MultiPostingIterEnum::Multi(i) => i.end_offset(),
            MultiPostingIterEnum::Raw(i) => i.end_offset(),
            MultiPostingIterEnum::Empty(i) => i.end_offset(),
        }
    }

    fn payload(&self) -> Result<Payload> {
        match self {
            MultiPostingIterEnum::Multi(i) => i.payload(),
            MultiPostingIterEnum::Raw(i) => i.payload(),
            MultiPostingIterEnum::Empty(i) => i.payload(),
        }
    }
}

impl<T: PostingIterator> DocIterator for MultiPostingIterEnum<T> {
    fn doc_id(&self) -> DocId {
        match self {
            MultiPostingIterEnum::Multi(i) => i.doc_id(),
            MultiPostingIterEnum::Raw(i) => i.doc_id(),
            MultiPostingIterEnum::Empty(i) => i.doc_id(),
        }
    }

    fn next(&mut self) -> Result<DocId> {
        match self {
            MultiPostingIterEnum::Multi(i) => i.next(),
            MultiPostingIterEnum::Raw(i) => i.next(),
            MultiPostingIterEnum::Empty(i) => i.next(),
        }
    }

    fn advance(&mut self, target: i32) -> Result<DocId> {
        match self {
            MultiPostingIterEnum::Multi(i) => i.advance(target),
            MultiPostingIterEnum::Raw(i) => i.advance(target),
            MultiPostingIterEnum::Empty(i) => i.advance(target),
        }
    }

    fn slow_advance(&mut self, target: i32) -> Result<DocId> {
        match self {
            MultiPostingIterEnum::Multi(i) => i.slow_advance(target),
            MultiPostingIterEnum::Raw(i) => i.slow_advance(target),
            MultiPostingIterEnum::Empty(i) => i.slow_advance(target),
        }
    }

    fn cost(&self) -> usize {
        match self {
            MultiPostingIterEnum::Multi(i) => i.cost(),
            MultiPostingIterEnum::Raw(i) => i.cost(),
            MultiPostingIterEnum::Empty(i) => i.cost(),
        }
    }

    fn matches(&mut self) -> Result<bool> {
        match self {
            MultiPostingIterEnum::Multi(i) => i.matches(),
            MultiPostingIterEnum::Raw(i) => i.matches(),
            MultiPostingIterEnum::Empty(i) => i.matches(),
        }
    }

    fn match_cost(&self) -> f32 {
        match self {
            MultiPostingIterEnum::Multi(i) => i.match_cost(),
            MultiPostingIterEnum::Raw(i) => i.match_cost(),
            MultiPostingIterEnum::Empty(i) => i.match_cost(),
        }
    }

    fn approximate_next(&mut self) -> Result<DocId> {
        match self {
            MultiPostingIterEnum::Multi(i) => i.approximate_next(),
            MultiPostingIterEnum::Raw(i) => i.approximate_next(),
            MultiPostingIterEnum::Empty(i) => i.approximate_next(),
        }
    }

    fn approximate_advance(&mut self, target: i32) -> Result<DocId> {
        match self {
            MultiPostingIterEnum::Multi(i) => i.approximate_advance(target),
            MultiPostingIterEnum::Raw(i) => i.approximate_advance(target),
            MultiPostingIterEnum::Empty(i) => i.approximate_advance(target),
        }
    }
}
