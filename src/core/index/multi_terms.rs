use core::index::multi_fields::{IterWithSlice, MultiPostingsIterator};
use core::index::reader_slice::ReaderSlice;
use core::index::term::{EmptyTermIterator, SeekStatus, TermIterator, Terms, TermsRef};
use core::search::posting_iterator::PostingIterator;
use core::util::binary_heap::BinaryHeapPub;

use error::ErrorKind::UnsupportedOperation;
use error::Result;

use std::any::Any;
use std::cmp::Ordering;
use std::mem;

pub struct MultiTerms {
    subs: Vec<TermsRef>,
    #[allow(dead_code)]
    sub_slices: Vec<ReaderSlice>,
    has_freqs: bool,
    has_offsets: bool,
    has_positions: bool,
    has_payloads: bool,
}

impl MultiTerms {
    pub fn new(subs: Vec<TermsRef>, sub_slices: Vec<ReaderSlice>) -> Result<MultiTerms> {
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

impl Terms for MultiTerms {
    fn iterator(&self) -> Result<Box<TermIterator>> {
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
            Ok(Box::new(EmptyTermIterator::default()))
        } else {
            let mut res = MultiTermIterator::new(self.sub_slices.clone());
            res.reset(terms_iters)?;
            Ok(Box::new(res))
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

pub struct TermIteratorIndex {
    pub sub_index: usize,
    pub terms: Box<TermIterator>,
}

impl TermIteratorIndex {
    pub fn new(terms: Box<TermIterator>, sub_index: usize) -> Self {
        TermIteratorIndex { sub_index, terms }
    }

    pub fn take_terms(&mut self) -> Box<TermIterator> {
        mem::replace(&mut self.terms, Box::new(EmptyTermIterator::default()))
    }
}

/// Exposes `TermsEnum` API, merged from `TermsEnum` API of sub-segments.
/// This does a merge sort, by term text, of the sub-readers
pub struct MultiTermIterator {
    queue: TermMergeQueue,
    pub subs: Vec<TermIteratorWithSlice>,
    current_subs_indexes: Vec<usize>,
    pub top_indexes: Vec<usize>,
    // sub_docs: Vec<IterWithSlice>,
    last_seek: Vec<u8>,
    last_seek_exact: bool,
    last_seek_scratch: Vec<u8>,
    pub num_top: usize,
    num_subs: usize,
    current: Vec<u8>,
    current_is_none: bool,
}

impl MultiTermIterator {
    pub fn new(slices: Vec<ReaderSlice>) -> MultiTermIterator {
        let len = slices.len();
        let queue = TermMergeQueue::new(len);
        let top_indexes = vec![0; len];
        // let mut sub_docs = Vec::with_capacity(len);
        let mut subs = Vec::with_capacity(len);
        let mut i = 0;
        for s in slices {
            subs.push(TermIteratorWithSlice::new(i, s.clone()));
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
            last_seek_scratch: Vec::with_capacity(0),
            num_top: 0,
            num_subs: 0,
            current: vec![],
            current_is_none: true,
        }
    }

    /// The terms array must be newly created TermIterator,
    pub fn reset(&mut self, term_iter_index: Vec<TermIteratorIndex>) -> Result<()> {
        debug_assert!(term_iter_index.len() <= self.top_indexes.len());
        self.num_subs = 0;
        self.num_top = 0;
        self.queue.queue.clear();
        for mut term_index in term_iter_index {
            if let Some(term) = term_index.terms.next()? {
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
        // TODO
        //        if self.queue.queue.is_empty() {
        //            return EmptyTermIterator{}
        //        } else {
        //            return self
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

impl TermIterator for MultiTermIterator {
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
            } else {
                if status == SeekStatus::NotFound {
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

    fn postings_with_flags(&mut self, flags: i16) -> Result<Box<PostingIterator>> {
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
        Ok(Box::new(docs_iter))
    }

    fn as_any(&self) -> &Any {
        self
    }
}

struct TermMergeQueue {
    queue: BinaryHeapPub<TIWSRefWithIndex>,
    stack: Vec<usize>,
}

impl TermMergeQueue {
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
struct TIWSRefWithIndex {
    term_slice: *mut TermIteratorWithSlice,
    index: usize,
}

impl TIWSRefWithIndex {
    fn new(term_slice: &mut TermIteratorWithSlice, index: usize) -> Self {
        TIWSRefWithIndex { term_slice, index }
    }

    fn slice(&self) -> &mut TermIteratorWithSlice {
        unsafe { &mut *self.term_slice }
    }
}

impl Eq for TIWSRefWithIndex {}

impl PartialEq for TIWSRefWithIndex {
    fn eq(&self, other: &TIWSRefWithIndex) -> bool {
        self.slice().current == other.slice().current
    }
}

impl Ord for TIWSRefWithIndex {
    // reverse ord for binary heap
    fn cmp(&self, other: &Self) -> Ordering {
        unsafe { (*other.term_slice).current.cmp(&(*self.term_slice).current) }
    }
}

impl PartialOrd for TIWSRefWithIndex {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub struct TermIteratorWithSlice {
    sub_slice: ReaderSlice,
    pub terms: Option<Box<TermIterator>>,
    current: Vec<u8>,
    pub index: usize,
}

impl TermIteratorWithSlice {
    fn new(index: usize, sub_slice: ReaderSlice) -> Self {
        TermIteratorWithSlice {
            sub_slice,
            index,
            terms: None,
            current: Vec::with_capacity(0),
        }
    }

    fn reset(&mut self, terms: Box<TermIterator>, term: Vec<u8>) {
        self.terms = Some(terms);
        self.current = term;
    }
}
