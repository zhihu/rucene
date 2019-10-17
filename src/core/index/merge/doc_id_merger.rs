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

use core::index::merge::{DocMap, LiveDocsDocMap};
use core::search::NO_MORE_DOCS;
use core::util::DocId;

use error::Result;

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;

/// Utility trait to help merging documents from sub-readers according to either simple
/// concatenated (unsorted) order, or by a specified index-time sort, skipping
/// deleted documents and remapping non-deleted documents.
pub trait DocIdMerger {
    type Sub: DocIdMergerSub;

    /// Reuse API, currently only used by postings during merge
    fn reset(&mut self) -> Result<()>;

    /// Return None when done
    fn next(&mut self) -> Result<Option<&mut Self::Sub>>;
}

pub fn doc_id_merger_of_count<T: DocIdMergerSub>(
    subs: Vec<T>,
    max_count: usize,
    index_sorted: bool,
) -> Result<DocIdMergerEnum<T>> {
    if index_sorted && max_count > 1 {
        Ok(DocIdMergerEnum::Sorted(SortedDocIdMerger::new(subs)?))
    } else {
        Ok(DocIdMergerEnum::Sequential(SequentialDocIdMerger::new(
            subs,
        )))
    }
}

pub fn doc_id_merger_of<T: DocIdMergerSub>(
    subs: Vec<T>,
    index_sorted: bool,
) -> Result<DocIdMergerEnum<T>> {
    let length = subs.len();
    doc_id_merger_of_count(subs, length, index_sorted)
}

#[derive(Clone)]
pub struct DocIdMergerSubBase {
    pub mapped_doc_id: DocId,
    pub doc_map: Arc<LiveDocsDocMap>,
}

impl DocIdMergerSubBase {
    pub fn new(doc_map: Arc<LiveDocsDocMap>) -> Self {
        DocIdMergerSubBase {
            mapped_doc_id: 0,
            doc_map,
        }
    }

    pub fn reset(&mut self) {
        self.mapped_doc_id = 0;
    }
}

/// Represents one sub-reader being merged
pub trait DocIdMergerSub {
    fn next_doc(&mut self) -> Result<DocId>;

    fn base(&self) -> &DocIdMergerSubBase;

    fn base_mut(&mut self) -> &mut DocIdMergerSubBase;

    fn reset(&mut self);
}

pub enum DocIdMergerEnum<T: DocIdMergerSub> {
    Sequential(SequentialDocIdMerger<T>),
    Sorted(SortedDocIdMerger<T>),
}

impl<T: DocIdMergerSub> DocIdMergerEnum<T> {
    pub fn subs_mut(&mut self) -> &mut Vec<T> {
        match self {
            DocIdMergerEnum::Sequential(s) => &mut s.subs,
            DocIdMergerEnum::Sorted(s) => &mut s.subs,
        }
    }

    pub fn subs(&self) -> &[T] {
        match self {
            DocIdMergerEnum::Sequential(s) => &s.subs,
            DocIdMergerEnum::Sorted(s) => &s.subs,
        }
    }

    pub fn reset(&mut self) -> Result<()> {
        match self {
            DocIdMergerEnum::Sequential(s) => s.reset(),
            DocIdMergerEnum::Sorted(s) => s.reset(),
        }
    }
}

impl<T: DocIdMergerSub> DocIdMerger for DocIdMergerEnum<T> {
    type Sub = T;

    /// Reuse API, currently only used by postings during merge
    fn reset(&mut self) -> Result<()> {
        match self {
            DocIdMergerEnum::Sequential(s) => s.reset(),
            DocIdMergerEnum::Sorted(s) => s.reset(),
        }
    }

    /// Return None when done
    fn next(&mut self) -> Result<Option<&mut T>> {
        match self {
            DocIdMergerEnum::Sequential(s) => s.next(),
            DocIdMergerEnum::Sorted(s) => s.next(),
        }
    }
}

pub struct SequentialDocIdMerger<T: DocIdMergerSub> {
    subs: Vec<T>,
    current_index: usize,
    next_index: usize,
}

impl<T: DocIdMergerSub> SequentialDocIdMerger<T> {
    fn new(subs: Vec<T>) -> Self {
        let mut merger = SequentialDocIdMerger {
            subs,
            current_index: 0,
            next_index: 0,
        };
        merger.reset().unwrap();
        merger
    }
}

impl<T: DocIdMergerSub> DocIdMerger for SequentialDocIdMerger<T> {
    type Sub = T;

    fn reset(&mut self) -> Result<()> {
        for s in &mut self.subs {
            s.reset();
        }
        self.current_index = 0;
        if self.subs.is_empty() {
            self.next_index = 0;
        } else {
            self.next_index = 1;
        }
        Ok(())
    }

    fn next(&mut self) -> Result<Option<&mut T>> {
        if self.current_index >= self.subs.len() {
            // NOTE: it's annoying that caller is allowed to call us
            // again even after we returned null before
            Ok(None)
        } else {
            loop {
                let idx = self.current_index;
                let doc_id = self.subs[idx].next_doc()?;
                if doc_id == NO_MORE_DOCS {
                    if self.next_index == self.subs.len() {
                        self.current_index = self.subs.len();
                        return Ok(None);
                    }
                    self.current_index = self.next_index;
                    self.next_index += 1;
                    continue;
                }
                let mapped_doc_id = self.subs[idx].base().doc_map.get(doc_id)?;
                if mapped_doc_id != -1 {
                    self.subs[idx].base_mut().mapped_doc_id = mapped_doc_id;
                    return Ok(Some(&mut self.subs[idx]));
                }
            }
        }
    }
}

pub struct SortedDocIdMerger<T: DocIdMergerSub> {
    subs: Vec<T>,
    queue: BinaryHeap<DocIdMergerSubRef<T>>,
}

impl<T: DocIdMergerSub> SortedDocIdMerger<T> {
    fn new(subs: Vec<T>) -> Result<Self> {
        let queue = BinaryHeap::with_capacity(subs.len());
        let mut merger = SortedDocIdMerger { subs, queue };
        merger.reset()?;
        Ok(merger)
    }
}

impl<T: DocIdMergerSub> DocIdMerger for SortedDocIdMerger<T> {
    type Sub = T;

    fn reset(&mut self) -> Result<()> {
        for s in &mut self.subs {
            s.reset();
        }
        // caller may not have fully consumed the queue:
        self.queue.clear();
        let mut first = true;
        for sub in &mut self.subs {
            if first {
                // by setting mappedDocID = -1, this entry is guaranteed
                // to be the top of the queue so the first call to
                // next() will advance it
                sub.base_mut().mapped_doc_id = -1;
                first = false;
            } else {
                let mut mapped_doc_id;
                loop {
                    let doc_id = sub.next_doc()?;
                    if doc_id == NO_MORE_DOCS {
                        mapped_doc_id = NO_MORE_DOCS;
                        break;
                    }
                    mapped_doc_id = sub.base().doc_map.get(doc_id)?;
                    if mapped_doc_id != -1 {
                        break;
                    }
                }
                if mapped_doc_id == NO_MORE_DOCS {
                    // all docs in this sub were deleted; do not add it to the queue!
                    continue;
                }
                sub.base_mut().mapped_doc_id = mapped_doc_id;
            }
            self.queue.push(DocIdMergerSubRef::new(sub));
        }
        Ok(())
    }

    fn next(&mut self) -> Result<Option<&mut T>> {
        if self.queue.peek().is_none() {
            // NOTE: it's annoying that caller is allowed to call us again
            // even after we returned null before
            return Ok(None);
        }

        loop {
            let doc_id = self.queue.peek_mut().unwrap().sub().next_doc()?;
            if doc_id == NO_MORE_DOCS {
                self.queue.pop();
                break;
            }
            let top = self.queue.peek_mut().unwrap();
            let mapped_doc_id = top.sub().base().doc_map.get(doc_id)?;
            if mapped_doc_id == -1 {
                // doc was deleted
                continue;
            } else {
                top.sub().base_mut().mapped_doc_id = mapped_doc_id;
                break;
            }
        }

        // unsafe action, becase we can't call self.queue.peek_mut() to
        // treat PeekMut<T> as &mut T
        // the caller will promise the mut action won't have effect on T's orders
        if let Some(sub) = self.queue.peek() {
            Ok(Some(sub.sub()))
        } else {
            Ok(None)
        }
    }
}

struct DocIdMergerSubRef<T: DocIdMergerSub> {
    sub: *mut T,
}

impl<T: DocIdMergerSub> DocIdMergerSubRef<T> {
    fn new(sub: &mut T) -> Self {
        DocIdMergerSubRef { sub }
    }

    #[allow(clippy::mut_from_ref)]
    fn sub(&self) -> &mut T {
        unsafe { &mut *self.sub }
    }
}

impl<T: DocIdMergerSub> Eq for DocIdMergerSubRef<T> {}

impl<T: DocIdMergerSub> PartialEq for DocIdMergerSubRef<T> {
    fn eq(&self, other: &Self) -> bool {
        unsafe { (*self.sub).base().mapped_doc_id == (*other.sub).base().mapped_doc_id }
    }
}

impl<T: DocIdMergerSub> Ord for DocIdMergerSubRef<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        // reversed for binary heap
        unsafe {
            (*other.sub)
                .base()
                .mapped_doc_id
                .cmp(&(*self.sub).base().mapped_doc_id)
        }
    }
}

impl<T: DocIdMergerSub> PartialOrd for DocIdMergerSubRef<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
