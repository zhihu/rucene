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

use core::search::DocIterator;
use core::util::DocId;
use error::Result;

use std::cmp::{Ord, Ordering};
use std::ops::{Deref, DerefMut};
use std::ptr;

pub struct DisiWrapper<T: DocIterator> {
    scorer: T,
    pub doc: DocId,
    matches: Option<bool>,
    pub next: *mut DisiWrapper<T>,
    pub last_approx_match_doc: DocId,
    pub last_approx_non_match_doc: DocId,
}

impl<T: DocIterator> DisiWrapper<T> {
    pub fn next_scorer(&self) -> Option<&mut DisiWrapper<T>> {
        if self.next.is_null() {
            None
        } else {
            unsafe { Some(&mut *self.next) }
        }
    }

    pub fn new(scorer: T) -> DisiWrapper<T> {
        DisiWrapper {
            scorer,
            doc: -1,
            matches: None,
            next: ptr::null_mut(),
            last_approx_match_doc: -1,
            last_approx_non_match_doc: -1,
        }
    }

    pub fn inner(&self) -> &T {
        &self.scorer
    }

    pub fn inner_mut(&mut self) -> &mut T {
        &mut self.scorer
    }

    pub fn set_doc(&mut self, doc: DocId) {
        if self.doc != doc {
            self.matches = None;
        }
        self.doc = doc;
    }

    pub fn doc(&self) -> DocId {
        self.scorer.doc_id()
    }

    pub fn doc_id(&self) -> DocId {
        self.scorer.doc_id()
    }

    pub fn next_doc(&mut self) -> Result<DocId> {
        let doc_id = self.inner_mut().next()?;
        self.set_doc(doc_id);
        Ok(doc_id)
    }

    pub fn advance(&mut self, target: DocId) -> Result<DocId> {
        let doc_id = self.inner_mut().advance(target)?;
        self.set_doc(doc_id);
        Ok(doc_id)
    }

    pub fn matches(&mut self) -> Result<bool> {
        if self.matches.is_none() {
            self.matches = Some(self.inner_mut().matches()?);
        }

        Ok(self.matches.unwrap())
    }

    pub fn match_cost(&self) -> f32 {
        self.scorer.match_cost()
    }

    pub fn approximate_next(&mut self) -> Result<DocId> {
        let doc_id = self.inner_mut().approximate_next()?;
        self.set_doc(doc_id);
        Ok(doc_id)
    }

    pub fn approximate_advance(&mut self, target: DocId) -> Result<DocId> {
        let doc_id = self.inner_mut().approximate_advance(target)?;
        self.set_doc(doc_id);
        Ok(doc_id)
    }

    pub fn cost(&self) -> usize {
        self.scorer.cost()
    }
}

impl<T: DocIterator> Ord for DisiWrapper<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.scorer.doc_id().cmp(&other.scorer.doc_id()).reverse()
    }
}

impl<T: DocIterator> Eq for DisiWrapper<T> {}

impl<T: DocIterator> PartialEq for DisiWrapper<T> {
    fn eq(&self, other: &Self) -> bool {
        self.scorer.doc_id() == other.scorer.doc_id()
    }
}

impl<T: DocIterator> PartialOrd for DisiWrapper<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub struct DisiPriorityQueue<T: DocIterator> {
    heap: Vec<*mut DisiWrapper<T>>,
    size: usize,
    buffer: Vec<DisiWrapper<T>>,
}

unsafe impl<T: DocIterator> Send for DisiPriorityQueue<T> {}

impl<T: DocIterator> DisiPriorityQueue<T> {
    #[inline]
    fn left_node(node: usize) -> usize {
        ((node + 1) << 1) - 1
    }

    #[inline]
    fn right_node(left_node: usize) -> usize {
        left_node + 1
    }

    #[inline]
    fn parent_node(node: usize) -> Option<usize> {
        if node > 0 {
            Some(((node + 1) >> 1) - 1)
        } else {
            None
        }
    }

    pub fn new(children: Vec<T>) -> DisiPriorityQueue<T> {
        let mut buffer: Vec<DisiWrapper<T>> = children.into_iter().map(DisiWrapper::new).collect();
        let children: Vec<*mut DisiWrapper<T>> = buffer
            .iter_mut()
            .map(|x| x as *mut DisiWrapper<T>)
            .collect();
        let mut queue = DisiPriorityQueue {
            heap: vec![ptr::null_mut(); children.len()],
            size: 0,
            buffer,
        };

        for disi in children {
            unsafe {
                queue.do_push(disi);
            }
        }
        queue
    }

    pub fn size(&self) -> usize {
        self.size
    }

    /// Get the list of scorers which are on the current doc.
    pub fn top_list(&mut self) -> &mut DisiWrapper<T> {
        unsafe { &mut *self.do_top_list() }
    }

    unsafe fn do_top_list(&self) -> *mut DisiWrapper<T> {
        let heap = &self.heap;
        let size = self.size;
        let mut list = heap[0];
        (*list).next = ptr::null_mut();
        if size >= 3 {
            list = Self::top_list_to(list, heap, size, 1);
            list = Self::top_list_to(list, heap, size, 2);
        } else if size == 2 && (*heap[1]).doc() == (*list).doc() {
            list = Self::prepend(heap[1], list);
        }
        list
    }

    unsafe fn prepend(w1: *mut DisiWrapper<T>, w2: *mut DisiWrapper<T>) -> *mut DisiWrapper<T> {
        (*w1).next = w2;
        w1
    }

    unsafe fn top_list_to(
        mut list: *mut DisiWrapper<T>,
        heap: &[*mut DisiWrapper<T>],
        size: usize,
        i: usize,
    ) -> *mut DisiWrapper<T> {
        let w = heap[i];
        if (*w).doc() == (*list).doc() {
            list = Self::prepend(w, list);
            let left = Self::left_node(i);
            let right = left + 1;
            if right < size {
                list = Self::top_list_to(list, heap, size, left);
                list = Self::top_list_to(list, heap, size, right);
            } else if left < size && (*heap[left]).doc() == (*list).doc() {
                list = Self::prepend(heap[left], list);
            }
        }
        list
    }

    pub fn push(&mut self, entry: &mut DisiWrapper<T>) {
        unsafe {
            self.do_push(entry as *mut DisiWrapper<T>);
        }
    }

    #[inline(always)]
    unsafe fn do_push(&mut self, entry: *mut DisiWrapper<T>) -> *mut DisiWrapper<T> {
        self.heap[self.size] = entry;
        let size = self.size;
        self.up_heap(size);
        self.size += 1;
        self.heap[0]
    }

    pub fn pop(&mut self) -> &'static mut DisiWrapper<T> {
        unsafe { &mut *self.do_pop() }
    }

    #[inline(always)]
    unsafe fn do_pop(&mut self) -> *mut DisiWrapper<T> {
        debug_assert!(self.size > 0);
        let result = self.heap[0];
        self.size -= 1;
        let size = self.size;
        self.heap[0] = self.heap[size];
        self.heap[size] = ptr::null_mut();
        self.down_heap(size);
        result
    }

    pub fn peek(&self) -> &DisiWrapper<T> {
        unsafe { &*self.heap[0] }
    }

    pub fn peek_mut(&mut self) -> PeekMut<T> {
        assert!(!self.is_empty());
        PeekMut {
            heap: self,
            sift: true,
        }
    }

    pub fn update_top(&mut self) -> &mut DisiWrapper<T> {
        unsafe { &mut *self.do_update_top() }
    }

    unsafe fn do_update_top(&mut self) -> *mut DisiWrapper<T> {
        let size = self.size;
        self.down_heap(size);
        self.heap[0]
    }

    pub fn update_top_with(&mut self, top_replacement: &mut DisiWrapper<T>) -> &mut DisiWrapper<T> {
        unsafe { &mut *self.do_update_top_with(top_replacement as *mut DisiWrapper<T>) }
    }

    unsafe fn do_update_top_with(
        &mut self,
        top_replacement: *mut DisiWrapper<T>,
    ) -> *mut DisiWrapper<T> {
        self.heap[0] = top_replacement;
        self.update_top()
    }

    unsafe fn up_heap(&mut self, mut i: usize) {
        let node = self.heap[i];
        let node_doc = (*node).doc();
        while let Some(j) = Self::parent_node(i) {
            if node_doc >= (*self.heap[j]).doc() {
                break;
            }
            self.heap[i] = self.heap[j];
            i = j;
        }

        self.heap[i] = node;
    }

    unsafe fn down_heap(&mut self, size: usize) {
        let mut i = 0;
        let node = self.heap[0];
        let mut j = Self::left_node(i);
        if j < self.size {
            let mut k = Self::right_node(j);
            if k < size && (*self.heap[k]).doc() < (*self.heap[j]).doc() {
                j = k;
            }
            if (*self.heap[j]).doc() < (*node).doc() {
                loop {
                    self.heap[i] = self.heap[j];
                    i = j;
                    j = Self::left_node(i);
                    k = Self::right_node(j);
                    if k < size && (*self.heap[k]).doc() < (*self.heap[j]).doc() {
                        j = k;
                    }
                    if j >= size || (*self.heap[j]).doc() >= (*node).doc() {
                        break;
                    }
                }
                self.heap[i] = node;
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.size == 0
    }
}

impl<'a, T: DocIterator> IntoIterator for &'a DisiPriorityQueue<T> {
    type Item = &'a T;
    type IntoIter = DisiQueueIterator<'a, T>;

    fn into_iter(self) -> <Self as IntoIterator>::IntoIter {
        DisiQueueIterator::new(self)
    }
}

pub struct DisiQueueIterator<'a, T: 'a + DocIterator> {
    queue: &'a DisiPriorityQueue<T>,
    index: usize,
}

impl<'a, T: 'a + DocIterator> DisiQueueIterator<'a, T> {
    fn new(queue: &'a DisiPriorityQueue<T>) -> DisiQueueIterator<'a, T> {
        DisiQueueIterator { queue, index: 0 }
    }
}

impl<'a, T: 'a + DocIterator> Iterator for DisiQueueIterator<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        if self.index < self.queue.buffer.len() {
            let index = self.index;
            let v = &self.queue.buffer[index].inner();
            self.index += 1;
            Some(v)
        } else {
            None
        }
    }
}

// copy from binary heap for `DisiPriorityQueue`
pub struct PeekMut<'a, T: 'a + DocIterator> {
    heap: &'a mut DisiPriorityQueue<T>,
    sift: bool,
}

impl<'a, T: 'a + DocIterator> Drop for PeekMut<'a, T> {
    fn drop(&mut self) {
        if self.sift {
            self.heap.update_top();
        }
    }
}

impl<'a, T: DocIterator> Deref for PeekMut<'a, T> {
    type Target = DisiWrapper<T>;
    fn deref(&self) -> &DisiWrapper<T> {
        unsafe { &*self.heap.heap[0] }
    }
}

impl<'a, T: DocIterator> DerefMut for PeekMut<'a, T> {
    fn deref_mut(&mut self) -> &mut DisiWrapper<T> {
        unsafe { &mut *self.heap.heap[0] }
    }
}

impl<'a, T: DocIterator> PeekMut<'a, T> {
    /// Removes the peeked value from the heap and returns it.
    pub fn pop(mut this: PeekMut<'a, T>) -> &'static mut DisiWrapper<T> {
        let value = this.heap.pop();
        this.sift = false;
        value
    }
}
