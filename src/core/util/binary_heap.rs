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

use std::mem::swap;
use std::ops::{Deref, DerefMut};
use std::ptr;

/// a copy of std::collection BinaryHeap that can visit the inner Vec directory for efficiency

pub struct BinaryHeapPub<T> {
    data: Vec<T>,
}

pub struct PeekMut<'a, T: 'a + Ord> {
    heap: &'a mut BinaryHeapPub<T>,
    sift: bool,
}

impl<'a, T: Ord> Drop for PeekMut<'a, T> {
    fn drop(&mut self) {
        if self.sift {
            self.heap.sift_down(0);
        }
    }
}

impl<'a, T: Ord> Deref for PeekMut<'a, T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.heap.data[0]
    }
}

impl<'a, T: Ord> DerefMut for PeekMut<'a, T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.heap.data[0]
    }
}

impl<'a, T: Ord> PeekMut<'a, T> {
    /// Removes the peeked value from the heap and returns it.
    pub fn pop(mut this: PeekMut<'a, T>) -> T {
        let value = this.heap.pop().unwrap();
        this.sift = false;
        value
    }
}

impl<T: Clone> Clone for BinaryHeapPub<T> {
    fn clone(&self) -> Self {
        BinaryHeapPub {
            data: self.data.clone(),
        }
    }

    fn clone_from(&mut self, source: &Self) {
        self.data.clone_from(&source.data);
    }
}

impl<T: Ord> Default for BinaryHeapPub<T> {
    /// Creates an empty `BinaryHeap<T>`.
    #[inline]
    fn default() -> BinaryHeapPub<T> {
        BinaryHeapPub::new()
    }
}

impl<T: Ord> BinaryHeapPub<T> {
    pub fn new() -> BinaryHeapPub<T> {
        BinaryHeapPub { data: vec![] }
    }

    pub fn with_capacity(capacity: usize) -> BinaryHeapPub<T> {
        BinaryHeapPub {
            data: Vec::with_capacity(capacity),
        }
    }

    pub fn peek(&self) -> Option<&T> {
        self.data.get(0)
    }

    pub fn peek_mut(&mut self) -> Option<PeekMut<T>> {
        if self.is_empty() {
            None
        } else {
            Some(PeekMut {
                heap: self,
                sift: true,
            })
        }
    }

    pub fn capacity(&self) -> usize {
        self.data.capacity()
    }

    pub fn reserve_exact(&mut self, additional: usize) {
        self.data.reserve_exact(additional);
    }

    pub fn reserve(&mut self, additional: usize) {
        self.data.reserve(additional);
    }

    pub fn shrink_to_fit(&mut self) {
        self.data.shrink_to_fit();
    }

    pub fn pop(&mut self) -> Option<T> {
        self.data.pop().map(|mut item| {
            if !self.is_empty() {
                swap(&mut item, &mut self.data[0]);
                self.sift_down_to_bottom(0);
            }
            item
        })
    }

    pub fn push(&mut self, item: T) {
        let old_len = self.len();
        self.data.push(item);
        self.sift_up(0, old_len);
    }

    pub fn into_vec(self) -> Vec<T> {
        self.data
    }

    pub fn inner(&self) -> &[T] {
        &self.data
    }

    pub fn inner_mut(&mut self) -> &mut [T] {
        &mut self.data
    }

    fn sift_up(&mut self, start: usize, pos: usize) -> usize {
        unsafe {
            // Take out the value at `pos` and create a hole.
            let mut hole = Hole::new(&mut self.data, pos);

            while hole.pos() > start {
                let parent = (hole.pos() - 1) / 2;
                if hole.element() <= hole.get(parent) {
                    break;
                }
                hole.move_to(parent);
            }
            hole.pos()
        }
    }

    /// Take an element at `pos` and move it down the heap,
    /// while its children are larger.
    fn sift_down_range(&mut self, pos: usize, end: usize) {
        unsafe {
            let mut hole = Hole::new(&mut self.data, pos);
            let mut child = 2 * pos + 1;
            while child < end {
                let right = child + 1;
                // compare with the greater of the two children
                if right < end && hole.get(child) <= hole.get(right) {
                    child = right;
                }
                // if we are already in order, stop.
                if hole.element() >= hole.get(child) {
                    break;
                }
                hole.move_to(child);
                child = 2 * hole.pos() + 1;
            }
        }
    }

    fn sift_down(&mut self, pos: usize) {
        let len = self.len();
        self.sift_down_range(pos, len);
    }

    fn sift_down_to_bottom(&mut self, mut pos: usize) {
        let end = self.len();
        let start = pos;
        unsafe {
            let mut hole = Hole::new(&mut self.data, pos);
            let mut child = 2 * pos + 1;
            while child < end {
                let right = child + 1;
                // compare with the greater of the two children
                if right < end && hole.get(child) <= hole.get(right) {
                    child = right;
                }
                hole.move_to(child);
                child = 2 * hole.pos() + 1;
            }
            pos = hole.pos;
        }
        self.sift_up(start, pos);
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn clear(&mut self) {
        self.data.clear();
    }
}

struct Hole<'a, T: 'a> {
    data: &'a mut [T],
    /// `elt` is always `Some` from new until drop.
    elt: Option<T>,
    pos: usize,
}

impl<'a, T> Hole<'a, T> {
    /// Create a new Hole at index `pos`.
    ///
    /// Unsafe because pos must be within the data slice.
    #[inline]
    unsafe fn new(data: &'a mut [T], pos: usize) -> Self {
        debug_assert!(pos < data.len());
        let elt = ptr::read(&data[pos]);
        Hole {
            data,
            elt: Some(elt),
            pos,
        }
    }

    #[inline]
    fn pos(&self) -> usize {
        self.pos
    }

    /// Returns a reference to the element removed.
    #[inline]
    fn element(&self) -> &T {
        self.elt.as_ref().unwrap()
    }

    /// Returns a reference to the element at `index`.
    ///
    /// Unsafe because index must be within the data slice and not equal to pos.
    #[inline]
    unsafe fn get(&self, index: usize) -> &T {
        debug_assert!(index != self.pos);
        debug_assert!(index < self.data.len());
        self.data.get_unchecked(index)
    }

    /// Move hole to new location
    ///
    /// Unsafe because index must be within the data slice and not equal to pos.
    #[inline]
    unsafe fn move_to(&mut self, index: usize) {
        debug_assert!(index != self.pos);
        debug_assert!(index < self.data.len());
        let index_ptr: *const _ = self.data.get_unchecked(index);
        let hole_ptr = self.data.get_unchecked_mut(self.pos);
        ptr::copy_nonoverlapping(index_ptr, hole_ptr, 1);
        self.pos = index;
    }
}

impl<'a, T> Drop for Hole<'a, T> {
    #[inline]
    fn drop(&mut self) {
        // fill the hole again
        unsafe {
            let pos = self.pos;
            ptr::write(self.data.get_unchecked_mut(pos), self.elt.take().unwrap());
        }
    }
}
