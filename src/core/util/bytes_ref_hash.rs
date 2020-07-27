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

use core::util::byte_block_pool::ByteBlockPool;
use core::util::math;
use core::util::sorter::{MSBRadixSorter, MSBSorter, Sorter};
use core::util::BytesRef;
use core::util::{fill_slice, over_size};

use fasthash::murmur3;

use std::cmp::Ordering;

pub const DEFAULT_CAPACITY: usize = 16;

/// {@link BytesRefHash} is a special purpose hash-map like data-structure
/// optimized for {@link BytesRef} instances. BytesRefHash maintains mappings of
/// byte arrays to ids (Map&lt;BytesRef,int&gt;) storing the hashed bytes
/// efficiently in continuous storage. The mapping to the id is
/// encapsulated inside {@link BytesRefHash} and is guaranteed to be increased
/// for each added {@link BytesRef}.
///
/// Note: The maximum capacity {@link BytesRef} instance passed to
/// {@link #add(BytesRef)} must not be longer than {@link ByteBlockPool#BYTE_BLOCK_SIZE}-2.
/// The internal storage is limited to 2GB total byte storage.
pub struct BytesRefHash {
    pub pool: *mut ByteBlockPool,
    // TODO BytesRef
    hash_size: usize,
    hash_half_size: usize,
    hash_mask: usize,
    count: usize,
    last_count: isize,
    pub ids: Vec<i32>,
    pub bytes_start_array: Box<dyn BytesStartArray>,
}

impl BytesRefHash {
    pub fn with_pool(pool: &mut ByteBlockPool) -> Self {
        Self::new(
            pool,
            DEFAULT_CAPACITY,
            Box::new(DirectByteStartArray::with_size(DEFAULT_CAPACITY)),
        )
    }

    pub fn new(
        pool: &mut ByteBlockPool,
        capacity: usize,
        bytes_start_array: Box<dyn BytesStartArray>,
    ) -> Self {
        BytesRefHash {
            pool,
            hash_size: capacity,
            hash_half_size: capacity >> 1,
            hash_mask: capacity - 1,
            count: 0,
            last_count: -1,
            ids: vec![-1; capacity],
            bytes_start_array,
        }
    }

    pub fn byte_pool(&self) -> &ByteBlockPool {
        unsafe { &*self.pool }
    }

    fn pool_mut(&mut self) -> &mut ByteBlockPool {
        unsafe { &mut *self.pool }
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get(&self, bytes_id: usize) -> BytesRef {
        assert!(bytes_id < self.bytes_start_array.bytes().len());
        let id = self.bytes_start_array.bytes()[bytes_id];
        self.byte_pool().set_bytes_ref(id as usize)
    }

    /// Returns the ids array in arbitrary order. Valid ids start at offset of 0
    /// and end at a limit of {@link #size()} - 1
    /// Note: This is a destructive operation. {@link #clear()} must be called in
    /// order to reuse this {@link BytesRefHash} instance.
    fn compact(&mut self) -> &mut [i32] {
        let mut upto = 0;

        for i in 0..self.hash_size {
            if self.ids[i] != -1 {
                if upto < i {
                    self.ids[upto] = self.ids[i];
                    self.ids[i] = -1;
                }
                upto += 1;
            }
        }
        assert_eq!(upto, self.count);
        self.last_count = self.count as isize;
        &mut self.ids
    }

    pub fn sort(&mut self) {
        self.compact();
        let count = self.count as i32;
        let sorter = BytesRefStringMSBSorter::new(self);
        let mut msb_sorter = MSBRadixSorter::new(i32::max_value(), sorter);
        msb_sorter.sort(0, count);
    }

    fn equals(&self, id: i32, b: &BytesRef) -> bool {
        let text_start = self.byte_start(id as usize);
        let bytes = self.byte_pool().set_bytes_ref(text_start);
        bytes.eq(b)
    }

    fn shrink(&mut self, target_size: usize) -> bool {
        let mut new_size = self.hash_size;
        loop {
            if new_size < 8 || new_size >> 2 <= target_size {
                break;
            }
            new_size >>= 1;
        }

        if new_size != self.hash_size {
            self.hash_size = new_size;
            self.ids.truncate(new_size);
            fill_slice(&mut self.ids, -1);
            self.hash_half_size = new_size >> 1;
            self.hash_mask = new_size - 1;
            true
        } else {
            false
        }
    }

    pub fn clear(&mut self, reset_pool: bool) {
        self.last_count = self.count as isize;
        self.count = 0;
        if reset_pool {
            self.pool_mut().reset(false, false); // we don't need to 0-fill the buffers
        }
        self.bytes_start_array.clear();
        let last_count = self.last_count;
        if last_count != -1 && self.shrink(last_count as usize) {
            // shrink clears the hash entries
            return;
        }
        fill_slice(&mut self.ids, -1);
    }

    pub fn close(&mut self) {
        self.clear(true);
        self.ids = Vec::with_capacity(0);
    }

    pub fn add(&mut self, bytes: &BytesRef) -> i32 {
        let length = bytes.len();
        // final position
        let hash_pos = self.find_hash(bytes);
        let mut e = self.ids[hash_pos];

        if e == -1 {
            // new entry
            let len2 = 2 + bytes.len();
            if len2 + self.byte_pool().byte_upto > ByteBlockPool::BYTE_BLOCK_SIZE {
                assert!(len2 <= ByteBlockPool::BYTE_BLOCK_SIZE);
                // this length check already done in the caller func
                //                if len2 > BYTE_BLOCK_SIZE {
                // bail!("bytes can be at most: {}, got: {}", BYTE_BLOCK_SIZE -
                // 2, bytes.len());                }
                self.pool_mut().next_buffer();
            }
            let buffer_upto = self.byte_pool().byte_upto as isize;
            if self.count >= self.bytes_start_array.bytes().len() {
                self.bytes_start_array.grow();
                debug_assert!(self.count < self.bytes_start_array.bytes().len() + 1)
            }
            e = self.count as i32;
            self.count += 1;
            self.bytes_start_array.bytes_mut()[e as usize] =
                (buffer_upto + self.byte_pool().byte_offset) as u32;

            // We first encode the length, followed by the
            // bytes. Length is encoded as vInt, but will consume
            // 1 or 2 bytes at most (we reject too-long terms,
            // above).
            let start = if length < 128 {
                // 1 byte to store length
                self.pool_mut().current_buffer()[buffer_upto as usize] = length as u8;
                self.pool_mut().byte_upto += length + 1;
                (buffer_upto + 1) as usize
            } else {
                // 2 bytes to store length
                self.pool_mut().current_buffer()[buffer_upto as usize] =
                    (0x80 | (length & 0x7f)) as u8;
                self.pool_mut().current_buffer()[buffer_upto as usize + 1] =
                    ((length >> 7) & 0xff) as u8;
                self.pool_mut().byte_upto += length + 2;
                (buffer_upto + 2) as usize
            };
            self.pool_mut().current_buffer()[start..start + length].copy_from_slice(bytes.bytes());
            assert_eq!(self.ids[hash_pos], -1);
            self.ids[hash_pos] = e;

            if self.count == self.hash_half_size {
                let new_size = self.hash_size * 2;
                self.rehash(new_size, true);
            }
            e
        } else {
            -(e + 1)
        }
    }

    /// Adds a "arbitrary" int offset instead of a BytesRef
    /// term.  This is used in the indexer to hold the hash for term
    /// vectors, because they do not redundantly store the bytes term
    /// directly and instead reference the bytes term
    /// already stored by the postings BytesRefHash.  See
    /// add(int textStart) in TermsHashPerField.
    pub fn add_by_pool_offset(&mut self, offset: usize) -> i32 {
        let mut code = offset;
        let mut hash_pos = offset & self.hash_mask;
        let mut e = self.ids[hash_pos];
        if e != -1 && self.bytes_start_array.bytes()[e as usize] as usize != offset {
            // Conflict; use linear probe to find an open slot
            // (see LUCENE-5604):
            loop {
                code += 1;
                hash_pos = code & self.hash_mask;
                e = self.ids[hash_pos];
                if e == -1 || self.bytes_start_array.bytes()[e as usize] as usize == offset {
                    break;
                }
            }
        }

        if e == -1 {
            // new entry
            if self.count >= self.bytes_start_array.bytes().len() {
                self.bytes_start_array.grow();
            }
            e = self.count as i32;
            self.count += 1;
            self.bytes_start_array.bytes_mut()[e as usize] = offset as u32;
            debug_assert_eq!(self.ids[hash_pos], -1);
            self.ids[hash_pos] = e;

            if self.count == self.hash_half_size {
                let new_size = self.hash_size * 2;
                self.rehash(new_size, false);
            }
            e
        } else {
            -(e + 1)
        }
    }

    fn find_hash(&self, bytes: &BytesRef) -> usize {
        let mut code = self.do_hash(bytes) as usize;

        // final position
        let mut hash_pos = code & self.hash_mask;
        let mut e = self.ids[hash_pos];
        if e != -1 && !self.equals(e, bytes) {
            // Conflict; use linear probe to find an open slot
            // (see LUCENE-5604):
            loop {
                code += 1;
                hash_pos = code & self.hash_mask;
                e = self.ids[hash_pos];
                if e == -1 || self.equals(e, bytes) {
                    break;
                }
            }
        }
        hash_pos
    }

    fn do_hash(&self, bytes: &BytesRef) -> u32 {
        murmur3::hash32(bytes)
    }

    fn rehash(&mut self, new_size: usize, hash_on_data: bool) {
        let new_mask = new_size - 1;
        let mut new_hash = vec![-1i32; new_size];
        for i in 0..self.hash_size {
            let e0 = self.ids[i];
            if e0 != -1 {
                let mut code = if hash_on_data {
                    let off = self.bytes_start_array.bytes_mut()[e0 as usize] as usize;
                    let start = off & ByteBlockPool::BYTE_BLOCK_MASK;
                    let bytes_idx = off >> ByteBlockPool::BYTE_BLOCK_SHIFT;
                    let (len, pos) = if self.byte_pool().buffers[bytes_idx][start] & 0x80 == 0 {
                        (
                            self.byte_pool().buffers[bytes_idx][start] as usize,
                            start + 1,
                        )
                    } else {
                        (
                            (self.byte_pool().buffers[bytes_idx][start] & 0x7f) as usize
                                | ((self.byte_pool().buffers[bytes_idx][start + 1] as usize) << 7),
                            start + 2,
                        )
                    };
                    self.do_hash(&BytesRef::new(
                        &self.byte_pool().buffers[bytes_idx][pos..pos + len],
                    )) as usize
                } else {
                    self.bytes_start_array.bytes_mut()[e0 as usize] as usize
                };

                let mut hash_pos = code & new_mask;
                if new_hash[hash_pos] != -1 {
                    // Conflict; use linear probe to find an open slot
                    // (see LUCENE-5604):
                    loop {
                        code += 1;
                        hash_pos = code & new_mask;
                        if new_hash[hash_pos] == -1 {
                            break;
                        }
                    }
                }
                new_hash[hash_pos] = e0;
            }
        }

        self.hash_mask = new_mask;
        self.ids = new_hash;
        self.hash_size = new_size;
        self.hash_half_size = new_size >> 1;
    }

    pub fn reinit(&mut self) {
        if self.bytes_start_array.bytes().is_empty() {
            self.bytes_start_array.init();
        }

        if self.ids.is_empty() {
            self.ids = vec![0i32; self.hash_size];
        }
    }

    pub fn byte_start(&self, id: usize) -> usize {
        self.bytes_start_array.bytes()[id] as usize
    }
}

struct BytesRefStringMSBSorter<'a> {
    bytes_hash: &'a mut BytesRefHash,
    // compact: &[i32]  // bytes_hash.ids after compacted
}

impl<'a> BytesRefStringMSBSorter<'a> {
    fn new(bytes_hash: &'a mut BytesRefHash) -> Self {
        bytes_hash.compact();
        BytesRefStringMSBSorter { bytes_hash }
    }

    fn get(&self, i: i32) -> BytesRef {
        self.bytes_hash.byte_pool().set_bytes_ref(
            self.bytes_hash
                .byte_start(self.bytes_hash.ids[i as usize] as usize) as usize,
        )
    }
}

impl<'a> MSBSorter for BytesRefStringMSBSorter<'a> {
    type Fallback = BytesRefStringMSBFallbackSorter<'a>;
    fn byte_at(&self, i: i32, k: i32) -> Option<u8> {
        let bytes = self.get(i);
        if bytes.len() <= k as usize {
            None
        } else {
            Some(bytes.byte_at(k as usize))
        }
    }

    fn msb_swap(&mut self, i: i32, j: i32) {
        self.bytes_hash.ids.swap(i as usize, j as usize)
    }

    fn fallback_sorter(&mut self, k: i32) -> BytesRefStringMSBFallbackSorter<'a> {
        let dummy = [0u8; 1];
        BytesRefStringMSBFallbackSorter {
            sorter: self,
            pivot: BytesRef::new(&dummy),
            // dummy init, never used
            k,
        }
    }
}

// TODO: the lifetime parameter of `sorter` can't be determined at Line#123
// thus, we have to use raw pointer to work around
// maybe we can't fix this unsafe use in the coming new version of Rust
struct BytesRefStringMSBFallbackSorter<'a> {
    sorter: *mut BytesRefStringMSBSorter<'a>,
    pivot: BytesRef,
    k: i32,
}

impl<'a> BytesRefStringMSBFallbackSorter<'a> {
    fn get(&self, i: i32, k: i32) -> BytesRef {
        let bytes = unsafe { (&*self.sorter).get(i) };
        BytesRef::new(&bytes.bytes()[k as usize..])
    }
}

impl<'a> Sorter for BytesRefStringMSBFallbackSorter<'a> {
    fn compare(&mut self, i: i32, j: i32) -> Ordering {
        let scratch1 = self.get(i, self.k);
        let scratch2 = self.get(j, self.k);
        scratch1.cmp(&scratch2)
    }

    fn swap(&mut self, i: i32, j: i32) {
        unsafe { (&mut *self.sorter).msb_swap(i, j) }
    }

    fn sort(&mut self, from: i32, to: i32) {
        assert!(from <= to);
        self.quick_sort(from, to, 2 * math::log((to - from) as i64, 2))
    }

    fn set_pivot(&mut self, i: i32) {
        let bytes = self.get(i, self.k);
        self.pivot = bytes;
    }

    fn compare_pivot(&mut self, j: i32) -> Ordering {
        let bytes = self.get(j, self.k);
        self.pivot.cmp(&bytes)
    }
}

/// Manages allocation of the per-term addresses
pub trait BytesStartArray {
    fn bytes_mut(&mut self) -> &mut [u32];

    fn bytes(&self) -> &[u32];

    fn init(&mut self);

    fn grow(&mut self);

    fn clear(&mut self);
}

/// A simple BytesStartArray that tracks memory allocation
/// using a private Counter instance.
pub struct DirectByteStartArray {
    init_size: usize,
    pub bytes_start: Vec<u32>,
}

impl DirectByteStartArray {
    pub fn new(init_size: usize) -> Self {
        DirectByteStartArray {
            init_size,
            bytes_start: Vec::with_capacity(0),
        }
    }

    pub fn with_size(init_size: usize) -> Self {
        Self::new(init_size)
    }
}

impl BytesStartArray for DirectByteStartArray {
    fn bytes_mut(&mut self) -> &mut [u32] {
        &mut self.bytes_start
    }

    fn bytes(&self) -> &[u32] {
        &self.bytes_start
    }

    fn init(&mut self) {
        self.bytes_start = vec![0u32; self.init_size];
    }

    fn grow(&mut self) {
        let new_length = over_size(self.bytes_start.len() + 1);
        self.bytes_start.resize(new_length, 0u32);
    }

    fn clear(&mut self) {
        self.bytes_start = Vec::with_capacity(0);
    }
}
