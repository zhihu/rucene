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

use core::util::{fill_slice, BytesRef};

/// Class that Posting and PostingVector use to write byte
/// streams into shared fixed-size bytes arrays.  The idea
/// is to allocate slices of increasing lengths For
/// example, the first slice is 5 bytes, the next slice is
/// 14, etc.  We start by writing our bytes into the first
/// 5 bytes.  When we hit the end of the slice, we allocate
/// the next slice and then write the address of the new
/// slice into the last 4 bytes of the previous slice (the
/// "forwarding address").
///
/// Each slice is filled with 0's initially, and we mark
/// the end with a non-zero byte.  This way the methods
/// that are writing into the slice don't need to record
/// its length and instead allocate a new slice once they
/// hit a non-zero byte.
pub struct ByteBlockPool {
    /// array of buffers currently used in the pool. Buffers are allocated if
    /// needed don't modify this outside of this class.
    pub buffers: Vec<Vec<u8>>,
    /// index into the buffers array pointing to the current buffer used as the head
    pub buffer_upto: isize,
    /// Where we are in head buffer
    pub byte_upto: usize,
    /// Current head offset
    pub byte_offset: isize,
    allocator: Box<dyn ByteBlockAllocator>,
}

impl Default for ByteBlockPool {
    fn default() -> Self {
        let mut buffers = Vec::with_capacity(10);
        for _ in 0..10 {
            buffers.push(vec![]);
        }
        ByteBlockPool {
            buffers,
            buffer_upto: -1,
            byte_upto: Self::BYTE_BLOCK_SIZE,
            byte_offset: -(Self::BYTE_BLOCK_SIZE as isize),
            allocator: Box::new(DirectAllocator::default()),
        }
    }
}

impl ByteBlockPool {
    pub const BYTE_BLOCK_SHIFT: usize = 15;
    pub const BYTE_BLOCK_SIZE: usize = 1 << Self::BYTE_BLOCK_SHIFT;
    pub const BYTE_BLOCK_MASK: usize = Self::BYTE_BLOCK_SIZE - 1;

    // Size of each slice.  These arrays should be at most 16
    // elements (index is encoded with 4 bits).  First array
    // is just a compact way to encode X+1 with a max.  Second
    // array is the length of each slice, ie first slice is 5
    // bytes, next slice is 14 bytes, etc.

    /// An array holding the offset into the {@link ByteBlockPool#LEVEL_SIZE_ARRAY}
    /// to quickly navigate to the next slice level.
    pub const NEXT_LEVEL_ARRAY: [usize; 10] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 9];

    /// An array holding the level sizes for byte slices.
    pub const LEVEL_SIZE_ARRAY: [usize; 10] = [5, 14, 20, 30, 40, 40, 80, 80, 120, 120];

    pub const FIRST_LEVEL_SIZE: usize = Self::LEVEL_SIZE_ARRAY[0];
}

impl ByteBlockPool {
    pub fn new(allocator: Box<dyn ByteBlockAllocator>) -> Self {
        let mut buffers = Vec::with_capacity(10);
        for _ in 0..10 {
            buffers.push(vec![]);
        }
        ByteBlockPool {
            buffers,
            buffer_upto: -1,
            byte_upto: Self::BYTE_BLOCK_SIZE,
            byte_offset: -(Self::BYTE_BLOCK_SIZE as isize),
            allocator,
        }
    }

    /// Expert: Resets the pool to its initial state reusing the first buffer.
    pub fn reset(&mut self, zero_fill_buffers: bool, reuse_first: bool) {
        if self.buffer_upto > -1 {
            // We allocated at lease one buffer
            if zero_fill_buffers {
                for i in 0..self.buffer_upto as usize {
                    fill_slice(&mut self.buffers[i], 0);
                }
                fill_slice(
                    &mut self.buffers[self.buffer_upto as usize][0..self.byte_upto],
                    0,
                );
            }

            if self.buffer_upto > 0 || !reuse_first {
                let offset = if reuse_first { 1 } else { 0 };
                // Recycle all but the first buffer
                self.allocator.recycle_byte_blocks(
                    &mut self.buffers,
                    offset,
                    (1 + self.buffer_upto) as usize,
                );
                for i in offset..(1 + self.buffer_upto) as usize {
                    self.buffers[i] = Vec::with_capacity(0);
                }
            }

            if reuse_first {
                // Re-use the first buffer
                self.buffer_upto = 0;
                self.byte_upto = 0;
                self.byte_offset = 0;
            } else {
                self.buffer_upto = -1;
                self.byte_upto = Self::BYTE_BLOCK_SIZE;
                self.byte_offset = -(Self::BYTE_BLOCK_SIZE as isize);
            }
        }
    }

    pub fn current_buffer(&mut self) -> &mut [u8] {
        debug_assert!(self.buffer_upto >= 0);
        &mut self.buffers[self.buffer_upto as usize]
    }

    /// Advances the pool to its next buffer. This method should be called once
    /// after the constructor to initialize the pool. In contrast to the
    /// constructor a {@link ByteBlockPool#reset()} call will advance the pool to
    /// its first buffer immediately.
    pub fn next_buffer(&mut self) {
        let idx = (self.buffer_upto + 1) as usize;
        if idx == self.buffers.len() {
            self.buffers.push(self.allocator.byte_block());
        } else {
            self.buffers[idx] = self.allocator.byte_block();
        }
        self.buffer_upto += 1;

        self.byte_upto = 0;
        self.byte_offset += Self::BYTE_BLOCK_SIZE as isize;
    }

    /// Allocates a new slice with the given size.
    pub fn new_slice(&mut self, size: usize) -> usize {
        if self.byte_upto > Self::BYTE_BLOCK_SIZE - size {
            self.next_buffer();
        }
        let upto = self.byte_upto;
        self.byte_upto += size;
        self.buffers[self.buffer_upto as usize][self.byte_upto - 1] = 16;
        upto
    }

    /// Creates a new byte slice with the given starting size and
    /// returns the slices offset in the pool.
    pub fn alloc_slice(&mut self, slice_index: usize, upto: usize) -> usize {
        let level = self.buffers[slice_index][upto] & 15;
        let new_level = Self::NEXT_LEVEL_ARRAY[level as usize];
        let new_size = Self::LEVEL_SIZE_ARRAY[new_level];

        // Maybe allocate another block
        if self.byte_upto > Self::BYTE_BLOCK_SIZE - new_size {
            self.next_buffer();
        }

        let new_upto = self.byte_upto;
        let offset = (new_upto as isize + self.byte_offset) as usize;
        self.byte_upto += new_size;

        // Copy ofrward the past 3 bytes (which we are about
        // to overwrite with the forwarding address):
        self.buffers[self.buffer_upto as usize][new_upto] = self.buffers[slice_index][upto - 3];
        self.buffers[self.buffer_upto as usize][new_upto + 1] = self.buffers[slice_index][upto - 2];
        self.buffers[self.buffer_upto as usize][new_upto + 2] = self.buffers[slice_index][upto - 1];

        // Write forwarding address at end of last slice:
        self.buffers[slice_index][upto - 3] = (offset >> 24) as u8;
        self.buffers[slice_index][upto - 2] = (offset >> 16) as u8;
        self.buffers[slice_index][upto - 1] = (offset >> 8) as u8;
        self.buffers[slice_index][upto] = offset as u8;

        // Write new level
        self.buffers[self.buffer_upto as usize][self.byte_upto - 1] = (16 | new_level) as u8;

        new_upto + 3
    }

    // Fill in a BytesRef from term's length & bytes encoded in
    // byte block
    pub fn set_bytes_ref(&self, text_start: usize) -> BytesRef {
        let bytes: &[u8] = &self.buffers[text_start >> Self::BYTE_BLOCK_SHIFT];
        let pos = text_start & Self::BYTE_BLOCK_MASK;

        let offset: usize;
        let length: usize;
        if bytes[pos] & 0x80u8 == 0 {
            // length is 1 byte
            length = bytes[pos] as usize;
            offset = pos + 1;
        } else {
            // length is 2 bytes
            length = (bytes[pos] as usize & 0x7f) + ((bytes[pos + 1] as usize) << 7);
            offset = pos + 2;
        }

        BytesRef::new(&bytes[offset..offset + length])
    }

    pub fn read_bytes(&self, offset: usize, bytes: &mut [u8], off: usize, length: usize) {
        if length == 0 {
            return;
        }

        let mut bytes_offset = off;
        let mut bytes_length = length;
        let mut buffer_index = offset >> Self::BYTE_BLOCK_SHIFT;
        let mut pos = offset & Self::BYTE_BLOCK_MASK;
        let overflow = (pos + length) as isize - Self::BYTE_BLOCK_SIZE as isize;
        loop {
            if overflow <= 0 {
                bytes[bytes_offset..bytes_offset + bytes_length]
                    .copy_from_slice(&self.buffers[buffer_index][pos..pos + bytes_length]);
                break;
            } else {
                let bytes_copy = length - overflow as usize;
                bytes[bytes_offset..bytes_offset + bytes_copy]
                    .copy_from_slice(&self.buffers[buffer_index][pos..pos + bytes_copy]);
                pos = 0;
                bytes_length -= bytes_copy;
                bytes_offset += bytes_copy;
                buffer_index += 1;
            }
        }
    }

    pub fn append(&mut self, bytes: &BytesRef) {
        let mut length = bytes.len();
        if length == 0 {
            return;
        }
        let mut offset = 0;
        loop {
            let start = self.byte_upto;
            if length + self.byte_upto <= Self::BYTE_BLOCK_SIZE {
                self.current_buffer()[start..start + bytes.len()]
                    .copy_from_slice(&bytes.bytes()[offset..offset + length]);
                self.byte_upto += length;
                break;
            } else {
                let bytes_copy = Self::BYTE_BLOCK_SIZE - self.byte_upto;
                if bytes_copy > 0 {
                    self.current_buffer()[start..start + bytes_copy].copy_from_slice(bytes.bytes());
                    debug_assert!(bytes_copy > 0);
                    offset += bytes_copy;
                    length -= bytes_copy;
                }
                self.next_buffer();
            }
        }
    }

    /// Set the given {@link BytesRef} so that its content is equal to the
    /// {@code ref.length} bytes starting at {@code offset}. Most of the time this
    /// method will set pointers to internal data-structures. However, in case a
    /// value crosses a boundary, a fresh copy will be returned.
    /// On the contrary to {@link #setBytesRef(BytesRef, int)}, this does not
    /// expect the length to be encoded with the data.
    pub fn set_raw_bytes_ref(&self, bytes: &mut [u8], offset: usize) {
        let length = bytes.len();
        self.read_bytes(offset, bytes, 0, length);
    }

    /// Read a single byte at the given offset
    pub fn read_byte(&self, offset: usize) -> u8 {
        let buffer_index = offset >> Self::BYTE_BLOCK_SHIFT;
        let pos = offset & Self::BYTE_BLOCK_MASK;
        self.buffers[buffer_index][pos]
    }
}

/// Abstract class for allocating and freeing byte blocks
pub trait ByteBlockAllocator {
    fn block_size(&self) -> usize;

    fn recycle_byte_blocks(&mut self, blocks: &mut [Vec<u8>], start: usize, end: usize);

    fn byte_block(&mut self) -> Vec<u8> {
        vec![0u8; self.block_size()]
    }

    fn shallow_copy(&self) -> Box<dyn ByteBlockAllocator>;
}

pub struct DirectAllocator {
    block_size: usize,
}

impl DirectAllocator {
    pub fn new(block_size: usize) -> Self {
        DirectAllocator { block_size }
    }
}

impl Default for DirectAllocator {
    fn default() -> Self {
        Self::new(ByteBlockPool::BYTE_BLOCK_SIZE)
    }
}

impl ByteBlockAllocator for DirectAllocator {
    fn block_size(&self) -> usize {
        self.block_size
    }

    fn recycle_byte_blocks(&mut self, _blocks: &mut [Vec<u8>], _start: usize, _end: usize) {}

    fn shallow_copy(&self) -> Box<dyn ByteBlockAllocator> {
        Box::new(DirectAllocator::new(self.block_size))
    }
}

/// A simple `Allocator` that never recycles, but tracks how much total RAM is in use.
pub struct DirectTrackingAllocator {
    block_size: usize,
}

impl DirectTrackingAllocator {
    pub fn new() -> Self {
        DirectTrackingAllocator {
            block_size: ByteBlockPool::BYTE_BLOCK_SIZE,
        }
    }
}

impl ByteBlockAllocator for DirectTrackingAllocator {
    fn block_size(&self) -> usize {
        self.block_size
    }

    fn recycle_byte_blocks(&mut self, blocks: &mut [Vec<u8>], start: usize, end: usize) {
        for i in start..end {
            blocks[i] = vec![];
        }
    }

    fn byte_block(&mut self) -> Vec<u8> {
        vec![0u8; self.block_size]
    }

    fn shallow_copy(&self) -> Box<dyn ByteBlockAllocator> {
        Box::new(DirectTrackingAllocator {
            block_size: self.block_size,
        })
    }
}
