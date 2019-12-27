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

use core::util::fill_slice;

pub const INT_BLOCK_SHIFT: usize = 13;
pub const INT_BLOCK_SIZE: usize = 1 << INT_BLOCK_SHIFT;
pub const INT_BLOCK_MASK: usize = INT_BLOCK_SIZE - 1;

// Size of each slice.  These arrays should be at most 16
// elements (index is encoded with 4 bits).  First array
// is just a compact way to encode X+1 with a max.  Second
// array is the length of each slice, ie first slice is 5
// bytes, next slice is 14 bytes, etc.

/// An array holding the offset into the {@link ByteBlockPool#LEVEL_SIZE_ARRAY}
/// to quickly navigate to the next slice level.
const NEXT_LEVEL_ARRAY: [usize; 10] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 9];

/// An array holding the level sizes for byte slices.
const LEVEL_SIZE_ARRAY: [usize; 10] = [2, 4, 8, 16, 32, 64, 128, 256, 512, 1024];

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
pub struct IntBlockPool {
    /// array of buffers currently used in the pool. Buffers are allocated if
    /// needed don't modify this outside of this class.
    pub buffers: Vec<Vec<i32>>,
    /// index into the buffers array pointing to the current buffer used as the head
    pub buffer_upto: isize,
    /// Where we are in head buffer
    pub int_upto: usize,
    /// Current head offset
    pub int_offset: isize,
    /// 2GB limit, auto flush
    pub need_flush: bool,
    allocator: Box<dyn IntAllocator>,
}

impl Default for IntBlockPool {
    fn default() -> Self {
        IntBlockPool {
            buffers: Vec::with_capacity(0),
            buffer_upto: -1,
            int_upto: INT_BLOCK_SIZE,
            int_offset: -(INT_BLOCK_SIZE as isize),
            need_flush: false,
            allocator: Box::new(DirectIntAllocator::default()),
        }
    }
}

impl IntBlockPool {
    pub fn new(allocator: Box<dyn IntAllocator>) -> Self {
        let mut buffers = Vec::with_capacity(10);
        for _ in 0..10 {
            buffers.push(vec![]);
        }
        IntBlockPool {
            buffers,
            buffer_upto: -1,
            int_upto: INT_BLOCK_SIZE,
            int_offset: -(INT_BLOCK_SIZE as isize),
            need_flush: false,
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
                    &mut self.buffers[self.buffer_upto as usize][0..self.int_upto],
                    0,
                );
            }

            if self.buffer_upto > 0 || !reuse_first {
                let offset = if reuse_first { 1 } else { 0 };
                // Recycle all but the first buffer
                self.allocator.recycle_int_blocks(
                    &mut self.buffers,
                    offset,
                    1 + self.buffer_upto as usize,
                );
                for i in offset..=self.buffer_upto as usize {
                    self.buffers[i] = Vec::with_capacity(0);
                }
            }

            if reuse_first {
                // Re-use the first buffer
                self.buffer_upto = 0;
                self.int_upto = 0;
                self.int_offset = 0;
            } else {
                self.buffer_upto = -1;
                self.int_upto = INT_BLOCK_SIZE;
                self.int_offset = -(INT_BLOCK_SIZE as isize);
            }
        }
    }

    pub fn current_buffer(&mut self) -> &mut [i32] {
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
            self.buffers.push(self.allocator.int_block());
        } else {
            self.buffers[idx] = self.allocator.int_block();
        }
        self.buffer_upto += 1;

        self.int_upto = 0;
        self.int_offset += INT_BLOCK_SIZE as isize;
    }

    /// Allocates a new slice with the given size.
    pub fn new_slice(&mut self, size: usize) -> usize {
        if self.int_upto > INT_BLOCK_SIZE - size {
            self.next_buffer();
        }
        let upto = self.int_upto;
        self.int_upto += size;
        let idx = self.int_upto - 1;
        self.current_buffer()[idx] = 1;
        upto
    }

    /// Creates a new byte slice with the given starting size and
    /// returns the slices offset in the pool.
    pub fn alloc_slice(&mut self, slice: &mut [usize], slice_offset: usize) -> usize {
        let level = slice[slice_offset];
        let new_level = NEXT_LEVEL_ARRAY[level - 1];
        let new_size = LEVEL_SIZE_ARRAY[new_level];

        // Maybe allocate another block
        if self.int_upto > INT_BLOCK_SIZE - new_size {
            self.next_buffer();
        }

        let new_upto = self.int_upto;
        let offset = (new_upto as isize + self.int_offset) as usize;
        self.int_upto += new_size;

        // Write forwarding address at end of last slice:
        slice[slice_offset] = offset;

        // Write new level
        self.buffers[self.buffer_upto as usize][self.int_upto - 1] = new_level as i32;

        new_upto
    }
}

/// Abstract class for allocating and freeing byte blocks
pub trait IntAllocator {
    fn block_size(&self) -> usize;

    fn recycle_int_blocks(&mut self, blocks: &mut [Vec<i32>], start: usize, end: usize);

    fn int_block(&mut self) -> Vec<i32> {
        vec![0i32; self.block_size()]
    }

    fn shallow_copy(&mut self) -> Box<dyn IntAllocator>;
}

pub struct DirectIntAllocator {
    block_size: usize,
}

impl DirectIntAllocator {
    pub fn new(block_size: usize) -> Self {
        DirectIntAllocator { block_size }
    }
}

impl Default for DirectIntAllocator {
    fn default() -> Self {
        Self::new(INT_BLOCK_SIZE)
    }
}

impl IntAllocator for DirectIntAllocator {
    fn block_size(&self) -> usize {
        self.block_size
    }

    fn recycle_int_blocks(&mut self, _blocks: &mut [Vec<i32>], _start: usize, _end: usize) {}

    fn shallow_copy(&mut self) -> Box<dyn IntAllocator> {
        Box::new(DirectIntAllocator {
            block_size: self.block_size,
        })
    }
}
