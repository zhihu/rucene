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

use core::store::io::DataInput;
use core::util::ByteBlockPool;

use std::io;
use std::ptr;

/// IndexInput that knows how to read the byte slices written
/// by Posting and PostingVector.  We read the bytes in
/// each slice until we hit the end of that slice at which
/// point we read the forwarding address of the next slice
/// and then jump to it.
pub struct ByteSliceReader {
    pool: *const ByteBlockPool,
    buffer_upto: usize,
    // current buffer index of pool.buffers
    upto: usize,
    limit: usize,
    level: usize,
    buffer_offset: usize,
    end_index: usize,
}

impl Default for ByteSliceReader {
    fn default() -> Self {
        ByteSliceReader {
            pool: ptr::null(),
            buffer_upto: 0,
            upto: 0,
            limit: 0,
            level: 0,
            buffer_offset: 0,
            end_index: 0,
        }
    }
}

impl Clone for ByteSliceReader {
    fn clone(&self) -> Self {
        ByteSliceReader {
            pool: self.pool,
            buffer_upto: self.buffer_upto,
            upto: self.upto,
            limit: self.limit,
            level: self.level,
            buffer_offset: self.buffer_offset,
            end_index: self.end_index,
        }
    }
}

impl ByteSliceReader {
    pub fn init(&mut self, pool: &ByteBlockPool, start_index: usize, end_index: usize) {
        debug_assert!(end_index >= start_index);

        self.pool = pool;
        self.end_index = end_index;
        self.level = 0;
        self.buffer_upto = start_index / ByteBlockPool::BYTE_BLOCK_SIZE;
        self.buffer_offset = self.buffer_upto * ByteBlockPool::BYTE_BLOCK_SIZE;
        self.upto = start_index & ByteBlockPool::BYTE_BLOCK_MASK;

        let first_size = ByteBlockPool::LEVEL_SIZE_ARRAY[0];
        self.limit = if start_index + first_size >= end_index {
            // There is noly this one slice to read
            end_index & ByteBlockPool::BYTE_BLOCK_MASK
        } else {
            self.upto + first_size - 4
        };
    }

    pub fn eof(&self) -> bool {
        debug_assert!(self.upto + self.buffer_offset <= self.end_index);
        self.upto + self.buffer_offset == self.end_index
    }

    unsafe fn next_slice(&mut self) {
        let pool = &*self.pool;
        // skip to next slice
        let next_index = {
            let buffer = &pool.buffers[self.buffer_upto];
            ((buffer[self.limit] as usize) << 24)
                + ((buffer[self.limit + 1] as usize) << 16)
                + ((buffer[self.limit + 2] as usize) << 8)
                + (buffer[self.limit + 3] as usize)
        };
        self.level = ByteBlockPool::NEXT_LEVEL_ARRAY[self.level];
        let new_size = ByteBlockPool::LEVEL_SIZE_ARRAY[self.level];

        self.buffer_upto = next_index / ByteBlockPool::BYTE_BLOCK_SIZE;
        self.buffer_offset = self.buffer_upto * ByteBlockPool::BYTE_BLOCK_SIZE;
        self.upto = next_index & ByteBlockPool::BYTE_BLOCK_MASK;
        if next_index + new_size >= self.end_index {
            // We are advancing to the final slice
            debug_assert!(self.end_index >= next_index);
            self.limit = self.end_index - self.buffer_offset;
        } else {
            // This is not the final slice (subtract 4 for the
            // forwarding address at the end of this new slice)
            self.limit = self.upto + new_size - 4;
        }
    }
}

impl io::Read for ByteSliceReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut len = buf.len();
        let mut offset = 0;
        while len > 0 {
            let num_left = self.limit - self.upto;
            unsafe {
                if num_left < len {
                    buf[offset..offset + num_left].copy_from_slice(
                        &(*self.pool).buffers[self.buffer_upto][self.upto..self.upto + num_left],
                    );
                    offset += num_left;
                    len -= num_left;
                    self.next_slice();
                } else {
                    // This slice is the last one
                    buf[offset..offset + len].copy_from_slice(
                        &(*self.pool).buffers[self.buffer_upto][self.upto..self.upto + len],
                    );
                    self.upto += len;
                    break;
                }
            }
        }
        Ok(buf.len())
    }
}

impl DataInput for ByteSliceReader {}
