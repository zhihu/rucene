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

use std::cmp::Ordering;
use std::fmt;

#[derive(Copy, Clone)]
pub struct BytesRef {
    slice: *const [u8],
}

const DUMMY_BYTE: [u8; 0] = [];

// return a dummy `BytesPtr` for some place need dummy init
// in order to avoid `Option`
impl Default for BytesRef {
    fn default() -> Self {
        BytesRef::new(&DUMMY_BYTE)
    }
}

impl BytesRef {
    pub fn new(bytes: &[u8]) -> BytesRef {
        BytesRef {
            slice: bytes as *const [u8],
        }
    }

    pub fn bytes(&self) -> &[u8] {
        unsafe { &*self.slice }
    }

    pub fn set_bytes(&mut self, bytes: &[u8]) {
        self.slice = bytes as *const [u8];
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        unsafe { (&*self.slice).len() }
    }

    pub fn byte_at(&self, idx: usize) -> u8 {
        unsafe { (&*self.slice)[idx] }
    }
}

impl AsRef<[u8]> for BytesRef {
    fn as_ref(&self) -> &[u8] {
        self.bytes()
    }
}

impl fmt::Debug for BytesRef {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("BytesPtr")
            .field("bytes", &self.bytes())
            .finish()
    }
}

impl Eq for BytesRef {}

impl PartialEq for BytesRef {
    fn eq(&self, other: &Self) -> bool {
        self.bytes().eq(other.bytes())
    }
}

impl Ord for BytesRef {
    fn cmp(&self, other: &Self) -> Ordering {
        self.bytes().cmp(other.bytes())
    }
}

impl PartialOrd for BytesRef {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// A builder for `BytesRef` instances
#[derive(Default)]
pub struct BytesRefBuilder {
    pub buffer: Vec<u8>,
    pub offset: usize,
    pub length: usize,
}

impl BytesRefBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn bytes_mut(&mut self) -> &mut [u8] {
        &mut self.buffer
    }

    pub fn grow(&mut self, size: usize) {
        self.buffer.resize(size, 0u8);
    }

    pub fn append(&mut self, b: u8) {
        let pos = self.offset + self.length;
        if pos >= self.buffer.len() {
            self.buffer.resize(pos + 1, 0u8);
        }
        self.buffer[pos] = b;
        self.length += 1;
    }

    pub fn appends(&mut self, bytes: &[u8]) {
        let start = self.offset + self.length;
        let end = start + bytes.len();
        if end >= self.buffer.len() {
            self.buffer.resize(end, 0u8);
        }
        self.buffer[start..end].copy_from_slice(bytes);
        self.length += bytes.len();
    }

    pub fn get(&self) -> BytesRef {
        BytesRef::new(&self.buffer[self.offset..self.length])
    }

    pub fn copy_from(&mut self, bytes: &[u8]) {
        if self.buffer.len() < bytes.len() {
            self.buffer.resize(bytes.len(), 0u8);
        }
        self.buffer[0..bytes.len()].copy_from_slice(bytes);
        self.offset = 0;
        self.length = bytes.len();
    }
}
