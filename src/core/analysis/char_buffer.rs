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

use error::Result;
use std::io::Read;

use unicode_reader::CodePoints;

/// a simple IO buffer to use
#[derive(Debug)]
pub struct CharacterBuffer {
    pub buffer: Vec<char>,
    pub offset: usize,
    pub length: usize,
}

impl CharacterBuffer {
    pub fn new(buffer_size: usize) -> Self {
        if buffer_size < 2 {
            panic!("buffer size must be >= 2");
        }
        CharacterBuffer {
            buffer: vec!['\0'; buffer_size],
            offset: 0,
            length: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    pub fn char_at(&self, index: usize) -> char {
        debug_assert!(index < self.buffer.len());
        self.buffer[index]
    }

    pub fn reset(&mut self) {
        self.offset = 0;
        self.length = 0;
    }

    pub fn fill<T: Read + ?Sized>(&mut self, reader: &mut T) -> Result<bool> {
        let mut unicode_reader = CodePoints::from(reader);
        self.offset = 0;
        let mut offset = 0;

        loop {
            if offset >= self.buffer.len() {
                break;
            }
            if let Some(res) = unicode_reader.next() {
                let cur_char = res?;
                self.buffer[offset] = cur_char;
                offset += 1;
            } else {
                break;
            }
        }
        self.length = offset;
        Ok(self.buffer.len() == self.length)
    }
}
