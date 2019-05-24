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

use core::util::BytesRef;
use error::{ErrorKind, Result};

use std::fmt;

pub struct TermFrequencyAttribute {
    term_freq: u32,
}

impl TermFrequencyAttribute {
    pub fn new() -> Self {
        TermFrequencyAttribute { term_freq: 1 }
    }

    pub fn set_term_frequency(&mut self, freq: u32) {
        if freq < 1 {
            panic!("Term frequency must be 1 or greater");
        }
        self.term_freq = freq;
    }

    pub fn term_frequency(&self) -> u32 {
        self.term_freq
    }

    pub fn clear(&mut self) {
        self.term_freq = 1;
    }

    pub fn end(&mut self) {
        self.term_freq = 1;
    }
}

#[derive(Debug, Default)]
pub struct OffsetAttribute {
    start_offset: usize,
    end_offset: usize,
}

impl OffsetAttribute {
    pub fn new() -> OffsetAttribute {
        OffsetAttribute {
            start_offset: 0,
            end_offset: 0,
        }
    }

    pub fn clear(&mut self) {
        // TODO: we could use -1 as default here?  Then we can
        // assert in setOffset...
        self.start_offset = 0;
        self.end_offset = 0;
    }

    pub fn start_offset(&self) -> usize {
        self.start_offset
    }

    pub fn set_offset(&mut self, start_offset: usize, end_offset: usize) -> Result<()> {
        // TODO: we could assert that this is set-once, ie,
        // current values are -1?  Very few token filters should
        // change offsets once set by the tokenizer... and
        // tokenizer should call clearAtts before re-using
        // OffsetAtt

        if end_offset < start_offset {
            bail!(
                "endOffset must be >= startOffset; got startOffset={}, endOffset={}",
                start_offset,
                end_offset
            )
        }

        self.start_offset = start_offset;
        self.end_offset = end_offset;
        Ok(())
    }

    pub fn end_offset(&self) -> usize {
        self.end_offset
    }

    pub fn end(&mut self) {
        self.clear();
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PositionIncrementAttribute {
    position_increment: u32,
}

impl Default for PositionIncrementAttribute {
    fn default() -> Self {
        PositionIncrementAttribute::new()
    }
}

impl PositionIncrementAttribute {
    #[inline]
    pub fn new() -> PositionIncrementAttribute {
        PositionIncrementAttribute {
            position_increment: 1,
        }
    }

    pub fn set_position_increment(&mut self, position_increment: u32) {
        self.position_increment = position_increment;
    }

    pub fn get_position_increment(&self) -> u32 {
        self.position_increment
    }

    pub fn clear(&mut self) {
        self.position_increment = 1
    }

    pub fn end(&mut self) {
        self.position_increment = 0
    }
}

#[derive(Debug)]
pub struct PayloadAttribute {
    payload: Vec<u8>,
}

impl PayloadAttribute {
    pub fn new(payload: Vec<u8>) -> PayloadAttribute {
        PayloadAttribute { payload }
    }

    pub fn from(payload: Vec<u8>) -> PayloadAttribute {
        PayloadAttribute { payload }
    }

    pub fn get_payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn set_payload(&mut self, payload: Vec<u8>) {
        self.payload = payload
    }

    pub fn clear(&mut self) {
        self.payload.clear()
    }

    pub fn end(&mut self) {
        self.clear();
    }
}

pub trait TermToBytesRefAttribute {
    fn get_bytes_ref(&self) -> BytesRef;

    fn clear(&mut self);

    fn end(&mut self);
}

///// The term text of a Token
// pub trait CharTermAttribute {
//    /// Copies the contents of buffer, starting at offset for
//    /// length characters, into the term_buffer array
//    fn copy_buffer(&mut self, buffer: &[u8]) -> Result<()>;
//
//    fn buffer(&self) -> &[u8];
//
//    fn resize_buffer(&mut self, new_size: usize);
//
//    /// Sets the length of the termBuffer to zero.
//    /// Use this method before appending contents
//    /// use the `appendable` method
//    fn set_empty(&mut self);
//
//    fn append(&mut self, csq: &[u8]);
//}

const MIN_BUFFER_SIZE: usize = 10;

/// The term text of a Token
#[derive(Debug)]
pub struct CharTermAttribute {
    pub term_buffer: Vec<u8>,
    pub term_length: usize, // count by byte
    pub char_cnt: usize,    /* count by chars
                             * builder: BytesRefBuilder, */
}

impl CharTermAttribute {
    pub fn new() -> Self {
        CharTermAttribute {
            term_buffer: Vec::with_capacity(MIN_BUFFER_SIZE),
            term_length: 0,
            char_cnt: 0,
        }
    }

    pub fn push_char(&mut self, c: char) {
        let char_len = c.len_utf8();
        if self.term_buffer.len() < self.term_length + char_len {
            self.term_buffer.resize(self.term_length + char_len, 0u8);
        }
        c.encode_utf8(&mut self.term_buffer[self.term_length..]);
        self.term_length += char_len;
        self.char_cnt += 1;
    }

    pub fn copy_buffer(&mut self, buffer: &[u8]) {
        self.term_buffer.resize(buffer.len(), 0u8);
        self.term_buffer[0..buffer.len()].copy_from_slice(buffer);
        self.term_length = buffer.len();
    }

    pub fn append(&mut self, s: &str) {
        for c in s.chars() {
            self.push_char(c);
        }
    }

    pub fn resize_buffer(&mut self, new_size: usize) {
        self.term_buffer.resize(new_size, 0u8);
    }

    pub fn set_length(&mut self, length: usize) -> Result<()> {
        if length > self.term_buffer.len() {
            bail!(ErrorKind::IllegalArgument(format!(
                "length {} exceeds the size of the term_buffer",
                length
            )));
        }
        self.term_length = length;
        Ok(())
    }

    pub fn set_empty(&mut self) {
        self.term_length = 0;
    }

    pub fn end(&mut self) {
        self.clear();
    }
}

impl TermToBytesRefAttribute for CharTermAttribute {
    fn get_bytes_ref(&self) -> BytesRef {
        BytesRef::new(&self.term_buffer[0..self.term_length])
    }

    fn clear(&mut self) {
        self.term_length = 0;
    }

    fn end(&mut self) {
        self.clear();
    }
}

pub struct BytesTermAttribute {
    bytes: BytesRef,
}

impl BytesTermAttribute {
    pub fn new() -> Self {
        BytesTermAttribute {
            bytes: BytesRef::default(),
        }
    }

    pub fn set_bytes(&mut self, bytes_ref: &[u8]) {
        self.bytes = BytesRef::new(bytes_ref);
    }
}

impl fmt::Debug for BytesTermAttribute {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("BytesTermAttribute")
            .field("bytes", &self.bytes.bytes())
            .finish()
    }
}

impl TermToBytesRefAttribute for BytesTermAttribute {
    fn get_bytes_ref(&self) -> BytesRef {
        BytesRef::new(self.bytes.bytes())
    }

    fn clear(&mut self) {
        self.bytes = BytesRef::default()
    }

    fn end(&mut self) {
        self.clear();
    }
}
