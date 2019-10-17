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

/// Sets the custom term frequency of a term within one document.
///
/// If this attribute is present in your analysis chain for a given field,
/// that field must be indexed with IndexOptions#DocsAndFreqs
pub struct TermFreqAttribute {
    term_freq: u32,
}

impl Default for TermFreqAttribute {
    fn default() -> Self {
        TermFreqAttribute { term_freq: 1 }
    }
}

impl TermFreqAttribute {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn set_term_freq(&mut self, freq: u32) {
        if freq < 1 {
            panic!("Term frequency must be 1 or greater");
        }
        self.term_freq = freq;
    }

    pub fn term_freq(&self) -> u32 {
        self.term_freq
    }

    pub fn clear(&mut self) {
        self.term_freq = 1;
    }

    pub fn end(&mut self) {
        self.term_freq = 1;
    }
}

/// The start and end character offset of a Token.
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

    /// Returns this Token's starting offset, the position of the first character
    /// corresponding to this token in the source text.
    ///
    /// Note that the difference between `#end_offset()` and `#start_offset()` may
    /// not be equal to term.len(), as the term text may have been altered by a
    /// stemmer or some other filter.
    pub fn start_offset(&self) -> usize {
        self.start_offset
    }

    /// Returns this Token's ending offset, one greater than the position of the
    /// last character corresponding to this token in the source text. The length
    /// of the token in the source text is (end_offset() - start_offset()).
    pub fn end_offset(&self) -> usize {
        self.end_offset
    }

    /// Set the starting and ending offset.
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

    pub fn clear(&mut self) {
        // TODO: we could use -1 as default here?  Then we can
        // assert in setOffset...
        self.start_offset = 0;
        self.end_offset = 0;
    }

    /// end this attribute
    pub fn end(&mut self) {
        self.clear();
    }
}

/// Determines the position of this token relative to the previous Token in a
/// TokenStream, used in phrase searching.
///
/// The default value is one.
///
/// Some common uses for this are:
///
/// * Set it to zero to put multiple terms in the same position.  This is
/// useful if, e.g., a word has multiple stems.  Searches for phrases
/// including either stem will match.  In this case, all but the first stem's
/// increment should be set to zero: the increment of the first instance
/// should be one.  Repeating a token with an increment of zero can also be
/// used to boost the scores of matches on that token.
///
/// * Set it to values greater than one to inhibit exact phrase matches.
/// If, for example, one does not want phrases to match across removed stop
/// words, then one could build a stop word filter that removes stop words and
/// also sets the increment to the number of stop words removed before each
/// non-stop word.  Then exact phrase queries will only match when the terms
/// occur with no intervening stop words.
#[derive(Debug, Clone, Copy)]
pub struct PositionAttribute {
    position: u32,
}

impl Default for PositionAttribute {
    fn default() -> Self {
        PositionAttribute::new()
    }
}

impl PositionAttribute {
    #[inline]
    pub fn new() -> PositionAttribute {
        PositionAttribute { position: 1 }
    }

    pub fn set_position(&mut self, position: u32) {
        self.position = position;
    }

    pub fn get_position(self) -> u32 {
        self.position
    }

    pub fn clear(&mut self) {
        self.position = 1
    }

    pub fn end(&mut self) {
        self.position = 0
    }
}

/// The payload of a Token.
///
/// The payload is stored in the index at each position, and can
/// be used to influence scoring when using Payload-based queries.
///
/// NOTE: because the payload will be stored at each position, it's usually
/// best to use the minimum number of bytes necessary. Some codec implementations
/// may optimize payload storage when all payloads have the same length.
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

/// This attribute is requested by TermsHashPerField to index the contents.
/// This attribute can be used to customize the final bytes encoding of terms.
///
/// Consumers of this attribute call `get_bytes_ref` for each term.
pub trait TermToBytesRefAttribute {
    fn get_bytes_ref(&self) -> BytesRef;

    fn clear(&mut self);

    fn end(&mut self);
}

const MIN_BUFFER_SIZE: usize = 10;

/// The term text of a Token
#[derive(Debug)]
pub struct CharTermAttribute {
    pub term_buffer: Vec<u8>,
}

impl CharTermAttribute {
    pub fn new() -> Self {
        CharTermAttribute {
            term_buffer: Vec::with_capacity(MIN_BUFFER_SIZE),
        }
    }

    /// Returns the length of this character sequence. the length is calculated
    /// by byte instead by char
    pub fn len(&self) -> usize {
        self.term_buffer.len()
    }

    pub fn push_char(&mut self, c: char) {
        let char_len = c.len_utf8();
        let term_len = self.term_buffer.len();
        self.term_buffer.resize(term_len + char_len, 0u8);
        c.encode_utf8(&mut self.term_buffer[term_len..]);
    }

    /// Copies the contents of buffer, starting at offset for length chars, into the term buffer vec
    pub fn copy_buffer(&mut self, buffer: &[u8]) {
        self.term_buffer.resize(buffer.len(), 0u8);
        self.term_buffer[0..buffer.len()].copy_from_slice(buffer);
    }

    /// Appends the specified string to this char sequence.
    pub fn append(&mut self, s: &str) {
        self.term_buffer.extend(s.bytes())
    }

    /// Set number of valid characters (length of the term) in  the term buffer vector.
    /// Use this to truncate the term buffer or to synchronize with external manipulation
    /// of the termBuffer.
    /// Note: to grow the size of the array, use {@link #resizeBuffer(int)} first.
    pub fn set_length(&mut self, length: usize) -> Result<()> {
        if length > self.term_buffer.len() {
            bail!(ErrorKind::IllegalArgument(format!(
                "length {} exceeds the size of the term_buffer",
                length
            )));
        }
        self.term_buffer.truncate(length);
        Ok(())
    }

    /// Sets the length of the termBuffer to zero. Use this method before appending contents
    pub fn set_empty(&mut self) {
        self.term_buffer.clear();
    }
}

impl TermToBytesRefAttribute for CharTermAttribute {
    fn get_bytes_ref(&self) -> BytesRef {
        BytesRef::new(&self.term_buffer)
    }

    fn clear(&mut self) {
        self.term_buffer.clear();
    }

    fn end(&mut self) {
        self.clear();
    }
}

/// This attribute can be used if you have the raw term bytes to be indexed.
///
/// It can be used as replacement for `CharTermAttribute`, if binary terms should be indexed.
#[derive(Default)]
pub struct BytesTermAttribute {
    bytes: BytesRef,
}

impl BytesTermAttribute {
    pub fn new() -> Self {
        Default::default()
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
