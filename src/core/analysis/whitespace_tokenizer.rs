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

use core::analysis::{Token, TokenStream};

use error::Result;

use std::fmt;
use std::io::Read;

use unicode_reader::CodePoints;

// NOTE: this length is length by byte, so it's different from Lucene's word length
const MAX_BYTES_LEN: usize = 511;
const IO_BUFFER_SIZE: usize = 4096;

/// A tokenizer that divides text at whitespace characters.
///
/// Note: That definition explicitly excludes the non-breaking space.
/// Adjacent sequences of non-Whitespace characters form tokens.
pub struct WhitespaceTokenizer {
    offset: usize,
    buffer_index: usize,
    data_len: usize,
    final_offset: usize,

    token: Token,
    io_buffer: CharacterBuffer,
    reader: Box<dyn Read>,
}

impl fmt::Debug for WhitespaceTokenizer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("WhitespaceTokenizer")
            .field("offset", &self.offset)
            .field("buffer_index", &self.buffer_index)
            .field("data_len", &self.data_len)
            .field("final_offset", &self.final_offset)
            .field(
                "term",
                &String::from_utf8(self.token.term.clone()).unwrap_or("".to_string()),
            )
            .field("position", &self.token.position)
            .field("start_offset", &self.token.start_offset)
            .field("end_offset", &self.token.end_offset)
            .field("io_buffer", &self.io_buffer)
            .finish()
    }
}

impl WhitespaceTokenizer {
    pub fn new(reader: Box<dyn Read>) -> Self {
        WhitespaceTokenizer {
            offset: 0,
            buffer_index: 0,
            data_len: 0,
            final_offset: 0,
            token: Token::new(),
            io_buffer: CharacterBuffer::new(IO_BUFFER_SIZE),
            reader,
        }
    }

    pub fn push_char(&mut self, c: char) {
        let char_len = c.len_utf8();
        let term_len = self.token.term.len();
        self.token.term.resize(term_len + char_len, 0u8);
        c.encode_utf8(&mut self.token.term[term_len..]);
    }
}

impl TokenStream for WhitespaceTokenizer {
    fn next_token(&mut self) -> Result<bool> {
        self.clear_token();

        let mut length = 0;
        let mut start = -1; // this variable is always initialized
        let mut end = -1;
        loop {
            if self.buffer_index >= self.data_len {
                self.offset += self.data_len;
                self.io_buffer.fill(&mut self.reader)?;
                if self.io_buffer.is_empty() {
                    self.data_len = 0; // so next offset += dataLen won't decrement offset
                    if length > 0 {
                        break;
                    } else {
                        self.final_offset = self.offset;
                        return Ok(false);
                    }
                }
                self.data_len = self.io_buffer.length;
                self.buffer_index = 0;
            }

            let cur_char = self.io_buffer.char_at(self.buffer_index);
            self.buffer_index += 1;
            if !cur_char.is_whitespace() {
                if length == 0 {
                    debug_assert_eq!(start, -1);
                    start = (self.offset + self.buffer_index - 1) as isize;
                    end = start;
                }
                end += 1;
                length += cur_char.len_utf8();
                self.push_char(cur_char);
                if self.token.term.len() >= MAX_BYTES_LEN {
                    break;
                }
            } else if length > 0 {
                break;
            }
        }

        assert_ne!(start, -1);

        self.final_offset = end as usize;
        self.token.set_offset(start as usize, end as usize)?;

        Ok(true)
    }

    fn end(&mut self) -> Result<()> {
        self.end_token();

        Ok(())
    }

    fn reset(&mut self) -> Result<()> {
        self.buffer_index = 0;
        self.offset = 0;
        self.data_len = 0;
        self.final_offset = 0;
        self.io_buffer.reset();
        Ok(())
    }

    fn token(&self) -> &Token {
        &self.token
    }

    fn token_mut(&mut self) -> &mut Token {
        &mut self.token
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::BufReader;

    #[test]
    fn test_whitespace_tokenizer() {
        let source = "The quick brown fox jumps over a lazy dog";
        let offsets = [
            (0usize, 3usize),
            (4, 9),
            (10, 15),
            (16, 19),
            (20, 25),
            (26, 30),
            (31, 32),
            (33, 37),
            (38, 41),
        ];
        let words: Vec<&str> = source.split(" ").collect();
        let reader = Box::new(BufReader::new(source.as_bytes()));

        let mut tokenizer = WhitespaceTokenizer::new(reader);

        for i in 0..9 {
            let res = tokenizer.next_token(); // Ok(true)
            assert!(res.is_ok());
            assert!(res.unwrap());
            assert_eq!(tokenizer.token().start_offset, offsets[i].0);
            assert_eq!(tokenizer.token().end_offset, offsets[i].1);
            assert_eq!(tokenizer.token().term.as_slice(), words[i].as_bytes());
        }
    }
}
