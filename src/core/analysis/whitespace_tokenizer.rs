use core::analysis::char_buffer::CharacterBuffer;
use core::analysis::TokenStream;
use core::attribute::PositionIncrementAttribute;
use core::attribute::TermToBytesRefAttribute;
use core::attribute::{CharTermAttribute, OffsetAttribute};

use error::Result;

use std::fmt;
use std::io::Read;

const MAX_WORD_LEN: usize = 255;
#[allow(dead_code)]
const IO_BUFFER_SIZE: usize = 4096;

/// A tokenizer that divides text at whitespace characters as defined by
/// {@link Character#isWhitespace(int)}.  Note: That definition explicitly excludes the
/// non-breaking space. Adjacent sequences of non-Whitespace characters form tokens.
///
/// @see UnicodeWhitespaceTokenizer
pub struct WhitespaceTokenizer {
    offset: usize,
    buffer_index: usize,
    data_len: usize,
    final_offset: usize,
    term_attr: CharTermAttribute,
    offset_attr: OffsetAttribute,
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
            .field("term_attr", &self.term_attr)
            .field("offset_attr", &self.offset_attr)
            .field("io_buffer", &self.io_buffer)
            .finish()
    }
}

impl WhitespaceTokenizer {
    pub fn new(reader: Box<Read>) -> Self {
        WhitespaceTokenizer {
            offset: 0,
            buffer_index: 0,
            data_len: 0,
            final_offset: 0,
            term_attr: CharTermAttribute::new(),
            offset_attr: OffsetAttribute::new(),
            io_buffer: CharacterBuffer::new(vec![], 0, 0),
            reader,
        }
    }

    pub fn is_token_char(&self, c: char) -> bool {
        !c.is_whitespace()
    }

    /// Called on each token character to normalize it before it is added to the
    /// token. The default implementation does nothing. Subclasses may use this to,
    /// e.g., lowercase tokens.
    #[allow(dead_code)]
    fn normalize(&self, c: i32) -> i32 {
        c
    }

    fn clear_attributes(&mut self) {
        self.term_attr.clear();
        self.offset_attr.clear();
    }

    fn correct_offset(&self, offset: usize) -> usize {
        offset
    }
}

impl TokenStream for WhitespaceTokenizer {
    fn increment_token(&mut self) -> Result<bool> {
        self.clear_attributes();
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
                        self.final_offset = self.correct_offset(self.offset);
                        return Ok(false);
                    }
                }
                self.data_len = self.io_buffer.length;
                self.buffer_index = 0;
            }

            let cur_char = self.io_buffer.char_at(self.buffer_index);
            if self.is_token_char(cur_char) {
                if length == 0 {
                    debug_assert_eq!(start, -1);
                    start = (self.offset + self.buffer_index) as isize;
                    end = start;
                }
                end += 1;
                length += cur_char.len_utf8();
                self.term_attr.push_char(cur_char);
                if self.term_attr.char_cnt >= MAX_WORD_LEN {
                    break;
                }
            } else if length > 0 {
                break;
            }
        }

        assert_ne!(start, -1);
        let final_start = self.correct_offset(start as usize);
        let final_end = self.correct_offset(end as usize);
        self.final_offset = final_end;
        self.offset_attr.set_offset(final_start, final_end)?;
        Ok(true)
    }

    fn end(&mut self) -> Result<()> {
        self.offset_attr.end();
        self.term_attr.end();
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

    fn offset_attribute_mut(&mut self) -> &mut OffsetAttribute {
        &mut self.offset_attr
    }

    fn offset_attribute(&self) -> &OffsetAttribute {
        &self.offset_attr
    }

    fn position_attribute_mut(&mut self) -> &mut PositionIncrementAttribute {
        unimplemented!()
    }

    fn term_bytes_attribute_mut(&mut self) -> &mut TermToBytesRefAttribute {
        &mut self.term_attr
    }

    fn term_bytes_attribute(&self) -> &TermToBytesRefAttribute {
        &self.term_attr
    }
}
