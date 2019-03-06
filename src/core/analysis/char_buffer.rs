use error::Result;
use std::io::Read;

/// a simple IO buffer to use
#[derive(Debug)]
pub struct CharacterBuffer {
    pub buffer: Vec<char>,
    pub offset: usize,
    pub length: usize,
}

impl CharacterBuffer {
    pub fn new(buffer: Vec<char>, offset: usize, length: usize) -> Self {
        CharacterBuffer {
            buffer,
            offset,
            length,
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
        let mut chars = reader.chars();
        self.offset = 0;
        let mut offset = 0;

        loop {
            if offset >= self.buffer.len() {
                break;
            }
            if let Some(res) = chars.next() {
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
