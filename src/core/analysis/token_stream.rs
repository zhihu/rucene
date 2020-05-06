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

use core::analysis::{Token, TokenStream, MAX_WORD_LEN};

use core::util::BytesRef;
use error::Result;
use std::cmp::Ordering;
use std::collections::HashSet;

#[derive(Debug)]
pub struct StringTokenStream {
    token: Token,
    used: bool,
    value: String,
}

impl StringTokenStream {
    pub fn new(value: String) -> Self {
        StringTokenStream {
            token: Token::new(),
            used: true,
            value,
        }
    }
}

impl TokenStream for StringTokenStream {
    fn next_token(&mut self) -> Result<bool> {
        if self.used {
            return Ok(false);
        }

        self.clear_token();
        self.token.term = self.value.as_bytes().to_vec();
        self.token.set_offset(0, self.value.len())?;

        self.used = true;

        Ok(true)
    }

    fn end(&mut self) -> Result<()> {
        self.end_token();
        self.token.set_offset(self.value.len(), self.value.len())
    }

    fn reset(&mut self) -> Result<()> {
        self.used = false;

        Ok(())
    }

    fn token(&self) -> &Token {
        &self.token
    }

    fn token_mut(&mut self) -> &mut Token {
        &mut self.token
    }
}

#[derive(Debug)]
pub struct BinaryTokenStream {
    token: Token,
    used: bool,
    value: BytesRef,
}

impl BinaryTokenStream {
    pub fn new(value: BytesRef) -> Self {
        BinaryTokenStream {
            token: Token::new(),
            used: true,
            value,
        }
    }
}

impl TokenStream for BinaryTokenStream {
    fn next_token(&mut self) -> Result<bool> {
        if self.used {
            return Ok(false);
        }

        self.clear_token();
        self.token.term = self.value.bytes().to_vec();

        self.used = true;

        Ok(true)
    }

    fn end(&mut self) -> Result<()> {
        self.end_token();

        Ok(())
    }

    fn reset(&mut self) -> Result<()> {
        self.used = false;
        Ok(())
    }

    fn token(&self) -> &Token {
        &self.token
    }

    fn token_mut(&mut self) -> &mut Token {
        &mut self.token
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Word {
    value: String,
    begin: usize,
    length: usize,
}

impl Word {
    pub fn new(value: &str, begin: usize, length: usize) -> Word {
        let mut value = value.to_string();
        let length = if length > MAX_WORD_LEN {
            value = value.chars().take(MAX_WORD_LEN).collect();
            MAX_WORD_LEN
        } else {
            length
        };

        Word {
            value,
            begin,
            length,
        }
    }
}

#[derive(Debug)]
pub struct WordTokenStream {
    token: Token,
    values: Vec<Word>,
    current: usize,
}

impl WordTokenStream {
    pub fn new(values: Vec<Word>) -> WordTokenStream {
        let mut elements = values;
        let set: HashSet<_> = elements.drain(..).collect();
        elements.extend(set.into_iter());

        elements.sort_by(|a, b| {
            let cmp = a.begin.cmp(&b.begin);
            if cmp == Ordering::Equal {
                a.length.cmp(&b.length)
            } else {
                cmp
            }
        });

        WordTokenStream {
            token: Token::new(),
            values: elements,
            current: 0,
        }
    }
}

impl TokenStream for WordTokenStream {
    fn next_token(&mut self) -> Result<bool> {
        if self.current == self.values.len() {
            return Ok(false);
        }

        self.clear_token();

        let word: &Word = &self.values[self.current];
        self.token.term = word.value.as_bytes().to_vec();
        self.token
            .set_offset(word.begin, word.begin + word.length)?;
        self.current += 1;

        Ok(true)
    }

    fn end(&mut self) -> Result<()> {
        self.end_token();

        if let Some(word) = self.values.last() {
            let final_offset = word.begin + word.length;
            self.token.set_offset(final_offset, final_offset)
        } else {
            self.token.set_offset(0, 0)
        }
    }

    fn reset(&mut self) -> Result<()> {
        self.current = 0;
        Ok(())
    }

    fn token(&self) -> &Token {
        &self.token
    }

    fn token_mut(&mut self) -> &mut Token {
        &mut self.token
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_token_stream() {
        let value = "case".to_string();
        let mut token = StringTokenStream::new(value.clone());
        token.reset().unwrap();

        assert_eq!(token.next_token().unwrap(), true);
        assert_eq!(token.token().term.as_slice(), value.as_bytes());
    }

    #[test]
    fn test_binary_token_stream() {
        let value = "case".as_bytes().to_vec();
        let mut token = BinaryTokenStream::new(BytesRef::new(&value));
        token.reset().unwrap();

        assert_eq!(token.next_token().unwrap(), true);
        assert_eq!(token.token().term.as_slice(), value.as_slice());
    }

    #[test]
    fn test_word_token_stream() {
        let words = vec![
            Word::new("The", 0, 3),
            Word::new("quick", 4, 9),
            Word::new("brown", 10, 15),
            Word::new("fox", 16, 19),
            Word::new("jumps", 20, 25),
            Word::new("over", 26, 30),
            Word::new("the", 31, 34),
            Word::new("lazy", 35, 39),
            Word::new("dog", 40, 43),
        ];
        let mut token = WordTokenStream::new(words);
        token.reset().unwrap();

        assert_eq!(token.next_token().unwrap(), true);
        assert_eq!(token.token().term.as_slice(), "The".as_bytes());
        assert_eq!(token.next_token().unwrap(), true);
        assert_eq!(token.token().term.as_slice(), "quick".as_bytes());
        assert_eq!(token.next_token().unwrap(), true);
        assert_eq!(token.token().term.as_slice(), "brown".as_bytes());
        assert_eq!(token.next_token().unwrap(), true);
        assert_eq!(token.token().term.as_slice(), "fox".as_bytes());
        assert_eq!(token.next_token().unwrap(), true);
        assert_eq!(token.token().term.as_slice(), "jumps".as_bytes());
        assert_eq!(token.next_token().unwrap(), true);
        assert_eq!(token.token().term.as_slice(), "over".as_bytes());
        assert_eq!(token.next_token().unwrap(), true);
        assert_eq!(token.token().term.as_slice(), "the".as_bytes());
        assert_eq!(token.next_token().unwrap(), true);
        assert_eq!(token.token().term.as_slice(), "lazy".as_bytes());
        assert_eq!(token.next_token().unwrap(), true);
        assert_eq!(token.token().term.as_slice(), "dog".as_bytes());
        assert_eq!(token.next_token().unwrap(), false);
    }
}
