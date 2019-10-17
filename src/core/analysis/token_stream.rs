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

use std::fmt::Debug;

use core::analysis::{
    BytesTermAttribute, CharTermAttribute, OffsetAttribute, PayloadAttribute, PositionAttribute,
    TermToBytesRefAttribute,
};

use core::util::BytesRef;
use error::Result;
use std::cmp::Ordering;
use std::collections::HashSet;

/// A `TokenStream` enumerates the sequence of tokens, either from
/// `Field`s of a `Document` or from query text.
///
/// <b>The workflow of the `TokenStream` API is as follows:</b>
///
/// - The consumer calls {@link TokenStream#reset()}.
/// - The consumer retrieves attributes from the stream and stores local
/// references to all attributes it wants to access.
/// - The consumer calls {@link #increment_token()} until it returns false
/// consuming the attributes after each call.
/// - The consumer calls {@link #end()} so that any end-of-stream operations
/// can be performed.
/// - The consumer calls {@link #close()} to release any resource when finished
/// using the `TokenStream`.
///
/// To make sure that filters and consumers know which attributes are available,
/// the attributes must be added during instantiation. Filters and consumers are
/// not required to check for availability of attributes in
/// {@link #increment_token()}.
///
/// You can find some example code for the new API in the analysis package level
/// Javadoc.
///
/// The `TokenStream`-API in Lucene is based on the decorator pattern.
/// Therefore all non-abstract subclasses must be final or have at least a final
/// implementation of {@link #incrementToken}! This is checked when Java
/// assertions are enabled.
pub trait TokenStream: Debug {
    /// Consumers (i.e., `IndexWriter`) use this method to advance the stream to
    /// the next token. Implementing classes must implement this method and update
    /// the appropriate {@link AttributeImpl}s with the attributes of the next
    /// token.
    ///
    /// The producer must make no assumptions about the attributes after the method
    /// has been returned: the caller may arbitrarily change it. If the producer
    /// needs to preserve the state for subsequent calls, it can use
    /// {@link #captureState} to create a copy of the current attribute state.
    ///
    /// This method is called for every token of a document, so an efficient
    /// implementation is crucial for good performance. To avoid calls to
    /// {@link #addAttribute(Class)} and {@link #getAttribute(Class)},
    /// references to all {@link AttributeImpl}s that this stream uses should be
    /// retrieved during instantiation.
    ///
    /// To ensure that filters and consumers know which attributes are available,
    /// the attributes must be added during instantiation. Filters and consumers
    /// are not required to check for availability of attributes in
    /// {@link #incrementToken()}.
    ///
    /// @return false for end of stream; true otherwise
    fn increment_token(&mut self) -> Result<bool>;

    /// This method is called by the consumer after the last token has been
    /// consumed, after {@link #incrementToken()} returned `false`
    /// (using the new `TokenStream` API). Streams implementing the old API
    /// should upgrade to use this feature.
    ///
    /// This method can be used to perform any end-of-stream operations, such as
    /// setting the final offset of a stream. The final offset of a stream might
    /// differ from the offset of the last token eg in case one or more whitespaces
    /// followed after the last token, but a WhitespaceTokenizer was used.
    ///
    /// Additionally any skipped positions (such as those removed by a stopfilter)
    /// can be applied to the position increment, or any adjustment of other
    /// attributes where the end-of-stream value may be important.
    ///
    /// If you override this method, always call {@code super.end()}.
    fn end(&mut self) -> Result<()>;

    /// This method is called by a consumer before it begins consumption using
    /// {@link #incrementToken()}.
    ///
    /// Resets this stream to a clean state. Stateful implementations must implement
    /// this method so that they can be reused, just as if they had been created fresh.
    ///
    /// If you override this method, always call {@code super.reset()}, otherwise
    /// some internal state will not be correctly reset (e.g., {@link Tokenizer} will
    /// throw {@link IllegalStateException} on further usage).
    fn reset(&mut self) -> Result<()>;

    // attributes used for build invert index

    /// Resets all attributes in this `TokenStream` by calling `clear` method
    /// on each Attribute implementation.
    fn clear_attributes(&mut self) {
        self.offset_attribute_mut().clear();
        self.position_attribute_mut().clear();
        if let Some(attr) = self.payload_attribute_mut() {
            attr.clear();
        }
        self.term_bytes_attribute_mut().clear();
    }

    /// Resets all attributes in this `TokenStream` by calling `end` method
    /// on each Attribute implementation.
    fn end_attributes(&mut self) {
        self.offset_attribute_mut().end();
        self.position_attribute_mut().end();
        if let Some(attr) = self.payload_attribute_mut() {
            attr.end();
        }
        self.term_bytes_attribute_mut().end();
    }

    /// mutable access of the `OffsetAttribute`
    fn offset_attribute_mut(&mut self) -> &mut OffsetAttribute;

    /// access of the `OffsetAttribute`
    fn offset_attribute(&self) -> &OffsetAttribute;

    /// mutable access of the `PositionIncrementAttribute`
    fn position_attribute_mut(&mut self) -> &mut PositionAttribute;

    /// mutable access of the `PayloadAttribute`, wound return None if not enabled
    fn payload_attribute_mut(&mut self) -> Option<&mut PayloadAttribute> {
        None
    }

    /// access of the `PayloadAttribute`, wound return None if not enabled
    fn payload_attribute(&self) -> Option<&PayloadAttribute> {
        None
    }

    /// mutable access of the `TermToBytesRefAttribute`
    fn term_bytes_attribute_mut(&mut self) -> &mut dyn TermToBytesRefAttribute;

    /// access of the [`TermToBytesRefAttribute`
    fn term_bytes_attribute(&self) -> &dyn TermToBytesRefAttribute;
}

#[derive(Debug)]
pub struct StringTokenStream {
    term_attribute: CharTermAttribute,
    offset_attribute: OffsetAttribute,
    position_attribute: PositionAttribute,
    payload_attribute: PayloadAttribute,
    used: bool,
    value: String,
}

impl StringTokenStream {
    pub fn new(value: String) -> Self {
        StringTokenStream {
            term_attribute: CharTermAttribute::new(),
            offset_attribute: OffsetAttribute::new(),
            position_attribute: PositionAttribute::new(),
            payload_attribute: PayloadAttribute::new(Vec::with_capacity(0)),
            used: true,
            value,
        }
    }
}

impl TokenStream for StringTokenStream {
    fn increment_token(&mut self) -> Result<bool> {
        if self.used {
            return Ok(false);
        }
        self.clear_attributes();

        self.term_attribute.append(&self.value);
        self.offset_attribute.set_offset(0, self.value.len())?;
        self.used = true;
        Ok(true)
    }

    fn end(&mut self) -> Result<()> {
        self.end_attributes();
        let final_offset = self.value.len();
        self.offset_attribute.set_offset(final_offset, final_offset)
    }

    fn reset(&mut self) -> Result<()> {
        self.used = false;
        Ok(())
    }

    fn offset_attribute_mut(&mut self) -> &mut OffsetAttribute {
        &mut self.offset_attribute
    }

    fn offset_attribute(&self) -> &OffsetAttribute {
        &self.offset_attribute
    }

    fn position_attribute_mut(&mut self) -> &mut PositionAttribute {
        &mut self.position_attribute
    }

    fn term_bytes_attribute_mut(&mut self) -> &mut dyn TermToBytesRefAttribute {
        &mut self.term_attribute
    }

    fn term_bytes_attribute(&self) -> &dyn TermToBytesRefAttribute {
        &self.term_attribute
    }
}

#[derive(Debug)]
pub struct BinaryTokenStream {
    term_attribute: BytesTermAttribute,
    offset_attribute: OffsetAttribute,
    position_attribute: PositionAttribute,
    payload_attribute: PayloadAttribute,
    used: bool,
    value: BytesRef,
}

impl BinaryTokenStream {
    pub fn new(value: BytesRef) -> Self {
        BinaryTokenStream {
            term_attribute: BytesTermAttribute::new(),
            offset_attribute: OffsetAttribute::new(),
            position_attribute: PositionAttribute::new(),
            payload_attribute: PayloadAttribute::new(Vec::with_capacity(0)),
            used: true,
            value,
        }
    }
}

impl TokenStream for BinaryTokenStream {
    fn increment_token(&mut self) -> Result<bool> {
        if self.used {
            return Ok(false);
        }
        self.clear_attributes();

        self.term_attribute.set_bytes(self.value.bytes());
        self.used = true;
        Ok(true)
    }

    fn end(&mut self) -> Result<()> {
        self.end_attributes();
        Ok(())
    }

    fn reset(&mut self) -> Result<()> {
        self.used = false;
        Ok(())
    }

    fn offset_attribute_mut(&mut self) -> &mut OffsetAttribute {
        &mut self.offset_attribute
    }

    fn offset_attribute(&self) -> &OffsetAttribute {
        &self.offset_attribute
    }

    fn position_attribute_mut(&mut self) -> &mut PositionAttribute {
        &mut self.position_attribute
    }

    fn term_bytes_attribute_mut(&mut self) -> &mut dyn TermToBytesRefAttribute {
        &mut self.term_attribute
    }

    fn term_bytes_attribute(&self) -> &dyn TermToBytesRefAttribute {
        &self.term_attribute
    }
}

pub const MAX_WORD_LEN: usize = 128;

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
    term_attribute: CharTermAttribute,
    offset_attribute: OffsetAttribute,
    position_attribute: PositionAttribute,
    payload_attribute: PayloadAttribute,
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
            term_attribute: CharTermAttribute::new(),
            offset_attribute: OffsetAttribute::new(),
            position_attribute: PositionAttribute::new(),
            payload_attribute: PayloadAttribute::new(Vec::with_capacity(0)),
            values: elements,
            current: 0,
        }
    }
}

impl TokenStream for WordTokenStream {
    fn increment_token(&mut self) -> Result<bool> {
        if self.current == self.values.len() {
            return Ok(false);
        }

        self.clear_attributes();

        let word: &Word = &self.values[self.current];
        self.term_attribute.append(&word.value);
        self.offset_attribute
            .set_offset(word.begin, word.begin + word.length)?;
        self.current += 1;
        Ok(true)
    }

    fn end(&mut self) -> Result<()> {
        self.end_attributes();
        if let Some(word) = self.values.last() {
            let final_offset = word.begin + word.length;
            self.offset_attribute.set_offset(final_offset, final_offset)
        } else {
            self.offset_attribute.set_offset(0, 0)
        }
    }

    fn reset(&mut self) -> Result<()> {
        self.current = 0;
        Ok(())
    }

    fn offset_attribute_mut(&mut self) -> &mut OffsetAttribute {
        &mut self.offset_attribute
    }

    fn offset_attribute(&self) -> &OffsetAttribute {
        &self.offset_attribute
    }

    fn position_attribute_mut(&mut self) -> &mut PositionAttribute {
        &mut self.position_attribute
    }

    fn term_bytes_attribute_mut(&mut self) -> &mut dyn TermToBytesRefAttribute {
        &mut self.term_attribute
    }

    fn term_bytes_attribute(&self) -> &dyn TermToBytesRefAttribute {
        &self.term_attribute
    }
}
