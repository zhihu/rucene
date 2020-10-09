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

mod token_stream;

pub use self::token_stream::*;

mod whitespace_tokenizer;

pub use self::whitespace_tokenizer::*;

use error::Result;

use std::fmt::Debug;

pub const MIN_BUFFER_SIZE: usize = 10;
pub const MAX_WORD_LEN: usize = 128;

#[derive(Debug, Clone)]
pub struct Token {
    pub term: Vec<u8>,
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
    pub position: usize,
    /// The start and end character offset of a Token.
    pub start_offset: usize,
    pub end_offset: usize,
    /// The payload of a Token.
    ///
    /// The payload is stored in the index at each position, and can
    /// be used to influence scoring when using Payload-based queries.
    ///
    /// NOTE: because the payload will be stored at each position, it's usually
    /// best to use the minimum number of bytes necessary. Some codec implementations
    /// may optimize payload storage when all payloads have the same length.
    pub payload: Vec<u8>,
}

impl Token {
    pub fn new() -> Token {
        Token {
            term: Vec::with_capacity(MIN_BUFFER_SIZE),
            position: 1,
            start_offset: 0,
            end_offset: 0,
            payload: Vec::with_capacity(0),
        }
    }

    pub fn clear(&mut self) {
        self.position = 1;
        self.start_offset = 0;
        self.end_offset = 0;
        self.payload.clear();
        self.term.clear();
    }

    pub fn end(&mut self) {
        self.clear();
        self.position = 0;
    }

    /// Set the starting and ending offset.
    pub fn set_offset(&mut self, start_offset: usize, end_offset: usize) -> Result<()> {
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
}

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
    fn next_token(&mut self) -> Result<bool>;

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

    /// mutable access of the `OffsetAttribute`
    fn token(&self) -> &Token;

    /// mutable access of the `OffsetAttribute`
    fn token_mut(&mut self) -> &mut Token;

    /// Resets all attributes in this `TokenStream` by calling `clear` method
    /// on each Attribute implementation.
    fn clear_token(&mut self) {
        self.token_mut().clear();
    }

    /// Resets all attributes in this `TokenStream` by calling `end` method
    /// on each Attribute implementation.
    fn end_token(&mut self) {
        self.token_mut().end();
    }
}
