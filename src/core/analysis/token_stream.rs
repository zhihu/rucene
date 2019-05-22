use std::fmt::Debug;

use core::attribute::TermToBytesRefAttribute;
use core::attribute::{OffsetAttribute, PayloadAttribute, PositionIncrementAttribute};

use error::Result;

/// A <code>TokenStream</code> enumerates the sequence of tokens, either from
/// {@link Field}s of a {@link Document} or from query text.
///
/// This is an abstract class; concrete subclasses are:
///
/// - {@link Tokenizer}, a <code>TokenStream</code> whose input is a Reader; and
/// - {@link TokenFilter}, a <code>TokenStream</code> whose input is another
/// <code>TokenStream</code>.
///
/// A new <code>TokenStream</code> API has been introduced with Lucene 2.9. This API
/// has moved from being {@link Token}-based to {@link Attribute}-based. While
/// {@link Token} still exists in 2.9 as a convenience class, the preferred way
/// to store the information of a {@link Token} is to use {@link AttributeImpl}s.
///
/// <code>TokenStream</code> now extends {@link AttributeSource}, which provides
/// access to all of the token {@link Attribute}s for the <code>TokenStream</code>.
/// Note that only one instance per {@link AttributeImpl} is created and reused
/// for every token. This approach reduces object creation and allows local
/// caching of references to the {@link AttributeImpl}s. See
/// {@link #incrementToken()} for further details.
///
/// <b>The workflow of the new <code>TokenStream</code> API is as follows:</b>
/// <ol>
/// - Instantiation of <code>TokenStream</code>/{@link TokenFilter}s which add/get
/// attributes to/from the {@link AttributeSource}.
/// - The consumer calls {@link TokenStream#reset()}.
/// - The consumer retrieves attributes from the stream and stores local
/// references to all attributes it wants to access.
/// - The consumer calls {@link #incrementToken()} until it returns false
/// consuming the attributes after each call.
/// - The consumer calls {@link #end()} so that any end-of-stream operations
/// can be performed.
/// - The consumer calls {@link #close()} to release any resource when finished
/// using the <code>TokenStream</code>.
/// </ol>
/// To make sure that filters and consumers know which attributes are available,
/// the attributes must be added during instantiation. Filters and consumers are
/// not required to check for availability of attributes in
/// {@link #incrementToken()}.
///
/// You can find some example code for the new API in the analysis package level
/// Javadoc.
///
/// Sometimes it is desirable to capture a current state of a <code>TokenStream</code>,
/// e.g., for buffering purposes (see {@link CachingTokenFilter},
/// TeeSinkTokenFilter). For this usecase
/// {@link AttributeSource#captureState} and {@link AttributeSource#restoreState}
/// can be used.
/// The {@code TokenStream}-API in Lucene is based on the decorator pattern.
/// Therefore all non-abstract subclasses must be final or have at least a final
/// implementation of {@link #incrementToken}! This is checked when Java
/// assertions are enabled.

pub trait TokenStream: Debug {
    /// Consumers (i.e., {@link IndexWriter}) use this method to advance the stream to
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
    /// consumed, after {@link #incrementToken()} returned <code>false</code>
    /// (using the new <code>TokenStream</code> API). Streams implementing the old API
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
    /// <p>
    /// Resets this stream to a clean state. Stateful implementations must implement
    /// this method so that they can be reused, just as if they had been created fresh.
    /// <p>
    /// If you override this method, always call {@code super.reset()}, otherwise
    /// some internal state will not be correctly reset (e.g., {@link Tokenizer} will
    /// throw {@link IllegalStateException} on further usage).
    fn reset(&mut self) -> Result<()>;

    // attributes used for build invert index

    fn clear_attributes(&mut self) {
        self.offset_attribute_mut().clear();
        self.position_attribute_mut().clear();
        if let Some(ref mut attr) = self.payload_attribute_mut() {
            attr.clear();
        }
        self.term_bytes_attribute_mut().clear();
    }

    fn end_attributes(&mut self) {
        self.offset_attribute_mut().end();
        self.position_attribute_mut().end();
        if let Some(ref mut attr) = self.payload_attribute_mut() {
            attr.end();
        }
        self.term_bytes_attribute_mut().end();
    }

    fn offset_attribute_mut(&mut self) -> &mut OffsetAttribute;

    fn offset_attribute(&self) -> &OffsetAttribute;

    fn position_attribute_mut(&mut self) -> &mut PositionIncrementAttribute;

    fn payload_attribute_mut(&mut self) -> Option<&mut PayloadAttribute> {
        None
    }

    fn payload_attribute(&self) -> Option<&PayloadAttribute> {
        None
    }

    fn term_bytes_attribute_mut(&mut self) -> &mut dyn TermToBytesRefAttribute;

    fn term_bytes_attribute(&self) -> &dyn TermToBytesRefAttribute;
}
