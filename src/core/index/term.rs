use error::ErrorKind::*;
use error::Result;

use std::mem::transmute;
use std::sync::Arc;

use core::search::posting_iterator::POSTING_ITERATOR_FLAG_FREQS;
use core::search::posting_iterator::{EmptyPostingIterator, PostingIterator};

/// Encapsulates all required internal state to position the associated
/// `TermIterator` without re-seeking
pub trait TermState {
    fn ord(&self) -> i64;

    fn serialize(&self) -> Vec<u8>;
}

/// An ordinal based `TermState`
pub struct OrdTermState {
    /// Term ordinal, i.e. its position in the full list of sorted terms
    pub ord: i64,
}

impl TermState for OrdTermState {
    fn ord(&self) -> i64 {
        self.ord
    }

    fn serialize(&self) -> Vec<u8> {
        let r: [u8; 8] = unsafe { transmute(self.ord.to_be()) };
        r.to_vec()
    }
}

/// Access to the terms in a specific field.  See {@link Fields}.
/// @lucene.experimental
///
pub trait Terms: Send + Sync {
    /// Returns an iterator that will step through all
    /// terms. This method will not return null. */
    fn iterator(&self) -> Result<Box<TermIterator>>;

    /// Returns a TermsEnum that iterates over all terms and
    /// documents that are accepted by the provided {@link
    /// CompiledAutomaton}.  If the <code>startTerm</code> is
    /// provided then the returned enum will only return terms
    /// {@code > startTerm}, but you still must call
    /// next() first to get to the first term.  Note that the
    /// provided <code>startTerm</code> must be accepted by
    /// the automaton.
    ///
    /// <p>This is an expert low-level API and will only work
    /// for {@code NORMAL} compiled automata.  To handle any
    /// compiled automata you should instead use
    /// {@link CompiledAutomaton#getTermsEnum} instead.
    ///
    /// <p><b>NOTE</b>: the returned TermsEnum cannot seek</p>.
    ///
    /// <p><b>NOTE</b>: the terms dictionary is free to
    /// return arbitrary terms as long as the resulted visited
    /// docs is the same.  E.g., {@link BlockTreeTermsWriter}
    /// creates auto-prefix terms during indexing to reduce the
    /// number of terms visited. */
    // fn TermsEnum intersect(CompiledAutomaton compiled, final BytesRef startTerm) throws
    // IOException {
    //
    // TODO: could we factor out a common interface b/w
    // CompiledAutomaton and FST?  Then we could pass FST there too,
    // and likely speed up resolving terms to deleted docs ... but
    // AutomatonTermsEnum makes this tricky because of its on-the-fly cycle
    // detection
    //
    // TODO: eventually we could support seekCeil/Exact on
    // the returned enum, instead of only being able to seek
    // at the start
    //
    // TermsEnum termsEnum = iterator();
    //
    // if (compiled.type != CompiledAutomaton.AUTOMATON_TYPE.NORMAL) {
    // throw new IllegalArgumentException("please use CompiledAutomaton.getTermsEnum instead");
    // }
    //
    // if (startTerm == null) {
    // return new AutomatonTermsEnum(termsEnum, compiled);
    // } else {
    // return new AutomatonTermsEnum(termsEnum, compiled) {
    // @Override
    // protected BytesRef nextSeekTerm(BytesRef term) throws IOException {
    // if (term == null) {
    // term = startTerm;
    // }
    // return super.nextSeekTerm(term);
    // }
    // };
    // }
    // }
    //
    /// Returns the number of terms for this field, or -1 if this
    /// measure isn't stored by the codec. Note that, just like
    /// other term measures, this measure does not take deleted
    /// documents into account. */
    fn size(&self) -> Result<i64>;

    /// Returns the sum of {@link TermsEnum#totalTermFreq} for
    /// all terms in this field, or -1 if this measure isn't
    /// stored by the codec (or if this fields omits term freq
    /// and positions).  Note that, just like other term
    /// measures, this measure does not take deleted documents
    /// into account. */
    fn sum_total_term_freq(&self) -> Result<i64>;

    /// Returns the sum of {@link TermsEnum#docFreq()} for
    /// all terms in this field, or -1 if this measure isn't
    /// stored by the codec.  Note that, just like other term
    /// measures, this measure does not take deleted documents
    /// into account. */
    fn sum_doc_freq(&self) -> Result<i64>;

    /// Returns the number of documents that have at least one
    /// term for this field, or -1 if this measure isn't
    /// stored by the codec.  Note that, just like other term
    /// measures, this measure does not take deleted documents
    /// into account. */
    fn doc_count(&self) -> Result<i32>;

    /// Returns true if documents in this field store
    /// per-document term frequency ({@link PostingsEnum#freq}). */
    fn has_freqs(&self) -> Result<bool>;

    /// Returns true if documents in this field store offsets. */
    fn has_offsets(&self) -> Result<bool>;

    /// Returns true if documents in this field store positions. */
    fn has_positions(&self) -> Result<bool>;

    /// Returns true if documents in this field store payloads. */
    fn has_payloads(&self) -> Result<bool>;

    /// Returns the smallest term (in lexicographic order) in the field.
    /// Note that, just like other term measures, this measure does not
    /// take deleted documents into account.  This returns
    /// null when there are no terms. */
    fn min(&self) -> Result<Vec<u8>> {
        self.iterator()?.as_mut().next()
    }
    /// Returns the largest term (in lexicographic order) in the field.
    /// Note that, just like other term measures, this measure does not
    /// take deleted documents into account.  This returns
    /// null when there are no terms. */
    fn max(&self) -> Result<Vec<u8>> {
        let size = self.size()?;
        if size == 0 {
            // empty: only possible from a FilteredTermsEnum...
            return Ok(Vec::with_capacity(0));
        } else if size > 0 {
            let mut iterator = self.iterator()?;
            iterator.as_mut().seek_exact_ord(size - 1)?;
            let term = iterator.as_mut().term()?;
            return Ok(term.to_vec());
        }

        // otherwise: binary search
        let mut iterator = self.iterator()?;
        let v = iterator.as_mut().next()?;
        if v.is_empty() {
            // empty: only possible from a FilteredTermsEnum...
            return Ok(v);
        }

        let mut scratch = Vec::new();
        scratch.push(0u8);

        // iterates over digits:
        loop {
            let mut low = 0;
            let mut high = 255;

            // Binary search current digit to find the highest
            // digit before END:
            while low != high {
                let mid = (low + high) / 2;
                let length = scratch.len();
                scratch[length - 1usize] = mid as u8;
                if iterator.as_mut().seek_ceil(&scratch)? == SeekStatus::End {
                    // scratch was to high
                    if mid == 0 {
                        scratch.pop();
                        return Ok(scratch);
                    }
                    high = mid;
                } else {
                    // Scratch was too low; there is at least one term
                    // still after it:
                    if low == mid {
                        break;
                    }
                    low = mid;
                }
            }

            // Recurse to next digit:
            scratch.push(0u8);
        }
    }

    /// Expert: returns additional information about this Terms instance
    /// for debugging purposes.
    ///
    fn stats(&self) -> Result<String> {
        Ok(format!(
            "size={:?}, doc_count={:?}, sum_total_term_freq={:?}, sum_doc_freq={:?}",
            self.size(),
            self.doc_count(),
            self.sum_total_term_freq(),
            self.sum_doc_freq()
        ))
    }
}

#[derive(Default, Debug)]
pub struct EmptyTerms;

impl EmptyTerms {
    pub fn new() -> EmptyTerms {
        EmptyTerms {}
    }
}

impl Terms for EmptyTerms {
    fn iterator(&self) -> Result<Box<TermIterator>> {
        unimplemented!()
    }

    fn size(&self) -> Result<i64> {
        Ok(0)
    }

    fn sum_total_term_freq(&self) -> Result<i64> {
        Ok(0)
    }

    fn sum_doc_freq(&self) -> Result<i64> {
        Ok(0)
    }

    fn doc_count(&self) -> Result<i32> {
        Ok(0)
    }

    fn has_freqs(&self) -> Result<bool> {
        Ok(false)
    }

    fn has_offsets(&self) -> Result<bool> {
        Ok(false)
    }

    fn has_positions(&self) -> Result<bool> {
        Ok(false)
    }

    fn has_payloads(&self) -> Result<bool> {
        Ok(false)
    }
}

pub type TermsRef = Arc<Terms>;

/// Represents returned result from {@link #seekCeil}.
#[derive(PartialEq)]
pub enum SeekStatus {
    /// The term was not found, and the end of iteration was hit.
    End,
    /// The precise term was found.
    Found,
    /// A different term was found after the requested term
    NotFound,
}

pub trait TermIterator {
    /// Increments the iteration to the next {@link BytesRef} in the iterator.
    /// Returns the resulting {@link BytesRef} or <code>null</code> if the end of
    /// the iterator is reached. The returned BytesRef may be re-used across calls
    /// to next. After this method returns null, do not call it again: the results
    /// are undefined.
    ///
    /// @return the next term in the iterator or empty vector if
    /// the end of the iterator is reached.
    /// @throws IOException If there is a low-level I/O error.
    ///
    fn next(&mut self) -> Result<Vec<u8>>;

    /// Attempts to seek to the exact term, returning
    /// true if the term is found.  If this returns false, the
    /// enum is unpositioned.  For some codecs, seekExact may
    /// be substantially faster than {@link #seekCeil}. */
    fn seek_exact(&mut self, text: &[u8]) -> Result<bool> {
        Ok(match self.seek_ceil(text)? {
            SeekStatus::Found => true,
            _ => false,
        })
    }

    /// Seeks to the specified term, if it exists, or to the
    /// next (ceiling) term.  Returns SeekStatus to
    /// indicate whether exact term was found, a different
    /// term was found, or EOF was hit.  The target term may
    /// be before or after the current term.  If this returns
    /// SeekStatus.END, the enum is unpositioned.
    fn seek_ceil(&mut self, text: &[u8]) -> Result<SeekStatus>;

    /// Seeks to the specified term by ordinal (position) as
    /// previously returned by {@link #ord}.  The target ord
    /// may be before or after the current ord, and must be
    /// within bounds.
    fn seek_exact_ord(&mut self, ord: i64) -> Result<()>;

    fn seek_exact_state(&mut self, text: &[u8], _state: &TermState) -> Result<()> {
        self.seek_exact(text).and_then(|r| {
            if r {
                Ok(())
            } else {
                bail!(IllegalArgument(format!("Term {:?} does not exist", text)))
            }
        })
    }

    /// Returns current term. Do not call this when the enum
    /// is unpositioned.
    fn term(&self) -> Result<&[u8]>;

    /// Returns ordinal position for current term.  This is an
    /// optional method (the codec may throw {@link
    /// UnsupportedOperationException}).  Do not call this
    /// when the enum is unpositioned.
    fn ord(&mut self) -> Result<i64>;

    /// Returns the number of documents containing the current
    /// term.  Do not call this when the enum is unpositioned.
    /// {@link SeekStatus#END}.
    fn doc_freq(&mut self) -> Result<i32>;

    /// Returns the total number of occurrences of this term
    /// across all documents (the sum of the freq() for each
    /// doc that has this term).  This will be -1 if the
    /// codec doesn't support this measure.  Note that, like
    /// other term measures, this measure does not take
    /// deleted documents into account.
    fn total_term_freq(&mut self) -> Result<i64>;

    /// Get {@link PostingsEnum} for the current term.  Do not
    /// call this when the enum is unpositioned.  This method
    /// will not return null.
    /// <p>
    /// <b>NOTE</b>: the returned iterator may return deleted documents, so
    /// deleted documents have to be checked on top of the {@link PostingsEnum}.
    /// <p>
    /// Use this method if you only require documents and frequencies,
    /// and do not need any proximity data.
    /// This method is equivalent to
    /// {@link #postings(PostingsEnum, int) postings(reuse, PostingsEnum.FREQS)}
    ///
    /// @param reuse pass a prior PostingsEnum for possible reuse
    /// @see #postings(PostingsEnum, int)
    fn postings(&mut self) -> Result<Box<PostingIterator>> {
        self.postings_with_flags(POSTING_ITERATOR_FLAG_FREQS)
    }
    fn postings_with_flags(&mut self, flags: i16) -> Result<Box<PostingIterator>>;

    /// Expert: Returns the TermsEnums internal state to position the TermsEnum
    /// without re-seeking the term dictionary.
    /// <p>
    /// NOTE: A seek by {@link TermState} might not capture the
    /// {@link AttributeSource}'s state. Callers must maintain the
    /// {@link AttributeSource} states separately
    ///
    /// @see TermState
    /// @see #seekExact(BytesRef, TermState)
    fn term_state(&mut self) -> Result<Box<TermState>> {
        bail!(UnsupportedOperation(
            "TermIterator::term_state unsupported".into()
        ))
    }
}

pub struct EmptyTermIterator {
    data: Vec<u8>,
}

impl Default for EmptyTermIterator {
    fn default() -> EmptyTermIterator {
        EmptyTermIterator { data: vec![0; 1] }
    }
}

impl TermIterator for EmptyTermIterator {
    fn next(&mut self) -> Result<Vec<u8>> {
        // TODO fix me
        bail!("no more terms!")
    }

    fn seek_ceil(&mut self, _text: &[u8]) -> Result<SeekStatus> {
        Ok(SeekStatus::End)
    }

    fn seek_exact_ord(&mut self, _ord: i64) -> Result<()> {
        unreachable!()
    }

    fn term(&self) -> Result<&[u8]> {
        Ok(&self.data[0..0])
    }

    fn ord(&mut self) -> Result<i64> {
        Ok(-1)
    }

    fn doc_freq(&mut self) -> Result<i32> {
        Ok(-1)
    }

    fn total_term_freq(&mut self) -> Result<i64> {
        Ok(-1)
    }

    fn postings_with_flags(&mut self, _flags: i16) -> Result<Box<PostingIterator>> {
        Ok(Box::new(EmptyPostingIterator::default()))
    }
}
