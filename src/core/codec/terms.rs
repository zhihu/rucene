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

use core::codec::{EmptyPostingIterator, PostingIterator, PostingIteratorFlags};

use error::ErrorKind::{IllegalArgument, UnsupportedOperation};
use error::Result;

use std::sync::Arc;

/// Encapsulates all required internal state to position the associated
/// `TermIterator` without re-seeking
pub trait TermState: Send + Sync + Clone {}

/// for `TermIterator`s that aren't support `TermState`.
impl TermState for () {}

/// An ordinal based `TermState`
#[derive(Clone)]
pub struct OrdTermState {
    /// Term ordinal, i.e. its position in the full list of sorted terms
    pub ord: i64,
}

impl OrdTermState {
    pub fn ord(&self) -> i64 {
        self.ord
    }
}

impl TermState for OrdTermState {}

/// Access to the terms in a specific field.  See `Fields`.
pub trait Terms {
    type Iterator: TermIterator;
    /// Returns an iterator that will step through all
    /// terms. This method will not return null. */
    fn iterator(&self) -> Result<Self::Iterator>;

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
    fn min(&self) -> Result<Option<Vec<u8>>> {
        self.iterator()?.next()
    }
    /// Returns the largest term (in lexicographic order) in the field.
    /// Note that, just like other term measures, this measure does not
    /// take deleted documents into account.  This returns
    /// null when there are no terms. */
    fn max(&self) -> Result<Option<Vec<u8>>> {
        let size = self.size()?;
        if size == 0 {
            // empty: only possible from a FilteredTermsEnum...
            return Ok(None);
        } else if size > 0 {
            let mut iterator = self.iterator()?;
            iterator.seek_exact_ord(size - 1)?;
            let term = iterator.term()?;
            return Ok(Some(term.to_vec()));
        }

        // otherwise: binary search
        let mut iterator = self.iterator()?;
        let v = iterator.next()?;
        if v.is_none() {
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
                if iterator.seek_ceil(&scratch)? == SeekStatus::End {
                    // scratch was to high
                    if mid == 0 {
                        scratch.pop();
                        return Ok(Some(scratch));
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

impl<T: Terms> Terms for Arc<T> {
    type Iterator = T::Iterator;
    fn iterator(&self) -> Result<Self::Iterator> {
        (**self).iterator()
    }

    fn size(&self) -> Result<i64> {
        (**self).size()
    }

    fn sum_total_term_freq(&self) -> Result<i64> {
        (**self).sum_total_term_freq()
    }

    fn sum_doc_freq(&self) -> Result<i64> {
        (**self).sum_doc_freq()
    }

    fn doc_count(&self) -> Result<i32> {
        (**self).doc_count()
    }

    fn has_freqs(&self) -> Result<bool> {
        (**self).has_freqs()
    }

    fn has_offsets(&self) -> Result<bool> {
        (**self).has_offsets()
    }

    fn has_positions(&self) -> Result<bool> {
        (**self).has_positions()
    }

    fn has_payloads(&self) -> Result<bool> {
        (**self).has_payloads()
    }

    fn min(&self) -> Result<Option<Vec<u8>>> {
        (**self).min()
    }

    fn max(&self) -> Result<Option<Vec<u8>>> {
        (**self).max()
    }

    fn stats(&self) -> Result<String> {
        (**self).stats()
    }
}

/// Represents returned result from {@link #seekCeil}.
#[derive(PartialEq, Debug)]
pub enum SeekStatus {
    /// The term was not found, and the end of iteration was hit.
    End,
    /// The precise term was found.
    Found,
    /// A different term was found after the requested term
    NotFound,
}

pub trait TermIterator: 'static {
    type Postings: PostingIterator;
    type TermState: TermState;
    /// Increments the iteration to the next {@link BytesRef} in the iterator.
    /// Returns the resulting {@link BytesRef} or <code>null</code> if the end of
    /// the iterator is reached. The returned BytesRef may be re-used across calls
    /// to next. After this method returns null, do not call it again: the results
    /// are undefined.
    ///
    /// @return the next term in the iterator or empty vector if
    /// the end of the iterator is reached.
    /// @throws IOException If there is a low-level I/O error.
    fn next(&mut self) -> Result<Option<Vec<u8>>>;

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

    fn seek_exact_state(&mut self, text: &[u8], _state: &Self::TermState) -> Result<()> {
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
    fn ord(&self) -> Result<i64>;

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
    fn postings(&mut self) -> Result<Self::Postings> {
        self.postings_with_flags(PostingIteratorFlags::FREQS)
    }
    fn postings_with_flags(&mut self, flags: u16) -> Result<Self::Postings>;

    /// Expert: Returns the TermsEnums internal state to position the TermsEnum
    /// without re-seeking the term dictionary.
    /// <p>
    /// NOTE: A seek by {@link TermState} might not capture the
    /// {@link AttributeSource}'s state. Callers must maintain the
    /// {@link AttributeSource} states separately
    ///
    /// @see TermState
    /// @see #seekExact(BytesRef, TermState)
    fn term_state(&mut self) -> Result<Self::TermState> {
        bail!(UnsupportedOperation(
            "TermIterator::term_state unsupported".into()
        ))
    }

    // whether this Iterator is EmptyIterator
    fn is_empty(&self) -> bool {
        false
    }
}

const EMPTY_BYTES: [u8; 0] = [];

#[derive(Default)]
pub struct EmptyTermIterator;

impl TermIterator for EmptyTermIterator {
    type Postings = EmptyPostingIterator;
    type TermState = ();
    fn next(&mut self) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }

    fn seek_ceil(&mut self, _text: &[u8]) -> Result<SeekStatus> {
        Ok(SeekStatus::End)
    }

    fn seek_exact_ord(&mut self, _ord: i64) -> Result<()> {
        unreachable!()
    }

    fn term(&self) -> Result<&[u8]> {
        Ok(&EMPTY_BYTES)
    }

    fn ord(&self) -> Result<i64> {
        Ok(-1)
    }

    fn doc_freq(&mut self) -> Result<i32> {
        Ok(-1)
    }

    fn total_term_freq(&mut self) -> Result<i64> {
        Ok(-1)
    }

    fn postings_with_flags(&mut self, _flags: u16) -> Result<Self::Postings> {
        Ok(EmptyPostingIterator::default())
    }

    fn is_empty(&self) -> bool {
        true
    }
}

pub struct FilteredTermIterBase<T: TermIterator> {
    pub initial_seek_term: Option<Vec<u8>>,
    pub do_seek: bool,
    pub actual_term: Option<Vec<u8>>,
    pub terms: T,
}

impl<T: TermIterator> FilteredTermIterBase<T> {
    pub fn new(terms: T, start_with_seek: bool) -> Self {
        FilteredTermIterBase {
            initial_seek_term: None,
            do_seek: start_with_seek,
            actual_term: None,
            terms,
        }
    }
}

/// Return value, if term should be accepted or the iteration should
/// {@code END}. The {@code *_SEEK} values denote, that after handling the current term
/// the enum should call {@link #nextSeekTerm} and step forward.
/// @see #accept(BytesRef)
pub enum AcceptStatus {
    /// Accept the term and position the enum at the next term.
    Yes,
    /// Accept the term and advance `FilteredTermsEnum#nextSeekTerm(BytesRef)` to the next term
    YesAndSeek,
    /// Reject the term and position the enum at the next term.
    No,
    /// Reject the term and advance `FilteredTermsEnum#nextSeekTerm(BytesRef)` to the next term
    NoAndSeek,
    /// Reject the term and stop enumerating.
    End,
}

/// enumerating a subset of all terms.
///
/// Term enumerations are always ordered by `BytesRef`. Each term in the enumeration is
/// greater than all that precede it.
/// *Please Note*: Consumers of this iterator cannot call seek(), it is forward only.
pub trait FilteredTermIterator {
    type Iter: TermIterator;
    fn base(&self) -> &FilteredTermIterBase<Self::Iter>;

    fn base_mut(&mut self) -> &mut FilteredTermIterBase<Self::Iter>;

    fn accept(&self, term: &[u8]) -> Result<AcceptStatus>;

    fn set_initial_seek_term(&mut self, term: Vec<u8>) {
        self.base_mut().initial_seek_term = Some(term);
    }

    fn next_seek_term(&mut self) -> Option<Vec<u8>> {
        self.base_mut().initial_seek_term.take()
    }
}

impl<T> TermIterator for T
where
    T: FilteredTermIterator + 'static,
{
    type Postings = <T::Iter as TermIterator>::Postings;
    type TermState = <T::Iter as TermIterator>::TermState;
    fn next(&mut self) -> Result<Option<Vec<u8>>> {
        loop {
            if self.base().do_seek {
                self.base_mut().do_seek = false;
                let t = self.next_seek_term();
                // Make sure we always seek forward:
                debug_assert!(
                    self.base().actual_term.is_none()
                        || t.is_none()
                        || t.as_ref().unwrap() > self.base().actual_term.as_ref().unwrap()
                );
                if t.is_none()
                    || self.base_mut().terms.seek_ceil(t.as_ref().unwrap())? == SeekStatus::End
                {
                    return Ok(None);
                }
                self.base_mut().actual_term = Some(self.base().terms.term()?.to_vec());
            } else {
                self.base_mut().actual_term = self.base_mut().terms.next()?;
                if self.base().actual_term.is_none() {
                    return Ok(None);
                }
            }

            debug_assert!(self.base().actual_term.is_some());
            match self.accept(self.base().actual_term.as_ref().unwrap().as_slice())? {
                AcceptStatus::YesAndSeek => {
                    self.base_mut().do_seek = true;
                }
                AcceptStatus::Yes => {
                    return Ok(self.base().actual_term.clone());
                }
                AcceptStatus::NoAndSeek => {
                    self.base_mut().do_seek = true;
                    break;
                }
                AcceptStatus::End => {
                    return Ok(None);
                }
                _ => {}
            }
        }
        unreachable!()
    }

    fn seek_exact(&mut self, _text: &[u8]) -> Result<bool> {
        bail!(UnsupportedOperation("".into()))
    }

    fn seek_ceil(&mut self, _text: &[u8]) -> Result<SeekStatus> {
        bail!(UnsupportedOperation("".into()))
    }

    fn seek_exact_ord(&mut self, _ord: i64) -> Result<()> {
        bail!(UnsupportedOperation("".into()))
    }

    fn term(&self) -> Result<&[u8]> {
        self.base().terms.term()
    }

    fn ord(&self) -> Result<i64> {
        self.base().terms.ord()
    }

    fn doc_freq(&mut self) -> Result<i32> {
        self.base_mut().terms.doc_freq()
    }

    fn total_term_freq(&mut self) -> Result<i64> {
        self.base_mut().terms.total_term_freq()
    }

    fn postings_with_flags(&mut self, flags: u16) -> Result<Self::Postings> {
        self.base_mut().terms.postings_with_flags(flags)
    }
}
