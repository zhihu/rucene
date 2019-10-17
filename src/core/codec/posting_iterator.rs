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

use core::search::{DocIterator, Payload, NO_MORE_DOCS};
use core::util::DocId;
use error::Result;

pub struct PostingIteratorFlags;

/// flags constants and helper function defined for `TermIterator::postings_with_flag()`.
impl PostingIteratorFlags {
    /// Flag to pass to {@link TermIterator#postings_with_flags(u16)} if you don't
    /// require per-document postings in the returned iterator.
    pub const NONE: u16 = 0;

    /// Flag to pass to {@link TermIterator#postings_with_flags(u16)}
    /// if you require term frequencies in the returned iterator.
    pub const FREQS: u16 = 1 << 3;

    /// Flag to pass to {@link TermIterator#postings_with_flags(u16)}
    /// if you require term positions in the returned iterator.
    pub const POSITIONS: u16 = Self::FREQS | 1 << 4;

    /// Flag to pass to {@link TermIterator#postings_with_flags(u16)}
    /// if you require offsets in the returned iterator.
    pub const OFFSETS: u16 = Self::POSITIONS | 1 << 5;

    /// Flag to pass to  {@link TermIterator#postings_with_flags(u16)}
    /// if you require payloads in the returned iterator.
    pub const PAYLOADS: u16 = Self::POSITIONS | 1 << 6;

    /// Flag to pass to {@link TermIterator#postings_with_flags(u16)}
    /// to get positions, payloads and offsets in the returned iterator.
    pub const ALL: u16 = Self::OFFSETS | Self::PAYLOADS;

    pub fn feature_requested(flags: u16, feature: u16) -> bool {
        (flags & feature) == feature
    }
}

/// Iterates through the postings.
///
/// NOTE: you must first call `next()` before using any of the per-doc methods.
pub trait PostingIterator: DocIterator {
    /// Returns term frequency in the current document, or 1 if the field was
    /// indexed with `IndexOptions::Docs`. Do not call this before
    /// `next_doc()` is first called, nor after `#next()` returns `NO_MORE_DOCS`.
    ///
    /// *NOTE:* if the [`PostingIterator`] was obtain with `PostingIteratorFlags::NONE`,
    /// the result of this method is undefined.
    fn freq(&self) -> Result<i32>;

    /// Returns the next position, or -1 if positions were not indexed.
    /// Calling this more than `freq()` times is undefined.
    fn next_position(&mut self) -> Result<i32>;

    /// Returns start offset for the current position, or -1
    /// if offsets were not indexed. */
    fn start_offset(&self) -> Result<i32>;

    /// Returns end offset for the current position, or -1 if
    /// offsets were not indexed. */
    fn end_offset(&self) -> Result<i32>;

    /// Returns the payload at this position, or null if no
    /// payload was indexed. You should not modify anything
    /// (neither members of the returned BytesRef nor bytes
    /// in the bytes). */
    fn payload(&self) -> Result<Payload>;
}

/// a `PostingIterator` that no matching docs are available.
#[derive(Clone)]
pub struct EmptyPostingIterator {
    doc_id: DocId,
}

impl Default for EmptyPostingIterator {
    fn default() -> Self {
        EmptyPostingIterator { doc_id: -1 }
    }
}

impl DocIterator for EmptyPostingIterator {
    fn doc_id(&self) -> DocId {
        self.doc_id
    }

    fn next(&mut self) -> Result<DocId> {
        self.doc_id = NO_MORE_DOCS;
        Ok(NO_MORE_DOCS)
    }

    fn advance(&mut self, _target: DocId) -> Result<DocId> {
        self.doc_id = NO_MORE_DOCS;
        Ok(NO_MORE_DOCS)
    }

    fn cost(&self) -> usize {
        0usize
    }
}

impl PostingIterator for EmptyPostingIterator {
    fn freq(&self) -> Result<i32> {
        Ok(0)
    }

    fn next_position(&mut self) -> Result<i32> {
        Ok(-1)
    }

    fn start_offset(&self) -> Result<i32> {
        Ok(-1)
    }

    fn end_offset(&self) -> Result<i32> {
        Ok(-1)
    }

    fn payload(&self) -> Result<Payload> {
        Ok(Payload::new())
    }
}
