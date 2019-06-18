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

use core::index::term::{OrdTermState, SeekStatus, TermIterator};
use core::index::SortedSetDocValues;
use core::search::posting_iterator::EmptyPostingIterator;
use error::ErrorKind::*;
use error::Result;

/// Implements a `TermIterator` wrapping a provided `SortedSetDocValues`
pub struct SortedSetDocValuesTermIterator<T: SortedSetDocValues + 'static> {
    values: T,
    current_ord: i64,
    scratch: Vec<u8>,
}

impl<T: SortedSetDocValues + 'static> SortedSetDocValuesTermIterator<T> {
    /// Creates a new TermIterator over the provided values
    pub fn new(values: T) -> SortedSetDocValuesTermIterator<T> {
        SortedSetDocValuesTermIterator {
            values,
            current_ord: -1,
            scratch: vec![],
        }
    }
}

impl<T: SortedSetDocValues + 'static> TermIterator for SortedSetDocValuesTermIterator<T> {
    type Postings = EmptyPostingIterator;
    type TermState = OrdTermState;
    fn next(&mut self) -> Result<Option<Vec<u8>>> {
        self.current_ord += 1;
        if self.current_ord >= self.values.get_value_count() as i64 {
            // FIXME: return Option<&[u8]> instead
            Ok(None)
        } else {
            let bytes = self.values.lookup_ord(self.current_ord)?;
            self.scratch = bytes;
            Ok(Some(self.scratch.clone()))
        }
    }

    fn seek_ceil(&mut self, text: &[u8]) -> Result<SeekStatus> {
        let ord = self.values.lookup_term(text)?;
        if ord >= 0 {
            self.current_ord = ord;
            self.scratch = text.to_vec();
            Ok(SeekStatus::Found)
        } else {
            self.current_ord = -ord - 1;
            if self.current_ord == self.values.get_value_count() as i64 {
                Ok(SeekStatus::End)
            } else {
                // TODO: hmm can we avoid this "extra" lookup?
                let bytes = self.values.lookup_ord(self.current_ord)?;
                self.scratch = bytes;
                Ok(SeekStatus::NotFound)
            }
        }
    }

    fn seek_exact(&mut self, text: &[u8]) -> Result<bool> {
        let ord = self.values.lookup_term(text)?;
        if ord >= 0 {
            self.current_ord = ord;
            self.scratch = text.to_vec();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn seek_exact_state(&mut self, _text: &[u8], state: &Self::TermState) -> Result<()> {
        self.seek_exact_ord(state.ord())
    }

    fn seek_exact_ord(&mut self, ord: i64) -> Result<()> {
        assert!(ord >= 0 && ord < self.values.get_value_count() as i64);
        self.current_ord = ord;
        let bytes = self.values.lookup_ord(self.current_ord)?;
        self.scratch = bytes;
        Ok(())
    }

    fn term(&self) -> Result<&[u8]> {
        Ok(&self.scratch)
    }

    fn ord(&self) -> Result<i64> {
        Ok(self.current_ord)
    }

    fn doc_freq(&mut self) -> Result<i32> {
        bail!(UnsupportedOperation(
            "doc_freq unsupported for SortedSetDocValuesTermIterator".into()
        ))
    }

    fn total_term_freq(&mut self) -> Result<i64> {
        Ok(-1)
    }

    fn postings_with_flags(&mut self, _flags: u16) -> Result<Self::Postings> {
        bail!(UnsupportedOperation(
            "postings_with_flags unsupported for SortedSetDocValuesTermIterator".into()
        ))
    }

    fn term_state(&mut self) -> Result<Self::TermState> {
        let state = OrdTermState {
            ord: self.current_ord,
        };
        Ok(state)
    }
}
