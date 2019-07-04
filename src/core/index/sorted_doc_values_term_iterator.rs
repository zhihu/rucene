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
use core::index::SortedDocValues;
use core::search::posting_iterator::EmptyPostingIterator;

use error::ErrorKind::UnsupportedOperation;
use error::Result;

/// Implements a `TermIterator` wrapping a provided `SortedDocValues`
pub(crate) struct SortedDocValuesTermIterator<T: SortedDocValues + 'static> {
    values: *mut T,
    current_ord: i32,
    scratch: Vec<u8>,
}

impl<T: SortedDocValues + 'static> SortedDocValuesTermIterator<T> {
    /// Creates a new TermIterator over the provided values
    pub fn new(values: &T) -> SortedDocValuesTermIterator<T> {
        SortedDocValuesTermIterator {
            values: values as *const _ as *mut _,
            current_ord: -1,
            scratch: vec![],
        }
    }

    #[inline]
    fn values(&mut self) -> &mut T {
        unsafe { &mut *self.values }
    }
}

impl<T: SortedDocValues + 'static> TermIterator for SortedDocValuesTermIterator<T> {
    type Postings = EmptyPostingIterator;
    type TermState = OrdTermState;
    fn next(&mut self) -> Result<Option<Vec<u8>>> {
        self.current_ord += 1;
        if self.current_ord >= self.values().value_count() as i32 {
            // FIXME: return Option<&[u8]> instead
            Ok(None)
        } else {
            let ord = self.current_ord;
            let bytes = self.values().lookup_ord(ord)?;
            self.scratch = bytes.to_vec();
            Ok(Some(self.scratch.clone()))
        }
    }

    fn seek_exact(&mut self, text: &[u8]) -> Result<bool> {
        let ord = self.values().lookup_term(text)?;
        if ord >= 0 {
            self.current_ord = ord;
            self.scratch = text.to_vec();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn seek_ceil(&mut self, text: &[u8]) -> Result<SeekStatus> {
        let ord = self.values().lookup_term(text)?;
        if ord >= 0 {
            self.current_ord = ord;
            self.scratch = text.to_vec();
            Ok(SeekStatus::Found)
        } else {
            self.current_ord = -ord - 1;
            if self.current_ord == self.values().value_count() as i32 {
                Ok(SeekStatus::End)
            } else {
                // TODO: hmm can we avoid this "extra" lookup?
                let ord = self.current_ord;
                let bytes = self.values().lookup_ord(ord)?;
                self.scratch = bytes.to_vec();
                Ok(SeekStatus::NotFound)
            }
        }
    }

    fn seek_exact_ord(&mut self, ord: i64) -> Result<()> {
        assert!(ord >= 0 && ord < self.values().value_count() as i64);
        self.current_ord = ord as i32;
        let bytes = self.values().lookup_ord(ord as i32)?;
        self.scratch = bytes.to_vec();
        Ok(())
    }

    fn seek_exact_state(&mut self, _text: &[u8], state: &Self::TermState) -> Result<()> {
        self.seek_exact_ord(state.ord())
    }

    fn term(&self) -> Result<&[u8]> {
        Ok(&self.scratch)
    }

    fn ord(&self) -> Result<i64> {
        Ok(i64::from(self.current_ord))
    }

    fn doc_freq(&mut self) -> Result<i32> {
        bail!(UnsupportedOperation(
            "doc_freq unsupported for SortedDocValuesTermIterator".into()
        ))
    }

    fn total_term_freq(&mut self) -> Result<i64> {
        Ok(-1)
    }

    fn postings_with_flags(&mut self, _flags: u16) -> Result<Self::Postings> {
        bail!(UnsupportedOperation(
            "postings_with_flags unsupported for SortedDocValuesTermIterator".into()
        ))
    }

    fn term_state(&mut self) -> Result<Self::TermState> {
        let state = OrdTermState {
            ord: i64::from(self.current_ord),
        };
        Ok(state)
    }
}
