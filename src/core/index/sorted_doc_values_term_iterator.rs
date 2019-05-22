use core::index::term::{OrdTermState, SeekStatus, TermIterator, TermState};
use core::index::SortedDocValues;
use core::search::posting_iterator::EmptyPostingIterator;

use error::ErrorKind::UnsupportedOperation;
use error::Result;

/// Implements a `TermIterator` wrapping a provided `SortedDocValues`
pub struct SortedDocValuesTermIterator<T: SortedDocValues + 'static> {
    values: T,
    current_ord: i32,
    scratch: Vec<u8>,
}

impl<T: SortedDocValues + 'static> SortedDocValuesTermIterator<T> {
    /// Creates a new TermIterator over the provided values
    pub fn new(values: T) -> SortedDocValuesTermIterator<T> {
        SortedDocValuesTermIterator {
            values,
            current_ord: -1,
            scratch: vec![],
        }
    }
}

impl<T: SortedDocValues + 'static> TermIterator for SortedDocValuesTermIterator<T> {
    type Postings = EmptyPostingIterator;
    type TermState = OrdTermState;
    fn next(&mut self) -> Result<Option<Vec<u8>>> {
        self.current_ord += 1;
        if self.current_ord >= self.values.get_value_count() as i32 {
            // FIXME: return Option<&[u8]> instead
            Ok(None)
        } else {
            let bytes = self.values.lookup_ord(self.current_ord)?;
            self.scratch = bytes.to_vec();
            Ok(Some(self.scratch.clone()))
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

    fn seek_ceil(&mut self, text: &[u8]) -> Result<SeekStatus> {
        let ord = self.values.lookup_term(text)?;
        if ord >= 0 {
            self.current_ord = ord;
            self.scratch = text.to_vec();
            Ok(SeekStatus::Found)
        } else {
            self.current_ord = -ord - 1;
            if self.current_ord == self.values.get_value_count() as i32 {
                Ok(SeekStatus::End)
            } else {
                // TODO: hmm can we avoid this "extra" lookup?
                let bytes = self.values.lookup_ord(self.current_ord)?;
                self.scratch = bytes.to_vec();
                Ok(SeekStatus::NotFound)
            }
        }
    }

    fn seek_exact_ord(&mut self, ord: i64) -> Result<()> {
        assert!(ord >= 0 && ord < self.values.get_value_count() as i64);
        self.current_ord = ord as i32;
        let bytes = self.values.lookup_ord(self.current_ord)?;
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
