use core::index::term::{OrdTermState, SeekStatus, TermIterator, TermState};
use core::index::SortedDocValues;
use core::search::posting_iterator::PostingIterator;

use error::ErrorKind::UnsupportedOperation;
use error::Result;

/// Implements a `TermIterator` wrapping a provided `SortedDocValues`
pub struct SortedDocValuesTermIterator<'a> {
    values: &'a SortedDocValues,
    current_ord: i32,
    scratch: Vec<u8>,
}

impl<'a> SortedDocValuesTermIterator<'a> {
    /// Creates a new TermIterator over the provided values
    pub fn new<'c, 'b: 'c>(values: &'b SortedDocValues) -> SortedDocValuesTermIterator<'c> {
        SortedDocValuesTermIterator {
            values,
            current_ord: -1,
            scratch: vec![],
        }
    }
}

impl<'a> TermIterator for SortedDocValuesTermIterator<'a> {
    fn next(&mut self) -> Result<Vec<u8>> {
        self.current_ord += 1;
        if self.current_ord >= self.values.get_value_count() as i32 {
            // FIXME: return Option<&[u8]> instead
            Ok(Vec::new())
        } else {
            let bytes = self.values.lookup_ord(self.current_ord)?;
            self.scratch = bytes.to_vec();
            Ok(self.scratch.clone())
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

    fn seek_exact_ord(&mut self, ord: i64) -> Result<()> {
        assert!(ord >= 0 && ord < self.values.get_value_count() as i64);
        self.current_ord = ord as i32;
        let bytes = self.values.lookup_ord(self.current_ord)?;
        self.scratch = bytes.to_vec();
        Ok(())
    }

    fn seek_exact_state(&mut self, _text: &[u8], state: &TermState) -> Result<()> {
        self.seek_exact_ord(state.ord())
    }

    fn term(&self) -> Result<&[u8]> {
            Ok(&self.scratch)
    }

    fn ord(&mut self) -> Result<i64> {
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

    fn postings_with_flags(&mut self, _flags: i16) -> Result<Box<PostingIterator>> {
        bail!(UnsupportedOperation(
            "postings_with_flags unsupported for SortedDocValuesTermIterator".into()
        ))
    }

    fn term_state(&mut self) -> Result<Box<TermState>> {
        let state = OrdTermState {
            ord: i64::from(self.current_ord),
        };
        Ok(Box::new(state))
    }
}
