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

use core::codec::doc_values::lucene54::{Lucene54DocValuesFormat, ReverseTermsIndex};
use core::codec::doc_values::{SortedDocValues, SortedSetDocValues};
use core::codec::{EmptyPostingIterator, OrdTermState, SeekStatus, TermIterator};
use core::store::io::IndexInput;
use core::util::packed::MonotonicBlockPackedReader;
use core::util::{LongValues, UnsignedShift};

use error::ErrorKind::UnsupportedOperation;
use error::Result;
use std::cmp::Ordering;
use std::sync::Arc;

pub struct CompressedBinaryTermIterator {
    num_reverse_index_values: i64,
    reverse_index: Arc<ReverseTermsIndex>,
    addresses: Arc<MonotonicBlockPackedReader>,
    num_values: i64,
    num_index_values: i64,
    current_ord: i64,
    current_block_start: i64,
    input: Box<dyn IndexInput>,
    offsets: [i32; Lucene54DocValuesFormat::INTERVAL_COUNT as usize],
    buffer: [u8; (Lucene54DocValuesFormat::INTERVAL_COUNT * 2 - 1) as usize],
    term: Vec<u8>,
    term_length: u32,
    first_term: Vec<u8>,
    first_term_length: u32,
}

impl CompressedBinaryTermIterator {
    pub fn new(
        input: Box<dyn IndexInput>,
        max_term_length: usize,
        num_reverse_index_values: i64,
        reverse_index: Arc<ReverseTermsIndex>,
        addresses: Arc<MonotonicBlockPackedReader>,
        num_values: i64,
        num_index_values: i64,
    ) -> Result<CompressedBinaryTermIterator> {
        let mut input = input;
        input.seek(0)?;
        Ok(CompressedBinaryTermIterator {
            num_reverse_index_values,
            reverse_index,
            addresses,
            num_values,
            num_index_values,
            current_ord: -1,
            current_block_start: 0,
            input,
            offsets: [0; Lucene54DocValuesFormat::INTERVAL_COUNT as usize],
            buffer: [0u8; (Lucene54DocValuesFormat::INTERVAL_COUNT * 2 - 1) as usize],
            term: vec![0u8; max_term_length],
            term_length: 0,
            first_term: vec![0u8; max_term_length],
            first_term_length: 0,
        })
    }

    fn read_header(&mut self) -> Result<()> {
        {
            let length = self.input.read_vint()? as u32;
            self.input
                .read_bytes(&mut self.first_term, 0, length as usize)?;
            self.first_term_length = length;
            self.input.read_bytes(
                &mut self.buffer,
                0,
                (Lucene54DocValuesFormat::INTERVAL_COUNT - 1) as usize,
            )?;
        }
        if self.buffer[0] == 0xFF {
            self.read_short_addresses()?;
        } else {
            self.read_byte_addresses();
        }
        self.current_block_start = self.input.file_pointer();
        Ok(())
    }

    // read single byte addresses: each is delta - 2
    // (shared prefix byte and length > 0 are both implicit
    fn read_byte_addresses(&mut self) {
        let mut address = 0;
        for i in 1..self.offsets.len() {
            address += 2 + (i32::from(self.buffer[i - 1]) & 0xFF);
            self.offsets[i] = address;
        }
    }

    // read double byte addresses: each is delta -2
    // (shared prefix byte and length > 0 are both implicit
    fn read_short_addresses(&mut self) -> Result<()> {
        self.input.read_bytes(
            &mut self.buffer,
            (Lucene54DocValuesFormat::INTERVAL_COUNT - 1) as usize,
            Lucene54DocValuesFormat::INTERVAL_COUNT as usize,
        )?;
        let mut address = 0;
        for i in 1..self.offsets.len() {
            let x = i << 1;
            address +=
                2 + ((u32::from(self.buffer[x - 1]) << 8) | u32::from(self.buffer[x])) as i32;
            self.offsets[i] = address;
        }
        Ok(())
    }

    // set term to the first term
    fn read_first_term(&mut self) -> Result<()> {
        let length = self.first_term_length as usize;
        self.term[0..length]
            .as_mut()
            .copy_from_slice(&self.first_term[0..length]);
        // FIXME: first_term.offset
        self.term_length = self.first_term_length;
        Ok(())
    }

    // read term at offset, delta encoded from first term
    fn read_term(&mut self, offset: i32) -> Result<()> {
        let start = self.input.read_byte()? as usize;
        // FIXME: first_term.offset
        self.term[0..start]
            .as_mut()
            .copy_from_slice(&self.first_term[0..start]);
        let offset = offset as usize;
        let suffix = (self.offsets[offset] - self.offsets[offset - 1] - 1) as usize;
        self.input.read_bytes(self.term.as_mut(), start, suffix)?;
        self.term_length = (start + suffix) as u32;
        Ok(())
    }

    // binary search reverse index to find smaller range of blocks to search
    fn binary_search_index(&self, text: &[u8]) -> Result<i64> {
        let mut low = 0_i64;
        let mut high = self.num_reverse_index_values - 1;
        while low < high {
            let mid = low + (high - low) / 2;
            let start = self.reverse_index.term_addresses.get64(mid)?;
            let scratch = self.reverse_index.terms.fill(start);
            match scratch[..].as_ref().cmp(text) {
                Ordering::Less => low = mid + 1,
                Ordering::Greater => high = mid - 1,
                Ordering::Equal => return Ok(mid),
            }
        }
        Ok(high)
    }

    // binary search against first term in block range to find term's block
    fn binary_search_block(&mut self, text: &[u8], mut low: i64, mut high: i64) -> Result<i64> {
        while low <= high {
            let mid = low + (high - low) / 2;
            let pos = self.addresses.get64(mid)?;
            self.input.seek(pos)?;
            let length = self.input.read_vint()? as usize;
            self.input.read_bytes(self.term.as_mut(), 0, length)?;
            self.term_length = length as u32;
            match self.term[0..length].as_ref().cmp(text) {
                Ordering::Less => low = mid + 1,
                Ordering::Greater => high = mid - 1,
                Ordering::Equal => return Ok(mid),
            };
        }
        Ok(high)
    }

    #[inline]
    pub fn num_values(&self) -> i64 {
        self.num_values
    }

    pub fn clone(&self) -> Result<Self> {
        let input = self.input.clone()?;
        Ok(Self {
            num_reverse_index_values: self.num_reverse_index_values,
            reverse_index: Arc::clone(&self.reverse_index),
            addresses: Arc::clone(&self.addresses),
            num_values: self.num_values,
            num_index_values: self.num_index_values,
            current_ord: self.current_ord,
            current_block_start: self.current_block_start,
            input,
            offsets: self.offsets,
            buffer: self.buffer,
            term: self.term.clone(),
            term_length: self.term_length,
            first_term: self.first_term.clone(),
            first_term_length: self.first_term_length,
        })
    }
}

impl TermIterator for CompressedBinaryTermIterator {
    type Postings = EmptyPostingIterator;
    // stub type
    type TermState = ();
    fn next(&mut self) -> Result<Option<Vec<u8>>> {
        self.current_ord += 1;
        if self.current_ord >= self.num_values {
            Ok(None)
        } else {
            let offset = self.current_ord as i32 & Lucene54DocValuesFormat::INTERVAL_MASK;
            if offset == 0 {
                // switch to next block
                self.read_header()?;
                self.read_first_term()?;
            } else {
                self.read_term(offset)?;
            }
            Ok(Some(self.term[0..self.term_length as usize].to_vec()))
        }
    }

    fn seek_ceil(&mut self, text: &[u8]) -> Result<SeekStatus> {
        let mut block = 0_i64;
        let index_pos = self.binary_search_index(text)?;
        if index_pos >= 0 {
            let low = index_pos << Lucene54DocValuesFormat::BLOCK_INTERVAL_SHIFT;
            let high = ::std::cmp::min(
                self.num_index_values - 1,
                low + i64::from(Lucene54DocValuesFormat::BLOCK_INTERVAL_MASK),
            );
            block = ::std::cmp::max(low, self.binary_search_block(text, low, high)?);
        }
        // position before block then scan to term
        self.input.seek(self.addresses.get64(block)?)?;
        self.current_ord = (block << Lucene54DocValuesFormat::INTERVAL_SHIFT) - 1;

        while self.next()?.is_some() {
            let cmp = self.term[0..self.term_length as usize].as_ref().cmp(text);
            if cmp == Ordering::Equal {
                return Ok(SeekStatus::Found);
            } else if cmp == Ordering::Greater {
                return Ok(SeekStatus::NotFound);
            }
        }

        Ok(SeekStatus::End)
    }

    fn seek_exact_ord(&mut self, ord: i64) -> Result<()> {
        let block = ord.unsigned_shift(Lucene54DocValuesFormat::INTERVAL_SHIFT as usize);
        if block
            != self
                .current_ord
                .unsigned_shift(Lucene54DocValuesFormat::INTERVAL_SHIFT as usize)
        {
            // switch to different block
            self.input.seek(self.addresses.get64(block)?)?;
            self.read_header()?;
        }
        self.current_ord = ord;
        let offset = (ord & i64::from(Lucene54DocValuesFormat::INTERVAL_MASK)) as i32;
        if offset == 0 {
            self.read_first_term()
        } else {
            let pos = self.current_block_start + i64::from(self.offsets[(offset - 1) as usize]);
            self.input.seek(pos)?;
            self.read_term(offset)
        }
    }

    fn term(&self) -> Result<&[u8]> {
        Ok(&self.term[0..self.term_length as usize])
    }

    fn ord(&self) -> Result<i64> {
        Ok(self.current_ord)
    }

    fn doc_freq(&mut self) -> Result<i32> {
        bail!(UnsupportedOperation(
            "doc_freq unsupported for CompressedBinaryTermIterator".into()
        ))
    }

    fn total_term_freq(&mut self) -> Result<i64> {
        Ok(-1)
    }

    fn postings_with_flags(&mut self, _flags: u16) -> Result<Self::Postings> {
        bail!(UnsupportedOperation(
            "postings_with_flags unsupported for CompressedBinaryTermIterator".into()
        ))
    }
}

/// Implements a `TermIterator` wrapping a provided `SortedSetDocValues`
pub struct SortedSetDocValuesTermIterator<T: SortedSetDocValues + 'static> {
    values: *mut T,
    current_ord: i64,
    scratch: Vec<u8>,
}

impl<T: SortedSetDocValues + 'static> SortedSetDocValuesTermIterator<T> {
    /// Creates a new TermIterator over the provided values
    pub fn new(values: &T) -> SortedSetDocValuesTermIterator<T> {
        SortedSetDocValuesTermIterator {
            values: values as *const T as *mut T,
            current_ord: -1,
            scratch: vec![],
        }
    }

    #[inline]
    fn values(&mut self) -> &mut T {
        unsafe { &mut *self.values }
    }
}

impl<T: SortedSetDocValues + 'static> TermIterator for SortedSetDocValuesTermIterator<T> {
    type Postings = EmptyPostingIterator;
    type TermState = OrdTermState;
    fn next(&mut self) -> Result<Option<Vec<u8>>> {
        self.current_ord += 1;
        if self.current_ord >= self.values().get_value_count() as i64 {
            // FIXME: return Option<&[u8]> instead
            Ok(None)
        } else {
            let ord = self.current_ord;
            let bytes = self.values().lookup_ord(ord)?;
            self.scratch = bytes;
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
            if self.current_ord == self.values().get_value_count() as i64 {
                Ok(SeekStatus::End)
            } else {
                // TODO: hmm can we avoid this "extra" lookup?
                let ord = self.current_ord;
                let bytes = self.values().lookup_ord(ord)?;
                self.scratch = bytes;
                Ok(SeekStatus::NotFound)
            }
        }
    }

    fn seek_exact_ord(&mut self, ord: i64) -> Result<()> {
        assert!(ord >= 0 && ord < self.values().get_value_count() as i64);
        self.current_ord = ord;
        let bytes = self.values().lookup_ord(ord)?;
        self.scratch = bytes;
        Ok(())
    }

    fn seek_exact_state(&mut self, _text: &[u8], state: &Self::TermState) -> Result<()> {
        self.seek_exact_ord(state.ord())
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

/// Implements a `TermIterator` wrapping a provided `SortedDocValues`
pub struct SortedDocValuesTermIterator<T: SortedDocValues + 'static> {
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
