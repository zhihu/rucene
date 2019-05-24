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

use core::codec::CompressedBinaryTermIterator;
use core::index::sorted_doc_values::TailoredSortedDocValues;
use core::index::sorted_doc_values_term_iterator::SortedDocValuesTermIterator;
use core::index::sorted_set_doc_values::{AddressedRandomAccessOrds, TabledRandomAccessOrds};
use core::index::sorted_set_doc_values_term_iterator::SortedSetDocValuesTermIterator;
use core::index::{EmptyTermIterator, OrdTermState, SeekStatus, TermIterator};
use core::search::posting_iterator::EmptyPostingIterator;

use error::Result;

pub struct DocValuesTermIterator(DocValuesTermIteratorEnum);

impl DocValuesTermIterator {
    pub fn comp_bin(d: CompressedBinaryTermIterator) -> Self {
        DocValuesTermIterator(DocValuesTermIteratorEnum::CompBin(d))
    }
    pub fn sorted(d: SortedDocValuesTermIterator<TailoredSortedDocValues>) -> Self {
        DocValuesTermIterator(DocValuesTermIteratorEnum::Sorted(d))
    }
    pub fn sorted_set_addr(d: SortedSetDocValuesTermIterator<AddressedRandomAccessOrds>) -> Self {
        DocValuesTermIterator(DocValuesTermIteratorEnum::SortedSetAddr(d))
    }
    pub fn sorted_set_table(d: SortedSetDocValuesTermIterator<TabledRandomAccessOrds>) -> Self {
        DocValuesTermIterator(DocValuesTermIteratorEnum::SortedSetTable(d))
    }
    pub fn empty() -> Self {
        DocValuesTermIterator(DocValuesTermIteratorEnum::Empty(EmptyTermIterator {}))
    }
}

enum DocValuesTermIteratorEnum {
    CompBin(CompressedBinaryTermIterator),
    Sorted(SortedDocValuesTermIterator<TailoredSortedDocValues>),
    SortedSetAddr(SortedSetDocValuesTermIterator<AddressedRandomAccessOrds>),
    SortedSetTable(SortedSetDocValuesTermIterator<TabledRandomAccessOrds>),
    Empty(EmptyTermIterator),
}

impl TermIterator for DocValuesTermIterator {
    type Postings = EmptyPostingIterator;
    type TermState = OrdTermState;
    fn next(&mut self) -> Result<Option<Vec<u8>>> {
        match &mut self.0 {
            DocValuesTermIteratorEnum::CompBin(t) => t.next(),
            DocValuesTermIteratorEnum::Sorted(t) => t.next(),
            DocValuesTermIteratorEnum::SortedSetAddr(t) => t.next(),
            DocValuesTermIteratorEnum::SortedSetTable(t) => t.next(),
            DocValuesTermIteratorEnum::Empty(t) => t.next(),
        }
    }

    fn seek_exact(&mut self, text: &[u8]) -> Result<bool> {
        match &mut self.0 {
            DocValuesTermIteratorEnum::CompBin(t) => t.seek_exact(text),
            DocValuesTermIteratorEnum::Sorted(t) => t.seek_exact(text),
            DocValuesTermIteratorEnum::SortedSetAddr(t) => t.seek_exact(text),
            DocValuesTermIteratorEnum::SortedSetTable(t) => t.seek_exact(text),
            DocValuesTermIteratorEnum::Empty(t) => t.seek_exact(text),
        }
    }

    fn seek_ceil(&mut self, text: &[u8]) -> Result<SeekStatus> {
        match &mut self.0 {
            DocValuesTermIteratorEnum::CompBin(t) => t.seek_ceil(text),
            DocValuesTermIteratorEnum::Sorted(t) => t.seek_ceil(text),
            DocValuesTermIteratorEnum::SortedSetAddr(t) => t.seek_ceil(text),
            DocValuesTermIteratorEnum::SortedSetTable(t) => t.seek_ceil(text),
            DocValuesTermIteratorEnum::Empty(t) => t.seek_ceil(text),
        }
    }

    fn seek_exact_ord(&mut self, ord: i64) -> Result<()> {
        match &mut self.0 {
            DocValuesTermIteratorEnum::CompBin(t) => t.seek_exact_ord(ord),
            DocValuesTermIteratorEnum::Sorted(t) => t.seek_exact_ord(ord),
            DocValuesTermIteratorEnum::SortedSetAddr(t) => t.seek_exact_ord(ord),
            DocValuesTermIteratorEnum::SortedSetTable(t) => t.seek_exact_ord(ord),
            DocValuesTermIteratorEnum::Empty(t) => t.seek_exact_ord(ord),
        }
    }

    fn seek_exact_state(&mut self, text: &[u8], state: &Self::TermState) -> Result<()> {
        match &mut self.0 {
            DocValuesTermIteratorEnum::CompBin(_) => unreachable!(),
            DocValuesTermIteratorEnum::Sorted(t) => t.seek_exact_state(text, state),
            DocValuesTermIteratorEnum::SortedSetAddr(t) => t.seek_exact_state(text, state),
            DocValuesTermIteratorEnum::SortedSetTable(t) => t.seek_exact_state(text, state),
            DocValuesTermIteratorEnum::Empty(_) => unreachable!(),
        }
    }

    fn term(&self) -> Result<&[u8]> {
        match &self.0 {
            DocValuesTermIteratorEnum::CompBin(t) => t.term(),
            DocValuesTermIteratorEnum::Sorted(t) => t.term(),
            DocValuesTermIteratorEnum::SortedSetAddr(t) => t.term(),
            DocValuesTermIteratorEnum::SortedSetTable(t) => t.term(),
            DocValuesTermIteratorEnum::Empty(t) => t.term(),
        }
    }

    fn ord(&self) -> Result<i64> {
        match &self.0 {
            DocValuesTermIteratorEnum::CompBin(t) => t.ord(),
            DocValuesTermIteratorEnum::Sorted(t) => t.ord(),
            DocValuesTermIteratorEnum::SortedSetAddr(t) => t.ord(),
            DocValuesTermIteratorEnum::SortedSetTable(t) => t.ord(),
            DocValuesTermIteratorEnum::Empty(t) => t.ord(),
        }
    }

    fn doc_freq(&mut self) -> Result<i32> {
        match &mut self.0 {
            DocValuesTermIteratorEnum::CompBin(t) => t.doc_freq(),
            DocValuesTermIteratorEnum::Sorted(t) => t.doc_freq(),
            DocValuesTermIteratorEnum::SortedSetAddr(t) => t.doc_freq(),
            DocValuesTermIteratorEnum::SortedSetTable(t) => t.doc_freq(),
            DocValuesTermIteratorEnum::Empty(t) => t.doc_freq(),
        }
    }

    fn total_term_freq(&mut self) -> Result<i64> {
        match &mut self.0 {
            DocValuesTermIteratorEnum::CompBin(t) => t.total_term_freq(),
            DocValuesTermIteratorEnum::Sorted(t) => t.total_term_freq(),
            DocValuesTermIteratorEnum::SortedSetAddr(t) => t.total_term_freq(),
            DocValuesTermIteratorEnum::SortedSetTable(t) => t.total_term_freq(),
            DocValuesTermIteratorEnum::Empty(t) => t.total_term_freq(),
        }
    }

    fn postings(&mut self) -> Result<Self::Postings> {
        match &mut self.0 {
            DocValuesTermIteratorEnum::CompBin(t) => t.postings(),
            DocValuesTermIteratorEnum::Sorted(t) => t.postings(),
            DocValuesTermIteratorEnum::SortedSetAddr(t) => t.postings(),
            DocValuesTermIteratorEnum::SortedSetTable(t) => t.postings(),
            DocValuesTermIteratorEnum::Empty(t) => t.postings(),
        }
    }

    fn postings_with_flags(&mut self, flags: u16) -> Result<Self::Postings> {
        match &mut self.0 {
            DocValuesTermIteratorEnum::CompBin(t) => t.postings_with_flags(flags),
            DocValuesTermIteratorEnum::Sorted(t) => t.postings_with_flags(flags),
            DocValuesTermIteratorEnum::SortedSetAddr(t) => t.postings_with_flags(flags),
            DocValuesTermIteratorEnum::SortedSetTable(t) => t.postings_with_flags(flags),
            DocValuesTermIteratorEnum::Empty(t) => t.postings_with_flags(flags),
        }
    }

    fn term_state(&mut self) -> Result<Self::TermState> {
        match &mut self.0 {
            DocValuesTermIteratorEnum::CompBin(_) => unimplemented!(),
            DocValuesTermIteratorEnum::Sorted(t) => t.term_state(),
            DocValuesTermIteratorEnum::SortedSetAddr(t) => t.term_state(),
            DocValuesTermIteratorEnum::SortedSetTable(t) => t.term_state(),
            DocValuesTermIteratorEnum::Empty(_) => unimplemented!(),
        }
    }

    fn is_empty(&self) -> bool {
        match &self.0 {
            DocValuesTermIteratorEnum::CompBin(t) => t.is_empty(),
            DocValuesTermIteratorEnum::Sorted(t) => t.is_empty(),
            DocValuesTermIteratorEnum::SortedSetAddr(t) => t.is_empty(),
            DocValuesTermIteratorEnum::SortedSetTable(t) => t.is_empty(),
            DocValuesTermIteratorEnum::Empty(t) => t.is_empty(),
        }
    }
}
