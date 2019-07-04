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

use core::codec::{NumericLongValuesEnum, TailoredBoxedBinaryDocValuesEnum};
use core::index::{
    sorted_set_doc_values_term_iterator::SortedSetDocValuesTermIterator, DocValuesTermIterator,
    LongBinaryDocValues, NumericDocValues,
};
use core::util::{packed::MixinMonotonicLongValues, DocId, LongValues};

use error::Result;

use std::cmp::Ordering;
use std::sync::Arc;

/// When returned by next_ord() it means there are no more ordinals for the document.
pub const NO_MORE_ORDS: i64 = -1;

pub trait SortedSetDocValuesProvider: Send + Sync {
    fn get(&self) -> Result<Box<dyn SortedSetDocValues>>;
}

pub struct EmptySortedSetDocValuesProvider;

impl SortedSetDocValuesProvider for EmptySortedSetDocValuesProvider {
    fn get(&self) -> Result<Box<dyn SortedSetDocValues>> {
        Ok(Box::new(EmptySortedSetDocValues))
    }
}

pub trait SortedSetDocValues: Send + Sync {
    /// positions to the specified document
    fn set_document(&mut self, doc: DocId) -> Result<()>;

    /// Returns the next ordinal for the current document (previously
    /// set by `Self::set_document()`)
    fn next_ord(&mut self) -> Result<i64>;

    /// Retrieves the value for the specified ordinal.
    fn lookup_ord(&mut self, ord: i64) -> Result<Vec<u8>>;

    /// Returns the number of unique values.
    fn get_value_count(&self) -> usize;

    /// if `key` exists, returns its ordinal, else return `-insertion_point - 1`
    fn lookup_term(&mut self, key: &[u8]) -> Result<i64> {
        let mut low = 0_i64;
        let mut high = self.get_value_count() as i64 - 1;
        while low <= high {
            let mid = low + (high - low) / 2;
            let term = self.lookup_ord(mid)?;
            match term.as_slice().cmp(key) {
                Ordering::Less => {
                    low = mid + 1;
                }
                Ordering::Greater => {
                    high = mid - 1;
                }
                Ordering::Equal => {
                    return Ok(mid);
                }
            }
        }
        Ok(-(low + 1)) // key not found
    }

    /// return a `TermIterator` over the values
    fn term_iterator(&self) -> Result<DocValuesTermIterator>;
}

pub struct EmptySortedSetDocValues;

impl SortedSetDocValues for EmptySortedSetDocValues {
    fn set_document(&mut self, _doc: DocId) -> Result<()> {
        Ok(())
    }
    fn next_ord(&mut self) -> Result<i64> {
        Ok(NO_MORE_ORDS)
    }
    fn lookup_ord(&mut self, _ord: i64) -> Result<Vec<u8>> {
        Ok(Vec::with_capacity(0))
    }
    fn get_value_count(&self) -> usize {
        0
    }

    fn lookup_term(&mut self, _key: &[u8]) -> Result<i64> {
        Ok(-1)
    }

    fn term_iterator(&self) -> Result<DocValuesTermIterator> {
        Ok(DocValuesTermIterator::empty())
    }
}

/// Extension of `SortedSetDocValues` that supports random access to the ordinals of a document.
///
/// Operations via this API are independent of the iterator api next_ord()
/// and do not impact its state.
///
/// Codecs can optionally extend this API if they support constant-time access
/// to ordinals for the document.
pub trait RandomAccessOrds: SortedSetDocValues {
    fn ord_at(&mut self, index: i32) -> Result<i64>;
    fn cardinality(&self) -> i32;
}

pub(crate) struct AddressedRandomAccessOrds {
    binary: TailoredBoxedBinaryDocValuesEnum,
    ordinals: NumericLongValuesEnum,
    ord_index: MixinMonotonicLongValues,
    value_count: usize,

    start_offset: i64,
    current_offset: i64,
    end_offset: i64,
}

impl AddressedRandomAccessOrds {
    pub fn new(
        binary: TailoredBoxedBinaryDocValuesEnum,
        ordinals: NumericLongValuesEnum,
        ord_index: MixinMonotonicLongValues,
        value_count: usize,
    ) -> Self {
        Self {
            binary,
            ordinals,
            ord_index,
            value_count,
            start_offset: 0,
            current_offset: 0,
            end_offset: 0,
        }
    }
}

impl SortedSetDocValues for AddressedRandomAccessOrds {
    fn set_document(&mut self, doc: DocId) -> Result<()> {
        self.start_offset = self.ord_index.get_mut(doc)?;
        self.current_offset = self.start_offset;
        self.end_offset = self.ord_index.get_mut(doc + 1)?;
        Ok(())
    }

    fn next_ord(&mut self) -> Result<i64> {
        if self.current_offset == self.end_offset {
            Ok(NO_MORE_ORDS)
        } else {
            let ord = self.ordinals.get64_mut(self.current_offset)?;
            self.current_offset += 1;
            Ok(ord)
        }
    }

    fn lookup_ord(&mut self, ord: i64) -> Result<Vec<u8>> {
        self.binary.get64(ord)
    }

    fn get_value_count(&self) -> usize {
        self.value_count
    }

    fn lookup_term(&mut self, key: &[u8]) -> Result<i64> {
        match self.binary {
            TailoredBoxedBinaryDocValuesEnum::Compressed(ref mut compressed_binary) => {
                let value = compressed_binary.lookup_term(key)?;
                Ok(value as i64)
            }
            _ => {
                let mut low = 0_i64;
                let mut high = self.value_count as i64 - 1;
                while low <= high {
                    let mid = low + (high - low) / 2;
                    let term = self.lookup_ord(mid)?;
                    match term.as_slice().cmp(key) {
                        Ordering::Less => {
                            low = mid + 1;
                        }
                        Ordering::Greater => {
                            high = mid - 1;
                        }
                        Ordering::Equal => {
                            return Ok(mid);
                        }
                    }
                }
                Ok(-(low + 1)) // key not found
            }
        }
    }

    fn term_iterator(&self) -> Result<DocValuesTermIterator> {
        match self.binary {
            TailoredBoxedBinaryDocValuesEnum::Compressed(ref bin) => {
                let boxed = bin.get_term_iterator()?;
                Ok(DocValuesTermIterator::comp_bin(boxed))
            }
            _ => {
                let ti = SortedSetDocValuesTermIterator::new(self);
                Ok(DocValuesTermIterator::sorted_set_addr(ti))
            }
        }
    }
}

impl RandomAccessOrds for AddressedRandomAccessOrds {
    fn ord_at(&mut self, index: i32) -> Result<i64> {
        let position = self.start_offset + i64::from(index);
        self.ordinals.get64_mut(position)
    }

    fn cardinality(&self) -> i32 {
        (self.end_offset - self.start_offset) as i32
    }
}

impl SortedSetDocValuesProvider for AddressedRandomAccessOrds {
    fn get(&self) -> Result<Box<dyn SortedSetDocValues>> {
        let binary = self.binary.clone()?;
        let dv = Self {
            binary,
            ordinals: self.ordinals.clone(),
            ord_index: self.ord_index.clone(),
            value_count: self.value_count,
            start_offset: self.start_offset,
            current_offset: self.current_offset,
            end_offset: self.end_offset,
        };
        Ok(Box::new(dv))
    }
}

pub(crate) struct TabledRandomAccessOrds {
    binary: TailoredBoxedBinaryDocValuesEnum,
    ordinals: NumericLongValuesEnum,
    table: Arc<[i64]>,
    table_offsets: Arc<[i32]>,
    value_count: usize,

    start_offset: i32,
    current_offset: i32,
    end_offset: i32,
}

impl TabledRandomAccessOrds {
    pub fn new(
        binary: TailoredBoxedBinaryDocValuesEnum,
        ordinals: NumericLongValuesEnum,
        table: Vec<i64>,
        table_offsets: Vec<i32>,
        value_count: usize,
    ) -> Self {
        Self {
            binary,
            ordinals,
            table: Arc::from(table),
            table_offsets: Arc::from(table_offsets),
            value_count,
            start_offset: 0,
            current_offset: 0,
            end_offset: 0,
        }
    }
}
impl SortedSetDocValues for TabledRandomAccessOrds {
    fn set_document(&mut self, doc: DocId) -> Result<()> {
        let ord = self.ordinals.get_mut(doc)? as usize;
        self.start_offset = self.table_offsets[ord];
        self.current_offset = self.start_offset;
        self.end_offset = self.table_offsets[ord as usize + 1];
        Ok(())
    }

    fn next_ord(&mut self) -> Result<i64> {
        if self.current_offset == self.end_offset {
            Ok(NO_MORE_ORDS)
        } else {
            let ord = self.table[self.current_offset as usize];
            self.current_offset += 1;
            Ok(ord)
        }
    }

    fn lookup_ord(&mut self, ord: i64) -> Result<Vec<u8>> {
        self.binary.get64(ord)
    }

    fn get_value_count(&self) -> usize {
        self.value_count
    }

    fn lookup_term(&mut self, key: &[u8]) -> Result<i64> {
        match self.binary {
            TailoredBoxedBinaryDocValuesEnum::Compressed(ref mut binary) => binary.lookup_term(key),
            _ => {
                let mut low = 0_i64;
                let mut high = self.value_count as i64 - 1;
                while low <= high {
                    let mid = low + (high - low) / 2;
                    let term = self.lookup_ord(mid)?;
                    match term.as_slice().cmp(key) {
                        Ordering::Less => {
                            low = mid + 1;
                        }
                        Ordering::Greater => {
                            high = mid - 1;
                        }
                        Ordering::Equal => {
                            return Ok(mid);
                        }
                    }
                }
                Ok(-(low + 1)) // key not found
            }
        }
    }

    fn term_iterator(&self) -> Result<DocValuesTermIterator> {
        match self.binary {
            TailoredBoxedBinaryDocValuesEnum::Compressed(ref bin) => {
                bin.get_term_iterator().map(DocValuesTermIterator::comp_bin)
            }
            _ => {
                let ti = SortedSetDocValuesTermIterator::new(self);
                Ok(DocValuesTermIterator::sorted_set_table(ti))
            }
        }
    }
}

impl RandomAccessOrds for TabledRandomAccessOrds {
    fn ord_at(&mut self, index: i32) -> Result<i64> {
        let position = self.start_offset + index;
        let value = self.table[position as usize];
        Ok(value)
    }

    fn cardinality(&self) -> i32 {
        self.end_offset - self.start_offset
    }
}

impl SortedSetDocValuesProvider for TabledRandomAccessOrds {
    fn get(&self) -> Result<Box<dyn SortedSetDocValues>> {
        let dv = Self {
            binary: self.binary.clone()?,
            ordinals: self.ordinals.clone(),
            table: self.table.clone(),
            table_offsets: self.table_offsets.clone(),
            value_count: self.value_count,
            start_offset: self.start_offset,
            current_offset: self.current_offset,
            end_offset: self.end_offset,
        };
        Ok(Box::new(dv))
    }
}
