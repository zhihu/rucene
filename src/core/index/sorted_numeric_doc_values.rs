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

use core::codec::NumericLongValuesEnum;
use core::index::NumericDocValues;
use core::util::{DocId, LongValues};
use error::Result;

pub trait SortedNumericDocValuesProvider: Send + Sync {
    fn get(&self) -> Result<Box<dyn SortedNumericDocValues>>;
}

pub trait SortedNumericDocValues: Send + Sync {
    /// positions to the specified document
    fn set_document(&mut self, doc: DocId) -> Result<()>;
    /// Retrieve the value for the current document at the specified index.
    /// An index ranges from 0 to count() - 1.
    fn value_at(&mut self, index: usize) -> Result<i64>;

    /// value count for current doc
    fn count(&self) -> usize;

    fn get_numeric_doc_values(&self) -> Option<Box<dyn NumericDocValues>> {
        None
    }
}

pub struct EmptySortedNumericDocValues;

impl SortedNumericDocValues for EmptySortedNumericDocValues {
    fn set_document(&mut self, _doc: DocId) -> Result<()> {
        Ok(())
    }

    fn value_at(&mut self, _index: usize) -> Result<i64> {
        unreachable!()
    }

    fn count(&self) -> usize {
        0
    }
}

#[derive(Clone)]
pub(crate) struct AddressedSortedNumericDocValues<T: LongValues + Clone> {
    values: NumericLongValuesEnum,
    ord_index: T,
    start_offset: i64,
    end_offset: i64,
}

impl<T: LongValues + Clone> AddressedSortedNumericDocValues<T> {
    pub fn new(values: NumericLongValuesEnum, ord_index: T) -> Self {
        Self {
            values,
            ord_index,
            start_offset: 0,
            end_offset: 0,
        }
    }
}

impl<T: LongValues + Clone> SortedNumericDocValues for AddressedSortedNumericDocValues<T> {
    fn set_document(&mut self, doc: i32) -> Result<()> {
        self.start_offset = self.ord_index.get_mut(doc)?;
        self.end_offset = self.ord_index.get_mut(doc + 1)?;
        Ok(())
    }

    fn value_at(&mut self, index: usize) -> Result<i64> {
        let idx = self.start_offset + index as i64;
        self.values.get64(idx)
    }

    fn count(&self) -> usize {
        (self.end_offset - self.start_offset) as usize
    }
}

impl<T: LongValues + Clone + 'static> SortedNumericDocValuesProvider
    for AddressedSortedNumericDocValues<T>
{
    fn get(&self) -> Result<Box<dyn SortedNumericDocValues>> {
        Ok(Box::new(self.clone()))
    }
}

#[derive(Clone)]
pub(crate) struct TabledSortedNumericDocValues {
    ordinals: NumericLongValuesEnum,
    table: Vec<i64>,
    offsets: Vec<i32>,
    start_offset: i32,
    end_offset: i32,
}

impl TabledSortedNumericDocValues {
    pub fn new(ordinals: NumericLongValuesEnum, table: &[i64], offsets: &[i32]) -> Self {
        TabledSortedNumericDocValues {
            ordinals,
            table: table.to_vec(),
            offsets: offsets.to_vec(),
            start_offset: 0,
            end_offset: 0,
        }
    }
}

impl SortedNumericDocValues for TabledSortedNumericDocValues {
    fn set_document(&mut self, doc: DocId) -> Result<()> {
        let ord = self.ordinals.get_mut(doc)? as usize;
        self.start_offset = self.offsets[ord];
        self.end_offset = self.offsets[ord + 1];

        Ok(())
    }

    fn value_at(&mut self, index: usize) -> Result<i64> {
        let offset = self.start_offset as usize + index;
        Ok(self.table[offset])
    }

    fn count(&self) -> usize {
        (self.end_offset - self.start_offset) as usize
    }
}

impl SortedNumericDocValuesProvider for TabledSortedNumericDocValues {
    fn get(&self) -> Result<Box<dyn SortedNumericDocValues>> {
        Ok(Box::new(self.clone()))
    }
}
