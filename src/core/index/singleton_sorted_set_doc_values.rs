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

use core::index::{
    DocValuesTermIterator, RandomAccessOrds, SortedDocValues, SortedDocValuesProvider,
    SortedSetDocValues, SortedSetDocValuesProvider, NO_MORE_ORDS,
};
use core::util::{bit_util::UnsignedShift, DocId};
use error::Result;

pub(crate) struct SingletonSortedSetDVProvider<T: SortedDocValuesProvider> {
    provider: T,
}

impl<T: SortedDocValuesProvider> SingletonSortedSetDVProvider<T> {
    pub fn new(provider: T) -> Self {
        Self { provider }
    }
}

impl<T: SortedDocValuesProvider> SortedSetDocValuesProvider for SingletonSortedSetDVProvider<T> {
    fn get(&self) -> Result<Box<dyn SortedSetDocValues>> {
        let sorted = self.provider.get()?;
        Ok(Box::new(SingletonSortedSetDocValues::new(sorted)))
    }
}

/// Exposes multi-valued view over a single-valued instance.
///
/// This can be used if you want to have one multi-valued implementation
/// that works for single or multi-valued types.
pub struct SingletonSortedSetDocValues<T: SortedDocValues> {
    dv_in: T,
    current_ord: i64,
    ord: i64,
}

impl<T: SortedDocValues> SingletonSortedSetDocValues<T> {
    pub fn new(dv_in: T) -> Self {
        Self {
            dv_in,
            current_ord: 0,
            ord: 0,
        }
    }
    pub fn get_sorted_doc_values(&self) -> &T {
        &self.dv_in
    }
}

impl<T: SortedDocValues> RandomAccessOrds for SingletonSortedSetDocValues<T> {
    fn ord_at(&mut self, _index: i32) -> Result<i64> {
        Ok(self.ord)
    }

    fn cardinality(&self) -> i32 {
        self.ord.unsigned_shift(63) as i32 ^ 1
    }
}

impl<T: SortedDocValues> SortedSetDocValues for SingletonSortedSetDocValues<T> {
    fn set_document(&mut self, doc: DocId) -> Result<()> {
        let v = i64::from(self.dv_in.get_ord(doc)?);
        self.ord = v;
        self.current_ord = v;
        Ok(())
    }

    fn next_ord(&mut self) -> Result<i64> {
        let v = self.current_ord;
        self.current_ord = NO_MORE_ORDS;
        Ok(v)
    }

    fn lookup_ord(&mut self, ord: i64) -> Result<Vec<u8>> {
        // cast is ok: single-valued cannot exceed i32::MAX
        self.dv_in.lookup_ord(ord as i32)
    }

    fn get_value_count(&self) -> usize {
        self.dv_in.value_count()
    }

    fn lookup_term(&mut self, key: &[u8]) -> Result<i64> {
        self.dv_in.lookup_term(key).map(i64::from)
    }

    fn term_iterator(&self) -> Result<DocValuesTermIterator> {
        self.dv_in.term_iterator()
    }
}
