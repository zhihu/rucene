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
    DocValuesTermIterator, RandomAccessOrds, SortedDocValues, SortedSetDocValues,
    SortedSetDocValuesContext, NO_MORE_ORDS,
};
use core::util::DocId;
use error::Result;

pub struct SingletonSortedSetDocValues<T: SortedDocValues> {
    dv_in: T,
}

impl<T: SortedDocValues> SingletonSortedSetDocValues<T> {
    pub fn new(dv_in: T) -> Self {
        SingletonSortedSetDocValues { dv_in }
    }
    pub fn get_sorted_doc_values(&self) -> &T {
        &self.dv_in
    }
}

impl<T: SortedDocValues> RandomAccessOrds for SingletonSortedSetDocValues<T> {
    fn ord_at(&self, ctx: &SortedSetDocValuesContext, _index: i32) -> Result<i64> {
        Ok(ctx.0)
    }

    fn cardinality(&self, ctx: &SortedSetDocValuesContext) -> i32 {
        ((ctx.0 as u64 >> 63) as i32) ^ 1
    }
}

impl<T: SortedDocValues> SortedSetDocValues for SingletonSortedSetDocValues<T> {
    fn set_document(&self, doc: DocId) -> Result<SortedSetDocValuesContext> {
        let v = i64::from(self.dv_in.get_ord(doc)?);
        Ok((v, v, v))
    }

    fn next_ord(&self, ctx: &mut SortedSetDocValuesContext) -> Result<i64> {
        let v = ctx.0;
        ctx.0 = NO_MORE_ORDS;
        Ok(v)
    }

    fn lookup_ord(&self, ord: i64) -> Result<Vec<u8>> {
        // cast is ok: single-valued cannot exceed i32::MAX
        self.dv_in.lookup_ord(ord as i32)
    }

    fn get_value_count(&self) -> usize {
        self.dv_in.get_value_count()
    }

    fn lookup_term(&self, key: &[u8]) -> Result<i64> {
        let val = self.dv_in.lookup_term(key)?;
        Ok(i64::from(val))
    }

    fn term_iterator(&self) -> Result<DocValuesTermIterator> {
        self.dv_in.term_iterator()
    }
}
