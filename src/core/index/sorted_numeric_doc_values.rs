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

use core::index::NumericDocValuesRef;
use core::util::{BitsContext, DocId, LongValues};
use error::Result;

use std::sync::Arc;

pub type SortedNumericDocValuesContext = (i64, i64, BitsContext);

pub trait SortedNumericDocValues: Send + Sync {
    /// positions to the specified document
    fn set_document(
        &self,
        ctx: Option<SortedNumericDocValuesContext>,
        doc: DocId,
    ) -> Result<SortedNumericDocValuesContext>;
    /// Retrieve the value for the current document at the specified index.
    /// An index ranges from 0 to count() - 1.
    fn value_at(&self, ctx: &SortedNumericDocValuesContext, index: usize) -> Result<i64>;
    /// Retrieves the count of values for the current document
    /// There may be zero if a document has no values
    fn count(&self, ctx: &SortedNumericDocValuesContext) -> usize;
    fn get_numeric_doc_values(&self) -> Option<NumericDocValuesRef> {
        None
    }
}

pub type SortedNumericDocValuesRef = Arc<dyn SortedNumericDocValues>;

pub struct EmptySortedNumericDocValues;

impl SortedNumericDocValues for EmptySortedNumericDocValues {
    fn set_document(
        &self,
        _ctx: Option<SortedNumericDocValuesContext>,
        _doc: DocId,
    ) -> Result<SortedNumericDocValuesContext> {
        Ok((0, 0, None))
    }

    fn value_at(&self, _ctx: &SortedNumericDocValuesContext, _index: usize) -> Result<i64> {
        unreachable!()
    }

    fn count(&self, _ctx: &SortedNumericDocValuesContext) -> usize {
        0
    }
}

pub struct AddressedSortedNumericDocValues<T: LongValues> {
    values: Box<dyn LongValues>,
    ord_index: T,
}

impl<T: LongValues> AddressedSortedNumericDocValues<T> {
    pub fn new(values: Box<dyn LongValues>, ord_index: T) -> Self {
        AddressedSortedNumericDocValues { values, ord_index }
    }
}

impl<T: LongValues> SortedNumericDocValues for AddressedSortedNumericDocValues<T> {
    fn set_document(
        &self,
        ctx: Option<SortedNumericDocValuesContext>,
        doc: DocId,
    ) -> Result<SortedNumericDocValuesContext> {
        let bc = ctx.and_then(|c| c.2);
        Ok((self.ord_index.get(doc)?, self.ord_index.get(doc + 1)?, bc))
    }

    fn value_at(&self, ctx: &SortedNumericDocValuesContext, index: usize) -> Result<i64> {
        let offset = ctx.0 + index as i64;
        self.values.get64(offset)
    }

    fn count(&self, ctx: &SortedNumericDocValuesContext) -> usize {
        (ctx.1 - ctx.0) as usize
    }
}

pub struct TabledSortedNumericDocValues {
    ordinals: Box<dyn LongValues>,
    table: Vec<i64>,
    offsets: Vec<i32>,
}

impl TabledSortedNumericDocValues {
    pub fn new(ordinals: Box<dyn LongValues>, table: &[i64], offsets: &[i32]) -> Self {
        TabledSortedNumericDocValues {
            ordinals,
            table: table.to_vec(),
            offsets: offsets.to_vec(),
        }
    }
}

impl SortedNumericDocValues for TabledSortedNumericDocValues {
    fn set_document(
        &self,
        ctx: Option<SortedNumericDocValuesContext>,
        doc: DocId,
    ) -> Result<SortedNumericDocValuesContext> {
        let ord = self.ordinals.get(doc)? as usize;
        let bc = ctx.and_then(|c| c.2);
        Ok((
            i64::from(self.offsets[ord]),
            i64::from(self.offsets[ord + 1]),
            bc,
        ))
    }

    fn value_at(&self, ctx: &SortedNumericDocValuesContext, index: usize) -> Result<i64> {
        let offset = ctx.0 + index as i64;
        Ok(self.table[offset as usize])
    }

    fn count(&self, ctx: &SortedNumericDocValuesContext) -> usize {
        (ctx.1 - ctx.0) as usize
    }
}
