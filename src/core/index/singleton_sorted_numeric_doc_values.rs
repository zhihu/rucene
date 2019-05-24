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
    NumericDocValues, NumericDocValuesRef, SortedNumericDocValues, SortedNumericDocValuesContext,
};
use core::util::{BitsRef, DocId};
use error::Result;

use std::sync::Arc;

pub struct SingletonSortedNumericDocValues {
    numeric_doc_values_in: NumericDocValuesRef,
    docs_with_field: BitsRef,
}

impl SingletonSortedNumericDocValues {
    pub fn new(numeric_doc_values_in: Box<dyn NumericDocValues>, docs_with_field: BitsRef) -> Self {
        SingletonSortedNumericDocValues {
            numeric_doc_values_in: Arc::from(numeric_doc_values_in),
            docs_with_field,
        }
    }

    pub fn get_numeric_doc_values_impl(&self) -> NumericDocValuesRef {
        Arc::clone(&self.numeric_doc_values_in)
    }
}

impl SortedNumericDocValues for SingletonSortedNumericDocValues {
    fn set_document(
        &self,
        ctx: Option<SortedNumericDocValuesContext>,
        doc: DocId,
    ) -> Result<SortedNumericDocValuesContext> {
        let value = self.numeric_doc_values_in.get(doc)?;
        let id = self.docs_with_field.id();
        let bc = ctx.and_then(|c| c.2);
        let (bc, count) = if id != 1 && value == 0 {
            let result = self.docs_with_field.get_with_ctx(bc, doc as usize)?;
            (result.1, if result.0 { 1 } else { 0 })
        } else {
            (bc, 1)
        };
        Ok((value, i64::from(count), bc))
    }

    fn value_at(&self, ctx: &SortedNumericDocValuesContext, _index: usize) -> Result<i64> {
        Ok(ctx.0)
    }

    fn count(&self, ctx: &SortedNumericDocValuesContext) -> usize {
        ctx.1 as usize
    }

    fn get_numeric_doc_values(&self) -> Option<NumericDocValuesRef> {
        Some(self.get_numeric_doc_values_impl())
    }
}
