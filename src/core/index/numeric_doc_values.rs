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

use core::util::{BitsContext, DocId};
use error::Result;

use std::sync::Arc;

/// information that make get doc values more faster
pub type NumericDocValuesContext = BitsContext;

/// A per-document numeric value.
pub trait NumericDocValues: Send + Sync {
    fn get_with_ctx(
        &self,
        ctx: NumericDocValuesContext,
        doc_id: DocId,
    ) -> Result<(i64, NumericDocValuesContext)>;

    fn get(&self, doc_id: DocId) -> Result<i64> {
        self.get_with_ctx(None, doc_id).map(|x| x.0)
    }
}

pub type NumericDocValuesRef = Arc<dyn NumericDocValues>;

/// a `NumericDocValues` that always produce 0
#[derive(Default)]
pub struct EmptyNumericDocValues;

impl NumericDocValues for EmptyNumericDocValues {
    fn get_with_ctx(
        &self,
        _ctx: NumericDocValuesContext,
        _doc_id: DocId,
    ) -> Result<(i64, NumericDocValuesContext)> {
        Ok((0, None))
    }
}
