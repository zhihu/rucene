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

use core::util::DocId;
use error::Result;

pub trait NumericDocValuesProvider: Send + Sync {
    fn get(&self) -> Result<Box<dyn NumericDocValues>>;
}

/// A per-document numeric value.
pub trait NumericDocValues: Send + Sync {
    fn get(&self, doc_id: DocId) -> Result<i64>;

    fn get_mut(&mut self, doc_id: DocId) -> Result<i64> {
        self.get(doc_id)
    }
}

impl<T: NumericDocValues + ?Sized> NumericDocValues for Box<T> {
    fn get(&self, doc_id: DocId) -> Result<i64> {
        (**self).get(doc_id)
    }

    fn get_mut(&mut self, doc_id: DocId) -> Result<i64> {
        (**self).get_mut(doc_id)
    }
}

pub trait CloneableNumericDocValues: NumericDocValues {
    fn clone_box(&self) -> Box<dyn NumericDocValues>;
}

impl<T: CloneableNumericDocValues + ?Sized> CloneableNumericDocValues for Box<T> {
    fn clone_box(&self) -> Box<dyn NumericDocValues> {
        (**self).clone_box()
    }
}

pub(crate) trait CloneableNumericDocValuesProvider: Send + Sync {
    fn get(&self) -> Result<Box<dyn CloneableNumericDocValues>>;
}

impl<T: CloneableNumericDocValues + Clone + 'static> CloneableNumericDocValuesProvider for T {
    fn get(&self) -> Result<Box<dyn CloneableNumericDocValues>> {
        Ok(Box::new(self.clone()))
    }
}

/// a `NumericDocValues` that always produce 0
#[derive(Default)]
pub struct EmptyNumericDocValues;

impl NumericDocValues for EmptyNumericDocValues {
    fn get(&self, _doc_id: DocId) -> Result<i64> {
        Ok(0)
    }
}
