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
    CloneableNumericDocValues, CloneableNumericDocValuesProvider, NumericDocValues,
    SortedNumericDocValues, SortedNumericDocValuesProvider,
};
use core::util::BitsMut;
use error::Result;

pub(crate) struct SingletonSortedNumericDVProvider<
    P: CloneableNumericDocValuesProvider,
    B: BitsMut + Clone,
> {
    provider: P,
    bits: B,
}

impl<P, B> SingletonSortedNumericDVProvider<P, B>
where
    P: CloneableNumericDocValuesProvider,
    B: BitsMut + Clone,
{
    pub fn new(provider: P, bits: B) -> Self {
        Self { provider, bits }
    }
}

impl<P, B> SortedNumericDocValuesProvider for SingletonSortedNumericDVProvider<P, B>
where
    P: CloneableNumericDocValuesProvider,
    B: BitsMut + Clone + 'static,
{
    fn get(&self) -> Result<Box<dyn SortedNumericDocValues>> {
        let dv = self.provider.get()?;
        Ok(Box::new(SingletonSortedNumericDocValues::new(
            dv,
            self.bits.clone(),
        )))
    }
}

pub struct SingletonSortedNumericDocValues<DV: CloneableNumericDocValues, B: BitsMut> {
    numeric_doc_values_in: DV,
    docs_with_field: B,
    value: i64,
    count: usize,
}

impl<DV: CloneableNumericDocValues, B: BitsMut> SingletonSortedNumericDocValues<DV, B> {
    pub fn new(numeric_doc_values_in: DV, docs_with_field: B) -> Self {
        SingletonSortedNumericDocValues {
            numeric_doc_values_in,
            docs_with_field,
            value: 0,
            count: 0,
        }
    }

    pub fn get_numeric_doc_values_impl(&self) -> Box<dyn NumericDocValues> {
        self.numeric_doc_values_in.clone_box()
    }
}

impl<DV: CloneableNumericDocValues, B: BitsMut> SortedNumericDocValues
    for SingletonSortedNumericDocValues<DV, B>
{
    fn set_document(&mut self, doc: i32) -> Result<()> {
        let value = self.numeric_doc_values_in.get(doc)?;
        let id = self.docs_with_field.id();
        self.count = if id != 1 && value == 0 {
            let result = self.docs_with_field.get(doc as usize)?;
            if result {
                1
            } else {
                0
            }
        } else {
            1
        };
        self.value = value;
        Ok(())
    }

    fn value_at(&mut self, _index: usize) -> Result<i64> {
        Ok(self.value)
    }

    fn count(&self) -> usize {
        self.count
    }

    fn get_numeric_doc_values(&self) -> Option<Box<dyn NumericDocValues>> {
        Some(self.numeric_doc_values_in.clone_box())
    }
}
