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

use core::codec::doc_values::{
    BinaryDocValues, NumericDocValues, SortedDocValues, SortedNumericDocValues, SortedSetDocValues,
};
use core::codec::Codec;
use core::doc::DocValuesType;
use core::index::reader::LeafReaderContext;
use core::search::DocIterator;
use core::search::NO_MORE_DOCS;
use core::util::DocId;
use error::Result;

pub struct DocValuesIterator {
    cost: i32,
    doc: DocId,
    doc_values: DocValuesEnum,
}

impl DocValuesIterator {
    pub fn new<C: Codec>(field: &str, cost: i32, leaf_reader: &LeafReaderContext<'_, C>) -> Self {
        if let Some(field_info) = leaf_reader.reader.field_info(field) {
            let mut doc_values = DocValuesEnum::None;
            match field_info.doc_values_type {
                DocValuesType::Binary => {
                    if let Ok(values) = leaf_reader.reader.get_binary_doc_values(field) {
                        doc_values = DocValuesEnum::Binary(values);
                    }
                }
                DocValuesType::Numeric => {
                    if let Ok(values) = leaf_reader.reader.get_numeric_doc_values(field) {
                        doc_values = DocValuesEnum::Numeric(values);
                    }
                }
                DocValuesType::Sorted => {
                    if let Ok(values) = leaf_reader.reader.get_sorted_doc_values(field) {
                        doc_values = DocValuesEnum::Sorted(values);
                    }
                }
                DocValuesType::SortedSet => {
                    if let Ok(values) = leaf_reader.reader.get_sorted_set_doc_values(field) {
                        doc_values = DocValuesEnum::SortedSet(values);
                    }
                }
                DocValuesType::SortedNumeric => {
                    if let Ok(values) = leaf_reader.reader.get_sorted_numeric_doc_values(field) {
                        doc_values = DocValuesEnum::SortedNumeric(values);
                    }
                }
                _ => {}
            }

            return Self {
                cost,
                doc: -1,
                doc_values,
            };
        }

        Self {
            cost: 0,
            doc: -1,
            doc_values: DocValuesEnum::None,
        }
    }

    fn exists(&mut self, doc_id: DocId) -> Result<bool> {
        match self.doc_values {
            DocValuesEnum::Binary(ref mut binary) => {
                if let Ok(v) = binary.get(doc_id) {
                    return Ok(v.len() > 0);
                }
            }
            DocValuesEnum::Numeric(ref mut numeric) => {
                if let Ok(_) = numeric.get(doc_id) {
                    return Ok(true);
                }
            }
            DocValuesEnum::Sorted(ref mut sorted) => {
                if let Ok(o) = sorted.get_ord(doc_id) {
                    return Ok(o > -1);
                }
            }
            DocValuesEnum::SortedNumeric(ref mut sorted_numeric) => {
                if let Ok(()) = sorted_numeric.set_document(doc_id) {
                    return Ok(sorted_numeric.count() > 0);
                }
            }
            DocValuesEnum::SortedSet(ref mut sorted_set) => {
                if let Ok(()) = sorted_set.set_document(doc_id) {
                    if let Ok(o) = sorted_set.next_ord() {
                        return Ok(o > -1);
                    }
                }
            }
            _ => {}
        }

        Ok(false)
    }
}

enum DocValuesEnum {
    Binary(Box<dyn BinaryDocValues>),
    Numeric(Box<dyn NumericDocValues>),
    Sorted(Box<dyn SortedDocValues>),
    SortedSet(Box<dyn SortedSetDocValues>),
    SortedNumeric(Box<dyn SortedNumericDocValues>),
    None,
}

impl DocIterator for DocValuesIterator {
    fn doc_id(&self) -> DocId {
        self.doc
    }

    fn next(&mut self) -> Result<DocId> {
        let next = self.doc + 1;
        self.advance(next)
    }

    fn advance(&mut self, target: DocId) -> Result<DocId> {
        let mut target = target;

        loop {
            if target >= self.cost() as i32 {
                self.doc = NO_MORE_DOCS;
                break;
            }

            if self.exists(target)? {
                self.doc = target;
                break;
            }

            target += 1;
        }

        Ok(self.doc)
    }

    fn cost(&self) -> usize {
        self.cost as usize
    }
}
