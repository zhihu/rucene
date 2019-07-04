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
use core::index::sorted_doc_values_term_iterator::SortedDocValuesTermIterator;
use core::index::{BinaryDocValues, DocValuesTermIterator, NumericDocValues};
use core::util::DocId;
use error::Result;

use std::cmp::Ordering;
use std::ops::Deref;

pub trait SortedDocValuesProvider: Send + Sync {
    fn get(&self) -> Result<Box<dyn SortedDocValues>>;
}

impl<T: Deref<Target = SortedDocValuesProvider> + Send + Sync> SortedDocValuesProvider for T {
    fn get(&self) -> Result<Box<dyn SortedDocValues>> {
        (**self).get()
    }
}

pub trait SortedDocValues: BinaryDocValues {
    fn get_ord(&mut self, doc_id: DocId) -> Result<i32>;

    fn lookup_ord(&mut self, ord: i32) -> Result<Vec<u8>>;

    fn value_count(&self) -> usize;

    /// if key exists, return its ordinal, else return
    /// - insertion_point - 1.
    fn lookup_term(&mut self, key: &[u8]) -> Result<i32> {
        let mut low = 0;
        let mut high = self.value_count() as i32 - 1;
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

    fn term_iterator(&self) -> Result<DocValuesTermIterator>;
}

impl<T: SortedDocValues + ?Sized> SortedDocValues for Box<T> {
    fn get_ord(&mut self, doc_id: DocId) -> Result<i32> {
        (**self).get_ord(doc_id)
    }

    fn lookup_ord(&mut self, ord: i32) -> Result<Vec<u8>> {
        (**self).lookup_ord(ord)
    }

    fn value_count(&self) -> usize {
        (**self).value_count()
    }

    fn term_iterator(&self) -> Result<DocValuesTermIterator> {
        (**self).term_iterator()
    }
}

pub struct EmptySortedDocValues;

impl SortedDocValues for EmptySortedDocValues {
    fn get_ord(&mut self, _doc_id: DocId) -> Result<i32> {
        Ok(-1)
    }

    fn lookup_ord(&mut self, _ord: i32) -> Result<Vec<u8>> {
        Ok(Vec::with_capacity(0))
    }

    fn value_count(&self) -> usize {
        0
    }

    fn term_iterator(&self) -> Result<DocValuesTermIterator> {
        Ok(DocValuesTermIterator::empty())
    }
}

impl BinaryDocValues for EmptySortedDocValues {
    fn get(&mut self, _doc_id: DocId) -> Result<Vec<u8>> {
        Ok(Vec::with_capacity(0))
    }
}

pub(crate) struct TailoredSortedDocValues {
    ordinals: NumericLongValuesEnum,
    binary: TailoredBoxedBinaryDocValuesEnum,
    value_count: usize,
}

impl TailoredSortedDocValues {
    pub fn new(
        ordinals: NumericLongValuesEnum,
        binary: TailoredBoxedBinaryDocValuesEnum,
        value_count: usize,
    ) -> Self {
        Self {
            ordinals,
            binary,
            value_count,
        }
    }
}

impl SortedDocValuesProvider for TailoredSortedDocValues {
    fn get(&self) -> Result<Box<dyn SortedDocValues>> {
        let doc_values = Self {
            ordinals: self.ordinals.clone(),
            binary: self.binary.clone()?,
            value_count: self.value_count,
        };
        Ok(Box::new(doc_values))
    }
}

impl SortedDocValues for TailoredSortedDocValues {
    fn get_ord(&mut self, doc_id: DocId) -> Result<i32> {
        self.ordinals.get_mut(doc_id).map(|v| v as i32)
    }

    fn lookup_ord(&mut self, ord: i32) -> Result<Vec<u8>> {
        self.binary.get(ord)
    }

    fn value_count(&self) -> usize {
        self.value_count
    }

    fn lookup_term(&mut self, key: &[u8]) -> Result<i32> {
        match self.binary {
            TailoredBoxedBinaryDocValuesEnum::Compressed(ref mut binary) => {
                let val = binary.lookup_term(key)? as i32;
                Ok(val)
            }
            _ => {
                // TODO: Copy from SortedDocValues#lookup_term
                let mut low = 0;
                let mut high = self.value_count as i32 - 1;
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
                let ti = SortedDocValuesTermIterator::new(self);
                Ok(DocValuesTermIterator::sorted(ti))
            }
        }
    }
}

impl BinaryDocValues for TailoredSortedDocValues {
    fn get(&mut self, doc_id: DocId) -> Result<Vec<u8>> {
        let ord = self.get_ord(doc_id)?;
        if ord == -1 {
            Ok(Vec::with_capacity(0))
        } else {
            self.lookup_ord(ord)
        }
    }
}
