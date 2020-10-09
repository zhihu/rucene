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

pub mod lucene54;

pub(crate) mod doc_values_format;

pub use self::doc_values_format::*;

mod doc_values_producer;

pub use self::doc_values_producer::*;

mod doc_values_consumer;

pub use self::doc_values_consumer::*;

mod doc_values_writer;

pub use self::doc_values_writer::*;

mod doc_values_iterator;

pub use self::doc_values_iterator::*;

use core::codec::doc_values::lucene54::DocValuesTermIterator;
use core::util::DocId;

use error::Result;
use std::cmp::Ordering;
use std::sync::Arc;

/// When returned by next_ord() it means there are no more ordinals for the document.
pub const NO_MORE_ORDS: i64 = -1;

pub trait BinaryDocValues: Send + Sync {
    fn get(&mut self, doc_id: DocId) -> Result<Vec<u8>>;
}

impl<T: BinaryDocValues + ?Sized> BinaryDocValues for Box<T> {
    fn get(&mut self, doc_id: DocId) -> Result<Vec<u8>> {
        (**self).get(doc_id)
    }
}

pub struct EmptyBinaryDocValues;

impl BinaryDocValues for EmptyBinaryDocValues {
    fn get(&mut self, _doc_id: DocId) -> Result<Vec<u8>> {
        Ok(Vec::with_capacity(0))
    }
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

/// a `NumericDocValues` that always produce 0
#[derive(Default)]
pub struct EmptyNumericDocValues;

impl NumericDocValues for EmptyNumericDocValues {
    fn get(&self, _doc_id: DocId) -> Result<i64> {
        Ok(0)
    }
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

pub trait SortedSetDocValues: Send + Sync {
    /// positions to the specified document
    fn set_document(&mut self, doc: DocId) -> Result<()>;

    /// Returns the next ordinal for the current document (previously
    /// set by `Self::set_document()`)
    fn next_ord(&mut self) -> Result<i64>;

    /// Retrieves the value for the specified ordinal.
    fn lookup_ord(&mut self, ord: i64) -> Result<Vec<u8>>;

    /// Returns the number of unique values.
    fn get_value_count(&self) -> usize;

    /// if `key` exists, returns its ordinal, else return `-insertion_point - 1`
    fn lookup_term(&mut self, key: &[u8]) -> Result<i64> {
        let mut low = 0_i64;
        let mut high = self.get_value_count() as i64 - 1;
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

    /// return a `TermIterator` over the values
    fn term_iterator(&self) -> Result<DocValuesTermIterator>;
}

pub struct EmptySortedSetDocValues;

impl SortedSetDocValues for EmptySortedSetDocValues {
    fn set_document(&mut self, _doc: DocId) -> Result<()> {
        Ok(())
    }
    fn next_ord(&mut self) -> Result<i64> {
        Ok(NO_MORE_ORDS)
    }
    fn lookup_ord(&mut self, _ord: i64) -> Result<Vec<u8>> {
        Ok(Vec::with_capacity(0))
    }
    fn get_value_count(&self) -> usize {
        0
    }

    fn lookup_term(&mut self, _key: &[u8]) -> Result<i64> {
        Ok(-1)
    }

    fn term_iterator(&self) -> Result<DocValuesTermIterator> {
        Ok(DocValuesTermIterator::empty())
    }
}

pub enum DocValuesProviderEnum {
    Binary(Arc<dyn BinaryDocValuesProvider>),
    Numeric(Arc<dyn NumericDocValuesProvider>),
    Sorted(Arc<dyn SortedDocValuesProvider>),
    SortedNumeric(Arc<dyn SortedNumericDocValuesProvider>),
    SortedSet(Arc<dyn SortedSetDocValuesProvider>),
}

pub trait BinaryDocValuesProvider: Send + Sync {
    fn get(&self) -> Result<Box<dyn BinaryDocValues>>;
}

pub trait NumericDocValuesProvider: Send + Sync {
    fn get(&self) -> Result<Box<dyn NumericDocValues>>;
}

pub trait SortedNumericDocValuesProvider: Send + Sync {
    fn get(&self) -> Result<Box<dyn SortedNumericDocValues>>;
}

pub trait SortedDocValuesProvider: Send + Sync {
    fn get(&self) -> Result<Box<dyn SortedDocValues>>;
}

pub trait SortedSetDocValuesProvider: Send + Sync {
    fn get(&self) -> Result<Box<dyn SortedSetDocValues>>;
}
