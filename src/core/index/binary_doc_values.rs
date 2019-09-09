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

use core::store::IndexInput;
use core::util::DocId;
use core::util::LongValues;
use error::Result;
use std::io::Read;

pub trait BinaryDocValuesProvider: Send + Sync {
    fn get(&self) -> Result<Box<dyn BinaryDocValues>>;
}

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

/// // internally we compose complex dv (sorted/sortedset) from other ones
pub(crate) trait LongBinaryDocValues: BinaryDocValues {
    fn get64(&mut self, doc_id: i64) -> Result<Vec<u8>>;

    fn clone_long(&self) -> Result<Box<dyn LongBinaryDocValues>>;
}

pub(crate) struct FixedBinaryDocValues {
    data: Box<dyn IndexInput>,
    buffer_len: usize,
}

impl FixedBinaryDocValues {
    pub fn new(data: Box<dyn IndexInput>, buffer_len: usize) -> Self {
        FixedBinaryDocValues { data, buffer_len }
    }

    pub fn clone(&self) -> Result<Self> {
        self.data.clone().map(|data| Self {
            data,
            buffer_len: self.buffer_len,
        })
    }
}

impl BinaryDocValuesProvider for FixedBinaryDocValues {
    fn get(&self) -> Result<Box<dyn BinaryDocValues>> {
        let data = self.data.as_ref().clone()?;
        Ok(Box::new(Self {
            data,
            buffer_len: self.buffer_len,
        }))
    }
}

impl LongBinaryDocValues for FixedBinaryDocValues {
    fn get64(&mut self, id: i64) -> Result<Vec<u8>> {
        let length = self.buffer_len;
        self.data.seek(id * length as i64)?;
        let mut buffer = vec![0u8; length];
        self.data.read_exact(&mut buffer)?;
        Ok(buffer)
    }

    fn clone_long(&self) -> Result<Box<dyn LongBinaryDocValues>> {
        Ok(Box::new(self.clone()?))
    }
}

impl BinaryDocValues for FixedBinaryDocValues {
    fn get(&mut self, doc_id: DocId) -> Result<Vec<u8>> {
        FixedBinaryDocValues::get64(self, i64::from(doc_id))
    }
}

pub(crate) struct VariableBinaryDocValues<T: LongValues + Clone + 'static> {
    addresses: T,
    data: Box<dyn IndexInput>,
}

impl<T: LongValues + Clone + 'static> VariableBinaryDocValues<T> {
    pub fn new(addresses: T, data: Box<dyn IndexInput>, _length: usize) -> Self {
        VariableBinaryDocValues { addresses, data }
    }

    pub fn clone(&self) -> Result<Self> {
        self.data.clone().map(|data| Self {
            addresses: self.addresses.clone(),
            data,
        })
    }
}

impl<T: LongValues + Clone + 'static> BinaryDocValuesProvider for VariableBinaryDocValues<T> {
    fn get(&self) -> Result<Box<dyn BinaryDocValues>> {
        let data = self.data.clone()?;
        Ok(Box::new(Self {
            addresses: self.addresses.clone(),
            data,
        }))
    }
}

impl<T: LongValues + Clone + 'static> LongBinaryDocValues for VariableBinaryDocValues<T> {
    fn get64(&mut self, id: i64) -> Result<Vec<u8>> {
        let start_address = self.addresses.get64(id)?;
        let end_address = self.addresses.get64(id + 1)?;
        let length = (end_address - start_address) as usize;
        self.data.seek(start_address)?;
        let mut buffer = vec![0u8; length];
        self.data.read_exact(&mut buffer)?;
        Ok(buffer)
    }

    fn clone_long(&self) -> Result<Box<dyn LongBinaryDocValues>> {
        Ok(Box::new(self.clone()?))
    }
}

impl<T: LongValues + Clone + 'static> BinaryDocValues for VariableBinaryDocValues<T> {
    fn get(&mut self, doc_id: DocId) -> Result<Vec<u8>> {
        self.get64(i64::from(doc_id))
    }
}
