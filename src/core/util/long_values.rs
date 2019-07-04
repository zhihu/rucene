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

use core::codec::LiveBitsEnum;
use core::index::NumericDocValues;
use core::util::packed::DirectPackedReader;
use core::util::Bits;
use core::util::DocId;
use error::Result;

use std::sync::Arc;

/// Abstraction over an array of longs.
///
/// This class extends `NumericDocValues` so that we don't need to add another
/// level of abstraction every time we want eg. to use the `PackedInts`
/// utility classes to represent a `NumericDocValues` instance.
pub trait LongValues: NumericDocValues {
    fn get64(&self, index: i64) -> Result<i64>;

    fn get64_mut(&mut self, index: i64) -> Result<i64> {
        self.get64(index)
    }
}

pub trait CloneableLongValues: LongValues {
    fn cloned(&self) -> Box<dyn CloneableLongValues>;

    fn cloned_lv(&self) -> Box<dyn LongValues>;
}

impl<T: LongValues + Clone + 'static> CloneableLongValues for T {
    fn cloned(&self) -> Box<dyn CloneableLongValues> {
        Box::new(self.clone())
    }

    fn cloned_lv(&self) -> Box<dyn LongValues> {
        Box::new(self.clone())
    }
}

pub struct EmptyLongValues;

impl LongValues for EmptyLongValues {
    fn get64(&self, _index: i64) -> Result<i64> {
        Ok(0)
    }
}

impl NumericDocValues for EmptyLongValues {
    fn get(&self, _doc_id: DocId) -> Result<i64> {
        Ok(0)
    }
}

pub struct IdentityLongValues;

impl LongValues for IdentityLongValues {
    fn get64(&self, index: i64) -> Result<i64> {
        Ok(index)
    }
}

impl NumericDocValues for IdentityLongValues {
    fn get(&self, doc_id: DocId) -> Result<i64> {
        Ok(doc_id as i64)
    }
}

#[derive(Clone)]
pub struct LiveLongValues {
    live: LiveBitsEnum,
    constant: i64,
}

impl LiveLongValues {
    pub fn new(live: LiveBitsEnum, constant: i64) -> Self {
        LiveLongValues { live, constant }
    }
}

impl LongValues for LiveLongValues {
    fn get64(&self, index: i64) -> Result<i64> {
        if self.live.get(index as usize)? {
            Ok(self.constant)
        } else {
            Ok(0)
        }
    }
}

impl NumericDocValues for LiveLongValues {
    fn get(&self, doc_id: DocId) -> Result<i64> {
        self.get64(i64::from(doc_id))
    }
}

#[derive(Clone)]
pub struct DeltaLongValues {
    values: DirectPackedReader,
    delta: i64,
}

impl DeltaLongValues {
    pub fn new(values: DirectPackedReader, delta: i64) -> Self {
        DeltaLongValues { values, delta }
    }
}

impl LongValues for DeltaLongValues {
    fn get64(&self, index: i64) -> Result<i64> {
        let packed = self.values.get64(index)?;
        Ok(self.delta + packed)
    }
}

impl NumericDocValues for DeltaLongValues {
    fn get(&self, doc_id: DocId) -> Result<i64> {
        self.get64(doc_id as i64)
    }
}

#[derive(Clone)]
pub struct GcdLongValues {
    quotient_reader: DirectPackedReader,
    base: i64,
    mult: i64,
}

impl GcdLongValues {
    pub fn new(quotient_reader: DirectPackedReader, base: i64, mult: i64) -> Self {
        GcdLongValues {
            quotient_reader,
            base,
            mult,
        }
    }
}

impl LongValues for GcdLongValues {
    fn get64(&self, index: i64) -> Result<i64> {
        let val = self.quotient_reader.get64(index)?;
        Ok(self.base + self.mult * val)
    }
}

impl NumericDocValues for GcdLongValues {
    fn get(&self, doc_id: DocId) -> Result<i64> {
        self.get64(i64::from(doc_id))
    }
}

#[derive(Clone)]
pub struct TableLongValues {
    ords: DirectPackedReader,
    table: Arc<[i64]>,
}

impl TableLongValues {
    pub fn new(ords: DirectPackedReader, table: Vec<i64>) -> TableLongValues {
        TableLongValues {
            ords,
            table: Arc::from(Box::from(table)),
        }
    }
}

impl LongValues for TableLongValues {
    fn get64(&self, index: i64) -> Result<i64> {
        self.ords.get64(index).map(|val| self.table[val as usize])
    }
}

impl NumericDocValues for TableLongValues {
    fn get(&self, doc_id: DocId) -> Result<i64> {
        self.get64(i64::from(doc_id))
    }
}
