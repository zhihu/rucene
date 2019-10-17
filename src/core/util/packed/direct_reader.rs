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

use core::codec::doc_values::NumericDocValues;
use core::store::io::RandomAccessInput;
use core::util::bit_util::UnsignedShift;
use core::util::LongValues;

use error::ErrorKind::IllegalArgument;
use error::ErrorKind::RuntimeError;
use error::Result;

use core::util::DocId;
use std::sync::Arc;

pub struct DirectReader;

impl DirectReader {
    pub fn get_instance(
        slice: Arc<dyn RandomAccessInput>,
        bits_per_value: i32,
        offset: i64,
    ) -> Result<DirectPackedReader> {
        match bits_per_value {
            1 => Ok(DirectPackedReader(DirectPackedReaderEnum::Bit1(
                DirectPackedReader1::new(slice, offset),
            ))),
            2 => Ok(DirectPackedReader(DirectPackedReaderEnum::Bit2(
                DirectPackedReader2::new(slice, offset),
            ))),
            4 => Ok(DirectPackedReader(DirectPackedReaderEnum::Bit4(
                DirectPackedReader4::new(slice, offset),
            ))),
            8 => Ok(DirectPackedReader(DirectPackedReaderEnum::Bit8(
                DirectPackedReader8::new(slice, offset),
            ))),
            12 => Ok(DirectPackedReader(DirectPackedReaderEnum::Bit12(
                DirectPackedReader12::new(slice, offset),
            ))),
            16 => Ok(DirectPackedReader(DirectPackedReaderEnum::Bit16(
                DirectPackedReader16::new(slice, offset),
            ))),
            20 => Ok(DirectPackedReader(DirectPackedReaderEnum::Bit20(
                DirectPackedReader20::new(slice, offset),
            ))),
            24 => Ok(DirectPackedReader(DirectPackedReaderEnum::Bit24(
                DirectPackedReader24::new(slice, offset),
            ))),
            28 => Ok(DirectPackedReader(DirectPackedReaderEnum::Bit28(
                DirectPackedReader28::new(slice, offset),
            ))),
            32 => Ok(DirectPackedReader(DirectPackedReaderEnum::Bit32(
                DirectPackedReader32::new(slice, offset),
            ))),
            40 => Ok(DirectPackedReader(DirectPackedReaderEnum::Bit40(
                DirectPackedReader40::new(slice, offset),
            ))),
            48 => Ok(DirectPackedReader(DirectPackedReaderEnum::Bit48(
                DirectPackedReader48::new(slice, offset),
            ))),
            56 => Ok(DirectPackedReader(DirectPackedReaderEnum::Bit56(
                DirectPackedReader56::new(slice, offset),
            ))),
            64 => Ok(DirectPackedReader(DirectPackedReaderEnum::Bit64(
                DirectPackedReader64::new(slice, offset),
            ))),
            _ => bail!(IllegalArgument(format!(
                "unsupported bits_per_value: {}",
                bits_per_value
            ))),
        }
    }
}

#[derive(Clone)]
pub struct DirectPackedReader(DirectPackedReaderEnum);

impl LongValues for DirectPackedReader {
    fn get64(&self, index: i64) -> Result<i64> {
        match &self.0 {
            DirectPackedReaderEnum::Bit1(r) => r.get64(index),
            DirectPackedReaderEnum::Bit2(r) => r.get64(index),
            DirectPackedReaderEnum::Bit4(r) => r.get64(index),
            DirectPackedReaderEnum::Bit8(r) => r.get64(index),
            DirectPackedReaderEnum::Bit12(r) => r.get64(index),
            DirectPackedReaderEnum::Bit16(r) => r.get64(index),
            DirectPackedReaderEnum::Bit20(r) => r.get64(index),
            DirectPackedReaderEnum::Bit24(r) => r.get64(index),
            DirectPackedReaderEnum::Bit28(r) => r.get64(index),
            DirectPackedReaderEnum::Bit32(r) => r.get64(index),
            DirectPackedReaderEnum::Bit40(r) => r.get64(index),
            DirectPackedReaderEnum::Bit48(r) => r.get64(index),
            DirectPackedReaderEnum::Bit56(r) => r.get64(index),
            DirectPackedReaderEnum::Bit64(r) => r.get64(index),
        }
    }
}

impl NumericDocValues for DirectPackedReader {
    fn get(&self, doc_id: DocId) -> Result<i64> {
        match &self.0 {
            DirectPackedReaderEnum::Bit1(r) => r.get(doc_id),
            DirectPackedReaderEnum::Bit2(r) => r.get(doc_id),
            DirectPackedReaderEnum::Bit4(r) => r.get(doc_id),
            DirectPackedReaderEnum::Bit8(r) => r.get(doc_id),
            DirectPackedReaderEnum::Bit12(r) => r.get(doc_id),
            DirectPackedReaderEnum::Bit16(r) => r.get(doc_id),
            DirectPackedReaderEnum::Bit20(r) => r.get(doc_id),
            DirectPackedReaderEnum::Bit24(r) => r.get(doc_id),
            DirectPackedReaderEnum::Bit28(r) => r.get(doc_id),
            DirectPackedReaderEnum::Bit32(r) => r.get(doc_id),
            DirectPackedReaderEnum::Bit40(r) => r.get(doc_id),
            DirectPackedReaderEnum::Bit48(r) => r.get(doc_id),
            DirectPackedReaderEnum::Bit56(r) => r.get(doc_id),
            DirectPackedReaderEnum::Bit64(r) => r.get(doc_id),
        }
    }
}

#[derive(Clone)]
enum DirectPackedReaderEnum {
    Bit1(DirectPackedReader1),
    Bit2(DirectPackedReader2),
    Bit4(DirectPackedReader4),
    Bit8(DirectPackedReader8),
    Bit12(DirectPackedReader12),
    Bit16(DirectPackedReader16),
    Bit20(DirectPackedReader20),
    Bit24(DirectPackedReader24),
    Bit28(DirectPackedReader28),
    Bit32(DirectPackedReader32),
    Bit40(DirectPackedReader40),
    Bit48(DirectPackedReader48),
    Bit56(DirectPackedReader56),
    Bit64(DirectPackedReader64),
}

// ================ Begin Reader 1 ================
#[derive(Clone)]
struct DirectPackedReader1 {
    random_access_input: Arc<dyn RandomAccessInput>,
    offset: i64,
}

impl DirectPackedReader1 {
    fn new(random_access_input: Arc<dyn RandomAccessInput>, offset: i64) -> DirectPackedReader1 {
        DirectPackedReader1 {
            random_access_input,
            offset,
        }
    }
}

impl LongValues for DirectPackedReader1 {
    fn get64(&self, index: i64) -> Result<i64> {
        if index < 0 {
            bail!(IllegalArgument(format!(
                "negative index encountered: {}",
                index
            )));
        }

        let shift = 7 - (index as i32 & 0x7);
        let byte_dance = self
            .random_access_input
            .read_byte((self.offset + (index >> 3)) as u64)?;

        Ok(i64::from((byte_dance >> shift) & 0x1))
    }
}

impl NumericDocValues for DirectPackedReader1 {
    fn get(&self, doc_id: DocId) -> Result<i64> {
        self.get64(doc_id as i64)
    }
}

// ================ Begin Reader 2 ================
#[derive(Clone)]
struct DirectPackedReader2 {
    random_access_input: Arc<dyn RandomAccessInput>,
    offset: i64,
}

impl DirectPackedReader2 {
    fn new(random_access_input: Arc<dyn RandomAccessInput>, offset: i64) -> DirectPackedReader2 {
        DirectPackedReader2 {
            random_access_input,
            offset,
        }
    }
}

impl LongValues for DirectPackedReader2 {
    fn get64(&self, index: i64) -> Result<i64> {
        if index < 0 {
            bail!(IllegalArgument(format!(
                "negative index encountered: {}",
                index
            )));
        }

        let shift = (3 - (index as i32 & 0x3)) << 1;

        let byte_dance = self
            .random_access_input
            .read_byte((self.offset + (index >> 2)) as u64)?;

        Ok(i64::from((byte_dance >> shift) & 0x3))
    }
}

impl NumericDocValues for DirectPackedReader2 {
    fn get(&self, doc_id: DocId) -> Result<i64> {
        self.get64(doc_id as i64)
    }
}

// ================ Begin Reader 4 ================
#[derive(Clone)]
struct DirectPackedReader4 {
    random_access_input: Arc<dyn RandomAccessInput>,
    offset: i64,
}

impl DirectPackedReader4 {
    fn new(random_access_input: Arc<dyn RandomAccessInput>, offset: i64) -> DirectPackedReader4 {
        DirectPackedReader4 {
            random_access_input,
            offset,
        }
    }
}

impl LongValues for DirectPackedReader4 {
    fn get64(&self, index: i64) -> Result<i64> {
        if index < 0 {
            bail!(IllegalArgument(format!(
                "negative index encountered: {}",
                index
            )));
        }

        let shift = (((index + 1) & 0x1) as i32) << 2;

        let byte_dance = match self
            .random_access_input
            .read_byte((self.offset + (index >> 1)) as u64)
        {
            Ok(byte_dance) => byte_dance,
            Err(ref e) => bail!(RuntimeError(format!("{:?}", e))),
        };

        Ok(i64::from((byte_dance >> shift) & 0xF))
    }
}

impl NumericDocValues for DirectPackedReader4 {
    fn get(&self, doc_id: DocId) -> Result<i64> {
        self.get64(doc_id as i64)
    }
}

// ================ Begin Reader 8 ================
#[derive(Clone)]
struct DirectPackedReader8 {
    random_access_input: Arc<dyn RandomAccessInput>,
    offset: i64,
}

impl DirectPackedReader8 {
    fn new(random_access_input: Arc<dyn RandomAccessInput>, offset: i64) -> DirectPackedReader8 {
        DirectPackedReader8 {
            random_access_input,
            offset,
        }
    }
}

impl LongValues for DirectPackedReader8 {
    fn get64(&self, index: i64) -> Result<i64> {
        debug_assert!(index >= 0);
        self.random_access_input
            .read_byte((self.offset + index) as u64)
            .map(i64::from)
    }
}

impl NumericDocValues for DirectPackedReader8 {
    fn get(&self, doc_id: DocId) -> Result<i64> {
        self.get64(doc_id as i64)
    }
}

// ================ Begin Reader 12 ================
#[derive(Clone)]
struct DirectPackedReader12 {
    random_access_input: Arc<dyn RandomAccessInput>,
    offset: i64,
}

impl DirectPackedReader12 {
    fn new(random_access_input: Arc<dyn RandomAccessInput>, offset: i64) -> DirectPackedReader12 {
        DirectPackedReader12 {
            random_access_input,
            offset,
        }
    }
}

impl LongValues for DirectPackedReader12 {
    fn get64(&self, index: i64) -> Result<i64> {
        debug_assert!(index >= 0);

        let offset = (index * 3) >> 1;
        let shift = ((index + 1) & 0x1) << 2;

        self.random_access_input
            .read_short((self.offset + offset) as u64)
            .map(|w| i64::from((w >> shift) & 0xFFF))
    }
}

impl NumericDocValues for DirectPackedReader12 {
    fn get(&self, doc_id: DocId) -> Result<i64> {
        self.get64(doc_id as i64)
    }
}

// ================ Begin Reader 16 ================
#[derive(Clone)]
struct DirectPackedReader16 {
    random_access_input: Arc<dyn RandomAccessInput>,
    offset: i64,
}

impl DirectPackedReader16 {
    fn new(random_access_input: Arc<dyn RandomAccessInput>, offset: i64) -> DirectPackedReader16 {
        DirectPackedReader16 {
            random_access_input,
            offset,
        }
    }
}

impl LongValues for DirectPackedReader16 {
    fn get64(&self, index: i64) -> Result<i64> {
        debug_assert!(index >= 0);

        self.random_access_input
            .read_short((self.offset + (index << 1)) as u64)
            .map(|w| i64::from(w as u16))
    }
}

impl NumericDocValues for DirectPackedReader16 {
    fn get(&self, doc_id: DocId) -> Result<i64> {
        self.get64(doc_id as i64)
    }
}

// ================ Begin Reader 20 ================
#[derive(Clone)]
struct DirectPackedReader20 {
    random_access_input: Arc<dyn RandomAccessInput>,
    offset: i64,
}

impl DirectPackedReader20 {
    fn new(random_access_input: Arc<dyn RandomAccessInput>, offset: i64) -> DirectPackedReader20 {
        DirectPackedReader20 {
            random_access_input,
            offset,
        }
    }
}

impl LongValues for DirectPackedReader20 {
    fn get64(&self, index: i64) -> Result<i64> {
        debug_assert!(index >= 0);

        let offset = (index * 5) >> 1;
        let shift = (((index + 1) & 0x1) << 2) + 8;
        self.random_access_input
            .read_int((self.offset + offset) as u64)
            .map(|dw| i64::from(((dw as u32) >> shift) & 0xFFFFF))
    }
}

impl NumericDocValues for DirectPackedReader20 {
    fn get(&self, doc_id: DocId) -> Result<i64> {
        self.get64(doc_id as i64)
    }
}

// ================ Begin Reader 24 ================
#[derive(Clone)]
struct DirectPackedReader24 {
    random_access_input: Arc<dyn RandomAccessInput>,
    offset: i64,
}

impl DirectPackedReader24 {
    fn new(random_access_input: Arc<dyn RandomAccessInput>, offset: i64) -> DirectPackedReader24 {
        DirectPackedReader24 {
            random_access_input,
            offset,
        }
    }
}

impl LongValues for DirectPackedReader24 {
    fn get64(&self, index: i64) -> Result<i64> {
        debug_assert!(index >= 0);

        self.random_access_input
            .read_int((self.offset + 3 * index) as u64)
            .map(|v| i64::from(v as u32 >> 8))
    }
}

impl NumericDocValues for DirectPackedReader24 {
    fn get(&self, doc_id: DocId) -> Result<i64> {
        self.get64(doc_id as i64)
    }
}

// ================ Begin Reader 28 ================
#[derive(Clone)]
struct DirectPackedReader28 {
    random_access_input: Arc<dyn RandomAccessInput>,
    offset: i64,
}

impl DirectPackedReader28 {
    fn new(random_access_input: Arc<dyn RandomAccessInput>, offset: i64) -> DirectPackedReader28 {
        DirectPackedReader28 {
            random_access_input,
            offset,
        }
    }
}

impl LongValues for DirectPackedReader28 {
    fn get64(&self, index: i64) -> Result<i64> {
        debug_assert!(index >= 0);

        let offset = (index * 7) >> 1;
        let shift = ((index + 1) & 0x1) << 2;
        self.random_access_input
            .read_int((self.offset + offset) as u64)
            .map(|v| i64::from((v as u32 >> shift) & 0x0FFF_FFFF))
    }
}

impl NumericDocValues for DirectPackedReader28 {
    fn get(&self, doc_id: DocId) -> Result<i64> {
        self.get64(doc_id as i64)
    }
}

// ================ Begin Reader 32 ================
#[derive(Clone)]
struct DirectPackedReader32 {
    random_access_input: Arc<dyn RandomAccessInput>,
    offset: i64,
}

impl DirectPackedReader32 {
    fn new(random_access_input: Arc<dyn RandomAccessInput>, offset: i64) -> DirectPackedReader32 {
        DirectPackedReader32 {
            random_access_input,
            offset,
        }
    }
}

impl LongValues for DirectPackedReader32 {
    fn get64(&self, index: i64) -> Result<i64> {
        debug_assert!(index >= 0);
        self.random_access_input
            .read_int((self.offset + (index << 2)) as u64)
            .map(|v| i64::from(v as u32))
    }
}

impl NumericDocValues for DirectPackedReader32 {
    fn get(&self, doc_id: DocId) -> Result<i64> {
        self.get64(doc_id as i64)
    }
}

// ================ Begin Reader 40 ================
#[derive(Clone)]
struct DirectPackedReader40 {
    random_access_input: Arc<dyn RandomAccessInput>,
    offset: i64,
}

impl DirectPackedReader40 {
    fn new(random_access_input: Arc<dyn RandomAccessInput>, offset: i64) -> DirectPackedReader40 {
        DirectPackedReader40 {
            random_access_input,
            offset,
        }
    }
}

impl LongValues for DirectPackedReader40 {
    fn get64(&self, index: i64) -> Result<i64> {
        debug_assert!(index >= 0);
        self.random_access_input
            .read_long((self.offset + index * 5) as u64)
            .map(|w| w.unsigned_shift(24))
    }
}

impl NumericDocValues for DirectPackedReader40 {
    fn get(&self, doc_id: DocId) -> Result<i64> {
        self.get64(doc_id as i64)
    }
}

// ================ Begin Reader 48 ================
#[derive(Clone)]
struct DirectPackedReader48 {
    random_access_input: Arc<dyn RandomAccessInput>,
    offset: i64,
}

impl DirectPackedReader48 {
    fn new(random_access_input: Arc<dyn RandomAccessInput>, offset: i64) -> DirectPackedReader48 {
        DirectPackedReader48 {
            random_access_input,
            offset,
        }
    }
}

impl LongValues for DirectPackedReader48 {
    fn get64(&self, index: i64) -> Result<i64> {
        debug_assert!(index >= 0);

        self.random_access_input
            .read_long((self.offset + index * 6) as u64)
            .map(|v| v.unsigned_shift(16))
    }
}

impl NumericDocValues for DirectPackedReader48 {
    fn get(&self, doc_id: DocId) -> Result<i64> {
        self.get64(doc_id as i64)
    }
}

// ================ Begin Reader 56 ================
#[derive(Clone)]
struct DirectPackedReader56 {
    random_access_input: Arc<dyn RandomAccessInput>,
    offset: i64,
}

impl DirectPackedReader56 {
    fn new(random_access_input: Arc<dyn RandomAccessInput>, offset: i64) -> DirectPackedReader56 {
        DirectPackedReader56 {
            random_access_input,
            offset,
        }
    }
}

impl LongValues for DirectPackedReader56 {
    fn get64(&self, index: i64) -> Result<i64> {
        debug_assert!(index >= 0);

        self.random_access_input
            .read_long((self.offset + 7 * index) as u64)
            .map(|v| v.unsigned_shift(8))
    }
}

impl NumericDocValues for DirectPackedReader56 {
    fn get(&self, doc_id: DocId) -> Result<i64> {
        self.get64(doc_id as i64)
    }
}

// ================ Begin Reader 64 ================
#[derive(Clone)]
struct DirectPackedReader64 {
    random_access_input: Arc<dyn RandomAccessInput>,
    offset: i64,
}

impl DirectPackedReader64 {
    fn new(random_access_input: Arc<dyn RandomAccessInput>, offset: i64) -> DirectPackedReader64 {
        DirectPackedReader64 {
            random_access_input,
            offset,
        }
    }
}

impl LongValues for DirectPackedReader64 {
    fn get64(&self, index: i64) -> Result<i64> {
        debug_assert!(index >= 0);

        self.random_access_input
            .read_long((self.offset + (index << 3)) as u64)
    }
}

impl NumericDocValues for DirectPackedReader64 {
    fn get(&self, doc_id: DocId) -> Result<i64> {
        self.get64(doc_id as i64)
    }
}
