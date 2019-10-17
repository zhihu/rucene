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
use core::store::io::IndexInput;
use core::util::packed::PackedIntsNullReader;
use core::util::packed::{self, Reader, ReaderEnum};
use core::util::{DocId, LongValues};
use error::ErrorKind::{CorruptIndex, IllegalArgument};
use error::Result;

use std::sync::Arc;

/// Provides random access to a stream written with MonotonicBlockPackedWriter
#[derive(Clone)]
pub struct MonotonicBlockPackedReader {
    inner: Arc<MonotonicBlockPackedReaderInner>,
}

struct MonotonicBlockPackedReaderInner {
    block_shift: usize,
    block_mask: usize,
    value_count: usize,
    min_values: Vec<i64>,
    averages: Vec<f32>,
    sub_readers: Vec<ReaderEnum>,
    #[allow(dead_code)]
    sum_bpv: i64,
}

impl MonotonicBlockPackedReader {
    pub fn expected(origin: i64, average: f32, index: i32) -> i64 {
        origin + (average * index as f32) as i64
    }

    pub fn new(
        input: &mut dyn IndexInput,
        packed_ints_version: i32,
        block_size: usize,
        value_count: usize,
        direct: bool,
    ) -> Result<MonotonicBlockPackedReader> {
        let block_shift =
            packed::check_block_size(block_size, packed::MIN_BLOCK_SIZE, packed::MAX_BLOCK_SIZE);
        let block_mask = block_size - 1;
        let num_blocks = packed::num_blocks(value_count, block_size);
        let mut min_values = vec![0_i64; num_blocks];
        let mut averages = vec![0.0_f32; num_blocks];
        let mut sub_readers = Vec::new();
        let mut sum_bpv: i64 = 0;

        for i in 0..num_blocks {
            min_values[i] = input.read_zlong()?;
            averages[i] = f32::from_bits(input.read_int()? as u32);
            let bits_per_value = input.read_vint()?;
            sum_bpv += i64::from(bits_per_value);
            if bits_per_value > 64 {
                bail!(CorruptIndex("bits_per_value > 64".to_owned()));
            }
            if bits_per_value == 0 {
                sub_readers.push(ReaderEnum::PackedIntsNull(PackedIntsNullReader::new(
                    block_size,
                )));
            } else {
                let left = value_count - i * block_size;
                let size = ::std::cmp::min(left, block_size);
                if direct {
                    unimplemented!();
                } else {
                    let one_reader = packed::get_reader_no_header(
                        input,
                        packed::Format::Packed,
                        packed_ints_version,
                        size,
                        bits_per_value,
                    )?;
                    sub_readers.push(one_reader);
                }
            }
        }

        let inner = MonotonicBlockPackedReaderInner {
            block_shift,
            block_mask,
            value_count,
            min_values,
            averages,
            sub_readers,
            sum_bpv,
        };

        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    /// Returns the number of values
    pub fn size(&self) -> usize {
        self.inner.value_count
    }
}

impl LongValues for MonotonicBlockPackedReader {
    fn get64(&self, index: i64) -> Result<i64> {
        if !(index >= 0 && index < self.inner.value_count as i64) {
            bail!(IllegalArgument(format!("index {} out of range", index)))
        }
        let block = (index >> self.inner.block_shift) as usize;
        let idx = (index & (self.inner.block_mask as i64)) as i32;
        let val = Self::expected(
            self.inner.min_values[block],
            self.inner.averages[block],
            idx,
        ) + self.inner.sub_readers[block].get(idx as usize);
        Ok(val)
    }
}

impl NumericDocValues for MonotonicBlockPackedReader {
    fn get(&self, doc_id: DocId) -> Result<i64> {
        self.get64(i64::from(doc_id))
    }
}
