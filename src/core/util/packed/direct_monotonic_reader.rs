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
use core::store::io::{IndexInput, RandomAccessInput};
use core::util::{packed::DirectReader, DocId, LongValues};
use error::Result;

use core::util::packed::direct_reader::DirectPackedReader;
use std::sync::Arc;

pub struct DirectMonotonicMeta {
    #[allow(dead_code)]
    num_values: i64,
    block_shift: i32,
    num_blocks: usize,
    mins: Arc<Vec<i64>>,
    avgs: Arc<Vec<f32>>,
    bpvs: Arc<Vec<u8>>,
    offsets: Arc<Vec<i64>>,
}

pub struct DirectMonotonicReader;

impl DirectMonotonicReader {
    pub fn load_meta(
        meta_in: &mut dyn IndexInput,
        num_values: i64,
        block_shift: i32,
    ) -> Result<DirectMonotonicMeta> {
        let mut num_blocks = num_values >> block_shift;
        if (num_blocks << block_shift) < num_values {
            num_blocks += 1;
        }
        let num_blocks = num_blocks as usize;

        let mut mins = vec![0i64; num_blocks];
        let mut avgs = vec![0f32; num_blocks];
        let mut bpvs = vec![0u8; num_blocks];
        let mut offsets = vec![0i64; num_blocks];

        for i in 0..num_blocks {
            mins[i] = meta_in.read_long()?;
            avgs[i] = f32::from_bits(meta_in.read_int()? as u32);
            offsets[i] = meta_in.read_long()?;
            bpvs[i] = meta_in.read_byte()?;
        }
        Ok(DirectMonotonicMeta {
            num_values,
            block_shift,
            num_blocks,
            mins: Arc::new(mins),
            avgs: Arc::new(avgs),
            bpvs: Arc::new(bpvs),
            offsets: Arc::new(offsets),
        })
    }

    pub fn get_instance(
        meta: &DirectMonotonicMeta,
        data: &Arc<dyn RandomAccessInput>,
    ) -> Result<MixinMonotonicLongValues> {
        let mut readers = Vec::with_capacity(meta.num_blocks);
        for i in 0..meta.num_blocks {
            let reader = if meta.bpvs[i] == 0 {
                None
            } else {
                Some(DirectReader::get_instance(
                    Arc::clone(data),
                    i32::from(meta.bpvs[i]),
                    meta.offsets[i],
                )?)
            };
            readers.push(reader);
        }

        Ok(MixinMonotonicLongValues {
            readers: Arc::from(Box::from(readers)),
            block_shift: meta.block_shift,
            mins: Arc::clone(&meta.mins),
            avgs: Arc::clone(&meta.avgs),
        })
    }
}

#[derive(Clone)]
pub struct MixinMonotonicLongValues {
    readers: Arc<[Option<DirectPackedReader>]>,
    block_shift: i32,
    mins: Arc<Vec<i64>>,
    avgs: Arc<Vec<f32>>,
}

impl LongValues for MixinMonotonicLongValues {
    fn get64(&self, index: i64) -> Result<i64> {
        // we know all readers don't require context
        let block = ((index as u64) >> self.block_shift) as usize;
        let block_index: i64 = index & ((1 << self.block_shift) - 1);
        let delta = if let Some(ref reader) = self.readers[block] {
            reader.get64(block_index)?
        } else {
            0
        };
        Ok(self.mins[block] + (self.avgs[block] * block_index as f32) as i64 + delta)
    }
}

impl NumericDocValues for MixinMonotonicLongValues {
    fn get(&self, doc_id: DocId) -> Result<i64> {
        self.get64(i64::from(doc_id))
    }
}
