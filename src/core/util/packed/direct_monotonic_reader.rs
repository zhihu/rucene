use core::index::{NumericDocValues, NumericDocValuesContext};
use core::store::IndexInput;
use core::store::RandomAccessInput;
use core::util::packed::DirectReader;
use core::util::DocId;
use core::util::{EmptyLongValues, LongValues, LongValuesContext};
use error::Result;

use std::sync::Arc;

pub struct DirectMonotonicMeta {
    #[allow(dead_code)]
    num_values: i64,
    block_shift: i32,
    num_blocks: i32,
    mins: Vec<i64>,
    avgs: Vec<f32>,
    bpvs: Vec<u8>,
    offsets: Vec<i64>,
}

impl DirectMonotonicMeta {
    pub fn new(num_values: i64, block_shift: i32) -> Self {
        let mut num_blocks = num_values >> block_shift;
        if (num_blocks << block_shift) < num_values {
            num_blocks += 1;
        }

        DirectMonotonicMeta {
            num_values,
            block_shift,
            num_blocks: num_blocks as i32,
            mins: vec![0i64; num_blocks as usize],
            avgs: vec![0f32; num_blocks as usize],
            bpvs: vec![0u8; num_blocks as usize],
            offsets: vec![0i64; num_blocks as usize],
        }
    }
}

pub struct DirectMonotonicReader;

impl DirectMonotonicReader {
    pub fn load_meta(
        meta_in: &mut IndexInput,
        num_values: i64,
        block_shift: i32,
    ) -> Result<DirectMonotonicMeta> {
        let mut meta = DirectMonotonicMeta::new(num_values, block_shift);
        for i in 0..meta.num_blocks as usize {
            meta.mins[i] = meta_in.read_long()?;
            meta.avgs[i] = f32::from_bits(meta_in.read_int()? as u32);
            meta.offsets[i] = meta_in.read_long()?;
            meta.bpvs[i] = meta_in.read_byte()?;
        }
        Ok(meta)
    }

    pub fn get_instance(
        meta: &DirectMonotonicMeta,
        data: &Arc<RandomAccessInput>,
    ) -> Result<Box<LongValues>> {
        let mut readers: Vec<Box<LongValues>> = Vec::new();
        for i in 0..meta.num_blocks as usize {
            let mut reader: Box<LongValues> = if meta.bpvs[i] == 0 {
                Box::new(EmptyLongValues)
            } else {
                DirectReader::get_instance(
                    Arc::clone(data),
                    i32::from(meta.bpvs[i]),
                    meta.offsets[i],
                )?
            };
            readers.push(reader);
        }

        Ok(Box::new(MixinMonotonicLongValues {
            readers,
            block_shift: meta.block_shift,
            mins: meta.mins.clone(),
            avgs: meta.avgs.clone(),
        }))
    }
}

pub struct MixinMonotonicLongValues {
    readers: Vec<Box<LongValues>>,
    block_shift: i32,
    mins: Vec<i64>,
    avgs: Vec<f32>,
}

impl LongValues for MixinMonotonicLongValues {
    fn get64_with_ctx(
        &self,
        ctx: LongValuesContext,
        index: i64,
    ) -> Result<(i64, LongValuesContext)> {
        // we know all readers don't require context
        let block = ((index as u64) >> self.block_shift) as usize;
        let block_index: i64 = index & ((1 << self.block_shift) - 1);
        let (delta, _) =
            LongValues::get64_with_ctx(self.readers[block].as_ref(), None, block_index)?;
        Ok((
            self.mins[block] + (self.avgs[block] * block_index as f32) as i64 + delta,
            ctx,
        ))
    }
}

impl NumericDocValues for MixinMonotonicLongValues {
    fn get_with_ctx(
        &self,
        ctx: NumericDocValuesContext,
        doc_id: DocId,
    ) -> Result<(i64, NumericDocValuesContext)> {
        LongValues::get64_with_ctx(self, ctx, i64::from(doc_id))
    }
}
