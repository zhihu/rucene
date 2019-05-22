use core::index::{NumericDocValues, NumericDocValuesContext};
use core::store::IndexInput;
use core::store::RandomAccessInput;
use core::util::{packed::DirectReader, DocId, LongValues, LongValuesContext};
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
        data: &Arc<RandomAccessInput>,
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
            readers,
            block_shift: meta.block_shift,
            mins: Arc::clone(&meta.mins),
            avgs: Arc::clone(&meta.avgs),
        })
    }
}

pub struct MixinMonotonicLongValues {
    readers: Vec<Option<DirectPackedReader>>,
    block_shift: i32,
    mins: Arc<Vec<i64>>,
    avgs: Arc<Vec<f32>>,
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
        let delta = if let Some(ref reader) = self.readers[block] {
            let (d, _) = LongValues::get64_with_ctx(reader, None, block_index)?;
            d
        } else {
            0
        };
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
