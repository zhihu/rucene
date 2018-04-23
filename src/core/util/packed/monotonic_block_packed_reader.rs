use core::index::NumericDocValues;
use core::store::IndexInput;
use core::util::packed::PackedIntsNullReader;
use core::util::packed_misc;
use core::util::DocId;
use core::util::LongValues;
use error::ErrorKind::{CorruptIndex, IllegalArgument};
use error::Result;

use std::sync::{Arc, Mutex};

/// Provides random access to a stream written with MonotonicBlockPackedWriter
pub struct MonotonicBlockPackedReader {
    block_shift: u32,
    block_mask: u32,
    value_count: u64,
    min_values: Vec<i64>,
    averages: Vec<f32>,
    sub_readers: Vec<Box<packed_misc::Reader>>,
    #[allow(dead_code)]
    sum_bpv: i64,
}

pub type MonotonicBlockPackedReaderRef = Arc<Mutex<MonotonicBlockPackedReader>>;

impl MonotonicBlockPackedReader {
    fn expected(origin: i64, average: f32, index: i32) -> i64 {
        origin + (average * index as f32) as i64
    }

    pub fn new(
        input: &mut IndexInput,
        packed_ints_version: i32,
        block_size: usize,
        value_count: u64,
        direct: bool,
    ) -> Result<MonotonicBlockPackedReader> {
        let block_shift = packed_misc::check_block_size(
            block_size,
            packed_misc::MIN_BLOCK_SIZE,
            packed_misc::MAX_BLOCK_SIZE,
        );
        let block_mask = (block_size - 1) as u32;
        let num_blocks = packed_misc::num_blocks(value_count, block_size);
        let mut min_values = vec![0_i64; num_blocks];
        let mut averages = vec![0.0_f32; num_blocks];
        let mut sub_readers: Vec<Box<packed_misc::Reader>> = Vec::new();
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
                sub_readers.push(Box::new(PackedIntsNullReader::new(block_size)));
            } else {
                let left = value_count - (i as u64) * (block_size as u64);
                let size = ::std::cmp::min(left, block_size as u64) as u32;
                if direct {
                    unimplemented!();
                } else {
                    let one_reader = packed_misc::get_reader_no_header(
                        input,
                        packed_misc::Format::Packed,
                        packed_ints_version,
                        size as usize,
                        bits_per_value,
                    )?;
                    sub_readers.push(one_reader);
                }
            }
        }
        Ok(MonotonicBlockPackedReader {
            block_shift,
            block_mask,
            value_count,
            min_values,
            averages,
            sub_readers,
            sum_bpv,
        })
    }

    /// Returns the number of values
    pub fn size(&self) -> u64 {
        self.value_count
    }
}

impl LongValues for MonotonicBlockPackedReader {
    fn get64(&self, index: i64) -> Result<i64> {
        if !(index >= 0 && index < self.value_count as i64) {
            bail!(IllegalArgument(format!("index {} out of range", index)))
        }
        let block = (index >> (self.block_shift as usize)) as usize;
        let idx = (index & (i64::from(self.block_mask))) as i32;
        let val = Self::expected(self.min_values[block], self.averages[block], idx)
            + self.sub_readers[block].get(idx as usize);
        Ok(val)
    }
}

impl NumericDocValues for MonotonicBlockPackedReader {
    fn get(&self, doc_id: DocId) -> Result<i64> {
        LongValues::get64(self, i64::from(doc_id))
    }
}
