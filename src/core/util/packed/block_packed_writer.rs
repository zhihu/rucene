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

use core::store::io::DataOutput;
use core::util::bit_util::{BitsRequired, UnsignedShift, ZigZagEncoding};
use core::util::packed::packed_misc::{
    check_block_size, get_encoder, max_value, Format, PackedIntEncoder, PackedIntMeta, BPV_SHIFT,
    MAX_BLOCK_SIZE, MIN_BLOCK_SIZE, MIN_VALUE_EQUALS_0, VERSION_CURRENT,
};

use error::{ErrorKind::IllegalState, Result};

pub struct BaseBlockPackedWriter {
    pub values: Vec<i64>,
    pub blocks: Vec<u8>,
    pub off: usize,
    pub ord: i64,
    pub finished: bool,
}

impl BaseBlockPackedWriter {
    pub fn new(block_size: usize) -> BaseBlockPackedWriter {
        check_block_size(block_size, MIN_BLOCK_SIZE, MAX_BLOCK_SIZE);

        BaseBlockPackedWriter {
            values: vec![0i64; block_size],
            blocks: vec![0u8; block_size],
            off: 0,
            ord: 0,
            finished: false,
        }
    }

    // same as DataOutput.write_vlong but accepts negative values
    pub fn write_vlong(out: &mut impl DataOutput, mut i: i64) -> Result<()> {
        let mut k = 0;
        while (i & (!0x7Fi64)) != 0i64 && k < 8 {
            k += 1;
            out.write_byte(((i & 0x7Fi64) | 0x80i64) as u8)?;
            i = i.unsigned_shift(7);
        }

        out.write_byte(i as u8)?;

        Ok(())
    }

    pub fn ord(&self) -> i64 {
        self.ord
    }

    pub fn reset(&mut self) {
        self.off = 0;
        self.ord = 0;
        self.finished = false;
    }

    pub fn check_not_finished(&self) -> Result<()> {
        if self.finished {
            bail!(IllegalState("Already finished".into()));
        }

        Ok(())
    }

    pub fn write_values(&mut self, bits_required: i32, out: &mut impl DataOutput) -> Result<()> {
        let encoder = get_encoder(Format::Packed, VERSION_CURRENT, bits_required)?;
        let iterations = self.values.len() / encoder.byte_value_count();
        let block_size = encoder.byte_block_count() * iterations;

        if self.blocks.len() < block_size {
            self.blocks.resize(block_size, 0);
        }

        if self.off < self.values.len() {
            for i in self.off..self.values.len() {
                self.values[i] = 0i64;
            }
        }

        encoder.encode_long_to_byte(&self.values, &mut self.blocks, iterations);
        let block_count =
            Format::Packed.byte_count(VERSION_CURRENT, self.off as i32, bits_required);
        out.write_bytes(&self.blocks, 0, block_count as usize)
    }
}

pub trait AbstractBlockPackedWriter {
    fn add(&mut self, l: i64, out: &mut impl DataOutput) -> Result<()>;

    /// Flush all buffered data to disk. This instance is not usable anymore
    ///  after this method has been called until {@link #reset(DataOutput)} has
    ///  been called. */
    fn finish(&mut self, out: &mut impl DataOutput) -> Result<()>;

    fn flush(&mut self, out: &mut impl DataOutput) -> Result<()>;

    fn reset(&mut self);
}

pub struct BlockPackedWriter {
    base_writer: BaseBlockPackedWriter,
}

impl BlockPackedWriter {
    pub fn new(block_size: usize) -> BlockPackedWriter {
        BlockPackedWriter {
            base_writer: BaseBlockPackedWriter::new(block_size),
        }
    }
}

impl AbstractBlockPackedWriter for BlockPackedWriter {
    fn add(&mut self, l: i64, out: &mut impl DataOutput) -> Result<()> {
        self.base_writer.check_not_finished()?;
        if self.base_writer.off == self.base_writer.values.len() {
            self.flush(out)?;
        }
        self.base_writer.values[self.base_writer.off] = l;
        self.base_writer.off += 1;
        self.base_writer.ord += 1;
        Ok(())
    }

    fn finish(&mut self, out: &mut impl DataOutput) -> Result<()> {
        self.base_writer.check_not_finished()?;
        if self.base_writer.off > 0 {
            self.flush(out)?;
        }

        self.base_writer.finished = true;
        Ok(())
    }

    fn flush(&mut self, out: &mut impl DataOutput) -> Result<()> {
        debug_assert!(self.base_writer.off > 0);
        let mut min: i64 = i64::max_value();
        let mut max: i64 = i64::min_value();
        for i in 0..self.base_writer.off {
            min = min.min(self.base_writer.values[i]);
            max = max.max(self.base_writer.values[i]);
        }

        let delta = max - min;
        let bits_required = if delta == 0 {
            0
        } else {
            delta.bits_required() as i32
        };
        if bits_required == 64 {
            // no need to delta-encode
            min = 0;
        } else if min > 0 {
            // make min as small as possible so that write_vlong requires fewer bytes
            min = 0i64.max(max - max_value(bits_required));
        }

        let mut token = bits_required << BPV_SHIFT;
        if min == 0 {
            token |= MIN_VALUE_EQUALS_0;
        }

        out.write_byte(token as u8)?;
        if min != 0 {
            BaseBlockPackedWriter::write_vlong(out, min.encode() - 1)?;
        }

        if bits_required > 0 {
            if min != 0 {
                for i in 0..self.base_writer.off {
                    self.base_writer.values[i] -= min;
                }
            }
            self.base_writer.write_values(bits_required, out)?;
        }

        self.base_writer.off = 0;
        Ok(())
    }

    fn reset(&mut self) {
        self.base_writer.reset();
    }
}
