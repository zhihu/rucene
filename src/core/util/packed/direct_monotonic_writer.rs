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

use core::store::io::IndexOutput;
use core::util::packed::DirectWriter;

use error::{
    ErrorKind::{IllegalArgument, IllegalState},
    Result,
};

pub const MIN_BLOCK_SHIFT: i32 = 3;
pub const MAX_BLOCK_SHIFT: i32 = 30;

pub struct DirectMonotonicWriter<'a, O: IndexOutput> {
    meta: &'a mut O,
    data: &'a mut O,
    num_values: usize,
    base_data_pointer: i64,
    buffer: Vec<i64>,
    buffer_size: usize,
    count: usize,
    finished: bool,
    previous: i64,
}

impl<'a, O: IndexOutput> DirectMonotonicWriter<'a, O> {
    pub fn new(
        meta: &'a mut O,
        data: &'a mut O,
        num_values: i64,
        block_shift: i32,
    ) -> Result<DirectMonotonicWriter<'a, O>> {
        if block_shift < MIN_BLOCK_SHIFT || block_shift > MAX_BLOCK_SHIFT {
            bail!(IllegalArgument(format!(
                "block_shift must be in [3-30], got {}",
                block_shift
            )));
        }

        let base_data_pointer = data.file_pointer();

        Ok(DirectMonotonicWriter {
            meta,
            data,
            num_values: num_values as usize,
            base_data_pointer,
            buffer: vec![0i64; (1 << block_shift) as usize],
            buffer_size: 0,
            count: 0,
            finished: false,
            previous: i64::min_value(),
        })
    }

    pub fn add(&mut self, v: i64) -> Result<()> {
        if v < self.previous {
            bail!(IllegalArgument(format!(
                "Values do not come in order: {}, {}",
                self.previous, v
            )));
        }

        if self.buffer_size == self.buffer.len() {
            self.flush()?;
        }

        self.buffer[self.buffer_size] = v;
        self.buffer_size += 1;
        self.previous = v;
        self.count += 1;
        Ok(())
    }

    pub fn finish(&mut self) -> Result<()> {
        if self.count != self.num_values {
            bail!(IllegalState(format!(
                "Wrong number of values added, expected: {}, got: {}",
                self.num_values, self.count
            )));
        }

        if self.finished {
            bail!(IllegalState("#finish has been called already".into()));
        }

        if self.buffer_size > 0 {
            self.flush()?;
        }

        self.finished = true;

        Ok(())
    }

    pub fn get_instance(
        meta: &'a mut O,
        data: &'a mut O,
        num_values: i64,
        block_shift: i32,
    ) -> Result<DirectMonotonicWriter<'a, O>> {
        DirectMonotonicWriter::new(meta, data, num_values, block_shift)
    }

    fn flush(&mut self) -> Result<()> {
        debug_assert!(self.buffer_size != 0);

        let avg_inc = ((self.buffer[self.buffer_size - 1] - self.buffer[0]) as f64
            / (self.buffer_size - 1).max(1) as f64) as f32;
        for i in 0..self.buffer_size {
            let expected = (avg_inc * i as f32) as i64;
            self.buffer[i] -= expected;
        }

        let mut min: i64 = self.buffer[0];
        for i in 1..self.buffer_size {
            min = min.min(self.buffer[i]);
        }

        let mut max_delta = 0;
        for i in 0..self.buffer_size {
            self.buffer[i] -= min;
            // use | will change nothing when it comes to computing required bits
            // but has the benefit of working fine with negative values too
            // (in case of overflow)
            max_delta |= self.buffer[i];
        }

        self.meta.write_long(min)?;
        self.meta.write_int(avg_inc.to_bits() as i32)?;
        self.meta
            .write_long(self.data.file_pointer() - self.base_data_pointer)?;

        if max_delta == 0 {
            self.meta.write_byte(0u8)?;
        } else {
            let bits_required = DirectWriter::<O>::unsigned_bits_required(max_delta);
            let mut writer =
                DirectWriter::get_instance(self.data, self.buffer_size as i64, bits_required)?;
            for i in 0..self.buffer_size {
                writer.add(self.buffer[i])?;
            }
            writer.finish()?;
            self.meta.write_byte(bits_required as u8)?;
        }
        self.buffer_size = 0;

        Ok(())
    }
}
