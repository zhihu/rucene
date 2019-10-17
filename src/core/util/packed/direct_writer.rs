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
use core::util::packed::{
    bulk_operation_of, max_value, BulkOperation, BulkOperationEnum, Format, PackedIntEncoder,
    PackedIntMeta, DEFAULT_BUFFER_SIZE, VERSION_CURRENT,
};

use core::util::bit_util::{BitsRequired, UnsignedShift};
use error::Result;

pub const SUPPORTED_BITS_PER_VALUE: &[i32] = &[1, 2, 4, 8, 12, 16, 20, 24, 28, 32, 40, 48, 56, 64];

pub struct DirectWriter<'a, O: IndexOutput> {
    bits_per_value: i32,
    num_values: usize,
    output: &'a mut O,
    count: usize,
    off: usize,
    finished: bool,
    next_blocks: Vec<u8>,
    next_values: Vec<i64>,
    encoder: BulkOperationEnum,
    iterations: usize,
}

impl<'a, O: IndexOutput> DirectWriter<'a, O> {
    pub fn new(output: &'a mut O, num_values: i64, bits_per_value: i32) -> DirectWriter<'a, O> {
        let encoder = bulk_operation_of(Format::Packed, bits_per_value as usize);
        let iterations = encoder.compute_iterations(
            num_values.min(i32::max_value() as i64) as i32,
            DEFAULT_BUFFER_SIZE,
        ) as usize;
        DirectWriter {
            bits_per_value,
            num_values: num_values as usize,
            output,
            count: 0,
            finished: false,
            off: 0,
            next_blocks: vec![0u8; iterations * encoder.byte_block_count()],
            next_values: vec![0i64; iterations * encoder.byte_value_count()],
            encoder,
            iterations,
        }
    }

    pub fn add(&mut self, l: i64) -> Result<()> {
        debug_assert!(self.bits_per_value == 64 || (l >= 0 && l <= max_value(self.bits_per_value)));
        debug_assert!(!self.finished);

        if self.count >= self.num_values {
            bail!(
                "Writing past end of stream, num values: {}, current count: {}",
                self.num_values,
                self.count
            );
        }

        self.next_values[self.off] = l;
        self.off += 1;
        if self.off == self.next_values.len() {
            self.flush()?;
        }

        self.count += 1;
        Ok(())
    }

    pub fn finish(&mut self) -> Result<()> {
        if self.count != self.num_values {
            bail!(
                "Wrong number of values added, expected: {}, got: {}",
                self.num_values,
                self.count
            );
        }

        debug_assert!(!self.finished);
        self.flush()?;
        // pad for fast io: we actually only need this for certain BPV, but its just 3 bytes...
        for _ in 0..3 {
            self.output.write_byte(0u8)?;
        }

        self.finished = true;

        Ok(())
    }

    // Returns an instance suitable for encoding {@code numValues} using {@code bitsPerValue}
    pub fn get_instance(
        output: &'a mut O,
        num_values: i64,
        bits_per_value: i32,
    ) -> Result<DirectWriter<'a, O>> {
        if DirectWriter::<O>::binary_search0(
            &SUPPORTED_BITS_PER_VALUE,
            0,
            SUPPORTED_BITS_PER_VALUE.len() as i32,
            bits_per_value,
        ) < 0
        {
            bail!(
                "Unsupported bitsPerValue {}. Did you use bitsRequired?",
                bits_per_value
            )
        } else {
            Ok(Self::new(output, num_values, bits_per_value))
        }
    }

    // Returns how many bits are required to hold values up to and including maxValue
    pub fn bits_required(max_value: i64) -> i32 {
        debug_assert!(max_value >= 0);
        DirectWriter::<O>::round_bits(max_value.bits_required() as i32)
    }

    // Returns how many bits are required to hold values up to and including maxValue, interpreted
    // as an unsigned value.
    pub fn unsigned_bits_required(max_value: i64) -> i32 {
        DirectWriter::<O>::round_bits(max_value.bits_required() as i32)
    }

    // Round a number of bits per value to the next amount of bits per value that is supported by
    // this writer.
    pub fn round_bits(bit_required: i32) -> i32 {
        let index = DirectWriter::<O>::binary_search0(
            &SUPPORTED_BITS_PER_VALUE,
            0,
            SUPPORTED_BITS_PER_VALUE.len() as i32,
            bit_required,
        );
        if index < 0 {
            SUPPORTED_BITS_PER_VALUE[(-index - 1) as usize]
        } else {
            bit_required
        }
    }

    fn flush(&mut self) -> Result<()> {
        self.encoder.encode_long_to_byte(
            self.next_values.as_ref(),
            self.next_blocks.as_mut(),
            self.iterations,
        );
        let block_count =
            Format::Packed.byte_count(VERSION_CURRENT, self.off as i32, self.bits_per_value);
        self.output
            .write_bytes(self.next_blocks.as_ref(), 0, block_count as usize)?;
        for i in 0..self.next_values.len() {
            self.next_values[i] = 0;
        }
        self.off = 0;
        Ok(())
    }

    fn binary_search0(a: &[i32], from: i32, to: i32, key: i32) -> i32 {
        let mut low = from;
        let mut high = to - 1;

        while low <= high {
            let mid = (low + high).unsigned_shift(1);
            let mid_val = a[mid as usize];

            if mid_val < key {
                low = mid + 1;
            } else if mid_val > key {
                high = mid - 1;
            } else {
                return mid;
            }
        }

        -(low + 1)
    }
}
