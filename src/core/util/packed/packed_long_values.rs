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
use core::util::bit_util::BitsRequired;
use core::util::packed::MonotonicBlockPackedReader;
use core::util::packed::{
    check_block_size, get_mutable_by_ratio, Mutable, MutableEnum, PackedIntsNullMutable, Reader,
};
use core::util::{DocId, LongValues, ReusableIterator};

use error::Result;
use std::mem;

pub const DEFAULT_PAGE_SIZE: usize = 1024;
pub const MIN_PAGE_SIZE: usize = 64;
// More than 1M doesn't really makes sense with these appending buffers
// since their goal is to try to have small numbers of bits per value
pub const MAX_PAGE_SIZE: usize = 1 << 20;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize)]
pub enum PackedLongValuesBuilderType {
    Default,
    Delta,
    Monotonic,
}

/// Utility class to compress integers into a `LongValues` instance.
pub struct PackedLongValues {
    page_shift: usize,
    page_mask: usize,
    size: i64,
    values: Vec<MutableEnum>,
    mins: Vec<i64>,
    averages: Vec<f32>,
    builder_type: PackedLongValuesBuilderType,
}

impl PackedLongValues {
    pub fn new(
        page_shift: usize,
        page_mask: usize,
        size: i64,
        values: Vec<MutableEnum>,
        mins: Vec<i64>,
        averages: Vec<f32>,
        builder_type: PackedLongValuesBuilderType,
    ) -> Self {
        Self {
            page_shift,
            page_mask,
            size,
            values,
            mins,
            averages,
            builder_type,
        }
    }

    /// Get the number of values in this array.
    pub fn size(&self) -> i64 {
        self.size
    }

    pub fn ram_bytes_used_estimate(&self) -> usize {
        let mut size = self.values.capacity() * 16; // fat pointer
        match self.builder_type {
            PackedLongValuesBuilderType::Monotonic => {
                size += self.mins.capacity() * 8;
                size += self.averages.capacity() * 4;
            }
            PackedLongValuesBuilderType::Delta => {
                size += self.mins.capacity() * 8;
            }
            PackedLongValuesBuilderType::Default => {}
        }
        size
    }

    fn decode_block(&self, block: usize, dest: &mut Vec<i64>) -> i32 {
        let vals = &self.values[block];
        let size = vals.size();
        let mut k = 0usize;
        while k < size {
            k += vals.bulk_get(k, &mut dest[k..size], size - k);
        }

        match self.builder_type {
            PackedLongValuesBuilderType::Delta | PackedLongValuesBuilderType::Monotonic => {
                let min = self.mins[block];
                k = 0;
                while k < size {
                    dest[k] = dest[k].wrapping_add(min);
                    k += 1;
                }
            }
            _ => {}
        }

        if let PackedLongValuesBuilderType::Monotonic = self.builder_type {
            let average = self.averages[block];
            k = 0;
            while k < size {
                dest[k] = dest[k]
                    .wrapping_add(MonotonicBlockPackedReader::expected(0, average, k as i32));
                k += 1;
            }
        }

        size as i32
    }

    fn get_by_block(&self, block: usize, element: usize) -> i64 {
        let mut v = self.values[block].get(element);
        match self.builder_type {
            PackedLongValuesBuilderType::Delta => v += self.mins[block],
            PackedLongValuesBuilderType::Monotonic => {
                v += MonotonicBlockPackedReader::expected(
                    self.mins[block],
                    self.averages[block],
                    element as i32,
                )
            }
            _ => {}
        }

        v
    }

    pub fn iterator(&self) -> LongValuesIterator {
        LongValuesIterator::new(self)
    }
}

impl LongValues for PackedLongValues {
    fn get64(&self, index: i64) -> Result<i64> {
        debug_assert!(index >= 0 && index < self.size());
        let block = (index >> self.page_shift as i64) as usize;
        let element = (index & self.page_mask as i64) as usize;

        Ok(self.get_by_block(block, element))
    }
}

impl NumericDocValues for PackedLongValues {
    fn get(&self, doc_id: DocId) -> Result<i64> {
        self.get64(i64::from(doc_id))
    }
}

pub struct LongValuesIterator<'a> {
    current_values: Vec<i64>,
    v_off: usize,
    p_off: usize,
    // number of entries of the current page
    current_count: usize,
    packed_values: &'a PackedLongValues,
}

impl<'a> LongValuesIterator<'a> {
    pub fn new(packed_values: &'a PackedLongValues) -> LongValuesIterator<'a> {
        let mut iter = LongValuesIterator {
            current_values: vec![0i64; packed_values.page_mask as usize + 1],
            v_off: 0,
            p_off: 0,
            current_count: 0,
            packed_values,
        };

        iter.fill_block();

        iter
    }

    fn fill_block(&mut self) {
        if self.v_off == self.packed_values.values.len() {
            self.current_count = 0;
        } else {
            self.current_count = self
                .packed_values
                .decode_block(self.v_off, self.current_values.as_mut())
                as usize;
            debug_assert!(self.current_count > 0);
        }
    }

    pub fn has_next(&self) -> bool {
        self.p_off < self.current_count
    }
}

impl<'a> Iterator for LongValuesIterator<'a> {
    type Item = i64;

    fn next(&mut self) -> Option<i64> {
        if self.has_next() {
            let result = self.current_values[self.p_off];
            self.p_off += 1;

            if self.p_off == self.current_count {
                self.v_off += 1;
                self.p_off = 0;
                self.fill_block();
            }
            Some(result)
        } else {
            None
        }
    }
}

impl<'a> ReusableIterator for LongValuesIterator<'a> {
    fn reset(&mut self) {
        self.v_off = 0;
        self.p_off = 0;
        self.current_count = 0;
        self.fill_block();
    }
}

pub const INITIAL_PAGE_COUNT: usize = 16;

pub struct PackedLongValuesBuilder {
    page_shift: usize,
    page_mask: usize,
    acceptable_over_head_ratio: f32,
    size: i64,
    values: Vec<MutableEnum>,
    values_off: usize,
    pending: Vec<i64>,
    pending_off: usize,
    mins: Vec<i64>,
    averages: Vec<f32>,
    builder_type: PackedLongValuesBuilderType,
    built: bool,
}

impl PackedLongValuesBuilder {
    pub fn new(
        page_size: usize,
        acceptable_over_head_ratio: f32,
        builder_type: PackedLongValuesBuilderType,
    ) -> PackedLongValuesBuilder {
        PackedLongValuesBuilder {
            page_shift: check_block_size(page_size, MIN_PAGE_SIZE, MAX_PAGE_SIZE),
            page_mask: page_size - 1,
            acceptable_over_head_ratio,
            size: 0,
            values: vec![],
            values_off: 0,
            pending: vec![0i64; page_size],
            pending_off: 0,
            mins: vec![],
            averages: vec![],
            builder_type,
            built: false,
        }
    }

    pub fn build(&mut self) -> PackedLongValues {
        assert!(!self.built);
        self.built = true;
        self.finish();
        self.pending = vec![];

        match self.builder_type {
            PackedLongValuesBuilderType::Delta => {
                debug_assert_eq!(self.values.len(), self.mins.len())
            }
            PackedLongValuesBuilderType::Monotonic => {
                debug_assert_eq!(self.values.len(), self.averages.len())
            }
            _ => {}
        }

        PackedLongValues::new(
            self.page_shift,
            self.page_mask,
            self.size,
            mem::replace(&mut self.values, vec![]),
            mem::replace(&mut self.mins, vec![]),
            mem::replace(&mut self.averages, vec![]),
            self.builder_type,
        )
    }

    pub fn add(&mut self, l: i64) {
        if self.pending.is_empty() {
            panic!("Cannot be reused after build()")
        }

        if self.pending_off == self.pending.capacity() {
            self.pack();
        }
        self.pending[self.pending_off] = l;
        self.pending_off += 1;
        self.size += 1;
    }

    fn finish(&mut self) {
        if self.pending_off > 0 {
            self.pack();
        }
    }

    fn pack(&mut self) {
        debug_assert!(self.pending_off > 0);

        let average_ori = if self.pending_off == 1 {
            0f32
        } else {
            self.pending[self.pending_off - 1].wrapping_sub(self.pending[0]) as f32
                / (self.pending_off - 1) as f32
        };

        if let PackedLongValuesBuilderType::Monotonic = self.builder_type {
            for i in 0..self.pending_off {
                self.pending[i] = self.pending[i].wrapping_sub(
                    MonotonicBlockPackedReader::expected(0, average_ori, i as i32),
                );
            }
        }

        let mut min_value_ori = self.pending[0];
        let mut max_value_ori = self.pending[0];
        for i in 1..self.pending_off {
            min_value_ori = min_value_ori.min(self.pending[i]);
            max_value_ori = max_value_ori.max(self.pending[i]);
        }

        match self.builder_type {
            PackedLongValuesBuilderType::Delta | PackedLongValuesBuilderType::Monotonic => {
                for i in 0..self.pending_off {
                    self.pending[i] = self.pending[i].wrapping_sub(min_value_ori);
                }
            }
            _ => {}
        }

        let mut min_value = self.pending[0];
        let mut max_value = self.pending[0];

        for i in 1..self.pending_off {
            min_value = min_value.min(self.pending[i]);
            max_value = max_value.max(self.pending[i]);
        }

        // build a new packed reader
        if min_value == 0 && max_value == 0 {
            self.values
                .push(MutableEnum::PackedIntsNull(PackedIntsNullMutable::new(
                    self.pending_off,
                )));
        } else {
            let bits_required = if min_value < 0 {
                64
            } else {
                max_value.bits_required() as i32
            };

            let mut mutable = get_mutable_by_ratio(
                self.pending_off,
                bits_required,
                self.acceptable_over_head_ratio,
            );

            let mut i = 0;
            while i < self.pending_off {
                i += mutable.bulk_set(i, &self.pending, i, self.pending_off - i);
            }

            self.values.push(mutable);
        }

        match self.builder_type {
            PackedLongValuesBuilderType::Delta | PackedLongValuesBuilderType::Monotonic => {
                self.mins.push(min_value_ori);
            }
            _ => {}
        }

        if let PackedLongValuesBuilderType::Monotonic = self.builder_type {
            self.averages.push(average_ori);
        }

        self.values_off += 1;
        self.pending_off = 0;
    }

    /// Get the number of values in this array.
    pub fn size(&self) -> i64 {
        self.size
    }

    pub fn ram_bytes_used_estimate(&self) -> usize {
        let mut size = self.values.capacity() * 16; // fat pointer
        match self.builder_type {
            PackedLongValuesBuilderType::Monotonic => {
                size += self.mins.capacity() * 8;
                size += self.averages.capacity() * 4;
            }
            PackedLongValuesBuilderType::Delta => {
                size += self.mins.capacity() * 8;
            }
            PackedLongValuesBuilderType::Default => {}
        }
        size
    }
}
