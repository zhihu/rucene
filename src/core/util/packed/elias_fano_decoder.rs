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

use core::util::bit_util::{self, UnsignedShift, LOG2_LONG_SIZE, LONG_SIZE, LONG_SIZE_32};
use core::util::packed::EliasFanoEncoder;
use error::ErrorKind::*;
use error::Result;
use std::sync::Arc;

pub const NO_MORE_VALUES: i64 = -1;

#[derive(Debug)]
pub struct EliasFanoDecoder {
    ef_encoder: Arc<EliasFanoEncoder>,
    // EF编码的整数个数
    num_encoded: i64,
    // 当前指向第几个数的索引下标，有效值从0开始，初始值-1
    ef_index: i64,
    // high bits 当前扫过的bits计数，减去ef_index个1就是高位base
    // high bits index, 从0开始，初始值-1
    set_bit_for_index: i64,
    // EF first level index & mask value
    num_index_entries: i64,
    index_mask: i64,

    // upper longs array, current long
    cur_high_long: i64,
}

impl EliasFanoDecoder {
    pub fn new(ef_encoder: Arc<EliasFanoEncoder>) -> Self {
        Self {
            ef_encoder: ef_encoder.clone(),
            num_encoded: ef_encoder.num_encoded,
            ef_index: -1,
            set_bit_for_index: -1,
            num_index_entries: ef_encoder.num_index_entries,
            index_mask: (1i64 << ef_encoder.n_index_entry_bits as i64) - 1,
            cur_high_long: 0,
        }
    }

    pub fn refresh(&mut self) {
        *self = Self::new(self.ef_encoder.clone());
    }

    pub fn get_encoder(&self) -> Arc<EliasFanoEncoder> {
        self.ef_encoder.clone()
    }

    pub fn num_encoded(&self) -> i64 {
        self.num_encoded
    }

    /// 当前访问第几个数，从0开始
    pub fn current_index(&self) -> Result<i64> {
        if self.ef_index < 0 {
            bail!(IllegalState(format!("index before sequence")));
        }
        if self.ef_index >= self.num_encoded {
            bail!(IllegalState(format!("index after sequence")));
        }
        Ok(self.ef_index)
    }

    pub fn current_value(&self) -> i64 {
        self.combine_high_low_values(self.current_high_value(), self.current_low_value())
    }

    /// 通过数1/0的个数计算第几个base, 000/001/010/011/100等这种称作base
    pub fn current_high_value(&self) -> i64 {
        self.set_bit_for_index - self.ef_index
    }

    pub fn current_low_value(&self) -> i64 {
        assert!((self.ef_index >= 0) && (self.ef_index < self.num_encoded));
        Self::unpack_value(
            &self.ef_encoder.lower_longs,
            self.ef_encoder.num_low_bits,
            self.ef_index,
            self.ef_encoder.lower_bits_mask,
        )
    }

    fn combine_high_low_values(&self, high_value: i64, low_value: i64) -> i64 {
        (high_value << self.ef_encoder.num_low_bits as i64) | low_value
    }
    /// 等bits数组中取对应索引的值
    fn unpack_value(long_array: &Vec<i64>, num_bits: i32, pack_index: i64, bits_mask: i64) -> i64 {
        if num_bits == 0 {
            return 0;
        }
        let bit_pos = pack_index * num_bits as i64;
        let index = bit_pos.unsigned_shift(LOG2_LONG_SIZE as usize) as usize;
        let bit_pos_at_index = (bit_pos & LONG_SIZE - 1) as i64;
        let mut value = long_array[index].unsigned_shift(bit_pos_at_index as usize);
        if bit_pos_at_index + num_bits as i64 > LONG_SIZE {
            value |= long_array[index + 1] << (LONG_SIZE - bit_pos_at_index);
        }
        value & bits_mask
    }

    /// reset index before the first integer
    pub fn to_before_sequence(&mut self) {
        self.ef_index = -1;
        self.set_bit_for_index = -1;
    }

    /// 当前current high long 需要右移的位数
    fn get_current_right_shift(&self) -> i32 {
        (self.set_bit_for_index & LONG_SIZE - 1) as i32
    }

    /// ef_index & set_bit_for_index increment, low bits to next, but high bits maybe not arrived
    fn to_after_current_high_bit(&mut self) -> bool {
        self.ef_index += 1;
        if self.ef_index >= self.num_encoded {
            return false;
        }
        self.set_bit_for_index += 1;
        let high_index = self
            .set_bit_for_index
            .unsigned_shift(LOG2_LONG_SIZE as usize) as usize;
        self.cur_high_long = self.ef_encoder.upper_longs[high_index]
            .unsigned_shift(self.get_current_right_shift() as usize);
        true
    }

    /// move to next upper long，advance set_bit_for_index
    fn to_next_high_long(&mut self) {
        self.set_bit_for_index += LONG_SIZE - (self.set_bit_for_index & LONG_SIZE - 1);
        let high_index = self
            .set_bit_for_index
            .unsigned_shift(LOG2_LONG_SIZE as usize) as usize;
        self.cur_high_long = self.ef_encoder.upper_longs[high_index];
    }
    /// advance to next 1 bit in upper longs
    /// must be used after to_after_current_high_bit
    fn to_next_high_value(&mut self) {
        while self.cur_high_long == 0 {
            self.to_next_high_long();
        }
        self.set_bit_for_index += self.cur_high_long.trailing_zeros() as i64;
    }

    fn next_high_value(&mut self) -> i64 {
        self.to_next_high_value();
        self.current_high_value()
    }
    /// ef_index + 1 get the next low bits
    /// set_bit_for_index advance to next 1 bit, then get high bits
    /// by set_bit_for_index - ef_index
    pub fn next_value(&mut self) -> i64 {
        if !self.to_after_current_high_bit() {
            return NO_MORE_VALUES;
        }
        let high_value = self.next_high_value();
        return self.combine_high_low_values(high_value, self.current_low_value());
    }
    /// advance the index to the [index]th integer
    pub fn advance_to_index(&mut self, index: i64) -> bool {
        assert!(index > self.ef_index);
        if index >= self.num_encoded {
            self.ef_index = self.num_encoded;
            return false;
        }
        if !self.to_after_current_high_bit() {
            assert!(false);
        }
        let mut cur_set_bits = self.cur_high_long.count_ones() as i32;
        while (self.ef_index + cur_set_bits as i64) < index {
            self.ef_index += cur_set_bits as i64;
            self.to_next_high_long();
            cur_set_bits = self.cur_high_long.count_ones() as i32;
        }
        while self.ef_index < index {
            if !self.to_after_current_high_bit() {
                assert!(false);
            }
            self.to_next_high_value();
        }
        true
    }

    pub fn advance_to_value(&mut self, target: i64) -> i64 {
        // equals to to_after_current_high_bit
        self.ef_index += 1;
        if self.ef_index >= self.num_encoded {
            return NO_MORE_VALUES;
        }
        self.set_bit_for_index += 1;

        let mut high_index = self
            .set_bit_for_index
            .unsigned_shift(LOG2_LONG_SIZE as usize) as usize;
        let mut upper_long = self.ef_encoder.upper_longs[high_index];
        self.cur_high_long =
            upper_long.unsigned_shift((self.set_bit_for_index & LONG_SIZE - 1) as usize);

        let high_target = target.unsigned_shift(self.ef_encoder.num_low_bits as usize);
        let mut index_entry_index = (high_target / self.ef_encoder.index_interval) - 1;
        // 跳过部分数据
        if index_entry_index >= 0 {
            if index_entry_index >= self.num_index_entries {
                index_entry_index = self.num_index_entries - 1;
            }
            let index_high_value = (index_entry_index + 1) * self.ef_encoder.index_interval;
            assert!(index_high_value <= high_target);
            if index_high_value > (self.set_bit_for_index - self.ef_index) {
                self.set_bit_for_index = Self::unpack_value(
                    &self.ef_encoder.upper_zero_bit_position_index,
                    self.ef_encoder.n_index_entry_bits,
                    index_entry_index,
                    self.index_mask,
                );
                self.ef_index = self.set_bit_for_index - index_high_value;
                high_index = self
                    .set_bit_for_index
                    .unsigned_shift(LOG2_LONG_SIZE as usize) as usize;
                upper_long = self.ef_encoder.upper_longs[high_index];
                self.cur_high_long =
                    upper_long.unsigned_shift((self.set_bit_for_index & LONG_SIZE - 1) as usize);
            }
            assert!(self.ef_index < self.num_encoded);
        }

        let mut cur_set_bits = self.cur_high_long.count_ones() as i32;
        let mut cur_clear_bits =
            LONG_SIZE_32 - cur_set_bits - (self.set_bit_for_index & LONG_SIZE - 1) as i32;

        // 定位到最终upper long
        while (self.set_bit_for_index - self.ef_index + cur_clear_bits as i64) < high_target {
            self.ef_index += cur_set_bits as i64;
            if self.ef_index >= self.num_encoded {
                return NO_MORE_VALUES;
            }
            self.set_bit_for_index += LONG_SIZE - (self.set_bit_for_index & LONG_SIZE - 1);
            assert_eq!(
                high_index + 1,
                (self
                    .set_bit_for_index
                    .unsigned_shift(LOG2_LONG_SIZE as usize) as usize)
            );
            high_index += 1;
            // upper_long += self.ef_encoder.upper_longs[high_index];
            upper_long = self.ef_encoder.upper_longs[high_index];
            self.cur_high_long = upper_long;
            cur_set_bits = self.cur_high_long.count_ones() as i32;
            cur_clear_bits = LONG_SIZE_32 - cur_set_bits;
        }

        while self.cur_high_long == 0 {
            self.set_bit_for_index += LONG_SIZE - (self.set_bit_for_index & LONG_SIZE - 1);
            assert_eq!(
                high_index + 1,
                (self
                    .set_bit_for_index
                    .unsigned_shift(LOG2_LONG_SIZE as usize) as usize)
            );
            high_index += 1;
            // if high_index >= self.ef_encoder.upper_longs.len() {}
            upper_long = self.ef_encoder.upper_longs[high_index];
            self.cur_high_long = upper_long;
        }

        let rank = (high_target - (self.set_bit_for_index - self.ef_index)) as i32;
        assert!(rank <= LONG_SIZE_32);
        if rank >= 1 {
            let inv_cur_high_long = !self.cur_high_long;
            let clear_bit_for_value = if rank <= 8 {
                bit_util::select_naive(inv_cur_high_long, rank)
            } else {
                bit_util::select(inv_cur_high_long, rank)
            };
            assert!(clear_bit_for_value >= 0);
            assert!(clear_bit_for_value <= LONG_SIZE_32 - 1);
            self.set_bit_for_index += (clear_bit_for_value + 1) as i64;
            let one_bits_before_clear_bit = clear_bit_for_value - rank + 1;
            self.ef_index += one_bits_before_clear_bit as i64;
            if self.ef_index >= self.num_encoded {
                return NO_MORE_VALUES;
            }

            if (self.set_bit_for_index & LONG_SIZE - 1) == 0 {
                assert_eq!(
                    high_index + 1,
                    self.set_bit_for_index
                        .unsigned_shift(LOG2_LONG_SIZE as usize) as usize
                );
                high_index += 1;
                upper_long = self.ef_encoder.upper_longs[high_index];
                self.cur_high_long = upper_long;
            } else {
                assert_eq!(
                    high_index,
                    self.set_bit_for_index
                        .unsigned_shift(LOG2_LONG_SIZE as usize) as usize
                );
                self.cur_high_long =
                    upper_long.unsigned_shift((self.set_bit_for_index & LONG_SIZE - 1) as usize);
            }

            while self.cur_high_long == 0 {
                self.set_bit_for_index += LONG_SIZE - (self.set_bit_for_index & LONG_SIZE - 1);
                assert_eq!(
                    high_index + 1,
                    self.set_bit_for_index
                        .unsigned_shift(LOG2_LONG_SIZE as usize) as usize
                );
                high_index += 1;
                upper_long = self.ef_encoder.upper_longs[high_index];
                self.cur_high_long = upper_long;
            }
        }
        // 当前定位到的current_value
        self.set_bit_for_index += self.cur_high_long.trailing_zeros() as i64;
        assert!(self.set_bit_for_index - self.ef_index >= high_target);
        let mut current_value = self.combine_high_low_values(
            self.set_bit_for_index - self.ef_index,
            self.current_low_value(),
        );
        // 线性扫描下一个值
        while current_value < target {
            current_value = self.next_value();
            if current_value == NO_MORE_VALUES {
                return NO_MORE_VALUES;
            }
        }
        current_value
    }

    pub fn to_after_sequence(&mut self) {
        self.ef_index = self.num_encoded;
        self.set_bit_for_index = self
            .ef_encoder
            .last_encoded
            .unsigned_shift(self.ef_encoder.num_low_bits as usize)
            + self.num_encoded;
    }
    /// reversed operator for next_value
    pub fn previous_value(&mut self) -> i64 {
        if !self.to_before_current_high_bit() {
            return NO_MORE_VALUES;
        }
        let high_value = self.previous_high_value();
        self.combine_high_low_values(high_value, self.current_low_value())
    }
    /// reversed operator for advance_to_value
    pub fn back_to_value(&mut self, target: i64) -> i64 {
        if !self.to_before_current_high_bit() {
            return NO_MORE_VALUES;
        }
        let high_target = target.unsigned_shift(self.ef_encoder.num_low_bits as usize);
        let high_value = self.back_to_high_value(high_target);
        if high_value == NO_MORE_VALUES {
            return NO_MORE_VALUES;
        }
        let mut current_value = self.combine_high_low_values(high_value, self.current_low_value());
        while current_value > target {
            current_value = self.previous_value();
            if current_value == NO_MORE_VALUES {
                return NO_MORE_VALUES;
            }
        }
        current_value
    }

    fn get_current_left_shift(&self) -> i32 {
        LONG_SIZE_32 - 1 - (self.set_bit_for_index & LONG_SIZE - 1) as i32
    }

    fn to_before_current_high_bit(&mut self) -> bool {
        self.ef_index -= 1;
        if self.ef_index < 0 {
            return false;
        }
        self.set_bit_for_index -= 1;
        let high_index = self
            .set_bit_for_index
            .unsigned_shift(LOG2_LONG_SIZE as usize) as usize;
        self.cur_high_long =
            self.ef_encoder.upper_longs[high_index] << self.get_current_left_shift() as i64;
        true
    }

    fn to_previous_high_long(&mut self) {
        self.set_bit_for_index -= (self.set_bit_for_index & LONG_SIZE - 1) + 1;
        let high_index = self
            .set_bit_for_index
            .unsigned_shift(LOG2_LONG_SIZE as usize) as usize;
        self.cur_high_long = self.ef_encoder.upper_longs[high_index];
    }

    fn previous_high_value(&mut self) -> i64 {
        while self.cur_high_long == 0 {
            self.to_previous_high_long();
        }
        self.set_bit_for_index -= self.cur_high_long.leading_zeros() as i64;
        self.current_high_value()
    }

    fn back_to_high_value(&mut self, high_target: i64) -> i64 {
        let mut cur_set_bits = self.cur_high_long.count_ones() as i32;
        let mut cur_clear_bits = LONG_SIZE_32 - cur_set_bits - self.get_current_left_shift();
        while self.current_high_value() - cur_clear_bits as i64 > high_target {
            self.ef_index -= cur_set_bits as i64;
            if self.ef_index < 0 {
                return NO_MORE_VALUES;
            }
            self.to_previous_high_long();
            cur_set_bits = self.cur_high_long.count_ones() as i32;
            cur_clear_bits = LONG_SIZE_32 - cur_set_bits;
        }
        let mut high_value = self.previous_high_value();
        while high_value > high_target {
            if !self.to_before_current_high_bit() {
                return NO_MORE_VALUES;
            }
            high_value = self.previous_high_value();
        }
        high_value
    }
}
