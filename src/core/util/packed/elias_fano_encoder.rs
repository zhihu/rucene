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

use core::codec::postings::{EncodeType, ForUtil};
use core::store::io::{IndexInput, IndexOutput};
use core::util::bit_util::*;
use error::ErrorKind::*;
use error::Result;
use std::ptr::{slice_from_raw_parts, slice_from_raw_parts_mut};

/// The default index interval for zero upper bits.
pub const DEFAULT_INDEX_INTERVAL: i64 = 256;

#[derive(Debug)]
pub struct EliasFanoEncoder {
    pub num_values: i64,
    upper_bound: i64,
    pub num_low_bits: i32,
    pub lower_bits_mask: i64,
    pub upper_longs: Vec<i64>,
    pub lower_longs: Vec<i64>,

    pub num_encoded: i64,
    pub last_encoded: i64,

    pub num_index_entries: i64,
    pub index_interval: i64,
    // index entry bits num
    pub n_index_entry_bits: i32,
    /// upper_zero_bit_position_index[i] (filled using packValue) will contain the bit position
    ///  just after the zero bit ((i+1) * index_interval) in the upper bits.
    pub upper_zero_bit_position_index: Vec<i64>,
    current_entry_index: i64, // also indicates how many entries in the index are valid.
}

impl EliasFanoEncoder {
    pub fn new(num_values: i64, upper_bound: i64, index_interval: i64) -> Result<Self> {
        if num_values < 0 {
            bail!(IllegalArgument(format!(
                "num_values should not be negative: {}",
                num_values
            )));
        }
        if num_values > 0 && upper_bound < 0 {
            bail!(IllegalArgument(format!(
                "upper_bound should not be negative: {} when num_values > 0",
                upper_bound
            )));
        }
        let upper_bound = if num_values > 0 { upper_bound } else { -1 };

        // the number of lower bits
        let mut num_low_bits = 0;
        if num_values > 0 {
            let low_bits_fac = upper_bound / num_values;
            if low_bits_fac > 0 {
                // different from lucene version
                // num_low_bits = LONG_SIZE_32 - number_of_leading_zeros(low_bits_fac);
                // floor(2_log(upper_bound / num_values)), default
                // ceil(2_log(upper_bound / num_values - 1))
                num_low_bits = LONG_SIZE_32 - 1 - low_bits_fac.leading_zeros() as i32;
            }
        }
        let lower_bits_mask =
            i64::max_value().unsigned_shift((LONG_SIZE_32 - 1 - num_low_bits) as usize);
        let num_longs_for_low_bits = Self::num_longs_for_bits(num_values * num_low_bits as i64);
        if num_longs_for_low_bits > i32::max_value().into() {
            bail!(IllegalArgument(format!(
                "num_longs_for_low_bits too large to index a long array: {}",
                num_longs_for_low_bits
            )));
        }
        let lower_longs = vec![0; num_longs_for_low_bits as usize];

        // high bits
        let mut num_high_bits_clear = if upper_bound > 0 { upper_bound } else { 0 };
        num_high_bits_clear = num_high_bits_clear.unsigned_shift(num_low_bits as usize);
        assert!(num_high_bits_clear <= 2 * num_values);
        let num_high_bits_set = num_values;

        // Todo: 感觉这里少计算了
        let num_longs_for_high_bits =
            Self::num_longs_for_bits(num_high_bits_clear + num_high_bits_set);
        if num_longs_for_high_bits > i32::max_value() as i64 {
            bail!(IllegalArgument(format!(
                "num_longs_for_high_bits too large to index a long array: {}",
                num_longs_for_high_bits
            )));
        }
        let upper_longs = vec![0; num_longs_for_high_bits as usize];
        if index_interval < 2 {
            bail!(IllegalArgument(format!(
                "index_interval should at least 2: {}",
                index_interval
            )));
        }

        // high bits的分区索引
        let max_high_value = upper_bound.unsigned_shift(num_low_bits as usize);
        let n_index_entries = max_high_value / index_interval;
        let num_index_entries = if n_index_entries >= 0 {
            n_index_entries
        } else {
            0
        };
        // Todo max value & first index
        let max_index_entry = max_high_value + num_values - 1;
        let n_index_entry_bits = if max_index_entry <= 0 {
            0
        } else {
            LONG_SIZE_32 - max_index_entry.leading_zeros() as i32
        };
        let num_longs_for_index_bits =
            Self::num_longs_for_bits(num_index_entries * n_index_entry_bits as i64);
        if num_longs_for_index_bits > i32::max_value() as i64 {
            bail!(IllegalArgument(format!(
                "num_longs_for_index_bits too large to index a long array: {}",
                num_longs_for_index_bits
            )));
        }
        Ok(Self {
            num_values,
            upper_bound,
            num_low_bits,
            lower_bits_mask,
            upper_longs,
            lower_longs,
            num_encoded: 0,
            last_encoded: 0,
            num_index_entries,
            index_interval,
            n_index_entry_bits,
            upper_zero_bit_position_index: vec![0; num_longs_for_index_bits as usize],
            current_entry_index: 0,
        })
    }

    pub fn rebuild_not_with_check(&mut self, num_values: i64, upper_bound: i64) -> Result<()> {
        self.num_values = num_values;
        self.upper_bound = upper_bound;
        self.num_encoded = num_values;
        self.last_encoded = upper_bound;
        // low bits num & mask
        self.num_low_bits = if num_values > 0 {
            let low_bits_fac = upper_bound / num_values;
            if low_bits_fac > 0 {
                LONG_SIZE_32 - 1 - low_bits_fac.leading_zeros() as i32
            } else {
                0
            }
        } else {
            0
        };
        self.lower_bits_mask =
            i64::max_value().unsigned_shift((LONG_SIZE_32 - 1 - self.num_low_bits) as usize);

        // low bits
        self.lower_longs.resize(
            Self::num_longs_for_bits(num_values * self.num_low_bits as i64) as usize,
            0,
        );
        // high bits
        self.upper_longs.resize(
            Self::num_longs_for_bits(
                upper_bound.unsigned_shift(self.num_low_bits as usize) + num_values,
            ) as usize,
            0,
        );

        // index
        let max_high_value = upper_bound.unsigned_shift(self.num_low_bits as usize);
        let n_index_entries = max_high_value / self.index_interval;
        let num_index_entries = if n_index_entries >= 0 {
            n_index_entries
        } else {
            0
        };
        let max_index_entry = max_high_value + num_values - 1;
        let n_index_entry_bits = if max_index_entry <= 0 {
            0
        } else {
            LONG_SIZE_32 - max_index_entry.leading_zeros() as i32
        };
        self.upper_zero_bit_position_index.resize(
            Self::num_longs_for_bits(num_index_entries * n_index_entry_bits as i64) as usize,
            0,
        );
        self.n_index_entry_bits = n_index_entry_bits;
        self.num_index_entries = num_index_entries;
        self.current_entry_index = 0;
        Ok(())
    }

    pub fn get_encoder(num_values: i64, upper_bound: i64) -> Result<Self> {
        Self::new(num_values, upper_bound, DEFAULT_INDEX_INTERVAL)
    }

    #[inline]
    pub fn encode_size(&self) -> i32 {
        ((self.upper_longs.len()
            + self.lower_longs.len()
            + self.upper_zero_bit_position_index.len())
            << 3) as i32
    }

    pub fn encode_next(&mut self, x: i64) -> Result<()> {
        if self.num_encoded >= self.num_values {
            bail!(IllegalState(format!(
                "encode_next called more than {} times.",
                self.num_values
            )));
        }
        if self.last_encoded > x {
            bail!(IllegalArgument(format!(
                "{} smaller than previous {}",
                x, self.last_encoded
            )));
        }
        if x > self.upper_bound {
            bail!(IllegalArgument(format!(
                "{} larger than upperBound {}",
                x, self.upper_bound
            )));
        }
        let high_value = x.unsigned_shift(self.num_low_bits as usize);
        self.encode_upper_bits(high_value);
        self.encode_lower_bits(x & self.lower_bits_mask);
        self.last_encoded = x;
        let mut index_value = (self.current_entry_index + 1) * self.index_interval;
        while index_value <= high_value {
            let after_zero_bit_position = index_value + self.num_encoded;
            Self::pack_value(
                after_zero_bit_position,
                &mut self.upper_zero_bit_position_index,
                self.n_index_entry_bits,
                self.current_entry_index,
            );
            self.current_entry_index += 1;
            index_value += self.index_interval;
        }
        self.num_encoded += 1;
        Ok(())
    }

    pub fn serialize(&mut self, out: &mut impl IndexOutput) -> Result<()> {
        out.write_byte(ForUtil::encode_type_to_code(EncodeType::EF))?;
        out.write_vlong(self.upper_bound)?;
        Self::write_data(&self.upper_longs, out)?;
        Self::write_data(&self.lower_longs, out)?;
        Self::write_data(&self.upper_zero_bit_position_index, out)?;
        Ok(())
    }

    pub fn deserialize(&mut self, encoded_data: &[u8]) -> Result<()> {
        self.num_encoded = self.num_values;
        self.last_encoded = self.upper_bound;
        let mut index = 0;
        Self::read_data(&mut self.upper_longs, encoded_data, &mut index);
        Self::read_data(&mut self.lower_longs, encoded_data, &mut index);
        Self::read_data(
            &mut self.upper_zero_bit_position_index,
            encoded_data,
            &mut index,
        );

        Ok(())
    }

    pub fn deserialize2(&mut self, input: &mut dyn IndexInput) -> Result<()> {
        self.num_encoded = self.num_values;
        self.last_encoded = self.upper_bound;
        Self::read_data2(&mut self.upper_longs, input)?;
        Self::read_data2(&mut self.lower_longs, input)?;
        Self::read_data2(&mut self.upper_zero_bit_position_index, input)?;
        Ok(())
    }

    #[inline]
    pub fn sufficiently_smaller_than_bit_set(num_values: i64, upper_bound: i64) -> bool {
        return (upper_bound > (4 * LONG_SIZE)) && (upper_bound / 7) > num_values;
    }

    // pub get_decoder(&self) -> Result<EliasFanoDecoder> {
    // EliasFanoDecoder::new(self)
    // }

    pub fn get_lower_bits(&self) -> &Vec<i64> {
        &self.lower_longs
    }

    pub fn get_upper_bits(&self) -> &Vec<i64> {
        &self.upper_longs
    }

    pub fn get_index_bits(&self) -> &Vec<i64> {
        &self.upper_zero_bit_position_index
    }
    #[inline]
    fn num_longs_for_bits(n: i64) -> i64 {
        assert!(n >= 0);
        (n + LONG_SIZE - 1).unsigned_shift(LOG2_LONG_SIZE as usize)
    }
    #[inline]
    fn encode_upper_bits(&mut self, high_value: i64) {
        let next_high_bit_num = self.num_encoded + high_value;
        self.upper_longs[next_high_bit_num.unsigned_shift(LOG2_LONG_SIZE as usize) as usize] |=
            1_i64 << (next_high_bit_num & LONG_SIZE - 1)
    }
    #[inline]
    fn encode_lower_bits(&mut self, low_value: i64) {
        Self::pack_value(
            low_value,
            &mut self.lower_longs,
            self.num_low_bits,
            self.num_encoded,
        );
    }

    /// 用Vec<i64>存储固定长度的bits array
    /// value: 待存储值，取从右到左的num_bits个位
    /// long_array: 用于存储的Vec<i64>
    /// num_bits: 固定bits的个数
    /// pack_index: 已经存储的值的个数
    #[inline]
    fn pack_value(value: i64, long_array: &mut Vec<i64>, num_bits: i32, pack_index: i64) {
        if num_bits != 0 {
            let bit_pos = num_bits as i64 * pack_index;
            let index = bit_pos.unsigned_shift(LOG2_LONG_SIZE as usize) as usize;
            let bit_pos_at_index = (bit_pos & LONG_SIZE - 1) as i32;
            long_array[index] |= value << bit_pos_at_index as i64;
            if (bit_pos_at_index + num_bits) > LONG_SIZE_32 {
                long_array[index + 1] =
                    value.unsigned_shift((LONG_SIZE_32 - bit_pos_at_index) as usize);
            }
        }
    }

    pub fn write_data(data: &Vec<i64>, out: &mut impl IndexOutput) -> Result<()> {
        if !data.is_empty() {
            let ptr = data.as_ptr() as *const u8;
            let length = data.len() << 3;
            let data = unsafe { &*slice_from_raw_parts(ptr, length) };
            out.write_bytes(data, 0, length)?;
        }
        Ok(())
    }

    pub fn read_data(data: &mut Vec<i64>, encoded_data: &[u8], index: &mut usize) {
        let length = data.len();
        if length > 0 {
            data.clear();
            let ptr = encoded_data[*index..].as_ptr() as *mut i64;

            let v = unsafe { Vec::from_raw_parts(ptr, length, length) };
            let _ = v.iter().map(|&x| data.push(x)).collect::<()>();
            v.into_raw_parts();
            *index += length << 3;
        }
    }

    pub fn read_data2(buf: &mut Vec<i64>, input: &mut dyn IndexInput) -> Result<()> {
        if buf.len() > 0 {
            let ptr = buf.as_mut_ptr() as *mut u8;
            let new_buf = unsafe { &mut *slice_from_raw_parts_mut(ptr, buf.len() << 3) };
            input.read_exact(new_buf)?;
        }
        Ok(())
    }
}
// impl Drop for EliasFanoEncoder {
// fn drop(&mut self) {
// if self.upper_longs.len() > 0 {
// mem::take(&mut self.upper_longs).into_raw_parts();
// }
// if self.lower_longs.len() > 0 {
// mem::take(&mut self.lower_longs).into_raw_parts();
// }
// if self.upper_zero_bit_position_index.len() > 0 {
// mem::take(&mut self.upper_zero_bit_position_index).into_raw_parts();
// }
// }
// }

#[cfg(test)]
mod tests {
    use core::util::packed::EliasFanoEncoder;

    #[test]
    fn num_longs_for_bits() {
        assert_eq!(EliasFanoEncoder::num_longs_for_bits(5), 1);
        assert_eq!(EliasFanoEncoder::num_longs_for_bits(31), 1);
        assert_eq!(EliasFanoEncoder::num_longs_for_bits(32), 1);
        assert_eq!(EliasFanoEncoder::num_longs_for_bits(33), 1);
        assert_eq!(EliasFanoEncoder::num_longs_for_bits(65), 2);
        assert_eq!(EliasFanoEncoder::num_longs_for_bits(128), 2);
        assert_eq!(EliasFanoEncoder::num_longs_for_bits(129), 3);
    }
    #[test]
    fn get_encoder() {
        let efe = EliasFanoEncoder::get_encoder(128, 510901);
        println!("efe: {:#?}", efe);
    }

    #[test]
    fn pack_value() {
        let mut lv = vec![0_i64; 2];
        EliasFanoEncoder::pack_value(2, &mut lv, 2, 31);
        println!("wjj: {}", lv[0]);
        assert_eq!(lv[0], 0x8000000000000000_u64 as i64);
        lv[0] = 0;
        EliasFanoEncoder::pack_value(0b11111_i64, &mut lv, 5, 12);
        println! {"wjj: {:?}", lv};
        assert_eq!(lv[0], 0xF000000000000000_u64 as i64);
        assert_eq!(lv[1], 1_i64);
    }

    #[test]
    fn encode_upper() {
        let mut ef = EliasFanoEncoder::new(7, 24, 256).unwrap();
        println!("encoder: {:?}", ef);
        ef.encode_upper_bits(0);
        ef.num_encoded += 1;
        assert_eq!(ef.upper_longs[0], 1_i64);
        println!("encoder: {:?}, num: {}", ef.upper_longs[0], ef.num_encoded);
        ef.encode_upper_bits(0);
        ef.num_encoded += 1;
        assert_eq!(ef.upper_longs[0], 3_i64);
        println!("encoder: {:?}, num: {}", ef.upper_longs[0], ef.num_encoded);
        ef.encode_upper_bits(1);
        ef.num_encoded += 1;
        assert_eq!(ef.upper_longs[0], 11_i64);
        ef.encode_upper_bits(1);
        ef.num_encoded += 1;
        assert_eq!(ef.upper_longs[0], 27_i64);
        ef.encode_upper_bits(2);
        ef.num_encoded += 1;
        assert_eq!(ef.upper_longs[0], 91_i64);
    }
}
