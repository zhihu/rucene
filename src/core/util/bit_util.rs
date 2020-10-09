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

use std::mem::size_of;

pub trait ZigZagEncoding {
    fn encode(&self) -> Self;
    fn decode(&self) -> Self;
}

impl ZigZagEncoding for i32 {
    fn encode(&self) -> i32 {
        (*self >> 31) ^ (self << 1)
    }
    fn decode(&self) -> i32 {
        (*self as u32 >> 1) as i32 ^ -(self & 1)
    }
}

impl ZigZagEncoding for i64 {
    fn encode(&self) -> i64 {
        (*self >> 63) ^ (self << 1)
    }
    fn decode(&self) -> i64 {
        (*self as u64 >> 1) as i64 ^ -(self & 1)
    }
}

pub trait UnsignedShift: Sized {
    fn unsigned_shift(&self, by: usize) -> Self;
}

macro_rules! impl_unsigned_shift {
    ($type: ty, $utype: ty) => {
        impl UnsignedShift for $type {
            #[inline]
            fn unsigned_shift(&self, by: usize) -> Self {
                (*self as $utype >> by) as $type
            }
        }
    };
}

impl_unsigned_shift!(i8, u8);
impl_unsigned_shift!(i16, u16);
impl_unsigned_shift!(i32, u32);
impl_unsigned_shift!(i64, u64);
impl_unsigned_shift!(isize, usize);

// a util trait to calculate bits need when compressed
pub trait BitsRequired {
    fn bits_required(&self) -> u32;
}

macro_rules! impl_bits_required {
    ($type:ty, $width: expr) => {
        impl BitsRequired for $type {
            #[inline]
            fn bits_required(&self) -> u32 {
                1.max($width - (*self).leading_zeros())
            }
        }
    };
}

impl_bits_required!(i8, 8);
impl_bits_required!(u8, 8);
impl_bits_required!(i16, 16);
impl_bits_required!(u16, 16);
impl_bits_required!(i32, 32);
impl_bits_required!(u32, 32);
impl_bits_required!(i64, 64);
impl_bits_required!(u64, 64);
impl_bits_required!(isize, (size_of::<isize>() * 8) as u32);
impl_bits_required!(usize, (size_of::<usize>() * 8) as u32);

// The pop methods used to rely on bit-manipulation tricks for speed but it
// turns out that it is faster to use the Long.bitCount method (which is an
// intrinsic since Java 6u18) in a naive loop, see LUCENE-2221

/// Returns the number of set bits in an array of longs.
pub fn pop_array(arr: &[i64], word_offset: usize, num_words: usize) -> usize {
    let mut pop_count = 0usize;
    for a in arr.iter().skip(word_offset).take(num_words) {
        pop_count += a.count_ones() as usize;
    }
    pop_count
}

pub const LONG_SIZE: i64 = 64;
pub const LONG_SIZE_32: i32 = LONG_SIZE as i32;
pub const LOG2_LONG_SIZE: i32 = 6;

/// the number of 0s of the value bits from the left
#[inline]
pub fn number_of_leading_zeros(value: i64) -> i32 {
    if value <= 0 {
        return if value == 0 { 64 } else { 0 };
    }
    let mut value = value;
    let mut n = 63;
    if value >= (1_i64 << 32) {
        n -= 32;
        value = value.unsigned_shift(32);
    }
    if value >= (1_i64 << 16) {
        n -= 16;
        value = value.unsigned_shift(16);
    }
    if value >= (1_i64 << 8) {
        n -= 8;
        value = value.unsigned_shift(8);
    }
    if value >= (1_i64 << 4) {
        n -= 4;
        value = value.unsigned_shift(4);
    }
    if value >= (1_i64 << 2) {
        n -= 2;
        value = value.unsigned_shift(2);
    }
    n - value.unsigned_shift(1) as i32
}

/// the number of 0s of the value bits from the right
#[inline]
pub fn number_of_trailing_zeros(value: i64) -> i32 {
    if value == 0 {
        return 64;
    }
    let mut value = value;
    let mut n = 63;
    let mut y = value << 32_i64;
    if y != 0 {
        n -= 32;
        value = y;
    }
    y = value << 16_i64;
    if y != 0 {
        n -= 16;
        value = y;
    }
    y = value << 8_i64;
    if y != 0 {
        n -= 8;
        value = y;
    }
    y = value << 4_i64;
    if y != 0 {
        n -= 4;
        value = y;
    }
    y = value << 2_i64;
    if y != 0 {
        n -= 2;
        value = y;
    }
    n - (value << 1).unsigned_shift(63) as i32
}

///  Input: i -> the value
///  Output: the number of ones in the value bits
#[inline]
pub fn bit_count(i: i64) -> i32 {
    // HD, Figure 5-2
    let mut i = i - ((i.unsigned_shift(1)) & 0x5555555555555555_i64);
    i = (i & 0x3333333333333333_i64) + ((i.unsigned_shift(2)) & 0x3333333333333333_i64);
    i = (i + (i.unsigned_shift(4))) & 0x0f0f0f0f0f0f0f0f_i64;
    i = i + (i.unsigned_shift(8));
    i = i + (i.unsigned_shift(16));
    i = i + (i.unsigned_shift(32));
    return (i & 0x7f) as i32;
}

const L8_L: i64 = 0x0101010101010101;

lazy_static! {
    static ref PS_OVERFLOW: [i64; 64] = {
        let mut ps_overflow: [i64; 64] = [0; 64];
        for s in 1..=ps_overflow.len() {
            ps_overflow[s - 1] = (128 - s as i64) * L8_L;
        }
        ps_overflow
    };
    static ref SELECT256: [u8; 2048] = {
        let mut select256: [u8; 2048] = [0; 2048];
        for b in 0..=0xFF {
            for s in 1..=8 {
                let byte_index = b | ((s - 1) << 8);
                let mut bit_index = select_naive(b, s as i32);
                if bit_index < 0 {
                    bit_index = 127;
                }
                assert!(bit_index >= 0);
                select256[byte_index as usize] = bit_index as u8;
            }
        }
        select256
    };
}

/// Select a 1-bit from a long. See also LUCENE-6040.
/// @return The index of the r-th 1 bit in x. This bit must exist.
#[inline]
pub fn select(x: i64, r: i32) -> i32 {
    let mut s = (x as i128 - (x & 0xAAAAAAAAAAAAAAAA_u64 as i64).unsigned_shift(1) as i128) as i64;
    s = (s & 0x3333333333333333_i64) + (s.unsigned_shift(2) & 0x3333333333333333_i64);
    s = (((s + s.unsigned_shift(4)) & 0x0F0F0F0F0F0F0F0F_i64) as u128 * L8_L as u128) as i64;
    let b = (((s as u128 + PS_OVERFLOW[(r - 1) as usize] as u128) as i64 & (L8_L << 7))
        .trailing_zeros()
        >> 3)
        << 3 as i32;
    let l = r as i64 - ((s << 8).unsigned_shift((b & 0x3F_u32) as usize) & 0xFF_i64);

    let select_index =
        ((x.unsigned_shift((b & 0x3F_u32) as usize) & 0xFF_i64) | ((l - 1) << 8)) as usize;
    b as i32 + SELECT256[select_index] as i32
}

/// Naive implementation of {@link #select(i64, i32)}, using(@link num::trailing_zeros())
/// repetitively. Works relatively fast for low ranks.
/// @return The index of the r-th 1 bit in x, or -1 if no such bit exists.
#[inline]
pub fn select_naive(mut x: i64, mut r: i32) -> i32 {
    assert!(r >= 1);
    let mut s = -1;
    while (x != 0) && (r > 0) {
        let ntz = x.trailing_zeros() as i32;
        x = x.unsigned_shift((ntz + 1) as usize);
        s += ntz + 1;
        r -= 1;
    }
    if r > 0 {
        -1
    } else {
        s
    }
}

#[cfg(test)]
mod tests {
    use core::util::bit_util;

    #[test]
    fn number_of_leading_zeros() {
        assert_eq!(bit_util::number_of_leading_zeros(-1), 0);
        assert_eq!(bit_util::number_of_leading_zeros(-2), 0);
        assert_eq!(bit_util::number_of_leading_zeros(-100002), 0);
        assert_eq!(bit_util::number_of_leading_zeros(128), 56);
        assert_eq!(bit_util::number_of_leading_zeros(2), 62);
        assert_eq!(bit_util::number_of_leading_zeros(0), 64);
        assert_eq!(bit_util::number_of_leading_zeros(i64::max_value()), 1);
    }

    #[test]
    fn number_of_trailing_zeros() {
        assert_eq!(bit_util::number_of_trailing_zeros(-1), 0);
        assert_eq!(bit_util::number_of_trailing_zeros(-2), 1);
        assert_eq!(bit_util::number_of_trailing_zeros(-3), 0);
        assert_eq!(bit_util::number_of_trailing_zeros(0), 64);
        assert_eq!(bit_util::number_of_trailing_zeros(i64::max_value()), 0);
    }
    #[test]
    fn bit_count() {
        assert_eq!(bit_util::bit_count(9), 2);
        assert_eq!(bit_util::bit_count(1), 1);
        assert_eq!(bit_util::bit_count(2), 1);
        assert_eq!(bit_util::bit_count(-1), 64);
        assert_eq!(bit_util::bit_count(0x180000000_i64), 2);
        assert_eq!(bit_util::bit_count(0xEFFFFFFFFFFFFFFF_u64 as i64), 63);
        assert_eq!(bit_util::bit_count(0xEFEFEFFEFFFFFFFF_u64 as i64), 60);
    }

    #[test]
    fn select_naive() {
        assert_eq!(bit_util::select_naive(1, 1), 0);
        assert_eq!(bit_util::select_naive(2, 1), 1);
        assert_eq!(bit_util::select_naive(4, 1), 2);
        assert_eq!(bit_util::select_naive(5, 2), 2);
        assert_eq!(bit_util::select_naive(10, 2), 3);
        assert_eq!(
            bit_util::select_naive(0xEFEFEFFEFFFFFFFF_u64 as i64, 44),
            45
        );
        assert_eq!(bit_util::select_naive(-1, 1), 0);
        assert_eq!(bit_util::select_naive(0x8000000000000001_u64 as i64, 2), 63);
    }

    #[test]
    fn select() {
        assert_eq!(bit_util::select(1, 1), 0);
        assert_eq!(bit_util::select(2, 1), 1);
        assert_eq!(bit_util::select(4, 1), 2);
        assert_eq!(bit_util::select(5, 2), 2);
        assert_eq!(bit_util::select(10, 2), 3);
        assert_eq!(bit_util::select(0xEFEFEFFEFFFFFFFF_u64 as i64, 44), 45);
        assert_eq!(bit_util::select(-1, 1), 0);
        // assert_eq!(bit_util::select(0x8000000000000001_u64 as i64, 2), 63);
        // assert_eq!(bit_util::select(0xF000000000000001_u64 as i64, 61), 60);
    }
}
