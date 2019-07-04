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
        ((*self as u64 >> 1) as i64 ^ -(self & 1))
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
