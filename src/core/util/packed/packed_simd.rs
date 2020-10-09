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

use std::arch::x86_64 as simd;
use std::slice::{from_raw_parts, from_raw_parts_mut};

pub trait SIMDPacker {
    const BLOCK_LEN: usize;
    type DataType;
    fn pack(data: &[u32], encoded_data: &mut [u8], bits_num: u8);
    fn unpack<T: AsRawPtr>(encoded_data: T, data: &mut [u32], bits_num: u8);
    fn delta_pack(&mut self, data: &[u32], encoded_data: &mut [u8], base: u32, bits_num: u8);
    fn delta_unpack<T: AsRawPtr>(
        &mut self,
        encoded_data: T,
        data: &mut [u32],
        base: u32,
        bits_num: u8,
    );
    fn trans_to_delta(&mut self, data: Self::DataType) -> Self::DataType {
        data
    }
    fn trans_from_delta(&mut self, delta: Self::DataType) -> Self::DataType {
        delta
    }
    fn max_bits_num(data: &[u32]) -> u8 {
        32 - data
            .iter()
            .fold(0u32, |result, &element| result | element)
            .leading_zeros() as u8
    }
    fn is_support() -> bool;
}

#[derive(Debug, Clone, Copy)]
pub struct SIMD128Packer {
    delta_base: u32,
}

impl SIMD128Packer {
    pub fn new(block_len: usize) -> Self {
        assert_eq!(block_len, 128usize);
        Self { delta_base: 0 }
    }

    #[inline(always)]
    fn direct_copy_to(data: &[u32], encoded_data: &mut [u8]) {
        encoded_data.copy_from_slice(unsafe { from_raw_parts(data.as_ptr() as *const u8, 512) })
    }

    #[inline(always)]
    fn direct_copy_from(encoded_data: &[u8], data: &mut [u32]) {
        unsafe { from_raw_parts_mut(data.as_mut_ptr() as *mut u8, 512) }
            .copy_from_slice(encoded_data);
    }

    #[allow(dead_code)]
    fn print_m128i_as_i32(v: simd::__m128i) {
        unsafe {
            let data = vec![
                simd::_mm_cvtsi128_si32(v),
                simd::_mm_cvtsi128_si32(simd::_mm_srli_si128(v, 4)),
                simd::_mm_cvtsi128_si32(simd::_mm_srli_si128(v, 8)),
                simd::_mm_cvtsi128_si32(simd::_mm_srli_si128(v, 16)),
            ];
            println!("{:?}", data);
        }
    }
}

macro_rules! pack_bits {
    ($data: expr, $encoded_data: expr, $num: literal $(,$transfer: expr, $obj: expr)?) => {
        unsafe {
            let mut _input = $data.as_ptr() as *const simd::__m128i;
            let mut _output = $encoded_data.as_mut_ptr() as *mut simd::__m128i;
            let mut _buffer = simd::_mm_set1_epi32(0);
            unroll! {
                for i in 0..32 {
                    let mut _input_data = simd::_mm_lddqu_si128(_input);
                    $(_input_data = $transfer($obj, _input_data);)?
                    const inner_pos: i32 = i as i32 * $num % 32;
                    _buffer = simd::_mm_or_si128(_buffer, simd::_mm_slli_epi32(_input_data, inner_pos));
                    const new_pos: i32 = inner_pos + $num;
                    if new_pos >= 32 { // buffer is full, store
                        simd::_mm_storeu_si128(_output, _buffer);
                        _output = _output.add(1);
                        _buffer = if new_pos > 32 {
                            simd::_mm_srli_epi32(_input_data, 32 - inner_pos)
                        } else {
                            simd::_mm_set1_epi32(0)
                        }
                    }
                    _input = _input.add(1);
                }
            }
        }
    };
}

pub trait AsRawPtr {
    fn as_ptr(&self) -> *const u8;
}

impl AsRawPtr for *const u8 {
    fn as_ptr(&self) -> *const u8 {
        *self
    }
}

impl AsRawPtr for &[u8] {
    fn as_ptr(&self) -> *const u8 {
        (*self).as_ptr()
    }
}

macro_rules! unpack_bits {
    ($encoded_data: expr, $data: expr, $num: literal $(,$transfer: expr, $obj: expr)?) => {
   unsafe {
            let mut input = $encoded_data.as_ptr() as *const simd::__m128i;
            let mut _output = $data.as_mut_ptr() as *mut simd::__m128i;
            let mask = simd::_mm_set1_epi32(((1u32 << $num) - 1) as i32);
            let mut _buffer = simd::_mm_lddqu_si128(input);
            unroll! {
                for i in 0..32 {
                    const inner_pos: i32 = i as i32 * $num % 32;
                    const new_pos: i32 = inner_pos + $num;
                    if new_pos >= 32 {
                        input = input.add(1);
                        _buffer = if new_pos == 32 {
                            $(_buffer = $transfer($obj, _buffer);)?
                            simd::_mm_storeu_si128(_output, _buffer);
                            simd::_mm_lddqu_si128(input)
                        } else {
                            const remain: i32 = 32 - inner_pos;
                            let temp = simd::_mm_lddqu_si128(input);
                            _buffer = simd::_mm_and_si128(simd::_mm_or_si128(
                                _buffer, simd::_mm_slli_epi32(temp, remain)), mask);
                            $(_buffer = $transfer($obj, _buffer);)?
                            simd::_mm_storeu_si128(_output, _buffer);
                            simd::_mm_srli_epi32(temp, $num - remain)
                        }
                    } else {
                        let mut _data = simd::_mm_and_si128(_buffer, mask);
                        $(_data = $transfer($obj, _data);)?
                        simd::_mm_storeu_si128(_output, _data);
                        _buffer = simd::_mm_srli_epi32(_buffer, $num);
                    }
                    _output = _output.add(1);
                }
            }
        }
    };
}

impl SIMDPacker for SIMD128Packer {
    const BLOCK_LEN: usize = 128;
    type DataType = simd::__m128i;

    fn pack(data: &[u32], encoded_data: &mut [u8], bits_num: u8) {
        match bits_num {
            // for unroll loop, reducing branch missing
            1 => pack_bits!(data, encoded_data, 1),
            2 => pack_bits!(data, encoded_data, 2),
            3 => pack_bits!(data, encoded_data, 3),
            4 => pack_bits!(data, encoded_data, 4),
            5 => pack_bits!(data, encoded_data, 5),
            6 => pack_bits!(data, encoded_data, 6),
            7 => pack_bits!(data, encoded_data, 7),
            8 => pack_bits!(data, encoded_data, 8),
            9 => pack_bits!(data, encoded_data, 9),
            10 => pack_bits!(data, encoded_data, 10),
            11 => pack_bits!(data, encoded_data, 11),
            12 => pack_bits!(data, encoded_data, 12),
            13 => pack_bits!(data, encoded_data, 13),
            14 => pack_bits!(data, encoded_data, 14),
            15 => pack_bits!(data, encoded_data, 15),
            16 => pack_bits!(data, encoded_data, 16),
            17 => pack_bits!(data, encoded_data, 17),
            18 => pack_bits!(data, encoded_data, 18),
            19 => pack_bits!(data, encoded_data, 19),
            20 => pack_bits!(data, encoded_data, 20),
            21 => pack_bits!(data, encoded_data, 21),
            22 => pack_bits!(data, encoded_data, 22),
            23 => pack_bits!(data, encoded_data, 23),
            24 => pack_bits!(data, encoded_data, 24),
            25 => pack_bits!(data, encoded_data, 25),
            26 => pack_bits!(data, encoded_data, 26),
            27 => pack_bits!(data, encoded_data, 27),
            28 => pack_bits!(data, encoded_data, 28),
            29 => pack_bits!(data, encoded_data, 29),
            30 => pack_bits!(data, encoded_data, 30),
            31 => pack_bits!(data, encoded_data, 31),
            32 => Self::direct_copy_to(data, encoded_data),
            0 => { /* do nothing! */ }
            _ => unimplemented!(),
        }
    }

    fn unpack<T: AsRawPtr>(encoded_data: T, data: &mut [u32], bits_num: u8) {
        match bits_num {
            // for unroll loop, reducing branch missing
            1 => unpack_bits!(encoded_data, data, 1),
            2 => unpack_bits!(encoded_data, data, 2),
            3 => unpack_bits!(encoded_data, data, 3),
            4 => unpack_bits!(encoded_data, data, 4),
            5 => unpack_bits!(encoded_data, data, 5),
            6 => unpack_bits!(encoded_data, data, 6),
            7 => unpack_bits!(encoded_data, data, 7),
            8 => unpack_bits!(encoded_data, data, 8),
            9 => unpack_bits!(encoded_data, data, 9),
            10 => unpack_bits!(encoded_data, data, 10),
            11 => unpack_bits!(encoded_data, data, 11),
            12 => unpack_bits!(encoded_data, data, 12),
            13 => unpack_bits!(encoded_data, data, 13),
            14 => unpack_bits!(encoded_data, data, 14),
            15 => unpack_bits!(encoded_data, data, 15),
            16 => unpack_bits!(encoded_data, data, 16),
            17 => unpack_bits!(encoded_data, data, 17),
            18 => unpack_bits!(encoded_data, data, 18),
            19 => unpack_bits!(encoded_data, data, 19),
            20 => unpack_bits!(encoded_data, data, 20),
            21 => unpack_bits!(encoded_data, data, 21),
            22 => unpack_bits!(encoded_data, data, 22),
            23 => unpack_bits!(encoded_data, data, 23),
            24 => unpack_bits!(encoded_data, data, 24),
            25 => unpack_bits!(encoded_data, data, 25),
            26 => unpack_bits!(encoded_data, data, 26),
            27 => unpack_bits!(encoded_data, data, 27),
            28 => unpack_bits!(encoded_data, data, 28),
            29 => unpack_bits!(encoded_data, data, 29),
            30 => unpack_bits!(encoded_data, data, 30),
            31 => unpack_bits!(encoded_data, data, 31),
            32 => {
                Self::direct_copy_from(
                    unsafe { from_raw_parts(encoded_data.as_ptr(), 4 * 128) },
                    data,
                );
            }
            0 => { /* do nothing! */ }
            _ => unimplemented!(),
        }
    }

    fn delta_pack(&mut self, data: &[u32], encoded_data: &mut [u8], base: u32, bits_num: u8) {
        self.delta_base = base;
        match bits_num {
            // for unroll loop, reducing branch missing
            1 => pack_bits!(data, encoded_data, 1, Self::trans_to_delta, self),
            2 => pack_bits!(data, encoded_data, 2, Self::trans_to_delta, self),
            3 => pack_bits!(data, encoded_data, 3, Self::trans_to_delta, self),
            4 => pack_bits!(data, encoded_data, 4, Self::trans_to_delta, self),
            5 => pack_bits!(data, encoded_data, 5, Self::trans_to_delta, self),
            6 => pack_bits!(data, encoded_data, 6, Self::trans_to_delta, self),
            7 => pack_bits!(data, encoded_data, 7, Self::trans_to_delta, self),
            8 => pack_bits!(data, encoded_data, 8, Self::trans_to_delta, self),
            9 => pack_bits!(data, encoded_data, 9, Self::trans_to_delta, self),
            10 => pack_bits!(data, encoded_data, 10, Self::trans_to_delta, self),
            11 => pack_bits!(data, encoded_data, 11, Self::trans_to_delta, self),
            12 => pack_bits!(data, encoded_data, 12, Self::trans_to_delta, self),
            13 => pack_bits!(data, encoded_data, 13, Self::trans_to_delta, self),
            14 => pack_bits!(data, encoded_data, 14, Self::trans_to_delta, self),
            15 => pack_bits!(data, encoded_data, 15, Self::trans_to_delta, self),
            16 => pack_bits!(data, encoded_data, 16, Self::trans_to_delta, self),
            17 => pack_bits!(data, encoded_data, 17, Self::trans_to_delta, self),
            18 => pack_bits!(data, encoded_data, 18, Self::trans_to_delta, self),
            19 => pack_bits!(data, encoded_data, 19, Self::trans_to_delta, self),
            20 => pack_bits!(data, encoded_data, 20, Self::trans_to_delta, self),
            21 => pack_bits!(data, encoded_data, 21, Self::trans_to_delta, self),
            22 => pack_bits!(data, encoded_data, 22, Self::trans_to_delta, self),
            23 => pack_bits!(data, encoded_data, 23, Self::trans_to_delta, self),
            24 => pack_bits!(data, encoded_data, 24, Self::trans_to_delta, self),
            25 => pack_bits!(data, encoded_data, 25, Self::trans_to_delta, self),
            26 => pack_bits!(data, encoded_data, 26, Self::trans_to_delta, self),
            27 => pack_bits!(data, encoded_data, 27, Self::trans_to_delta, self),
            28 => pack_bits!(data, encoded_data, 28, Self::trans_to_delta, self),
            29 => pack_bits!(data, encoded_data, 29, Self::trans_to_delta, self),
            30 => pack_bits!(data, encoded_data, 30, Self::trans_to_delta, self),
            31 => pack_bits!(data, encoded_data, 31, Self::trans_to_delta, self),
            32 => Self::direct_copy_to(data, encoded_data),
            0 => { /* do nothing! */ }
            _ => unimplemented!(),
        }
    }

    fn delta_unpack<T: AsRawPtr>(
        &mut self,
        encoded_data: T,
        data: &mut [u32],
        base: u32,
        bits_num: u8,
    ) {
        self.delta_base = base;
        match bits_num {
            // for unroll loop, reducing branch missing
            1 => unpack_bits!(encoded_data, data, 1, Self::trans_from_delta, self),
            2 => unpack_bits!(encoded_data, data, 2, Self::trans_from_delta, self),
            3 => unpack_bits!(encoded_data, data, 3, Self::trans_from_delta, self),
            4 => unpack_bits!(encoded_data, data, 4, Self::trans_from_delta, self),
            5 => unpack_bits!(encoded_data, data, 5, Self::trans_from_delta, self),
            6 => unpack_bits!(encoded_data, data, 6, Self::trans_from_delta, self),
            7 => unpack_bits!(encoded_data, data, 7, Self::trans_from_delta, self),
            8 => unpack_bits!(encoded_data, data, 8, Self::trans_from_delta, self),
            9 => unpack_bits!(encoded_data, data, 9, Self::trans_from_delta, self),
            10 => unpack_bits!(encoded_data, data, 10, Self::trans_from_delta, self),
            11 => unpack_bits!(encoded_data, data, 11, Self::trans_from_delta, self),
            12 => unpack_bits!(encoded_data, data, 12, Self::trans_from_delta, self),
            13 => unpack_bits!(encoded_data, data, 13, Self::trans_from_delta, self),
            14 => unpack_bits!(encoded_data, data, 14, Self::trans_from_delta, self),
            15 => unpack_bits!(encoded_data, data, 15, Self::trans_from_delta, self),
            16 => unpack_bits!(encoded_data, data, 16, Self::trans_from_delta, self),
            17 => unpack_bits!(encoded_data, data, 17, Self::trans_from_delta, self),
            18 => unpack_bits!(encoded_data, data, 18, Self::trans_from_delta, self),
            19 => unpack_bits!(encoded_data, data, 19, Self::trans_from_delta, self),
            20 => unpack_bits!(encoded_data, data, 20, Self::trans_from_delta, self),
            21 => unpack_bits!(encoded_data, data, 21, Self::trans_from_delta, self),
            22 => unpack_bits!(encoded_data, data, 22, Self::trans_from_delta, self),
            23 => unpack_bits!(encoded_data, data, 23, Self::trans_from_delta, self),
            24 => unpack_bits!(encoded_data, data, 24, Self::trans_from_delta, self),
            25 => unpack_bits!(encoded_data, data, 25, Self::trans_from_delta, self),
            26 => unpack_bits!(encoded_data, data, 26, Self::trans_from_delta, self),
            27 => unpack_bits!(encoded_data, data, 27, Self::trans_from_delta, self),
            28 => unpack_bits!(encoded_data, data, 28, Self::trans_from_delta, self),
            29 => unpack_bits!(encoded_data, data, 29, Self::trans_from_delta, self),
            30 => unpack_bits!(encoded_data, data, 30, Self::trans_from_delta, self),
            31 => unpack_bits!(encoded_data, data, 31, Self::trans_from_delta, self),
            32 => {
                Self::direct_copy_from(
                    unsafe { from_raw_parts(encoded_data.as_ptr(), 4 * 128) },
                    data,
                );
            }
            0 => { /* do nothing! */ }
            _ => unimplemented!(),
        }
    }

    #[inline(always)]
    fn trans_to_delta(&mut self, data: simd::__m128i) -> simd::__m128i {
        unsafe {
            let deltas = simd::_mm_sub_epi32(
                data,
                simd::_mm_or_si128(
                    simd::_mm_slli_si128(data, 4),
                    simd::_mm_set_epi32(0, 0, 0, self.delta_base as i32),
                ),
            );

            self.delta_base = simd::_mm_cvtsi128_si32(simd::_mm_srli_si128(data, 12)) as u32;
            deltas
        }
    }

    #[inline(always)]
    fn trans_from_delta(&mut self, delta: simd::__m128i) -> simd::__m128i {
        unsafe {
            let a_ab_bc_cd = simd::_mm_add_epi32(delta, simd::_mm_slli_si128(delta, 4));
            let a_ab_abc_abcd =
                simd::_mm_add_epi32(a_ab_bc_cd, simd::_mm_slli_si128(a_ab_bc_cd, 8));
            let value =
                simd::_mm_add_epi32(a_ab_abc_abcd, simd::_mm_set1_epi32(self.delta_base as i32));
            self.delta_base = simd::_mm_cvtsi128_si32(simd::_mm_srli_si128(value, 12)) as u32;
            value
        }
    }

    fn max_bits_num(data: &[u32]) -> u8 {
        unsafe {
            let mut data_input = data.as_ptr() as *const simd::__m128i;
            let mut result = simd::_mm_lddqu_si128(data_input);
            for _ in 1..32 {
                data_input = data_input.add(1);
                let data = simd::_mm_lddqu_si128(data_input);
                result = simd::_mm_or_si128(result, data);
            }
            // result like a_b_c_d
            let a_b_c = simd::_mm_srli_si128(result, 4);
            let a_ab_bc_cd = simd::_mm_or_si128(result, a_b_c);
            let a_ab = simd::_mm_srli_si128(a_ab_bc_cd, 8);
            let a_ab_abc_abcd = simd::_mm_or_si128(a_ab_bc_cd, a_ab);
            32 - simd::_mm_cvtsi128_si32(a_ab_abc_abcd).leading_zeros() as u8
        }
    }

    #[inline(always)]
    fn is_support() -> bool {
        std::is_x86_feature_detected!("sse3")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_max_bits_num() {
        // test basic algorithm
        assert_eq!(MockPacker::max_bits_num(&[5, 8, 7]), 4);
        assert_eq!(MockPacker::max_bits_num(&[0b10101, 0b111, 0b11101]), 5);
        assert_eq!(
            MockPacker::max_bits_num(&[0b10101, 0b1000000111, 0b11101]),
            10
        );
        // construct 128 array, test simd algorithm
        let mut data = [0u32; 128];
        for (i, e) in data.iter_mut().enumerate() {
            *e = i as u32 * 5;
        }
        assert_eq!(
            MockPacker::max_bits_num(&data),
            SIMD128Packer::max_bits_num(&data)
        );
    }

    #[test]
    fn test_direct_copy() {
        let mut data = [0u32; 128];
        for (i, e) in data.iter_mut().enumerate() {
            *e = i as u32 % 9 * (i + 1) as u32;
        }

        let mut temp = [0u8; 4 * 128];
        SIMD128Packer::direct_copy_to(&data, &mut temp);
        let mut data2 = [0u32; 128];
        SIMD128Packer::direct_copy_from(&temp, &mut data2);
        assert_eq!(data2[0], 0);
        assert_eq!(data2[1], 2);
        assert_eq!(data2[9], 0);
        assert_eq!(data2[10], 11);
        assert_eq!(&data[0..32], &data2[0..32]);
        assert_eq!(&data[33..64], &data2[33..64]);
        assert_eq!(&data[65..96], &data2[65..96]);
        assert_eq!(&data[97..128], &data2[97..128]);
    }

    macro_rules! assert_128_array_eq {
        ($arr1: expr, $arr2: expr) => {
            let mut is_equal = true;
            let mut pos = 0;
            let mut left = 0;
            let mut right = 0;
            assert_eq!($arr1.len(), 128usize);
            assert_eq!($arr2.len(), 128usize);
            for i in 0..128 {
                if $arr1[i] != $arr2[i] {
                    is_equal = false;
                    pos = i;
                    left = $arr1[i];
                    right = $arr2[i];
                    break;
                }
            }
            assert!(
                is_equal,
                "value@{} is not equal! left={}, right={}.",
                pos, left, right
            );
        };
    }

    #[test]
    fn test_pack_unpack_bits() {
        let mut data_1bits = [0u32; 128];
        let mut data_5bits = [0u32; 128];
        let mut data_31bits = [0u32; 128];
        for i in 0..128 {
            if i % 5 == 0 {
                data_1bits[i] = 1;
                data_5bits[i] = (0b10000 | (i & 0b1111)) as u32;
                data_31bits[i] = 0x40000000u32 | i as u32;
            }
        }
        let mut encoded = [0u8; 512];
        let mut decoded = [0u32; 128];
        pack_bits!(&data_1bits, &mut encoded, 1);
        unpack_bits!(&encoded, &mut decoded, 1);
        assert_128_array_eq!(&data_1bits, &decoded);
        assert_eq!(decoded[0], 1);
        assert_eq!(decoded[1], 0);
        assert_eq!(decoded[4], 0);
        assert_eq!(decoded[5], 1);
        assert_eq!(decoded[6], 0);
        assert_eq!(decoded[34], 0);
        assert_eq!(decoded[35], 1);
        assert_eq!(decoded[36], 0);

        pack_bits!(&data_5bits, &mut encoded, 5);
        unpack_bits!(&encoded, &mut decoded, 5);
        assert_128_array_eq!(&data_5bits, &decoded);
        assert_eq!(decoded[35], (0b10000 | (35 & 0b1111)) as u32);

        pack_bits!(&data_31bits, &mut encoded, 31);
        unpack_bits!(&encoded, &mut decoded, 31);
        assert_128_array_eq!(&data_31bits, &decoded);
        assert_eq!(decoded[40], 0x40000000u32 | 40 as u32);
    }

    #[test]
    fn test_delta_pack_unpack() {
        let mut data = [0u32; 128];
        let mut i = 1;
        data.iter_mut().for_each(|x| {
            *x = i * 128;
            i += 1;
        });
        assert_eq!(data[127], 128 * 128);
        let mut encoded = [0u8; 512];
        let mut decoded = [0u32; 128];
        let mut packer = SIMD128Packer::new(128);
        SIMD128Packer::pack(&data, &mut encoded, 15);
        SIMD128Packer::unpack(&encoded[..], &mut decoded, 15);
        assert_128_array_eq!(&data, &decoded);
        packer.delta_pack(&data, &mut encoded, 128, 14);
        packer.delta_unpack(&encoded[..], &mut decoded, 128, 14);
        assert_128_array_eq!(&data, &decoded);
    }

    struct MockPacker;
    impl SIMDPacker for MockPacker {
        const BLOCK_LEN: usize = 1;
        type DataType = ();

        fn pack(_data: &[u32], _encoded_data: &mut [u8], _bits_num: u8) {
            unimplemented!()
        }

        fn unpack<T: AsRawPtr>(_encoded_data: T, _data: &mut [u32], _bits_num: u8) {
            unimplemented!()
        }

        fn delta_pack(
            &mut self,
            _data: &[u32],
            _encoded_data: &mut [u8],
            _base: u32,
            _bits_num: u8,
        ) {
            unimplemented!()
        }

        fn delta_unpack<T: AsRawPtr>(
            &mut self,
            _encoded_data: T,
            _data: &mut [u32],
            _base: u32,
            _bits_num: u8,
        ) {
            unimplemented!()
        }

        fn is_support() -> bool {
            unimplemented!()
        }
    }
}
