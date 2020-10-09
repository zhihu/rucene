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

use core::codec::postings::BLOCK_SIZE;
use core::search::NO_MORE_DOCS;
use core::store::io::IndexInput;
use core::util::packed::{SIMD128Packer, SIMDPacker};
use error::Result;
use std::arch::x86_64 as simd;

#[repr(align(128))]
struct AlignedBuffer([u32; (BLOCK_SIZE + 4) as usize]);

pub struct SIMDBlockDecoder {
    data: AlignedBuffer,
    encoded: [u8; BLOCK_SIZE as usize * 4],
    packer: SIMD128Packer,
    next_index: usize,
    base_value: i32,
}

impl SIMDBlockDecoder {
    pub fn new() -> Self {
        assert_eq!(BLOCK_SIZE, 128);
        Self {
            data: AlignedBuffer([NO_MORE_DOCS as u32; (BLOCK_SIZE + 4) as usize]),
            encoded: [0u8; BLOCK_SIZE as usize * 4],
            packer: SIMD128Packer::new(BLOCK_SIZE as usize),
            next_index: 0,
            base_value: 0,
        }
    }

    pub fn reset_delta_base(&mut self, base: i32) {
        self.base_value = base;
    }

    #[inline(always)]
    pub fn set_single(&mut self, value: i32) {
        self.next_index = 0;
        let mut v = self.base_value;
        self.data.0[..BLOCK_SIZE as usize].iter_mut().for_each(|e| {
            v += value;
            *e = v as u32;
        });
    }

    pub fn parse_from(
        &mut self,
        input: &mut dyn IndexInput,
        num: usize,
        bits_num: usize,
    ) -> Result<()> {
        input.read_exact(&mut self.encoded[0..num])?;
        self.next_index = 0;
        self.packer.delta_unpack(
            &self.encoded[0..num],
            &mut self.data.0,
            self.base_value as u32,
            bits_num as u8,
        );
        Ok(())
    }

    pub fn parse_from_no_copy(
        &mut self,
        input: &mut dyn IndexInput,
        num: usize,
        bits_num: usize,
    ) -> Result<()> {
        let encoded = unsafe { input.get_and_advance(num) };
        self.next_index = 0;
        self.packer.delta_unpack(
            encoded,
            &mut self.data.0,
            self.base_value as u32,
            bits_num as u8,
        );
        Ok(())
    }

    #[inline(always)]
    pub fn next(&mut self) -> i32 {
        let pos = self.next_index;
        self.next_index += 1;
        self.data.0[pos] as i32
    }

    #[inline(always)]
    pub fn advance(&mut self, target: i32) -> (i32, usize) {
        unsafe {
            let input = self.data.0.as_ptr() as *const simd::__m128i;
            let target = simd::_mm_set1_epi32(target);
            let mut count = simd::_mm_set1_epi32(0);
            unroll! {
                for i in 0..8 {
                    let r1 = simd::_mm_cmplt_epi32(
                        simd::_mm_load_si128(input.add(i * 4)), target);
                    let r2 = simd::_mm_cmplt_epi32(
                        simd::_mm_load_si128(input.add(i * 4 + 1)), target);
                    let r3 = simd::_mm_cmplt_epi32(
                        simd::_mm_load_si128(input.add(i * 4 + 2)), target);
                    let r4 = simd::_mm_cmplt_epi32(
                        simd::_mm_load_si128(input.add(i * 4 + 3)), target);
                    let sum = simd::_mm_add_epi32(
                        simd::_mm_add_epi32(r1, r2),
                        simd::_mm_add_epi32(r3, r4)
                    );
                    count = simd::_mm_sub_epi32(count, sum);
                }
            };
            let count = simd::_mm_add_epi32(count, simd::_mm_srli_si128(count, 8));
            let count = simd::_mm_add_epi32(count, simd::_mm_srli_si128(count, 4));
            let count = simd::_mm_cvtsi128_si32(count) as usize;
            self.next_index = count + 1;
            (self.data.0[count] as i32, count)
        }
    }

    #[inline(always)]
    pub fn advance_by_partial(&mut self, target: i32) -> (i32, usize) {
        let mut index = self.next_index & 0xFCusize;
        let mut input = self.data.0[index..].as_ptr() as *const simd::__m128i;
        unsafe {
            let target = simd::_mm_set1_epi32(target);
            while index < 128 {
                let res = simd::_mm_cmplt_epi32(simd::_mm_load_si128(input), target);
                let res = simd::_mm_movemask_epi8(res);
                if res != 0xFFFF {
                    index += ((32 - res.leading_zeros()) >> 2) as usize;
                    break;
                } else {
                    index += 4;
                    input = input.add(1);
                }
            }
            self.next_index = index + 1;
            (self.data.0[index] as i32, index)
        }
    }

    #[inline(always)]
    pub fn advance_by_binary_search(&mut self, target: i32) -> (i32, usize) {
        match self.data.0[self.next_index..BLOCK_SIZE as usize].binary_search(&(target as u32)) {
            Ok(p) | Err(p) => {
                let pos = self.next_index + p;
                self.next_index = pos + 1;
                (self.data.0[pos] as i32, pos)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use core::codec::postings::SIMDBlockDecoder;

    #[test]
    fn test_simd_advance() {
        let mut decoder = SIMDBlockDecoder::new();
        let mut i = 0;
        decoder.data.0.iter_mut().for_each(|e| {
            i += 128;
            *e = i;
        });
        assert_eq!(decoder.advance(1).0, 128);
        assert_eq!(decoder.advance(129).0, 256);
        assert_eq!(decoder.advance(130).0, 256);
        assert_eq!(decoder.advance(255).0, 256);
        assert_eq!(decoder.advance(256).0, 256);
        assert_eq!(decoder.advance(257).0, 384);
        assert_eq!(decoder.next(), 512);
        assert_eq!(decoder.advance(16283).0, 16384);
    }
    #[test]
    fn test_binary_search() {
        let mut decoder = SIMDBlockDecoder::new();
        let mut i = 0;
        decoder.data.0.iter_mut().for_each(|e| {
            i += 128;
            *e = i;
        });
        assert_eq!(decoder.advance_by_binary_search(1), (128, 0));
        assert_eq!(decoder.advance_by_binary_search(129), (256, 1));
        assert_eq!(decoder.advance_by_binary_search(512), (512, 3));
    }
}
