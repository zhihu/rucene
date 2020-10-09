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

use core::codec::postings::MAX_ENCODED_SIZE;
use core::store::io::IndexInput;
use core::util::packed::Format;
use error::Result;
use std::convert::TryInto;
use std::intrinsics::unlikely;

pub struct PartialBlockDecoder {
    encoded: [u8; MAX_ENCODED_SIZE + 8],
    index: usize,
    data: u64,
    bits_num: usize,
    format: Format,
    is_single: bool,
    block_capacity: usize,
    mask: u64,
}

impl PartialBlockDecoder {
    pub fn new() -> Self {
        Self {
            encoded: [0u8; MAX_ENCODED_SIZE + 8],
            index: 0,
            data: 0,
            bits_num: 0,
            format: Format::Packed,
            is_single: false,
            block_capacity: 0,
            mask: 0,
        }
    }

    pub fn set_single(&mut self, value: i32) {
        self.is_single = true;
        self.data = value as u64;
    }

    pub fn parse_from(
        &mut self,
        input: &mut dyn IndexInput,
        num: usize,
        bits_num: usize,
        format: Format,
    ) -> Result<()> {
        self.is_single = false;
        input.read_exact(&mut self.encoded[0..num])?;
        self.index = 0;
        self.bits_num = bits_num;
        self.format = format;
        self.block_capacity = 64 / bits_num;
        self.mask = (1 << self.bits_num) as u64 - 1;
        Ok(())
    }

    #[inline(always)]
    pub fn get(&mut self, index: usize) -> i32 {
        if unlikely(self.is_single) {
            self.data as i32
        } else {
            match self.format {
                Format::Packed => {
                    let bit_index = index * self.bits_num;
                    let pos = bit_index >> 3;
                    let target = u64::from_be_bytes(self.encoded[pos..pos + 8].try_into().unwrap());
                    let left_bits = bit_index & 7usize;
                    ((target >> (64 - left_bits - self.bits_num)) & self.mask) as i32
                }
                Format::PackedSingleBlock => {
                    let block_index = index / self.block_capacity;
                    let pos = block_index << 3;
                    let target = u64::from_be_bytes(self.encoded[pos..pos + 8].try_into().unwrap());
                    ((target >> index % self.block_capacity * self.bits_num) & self.mask) as i32
                }
            }
        }
    }

    #[inline(always)]
    pub fn next(&mut self) -> i32 {
        if unlikely(self.is_single) {
            self.data as i32
        } else {
            match self.format {
                Format::Packed => {
                    let pos = self.index >> 3;
                    let mut target =
                        u64::from_be_bytes(self.encoded[pos..pos + 8].try_into().unwrap());
                    target = (target >> (64 - (self.index & 7) - self.bits_num)) & self.mask;
                    self.index += self.bits_num;
                    target as i32
                }
                Format::PackedSingleBlock => {
                    let inner_index = self.index % self.block_capacity;
                    if inner_index == 0 {
                        let pos = self.index / self.block_capacity << 3;
                        self.data =
                            u64::from_be_bytes(self.encoded[pos..pos + 8].try_into().unwrap());
                    }
                    self.index += 1;
                    ((self.data >> (inner_index * self.bits_num) as u64) & self.mask) as i32
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use core::codec::postings::PartialBlockDecoder;
    use core::util::fst::{BytesStore, StoreBytesReader};
    use core::util::packed::Format;
    use error::Result;
    use std::io::Write;

    #[test]
    fn test_get() -> Result<()> {
        // Test Packed block
        let mut decoder = create_decoder(&[0xFFu8, 0xFF, 0, 0xFF], 4, Format::Packed);
        assert_eq!(decoder.get(0), 0xFi32);
        assert_eq!(decoder.get(1), 0xFi32);
        assert_eq!(decoder.get(2), 0xFi32);
        assert_eq!(decoder.get(3), 0xFi32);
        assert_eq!(decoder.get(4), 0x0i32);
        assert_eq!(decoder.get(5), 0x0i32);
        assert_eq!(decoder.get(6), 0xFi32);
        assert_eq!(decoder.get(7), 0xFi32);

        // Test PackedSingleBlock
        let data = [
            0xFFu8, 0xF, 0, 0, 0, 0, 0xFF, 0, 0x8F, 0xFF, 0x8F, 0x8F, 0x8F, 0x8F, 0x8F, 0x8F,
        ];
        let mut decoder = create_decoder(&data, 6, Format::PackedSingleBlock);
        assert_eq!(decoder.get(0), 0);
        assert_eq!(decoder.get(1), 0x3C);
        assert_eq!(decoder.get(9), 0x3C);
        assert_eq!(decoder.get(10), 0xF);
        Ok(())
    }

    #[test]
    fn test_next() -> Result<()> {
        // Test Packed block
        let mut decoder = create_decoder(&[0xFFu8, 0xFF, 0, 0xFF], 4, Format::Packed);
        assert_eq!(decoder.next(), 0xFi32);
        assert_eq!(decoder.next(), 0xFi32);
        assert_eq!(decoder.next(), 0xFi32);
        assert_eq!(decoder.next(), 0xFi32);
        assert_eq!(decoder.next(), 0x0i32);
        assert_eq!(decoder.next(), 0x0i32);
        assert_eq!(decoder.next(), 0xFi32);
        assert_eq!(decoder.next(), 0xFi32);

        // Test PackedSingleBlock
        let data = [
            0xFFu8, 0xF, 0, 0, 0, 0, 0xFF, 0, 0x8F, 0xFF, 0x8F, 0x8F, 0x8F, 0x8F, 0x8F, 0x8F,
        ];
        let mut decoder = create_decoder(&data, 6, Format::PackedSingleBlock);
        assert_eq!(decoder.next(), 0);
        assert_eq!(decoder.next(), 0x3C);
        assert_eq!(decoder.next(), 0xF);
        assert_eq!(decoder.next(), 0);
        for _i in 1..=6 {
            decoder.next();
        }
        assert_eq!(decoder.next(), 0xF);
        assert_eq!(decoder.next(), 0x3E);
        Ok(())
    }

    #[test]
    fn test_single() -> Result<()> {
        let mut decoder = PartialBlockDecoder::new();
        decoder.set_single(5);
        for i in 0..128 {
            assert_eq!(decoder.get(i), 5);
        }
        for _i in 0..128 {
            assert_eq!(decoder.next(), 5);
        }
        Ok(())
    }

    fn create_decoder(data: &[u8], bits_per_value: usize, format: Format) -> PartialBlockDecoder {
        let mut bs = BytesStore::with_block_bits(8);
        let _ = bs.write(data);
        let mut sbr = StoreBytesReader::from_bytes_store(bs, false);
        let mut decoder = PartialBlockDecoder::new();
        decoder
            .parse_from(&mut sbr, data.len(), bits_per_value, format)
            .unwrap();
        decoder
    }
}
