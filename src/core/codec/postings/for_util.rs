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

use std::cmp::max;
use std::sync::{Arc, Once};

use core::codec::postings::posting_format::BLOCK_SIZE;
use core::store::io::{DataOutput, IndexInput, IndexOutput};
use core::util::packed::*;
use core::util::BitsRequired;

use error::Result;
use std::mem::MaybeUninit;
use std::ptr;

/// Special number of bits per value used whenever all values to encode are equal.
const ALL_VALUES_EQUAL: i32 = 0;

/// Upper limit of the number of bytes that might be required to stored
/// <code>BLOCK_SIZE</code> encoded values.
pub const MAX_ENCODED_SIZE: usize = BLOCK_SIZE as usize * 4;

/// Upper limit of the number of values that might be decoded in a single call to
/// {@link #read_block(IndexInput, [u8], [i32])}. Although values after
/// `BLOCK_SIZE` are garbage, it is necessary to allocate value buffers
/// whose size is `MAX_DATA_SIZE` to avoid `IndexOutOfBounds` Error.
///
/// NOTE: this value is always equal to `max_data_size()`, use const instead of
/// something like lazy_static can allow us use [; MAX_DATA_SIZE] instead of Vec.
pub const MAX_DATA_SIZE: usize = 147;

#[test]
fn test_max_data_size() {
    assert_eq!(MAX_DATA_SIZE, max_data_size());
}

static START: Once = Once::new();

fn compute_iterations(decoder: &impl PackedIntDecoder) -> i32 {
    (BLOCK_SIZE as f32 / decoder.byte_value_count() as f32).ceil() as i32
}

pub fn max_data_size() -> usize {
    static mut MAX_DATA_SIZE: usize = 0;
    START.call_once(|| {
        let mut max_data_size: usize = 0;
        for version in VERSION_START..=VERSION_CURRENT {
            let format = Format::Packed;
            for bpv in 1..33 {
                if let Ok(decoder) = get_decoder(format, version, bpv) {
                    let iterations = compute_iterations(&decoder) as usize;
                    max_data_size = max(max_data_size, iterations * decoder.byte_value_count());
                } else {
                    panic!(format!(
                        "get_decoder({:?},{:?},{:?}) failed.",
                        format, version, bpv
                    ));
                }
            }
            let format = Format::PackedSingleBlock;
            for bpv in 1..33 {
                if let Ok(decoder) = get_decoder(format, version, bpv) {
                    let iterations = compute_iterations(&decoder) as usize;
                    max_data_size = max(max_data_size, iterations * decoder.byte_value_count());
                } else {
                    panic!(format!(
                        "get_decoder({:?},{:?},{:?}) failed.",
                        format, version, bpv
                    ));
                }
            }
        }
        unsafe { MAX_DATA_SIZE = max_data_size };
    });
    unsafe { MAX_DATA_SIZE }
}

fn encoded_size(format: Format, version: i32, bits_per_value: i32) -> i32 {
    format.byte_count(version, BLOCK_SIZE, bits_per_value) as i32
}

struct ForUtilInstance {
    encoded_sizes: [i32; 32],
    decoders: MaybeUninit<[BulkOperationEnum; 32]>,
    encoders: MaybeUninit<[BulkOperationEnum; 32]>,
    iterations: [i32; 32],
}

impl Drop for ForUtilInstance {
    fn drop(&mut self) {
        unsafe {
            ptr::drop_in_place(self.decoders.as_mut_ptr());
            ptr::drop_in_place(self.encoders.as_mut_ptr());
        }
    }
}

impl ForUtilInstance {
    fn with_input(input: &mut dyn IndexInput) -> Result<ForUtilInstance> {
        let packed_ints_version = input.read_vint()?;
        check_version(packed_ints_version)?;
        let mut encoded_sizes = [0; 32];
        let mut iterations = [0; 32];
        let mut decoders = MaybeUninit::<[BulkOperationEnum; 32]>::uninit();
        let mut encoders = MaybeUninit::<[BulkOperationEnum; 32]>::uninit();

        for bpv in 0..32 {
            let code = input.read_vint()?;
            let format_id = ((code as usize) >> 5) as i32;
            let bits_per_value = (code & 31) + 1;
            let format = Format::with_id(format_id);
            encoded_sizes[bpv] = encoded_size(format, packed_ints_version, bits_per_value);
            unsafe {
                decoders.get_mut()[bpv] = get_decoder(format, packed_ints_version, bits_per_value)?;
                encoders.get_mut()[bpv] = get_encoder(format, packed_ints_version, bits_per_value)?;
                iterations[bpv] = compute_iterations(&decoders.get_ref()[bpv]);
            }
        }

        Ok(ForUtilInstance {
            encoded_sizes,
            decoders,
            encoders,
            // not used when read
            iterations,
        })
    }

    fn with_output<T: DataOutput + ?Sized>(
        acceptable_overhead_ratio: f32,
        output: &mut T,
    ) -> Result<Self> {
        output.write_vint(VERSION_CURRENT)?;

        let mut encoders = MaybeUninit::<[BulkOperationEnum; 32]>::uninit();
        let mut decoders = MaybeUninit::<[BulkOperationEnum; 32]>::uninit();
        let mut iterations = [0i32; 32];
        let mut encoded_sizes = [0i32; 32];

        for bpv in 1..33usize {
            let FormatAndBits {
                format,
                bits_per_value,
            } = FormatAndBits::fastest(BLOCK_SIZE, bpv as i32, acceptable_overhead_ratio);

            debug_assert!(format.is_supported(bits_per_value));
            debug_assert!(bits_per_value <= 32);
            encoded_sizes[bpv - 1] = encoded_size(format, VERSION_CURRENT, bits_per_value);
            unsafe {
                decoders.get_mut()[bpv - 1] = get_decoder(format, VERSION_CURRENT, bits_per_value)?;
                encoders.get_mut()[bpv - 1] = get_encoder(format, VERSION_CURRENT, bits_per_value)?;
                iterations[bpv - 1] = compute_iterations(&decoders.get_ref()[bpv - 1]);
            }

            output.write_vint(format.get_id() << 5 | (bits_per_value - 1))?;
        }

        Ok(ForUtilInstance {
            encoded_sizes,
            decoders,
            encoders,
            iterations,
        })
    }

    pub fn read_block(
        &self,
        input: &mut dyn IndexInput,
        encoded: &mut [u8],
        decoded: &mut [i32],
    ) -> Result<()> {
        let num_bits = input.read_byte()? as usize;
        debug_assert!(num_bits <= 32);

        if num_bits as i32 == ALL_VALUES_EQUAL {
            let value = input.read_vint()?;
            decoded[0..BLOCK_SIZE as usize]
                .iter_mut()
                .map(|x| *x = value)
                .count();
            return Ok(());
        }

        let encoded_size = self.encoded_sizes[num_bits - 1];
        input.read_exact(&mut encoded[0..encoded_size as usize])?;

        let decoder = unsafe { &self.decoders.get_ref()[num_bits - 1] };
        let iters = self.iterations[num_bits - 1] as usize;
        decoder.decode_byte_to_int(encoded, decoded, iters);
        Ok(())
    }

    pub fn skip_block(&self, input: &mut dyn IndexInput) -> Result<()> {
        let num_bits = input.read_byte()? as usize;
        if num_bits as i32 == ALL_VALUES_EQUAL {
            input.read_vint()?;
            return Ok(());
        }
        let encoded_size = self.encoded_sizes[num_bits - 1];
        let fp = input.file_pointer();
        input.seek(fp + i64::from(encoded_size))
    }
}

#[derive(Clone)]
pub struct ForUtil {
    instance: Arc<ForUtilInstance>,
}

impl ForUtil {
    pub fn with_input(input: &mut dyn IndexInput) -> Result<ForUtil> {
        Ok(ForUtil {
            instance: Arc::new(ForUtilInstance::with_input(input)?),
        })
    }

    pub fn with_output<T: DataOutput + ?Sized>(
        acceptable_overhead_ratio: f32,
        output: &mut T,
    ) -> Result<Self> {
        Ok(ForUtil {
            instance: Arc::new(ForUtilInstance::with_output(
                acceptable_overhead_ratio,
                output,
            )?),
        })
    }

    pub fn read_block(
        &self,
        input: &mut dyn IndexInput,
        encoded: &mut [u8],
        decoded: &mut [i32],
    ) -> Result<()> {
        self.instance.read_block(input, encoded, decoded)
    }

    fn is_all_equal(data: &[i32]) -> bool {
        assert!(!data.is_empty());
        let v = data[0];
        for i in &data[1..BLOCK_SIZE as usize] {
            if *i != v {
                return false;
            }
        }
        true
    }

    fn bits_required(data: &[i32]) -> i32 {
        let mut or = 0;
        for i in &data[..BLOCK_SIZE as usize] {
            debug_assert!(*i >= 0);
            or |= *i;
        }

        debug_assert!(or >= 0);
        or.bits_required() as i32
    }

    pub fn write_block(
        &self,
        data: &[i32],
        encoded: &mut [u8],
        out: &mut impl IndexOutput,
    ) -> Result<()> {
        if Self::is_all_equal(data) {
            out.write_byte(0)?;
            return out.write_vint(data[0]);
        }

        let num_bits = Self::bits_required(data) as usize;
        assert!(num_bits > 0 && num_bits <= 32);

        let iters = self.instance.iterations[num_bits - 1];
        let encoder = unsafe { &self.instance.encoders.get_ref()[num_bits - 1] };
        assert!(iters * encoder.byte_value_count() as i32 >= BLOCK_SIZE);
        let encoded_size = self.instance.encoded_sizes[num_bits - 1];
        debug_assert!(iters * encoder.byte_block_count() as i32 >= encoded_size);

        out.write_byte(num_bits as u8)?;
        encoder.encode_int_to_byte(data, encoded, iters as usize);
        out.write_bytes(encoded, 0, encoded_size as usize)
    }

    pub fn skip_block(&self, input: &mut dyn IndexInput) -> Result<()> {
        self.instance.skip_block(input)
    }
}
