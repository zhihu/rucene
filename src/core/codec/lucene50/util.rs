use std::cmp::max;
use std::sync::{Arc, Once, ONCE_INIT};

use core::codec::lucene50::posting::BLOCK_SIZE;
use core::store::IndexInput;
use core::util::packed::packed_misc::*;
use error::*;

/// Special number of bits per value used whenever all values to encode are equal.
///
const ALL_VALUES_EQUAL: i32 = 0;

/// Upper limit of the number of bytes that might be required to stored
/// <code>BLOCK_SIZE</code> encoded values.
///
pub const MAX_ENCODED_SIZE: usize = BLOCK_SIZE as usize * 4;

/// Upper limit of the number of values that might be decoded in a single call to
/// {@link #readBlock(IndexInput, byte[], int[])}. Although values after
/// <code>BLOCK_SIZE</code> are garbage, it is necessary to allocate value buffers
/// whose size is {@code >= MAX_DATA_SIZE} to avoid {@link ArrayIndexOutOfBoundsException}s.
static mut MAX_DATA_SIZE: usize = 0;

static START: Once = ONCE_INIT;

fn compute_iterations(decoder: &PackedIntDecoder) -> i32 {
    (BLOCK_SIZE as f32 / decoder.byte_value_count() as f32).ceil() as i32
}

pub fn max_data_size() -> usize {
    START.call_once(|| {
        let mut max_data_size: usize = 0;
        for version in VERSION_START..VERSION_CURRENT + 1 {
            let format = Format::Packed;
            for bpv in 1..33 {
                if let Ok(decoder) = get_decoder(&format, version, bpv) {
                    let iterations = compute_iterations(decoder.as_ref()) as usize;
                    max_data_size = max(max_data_size, iterations * decoder.byte_value_count());
                } else {
                    assert!(
                        false,
                        format!("get_decoder({:?},{:?},{:?}) failed.", format, version, bpv)
                    );
                }
            }
            let format = Format::PackedSingleBlock;
            for bpv in 1..33 {
                if let Ok(decoder) = get_decoder(&format, version, bpv) {
                    let iterations = compute_iterations(decoder.as_ref()) as usize;
                    max_data_size = max(max_data_size, iterations * decoder.byte_value_count());
                } else {
                    assert!(
                        false,
                        format!("get_decoder({:?},{:?},{:?}) failed.", format, version, bpv)
                    );
                }
            }
        }
        unsafe { MAX_DATA_SIZE = max_data_size };
    });
    unsafe { MAX_DATA_SIZE }
}

fn encoded_size(format: &Format, version: i32, bits_per_value: i32) -> i32 {
    format.byte_count(version, BLOCK_SIZE, bits_per_value) as i32
}

struct ForUtilInstance {
    encoded_sizes: [i32; 33],
    decoders: Vec<Box<PackedIntDecoder>>,
    iterations: [i32; 33],
}

impl ForUtilInstance {
    pub fn with_input(mut input: Box<IndexInput>) -> Result<ForUtilInstance> {
        let packed_ints_version = input.read_vint()?;
        check_version(packed_ints_version)?;
        let mut encoded_sizes = [0 as i32; 33];
        let mut iterations = [0 as i32; 33];
        let mut decoders = Vec::with_capacity(33);

        for bpv in 1..33 {
            let code = input.read_vint()?;
            let format_id = ((code as usize) >> 5) as i32;
            let bits_per_value = (code & 31) + 1;
            let format = Format::with_id(format_id);
            encoded_sizes[bpv] = encoded_size(&format, packed_ints_version, bits_per_value);
            if bpv == 1 {
                decoders.push(get_decoder(&format, packed_ints_version, bits_per_value)?);
            }
            decoders.push(get_decoder(&format, packed_ints_version, bits_per_value)?);
            iterations[bpv] = compute_iterations(decoders[bpv].as_ref());
        }

        Ok(ForUtilInstance {
            encoded_sizes,
            decoders,
            iterations,
        })
    }

    pub fn read_block(
        &self,
        input: &mut IndexInput,
        encoded: &mut [u8],
        decoded: &mut [i32],
    ) -> Result<()> {
        let num_bits = input.read_byte()? as usize;

        if num_bits as i32 == ALL_VALUES_EQUAL {
            let value = input.read_vint()?;
            decoded[0..BLOCK_SIZE as usize]
                .iter_mut()
                .map(|x| *x = value)
                .count();
            return Ok(());
        }

        let encoded_size = self.encoded_sizes[num_bits];
        input.read_exact(&mut encoded[0..encoded_size as usize])?;

        let decoder = &self.decoders[num_bits];
        let iters = self.iterations[num_bits] as usize;
        decoder.decode_byte_to_int(encoded, decoded, iters);
        Ok(())
    }

    pub fn skip_block(&self, input: &mut IndexInput) -> Result<()> {
        let num_bits = input.read_byte()? as usize;
        if num_bits as i32 == ALL_VALUES_EQUAL {
            input.read_vint()?;
            return Ok(());
        }
        let encoded_size = self.encoded_sizes[num_bits];
        let fp = input.file_pointer();
        input.seek(fp + i64::from(encoded_size))
    }
}

#[derive(Clone)]
pub struct ForUtil {
    instance: Arc<ForUtilInstance>,
}

impl ForUtil {
    pub fn with_input(input: Box<IndexInput>) -> Result<ForUtil> {
        Ok(ForUtil {
            instance: Arc::new(ForUtilInstance::with_input(input)?),
        })
    }

    pub fn read_block(
        &self,
        input: &mut IndexInput,
        encoded: &mut [u8],
        decoded: &mut [i32],
    ) -> Result<()> {
        self.instance.read_block(input, encoded, decoded)
    }

    pub fn skip_block(&self, input: &mut IndexInput) -> Result<()> {
        self.instance.skip_block(input)
    }
}
