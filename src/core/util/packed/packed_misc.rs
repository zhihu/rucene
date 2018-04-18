use core::codec::codec_util;
use core::store::IndexInput;
use core::store::{DataInput, DataOutput};
use core::util::bit_util::ZigZagEncoding;
use error::*;
use std::cmp::min;

/// Simplistic compression for array of unsigned long values.
/// Each value is {@code >= 0} and {@code <=} a specified maximum value.  The
/// values are stored as packed ints, with each value
/// consuming a fixed number of bits.
///
/// @lucene.internal
pub const FASTEST: f32 = 7.0;

/// At most 50% memory overhead, always select a reasonably fast implementation.
///
pub const FAST: f32 = 0.5;

/// At most 25% memory overhead.
///
pub const DEFAULT: f32 = 0.25;

/// No memory overhead at all, but the returned implementation may be slow.
///
pub const COMPACT: f32 = 0.0;

/// Default amount of memory to use for bulk operations.
///
pub const DEFAULT_BUFFER_SIZE: i32 = 1024; // 1K

pub const CODEC_NAME: &str = "PackedInts";
pub const VERSION_MONOTONIC_WITHOUT_ZIGZAG: i32 = 2;
pub const VERSION_START: i32 = VERSION_MONOTONIC_WITHOUT_ZIGZAG;
pub const VERSION_CURRENT: i32 = VERSION_MONOTONIC_WITHOUT_ZIGZAG;

pub fn check_version(version: i32) -> Result<()> {
    if version < VERSION_START {
        bail!(
            "Version is too old, should be at least {} (got {})",
            VERSION_START,
            version
        )
    } else if version > VERSION_CURRENT {
        bail!(
            "Version is too new, should be at most {} (got {})",
            VERSION_CURRENT,
            version
        )
    } else {
        Ok(())
    }
}

pub fn get_writer_no_header(
    format: Format,
    value_count: usize,
    bits_per_value: i32,
    mem: i32,
) -> Box<Writer> {
    Box::new(PackedWriter::new(
        format,
        value_count as i32,
        bits_per_value,
        mem,
    ))
}

pub fn get_reader_no_header<T: DataInput + ?Sized>(
    input: &mut T,
    format: Format,
    version: i32,
    value_count: usize,
    bits_per_value: i32,
) -> Result<Box<Reader>> {
    check_version(version)?;
    let reader: Box<Reader> = match format {
        Format::PackedSingleBlock => {
            Packed64SingleBlock::create(input, value_count, bits_per_value)?
        }
        Format::Packed => match bits_per_value {
            8 => Box::new(Direct8::from_input(version, input, value_count)?),
            16 => Box::new(Direct16::from_input(version, input, value_count)?),
            32 => Box::new(Direct32::from_input(version, input, value_count)?),
            64 => Box::new(Direct64::from_input(version, input, value_count)?),
            24 if value_count <= PACKED8_THREE_BLOCKS_MAX_SIZE as usize => {
                Box::new(Packed8ThreeBlocks::from_input(version, input, value_count)?)
            }
            48 if value_count <= PACKED16_THREE_BLOCKS_MAX_SIZE as usize => Box::new(
                Packed16ThreeBlocks::from_input(version, input, value_count)?,
            ),
            _ => Box::new(Packed64::from_input(
                version,
                input,
                value_count,
                bits_per_value,
            )?),
        },
    };
    Ok(reader)
}

pub fn get_reader<T: DataInput + ?Sized>(input: &mut T) -> Result<Box<Reader>> {
    let version = codec_util::check_header(input, CODEC_NAME, VERSION_START, VERSION_CURRENT)?;
    let bits_per_value = input.read_vint()?;
    debug_assert!(
        bits_per_value > 0 && bits_per_value <= 64,
        "bits_per_value={}",
        bits_per_value
    );
    let value_count = input.read_vint()? as usize;
    let format = Format::with_id(input.read_vint()?);
    get_reader_no_header(input, format, version, value_count, bits_per_value)
}

/// Expert: Restore a {@link ReaderIterator} from a stream without reading
/// metadata at the beginning of the stream. This method is useful to restore
/// data from streams which have been created using
/// {@link PackedInts#getWriterNoHeader(DataOutput, Format, int, int, int)}.
///
/// @param in           the stream to read data from, positioned at the beginning of the packed
/// values @param format       the format used to serialize
/// @param version      the version used to serialize the data
/// @param valueCount   how many values the stream holds
/// @param bitsPerValue the number of bits per value
/// @param mem          how much memory the iterator is allowed to use to read-ahead (likely to
/// speed up iteration) @return             a ReaderIterator
/// @see PackedInts#getWriterNoHeader(DataOutput, Format, int, int, int)
/// @lucene.internal
///
pub fn get_reader_iterator_no_header(
    format: Format,
    version: i32,
    value_count: usize,
    bits_per_value: i32,
    mem: i32,
) -> Result<PackedReaderIterator> {
    check_version(version)?;
    Ok(PackedReaderIterator::new(
        format,
        version,
        value_count,
        bits_per_value,
        mem,
    ))
}

/// Create a packed integer array with the given amount of values initialized
/// to 0. the valueCount and the bitsPerValue cannot be changed after creation.
/// All Mutables known by this factory are kept fully in RAM.
///
/// Positive values of <code>acceptableOverheadRatio</code> will trade space
/// for speed by selecting a faster but potentially less memory-efficient
/// implementation. An <code>acceptableOverheadRatio</code> of
/// {@link PackedInts#COMPACT} will make sure that the most memory-efficient
/// implementation is selected whereas {@link PackedInts#FASTEST} will make sure
/// that the fastest implementation is selected.
///
/// @param valueCount   the number of elements
/// @param bitsPerValue the number of bits available for any given value
/// @param acceptableOverheadRatio an acceptable overhead
///        ratio per value
/// @return a mutable packed integer array
///
pub fn get_mutable_by_ratio(
    value_count: usize,
    bits_per_value: i32,
    acceptable_overhead_ratio: f32,
) -> Box<Mutable> {
    let format_and_bits = FormatAndBits::fastest(
        value_count as i32,
        bits_per_value,
        acceptable_overhead_ratio,
    );
    get_mutable_by_format(
        value_count,
        format_and_bits.bits_per_value,
        format_and_bits.format,
    )
}

/// Same as {@link #getMutable(int, int, float)} with a pre-computed number of bits per value and
/// format.
///
pub fn get_mutable_by_format(
    value_count: usize,
    bits_per_value: i32,
    format: Format,
) -> Box<Mutable> {
    debug_assert!(value_count > 0);
    match format {
        Format::PackedSingleBlock => {
            Box::new(Packed64SingleBlock::new(value_count, bits_per_value))
        }
        Format::Packed => match bits_per_value {
            8 => Box::new(Direct8::new(value_count)),
            16 => Box::new(Direct16::new(value_count)),
            32 => Box::new(Direct32::new(value_count)),
            64 => Box::new(Direct64::new(value_count)),
            24 if value_count <= PACKED8_THREE_BLOCKS_MAX_SIZE as usize => {
                Box::new(Packed8ThreeBlocks::new(value_count))
            }
            48 if value_count <= PACKED16_THREE_BLOCKS_MAX_SIZE as usize => {
                Box::new(Packed16ThreeBlocks::new(value_count))
            }
            _ => Box::new(Packed64::new(value_count, bits_per_value)),
        },
    }
}

pub fn max_value(bits_per_value: i32) -> i64 {
    debug_assert!(bits_per_value <= 64);
    if bits_per_value == 64 {
        i64::max_value()
    } else {
        (1i64 << (bits_per_value + 1)) - 1
    }
}

pub fn packed_ints_copy(
    src: &Reader,
    src_pos: usize,
    dest: &mut Mutable,
    dest_pos: usize,
    len: usize,
    mem: i32,
) {
    debug_assert!(src_pos + len <= src.size());
    debug_assert!(dest_pos + len <= dest.size());

    let capacity = rshift_32(mem, 3) as usize;
    if capacity == 0 {
        let mut cur_dest_pos = dest_pos;
        let mut cur_src_pos = src_pos;
        for _ in 0..len {
            dest.set(cur_dest_pos, src.get(cur_src_pos));
            cur_dest_pos += 1;
            cur_src_pos += 1;
        }
    } else if len > 0 {
        let mut buf = vec![0i64; min(len, capacity)];
        copy_by_buf(src, src_pos, dest, dest_pos, len, buf.as_mut());
    }
}

fn copy_by_buf(
    src: &Reader,
    src_pos: usize,
    dest: &mut Mutable,
    dest_pos: usize,
    len: usize,
    buf: &mut [i64],
) {
    debug_assert!(!buf.is_empty());
    let mut remain = 0usize;
    let mut cur_len = len;
    let mut src_pos = src_pos;
    let mut dest_pos = dest_pos;
    while cur_len > 0 {
        let get_len = cur_len.min(buf.len() - remain);
        let read = src.bulk_get(src_pos, &mut buf[remain..], get_len);
        debug_assert!(read > 0);
        src_pos += read;
        cur_len -= read;
        remain += read;
        let written = dest.bulk_set(dest_pos, buf, 0, remain);
        debug_assert!(written > 0);
        dest_pos += written;
        if written < remain {
            for i in 0..remain - written {
                buf[i] = buf[i + written]
            }
        }
        remain -= written;
    }
    while remain > 0 {
        let written = dest.bulk_set(dest_pos, buf, 0, remain);
        dest_pos += written;
        remain -= written;
        for i in 0usize..remain {
            buf[i] = buf[written + i];
        }
    }
}

/// `>>>` operator for i64
pub fn rshift_64(value: i64, bits: i32) -> i64 {
    ((value as u64) >> bits) as i64
}

/// `>>>` operator for i32
pub fn rshift_32(value: i32, bits: i32) -> i32 {
    ((value as u32) >> bits) as i32
}

pub fn unsigned_bits_required(bits: i64) -> i32 {
    1.max(64 - bits.leading_zeros() as i32)
}

/// A format to write packed ints.
#[derive(Copy, Clone, Debug)]
pub enum Format {
    /// Compact format, all bits are written contiguously.
    Packed,
    /// A format that may insert padding bits to improve encoding and decoding
    /// speed. Since this format doesn't support all possible bits per value, you
    /// should never use it directly, but rather use
    /// {@link PackedInts#fastestFormatAndBits(int, int, float)} to find the
    /// format that best suits your needs.
    PackedSingleBlock,
}

impl Format {
    /// Computes how many byte blocks are needed to store <code>values</code>
    /// values of size <code>bits_per_value</code>.

    pub fn with_id(id: i32) -> Format {
        match id {
            0 => Format::Packed,
            1 => Format::PackedSingleBlock,
            _ => panic!("Invalid format id"),
        }
    }

    pub fn get_id(&self) -> i32 {
        match *self {
            Format::Packed => 0,
            Format::PackedSingleBlock => 1,
        }
    }

    pub fn byte_count(
        &self,
        packed_ints_version: i32,
        value_count: i32,
        bits_per_value: i32,
    ) -> i64 {
        match *self {
            Format::Packed => (f64::from(value_count * bits_per_value) / 8f64).ceil() as i64,
            _ => i64::from(8 * self.long_count(packed_ints_version, value_count, bits_per_value)),
        }
    }
    /// Computes how many long blocks are needed to store <code>values</code>
    /// values of size <code>bits_per_value</code>.
    pub fn long_count(
        &self,
        packed_ints_version: i32,
        value_count: i32,
        bits_per_value: i32,
    ) -> i32 {
        match *self {
            Format::Packed => {
                let byte_count = self.byte_count(packed_ints_version, value_count, bits_per_value);
                if (byte_count % 8) == 0 {
                    (byte_count / 8) as i32
                } else {
                    (byte_count / 8 + 1) as i32
                }
            }
            _ => {
                let values_per_block = 64 / bits_per_value;
                (f64::from(value_count) / f64::from(values_per_block)).ceil() as i32
            }
        }
    }

    /// Tests whether the provided number of bits per value is supported by the
    /// format.
    pub fn is_supported(&self, bits_per_value: i32) -> bool {
        match *self {
            Format::Packed => bits_per_value >= 1 && bits_per_value <= 64,
            _ => Packed64SingleBlock::is_supported(bits_per_value),
        }
    }

    /// Returns the overhead per value, in bits.
    pub fn overhead_per_value(&self, bits_per_value: i32) -> f32 {
        match *self {
            Format::Packed => 0f32,
            _ => {
                let values_per_block = 64 / bits_per_value;
                let overhead = 64 % bits_per_value;
                overhead as f32 / values_per_block as f32
            }
        }
    }

    /// Returns the overhead ratio (<code>overhead per value / bits per value</code>).
    pub fn overhead_ratio(&self, bits_per_value: i32) -> f32 {
        self.overhead_per_value(bits_per_value) / bits_per_value as f32
    }
}

pub struct FormatAndBits {
    pub format: Format,
    pub bits_per_value: i32,
}

impl FormatAndBits {
    /// Try to find the {@link Format} and number of bits per value that would
    /// restore from disk the fastest reader whose overhead is less than
    /// <code>acceptableOverheadRatio</code>.
    /// <p>
    /// The <code>acceptableOverheadRatio</code> parameter makes sense for
    /// random-access {@link Reader}s. In case you only plan to perform
    /// sequential access on this stream later on, you should probably use
    /// {@link PackedInts#COMPACT}.
    /// <p>
    /// If you don't know how many values you are going to write, use
    /// <code>valueCount = -1</code>.
    ///
    pub fn fastest(
        value_count: i32,
        bits_per_value: i32,
        acceptable_overhead_ratio: f32,
    ) -> FormatAndBits {
        let value_count = if value_count == -1 {
            i32::max_value()
        } else {
            value_count
        };

        let mut acceptable_overhead_ratio = COMPACT.max(acceptable_overhead_ratio);
        acceptable_overhead_ratio = FASTEST.min(acceptable_overhead_ratio);
        let acceptable_overhead_per_value = acceptable_overhead_ratio * bits_per_value as f32; // in bits
        let max_bits_per_value = bits_per_value + acceptable_overhead_per_value as i32;
        let mut actual_bits_per_value = -1;
        let mut format = Format::Packed;
        if bits_per_value <= 8 && max_bits_per_value >= 8 {
            actual_bits_per_value = 8;
        } else if bits_per_value <= 16 && max_bits_per_value >= 16 {
            actual_bits_per_value = 16;
        } else if bits_per_value <= 32 && max_bits_per_value >= 32 {
            actual_bits_per_value = 32;
        } else if bits_per_value <= 64 && max_bits_per_value >= 64 {
            actual_bits_per_value = 64;
        } else if value_count <= PACKED8_THREE_BLOCKS_MAX_SIZE && bits_per_value <= 24
            && max_bits_per_value >= 24
        {
            actual_bits_per_value = 24;
        } else if value_count <= PACKED16_THREE_BLOCKS_MAX_SIZE && bits_per_value <= 48
            && max_bits_per_value >= 48
        {
            actual_bits_per_value = 48;
        } else {
            for bpv in bits_per_value..max_bits_per_value {
                if Format::PackedSingleBlock.is_supported(bpv) {
                    let overhead = Format::PackedSingleBlock.overhead_per_value(bpv);
                    let acceptable_overhead =
                        acceptable_overhead_per_value + bits_per_value as f32 - bpv as f32;
                    if overhead <= acceptable_overhead {
                        actual_bits_per_value = bpv;
                        format = Format::PackedSingleBlock;
                        break;
                    }
                }
            }
            if actual_bits_per_value < 0 {
                actual_bits_per_value = bits_per_value;
            }
        }

        FormatAndBits {
            format,
            bits_per_value: actual_bits_per_value,
        }
    }
}

pub const PACKED8_THREE_BLOCKS_MAX_SIZE: i32 = i32::max_value() / 3;
pub const PACKED16_THREE_BLOCKS_MAX_SIZE: i32 = i32::max_value() / 3;

/// A decoder for packed integers.
pub trait PackedIntDecoder: Send {
    /// The minimum number of long blocks to encode in a single iteration, when
    /// using long encoding.
    fn long_block_count(&self) -> usize;

    /// The number of values that can be stored in {@link #longBlockCount()} long
    /// blocks.
    fn long_value_count(&self) -> usize;

    /// The minimum number of byte blocks to encode in a single iteration, when
    /// using byte encoding.
    fn byte_block_count(&self) -> usize;

    /// The number of values that can be stored in {@link #byteBlockCount()} byte
    /// blocks.
    fn byte_value_count(&self) -> usize;

    /// Read <code>iterations * blockCount()</code> blocks from <code>blocks</code>,
    /// decode them and write <code>iterations * valueCount()</code> values into
    /// <code>values</code>.
    ///
    /// @param blocks       the long blocks that hold packed integer values
    /// @param values       the values buffer
    /// @param iterations   controls how much data to decode
    fn decode_long_to_long(&self, blocks: &[i64], values: &mut [i64], iterations: usize);

    /// Read <code>8 * iterations * blockCount()</code> blocks from <code>blocks</code>,
    /// decode them and write <code>iterations * valueCount()</code> values into
    /// <code>values</code>.
    ///
    /// @param blocks       the long blocks that hold packed integer values
    /// @param values       the values buffer
    /// @param iterations   controls how much data to decode
    fn decode_byte_to_long(&self, blocks: &[u8], values: &mut [i64], iterations: usize);

    /// Read <code>iterations * blockCount()</code> blocks from <code>blocks</code>,
    /// decode them and write <code>iterations * valueCount()</code> values into
    /// <code>values</code>.
    ///
    /// @param blocks       the long blocks that hold packed integer values
    /// @param values       the values buffer
    /// @param iterations   controls how much data to decode
    fn decode_long_to_int(&self, blocks: &[i64], values: &mut [i32], iterations: usize);

    /// Read <code>8 * iterations * blockCount()</code> blocks from <code>blocks</code>,
    /// decode them and write <code>iterations * valueCount()</code> values into
    /// <code>values</code>.
    ///
    /// @param blocks       the long blocks that hold packed integer values
    /// @param values       the values buffer
    /// @param iterations   controls how much data to decode
    fn decode_byte_to_int(&self, blocks: &[u8], values: &mut [i32], iterations: usize);
}

/// An encoder for packed integers.
pub trait PackedIntEncoder {
    /// The minimum number of long blocks to encode in a single iteration, when
    /// using long encoding.
    fn long_block_count(&self) -> usize;

    /// The number of values that can be stored in {@link #long_block_count()} long
    /// blocks.
    fn long_value_count(&self) -> usize;

    /// The minimum number of byte blocks to encode in a single iteration, when
    /// using byte encoding.
    fn byte_block_count(&self) -> usize;

    /// The number of values that can be stored in {@link #byte_block_count()} byte
    /// blocks.
    fn byte_value_count(&self) -> usize;

    /// Read <code>iterations * valueCount()</code> values from <code>values</code>,
    /// encode them and write <code>iterations * blockCount()</code> blocks into
    /// <code>blocks</code>.
    /// @param blocks       the long blocks that hold packed integer values
    /// @param values       the values buffer
    /// @param iterations   controls how much data to encode
    fn encode_long_to_long(&self, values: &[i64], blocks: &mut [i64], iterations: usize);

    /// Read <code>iterations * valueCount()</code> values from <code>values</code>,
    /// encode them and write <code>8 * iterations * blockCount()</code> blocks into
    /// <code>blocks</code>.
    ///
    /// @param blocks       the long blocks that hold packed integer values
    /// @param values       the values buffer
    /// @param iterations   controls how much data to encode
    fn encode_long_to_byte(&self, values: &[i64], blocks: &mut [u8], iterations: usize);

    /// Read <code>iterations * valueCount()</code> values from <code>values</code>,
    /// encode them and write <code>iterations * blockCount()</code> blocks into
    /// <code>blocks</code>.
    ///
    /// @param blocks       the long blocks that hold packed integer values
    /// @param values       the values buffer
    /// @param iterations   controls how much data to encode
    fn encode_int_to_long(&self, values: &[i32], blocks: &mut [i64], iterations: usize);

    /// Read <code>iterations * valueCount()</code> values from <code>values</code>,
    /// encode them and write <code>8 * iterations * blockCount()</code> blocks into
    /// <code>blocks</code>.
    ///
    /// @param blocks       the long blocks that hold packed integer values
    /// @param values       the values buffer
    /// @param iterations   controls how much data to encode
    fn encode_int_to_byte(&self, values: &[i32], blocks: &mut [u8], iterations: usize);
}

/// A read-only random access array of positive integers.
/// @lucene.internal
/// A per-document numeric value.
pub trait Reader: Send + Sync {
    /// Returns the numeric value for the specified document ID.
    /// @param doc_id document ID to lookup
    /// @return numeric value
    fn get(&self, doc_id: usize) -> i64;

    /// Bulk get: read at least one and at most <code>len</code> longs starting
    /// from <code>index</code> into <code>arr[off:off+len]</code> and return
    /// the actual number of values that have been read.
    fn bulk_get(&self, index: usize, output: &mut [i64], len: usize) -> usize {
        let gets = min(self.size() - index, len);
        for i in index..index + gets {
            output[i - index] = self.get(i);
        }
        gets
    }

    /// @return the number of values.
    fn size(&self) -> usize;
}

pub trait Mutable: Reader {
    /// @return the number of bits used to store any given value.
    ///        Note: This does not imply that memory usage is
    ///        {@code bitsPerValue * #values} as implementations are free to
    ///        use non-space-optimal packing of bits.
    ///
    fn get_bits_per_value(&self) -> i32;

    /// Set the value at the given index in the array.
    /// @param index where the value should be positioned.
    /// @param value a value conforming to the constraints set by the array.
    ///
    fn set(&mut self, index: usize, value: i64);

    /// Bulk set: set at least one and at most <code>len</code> longs starting
    /// at <code>off</code> in <code>arr</code> into this mutable, starting at
    /// <code>index</code>. Returns the actual number of values that have been
    /// set.
    ///
    fn bulk_set(&mut self, index: usize, arr: &[i64], off: usize, len: usize) -> usize {
        debug_assert!(len > 0);
        debug_assert!(index as i64 >= 0 && index < self.size());
        let len = min(len, self.size() - index);
        debug_assert!(off + len < arr.len());
        let mut i = index;
        let mut o = off;
        while i < index + len {
            self.set(i, arr[o]);
            i += 1;
            o += 1;
        }
        len
    }

    /// Fill the mutable from <code>fromIndex</code> (inclusive) to
    /// <code>toIndex</code> (exclusive) with <code>val</code>.
    fn fill(&mut self, from: usize, to: usize, val: i64) {
        debug_assert!(val <= max_value(self.get_bits_per_value()));
        debug_assert!(from <= to);
        for i in from..to {
            self.set(i, val);
        }
    }

    fn clear(&mut self) {
        let len = self.size();
        self.fill(0, len, 0);
    }

    fn save(&self, out: &mut DataOutput) -> Result<()> {
        let mut writer = get_writer_no_header(
            self.get_format(),
            self.size(),
            self.get_bits_per_value(),
            DEFAULT_BUFFER_SIZE,
        );
        writer.write_header(out)?;
        for i in 0..self.size() {
            writer.add(self.get(i), out)?;
        }
        writer.finish(out)
    }

    fn get_format(&self) -> Format {
        Format::Packed
    }

    fn as_reader(&self) -> &Reader;
}

pub struct Direct8 {
    value_count: usize,
    bits_per_value: i32,
    values: Vec<u8>,
}

impl Direct8 {
    pub fn new(value_count: usize) -> Direct8 {
        Direct8 {
            value_count,
            bits_per_value: 8,
            values: vec![0u8; value_count],
        }
    }

    pub fn from_input<T: DataInput + ?Sized>(
        packed_ints_version: i32,
        input: &mut T,
        value_count: usize,
    ) -> Result<Direct8> {
        let mut res = Direct8::new(value_count);
        input.read_bytes(res.values.as_mut(), 0, value_count)?;
        // because packed ints have not always been byte-aligned
        let remain = Format::Packed.byte_count(packed_ints_version, value_count as i32, 8)
            - value_count as i64;
        for _i in 0..remain {
            let _ = input.read_byte();
        }
        Ok(res)
    }
}

impl Reader for Direct8 {
    fn get(&self, doc_id: usize) -> i64 {
        i64::from(self.values[doc_id])
    }

    fn bulk_get(&self, index: usize, output: &mut [i64], len: usize) -> usize {
        debug_assert!(len > 0, "len must be > 0 (got {})", len);
        debug_assert!(index as i64 >= 0 && index < self.value_count);
        debug_assert!(len <= output.len());

        let gets = len.min(self.value_count - index);
        for (i, o) in output.iter_mut().enumerate().take(gets) {
            *o = self.get(index + i);
        }
        gets
    }

    fn size(&self) -> usize {
        self.value_count
    }
}

impl Mutable for Direct8 {
    fn get_bits_per_value(&self) -> i32 {
        self.bits_per_value
    }

    fn set(&mut self, index: usize, value: i64) {
        self.values[index] = value as u8;
    }

    fn clear(&mut self) {
        let len = self.values.len();
        self.fill(0, len, 0i64);
    }
    fn bulk_set(&mut self, index: usize, arr: &[i64], off: usize, len: usize) -> usize {
        debug_assert!(len > 0);
        debug_assert!(index < self.value_count);
        debug_assert!(off + len < arr.len());

        let sets = len.min(self.value_count - index);
        let mut i = index;
        let mut o = off;
        while i < index + sets {
            self.set(i, arr[o]);
            i += 1;
            o += 1;
        }
        sets
    }

    fn fill(&mut self, from: usize, to: usize, val: i64) {
        debug_assert_eq!(val, val & 0xffi64);
        for i in from..to {
            self.set(i, val);
        }
    }

    fn as_reader(&self) -> &Reader {
        self
    }
}

pub struct Direct16 {
    value_count: usize,
    bits_per_value: i32,
    values: Vec<i16>,
}

impl Direct16 {
    pub fn new(value_count: usize) -> Direct16 {
        Direct16 {
            value_count,
            bits_per_value: 16,
            values: vec![0i16; value_count],
        }
    }

    pub fn from_input<T: DataInput + ?Sized>(
        packed_ints_version: i32,
        input: &mut T,
        value_count: usize,
    ) -> Result<Direct16> {
        let mut res = Direct16::new(value_count);
        for i in 0..res.value_count {
            res.values[i] = input.read_short()?;
        }
        let remain = Format::Packed.byte_count(packed_ints_version, value_count as i32, 16)
            - 2i64 * (value_count as i64);
        for _i in 0..remain {
            let _ = input.read_byte();
        }
        Ok(res)
    }
}

impl Reader for Direct16 {
    fn get(&self, doc_id: usize) -> i64 {
        i64::from(self.values[doc_id] as u16)
    }

    fn bulk_get(&self, index: usize, output: &mut [i64], len: usize) -> usize {
        debug_assert!(len > 0);
        debug_assert!(index as i64 >= 0 && index < self.value_count);
        debug_assert!(len <= output.len());

        let gets = len.min(self.value_count - index);
        for (i, o) in output.iter_mut().enumerate().take(gets) {
            *o = self.get(index + i);
        }
        gets
    }

    fn size(&self) -> usize {
        self.value_count
    }
}

impl Mutable for Direct16 {
    fn get_bits_per_value(&self) -> i32 {
        self.bits_per_value
    }

    fn set(&mut self, index: usize, value: i64) {
        self.values[index] = value as i16;
    }

    fn bulk_set(&mut self, index: usize, arr: &[i64], off: usize, len: usize) -> usize {
        debug_assert!(len > 0);
        debug_assert!(index as i64 >= 0 && index < self.value_count);
        debug_assert!(off + len < arr.len());

        let sets = len.min(self.value_count - index);
        let mut i = index;
        let mut o = off;
        while i < index + sets {
            self.set(i, arr[o]);
            i += 1;
            o += 1;
        }
        sets
    }

    fn fill(&mut self, from: usize, to: usize, val: i64) {
        debug_assert_eq!(val, val & 0xffffi64);
        for i in from..to {
            self.set(i, val);
        }
    }

    fn clear(&mut self) {
        let len = self.values.len();
        self.fill(0, len, 0i64);
    }

    fn as_reader(&self) -> &Reader {
        self
    }
}

pub struct Direct32 {
    value_count: usize,
    bits_per_value: i32,
    values: Vec<i32>,
}

impl Direct32 {
    pub fn new(value_count: usize) -> Direct32 {
        Direct32 {
            value_count,
            bits_per_value: 32,
            values: vec![0i32; value_count],
        }
    }

    pub fn from_input<T: DataInput + ?Sized>(
        packed_ints_version: i32,
        input: &mut T,
        value_count: usize,
    ) -> Result<Direct32> {
        let mut res = Direct32::new(value_count);
        for i in 0..res.value_count {
            res.values[i] = input.read_int()?;
        }
        let remain = Format::Packed.byte_count(packed_ints_version, value_count as i32, 32)
            - 4i64 * value_count as i64;
        for _i in 0..remain {
            let _ = input.read_byte();
        }
        Ok(res)
    }
}

impl Reader for Direct32 {
    fn get(&self, doc_id: usize) -> i64 {
        i64::from(self.values[doc_id] as u32)
    }

    fn bulk_get(&self, index: usize, output: &mut [i64], len: usize) -> usize {
        debug_assert!(len > 0);
        debug_assert!(index as i64 >= 0 && index < self.value_count);
        debug_assert!(len <= output.len());

        let gets = len.min(self.value_count - index);
        for (i, o) in output.iter_mut().enumerate().take(gets) {
            *o = self.get(index + i);
        }
        gets
    }

    fn size(&self) -> usize {
        self.value_count
    }
}

impl Mutable for Direct32 {
    fn get_bits_per_value(&self) -> i32 {
        self.bits_per_value
    }

    fn set(&mut self, index: usize, value: i64) {
        self.values[index] = value as i32;
    }

    fn bulk_set(&mut self, index: usize, arr: &[i64], off: usize, len: usize) -> usize {
        debug_assert!(len > 0);
        debug_assert!(index as i64 >= 0 && index < self.value_count);
        debug_assert!(off + len < arr.len());

        let sets = len.min(self.value_count - index);
        let mut i = index;
        let mut o = off;
        while i < index + sets {
            self.set(i, arr[o]);
            i += 1;
            o += 1;
        }
        sets
    }

    fn fill(&mut self, from: usize, to: usize, val: i64) {
        debug_assert_eq!(val, val & 0xffff_ffffi64);
        for i in from..to {
            self.set(i, val);
        }
    }

    fn clear(&mut self) {
        let len = self.values.len();
        self.fill(0, len, 0i64);
    }

    fn as_reader(&self) -> &Reader {
        self
    }
}

pub struct Direct64 {
    value_count: usize,
    bits_per_value: i32,
    values: Vec<i64>,
}

impl Direct64 {
    pub fn new(value_count: usize) -> Direct64 {
        Direct64 {
            value_count,
            bits_per_value: 64,
            values: vec![0i64; value_count],
        }
    }

    pub fn from_input<T: DataInput + ?Sized>(
        _packed_ints_version: i32,
        input: &mut T,
        value_count: usize,
    ) -> Result<Direct64> {
        let mut res = Direct64::new(value_count);
        for i in 0..res.value_count {
            res.values[i] = input.read_long()?;
        }
        Ok(res)
    }
}

impl Reader for Direct64 {
    fn get(&self, doc_id: usize) -> i64 {
        self.values[doc_id]
    }

    fn bulk_get(&self, index: usize, output: &mut [i64], len: usize) -> usize {
        debug_assert!(len > 0);
        debug_assert!(index as i64 >= 0 && index < self.value_count);
        debug_assert!(len <= output.len());

        let gets = len.min(self.value_count - index);
        for (i, o) in output.iter_mut().enumerate().take(gets) {
            *o = self.get(index + i);
        }
        gets
    }

    fn size(&self) -> usize {
        self.value_count
    }
}

impl Mutable for Direct64 {
    fn get_bits_per_value(&self) -> i32 {
        self.bits_per_value
    }

    fn set(&mut self, index: usize, value: i64) {
        self.values[index] = value;
    }

    fn bulk_set(&mut self, index: usize, arr: &[i64], off: usize, len: usize) -> usize {
        debug_assert!(len > 0);
        debug_assert!(index as i64 >= 0 && index < self.value_count);
        debug_assert!(off + len < arr.len());

        let sets = len.min(self.value_count - index);
        let mut i = index;
        let mut o = off;
        while i < index + sets {
            self.set(i, arr[o]);
            i += 1;
            o += 1;
        }
        sets
    }

    fn fill(&mut self, from: usize, to: usize, val: i64) {
        for i in from..to {
            self.set(i, val);
        }
    }

    fn clear(&mut self) {
        let len = self.values.len();
        self.fill(0, len, 0i64);
    }

    fn as_reader(&self) -> &Reader {
        self
    }
}

pub struct Packed8ThreeBlocks {
    value_count: usize,
    bits_per_value: i32,
    blocks: Vec<u8>,
}

impl Packed8ThreeBlocks {
    pub fn new(value_count: usize) -> Packed8ThreeBlocks {
        if value_count > PACKED8_THREE_BLOCKS_MAX_SIZE as usize {
            panic!("MAX_SIZE exceeded");
        }
        Packed8ThreeBlocks {
            value_count,
            bits_per_value: 24,
            blocks: vec![0u8; 3 * value_count],
        }
    }

    pub fn from_input<T: DataInput + ?Sized>(
        _packed_ints_version: i32,
        input: &mut T,
        value_count: usize,
    ) -> Result<Packed8ThreeBlocks> {
        let mut res = Packed8ThreeBlocks::new(value_count);
        input.read_bytes(res.blocks.as_mut(), 0, 3 * value_count)?;
        Ok(res)
    }
}

impl Reader for Packed8ThreeBlocks {
    fn get(&self, doc_id: usize) -> i64 {
        let o = doc_id * 3;
        (i64::from(self.blocks[o])) << 16 | (i64::from(self.blocks[o + 1])) << 8
            | (i64::from(self.blocks[o + 2]))
    }

    fn bulk_get(&self, index: usize, output: &mut [i64], len: usize) -> usize {
        debug_assert!(len > 0);
        debug_assert!(index as i64 >= 0 && index < self.value_count);
        debug_assert!(len <= output.len());

        let gets = len.min(self.value_count - index);
        for (i, o) in output.iter_mut().enumerate().take(gets) {
            *o = self.get(index + i);
        }
        gets
    }

    fn size(&self) -> usize {
        self.value_count
    }
}

impl Mutable for Packed8ThreeBlocks {
    fn get_bits_per_value(&self) -> i32 {
        self.bits_per_value
    }

    fn set(&mut self, index: usize, value: i64) {
        let o = index * 3;
        self.blocks[o] = (value >> 16) as u8; // 这里的移位操作之后的高位不会被用到，所以不必考虑需要使用那种符号的问题
        self.blocks[o + 1] = (value >> 8) as u8;
        self.blocks[o + 2] = value as u8;
    }

    fn bulk_set(&mut self, index: usize, arr: &[i64], off: usize, len: usize) -> usize {
        debug_assert!(len > 0);
        debug_assert!(index as i64 >= 0 && index < self.value_count);
        debug_assert!(off + len < arr.len());

        let sets = len.min(self.value_count - index);
        for i in 0..sets {
            self.set(index + i, arr[off + i]);
        }

        sets
    }

    fn fill(&mut self, from: usize, to: usize, val: i64) {
        for i in from..to {
            self.set(i, val);
        }
    }

    fn clear(&mut self) {
        let value_count = self.value_count;
        self.fill(0, value_count, 0i64);
    }

    fn as_reader(&self) -> &Reader {
        self
    }
}

const PACKED_16THREE_MAX_SIZE: i32 = i32::max_value() / 3;

pub struct Packed16ThreeBlocks {
    value_count: usize,
    bits_per_value: i32,
    blocks: Vec<i16>,
}

impl Packed16ThreeBlocks {
    pub fn new(value_count: usize) -> Packed16ThreeBlocks {
        if value_count > PACKED_16THREE_MAX_SIZE as usize {
            panic!("MAX_SIZE exceeded");
        }

        Packed16ThreeBlocks {
            value_count,
            bits_per_value: 48,
            blocks: vec![0i16; 3 * value_count],
        }
    }

    pub fn from_input<T: DataInput + ?Sized>(
        _packed_ints_version: i32,
        input: &mut T,
        value_count: usize,
    ) -> Result<Packed16ThreeBlocks> {
        let mut res = Packed16ThreeBlocks::new(value_count);
        for i in 0..3 * value_count {
            res.blocks[i] = input.read_short()?;
        }
        Ok(res)
    }
}

impl Reader for Packed16ThreeBlocks {
    fn get(&self, doc_id: usize) -> i64 {
        let o = doc_id * 3;
        i64::from(self.blocks[o] as u16) << 32 | i64::from(self.blocks[o + 1] as u16) << 16
            | i64::from(self.blocks[o + 2] as u16)
    }

    fn bulk_get(&self, index: usize, output: &mut [i64], len: usize) -> usize {
        debug_assert!(len > 0);
        debug_assert!(index as i64 >= 0 && index < self.value_count);
        debug_assert!(len <= output.len());

        let gets = len.min(self.value_count - index);
        for (i, o) in output.iter_mut().enumerate().take(gets) {
            *o = self.get(index + i);
        }
        gets
    }

    fn size(&self) -> usize {
        self.value_count
    }
}

impl Mutable for Packed16ThreeBlocks {
    fn get_bits_per_value(&self) -> i32 {
        self.bits_per_value
    }

    fn set(&mut self, index: usize, value: i64) {
        let o = index * 3;
        self.blocks[o] = (value >> 32) as i16;
        self.blocks[o + 1] = (value >> 16) as i16;
        self.blocks[o + 2] = value as i16;
    }

    fn bulk_set(&mut self, index: usize, arr: &[i64], off: usize, len: usize) -> usize {
        debug_assert!(len > 0);
        debug_assert!(index as i64 >= 0 && index < self.value_count);
        debug_assert!(off + len < arr.len());

        let sets = len.min(self.value_count - index);
        for i in 0..sets {
            self.set(index + i, arr[off + i]);
        }

        sets
    }

    fn fill(&mut self, from: usize, to: usize, val: i64) {
        for i in from..to {
            self.set(i, val);
        }
    }

    fn clear(&mut self) {
        let value_count = self.value_count;
        self.fill(0, value_count, 0i64);
    }

    fn as_reader(&self) -> &Reader {
        self
    }
}

/// Space optimized random access capable array of values with a fixed number of
/// bits/value. Values are packed contiguously.
/// <p>
/// The implementation strives to perform as fast as possible under the
/// constraint of contiguous bits, by avoiding expensive operations. This comes
/// at the cost of code clarity.
/// <p>
/// Technical details: This implementation is a refinement of a non-branching
/// version. The non-branching get and set methods meant that 2 or 4 atomics in
/// the underlying array were always accessed, even for the cases where only
/// 1 or 2 were needed. Even with caching, this had a detrimental effect on
/// performance.
/// Related to this issue, the old implementation used lookup tables for shifts
/// and masks, which also proved to be a bit slower than calculating the shifts
/// and masks on the fly.
/// See https://issues.apache.org/jira/browse/LUCENE-4062 for details.
///
///
struct Packed64 {
    value_count: usize,
    bits_per_value: i32,
    /// Values are stores contiguously in the blocks array.
    blocks: Vec<i64>,
    /// A right-aligned mask of width BitsPerValue used by {@link #get(int)}.
    mask_right: i64,
    /// Optimization: Saves one lookup in {@link #get(int)}.
    bpv_minus_block_size: i32,
}

const PACKED64_BLOCK_SIZE: i32 = 64;
const PACKED64_BLOCK_BITS: i32 = 6; // The #bits representing BLOCK_SIZE
const PACKED64_BLOCK_MOD_MASK: i64 = (PACKED64_BLOCK_SIZE - 1) as i64; // x % BLOCK_SIZE

impl Packed64 {
    pub fn new(value_count: usize, bits_per_value: i32) -> Packed64 {
        let format = Format::Packed;
        let long_count =
            format.long_count(VERSION_CURRENT, value_count as i32, bits_per_value) as usize;
        Packed64 {
            value_count,
            bits_per_value,
            blocks: vec![0i64; long_count],
            mask_right: (u64::max_value() << (PACKED64_BLOCK_SIZE - bits_per_value) >> // 无符号右移
                (PACKED64_BLOCK_SIZE - bits_per_value)) as i64,
            bpv_minus_block_size: bits_per_value - PACKED64_BLOCK_SIZE,
        }
    }

    pub fn from_input<T: DataInput + ?Sized>(
        packed_ints_version: i32,
        input: &mut T,
        value_count: usize,
        bits_per_value: i32,
    ) -> Result<Packed64> {
        let format = Format::Packed;
        let long_count =
            format.long_count(VERSION_CURRENT, value_count as i32, bits_per_value) as usize;
        let byte_count = format.byte_count(packed_ints_version, value_count as i32, bits_per_value);
        let mut blocks = Vec::with_capacity(long_count);
        for _i in 0..blocks.capacity() {
            blocks.push(0i64);
        }
        for i in 0..byte_count / 8 {
            blocks[i as usize] = input.read_long()?;
        }
        let remain = (byte_count % 8) as i32;
        if remain != 0 {
            let mut last_long = 0i64;
            for i in 0..remain {
                last_long |= (i64::from(input.read_byte()?)) << i64::from(56 - i * 8);
            }
            blocks[long_count - 1] = last_long;
        }
        Ok(Packed64 {
            value_count,
            bits_per_value,
            blocks,
            mask_right: (u64::max_value() << (PACKED64_BLOCK_SIZE - bits_per_value)
                >> (PACKED64_BLOCK_SIZE - bits_per_value)) as i64,
            bpv_minus_block_size: bits_per_value - PACKED64_BLOCK_SIZE,
        })
    }

    fn gcd(&self, a: i32, b: i32) -> i32 {
        if a < b {
            self.gcd(b, a)
        } else if b == 0 {
            a
        } else {
            self.gcd(b, a % b)
        }
    }
}

impl Reader for Packed64 {
    fn get(&self, doc_id: usize) -> i64 {
        let major_bit_pos = doc_id * self.bits_per_value as usize;
        let element_pos = major_bit_pos >> PACKED64_BLOCK_BITS as usize;
        let end_bits =
            (major_bit_pos as i64 & PACKED64_BLOCK_MOD_MASK) + i64::from(self.bpv_minus_block_size);

        if end_bits <= 0 {
            rshift_64(self.blocks[element_pos], -end_bits as i32) & self.mask_right
        } else {
            ((self.blocks[element_pos] << end_bits)
                | rshift_64(
                    self.blocks[element_pos + 1],
                    PACKED64_BLOCK_SIZE - end_bits as i32,
                )) & self.mask_right
        }
    }

    fn bulk_get(&self, index: usize, output: &mut [i64], len: usize) -> usize {
        debug_assert!(len > 0);
        debug_assert!(index < self.value_count);

        let mut len = len.min(self.value_count - index);
        debug_assert!(len < output.len());

        let original_index = index;
        let mut index = index;
        let decoder = bulk_operation_of(&Format::Packed, self.bits_per_value);

        let offset_in_blocks = index % PackedIntDecoder::long_value_count(decoder.as_ref());
        let mut off = 0usize;
        if offset_in_blocks != 0 {
            let mut i = offset_in_blocks;
            while i < PackedIntDecoder::long_value_count(decoder.as_ref()) && len > 0 {
                output[off] = self.get(index);
                off += 1;
                index += 1;
                len -= 1;
                i += 1;
            }
            if len == 0 {
                return index - original_index;
            }
        }

        debug_assert_eq!(
            index % PackedIntDecoder::long_value_count(decoder.as_ref()),
            0
        );
        let block_index = (index * (self.bits_per_value as usize)) >> PACKED64_BLOCK_BITS as usize;
        debug_assert_eq!(
            ((index * self.bits_per_value as usize) & PACKED64_BLOCK_MOD_MASK as usize),
            0
        );
        let iterations = len / PackedIntDecoder::long_value_count(decoder.as_ref());
        PackedIntDecoder::decode_long_to_long(
            decoder.as_ref(),
            &self.blocks[block_index..],
            &mut output[off..],
            iterations,
        );
        let got_values = iterations * PackedIntDecoder::long_value_count(decoder.as_ref());
        index += got_values;
        debug_assert!(len >= got_values);
        len -= got_values;

        if index > original_index {
            index - original_index
        } else {
            // no progress so far => already at a block boundary but no full block to get
            debug_assert_eq!(index, original_index);
            <Packed64 as Reader>::bulk_get(self, index, &mut output[off..], len)
        }
    }

    fn size(&self) -> usize {
        self.value_count
    }
}

impl Mutable for Packed64 {
    fn get_bits_per_value(&self) -> i32 {
        self.bits_per_value
    }

    fn set(&mut self, index: usize, value: i64) {
        // The abstract index in a contiguous bit stream
        let major_bit_pos = index * self.bits_per_value as usize;
        // The index in the backing long-array
        let element_pos = major_bit_pos >> PACKED64_BLOCK_BITS as usize; // / BLOCK_SIZE
                                                                         // The number of value-bits in the second long
        let end_bits =
            (major_bit_pos as i64 & PACKED64_BLOCK_MOD_MASK) + i64::from(self.bpv_minus_block_size);

        if end_bits <= 0 {
            // single block
            self.blocks[element_pos] = self.blocks[element_pos]
                & ((self.mask_right << -end_bits) as u64 ^ u64::max_value()) as i64
                | (value << -end_bits);
        } else {
            // two block
            self.blocks[element_pos] = self.blocks[element_pos]
                & (((self.mask_right as u64) >> end_bits) ^ u64::max_value()) as i64
                | rshift_64(value, end_bits as i32);
            self.blocks[element_pos + 1usize] = self.blocks[element_pos + 1usize]
                & ((u64::max_value() >> end_bits) as i64)
                | (value << (i64::from(PACKED64_BLOCK_SIZE) - end_bits));
        }
    }

    fn bulk_set(&mut self, index: usize, arr: &[i64], off: usize, len: usize) -> usize {
        debug_assert!(len > 0);
        debug_assert!(index as i64 >= 0 && index < self.value_count);

        let mut len = len.min(self.value_count - index);
        debug_assert!(off + len < arr.len());

        let original_index = index;
        let mut index = index;
        let encoder = bulk_operation_of(&Format::Packed, self.bits_per_value);

        let offset_in_blocks = index % PackedIntEncoder::long_value_count(encoder.as_ref());
        let mut off = off;
        if offset_in_blocks != 0 {
            let mut i = offset_in_blocks;
            while i < PackedIntEncoder::long_value_count(encoder.as_ref()) && len > 0 {
                self.set(index, arr[off]);
                off += 1usize;
                index += 1;
                len -= 1;
                i += 1;
            }
            if len == 0 {
                return index - original_index;
            }
        }

        // bulk set
        debug_assert_eq!(
            index % PackedIntEncoder::long_value_count(encoder.as_ref()),
            0
        );
        let block_index = (index * (self.bits_per_value as usize)) >> PACKED64_BLOCK_BITS as usize;
        debug_assert_eq!(
            ((index as i64 * i64::from(self.bits_per_value)) & PACKED64_BLOCK_MOD_MASK),
            0
        );
        let iterations = len / PackedIntEncoder::long_value_count(encoder.as_ref());
        encoder.as_ref().encode_long_to_long(
            &arr[off..],
            &mut self.blocks[block_index..],
            iterations,
        );
        let set_values = iterations * PackedIntEncoder::long_value_count(encoder.as_ref());
        index += set_values;
        len -= set_values;
        debug_assert!(len as i64 >= 0);

        if index > original_index {
            index - original_index
        } else {
            // no progress so far => already at a block boundary but no full block to get
            debug_assert_eq!(index, original_index);
            <Packed64 as Mutable>::bulk_set(self, index, arr, off, len)
        }
    }

    fn fill(&mut self, from: usize, to: usize, val: i64) {
        debug_assert!(unsigned_bits_required(val) <= self.bits_per_value);
        debug_assert!(from <= to);

        let n_aligned_values = 64usize / self.gcd(64, self.bits_per_value) as usize;
        let span = to - from;
        if span <= 3 * n_aligned_values {
            <Packed64 as Mutable>::fill(self, from, to, val);
            return;
        }

        // fill the first values naively until the next block start
        let from_index_mod_aligned_values = from % n_aligned_values;
        let mut from_index = from;
        if from_index_mod_aligned_values != 0 {
            for _i in from_index_mod_aligned_values..n_aligned_values {
                self.set(from_index, val);
                from_index += 1;
            }
        }
        debug_assert_eq!(from_index % n_aligned_values, 0);

        // compute the long[] blocks for nAlignedValues consecutive values and
        // use them to set as many values as possible without applying any mask
        // or shift
        let aligned_blocks = (n_aligned_values * self.bits_per_value as usize) >> 6;

        let mut values = Packed64::new(n_aligned_values, self.bits_per_value);
        for i in 0..n_aligned_values {
            values.set(i, val);
        }
        let aligned_values_blocks = values.blocks;
        debug_assert!(aligned_blocks <= aligned_values_blocks.len());

        let start_block = (from_index * self.bits_per_value as usize) >> 6;
        let end_block = (to * self.bits_per_value as usize) >> 6;
        for block in start_block..end_block {
            self.blocks[block] = aligned_values_blocks[block % aligned_blocks];
        }

        // fill the gap
        for i in ((end_block << 6) / self.bits_per_value as usize)..to {
            self.set(i, val);
        }
    }

    fn clear(&mut self) {
        for i in 0..self.blocks.len() {
            self.blocks[i] = 0i64;
        }
    }

    fn as_reader(&self) -> &Reader {
        self
    }
}

pub const MAX_SUPPORTED_BITS_PER_VALUE: i32 = 32;
const SUPPORTED_BITS_PER_VALUE: [i32; 14] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 16, 21, 32];

/// This class is similar to {@link Packed64} except that it trades space for
/// speed by ensuring that a single block needs to be read/written in order to
/// read/write a value.
///
pub struct Packed64SingleBlock {
    value_count: usize,
    bits_per_value: i32,
    value_per_block: usize,
    blocks: Vec<i64>,
}

impl Packed64SingleBlock {
    pub fn new(value_count: usize, bits_per_value: i32) -> Packed64SingleBlock {
        debug_assert!(
            Packed64SingleBlock::is_supported(bits_per_value),
            format!("Unsupported number of bits per value: {}", bits_per_value)
        );
        let value_per_block = 64 / bits_per_value;
        let blocks =
            vec![0i64; Packed64SingleBlock::required_capacity(value_count, value_per_block)];
        Packed64SingleBlock {
            value_count,
            bits_per_value,
            value_per_block: value_per_block as usize,
            blocks,
        }
    }

    pub fn create<T: DataInput + ?Sized>(
        input: &mut T,
        value_count: usize,
        bits_per_value: i32,
    ) -> Result<Box<Packed64SingleBlock>> {
        let mut reader = Packed64SingleBlock::new(value_count, bits_per_value);
        for i in 0..reader.blocks.len() {
            reader.blocks[i] = input.read_long()?;
        }

        Ok(Box::new(reader))
    }

    pub fn is_supported(bits_per_value: i32) -> bool {
        (&SUPPORTED_BITS_PER_VALUE)
            .binary_search(&bits_per_value)
            .is_ok()
    }

    fn required_capacity(value_count: usize, value_per_block: i32) -> usize {
        let extra = if value_count % value_per_block as usize == 0 {
            0
        } else {
            1
        };
        value_count / value_per_block as usize + extra
    }
}

impl Reader for Packed64SingleBlock {
    fn get(&self, doc_id: usize) -> i64 {
        let o = doc_id / self.value_per_block;
        let b = doc_id % self.value_per_block;
        let shift = b as i32 * self.bits_per_value;
        rshift_64(self.blocks[o], shift) & ((1i64 << self.bits_per_value) - 1)
    }

    fn bulk_get(&self, index: usize, output: &mut [i64], len: usize) -> usize {
        debug_assert!(len > 0);
        debug_assert!(index as i64 >= 0 && index < self.value_count);

        let mut len = len.min(self.value_count - index);
        debug_assert!(len <= output.len());

        // go to the next block boundary
        let offset_in_block = index % self.value_per_block;
        let mut value_index = index;
        let mut off = 0usize;
        if offset_in_block != 0 {
            while off + offset_in_block < self.value_per_block && len > 0 {
                output[off] = self.get(value_index);
                off += 1;
                value_index += 1;
                len -= 1;
            }
            if len == 0 {
                return value_index - index;
            }
        }

        // bulk get
        debug_assert_eq!(value_index % self.value_per_block, 0);
        let decoder = bulk_operation_of(&Format::PackedSingleBlock, self.bits_per_value);
        debug_assert_eq!(PackedIntDecoder::long_block_count(decoder.as_ref()), 1);
        debug_assert_eq!(
            PackedIntDecoder::long_value_count(decoder.as_ref()),
            self.value_per_block
        );
        let block_index = value_index / self.value_per_block;
        let nblocks = (value_index + len) / self.value_per_block - block_index;
        PackedIntDecoder::decode_long_to_long(
            decoder.as_ref(),
            &self.blocks[block_index..],
            &mut output[off..],
            nblocks,
        );
        let diff = nblocks * self.value_per_block;
        value_index += diff;
        len -= diff;

        if value_index > index {
            value_index - index
        } else {
            debug_assert_eq!(value_index, index);
            <Packed64SingleBlock as Reader>::bulk_get(self, value_index, &mut output[off..], len)
        }
    }

    fn size(&self) -> usize {
        self.value_count
    }
}

impl Mutable for Packed64SingleBlock {
    fn get_bits_per_value(&self) -> i32 {
        self.bits_per_value
    }

    fn set(&mut self, index: usize, value: i64) {
        let o = index / self.value_per_block;
        let b = index % self.value_per_block;
        let shift = (b * self.value_per_block) as i64;
        self.blocks[o] =
            self.blocks[o] & (((1i64 << self.bits_per_value) - 1i64) << shift) | (value << shift);
    }

    fn bulk_set(&mut self, index: usize, arr: &[i64], off: usize, len: usize) -> usize {
        debug_assert!(len > 0);
        debug_assert!(index as i64 >= 0 && index < self.value_count);
        let mut len = len.min(self.value_count - index);
        debug_assert!(off + len <= arr.len());

        // go to the next block boundary
        let offset_in_block = index % self.value_per_block;
        let mut value_index = index;
        let mut off = off;
        if offset_in_block != 0 {
            while off + offset_in_block < self.value_per_block && len > 0 {
                self.set(value_index, arr[off]);
                off += 1;
                value_index += 1;
                len -= 1;
            }
            if len == 0 {
                return value_index - index;
            }
        }

        // bulk get
        debug_assert_eq!(value_index % self.value_per_block, 0);
        let op = bulk_operation_of(&Format::PackedSingleBlock, self.bits_per_value);
        debug_assert_eq!(PackedIntEncoder::long_block_count(op.as_ref()), 1);
        debug_assert_eq!(
            PackedIntEncoder::long_value_count(op.as_ref()),
            self.value_per_block
        );
        let block_index = value_index / self.value_per_block;
        let nblocks = (value_index + len) / self.value_per_block - block_index;
        op.encode_long_to_long(&arr[off..], &mut self.blocks[block_index..], nblocks);
        let diff = nblocks * self.value_per_block;
        value_index += diff;
        len -= diff;

        if value_index > index {
            value_index - index
        } else {
            debug_assert_eq!(value_index, index);
            <Packed64SingleBlock as Mutable>::bulk_set(self, value_index, arr, off, len)
        }
    }

    fn fill(&mut self, from: usize, to: usize, val: i64) {
        debug_assert!(from as i64 >= 0 && from <= to);
        debug_assert!(unsigned_bits_required(val) <= self.bits_per_value);

        if to - from < self.value_per_block << 1 {
            // there needs to be at least one full block to set for the block
            // approach to be worth trying
            <Packed64SingleBlock as Mutable>::fill(self, from, to, val);
            return;
        }

        // set values natively until the next block start
        let mut from_index = from;
        let from_offset_in_block = from % self.value_per_block;
        if from_offset_in_block != 0 {
            for _i in from_offset_in_block..self.value_per_block {
                self.set(from_index, val);
                from_index += 1;
            }
            debug_assert_eq!(from_index % self.value_per_block, 0);
        }

        // bulk set of the inner blocks
        let from_block = from_index / self.value_per_block;
        let to_block = to / self.value_per_block;
        debug_assert_eq!(from_block * self.value_per_block, from_index);

        let mut block_value = 0i64;
        for i in 0..self.value_per_block {
            block_value |= val << (i as i32 * self.bits_per_value);
        }
        for i in from_block..to_block {
            self.blocks[i] = block_value;
        }

        // fill the gap
        for i in (self.value_per_block * to_block)..to {
            self.set(i, val);
        }
    }

    fn clear(&mut self) {
        for i in 0..self.blocks.len() {
            self.blocks[i] = 0i64;
        }
    }

    fn get_format(&self) -> Format {
        Format::PackedSingleBlock
    }

    fn as_reader(&self) -> &Reader {
        self
    }
}

/// Implements {@link PackedInts.Mutable}, but grows the
/// bit count of the underlying packed ints on-demand.
/// <p>Beware that this class will accept to set negative values but in order
/// to do this, it will grow the number of bits per value to 64.
///
pub struct GrowableWriter {
    current_mark: i64,
    current: Box<Mutable>,
    acceptable_overhead_ratio: f32,
}

impl GrowableWriter {
    pub fn new(
        start_bits_per_value: i32,
        value_count: usize,
        acceptable_overhead_ratio: f32,
    ) -> GrowableWriter {
        let current =
            get_mutable_by_ratio(value_count, start_bits_per_value, acceptable_overhead_ratio);
        GrowableWriter {
            current_mark: GrowableWriter::mask(current.get_bits_per_value()),
            current,
            acceptable_overhead_ratio,
        }
    }

    fn mask(bits_per_value: i32) -> i64 {
        if bits_per_value == 64 {
            u64::max_value() as i64
        } else {
            max_value(bits_per_value)
        }
    }

    fn ensure_capacity(&mut self, value: i64) {
        if value & self.current_mark == value {
            return;
        }
        let bits_required = unsigned_bits_required(value);
        debug_assert!(bits_required > self.current.get_bits_per_value());
        let value_count = self.size();
        let mut next =
            get_mutable_by_ratio(value_count, bits_required, self.acceptable_overhead_ratio);
        packed_ints_copy(
            self.current.as_ref().as_reader(),
            0,
            next.as_mut(),
            0,
            value_count,
            DEFAULT_BUFFER_SIZE,
        );
        self.current = next;
        self.current_mark = GrowableWriter::mask(self.current.get_bits_per_value());
    }

    pub fn get_mutable(&mut self) -> &mut Mutable {
        self.current.as_mut()
    }

    pub fn resize(&mut self, new_size: usize) -> GrowableWriter {
        let mut next = GrowableWriter::new(
            self.get_bits_per_value(),
            new_size,
            self.acceptable_overhead_ratio,
        );
        let limit = new_size.min(self.size());
        packed_ints_copy(
            self.current.as_ref().as_reader(),
            0,
            &mut next,
            0,
            limit,
            DEFAULT_BUFFER_SIZE,
        );
        next
    }
}

impl Reader for GrowableWriter {
    fn get(&self, doc_id: usize) -> i64 {
        self.current.as_ref().get(doc_id)
    }

    fn bulk_get(&self, index: usize, output: &mut [i64], len: usize) -> usize {
        self.current.as_ref().bulk_get(index, output, len)
    }

    fn size(&self) -> usize {
        self.current.size()
    }
}

impl Mutable for GrowableWriter {
    fn get_bits_per_value(&self) -> i32 {
        self.current.get_bits_per_value()
    }

    fn set(&mut self, index: usize, value: i64) {
        self.ensure_capacity(value);
        self.current.as_mut().set(index, value);
    }

    fn bulk_set(&mut self, index: usize, arr: &[i64], off: usize, len: usize) -> usize {
        let mut max = 0i64;
        for v in arr.iter().skip(off).take(len) {
            max |= v;
        }
        self.ensure_capacity(max);
        self.current.as_mut().bulk_set(index, arr, off, len)
    }

    fn fill(&mut self, from: usize, to: usize, val: i64) {
        self.ensure_capacity(val);
        self.current.as_mut().fill(from, to, val);
    }

    fn clear(&mut self) {
        self.current.clear();
    }

    fn save(&self, out: &mut DataOutput) -> Result<()> {
        self.current.save(out)
    }

    fn as_reader(&self) -> &Reader {
        self
    }
}

/// Run-once iterator interface, to decode previously saved PackedInts.
pub trait ReaderIterator {
    /// Returns next value
    fn next(&mut self, input: &mut IndexInput) -> Result<i64>;
    /// Returns number of bits per value
    fn get_bits_per_value(&self) -> i32;
    /// Returns number of values
    fn size(&self) -> usize;
    /// Returns the current position
    fn ord(&self) -> i32;
}

/// merge from lucene `PackedReaderIterator` and `PackedInts.ReaderIteratorImpl`
pub struct PackedReaderIterator {
    pub bits_per_value: i32,
    pub value_count: usize,
    packed_ints_version: i32,
    format: Format,
    bulk_operation: Box<BulkOperation>,
    next_blocks: Vec<u8>,
    // the following 3 fields is representing lucene `LongsRef` class
    next_values: Vec<i64>,
    next_values_offset: usize,
    next_values_length: usize,
    iterations: usize,
    position: i32,
}

impl PackedReaderIterator {
    pub fn new(
        format: Format,
        packed_ints_version: i32,
        value_count: usize,
        bits_per_value: i32,
        mem: i32,
    ) -> PackedReaderIterator {
        let bulk_operation = bulk_operation_of(&format, bits_per_value);
        let iterations = bulk_operation.compute_iterations(value_count as i32, mem) as usize;
        debug_assert!(value_count == 0 || iterations > 0);
        let next_blocks =
            vec![0u8; iterations * PackedIntDecoder::byte_block_count(bulk_operation.as_ref())];
        let next_values =
            vec![0i64; iterations * PackedIntDecoder::byte_value_count(bulk_operation.as_ref())];
        let next_values_offset = next_values.len();
        PackedReaderIterator {
            bits_per_value,
            value_count,
            packed_ints_version,
            format,
            bulk_operation,
            next_blocks,
            next_values,
            next_values_offset,
            next_values_length: 0,
            iterations,
            position: -1,
        }
    }

    fn next_n(&mut self, count: i32, input: &mut IndexInput) -> Result<()> {
        debug_assert!(self.next_values_length as i64 >= 0);
        debug_assert!(count > 0);
        debug_assert!(self.next_values_offset + self.next_values_length <= self.next_values.len());

        self.next_values_offset += self.next_values_length;
        let remain = self.value_count as i32 - self.position - 1;
        if remain < 0 {
            return Err("end of file".into());
        }
        let count = remain.min(count);

        if self.next_values_offset == self.next_values.len() {
            let remain_blocks =
                self.format
                    .byte_count(self.packed_ints_version, remain, self.bits_per_value);
            let blocks_to_read = min(self.next_blocks.len(), remain_blocks as usize);
            input.read_bytes(self.next_blocks.as_mut(), 0, blocks_to_read)?;
            if blocks_to_read < self.next_blocks.len() {
                for i in blocks_to_read..self.next_blocks.len() {
                    self.next_blocks[i] = 0u8;
                }
            }
            PackedIntDecoder::decode_byte_to_long(
                self.bulk_operation.as_ref(),
                self.next_blocks.as_ref(),
                self.next_values.as_mut(),
                self.iterations,
            );
            self.next_values_offset = 0;
        }

        self.next_values_length = min(
            self.next_values.len() - self.next_values_offset,
            count as usize,
        );
        self.position += self.next_values_length as i32;
        Ok(())
    }
}

impl ReaderIterator for PackedReaderIterator {
    fn next(&mut self, input: &mut IndexInput) -> Result<i64> {
        self.next_n(1, input)?;
        debug_assert!(self.next_values_length > 0);
        let result = self.next_values[self.next_values_offset as usize];
        self.next_values_offset += 1;
        self.next_values_length -= 1;
        Ok(result)
    }

    fn get_bits_per_value(&self) -> i32 {
        self.bits_per_value
    }

    fn size(&self) -> usize {
        self.value_count
    }

    fn ord(&self) -> i32 {
        self.position
    }
}

pub fn get_encoder(
    _format: Format,
    _version: i32,
    _bits_per_value: i32,
) -> Result<Box<PackedIntEncoder>> {
    unimplemented!();
}

pub fn get_decoder(
    format: &Format,
    version: i32,
    bits_per_value: i32,
) -> Result<Box<PackedIntDecoder>> {
    check_version(version)?;
    Ok(bulk_operation_of(format, bits_per_value).as_decoder())
}

/// A write-once Writer.
pub trait Writer {
    fn write_header(&self, out: &mut DataOutput) -> Result<()>;
    /// The format used to serialize values
    fn get_format(&self) -> Format;
    /// Add a value to the stream.
    fn add(&mut self, v: i64, out: &mut DataOutput) -> Result<()>;
    /// The number of bits per value.
    fn bits_per_value(&self) -> i32;
    /// Perform end-of-stream operations.
    fn finish(&mut self, out: &mut DataOutput) -> Result<()>;
    fn ord(&self) -> i32;
}

/// Packs high order byte first, to match
/// IndexOutput.writeInt/Long/Short byte order
pub struct PackedWriter {
    value_count: i32,
    bits_per_value: i32,
    finish: bool,
    format: Format,
    encoder: Box<BulkOperation>,
    next_blocks: Vec<u8>,
    next_values: Vec<i64>,
    iterations: usize,
    off: usize,
    written: usize,
}

impl PackedWriter {
    pub fn new(format: Format, value_count: i32, bits_per_value: i32, mem: i32) -> PackedWriter {
        debug_assert!(bits_per_value <= 64);
        debug_assert!(value_count >= 0 || value_count == -1);
        let encoder = bulk_operation_of(&format, bits_per_value);
        let iterations = encoder.as_ref().compute_iterations(value_count, mem) as usize;
        let next_blocks =
            vec![0u8; iterations * PackedIntEncoder::byte_block_count(encoder.as_ref())];
        let next_values =
            vec![0i64; iterations * PackedIntEncoder::byte_value_count(encoder.as_ref())];
        PackedWriter {
            value_count,
            bits_per_value,
            finish: false,
            format,
            encoder,
            next_blocks,
            next_values,
            iterations,
            off: 0,
            written: 0,
        }
    }

    fn flush(&mut self, out: &mut DataOutput) -> Result<()> {
        self.encoder.as_ref().encode_long_to_byte(
            self.next_values.as_ref(),
            self.next_blocks.as_mut(),
            self.iterations,
        );
        let block_count =
            self.format
                .byte_count(VERSION_CURRENT, self.off as i32, self.bits_per_value)
                as usize;
        out.write_bytes(self.next_blocks.as_mut(), 0, block_count)?;
        for i in 0..self.next_values.len() {
            self.next_values[i] = 0i64;
        }
        self.off = 0;
        Ok(())
    }
}

impl Writer for PackedWriter {
    fn write_header(&self, out: &mut DataOutput) -> Result<()> {
        debug_assert_ne!(self.value_count, -1);
        codec_util::write_header(out, CODEC_NAME, VERSION_CURRENT)?;
        out.write_vint(self.bits_per_value)?;
        out.write_vint(self.value_count)?;
        out.write_vint(self.get_format().get_id())?;
        Ok(())
    }

    fn get_format(&self) -> Format {
        self.format
    }

    fn add(&mut self, v: i64, out: &mut DataOutput) -> Result<()> {
        debug_assert!(unsigned_bits_required(v) <= self.bits_per_value);
        debug_assert!(!self.finish);

        if self.value_count != -1 && self.written >= self.value_count as usize {
            return Err("Writing past end of stream".into());
        }
        self.next_values[self.off] = v;
        self.off += 1;
        if (self.off) == self.next_values.len() {
            self.flush(out)?;
        }
        self.written += 1;
        Ok(())
    }

    fn bits_per_value(&self) -> i32 {
        self.bits_per_value
    }

    fn finish(&mut self, out: &mut DataOutput) -> Result<()> {
        debug_assert!(!self.finish);
        if self.value_count != -1 {
            while self.written < self.value_count as usize {
                self.add(0i64, out)?;
            }
        }
        self.flush(out)?;
        self.finish = true;
        Ok(())
    }

    fn ord(&self) -> i32 {
        self.written as i32 - 1
    }
}

pub fn bulk_operation_of(format: &Format, bits_per_value: i32) -> Box<BulkOperation> {
    match *format {
        // TODO 部分 bits 的bulk operation 实际在 lucene 中实现了优化的方式
        Format::Packed => Box::new(BulkOperationPacked::new(bits_per_value)),
        Format::PackedSingleBlock => Box::new(BulkOperationPackedSingleBlock::new(bits_per_value)),
    }
}

pub trait BulkOperation: PackedIntDecoder + PackedIntEncoder {
    fn compute_iterations(&self, value_count: i32, ram_budget: i32) -> i32 {
        let byte_value_count = PackedIntDecoder::byte_value_count(self);
        let byte_block_count = PackedIntDecoder::byte_block_count(self);
        let iterations = ram_budget as usize / (byte_block_count + 8 * byte_value_count);
        if iterations == 0 {
            1
        } else if (iterations - 1) * byte_value_count >= value_count as usize {
            ((value_count as f32) / (byte_value_count as f32)).ceil() as i32
        } else {
            iterations as i32
        }
    }

    fn as_decoder(&self) -> Box<PackedIntDecoder>;
}

#[derive(Clone)]
pub struct BulkOperationPacked {
    bits_per_value: i32,
    long_block_count: usize,
    long_value_count: usize,
    byte_block_count: usize,
    byte_value_count: usize,
    mask: i64,
    int_mask: i32,
}

impl BulkOperationPacked {
    pub fn new(bits_per_value: i32) -> BulkOperationPacked {
        debug_assert!(bits_per_value > 0 && bits_per_value <= 64);
        let mut blocks = bits_per_value as usize;
        while (blocks & 1) == 0 {
            blocks >>= 1;
        }
        let long_block_count = blocks;
        let long_value_count = 64 * long_block_count / bits_per_value as usize;
        let mut byte_block_count = 8 * blocks;
        let mut byte_value_count = long_value_count;
        while (byte_block_count & 1usize) == 0 && (byte_value_count & 1usize) == 0 {
            byte_block_count >>= 1;
            byte_value_count >>= 1;
        }
        let mask = if bits_per_value == 64 {
            i64::max_value()
        } else {
            (1i64 << bits_per_value) - 1
        };
        debug_assert_eq!(
            long_value_count * bits_per_value as usize,
            64 * long_block_count
        );
        BulkOperationPacked {
            bits_per_value,
            long_block_count,
            long_value_count,
            byte_block_count,
            byte_value_count,
            mask,
            int_mask: mask as i32,
        }
    }
}

impl BulkOperation for BulkOperationPacked {
    fn as_decoder(&self) -> Box<PackedIntDecoder> {
        Box::new(self.clone())
    }
}

impl PackedIntEncoder for BulkOperationPacked {
    fn long_block_count(&self) -> usize {
        self.long_block_count
    }

    fn long_value_count(&self) -> usize {
        self.long_value_count
    }

    fn byte_block_count(&self) -> usize {
        self.byte_block_count
    }

    fn byte_value_count(&self) -> usize {
        self.byte_value_count
    }

    fn encode_long_to_long(&self, values: &[i64], blocks: &mut [i64], iterations: usize) {
        let mut next_block = 0i64;
        let mut bits_left = 64;
        let mut values_offset = 0;
        let mut blocks_offset = 0;
        for _i in 0..self.long_value_count * iterations {
            bits_left -= self.bits_per_value;
            if bits_left < 0 {
                next_block |= values[values_offset];
                values_offset += 1;
            } else if bits_left == 0 {
                next_block |= values[values_offset];
                values_offset += 1;
                blocks[blocks_offset] = next_block;
                blocks_offset += 1;
                next_block = 0;
                bits_left = 64;
            } else {
                next_block |= values[values_offset] >> -bits_left;
                blocks[blocks_offset] = next_block;
                blocks_offset += 1;
                next_block =
                    (values[values_offset] & ((1i64 << -bits_left) - 1)) << (64 + bits_left);
                values_offset += 1;
                bits_left += 64;
            }
        }
    }

    fn encode_long_to_byte(&self, values: &[i64], blocks: &mut [u8], iterations: usize) {
        let mut next_block: i32 = 0;
        let mut bits_left = 8;
        let mut values_offset = 0;
        let mut blocks_offset = 0;
        for _i in 0..self.byte_value_count * iterations {
            let v = values[values_offset];
            values_offset += 1;
            if self.bits_per_value < bits_left {
                next_block |= (v << (bits_left - self.bits_per_value)) as i32;
                bits_left -= self.bits_per_value;
            } else {
                let mut bits = self.bits_per_value - bits_left;
                blocks[blocks_offset] = (next_block | (v >> bits) as i32) as u8;
                blocks_offset += 1;
                while bits >= 8 {
                    bits -= 8;
                    blocks[blocks_offset] = (v >> bits) as u8;
                    blocks_offset += 1;
                }
                bits_left = 8 - bits;
                next_block = (((v as i64) & ((1i64 << bits) - 1)) << bits_left) as i32;
            }
        }
        debug_assert_eq!(bits_left, 8);
    }

    fn encode_int_to_long(&self, values: &[i32], blocks: &mut [i64], iterations: usize) {
        let mut next_block = 0i64;
        let mut bits_left = 64;
        let values_offset = 0;
        let mut blocks_offset = 0;
        for _i in 0..self.long_value_count * iterations {
            if bits_left > 0 {
                next_block |= (i64::from(values[values_offset]) & 0xffff_ffffi64) << bits_left;
            } else if bits_left == 0 {
                next_block |= i64::from(values[values_offset]) & 0xffff_ffffi64;
                blocks[blocks_offset] = next_block;
                blocks_offset += 1;
                next_block = 0;
                bits_left = 64;
            } else {
                next_block |= (i64::from(values[values_offset]) & 0xffff_ffffi64) >> -bits_left;
                blocks[blocks_offset] = next_block;
                blocks_offset += 1;
                next_block = (i64::from(values[values_offset]) & ((1i64 << -bits_left) - 1))
                    << (64 + bits_left);
                bits_left += 64;
            }
        }
    }

    fn encode_int_to_byte(&self, values: &[i32], blocks: &mut [u8], iterations: usize) {
        let mut next_block = 0;
        let mut bits_left = 8;
        let mut values_offset = 0;
        let mut blocks_offset = 0;
        for _i in 0..self.byte_value_count * iterations {
            let v = values[values_offset];
            values_offset += 1;
            if self.bits_per_value < bits_left {
                next_block |= v << (bits_left - self.bits_per_value);
                bits_left -= self.bits_per_value;
            } else {
                let mut bits = self.bits_per_value - bits_left;
                blocks[blocks_offset] = (next_block | (v >> bits)) as u8;
                blocks_offset += 1;
                while bits >= 8 {
                    bits -= 8;
                    blocks[blocks_offset] = (v >> bits) as u8;
                    blocks_offset += 1;
                }
                bits_left = 8 - bits;
                next_block = (v & ((1 << bits) - 1)) << bits_left;
            }
        }
        debug_assert_eq!(bits_left, 8);
    }
}

impl PackedIntDecoder for BulkOperationPacked {
    fn long_block_count(&self) -> usize {
        self.long_block_count
    }

    fn long_value_count(&self) -> usize {
        self.long_value_count
    }

    fn byte_block_count(&self) -> usize {
        self.byte_block_count
    }

    fn byte_value_count(&self) -> usize {
        self.byte_value_count
    }

    fn decode_long_to_long(&self, blocks: &[i64], values: &mut [i64], iterations: usize) {
        let mut bits_left = 64;
        let mut block_offset = 0;
        for v in values.iter_mut().take(self.long_value_count * iterations) {
            bits_left -= self.bits_per_value;
            if bits_left < 0 {
                *v = ((blocks[block_offset] & ((1i64 << (self.bits_per_value + bits_left)) - 1))
                    << -bits_left)
                    | rshift_64(blocks[block_offset + 1], 64 + bits_left);
                block_offset += 1;
            } else {
                *v = (blocks[block_offset] >> bits_left) & self.mask;
            }
        }
    }

    fn decode_byte_to_long(&self, blocks: &[u8], values: &mut [i64], iterations: usize) {
        let mut next_value = 0i64;
        let mut bits_left = self.bits_per_value;
        let mut blocks_offset = 0;
        let mut values_offset = 0;
        for _i in 0..iterations * self.byte_block_count {
            let bytes = i64::from(blocks[blocks_offset]);
            blocks_offset += 1;
            if bits_left > 8 {
                bits_left -= 8;
                next_value |= bytes << bits_left;
            } else {
                let mut bits = 8 - bits_left;
                values[values_offset] = next_value | rshift_64(bytes, bits);
                values_offset += 1;
                while bits >= self.bits_per_value {
                    bits -= self.bits_per_value;
                    values[values_offset] = rshift_64(bytes, bits) & self.mask;
                    values_offset += 1;
                }
                bits_left = self.bits_per_value - bits;
                next_value = (bytes & ((1i64 << bits) - 1)) << bits_left;
            }
        }
        debug_assert_eq!(bits_left, self.bits_per_value);
    }

    fn decode_long_to_int(&self, blocks: &[i64], values: &mut [i32], iterations: usize) {
        if self.bits_per_value > 32 {
            panic!(format!(
                "Cannot decode {} -bits values into an int[]",
                self.bits_per_value
            ));
        }

        let mut bits_left = 64;
        let mut blocks_offset = 0;
        for v in values.iter_mut().take(self.long_value_count * iterations) {
            bits_left -= self.bits_per_value;
            if bits_left < 0 {
                *v = (((blocks[blocks_offset] & ((1i64 << (self.bits_per_value + bits_left)) - 1))
                    << -bits_left)
                    | rshift_64(blocks[blocks_offset + 1], 64 + bits_left))
                    as i32;
                bits_left += 64;
                blocks_offset += 1;
            } else {
                *v = (rshift_64(blocks[blocks_offset], bits_left) & self.mask) as i32;
            }
        }
    }

    fn decode_byte_to_int(&self, blocks: &[u8], values: &mut [i32], iterations: usize) {
        let mut next_value = 0;
        let mut bits_left = self.bits_per_value;
        let mut values_offset = 0usize;
        let mut blocks_offset = 0usize;
        for _i in 0..iterations * self.byte_block_count {
            let bytes = i32::from(blocks[blocks_offset]);
            blocks_offset += 1;
            if bits_left > 8 {
                bits_left -= 8;
                next_value |= bytes << bits_left;
            } else {
                let mut bits: i32 = 8 - bits_left;
                values[values_offset] = (next_value | (bytes >> bits)) as i32;
                values_offset += 1;
                while bits >= self.bits_per_value {
                    bits -= self.bits_per_value;
                    values[values_offset] = ((bytes >> bits) as i32) & self.int_mask;
                    values_offset += 1;
                }
                bits_left = self.bits_per_value - bits;
                next_value = (bytes & ((1 << bits) - 1)) << bits_left;
            }
        }
        debug_assert_eq!(bits_left, self.bits_per_value);
    }
}

const BLOCK_COUNT: usize = 1;

#[derive(Clone)]
struct BulkOperationPackedSingleBlock {
    bits_per_value: i32,
    value_count: usize,
    mask: i64,
}

impl BulkOperationPackedSingleBlock {
    pub fn new(bits_per_value: i32) -> BulkOperationPackedSingleBlock {
        BulkOperationPackedSingleBlock {
            bits_per_value,
            value_count: 64usize / bits_per_value as usize,
            mask: (1i64 << bits_per_value) - 1,
        }
    }

    fn read_long(&self, blocks: &[u8], offset: usize) -> i64 {
        i64::from(blocks[offset]) << 56 | i64::from(blocks[offset + 1]) << 48
            | i64::from(blocks[offset + 2]) << 40 | i64::from(blocks[offset + 3]) << 32
            | i64::from(blocks[offset + 4]) << 24 | i64::from(blocks[offset + 5]) << 16
            | i64::from(blocks[offset + 6]) << 8 | i64::from(blocks[offset + 7])
    }

    fn write_long(&self, block: i64, blocks: &mut [u8], blocks_offset: usize) -> usize {
        let mut offset = blocks_offset;
        for i in 0..8 {
            blocks[offset] = (block >> (64 - (i << 3))) as u8;
            offset += 1;
        }
        offset
    }

    fn decode_long_value_to_long(
        &self,
        block_value: i64,
        values: &mut [i64],
        offset: usize,
    ) -> usize {
        let mut values_offset = offset;
        let mut block = block_value;
        values[values_offset] = block & self.mask;
        values_offset += 1;
        for _i in 1..self.value_count {
            block = rshift_64(block, self.bits_per_value);
            values[values_offset] = block & self.mask;
        }
        values_offset
    }

    fn decode_long_value_to_int(
        &self,
        block_value: i64,
        values: &mut [i32],
        offset: usize,
    ) -> usize {
        let mut values_offset = offset;
        let mut block = block_value;
        values[values_offset] = (block & self.mask) as i32;
        values_offset += 1;
        for _i in 1..self.value_count {
            block = rshift_64(block, self.bits_per_value);
            values[values_offset] = (block & self.mask) as i32;
            values_offset += 1;
        }
        values_offset
    }

    fn encode_long(&self, values: &[i64], values_offset: usize) -> i64 {
        let mut offset = values_offset;
        let mut block = values[offset];
        offset += 1;
        for i in 1..self.value_count {
            block |= values[offset] << (i as i32 * self.bits_per_value);
            offset += 1;
        }
        block
    }

    fn encode_int(&self, values: &[i32], values_offset: usize) -> i64 {
        let mut offset = values_offset;
        let mut block = i64::from(values[offset] as u32);
        offset += 1;
        for i in 1..self.value_count {
            block |= i64::from(values[offset] as u32) << (i as i32 * self.bits_per_value);
            offset += 1;
        }
        block
    }
}

impl BulkOperation for BulkOperationPackedSingleBlock {
    fn as_decoder(&self) -> Box<PackedIntDecoder> {
        Box::new(self.clone())
    }
}

impl PackedIntDecoder for BulkOperationPackedSingleBlock {
    fn long_block_count(&self) -> usize {
        BLOCK_COUNT
    }

    fn long_value_count(&self) -> usize {
        self.value_count
    }

    fn byte_block_count(&self) -> usize {
        BLOCK_COUNT * 8
    }

    fn byte_value_count(&self) -> usize {
        self.value_count
    }

    fn decode_long_to_long(&self, blocks: &[i64], values: &mut [i64], iterations: usize) {
        let mut values_offset = 0;
        for b in blocks.iter().take(iterations) {
            values_offset = self.decode_long_value_to_long(*b, values, values_offset);
        }
    }

    fn decode_byte_to_long(&self, blocks: &[u8], values: &mut [i64], iterations: usize) {
        let mut values_offset = 0;
        for i in 0..iterations {
            let mut block = self.read_long(blocks, i * 8);
            values_offset = self.decode_long_value_to_long(block, values, values_offset);
        }
    }

    fn decode_long_to_int(&self, blocks: &[i64], values: &mut [i32], iterations: usize) {
        if self.bits_per_value > 32 {
            panic!(format!(
                "Cannot decode {} -bits values into an int[]",
                self.bits_per_value
            ));
        }
        let mut values_offset = 0;
        for b in blocks.iter().take(iterations) {
            values_offset = self.decode_long_value_to_int(*b, values, values_offset);
        }
    }

    fn decode_byte_to_int(&self, blocks: &[u8], values: &mut [i32], iterations: usize) {
        if self.bits_per_value > 32 {
            panic!(format!(
                "Cannot decode {} -bits values into an int[]",
                self.bits_per_value
            ));
        }
        let mut values_offset = 0;
        for i in 0..iterations {
            let block = self.read_long(blocks, i * 8);
            values_offset = self.decode_long_value_to_int(block, values, values_offset);
        }
    }
}

impl PackedIntEncoder for BulkOperationPackedSingleBlock {
    fn long_block_count(&self) -> usize {
        BLOCK_COUNT
    }

    fn long_value_count(&self) -> usize {
        self.value_count
    }

    fn byte_block_count(&self) -> usize {
        BLOCK_COUNT * 8
    }

    fn byte_value_count(&self) -> usize {
        self.value_count
    }

    fn encode_long_to_long(&self, values: &[i64], blocks: &mut [i64], iterations: usize) {
        let mut values_offset = 0;
        for b in blocks.iter_mut().take(iterations) {
            *b = self.encode_long(values, values_offset);
            values_offset += self.value_count;
        }
    }

    fn encode_long_to_byte(&self, values: &[i64], blocks: &mut [u8], iterations: usize) {
        let mut blocks_offset = 0;
        for i in 0..iterations {
            let block = self.encode_long(values, i * self.value_count);
            blocks_offset = self.write_long(block, blocks, blocks_offset);
        }
    }

    fn encode_int_to_long(&self, values: &[i32], blocks: &mut [i64], iterations: usize) {
        let mut values_offset = 0;
        for b in blocks.iter_mut().take(iterations) {
            *b = self.encode_int(values, values_offset);
            values_offset += self.value_count;
        }
    }

    fn encode_int_to_byte(&self, values: &[i32], blocks: &mut [u8], iterations: usize) {
        let mut blocks_offset = 0;
        for i in 0..iterations {
            let block = self.encode_int(values, i * self.value_count);
            blocks_offset = self.write_long(block, blocks, blocks_offset);
        }
    }
}

// use for represent LongsRef's offset and length
#[derive(Clone)]
pub struct OffsetAndLength(pub usize, pub usize);

/// Reader for sequences of longs written with {@link BlockPackedWriter}.
/// @see BlockPackedWriter
/// @lucene.internal
///
#[derive(Clone)]
pub struct BlockPackedReaderIterator {
    packed_ints_version: i32,
    value_count: i64,
    block_size: usize,
    pub values: Vec<i64>,
    // 一下两个字段用于替代原始定义中的 `values_ref` 成员
    values_offset: usize,
    values_length: usize,
    blocks: Vec<u8>,
    off: usize,
    pub ord: i64,
}

pub const MIN_BLOCK_SIZE: usize = 64;
pub const MAX_BLOCK_SIZE: usize = 1 << (30 - 3);
const MIN_VALUE_EQUALS_0: i32 = 1;
const BPV_SHIFT: i32 = 1;

/// Return the number of blocks required to store `size` values on `block_size`
pub fn num_blocks(size: u64, block_size: usize) -> usize {
    let block_size = block_size as u64;
    let padding = if (size % block_size) == 0 { 0 } else { 1 };
    (size / block_size + padding) as usize
}

pub fn check_block_size(block_size: usize, min_block_size: usize, max_block_size: usize) -> u32 {
    debug_assert!(block_size >= min_block_size && block_size <= max_block_size);
    debug_assert_eq!(block_size & (block_size - 1), 0);
    block_size.trailing_zeros()
}

impl BlockPackedReaderIterator {
    /// Sole constructor.
    ///  @param blockSize the number of values of a block, must be equal to the
    ///                   block size of the {@link BlockPackedWriter} which has
    ///                   been used to write the stream
    ///
    pub fn new<T: DataInput + ?Sized>(
        _input: &mut T,
        packed_ints_version: i32,
        block_size: usize,
        value_count: i64,
    ) -> BlockPackedReaderIterator {
        check_block_size(block_size, MIN_BLOCK_SIZE, MAX_BLOCK_SIZE);
        let values = vec![0i64; block_size];
        BlockPackedReaderIterator {
            packed_ints_version,
            value_count,
            block_size,
            values,
            values_offset: 0,
            values_length: 0,
            blocks: Vec::with_capacity(0),
            off: block_size,
            ord: 0,
        }
    }

    /// Reset the current reader to wrap a stream of <code>valueCount</code>
    /// values contained in <code>in</code>. The block size remains unchanged.
    pub fn reset(&mut self, value_count: i64) {
        debug_assert!(value_count > 0);
        self.value_count = value_count;
        self.off = self.block_size;
        self.ord = 0;
    }

    pub fn skip<T: DataInput + ?Sized>(&mut self, input: &mut T, count: i64) -> Result<()> {
        debug_assert!(count >= 0);
        let mut count = count;
        if self.ord + count > self.value_count || self.ord + count < 0 {
            // TODO should return end-of-file error
            bail!("end of file!");
        }

        // 1. skip buffered values
        let skip_buffer = min(count, self.block_size as i64 - self.off as i64);
        self.off += skip_buffer as usize;
        self.ord += skip_buffer;
        count -= skip_buffer;
        if count == 0 {
            return Ok(());
        }

        // 2. skip as many blocks as necessary
        debug_assert_eq!(self.off, self.block_size);
        while count >= self.block_size as i64 {
            let token = i32::from(input.read_byte()?);
            let bits_per_value = rshift_32(token, BPV_SHIFT);
            if bits_per_value > 64 {
                bail!("Corrupted");
            }
            if (token & MIN_VALUE_EQUALS_0) == 0 {
                BlockPackedReaderIterator::read_v_long(input)?;
            }
            let block_bytes = Format::Packed.byte_count(
                self.packed_ints_version,
                self.block_size as i32,
                bits_per_value,
            );
            self.skip_bytes(input, block_bytes)?;
            self.ord += self.block_size as i64;
            count -= self.block_size as i64;
        }
        if count == 0 {
            return Ok(());
        }

        // 3. skip last values
        debug_assert!(count < self.block_size as i64);
        self.refill(input)?;
        self.ord += count;
        self.off += count as usize;
        Ok(())
    }

    fn skip_bytes<T: DataInput + ?Sized>(&mut self, input: &mut T, count: i64) -> Result<()> {
        // TODO maybe this is not right
        if count > 0 {
            input.skip_bytes(count as usize)?
        }
        Ok(())
    }

    fn refill<T: DataInput + ?Sized>(&mut self, input: &mut T) -> Result<()> {
        let token = i32::from(input.read_byte()?);
        let min_equals_0 = (token & MIN_VALUE_EQUALS_0) != 0;
        let bits_per_value = rshift_32(token, BPV_SHIFT);
        if bits_per_value > 64 {
            bail!("Corrupted");
        }
        let min_value = if min_equals_0 {
            0i64
        } else {
            (1i64 + BlockPackedReaderIterator::read_v_long(input)?).decode()
        };
        debug_assert!(min_equals_0 || min_value != 0);

        if bits_per_value == 0 {
            for i in 0..self.values.len() {
                self.values[i] = min_value;
            }
        } else {
            let decoder = get_decoder(&Format::Packed, self.packed_ints_version, bits_per_value)?;
            let iterations = self.block_size / PackedIntDecoder::byte_value_count(decoder.as_ref());
            let blocks_size = iterations * PackedIntDecoder::byte_block_count(decoder.as_ref());
            if self.blocks.len() < blocks_size {
                for _ in 0..blocks_size - self.blocks.len() {
                    self.blocks.push(0u8);
                }
            }
            let value_count = min(self.value_count - self.ord, self.block_size as i64) as i32;
            let blocks_count =
                Format::Packed.byte_count(self.packed_ints_version, value_count, bits_per_value);
            input.read_bytes(&mut self.blocks, 0, blocks_count as usize)?;
            PackedIntDecoder::decode_byte_to_long(
                decoder.as_ref(),
                &self.blocks,
                &mut self.values,
                iterations,
            );

            if min_value != 0 {
                for i in 0..value_count as usize {
                    self.values[i] += min_value;
                }
            }
        }
        self.off = 0;
        Ok(())
    }

    pub fn next<T: DataInput + ?Sized>(&mut self, input: &mut T) -> Result<i64> {
        if self.ord == self.value_count {
            bail!("end of file!");
        }
        if self.off == self.block_size {
            self.refill(input)?;
        }
        let value = self.values[self.off];
        self.off += 1;
        self.ord += 1;
        Ok(value)
    }

    pub fn next_longs_ref<T: DataInput + ?Sized>(
        &mut self,
        input: &mut T,
        count: usize,
    ) -> Result<OffsetAndLength> {
        debug_assert!(count as i32 > 0);
        if self.ord == self.value_count {
            bail!("end of file");
        }
        if self.off == self.block_size {
            self.refill(input)?;
        }

        let count = min(count, self.block_size - self.off);
        let count = min(count, (self.value_count - self.ord) as usize);
        let off = self.off;
        self.off += count;
        self.ord += count as i64;
        Ok(OffsetAndLength(off, count))
    }

    fn read_v_long<T: DataInput + ?Sized>(input: &mut T) -> Result<i64> {
        let mut b = input.read_byte()? as i8;
        if b >= 0 {
            return Ok(i64::from(b));
        }

        let mut i = i64::from(b) & 0x7f_i64;

        b = input.read_byte()? as i8;
        i |= (i64::from(b) & 0x7f_i64) << 7;
        if b >= 0 {
            return Ok(i);
        }

        b = input.read_byte()? as i8;
        i |= (i64::from(b) & 0x7f_i64) << 14;
        if b >= 0 {
            return Ok(i);
        }

        b = input.read_byte()? as i8;
        i |= (i64::from(b) & 0x7f_i64) << 21;
        if b >= 0 {
            return Ok(i);
        }

        b = input.read_byte()? as i8;
        i |= (i64::from(b) & 0x7f_i64) << 28;
        if b >= 0 {
            return Ok(i);
        }

        b = input.read_byte()? as i8;
        i |= (i64::from(b) & 0x7f_i64) << 35;
        if b >= 0 {
            return Ok(i);
        }

        b = input.read_byte()? as i8;
        i |= (i64::from(b) & 0x7f_i64) << 42;
        if b >= 0 {
            return Ok(i);
        }

        b = input.read_byte()? as i8;
        i |= (i64::from(b) & 0x7f_i64) << 49;
        if b >= 0 {
            return Ok(i);
        }

        b = input.read_byte()? as i8;
        i |= (i64::from(b) & 0x7f_i64) << 56;
        Ok(i)
    }
}
