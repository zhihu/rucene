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

use error::Result;

use std;
use std::cmp::min;
use std::io::{Read, Write};

use flate2::read::{DeflateDecoder, DeflateEncoder};
use flate2::Compression;

use core::store::io::{DataInput, DataOutput};
use core::util::packed::{
    get_mutable_by_ratio, Mutable, MutableEnum, OffsetAndLength, Reader,
    DEFAULT as DEFAULT_COMPRESSION_RATIO,
};
use core::util::{BitsRequired, UnsignedShift};

const MEMORY_USAGE: i32 = 14;
const MIN_MATCH: i32 = 4;
// minimum length of a match
const MAX_DISTANCE: i32 = 1 << 16;
// maximum distance of a reference
const LAST_LITERALS: i32 = 5;
// the last 5 bytes must be encoded as literals
// log size of the dictionary for compress_hc
const HASH_LOG_HC: i32 = 15;
// const HASH_TABLE_SIZE_HC: i32 = 1 << HASH_LOG_HC;
// const OPTIMAL_ML: i32 = 0x0fi32 + 4 - 1; // match length that doesn't require an additional byte

/// LZ4 compression and decompression routines.
///
/// http://code.google.com/p/lz4/
/// http://fastcompression.blogspot.fr/p/lz4.html
pub struct LZ4;

impl LZ4 {
    fn hash(i: i32, hash_bits: i32) -> i32 {
        ((i as i64 * -1_640_531_535i64) as i32).unsigned_shift((32 - hash_bits) as usize)
    }

    #[allow(dead_code)]
    fn hash_hc(i: i32) -> i32 {
        LZ4::hash(i, HASH_LOG_HC)
    }

    fn read_int(buf: &[u8], i: usize) -> i32 {
        ((i32::from(buf[i])) << 24)
            | ((i32::from(buf[i + 1])) << 16)
            | ((i32::from(buf[i + 2])) << 8)
            | (i32::from(buf[i + 3]))
    }

    #[allow(dead_code)]
    fn read_int_equals(buf: &[u8], i: usize, j: usize) -> bool {
        LZ4::read_int(buf, i) == LZ4::read_int(buf, j)
    }

    fn common_bytes(b: &[u8], o1: usize, o2: usize, limit: usize) -> i32 {
        debug_assert!(o1 < o2);
        let mut count = 0usize;
        while count < limit - o2 && b[o1 + count] == b[o2 + count] {
            count += 1;
        }
        count as i32
    }

    #[allow(dead_code)]
    fn common_bytes_backward(b: &[u8], o1: i32, o2: i32, l1: i32, l2: i32) -> i32 {
        let mut count = 0;
        while o1 - count > l1
            && o2 - count > l2
            && b[(o1 - count) as usize] == b[(o2 - count) as usize]
        {
            count += 1;
        }
        count
    }

    /// Decompress at least <code>decompressedLen</code> bytes into
    /// dest. Please note that <code>dest</code> must be large
    /// enough to be able to hold <b>all</b> decompressed data (meaning that you
    /// need to know the total decompressed length).
    pub fn decompress<R: DataInput + ?Sized>(
        compressed: &mut R,
        decompressed_len: usize,
        dest: &mut [u8],
    ) -> Result<usize> {
        let dest_end = dest.len();
        let mut dest_off = 0;
        loop {
            let token = i32::from(compressed.read_byte()?) & 0xff;
            let mut literal_len = token.unsigned_shift(4);

            if literal_len != 0 {
                if literal_len == 0x0f {
                    let mut len = compressed.read_byte()?;
                    while len == 0xffu8 {
                        literal_len += 0xffi32;
                        len = compressed.read_byte()?;
                    }
                    literal_len += i32::from(len) & 0xff;
                }
                compressed.read_bytes(dest, dest_off, literal_len as usize)?;
                dest_off += literal_len as usize;
            }

            if dest_off >= decompressed_len {
                break;
            }

            // matches
            let match_dec = (i32::from(compressed.read_byte()?) & 0xff)
                | ((i32::from(compressed.read_byte()?) & 0xff) << 8);
            debug_assert!(match_dec > 0);

            let mut match_len = token & 0x0f;
            if match_len == 0x0f {
                let mut len = compressed.read_byte()?;
                while len == 0xff {
                    match_len += 0xFF;
                    len = compressed.read_byte()?;
                }
                match_len += i32::from(len) & 0xff;
            }
            match_len += MIN_MATCH;

            // copying a multiple of 8 bytes can make decompression from 5% to 10% faster
            // Does this applies to Rust ? It's a question.
            let fast_len = ((match_len + 7) as usize) & 0xffff_fff8;
            if match_dec < match_len || dest_off + fast_len > dest_end {
                // overlap -> naive incremental copy
                let mut dest_cur = dest_off - match_dec as usize;
                let end = dest_off + match_len as usize;
                while dest_off < end {
                    dest[dest_off] = dest[dest_cur];
                    dest_off += 1;
                    dest_cur += 1;
                }
            } else {
                // non-overlap -> arraycopy
                let ref_pos = dest_off - match_dec as usize;
                let match_len = match_len as usize;
                let (lhs, rhs) = dest.split_at_mut(dest_off);
                rhs[0..match_len].copy_from_slice(&lhs[ref_pos..ref_pos + match_len]);
                dest_off += match_len;
            }
            if dest_off >= decompressed_len {
                break;
            }
        }
        Ok(dest_off)
    }

    fn encode_len<R: DataOutput + ?Sized>(l: i32, out: &mut R) -> Result<()> {
        let mut value = l;
        while value >= 0xff {
            out.write_byte(0xffu8)?;
            value -= 0xff;
        }
        out.write_byte((value & 0xff) as u8)
    }

    fn encode_literals<R: DataOutput + ?Sized>(
        bytes: &[u8],
        token: i32,
        anchor: usize,
        literal_len: usize,
        out: &mut R,
    ) -> Result<()> {
        out.write_byte(token as u8)?;

        // encode literal length
        if literal_len >= 0x0f {
            LZ4::encode_len(literal_len as i32 - 0x0f, out)?;
        }

        // encode literals
        out.write_bytes(bytes, anchor, literal_len)
    }

    fn encode_last_literals<R: DataOutput + ?Sized>(
        bytes: &[u8],
        anchor: usize,
        literal_len: usize,
        out: &mut R,
    ) -> Result<()> {
        let token: i32 = min(literal_len as i32, 0x0fi32) << 4;
        LZ4::encode_literals(bytes, token, anchor, literal_len, out)
    }

    fn encode_sequence<R: DataOutput + ?Sized>(
        bytes: &[u8],
        anchor: usize,
        match_ref: usize,
        match_off: usize,
        match_len: usize,
        out: &mut R,
    ) -> Result<()> {
        let literal_len = match_off - anchor;
        debug_assert!(match_len >= 4);
        // encode token
        let token: i32 = (0x0f.min(literal_len as i32) << 4) | 0x0f.min(match_len as i32 - 4);
        LZ4::encode_literals(bytes, token, anchor, literal_len, out)?;

        // encode match dec
        let match_dec = match_off - match_ref;
        debug_assert!(match_dec < 1 << 16);
        out.write_byte(match_dec as u8)?;
        out.write_byte((match_dec >> 8) as u8)?;
        // encode match len
        if match_len >= (MIN_MATCH + 0x0f) as usize {
            LZ4::encode_len(match_len as i32 - 0x0f - MIN_MATCH, out)?;
        }

        Ok(())
    }

    /// Compress <code>bytes[off:off+len]</code> into <code>out</code> using
    /// at most 16KB of memory. <code>ht</code> shouldn't be shared across threads
    /// but can safely be reused.
    fn compress<R: DataOutput + ?Sized>(
        bytes: &[u8],
        off: usize,
        len: usize,
        out: &mut R,
        ht: &mut LZ4HashTable,
    ) -> Result<()> {
        let mut off_cur = off;
        let mut anchor = off;
        let end = off + len;

        off_cur += 1;

        if len > (LAST_LITERALS + MIN_MATCH) as usize {
            let limit = end - LAST_LITERALS as usize;
            let match_limit = limit - MIN_MATCH as usize;
            ht.reset(len as i32);
            let hash_log = ht.hash_log;
            let hash_table = ht.hash_table.as_mut().unwrap();
            'main: while off_cur <= limit {
                let mut refer: usize;
                loop {
                    if off_cur >= match_limit {
                        break 'main;
                    }
                    let v = LZ4::read_int(bytes, off_cur);
                    let h = LZ4::hash(v, hash_log) as usize;
                    refer = off + hash_table.get(h) as usize;
                    debug_assert!(
                        (off_cur - off).bits_required() as i32 <= hash_table.get_bits_per_value()
                    );
                    hash_table.set(h, (off_cur - off) as i64);
                    if off_cur - refer < MAX_DISTANCE as usize && LZ4::read_int(bytes, refer) == v {
                        break;
                    }
                    off_cur += 1;
                }

                // compute match length
                let match_len = (MIN_MATCH
                    + LZ4::common_bytes(
                        bytes,
                        refer + MIN_MATCH as usize,
                        off_cur + MIN_MATCH as usize,
                        limit,
                    )) as usize;

                LZ4::encode_sequence(bytes, anchor, refer, off_cur, match_len, out)?;
                off_cur += match_len;
                anchor = off_cur;
            }
        }

        // last literals
        let literal_len = end - anchor;
        debug_assert!(literal_len >= LAST_LITERALS as usize || literal_len == len);
        LZ4::encode_last_literals(bytes, anchor, end - anchor, out)
    }
}

struct LZ4HashTable {
    hash_log: i32,
    hash_table: Option<MutableEnum>,
}

impl Default for LZ4HashTable {
    fn default() -> LZ4HashTable {
        LZ4HashTable {
            hash_log: 0,
            hash_table: None,
        }
    }
}

impl LZ4HashTable {
    pub fn reset(&mut self, len: i32) {
        let bits_per_offset = (len - LAST_LITERALS).bits_required() as i32;
        let bits_per_offset_log = 32 - (bits_per_offset - 1).leading_zeros() as i32;
        self.hash_log = MEMORY_USAGE + 3 - bits_per_offset_log;
        if self.hash_table.is_none()
            || self.hash_table.as_ref().unwrap().size() < (1 << self.hash_log) as usize
            || self.hash_table.as_ref().unwrap().get_bits_per_value() < bits_per_offset
        {
            self.hash_table = Some(get_mutable_by_ratio(
                1usize << self.hash_log,
                bits_per_offset,
                DEFAULT_COMPRESSION_RATIO,
            ));
        } else {
            self.hash_table.as_mut().unwrap().clear();
        }
    }
}

pub trait Compress {
    fn compress(
        &mut self,
        bytes: &[u8],
        off: usize,
        len: usize,
        out: &mut impl DataOutput,
    ) -> Result<()>;
}

#[derive(Default)]
struct LZ4FastCompressor {
    ht: LZ4HashTable,
}

impl Compress for LZ4FastCompressor {
    fn compress(
        &mut self,
        bytes: &[u8],
        off: usize,
        len: usize,
        out: &mut impl DataOutput,
    ) -> Result<()> {
        LZ4::compress(bytes, off, len, out, &mut self.ht)
    }
}

// use vector as a read write buf
struct VecReadWriteBuf {
    buf: Vec<u8>,
    offset: usize,
    length: usize,
}

impl VecReadWriteBuf {
    pub fn new(size: usize) -> VecReadWriteBuf {
        VecReadWriteBuf {
            buf: Vec::with_capacity(size),
            offset: 0,
            length: 0,
        }
    }
}

impl Read for VecReadWriteBuf {
    fn read(&mut self, buf: &mut [u8]) -> std::result::Result<usize, std::io::Error> {
        let read_size = min(buf.len(), self.length);
        if self.offset + read_size <= self.buf.len() {
            buf[0..read_size]
                .as_mut()
                .copy_from_slice(&self.buf[self.offset..self.offset + read_size]);
        } else {
            let ahead = self.buf.len() - self.offset;
            buf[0..ahead]
                .as_mut()
                .copy_from_slice(&self.buf[self.offset..self.buf.len()]);
            buf[ahead..read_size]
                .as_mut()
                .copy_from_slice(&self.buf[0..read_size - ahead]);
        }
        self.offset = (self.offset + read_size) % self.buf.capacity();
        self.length -= read_size;
        Ok(read_size)
    }
}

impl Write for VecReadWriteBuf {
    fn write(&mut self, buf: &[u8]) -> std::result::Result<usize, std::io::Error> {
        if buf.is_empty() {
            return Ok(0);
        }
        let total_size = min(self.buf.capacity() - self.length, buf.len());
        let mut written_size = 0usize;

        if self.buf.len() > self.offset + self.length {
            let write_len = min(self.buf.len() - self.offset - self.length, total_size);
            self.buf[self.offset + self.length..self.offset + self.length + write_len]
                .copy_from_slice(&buf[0..write_len]);
            written_size += write_len;
        }
        if written_size < total_size && self.buf.len() < self.buf.capacity() {
            let write_len = min(
                self.buf.capacity() - self.buf.len(),
                total_size - written_size,
            );
            for _ in 0..write_len {
                self.buf.push(buf[written_size]);
                written_size += 1;
            }
        }
        if written_size < total_size {
            self.buf[0..total_size - written_size]
                .copy_from_slice(&buf[total_size - written_size..total_size]);
        }
        self.length += total_size;
        Ok(total_size)
    }

    fn flush(&mut self) -> std::result::Result<(), std::io::Error> {
        unimplemented!()
    }
}

struct DeflateCompressor {
    compressor: DeflateEncoder<VecReadWriteBuf>,
    compressed: Vec<u8>,
}

impl DeflateCompressor {
    pub fn new(level: i32) -> DeflateCompressor {
        DeflateCompressor {
            compressor: DeflateEncoder::new(
                VecReadWriteBuf::new(1024),
                Compression::new(level as u32),
            ),
            compressed: Vec::with_capacity(64usize),
        }
    }
}

impl Compress for DeflateCompressor {
    fn compress(
        &mut self,
        bytes: &[u8],
        off: usize,
        len: usize,
        out: &mut impl DataOutput,
    ) -> Result<()> {
        let mut total = 0usize;
        let mut finish = false;
        let mut compressed_bytes = 0usize;
        self.compressed.clear();
        loop {
            if total < len {
                let size = self.compressor.write(&bytes[off + total..off + len])?;
                total += size;
                debug_assert!(total <= len);
            } else {
                finish = true;
            }

            compressed_bytes += self.compressor.read_to_end(&mut self.compressed)?;
            if finish {
                break;
            }
        }
        out.write_vint(compressed_bytes as i32)?;
        out.write_bytes(&self.compressed, 0, compressed_bytes)
    }
}

/// A decompressor.
pub trait Decompress: Clone {
    /// Decompress bytes that were stored between offsets <code>offset</code> and
    /// <code>offset+length</code> in the original stream from the compressed
    /// stream <code>in</code> to <code>bytes</code>. After returning, the length
    /// of <code>bytes</code> (<code>bytes.length</code>) must be equal to
    /// <code>length</code>. Implementations of this method are free to resize
    /// <code>bytes</code> depending on their needs.
    ///
    /// @param input, the input that stores the compressed stream
    /// @param original_length, the length of the original data (before compression)
    /// @param offset, bytes before this offset do not need to be decompressed
    /// @param length, bytes after <code>offset+length</code> do not need to be decompressed
    /// @param bytes, a `SimpleBytesStore` where to store the decompressed data
    fn decompress<R: DataInput + ?Sized>(
        &self,
        input: &mut R,
        original_length: usize,
        offset: usize,
        length: usize,
        bytes: &mut Vec<u8>,
        bytes_position: &mut OffsetAndLength,
    ) -> Result<()>;
}

#[derive(Clone)]
struct LZ4Decompressor;

impl Decompress for LZ4Decompressor {
    fn decompress<R: DataInput + ?Sized>(
        &self,
        input: &mut R,
        original_length: usize,
        offset: usize,
        length: usize,
        bytes: &mut Vec<u8>,
        bytes_position: &mut OffsetAndLength,
    ) -> Result<()> {
        debug_assert!(offset + length <= original_length);
        // add 7 padding bytes, this is not necessary but can help decompression run faster
        if bytes.len() < original_length + 7 {
            bytes.resize(original_length + 7, 0u8);
        }
        let decompressed_len = LZ4::decompress(input, offset + length, bytes.as_mut())?;
        if decompressed_len > original_length {
            bail!(
                "Corrupted: lengths mismatch: {} > {}",
                decompressed_len,
                original_length
            );
        }
        bytes_position.0 = offset;
        bytes_position.1 = length;
        Ok(())
    }
}

#[derive(Clone)]
struct DeflateDecompressor;

impl Default for DeflateDecompressor {
    fn default() -> DeflateDecompressor {
        DeflateDecompressor {}
    }
}

impl Decompress for DeflateDecompressor {
    fn decompress<R: DataInput + ?Sized>(
        &self,
        input: &mut R,
        original_length: usize,
        offset: usize,
        length: usize,
        bytes: &mut Vec<u8>,
        bytes_position: &mut OffsetAndLength,
    ) -> Result<()> {
        debug_assert!(offset + length <= original_length);
        if length == 0 {
            bytes_position.1 = 0;
            return Ok(());
        }

        let compressed_length = input.read_vint()? as usize;
        let mut compressed = vec![0u8; compressed_length];
        // compressed.resize(compressed_length, 0u8);
        input.read_exact(&mut compressed)?;
        let mut decompressor = DeflateDecoder::new(compressed[0..compressed_length].as_ref());

        bytes.clear();
        let size = decompressor.read_to_end(bytes)?;
        if size != original_length {
            bail!(
                "Corrupt: lengths mismatch: {}, != {}",
                size,
                original_length
            );
        }
        bytes_position.0 = offset;
        bytes_position.1 = length;
        Ok(())
    }
}

/// A decompressor.
///
/// Current we support [`LZ4`](http://www.lz4.org) and
/// [`Deflate`](https://en.wikipedia.org/wiki/DEFLATE) two algorithms.
#[derive(Clone)]
pub struct Decompressor(DecompressorEnum);

#[derive(Clone)]
enum DecompressorEnum {
    LZ4(LZ4Decompressor),
    Deflate(DeflateDecompressor),
}

impl Decompress for Decompressor {
    fn decompress<R: DataInput + ?Sized>(
        &self,
        input: &mut R,
        original_length: usize,
        offset: usize,
        length: usize,
        bytes: &mut Vec<u8>,
        bytes_position: &mut OffsetAndLength,
    ) -> Result<()> {
        match &self.0 {
            DecompressorEnum::LZ4(d) => d.decompress(
                input,
                original_length,
                offset,
                length,
                bytes,
                bytes_position,
            ),
            DecompressorEnum::Deflate(d) => d.decompress(
                input,
                original_length,
                offset,
                length,
                bytes,
                bytes_position,
            ),
        }
    }
}

/// A compression mode. Tells how much effort should be spent on compression and
/// decompression of stored fields.
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum CompressionMode {
    /// A compression mode that trades compression ratio for speed. Although the
    /// compression ratio might remain high, compression and decompression are
    /// very fast. Use this mode with indices that have a high update rate but
    /// should be able to load documents from disk quickly.
    FAST,
    /// A compression mode that trades speed for compression ratio. Although
    /// compression and decompression might be slow, this compression mode should
    /// provide a good compression ratio. This mode might be interesting if/when
    /// your index size is much bigger than your OS cache.
    HighCompression,
    // FastDecompression,  // currently not implemented
}

/// A data compressor.
///
/// Current we support [`LZ4`](http://www.lz4.org) and
/// [`Deflate`](https://en.wikipedia.org/wiki/DEFLATE) two algorithms.
pub struct Compressor(CompressorEnum);

enum CompressorEnum {
    LZ4Fast(LZ4FastCompressor),
    Deflate(DeflateCompressor),
}

impl Compress for Compressor {
    fn compress(
        &mut self,
        bytes: &[u8],
        off: usize,
        len: usize,
        out: &mut impl DataOutput,
    ) -> Result<()> {
        match &mut self.0 {
            CompressorEnum::LZ4Fast(c) => c.compress(bytes, off, len, out),
            CompressorEnum::Deflate(c) => c.compress(bytes, off, len, out),
        }
    }
}

impl CompressionMode {
    pub fn new_compressor(self) -> Compressor {
        match self {
            CompressionMode::FAST => {
                Compressor(CompressorEnum::LZ4Fast(LZ4FastCompressor::default()))
            }
            // notes:
            // 3 is the highest level that doesn't have lazy match evaluation
            // 6 is the default, higher than that is just a waste of cpu
            CompressionMode::HighCompression => {
                Compressor(CompressorEnum::Deflate(DeflateCompressor::new(6)))
            }
        }
    }

    pub fn new_decompressor(self) -> Decompressor {
        match self {
            CompressionMode::FAST => Decompressor(DecompressorEnum::LZ4(LZ4Decompressor {})),
            CompressionMode::HighCompression => {
                Decompressor(DecompressorEnum::Deflate(DeflateDecompressor::default()))
            }
        }
    }
}
