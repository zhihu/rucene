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

//! Utility functions for reading and writing versioned headers.
//!
//! Writing codec headers is useful to ensure that a file is in
//! the format you think it is.

use core::store::io::{
    BufferedChecksumIndexInput, ChecksumIndexInput, DataInput, DataOutput, IndexInput, IndexOutput,
};

use core::util::id2str;
use core::util::ID_LENGTH;
use error::ErrorKind::{CorruptIndex, IllegalArgument, IllegalState};
use error::Result;
use std::io::Read;

/// Constant to identify the start of a codec header.
pub const CODEC_MAGIC: i32 = 0x3FD7_6C17;
/// Constant to identify the start of a codec footer.
pub const FOOTER_MAGIC: i32 = !CODEC_MAGIC;

/// Writes a codec header, which records both a string to identify the file and a version number.
///
/// This header can be parsed and validated with {@link #check_header()}.
///
/// CodecHeader --> Magic,CodecName,Version
/// * Magic -->  DataOutput#write_int. This identifies the start of the header. It is always
///   `CODEC_MAGIC`.
/// * CodecName --> DataOutput#write_string. This is a string to identify this file.
/// * Version --> DataOutput#write_int. Records the version of the file.
///
/// Note that the length of a codec header depends only upon the name of the codec,
/// so this length can be computed at any time with {@link #header_len(&str)}.
pub fn write_header<T: DataOutput + ?Sized>(out: &mut T, codec: &str, version: i32) -> Result<()> {
    let clen = codec.len();
    if clen >= 128 {
        bail!(IllegalArgument(format!(
            "codec must be simple ASCII less than 128 characters, got {}[length={}]",
            codec, clen,
        )));
    }
    out.write_int(CODEC_MAGIC)?;
    out.write_string(codec)?;
    out.write_int(version)
}

/// Writes a codec header for an index file, which records both a string to
/// identify the format of the file, a version number, and data to identify
/// the file instance (ID and auxiliary suffix such as generation).
///
/// This header can be parsed and validated with check_index_header().
///
/// IndexHeader --> CodecHeader,ObjectID,ObjectSuffix
/// * CodecHeader   --> {@link #writeHeader}
/// * ObjectID     --> DataOutput#write_byte
/// * ObjectSuffix --> SuffixLength,SuffixBytes
/// * SuffixLength  --> DataOutput#write_byte
/// * SuffixBytes   --> {@link DataOutput#writeByte byte}<sup>SuffixLength</sup>
///
/// Note that the length of an index header depends only upon the
/// name of the codec and suffix, so this length can be computed at any time
/// with {@link #index_header_length(String,String)}.
pub fn write_index_header(
    out: &mut impl DataOutput,
    codec: &str,
    version: i32,
    id: &[u8],
    suffix: &str,
) -> Result<()> {
    if id.len() != ID_LENGTH {
        bail!(IllegalArgument(format!("Invalid id: {:?}", id)));
    }
    write_header(out, codec, version)?;
    out.write_bytes(id, 0, id.len())?;
    let slen = suffix.len() as usize;

    if slen >= 256 {
        bail!(IllegalArgument(format!(
            "suffix must be simple ASCII less than 256 characters, got {}[length={}]",
            suffix, slen
        )));
    }
    out.write_byte(slen as u8)?;
    out.write_bytes(&suffix.as_bytes(), 0, slen)
}

/// Writes a codec footer, which records both a checksum
/// algorithm ID and a checksum. This footer can
/// be parsed and validated with check_footer(ChecksumIndexInput).
///
/// CodecFooter --> Magic,AlgorithmID,Checksum
/// * Magic --> {@link DataOutput#writeInt Uint32}. This identifies the start of the footer. It is
///   always {@value #FOOTER_MAGIC}.
/// * AlgorithmID --> {@link DataOutput#write_Int}. This indicates the checksum algorithm used.
///   Currently this is always 0, for zlib-crc32.
/// * Checksum --> {@link DataOutput#write_long}. The actual checksum value for all previous bytes
///   in the stream, including the bytes from Magic and AlgorithmID.
pub fn write_footer(output: &mut impl IndexOutput) -> Result<()> {
    output.write_int(FOOTER_MAGIC)?;
    output.write_int(0)?;
    write_crc(output)
}

/// Computes the length of a codec header.
fn header_length(codec: &str) -> usize {
    9 + codec.len()
}

/// Computes the length of an index header.
pub fn index_header_length(codec: &str, suffix: &str) -> usize {
    header_length(codec) + ID_LENGTH + 1 + suffix.len()
}

/// Reads and validates a header previously written with `writer_header`.
///
/// When reading a file, supply the expected `Codec` and an expected version range
/// (min_version to max_version)
pub fn check_header<T: DataInput + ?Sized>(
    data_input: &mut T,
    codec: &str,
    min_ver: i32,
    max_ver: i32,
) -> Result<i32> {
    let actual_header = data_input.read_int()?;
    if actual_header != CODEC_MAGIC {
        bail!(CorruptIndex(format!(
            "codec header mismatch: actual=0x{:X}, expected=0x{:X}",
            actual_header, CODEC_MAGIC
        )));
    }
    check_header_no_magic(data_input, codec, min_ver, max_ver)
}

/// Like `check_header` except this version assumes the first i32 has already
/// been read and validated from the input.
pub fn check_header_no_magic<T: DataInput + ?Sized>(
    data_input: &mut T,
    codec: &str,
    min_ver: i32,
    max_ver: i32,
) -> Result<i32> {
    let actual_codec = data_input.read_string()?;
    if actual_codec != codec {
        bail!(CorruptIndex(format!(
            "codec mismatch: actual={}, expected={}",
            actual_codec, codec
        )));
    }
    let actual_ver = data_input.read_int()?;
    if actual_ver < min_ver || actual_ver > max_ver {
        bail!(CorruptIndex(format!(
            "index format either too new or too old: {} <= {} <= {} doesn't hold",
            min_ver, actual_ver, max_ver
        )));
    }
    Ok(actual_ver)
}

/// Reads and validates a header previously written with write_index_header().
///
/// When reading a file, supply the expected `Codec` and an expected version range
/// (min_version to max_version), and object ID and suffix.
pub fn check_index_header<T: DataInput + ?Sized>(
    data_input: &mut T,
    codec: &str,
    min_ver: i32,
    max_ver: i32,
    expected_id: &[u8],
    expected_suffix: &str,
) -> Result<i32> {
    let version = check_header(data_input, codec, min_ver, max_ver)?;
    check_index_header_id(data_input, expected_id)?;
    check_index_header_suffix(data_input, expected_suffix)?;
    Ok(version)
}

fn check_index_header_id<T: DataInput + ?Sized>(
    data_input: &mut T,
    expected_id: &[u8],
) -> Result<()> {
    let mut actual_id = [0u8; ID_LENGTH];
    data_input.read_exact(&mut actual_id)?;
    if actual_id != expected_id {
        bail!(CorruptIndex(format!(
            "file mismatch, expected_id={}, got={}",
            id2str(expected_id),
            id2str(&actual_id)
        )));
    }
    Ok(())
}

/// Expert: just reads and verifies the suffix of an index header
pub fn check_index_header_suffix<T: DataInput + ?Sized>(
    data_input: &mut T,
    expected_suffix: &str,
) -> Result<()> {
    let suffix_len = data_input.read_byte()? as usize;
    let mut suffix_bytes = vec![0u8; suffix_len];
    data_input.read_exact(&mut suffix_bytes)?;
    let suffix = ::std::str::from_utf8(&suffix_bytes)?;
    if suffix != expected_suffix {
        bail!(CorruptIndex(format!(
            "file mismatch, expected suffix={}, got={}",
            expected_suffix, suffix
        )));
    }
    Ok(())
}

/// Expert: verifies the incoming `IndexInput` has an index header
/// and that its segment ID matches the expected one, and then copies
/// that index header into the provided `DataOutput`.  This is
/// useful when building compound files.
pub fn verify_and_copy_index_header<I: IndexInput + ?Sized, O: DataOutput + ?Sized>(
    input: &mut I,
    output: &mut O,
    expected_id: &[u8],
) -> Result<()> {
    // make sure it's large enough to have a header and footer
    if (input.len() as usize) < footer_length() + header_length("") {
        bail!(CorruptIndex(
            "compound sub-files must have a valid codec header and footer: file is too small"
                .into()
        ));
    }

    let actual_header = input.read_int()?;
    if actual_header != CODEC_MAGIC {
        bail!(CorruptIndex(
            "compound sub-files must have a valid codec header and footer: codec header mismatch"
                .into()
        ));
    }

    // we can't verify these, so we pass-through
    let codec = input.read_string()?;
    let version = input.read_int()?;

    // verify id:
    check_index_header_id(input, expected_id)?;

    // we can't verify extension either, so we pass-through
    let suffix_length = input.read_byte()? as usize & 0xff;
    let mut suffix_bytes = vec![0u8; suffix_length];
    input.read_exact(&mut suffix_bytes)?;

    // now write the header we just verified
    output.write_int(CODEC_MAGIC)?;
    output.write_string(&codec)?;
    output.write_int(version)?;
    output.write_bytes(expected_id, 0, expected_id.len())?;
    output.write_byte(suffix_length as u8)?;
    output.write_bytes(&suffix_bytes, 0, suffix_length)
}

/// Computes the length of a codec footer.
#[inline(always)]
pub fn footer_length() -> usize {
    16
}

pub fn validate_footer<T: IndexInput + ?Sized>(input: &mut T) -> Result<()> {
    let remaining = input.len() as i64 - input.file_pointer();
    let expected = footer_length() as i64;

    if remaining < expected {
        bail!(CorruptIndex(format!(
            "misplaced codec footer (file truncated?): remaining={}, expected={}",
            remaining, expected
        )))
    } else if remaining > expected {
        bail!(CorruptIndex(format!(
            "misplaced codec footer (file extended?): remaining={}, expected={}",
            remaining, expected
        )))
    } else {
        let magic = input.read_int()?;
        if magic != FOOTER_MAGIC {
            bail!(CorruptIndex(format!(
                "codec footer mismatch: actual={} vs expected={}",
                magic, FOOTER_MAGIC
            )));
        }
        let algorithm_id = input.read_int()?;
        if algorithm_id != 0 {
            bail!(CorruptIndex(format!(
                "codec footer mismatch: unknown algorithm_id: {}",
                algorithm_id
            )));
        }
        Ok(())
    }
}

/// Validates the codec footer previously written by writer_footer(), optionally
/// passing an unexpected exception that has already occurred.
pub fn check_footer(input: &mut impl ChecksumIndexInput) -> Result<i64> {
    validate_footer(input)?;
    let actual_checksum: i64 = input.checksum();
    let expected_checksum: i64 = read_crc(input)?;
    if actual_checksum != expected_checksum {
        bail!(CorruptIndex(format!(
            "checksum failed (hardware problems?): expected=0x{:X}, actual=0x{:X}",
            expected_checksum, actual_checksum
        )));
    }
    Ok(actual_checksum)
}

fn read_crc<T: IndexInput + ?Sized>(input: &mut T) -> Result<i64> {
    let val = input.read_long()?;
    if (val as u64 & 0xFFFF_FFFF_0000_0000) != 0 {
        bail!(CorruptIndex(format!("Illegal CRC-32 checksum: {}", val)));
    }
    Ok(val)
}

fn write_crc<T: IndexOutput + ?Sized>(output: &mut T) -> Result<()> {
    let value = output.checksum()?;
    if value as u64 & 0xFFFF_FFFF_0000_0000 != 0 {
        bail!(IllegalState(format!("Illegal CRC-32 checksum: {}", value)));
    }
    output.write_long(value)
}

/// Returns (but does not validate) the checksum previously written by check_footer().
pub fn retrieve_checksum<T: IndexInput + ?Sized>(input: &mut T) -> Result<i64> {
    let length = input.len();
    let footer_length = footer_length() as u64;
    if length < footer_length {
        bail!(CorruptIndex(format!(
            "misplaced codec footer (file truncated?): length={}, but footer_length={}",
            length, footer_length
        )));
    }
    input.seek((length - footer_length) as i64)?;
    validate_footer(input)?;

    read_crc(input)
}

// TODO: duplicates to refactor

/// validate the input checksum
pub fn check_checksum<T: IndexInput + ?Sized>(input: &mut T, actual_checksum: i64) -> Result<()> {
    let expected_checksum: i64 = read_crc(input)?;
    if actual_checksum != expected_checksum {
        bail!(CorruptIndex(format!(
            "checksum failed (hardware problems?): expected=0x{:X}, actual=0x{:X}",
            expected_checksum, actual_checksum
        )));
    }
    Ok(())
}

/// Clones the provided input, reads all bytes from the file, and calls check_footer().
///
/// Note that this method may be slow, as it must process the entire file.
/// If you just need to extract the checksum value, call {@link #retrieve_checksum}.
pub fn checksum_entire_file<T: IndexInput + ?Sized>(input: &T) -> Result<i64> {
    let mut index = input.clone()?;
    index.seek(0)?;
    let mut checksum = BufferedChecksumIndexInput::new(index);
    let mut len = checksum.len();
    let mut pos = checksum.file_pointer() as u64;
    if len < footer_length() as u64 {
        bail!(CorruptIndex(format!(
            "misplaced codec footer (file truncated?): length={} but footerLength=={}",
            checksum.len(),
            footer_length()
        )));
    }
    const BUFSIZ: u64 = 1024 * 64;
    let mut buffer = [0u8; BUFSIZ as usize];
    len -= footer_length() as u64;

    while pos < len {
        let size = if len - pos < BUFSIZ {
            len - pos
        } else {
            BUFSIZ
        };
        pos += checksum.read(&mut buffer[0..size as usize])? as u64;
    }

    validate_footer(&mut checksum)?;
    let actual = checksum.checksum();
    check_checksum(&mut checksum, actual)?;
    Ok(actual)
}
