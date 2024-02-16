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

use error::{
    ErrorKind::{CorruptIndex, IllegalArgument, UnexpectedEOF},
    Result,
};

use std::any::Any;
use std::boxed::Box;
use std::cmp::min;
use std::io::{self, Read};
use std::ptr;
use std::sync::Arc;

use core::codec::codec_util::*;
use core::codec::field_infos::{FieldInfo, FieldInfos};
use core::codec::segment_infos::{segment_file_name, SegmentInfo};
use core::codec::stored_fields::{
    CompressingStoredFieldsWriter, StoredFieldsFormat, StoredFieldsReader, StoredFieldsWriterEnum,
};
use core::codec::Codec;
use core::doc::{Status as VisitStatus, StoredFieldVisitor};
use core::store::directory::Directory;
use core::store::io::{ByteArrayDataInput, DataInput, IndexInput};
use core::store::IOContext;
use core::util::packed::{get_reader_iterator_no_header, get_reader_no_header};
use core::util::packed::{Format, OffsetAndLength, Reader, ReaderEnum, ReaderIterator};
use core::util::BytesRef;
use core::util::DocId;
use core::util::{CompressionMode, Decompress, Decompressor};
use core::util::{UnsignedShift, ZigZagEncoding};

/// Extension of stored fields file
pub const STORED_FIELDS_EXTENSION: &str = "fdt";
/// Extension of stored fields index file
pub const STORED_FIELDS_INDEX_EXTENSION: &str = "fdx";

pub const CODEC_SFX_IDX: &str = "Index";
pub const CODEC_SFX_DAT: &str = "Data";

pub const VERSION_START: i32 = 0;
pub const VERSION_CHUNK_STATS: i32 = 1;
pub const VERSION_CURRENT: i32 = VERSION_CHUNK_STATS;

pub const STRING: i32 = 0x00;
pub const BYTE_ARR: i32 = 0x01;
pub const NUMERIC_INT: i32 = 0x02;
pub const NUMERIC_FLOAT: i32 = 0x03;
pub const NUMERIC_LONG: i32 = 0x04;
pub const NUMERIC_DOUBLE: i32 = 0x05;

pub const TYPE_BITS: i32 = 3;
// NUMERIC_DOUBLE.bits_required()
pub const TYPE_MASK: i32 = 7; // 3 bits max value, PackedInts.maxValue(TYPE_BITS)

// -0 isn't compressed
pub const NEGATIVE_ZERO_FLOAT: i32 = -2_147_483_648;
// f32::to_bits(-0f32) as i32;
pub const NEGATIVE_ZERO_DOUBLE: i64 = -9_223_372_036_854_775_808;
// f64::to_bits(-0f64) as i64;
// for compression of timestamps
pub const SECOND: i64 = 1000;
pub const HOUR: i64 = 60 * 60 * SECOND;
pub const DAY: i64 = HOUR * 24;
pub const SECOND_ENCODING: i32 = 0x40;
pub const HOUR_ENCODING: i32 = 0x80;
pub const DAY_ENCODING: i32 = 0xC0;

/// A `StoredFieldsFormat` that compresses documents in chunks in
/// order to improve the compression ratio.
///
/// For a chunk size of chunk_size bytes, `StoredFieldsFormat` does not support
/// documents larger than (2^31 - chunk_size) bytes.
///
/// For optimal performance, you should use a `MergePolicy` that returns
/// segments that have the biggest byte size first.
pub struct CompressingStoredFieldsFormat {
    format_name: String,
    segment_suffix: String,
    compression_mode: CompressionMode,
    chunk_size: i32,
    max_docs_per_chunk: i32,
    block_size: i32,
}

impl CompressingStoredFieldsFormat {
    pub fn new(
        format_name: &str,
        segment_suffix: &str,
        compression_mode: CompressionMode,
        chunk_size: i32,
        max_docs_per_chunk: i32,
        block_size: i32,
    ) -> CompressingStoredFieldsFormat {
        debug_assert!(chunk_size >= 1 && max_docs_per_chunk >= 1 && block_size >= 1);

        CompressingStoredFieldsFormat {
            format_name: String::from(format_name),
            segment_suffix: String::from(segment_suffix),
            compression_mode,
            chunk_size,
            max_docs_per_chunk,
            block_size,
        }
    }
}

impl StoredFieldsFormat for CompressingStoredFieldsFormat {
    type Reader = CompressingStoredFieldsReader;
    fn fields_reader<D: Directory, DW: Directory, C: Codec>(
        &self,
        directory: &DW,
        si: &SegmentInfo<D, C>,
        field_info: Arc<FieldInfos>,
        io_ctx: &IOContext,
    ) -> Result<Self::Reader> {
        CompressingStoredFieldsReader::new(
            directory,
            si,
            &self.segment_suffix,
            field_info,
            io_ctx,
            &self.format_name,
            self.compression_mode,
        )
    }

    fn fields_writer<D, DW, C>(
        &self,
        directory: Arc<DW>,
        si: &mut SegmentInfo<D, C>,
        ioctx: &IOContext,
    ) -> Result<StoredFieldsWriterEnum<DW::IndexOutput>>
    where
        D: Directory,
        DW: Directory,
        DW::IndexOutput: 'static,
        C: Codec,
    {
        Ok(StoredFieldsWriterEnum::Compressing(
            CompressingStoredFieldsWriter::new(
                directory,
                si,
                &self.segment_suffix,
                ioctx,
                &self.format_name,
                self.compression_mode,
                self.chunk_size as usize,
                self.max_docs_per_chunk as usize,
                self.block_size as usize,
            )?,
        ))
    }
}

pub struct CompressingStoredFieldsIndexReader {
    max_doc: i32,
    doc_bases: Vec<i32>,
    start_pointers: Vec<i64>,
    avg_chunk_docs: Vec<i32>,
    avg_chunk_sizes: Vec<i64>,
    doc_bases_deltas: Vec<ReaderEnum>,
    // delta from the avg
    start_pointers_deltas: Vec<ReaderEnum>,
    // delta from the avg
}

impl CompressingStoredFieldsIndexReader {
    // It is the responsibility of the caller to close fieldsIndexIn after this constructor
    // has been called
    pub fn new<T: IndexInput + ?Sized, D: Directory, C: Codec>(
        fields_index_in: &mut T,
        si: &SegmentInfo<D, C>,
    ) -> Result<CompressingStoredFieldsIndexReader> {
        let mut doc_bases = Vec::new();
        let mut start_pointers = Vec::new();
        let mut avg_chunk_docs = Vec::new();
        let mut avg_chunk_sizes = Vec::new();
        let mut doc_bases_deltas = Vec::new();
        let mut start_pointers_deltas = Vec::new();

        let packed_ints_version = fields_index_in.read_vint()?;

        // let mut block_count = 0;
        loop {
            let num_chunks = fields_index_in.read_vint()?;
            if num_chunks == 0 {
                break;
            }
            // doc_bases
            doc_bases.push(fields_index_in.read_vint()?);
            avg_chunk_docs.push(fields_index_in.read_vint()?);
            let bits_per_doc_base = fields_index_in.read_vint()?;
            if bits_per_doc_base > 32 {
                bail!(CorruptIndex(format!(
                    "bits_per_doc_base: {}",
                    bits_per_doc_base
                )));
            }
            doc_bases_deltas.push(get_reader_no_header(
                fields_index_in,
                Format::Packed,
                packed_ints_version,
                num_chunks as usize,
                bits_per_doc_base,
            )?);

            // start pointers
            start_pointers.push(fields_index_in.read_vlong()?);
            avg_chunk_sizes.push(fields_index_in.read_vlong()?);
            let bits_per_start_pointer = fields_index_in.read_vint()?;
            if bits_per_start_pointer > 64 {
                bail!(CorruptIndex(format!(
                    "bits_per_start_pointer: {}",
                    bits_per_start_pointer
                )));
            }
            start_pointers_deltas.push(get_reader_no_header(
                fields_index_in,
                Format::Packed,
                packed_ints_version,
                num_chunks as usize,
                bits_per_start_pointer,
            )?);

            // block_count += 1;
        }

        Ok(CompressingStoredFieldsIndexReader {
            max_doc: si.max_doc,
            doc_bases,
            start_pointers,
            avg_chunk_docs,
            avg_chunk_sizes,
            doc_bases_deltas,
            start_pointers_deltas,
        })
    }

    fn block(&self, doc_id: DocId) -> usize {
        let mut lo = 0usize;
        let mut hi = self.doc_bases.len() - 1usize;
        while lo <= hi {
            let mid = (lo + hi) >> 1;
            let mid_value = self.doc_bases[mid];
            if mid_value == doc_id {
                return mid;
            } else if mid_value < doc_id {
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        hi
    }

    fn relative_doc_base(&self, block: usize, relative_chunk: usize) -> DocId {
        let expected = self.avg_chunk_docs[block] as usize * relative_chunk;
        let delta = self.doc_bases_deltas[block].get(relative_chunk).decode();
        (expected as i64 + delta) as DocId
    }

    fn relative_start_pointer(&self, block: usize, relative_chunk: usize) -> i64 {
        let expected = self.avg_chunk_sizes[block] as usize * relative_chunk;
        let delta = self.start_pointers_deltas[block]
            .get(relative_chunk)
            .decode();
        expected as i64 + delta
    }

    fn relative_chunk(&self, block: usize, relative_doc: DocId) -> Result<usize> {
        let mut lo = 0usize;
        let mut hi = self.doc_bases_deltas[block].size() - 1usize;
        while lo <= hi {
            let mid = (lo + hi) >> 1;
            let mid_value = self.relative_doc_base(block, mid);
            if mid_value == relative_doc {
                return Ok(mid);
            } else if mid_value < relative_doc {
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        Ok(hi)
    }

    pub fn start_pointer(&self, doc_id: DocId) -> Result<i64> {
        debug_assert!(doc_id >= 0 && doc_id < self.max_doc);
        let block = self.block(doc_id);
        let relative_chunk = self.relative_chunk(block, doc_id - self.doc_bases[block])?;
        Ok(self.start_pointers[block] + self.relative_start_pointer(block, relative_chunk))
    }
}

/// `StoredFieldsReader` impl for `CompressingStoredFieldsFormat`
pub struct CompressingStoredFieldsReader {
    version: i32,
    field_infos: Arc<FieldInfos>,
    index_reader: Arc<CompressingStoredFieldsIndexReader>,
    max_pointer: i64,
    fields_stream: Box<dyn IndexInput>,
    chunk_size: i32,
    packed_ints_version: i32,
    compression_mode: CompressionMode,
    decompressor: Decompressor,
    num_docs: i32,
    merging: bool,
    num_chunks: i64,
    // number of compressed blocks written
    num_dirty_chunks: i64,
    // number of incomplete compressed blocks written
    // the next fiels are for `BlockState` in lucene
    doc_base: DocId,
    chunk_docs: usize,
    sliced: bool,
    offsets: Vec<i32>,
    num_stored_fields: Vec<i32>,
    start_pointer: i64,
    bytes: Vec<u8>,
    bytes_position: OffsetAndLength,
    spare: Vec<u8>,
    spare_position: OffsetAndLength,
    // from document data input
    current_doc: SerializedDocument,
    // current decompressed byte length for filed `bytes`
}

unsafe impl Send for CompressingStoredFieldsReader {}

unsafe impl Sync for CompressingStoredFieldsReader {}

impl CompressingStoredFieldsReader {
    pub fn new<D: Directory, DW: Directory, C: Codec>(
        dir: &DW,
        si: &SegmentInfo<D, C>,
        segment_suffix: &str,
        field_infos: Arc<FieldInfos>,
        context: &IOContext,
        format_name: &str,
        compression_mode: CompressionMode,
    ) -> Result<CompressingStoredFieldsReader> {
        // load the index into memory
        let index_name = segment_file_name(&si.name, segment_suffix, STORED_FIELDS_INDEX_EXTENSION);
        let mut index_stream = dir.open_checksum_input(&index_name, context)?;
        let codec_name_idx = String::from(format_name) + CODEC_SFX_IDX;
        let version = check_index_header(
            &mut index_stream,
            &codec_name_idx,
            VERSION_START,
            VERSION_CURRENT,
            &si.id,
            segment_suffix,
        )?;
        debug_assert_eq!(
            index_header_length(&codec_name_idx, segment_suffix),
            index_stream.file_pointer() as usize
        );
        let index_reader = CompressingStoredFieldsIndexReader::new(&mut index_stream, si)?;
        let max_pointer = index_stream.read_vlong()?;
        check_footer(&mut index_stream)?;

        let fields_stream_fn = segment_file_name(&si.name, segment_suffix, STORED_FIELDS_EXTENSION);
        let mut fields_stream = dir.open_input(&fields_stream_fn, context)?;
        let codec_name_dat = String::from(format_name) + CODEC_SFX_DAT;
        let fields_version = check_index_header(
            fields_stream.as_mut(),
            &codec_name_dat,
            VERSION_START,
            VERSION_CURRENT,
            &si.id,
            segment_suffix,
        )?;
        if version != fields_version {
            bail!(CorruptIndex(format!(
                "Version mismatch between stored fields index and data: {} != {}",
                version, fields_version
            )));
        }
        debug_assert_eq!(
            index_header_length(&codec_name_dat, segment_suffix),
            fields_stream.file_pointer() as usize
        );

        let chunk_size = fields_stream.read_vint()?;
        let packed_ints_version = fields_stream.read_vint()?;

        let mut num_chunks = -1i64;
        let mut num_dirty_chunks = -1i64;
        if version >= VERSION_CHUNK_STATS {
            fields_stream.seek(max_pointer)?;
            num_chunks = fields_stream.read_vlong()?;
            num_dirty_chunks = fields_stream.read_vlong()?;
            if num_dirty_chunks > num_chunks {
                bail!(CorruptIndex(format!(
                    "invalid chunk counts: dirty={}, total={}",
                    num_dirty_chunks, num_chunks
                )));
            }
        }

        retrieve_checksum(fields_stream.as_mut())?;

        let decompressor = compression_mode.new_decompressor();
        Ok(CompressingStoredFieldsReader {
            version,
            field_infos,
            index_reader: Arc::new(index_reader),
            max_pointer,
            fields_stream,
            chunk_size,
            packed_ints_version,
            compression_mode,
            decompressor,
            num_docs: si.max_doc,
            merging: false,
            num_chunks,
            num_dirty_chunks,
            doc_base: 0,
            chunk_docs: 0,
            sliced: false,
            offsets: Vec::new(),
            num_stored_fields: Vec::new(),
            start_pointer: 0i64,
            bytes: vec![0u8; chunk_size as usize * 2],
            bytes_position: OffsetAndLength(0, 0),
            spare: vec![],
            spare_position: OffsetAndLength(0, 0),
            current_doc: SerializedDocument {
                length: 0,
                num_stored_fields: 0,
                decompressed: 0,
                input: DocumentInput::Bytes(ByteArrayDataInput::new(BytesRef::default())),
            },
        })
    }

    #[inline]
    pub fn version(&self) -> i32 {
        self.version
    }

    #[inline]
    pub fn index_reader(&self) -> &CompressingStoredFieldsIndexReader {
        self.index_reader.as_ref()
    }

    #[inline]
    pub fn chunk_size(&self) -> i32 {
        self.chunk_size
    }

    #[inline]
    pub fn packed_ints_version(&self) -> i32 {
        self.packed_ints_version
    }

    #[inline]
    pub fn compression_mode(&self) -> CompressionMode {
        self.compression_mode
    }

    #[inline]
    pub fn num_chunks(&self) -> i64 {
        self.num_chunks
    }

    #[inline]
    pub fn num_dirty_chunks(&self) -> i64 {
        self.num_dirty_chunks
    }

    #[inline]
    pub fn max_pointer(&self) -> i64 {
        self.max_pointer
    }

    #[inline]
    pub fn fields_stream_mut(&mut self) -> &mut dyn IndexInput {
        &mut *self.fields_stream
    }

    pub fn current_doc_mut(&mut self) -> &mut SerializedDocument {
        &mut self.current_doc
    }

    fn copy_for_merge(&self) -> Result<CompressingStoredFieldsReader> {
        Ok(CompressingStoredFieldsReader {
            version: self.version,
            field_infos: self.field_infos.clone(),
            index_reader: Arc::clone(&self.index_reader),
            max_pointer: self.max_pointer,
            fields_stream: self.fields_stream.clone()?,
            chunk_size: self.chunk_size,
            packed_ints_version: self.packed_ints_version,
            compression_mode: self.compression_mode,
            decompressor: self.decompressor.clone(),
            num_docs: self.num_docs,
            merging: true,
            num_chunks: self.num_chunks,
            num_dirty_chunks: self.num_dirty_chunks,
            doc_base: 0,
            chunk_docs: 0,
            sliced: false,
            offsets: Vec::new(),
            num_stored_fields: Vec::new(),
            start_pointer: 0i64,
            bytes: vec![0u8; self.chunk_size as usize * 2],
            bytes_position: OffsetAndLength(0, 0),
            spare: vec![],
            spare_position: OffsetAndLength(0, 0),
            current_doc: SerializedDocument {
                length: 0,
                num_stored_fields: 0,
                decompressed: 0,
                input: DocumentInput::Compressing(CompressingStoredFieldsInput {
                    reader: ptr::null_mut(),
                }),
            },
        })
    }

    fn read_field<V: StoredFieldVisitor + ?Sized>(
        input: &mut impl DataInput,
        visitor: &mut V,
        info: &FieldInfo,
        bits: i32,
    ) -> Result<()> {
        match bits & TYPE_MASK {
            BYTE_ARR => {
                let length = input.read_vint()? as usize;
                let mut data = vec![0u8; length];
                input.read_exact(data.as_mut())?;
                visitor.add_binary_field(info, data)?;
            }
            STRING => {
                let length = input.read_vint()? as usize;
                let mut data = vec![0u8; length];
                input.read_exact(data.as_mut())?;
                visitor.add_string_field(info, data)?;
            }
            NUMERIC_INT => {
                visitor.add_int_field(info, input.read_zint()?)?;
            }
            NUMERIC_FLOAT => {
                visitor.add_float_field(info, Self::read_zfloat(input)?)?;
            }
            NUMERIC_LONG => {
                visitor.add_long_field(info, Self::read_tlong(input)?)?;
            }
            NUMERIC_DOUBLE => {
                visitor.add_double_field(info, Self::read_zdouble(input)?)?;
            }
            _ => {
                debug_assert!(false, "Unknown type flag!");
            }
        }
        Ok(())
    }

    /// Reads a float in a variable-length format.  Reads between one and
    /// five bytes. Small integral values typically take fewer bytes.
    fn read_zfloat(input: &mut impl DataInput) -> Result<f32> {
        let b = i32::from(input.read_byte()?) & 0xffi32;
        if b == 0xff {
            // negative value
            Ok(f32::from_bits(input.read_int()? as u32))
        } else if (b & 0x80) != 0 {
            // small integer [-1..125]
            Ok(((b & 0x7f) - 1) as f32)
        } else {
            let bits = b << 24
                | ((i32::from(input.read_short()?) & 0xffff) << 8)
                | (i32::from(input.read_byte()?) & 0xff);
            Ok(f32::from_bits(bits as u32))
        }
    }

    /// Reads a double in a variable-length format.  Reads between one and
    /// nine bytes. Small integral values typically take fewer bytes.
    fn read_zdouble(input: &mut impl DataInput) -> Result<f64> {
        let b = i32::from(input.read_byte()?) & 0xffi32;
        if b == 0xff {
            // negative value
            Ok(f64::from_bits(input.read_long()? as u64))
        } else if (b & 0x80) != 0 {
            // small integer [-1..125]
            Ok(f64::from((b & 0x7f) - 1))
        } else {
            let bits = (i64::from(b)) << 56i64
                | ((i64::from(input.read_int()?) & 0xffff_ffffi64) << 24)
                | ((i64::from(input.read_short()?) & 0xffffi64) << 8)
                | (i64::from(input.read_byte()?) & 0xffi64);
            Ok(f64::from_bits(bits as u64))
        }
    }

    /// Reads a long in a variable-length format.  Reads between one andCorePropLo
    /// nine bytes. Small values typically take fewer bytes.
    fn read_tlong(input: &mut impl DataInput) -> Result<i64> {
        let header = i32::from(input.read_byte()?) & 0xff;

        let mut bits = i64::from(header) & 0x1fi64;
        if (header & 0x20) != 0 {
            // continuation bit
            bits |= input.read_vlong()? << 5i64;
        }

        let mut l = bits.decode();

        match header & DAY_ENCODING {
            SECOND_ENCODING => {
                l *= SECOND;
            }
            HOUR_ENCODING => {
                l *= HOUR;
            }
            DAY_ENCODING => {
                l *= DAY;
            }
            0 => {}
            _ => {
                debug_assert!(false);
            }
        }
        Ok(l)
    }

    fn skip_field(input: &mut impl DataInput, bits: i32) -> Result<()> {
        match bits & TYPE_MASK {
            BYTE_ARR | STRING => {
                let length = input.read_vint()?;
                input.skip_bytes(length as usize)?;
            }
            NUMERIC_INT => {
                input.read_zint()?;
            }
            NUMERIC_FLOAT => {
                Self::read_zfloat(input)?;
            }
            NUMERIC_LONG => {
                Self::read_tlong(input)?;
            }
            NUMERIC_DOUBLE => {
                Self::read_zdouble(input)?;
            }
            _ => {
                debug_assert!(false, "Unknown type flag: {}", bits);
            }
        }
        Ok(())
    }

    fn contains(&self, doc_id: i32) -> bool {
        doc_id >= self.doc_base && doc_id < self.doc_base + self.chunk_docs as i32
    }

    fn reset_state(&mut self, doc_id: i32) {
        if self.do_reset(doc_id).is_err() {
            self.chunk_docs = 0;
        }
    }

    fn do_reset(&mut self, doc_id: i32) -> Result<()> {
        self.doc_base = self.fields_stream.read_vint()?;
        let token = self.fields_stream.read_vint()?;
        debug_assert!(token >= 0);
        self.chunk_docs = token.unsigned_shift(1usize) as usize;
        if !self.contains(doc_id) || self.doc_base + self.chunk_docs as i32 > self.num_docs {
            bail!(CorruptIndex(format!(
                "doc_id={}, doc_base={}, chunk_docs={}, num_docs={}",
                doc_id, self.doc_base, self.chunk_docs, self.num_docs
            )));
        }

        self.sliced = (token & 1) != 0;

        self.offsets.resize((self.chunk_docs + 1) as usize, 0);
        self.num_stored_fields.resize(self.chunk_docs as usize, 0);

        if self.chunk_docs == 1 {
            self.num_stored_fields[0] = self.fields_stream.read_vint()?;
            self.offsets[1] = self.fields_stream.read_vint()?;
        } else {
            // Number of stored fields per document
            let bits_per_stored_fields = self.fields_stream.read_vint()?;
            if bits_per_stored_fields == 0 {
                let value = self.fields_stream.read_vint()?;
                for i in 0..self.chunk_docs {
                    self.num_stored_fields[i] = value;
                }
            } else if bits_per_stored_fields > 31 {
                bail!(CorruptIndex(format!(
                    "bits_per_stored_fields={}",
                    bits_per_stored_fields
                )));
            } else {
                let mut it = get_reader_iterator_no_header(
                    Format::Packed,
                    self.packed_ints_version,
                    self.chunk_docs,
                    bits_per_stored_fields,
                    1,
                )?;

                for i in 0..self.chunk_docs {
                    self.num_stored_fields[i] = it.next(self.fields_stream.as_mut())? as i32;
                }
            }

            // The stream encodes the length of each document and we decode
            // it into a list of monotonically increasing offsets
            let bits_per_length = self.fields_stream.read_vint()?;
            if bits_per_length == 0 {
                let length = self.fields_stream.read_vint()?;
                for i in 0..self.chunk_docs {
                    self.offsets[1 + i] = (1 + i as i32) * length;
                }
            } else {
                let mut it = get_reader_iterator_no_header(
                    Format::Packed,
                    self.packed_ints_version,
                    self.chunk_docs,
                    bits_per_length,
                    1,
                )?;

                for i in 0..self.chunk_docs {
                    self.offsets[i + 1] = it.next(self.fields_stream.as_mut())? as i32;
                }
                for i in 0..self.chunk_docs {
                    self.offsets[i + 1] += self.offsets[i];
                }
            }

            // Additional validation: only the empty document has a serialized length of 0
            for i in 0..self.chunk_docs {
                let len = self.offsets[i + 1] - self.offsets[i];
                let stored_fields = self.num_stored_fields[i];

                if (len == 0) != (stored_fields == 0) {
                    bail!(CorruptIndex(format!(
                        "length={}, num_stored_fields={}",
                        len, stored_fields
                    )));
                }
            }
        }

        self.start_pointer = self.fields_stream.file_pointer();
        if self.merging {
            let total_length = self.offsets[self.chunk_docs];
            // decompress eagerly
            if self.sliced {
                self.bytes_position.0 = 0;
                self.bytes_position.1 = 0;
                let mut decompressed = 0;
                while decompressed < total_length {
                    let to_decompress = min(total_length - decompressed, self.chunk_size);
                    self.decompressor.decompress(
                        self.fields_stream.as_mut(),
                        to_decompress as usize,
                        0,
                        to_decompress as usize,
                        &mut self.spare,
                        &mut self.spare_position,
                    )?;
                    self.bytes
                        .resize(self.bytes_position.1 + self.spare_position.1, 0);
                    self.bytes
                        [self.bytes_position.0..self.bytes_position.0 + self.spare_position.1]
                        .copy_from_slice(
                            &self.spare[self.spare_position.0
                                ..self.spare_position.0 + self.spare_position.1],
                        );
                    self.bytes_position.1 += self.spare_position.1;
                    decompressed += to_decompress;
                }
            } else {
                self.decompressor.decompress(
                    self.fields_stream.as_mut(),
                    total_length as usize,
                    0,
                    total_length as usize,
                    &mut self.bytes,
                    &mut self.bytes_position,
                )?;
            }
            if self.bytes_position.1 != total_length as usize {
                bail!(CorruptIndex(format!(
                    "expected chunk size = {}, got {}",
                    total_length, self.bytes_position.1
                )));
            }
        }

        Ok(())
    }

    pub fn document(&mut self, doc_id: DocId) -> Result<()> {
        if !self.contains(doc_id) {
            self.fields_stream
                .seek(self.index_reader.start_pointer(doc_id)?)?;
            self.reset_state(doc_id);
        }
        debug_assert!(self.contains(doc_id));
        self.do_get_document(doc_id)
    }

    /// Get the serialized representation of the given docID. This docID has
    /// to be contained in the current block.
    fn do_get_document(&mut self, doc_id: DocId) -> Result<()> {
        if !self.contains(doc_id) {
            bail!(IllegalArgument(format!("doc {} don't exist", doc_id)));
        }

        let index = (doc_id - self.doc_base) as usize;
        let offset = self.offsets[index] as usize;
        let length = self.offsets[index + 1] as usize - offset;
        let total_length = self.offsets[self.chunk_docs] as usize;
        let num_stored_fields = self.num_stored_fields[index];

        self.current_doc.num_stored_fields = num_stored_fields;
        self.current_doc.length = length;

        self.current_doc.input = if length == 0 {
            DocumentInput::Bytes(ByteArrayDataInput::new(BytesRef::default()))
        } else if self.merging {
            // already decompressed
            let start = self.bytes_position.0 + offset;
            DocumentInput::Bytes(ByteArrayDataInput::new(BytesRef::new(
                &self.bytes[start..start + length],
            )))
        } else if self.sliced {
            self.fields_stream.seek(self.start_pointer)?;
            self.decompressor.decompress(
                self.fields_stream.as_mut(),
                self.chunk_size as usize,
                offset,
                min(length, self.chunk_size as usize - offset),
                self.bytes.as_mut(),
                &mut self.bytes_position,
            )?;
            self.current_doc.decompressed = self.bytes_position.1;
            DocumentInput::Compressing(CompressingStoredFieldsInput::new(self))
        } else {
            self.fields_stream.seek(self.start_pointer)?;
            self.decompressor.decompress(
                self.fields_stream.as_mut(),
                total_length,
                offset,
                length,
                self.bytes.as_mut(),
                &mut self.bytes_position,
            )?;
            debug_assert_eq!(self.bytes_position.1, length);
            self.current_doc.decompressed = self.bytes_position.1;
            DocumentInput::Compressing(CompressingStoredFieldsInput::new(self))
        };

        Ok(())
    }

    fn fill_buffer(&mut self) -> Result<()> {
        debug_assert!(self.current_doc.decompressed <= self.current_doc.length);
        if self.current_doc.decompressed == self.current_doc.length {
            bail!(UnexpectedEOF("".into()));
        }

        let to_decompress = min(
            self.current_doc.length - self.current_doc.decompressed,
            self.chunk_size as usize,
        );
        self.decompressor.decompress(
            self.fields_stream.as_mut(),
            to_decompress,
            0,
            to_decompress,
            &mut self.bytes,
            &mut self.bytes_position,
        )?;
        self.current_doc.decompressed += to_decompress;
        Ok(())
    }

    pub fn clone(&self) -> Result<Self> {
        Ok(CompressingStoredFieldsReader {
            version: self.version,
            field_infos: self.field_infos.clone(),
            index_reader: self.index_reader.clone(),
            max_pointer: self.max_pointer,
            fields_stream: self.fields_stream.as_ref().clone()?,
            chunk_size: self.chunk_size,
            packed_ints_version: self.packed_ints_version,
            compression_mode: self.compression_mode,
            decompressor: self.decompressor.clone(),
            num_docs: self.num_docs,
            merging: false,
            num_chunks: self.num_chunks,
            num_dirty_chunks: self.num_dirty_chunks,
            doc_base: self.doc_base,
            chunk_docs: self.chunk_docs,
            sliced: self.sliced,
            offsets: self.offsets.clone(),
            num_stored_fields: self.num_stored_fields.clone(),
            start_pointer: self.start_pointer,
            bytes: vec![0u8; self.chunk_size as usize * 2],
            bytes_position: OffsetAndLength(0, 0),
            spare: vec![],
            spare_position: OffsetAndLength(0, 0),
            current_doc: SerializedDocument {
                length: self.current_doc.length,
                num_stored_fields: self.current_doc.num_stored_fields,
                decompressed: self.current_doc.decompressed,
                input: DocumentInput::Bytes(ByteArrayDataInput::new(BytesRef::default())),
            },
        })
    }

    fn do_visit_document<V: StoredFieldVisitor + ?Sized>(
        &mut self,
        doc_id: DocId,
        visitor: &mut V,
    ) -> Result<()> {
        self.document(doc_id)?;

        for field_idx in 0..self.current_doc.num_stored_fields {
            let info_and_bits = self.current_doc.input.read_vlong()?;
            let field_number = info_and_bits.unsigned_shift(TYPE_BITS as usize) as u32;
            let field_info = self.field_infos.by_number[&field_number].clone();
            let bits = (info_and_bits & i64::from(TYPE_MASK)) as i32;
            debug_assert!(bits <= NUMERIC_DOUBLE);

            match visitor.needs_field(field_info.as_ref()) {
                VisitStatus::Yes => {
                    Self::read_field(
                        &mut self.current_doc.input,
                        visitor,
                        field_info.as_ref(),
                        bits,
                    )?;
                }
                VisitStatus::No => {
                    if field_idx == self.current_doc.num_stored_fields - 1 {
                        return Ok(());
                    }
                    Self::skip_field(&mut self.current_doc.input, bits)?;
                }
                VisitStatus::Stop => {
                    return Ok(());
                }
            }
        }
        Ok(())
    }
}

impl StoredFieldsReader for CompressingStoredFieldsReader {
    fn visit_document(&self, doc_id: DocId, visitor: &mut dyn StoredFieldVisitor) -> Result<()> {
        self.clone()?.do_visit_document(doc_id, visitor)
    }

    fn visit_document_mut(
        &mut self,
        doc_id: i32,
        visitor: &mut dyn StoredFieldVisitor,
    ) -> Result<()> {
        self.do_visit_document(doc_id, visitor)
    }

    fn get_merge_instance(&self) -> Result<Self> {
        self.copy_for_merge()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub struct CompressingStoredFieldsInput {
    reader: *mut CompressingStoredFieldsReader,
}

impl CompressingStoredFieldsInput {
    fn new(reader: &mut CompressingStoredFieldsReader) -> Self {
        CompressingStoredFieldsInput { reader }
    }

    #[allow(clippy::mut_from_ref)]
    fn reader(&self) -> &mut CompressingStoredFieldsReader {
        unsafe { &mut *self.reader }
    }
}

impl DataInput for CompressingStoredFieldsInput {
    fn read_byte(&mut self) -> Result<u8> {
        let reader = self.reader();
        if reader.bytes_position.1 == 0 {
            reader.fill_buffer()?;
        }
        let res = reader.bytes[reader.bytes_position.0];
        reader.bytes_position.0 += 1;
        reader.bytes_position.1 -= 1;
        Ok(res)
    }

    fn read_bytes(&mut self, b: &mut [u8], offset: usize, len: usize) -> Result<()> {
        let reader = self.reader();
        let mut len = len;
        let mut offset = offset;
        while len > reader.bytes_position.1 {
            (&mut b[offset..offset + reader.bytes_position.1]).copy_from_slice(
                &reader.bytes
                    [reader.bytes_position.0..reader.bytes_position.0 + reader.bytes_position.1],
            );
            len -= reader.bytes_position.1;
            offset += reader.bytes_position.1;
            reader.fill_buffer()?;
        }
        (&mut b[offset..offset + len])
            .copy_from_slice(&reader.bytes[reader.bytes_position.0..reader.bytes_position.0 + len]);
        reader.bytes_position.0 += len;
        reader.bytes_position.1 -= len;
        Ok(())
    }
}

impl Read for CompressingStoredFieldsInput {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let reader = self.reader();
        let size = min(buf.len(), reader.bytes_position.1);
        buf[0..size].copy_from_slice(
            &reader.bytes[reader.bytes_position.0..reader.bytes_position.0 + size],
        );
        reader.bytes_position.0 += size;
        reader.bytes_position.1 -= size;
        Ok(size)
    }
}

pub enum DocumentInput {
    Compressing(CompressingStoredFieldsInput),
    Bytes(ByteArrayDataInput<BytesRef>),
}

impl Read for DocumentInput {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            DocumentInput::Compressing(c) => c.read(buf),
            DocumentInput::Bytes(b) => b.read(buf),
        }
    }
}

impl DataInput for DocumentInput {
    fn read_byte(&mut self) -> Result<u8> {
        match self {
            DocumentInput::Compressing(c) => c.read_byte(),
            DocumentInput::Bytes(b) => b.read_byte(),
        }
    }

    fn read_bytes(&mut self, b: &mut [u8], offset: usize, len: usize) -> Result<()> {
        match self {
            DocumentInput::Compressing(c) => c.read_bytes(b, offset, len),
            DocumentInput::Bytes(i) => i.read_bytes(b, offset, len),
        }
    }

    fn skip_bytes(&mut self, count: usize) -> Result<()> {
        match self {
            DocumentInput::Compressing(c) => c.skip_bytes(count),
            DocumentInput::Bytes(b) => b.skip_bytes(count),
        }
    }
}

pub struct SerializedDocument {
    pub length: usize,
    pub num_stored_fields: i32,
    pub decompressed: usize,
    pub input: DocumentInput,
    // decompressed bytes in `bytes` in store field reader
}
