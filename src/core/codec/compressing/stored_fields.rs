use error::Result;
use std::boxed::Box;
use std::cmp::min;
use std::io::{self, Read};
use std::sync::Arc;

use core::codec::codec_util::*;
use core::codec::compressing::{CompressionMode, Decompress, Decompressor};
use core::codec::format::StoredFieldsFormat;
use core::codec::reader::StoredFieldsReader;
use core::codec::writer::StoredFieldsWriter;
use core::index::field_info::{FieldInfo, FieldInfos};
use core::index::stored_field_visitor::{Status as VisitStatus, StoredFieldVisitor};
use core::index::{segment_file_name, SegmentInfo};
use core::store::{ChecksumIndexInput, DataInput, IOContext, IndexInput};
// use core::store::byte_array_data_input::ByteArrayDataInput;
use core::store::DirectoryRc;
use core::util::bit_util::{UnsignedShift, ZigZagEncoding};
use core::util::packed_misc::{get_reader_iterator_no_header, get_reader_no_header};
use core::util::packed_misc::{Format, OffsetAndLength, Reader, ReaderIterator};
use core::util::{ComputeTime, DocId};

pub struct CompressingStoredFieldsFormat {
    format_name: String,
    segment_suffix: String,
    compression_mode: CompressionMode,
    #[allow(dead_code)]
    chunk_size: i32,
    #[allow(dead_code)]
    max_docs_per_chunk: i32,
    #[allow(dead_code)]
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
    fn fields_reader(
        &self,
        directory: DirectoryRc,
        si: &SegmentInfo,
        fi: Arc<FieldInfos>,
        context: &IOContext,
    ) -> Result<Box<StoredFieldsReader>> {
        Ok(Box::new(CompressingStoredFieldsReader::new(
            &directory,
            si,
            &self.segment_suffix,
            &fi,
            context,
            &self.format_name,
            self.compression_mode.clone(),
        )?))
    }

    fn fields_writer(
        &self,
        _directory: DirectoryRc,
        _si: &SegmentInfo,
        _ioctx: &IOContext,
    ) -> Result<Box<StoredFieldsWriter>> {
        unimplemented!()
    }
}

pub struct CompressingStoredFieldsIndexReader {
    max_doc: i32,
    doc_bases: Vec<i32>,
    start_pointers: Vec<i64>,
    avg_chunk_docs: Vec<i32>,
    avg_chunk_sizes: Vec<i64>,
    doc_bases_deltas: Vec<Box<Reader>>,
    // delta from the avg
    start_pointers_deltas: Vec<Box<Reader>>,
    // delta from the avg
}

impl CompressingStoredFieldsIndexReader {
    // It is the responsibility of the caller to close fieldsIndexIn after this constructor
    // has been called
    pub fn new<T: IndexInput + ?Sized>(
        fields_index_in: &mut T,
        si: &SegmentInfo,
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
                bail!("Corrupted bitsPerDocBase: {}", bits_per_doc_base);
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
                bail!("Corrupted bitsPerStartPointer: {}", bits_per_start_pointer);
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

    //    pub fn clone(&self) -> CompressingStoredFieldsIndexReader {
    //    }
}

/// Extension of stored fields file
pub const STORED_FIELDS_EXTENSION: &str = "fdt";
/// Extension of stored fields index file
pub const STORED_FIELDS_INDEX_EXTENSION: &str = "fdx";

const CODEC_SFX_IDX: &str = "Index";
const CODEC_SFX_DAT: &str = "Data";

const VERSION_START: i32 = 0;
const VERSION_CHUNK_STATS: i32 = 1;
const VERSION_CURRENT: i32 = VERSION_CHUNK_STATS;

const STRING: i32 = 0x00;
const BYTE_ARR: i32 = 0x01;
const NUMERIC_INT: i32 = 0x02;
const NUMERIC_FLOAT: i32 = 0x03;
const NUMERIC_LONG: i32 = 0x04;
const NUMERIC_DOUBLE: i32 = 0x05;

const TYPE_BITS: i32 = 3; // unsigned_bits_required(NUMERIC_DOUBLE)
const TYPE_MASK: i32 = 7; // 3 bits max value, PackedInts.maxValue(TYPE_BITS)

// -0 isn't compressed
// const NEGATIVE_ZERO_FLOAT: i32 = -2147483648; // f32::to_bits(-0f32) as i32;
// const NEGATIVE_ZERO_DOUBLE: i64 = -9223372036854775808; // f64::to_bits(-0f64) as i64;
// for compression of timestamps
const SECOND: i64 = 1000;
const HOUR: i64 = 60 * 60 * SECOND;
const DAY: i64 = HOUR * 24;
const SECOND_ENCODING: i32 = 0x40;
const HOUR_ENCODING: i32 = 0x80;
const DAY_ENCODING: i32 = 0xC0;

pub struct CompressingStoredFieldsReader {
    pub version: i32,
    field_infos: Arc<FieldInfos>,
    pub index_reader: Arc<CompressingStoredFieldsIndexReader>,
    pub max_pointer: i64,
    pub fields_stream: Box<IndexInput>,
    pub chunk_size: i32,
    pub packed_ints_version: i32,
    pub compression_mode: CompressionMode,
    decompressor: Decompressor,
    num_docs: i32,
    merging: bool,
    pub num_chunks: i64,
    // number of compressed blocks written
    pub num_dirty_chunks: i64,
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
    // from document data input
    current_doc: SerializedDocument,
    // current decompressed byte length for filed `bytes`
}

impl CompressingStoredFieldsReader {
    pub fn new(
        dir: &DirectoryRc,
        si: &SegmentInfo,
        segment_suffix: &str,
        field_infos: &Arc<FieldInfos>,
        context: &IOContext,
        format_name: &str,
        compression_mode: CompressionMode,
    ) -> Result<CompressingStoredFieldsReader> {
        // load the index into memory
        let index_name = segment_file_name(&si.name, segment_suffix, STORED_FIELDS_INDEX_EXTENSION);
        let mut index_stream: Box<ChecksumIndexInput> =
            dir.open_checksum_input(&index_name, context)?;
        let codec_name_idx = String::from(format_name) + CODEC_SFX_IDX;
        let version = check_index_header(
            index_stream.as_mut(),
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
        let index_reader = CompressingStoredFieldsIndexReader::new(index_stream.as_mut(), si)?;
        let max_pointer = index_stream.as_mut().read_vlong()?;
        check_footer(index_stream.as_mut())?;

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
            bail!(
                "CorruptIndexException: Version mismatch between stored fields index and data: {} \
                 != {}",
                version,
                fields_version
            );
        }
        debug_assert_eq!(
            index_header_length(&codec_name_dat, segment_suffix),
            fields_stream.as_mut().file_pointer() as usize
        );

        let chunk_size = fields_stream.as_mut().read_vint()?;
        let packed_ints_version = fields_stream.as_mut().read_vint()?;

        let mut num_chunks = -1i64;
        let mut num_dirty_chunks = -1i64;
        if version >= VERSION_CHUNK_STATS {
            fields_stream.as_mut().seek(max_pointer)?;
            num_chunks = fields_stream.as_mut().read_vlong()?;
            num_dirty_chunks = fields_stream.as_mut().read_vlong()?;
            if num_dirty_chunks > num_chunks {
                bail!(
                    "CorruptIndexException: invalid chunk counts: dirty={}, total={}",
                    num_dirty_chunks,
                    num_chunks
                );
            }
        }

        retrieve_checksum(fields_stream.as_mut())?;

        let decompressor = compression_mode.new_decompressor();
        Ok(CompressingStoredFieldsReader {
            version,
            field_infos: field_infos.clone(),
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
            current_doc: SerializedDocument {
                length: 0,
                num_stored_fields: 0,
                decompressed: 0,
            },
        })
    }

    fn read_field(
        &mut self,
        visitor: &mut StoredFieldVisitor,
        info: &FieldInfo,
        bits: i32,
    ) -> Result<()> {
        match bits & TYPE_MASK {
            BYTE_ARR => {
                let length = self.read_vint()? as usize;
                let mut data = vec![0u8; length];
                self.read_bytes(data.as_mut(), 0, length)?;
                visitor.binary_field(info, data.as_ref());
            }
            STRING => {
                let length = self.read_vint()? as usize;
                let mut data = vec![0u8; length];
                self.read_bytes(data.as_mut(), 0, length)?;
                visitor.string_field(info, data.as_ref());
            }
            NUMERIC_INT => {
                visitor.int_field(info, self.read_zint()?);
            }
            NUMERIC_FLOAT => {
                visitor.float_field(info, self.read_zfloat()?);
            }
            NUMERIC_LONG => {
                visitor.long_field(info, self.read_tlong()?);
            }
            NUMERIC_DOUBLE => {
                visitor.double_field(info, self.read_zdouble()?);
            }
            _ => {
                debug_assert!(false, "Unknown type flag!");
            }
        }
        Ok(())
    }

    /// Reads a float in a variable-length format.  Reads between one and
    /// five bytes. Small integral values typically take fewer bytes.
    ///
    fn read_zfloat(&mut self) -> Result<f32> {
        let b = i32::from(self.read_byte()?) & 0xffi32;
        if b == 0xff {
            // negative value
            Ok(f32::from_bits(self.read_int()? as u32))
        } else if (b & 0x80) != 0 {
            // small integer [-1..125]
            Ok(((b & 0x7f) - 1) as f32)
        } else {
            let bits = b << 24 | ((i32::from(self.read_short()?) & 0xffff) << 8)
                | (i32::from(self.read_byte()?) & 0xff);
            Ok(f32::from_bits(bits as u32))
        }
    }

    /// Reads a double in a variable-length format.  Reads between one and
    /// nine bytes. Small integral values typically take fewer bytes.
    ///
    fn read_zdouble(&mut self) -> Result<f64> {
        let b = i32::from(self.read_byte()?) & 0xffi32;
        if b == 0xff {
            // negative value
            Ok(f64::from_bits(self.read_long()? as u64))
        } else if (b & 0x80) != 0 {
            // small integer [-1..125]
            Ok(f64::from((b & 0x7f) - 1))
        } else {
            let bits = (i64::from(b)) << 56i64
                | ((i64::from(self.read_int()?) & 0xffff_ffffi64) << 24)
                | ((i64::from(self.read_short()?) & 0xffffi64) << 8)
                | (i64::from(self.read_byte()?) & 0xffi64);
            Ok(f64::from_bits(bits as u64))
        }
    }

    /// Reads a long in a variable-length format.  Reads between one andCorePropLo
    /// nine bytes. Small values typically take fewer bytes.
    ///
    fn read_tlong(&mut self) -> Result<i64> {
        let header = i32::from(self.read_byte()?) & 0xff;

        let mut bits = i64::from(header) & 0x1fi64;
        if (header & 0x20) != 0 {
            // continuation bit
            bits |= self.read_vlong()? << 5i64;
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

    fn skip_field(&mut self, bits: i32) -> Result<()> {
        match bits & TYPE_MASK {
            BYTE_ARR | STRING => {
                let length = self.read_vint()?;
                self.skip_bytes(length as usize)?;
            }
            NUMERIC_INT => {
                self.read_zint()?;
            }
            NUMERIC_FLOAT => {
                self.read_zfloat()?;
            }
            NUMERIC_LONG => {
                self.read_tlong()?;
            }
            NUMERIC_DOUBLE => {
                self.read_zdouble()?;
            }
            _ => {
                debug_assert!(false, format!("Unknown type flag: {}", bits));
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
        self.doc_base = self.fields_stream.as_mut().read_vint()?;
        let token = self.fields_stream.as_mut().read_vint()?;
        debug_assert!(token >= 0);
        self.chunk_docs = token.unsigned_shift(1usize) as usize;
        if !self.contains(doc_id) || self.doc_base + self.chunk_docs as i32 > self.num_docs {
            bail!(
                "Corrupted: docID={}, docBase={}, chunkDocs={}, numDocs={}",
                doc_id,
                self.doc_base,
                self.chunk_docs,
                self.num_docs
            );
        }

        self.sliced = (token & 1) != 0;

        self.offsets.resize((self.chunk_docs + 1) as usize, 0);
        self.num_stored_fields.resize(self.chunk_docs as usize, 0);

        if self.chunk_docs == 1 {
            self.num_stored_fields[0] = self.fields_stream.as_mut().read_vint()?;
            self.offsets[1] = self.fields_stream.as_mut().read_vint()?;
        } else {
            // Number of stored fields per document
            let bits_per_stored_fields = self.fields_stream.as_mut().read_vint()?;
            if bits_per_stored_fields == 0 {
                let value = self.fields_stream.as_mut().read_vint()?;
                for i in 0..self.chunk_docs {
                    self.num_stored_fields[i] = value;
                }
            } else if bits_per_stored_fields > 31 {
                bail!("bitsPerStoredFields={}", bits_per_stored_fields);
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
            let bits_per_length = self.fields_stream.as_mut().read_vint()?;
            if bits_per_length == 0 {
                let length = self.fields_stream.as_mut().read_vint()?;
                for i in 0..self.chunk_docs {
                    self.offsets[1 + i] = (1 + i as i32) * length;
                }
            } else if bits_per_length > 31 {
                bail!("bitsPerLength={}", bits_per_length);
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
                    bail!("length={}, numStoredFields={}", len, stored_fields);
                }
            }
        }

        self.start_pointer = self.fields_stream.as_ref().file_pointer();
        if self.merging {
            unimplemented!()
        }

        Ok(())
    }

    fn document(&mut self, doc_id: DocId) -> Result<()> {
        if !self.contains(doc_id) {
            self.fields_stream
                .as_mut()
                .seek(self.index_reader.start_pointer(doc_id)?)?;
            self.reset_state(doc_id);
        }
        debug_assert!(self.contains(doc_id));
        self.do_get_document(doc_id)
    }

    /// Get the serialized representation of the given docID. This docID has
    /// to be contained in the current block.
    ///
    fn do_get_document(&mut self, doc_id: DocId) -> Result<()> {
        if !self.contains(doc_id) {
            bail!("document failed. docid={}", doc_id)
        }

        let index = (doc_id - self.doc_base) as usize;
        let offset = self.offsets[index] as usize;
        let length = self.offsets[index + 1] as usize - offset;
        let total_length = self.offsets[self.chunk_docs] as usize;
        let num_stored_fields = self.num_stored_fields[index];

        self.current_doc.num_stored_fields = num_stored_fields;
        self.current_doc.length = length;

        if length == 0 {
            // do nothing
        } else if self.merging {
            unimplemented!()
        } else if self.sliced {
            self.fields_stream.as_mut().seek(self.start_pointer)?;
            self.decompressor.decompress(
                self.fields_stream.as_mut(),
                self.chunk_size as usize,
                offset,
                min(length, self.chunk_size as usize - offset),
                self.bytes.as_mut(),
                &mut self.bytes_position,
            )?;
            self.current_doc.decompressed = self.bytes_position.1;
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
        }

        Ok(())
    }

    fn fill_buffer(&mut self) -> Result<()> {
        debug_assert!(self.current_doc.decompressed <= self.current_doc.length);
        if self.current_doc.decompressed == self.current_doc.length {
            bail!("End of file!");
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

    fn clone(&self) -> Result<Self> {
        Ok(CompressingStoredFieldsReader {
            version: self.version,
            field_infos: self.field_infos.clone(),
            index_reader: self.index_reader.clone(),
            max_pointer: self.max_pointer,
            fields_stream: self.fields_stream.as_ref().clone()?,
            chunk_size: self.chunk_size,
            packed_ints_version: self.packed_ints_version,
            compression_mode: self.compression_mode.clone(),
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
            bytes: self.bytes.clone(),
            bytes_position: self.bytes_position.clone(),
            current_doc: SerializedDocument {
                length: self.current_doc.length,
                num_stored_fields: self.current_doc.num_stored_fields,
                decompressed: self.current_doc.decompressed,
            },
        })
    }

    fn visit_document_mut(
        &mut self,
        doc_id: DocId,
        visitor: &mut StoredFieldVisitor,
    ) -> Result<()> {
        self.document(doc_id)?;

        for field_idx in 0..self.current_doc.num_stored_fields {
            let info_and_bits = self.read_vlong()?;
            let field_number = info_and_bits.unsigned_shift(TYPE_BITS as usize) as i32;
            let field_info = self.field_infos.by_number[&field_number].clone();
            let bits = (info_and_bits & i64::from(TYPE_MASK)) as i32;
            debug_assert!(bits <= NUMERIC_DOUBLE);

            match visitor.needs_field(field_info.as_ref()) {
                VisitStatus::YES => {
                    self.read_field(visitor, field_info.as_ref(), bits)?;
                }
                VisitStatus::NO => {
                    if field_idx == self.current_doc.num_stored_fields - 1 {
                        return Ok(());
                    }
                    self.skip_field(bits)?;
                }
                VisitStatus::STOP => {
                    return Ok(());
                }
            }
        }
        Ok(())
    }
}

impl StoredFieldsReader for CompressingStoredFieldsReader {
    fn visit_document(&self, doc_id: DocId, visitor: &mut StoredFieldVisitor) -> Result<()> {
        self.clone()?.visit_document_mut(doc_id, visitor)
    }
}

impl DataInput for CompressingStoredFieldsReader {
    fn read_byte(&mut self) -> Result<u8> {
        if self.bytes_position.1 == 0 {
            self.fill_buffer()?;
        }
        let res = self.bytes[self.bytes_position.0];
        self.bytes_position.0 += 1;
        self.bytes_position.1 -= 1;
        Ok(res)
    }

    fn read_bytes(&mut self, b: &mut [u8], offset: usize, len: usize) -> Result<()> {
        let mut len = len;
        let mut offset = offset;
        while len > self.bytes_position.1 {
            (&mut b[offset..offset + self.bytes_position.1]).copy_from_slice(
                &self.bytes[self.bytes_position.0..self.bytes_position.0 + self.bytes_position.1],
            );
            len -= self.bytes_position.1;
            offset += self.bytes_position.1;
            self.fill_buffer()?;
        }
        (&mut b[offset..offset + len])
            .copy_from_slice(&self.bytes[self.bytes_position.0..self.bytes_position.0 + len]);
        self.bytes_position.0 += len;
        self.bytes_position.1 -= len;
        Ok(())
    }
}

impl Read for CompressingStoredFieldsReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let size = min(buf.len(), self.bytes_position.1);
        buf[0..size].copy_from_slice(&self.bytes[self.bytes_position.0..size]);
        self.bytes_position.0 += size;
        self.bytes_position.1 -= size;
        Ok(size)
    }
}

struct SerializedDocument {
    pub length: usize,
    pub num_stored_fields: i32,
    pub decompressed: usize,
    // decompressed bytes in `bytes` in store field reader
}
