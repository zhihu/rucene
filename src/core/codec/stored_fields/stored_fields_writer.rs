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

use core::codec::codec_util::*;
use core::codec::field_infos::{FieldInfo, FieldInfos};
use core::codec::segment_infos::{segment_file_name, SegmentInfo};
use core::codec::stored_fields::stored_fields_reader::*;
use core::codec::stored_fields::{
    merge_store_fields, MergeVisitor, StoredFieldsReader, StoredFieldsWriter,
};
use core::codec::Codec;
use core::codec::MatchingReaders;
use core::doc::Fieldable;
use core::index::merge::{
    doc_id_merger_of, DocIdMerger, DocIdMergerSub, DocIdMergerSubBase, LiveDocsDocMap, MergeState,
};
use core::search::NO_MORE_DOCS;
use core::store::directory::Directory;
use core::store::io::{DataOutput, GrowableByteArrayDataOutput, IndexOutput};
use core::store::IOContext;
use core::util::packed::Format;
use core::util::packed::VERSION_CURRENT as PACKED_VERSION_CURRENT;
use core::util::packed::{get_writer_no_header, Writer};
use core::util::DocId;
use core::util::Numeric;
use core::util::{BitsRequired, UnsignedShift, ZigZagEncoding};
use core::util::{Compress, CompressionMode, Compressor};

use error::{
    ErrorKind::{IllegalArgument, IllegalState, RuntimeError},
    Result,
};

use std::sync::Arc;

pub struct CompressingStoredFieldsIndexWriter<O: IndexOutput> {
    fields_index_output: O,
    block_size: usize,
    total_docs: usize,
    block_docs: usize,
    block_chunks: usize,
    first_start_pointer: i64,
    max_start_pointer: i64,
    doc_base_deltas: Vec<i32>,
    start_pointer_deltas: Vec<i64>,
}

impl<O: IndexOutput> CompressingStoredFieldsIndexWriter<O> {
    pub fn new(output: O, block_size: usize) -> Result<CompressingStoredFieldsIndexWriter<O>> {
        if block_size == 0 {
            bail!(IllegalArgument("block size must be 0".into()));
        }

        let mut writer = CompressingStoredFieldsIndexWriter {
            fields_index_output: output,
            block_size,
            total_docs: 0,
            block_docs: 0,
            block_chunks: 0,
            first_start_pointer: -1,
            max_start_pointer: -1,
            doc_base_deltas: vec![0i32; block_size],
            start_pointer_deltas: vec![0i64; block_size],
        };

        writer
            .fields_index_output
            .write_vint(PACKED_VERSION_CURRENT as i32)?;

        Ok(writer)
    }

    pub fn reset(&mut self) {
        self.block_chunks = 0;
        self.block_docs = 0;
        self.first_start_pointer = -1; // means unset
    }

    pub fn write_block(&mut self) -> Result<()> {
        debug_assert!(self.block_chunks > 0);
        self.fields_index_output
            .write_vint(self.block_chunks as i32)?;

        // The trick here is that we only store the difference from the average start
        // pointer or doc base, this helps save bits per value.
        // And in order to prevent a few chunks that would be far from the average to
        // raise the number of bits per value for all of them, we only encode blocks
        // of 1024 chunks at once
        // See LUCENE-4512

        // doc bases
        let avg_chunk_docs = if self.block_chunks == 1 {
            0
        } else {
            ((self.block_docs as i32 - self.doc_base_deltas[self.block_chunks - 1]) as f32
                / (self.block_chunks - 1) as f32)
                .round() as i32
        };
        self.fields_index_output
            .write_vint((self.total_docs - self.block_docs) as i32)?;
        self.fields_index_output.write_vint(avg_chunk_docs)?;

        let mut doc_base = 0;
        let mut max_delta: i64 = 0;
        for i in 0..self.block_chunks {
            let delta = doc_base - avg_chunk_docs * (i as i32);
            max_delta |= delta.encode() as i64;
            doc_base += self.doc_base_deltas[i];
        }

        let bits_per_doc_base = max_delta.bits_required() as i32;
        self.fields_index_output.write_vint(bits_per_doc_base)?;
        let mut writer =
            get_writer_no_header(Format::Packed, self.block_chunks, bits_per_doc_base, 1);
        doc_base = 0;
        for i in 0..self.block_chunks {
            let delta = doc_base - avg_chunk_docs * (i as i32);
            debug_assert!(delta.encode().bits_required() as i32 <= writer.bits_per_value());
            writer.add(delta.encode() as i64, &mut self.fields_index_output)?;
            doc_base += self.doc_base_deltas[i];
        }
        writer.finish(&mut self.fields_index_output)?;

        // start pointers
        self.fields_index_output
            .write_vlong(self.first_start_pointer)?;
        let avg_chunk_size = if self.block_chunks == 1 {
            0
        } else {
            (self.max_start_pointer - self.first_start_pointer) / (self.block_chunks as i64 - 1)
        };
        self.fields_index_output.write_vlong(avg_chunk_size)?;

        let mut start_pointer = 0i64;
        max_delta = 0;
        for i in 0..self.block_chunks {
            start_pointer += self.start_pointer_deltas[i];
            let delta = start_pointer - avg_chunk_size * (i as i64);
            max_delta |= delta.encode();
        }

        let bits_per_start_pointer = max_delta.bits_required() as i32;
        self.fields_index_output
            .write_vint(bits_per_start_pointer)?;
        writer = get_writer_no_header(Format::Packed, self.block_chunks, bits_per_start_pointer, 1);
        start_pointer = 0;
        for i in 0..self.block_chunks {
            start_pointer += self.start_pointer_deltas[i];
            let delta = start_pointer - avg_chunk_size * (i as i64);
            debug_assert!(delta.encode().bits_required() as i32 <= writer.bits_per_value());
            writer.add(delta.encode(), &mut self.fields_index_output)?;
        }
        writer.finish(&mut self.fields_index_output)
    }

    pub fn write_index(&mut self, num_docs: usize, start_pointer: i64) -> Result<()> {
        if self.block_chunks == self.block_size {
            self.write_block()?;
            self.reset();
        }

        if self.first_start_pointer == -1 {
            self.first_start_pointer = start_pointer;
            self.max_start_pointer = start_pointer;
        }

        debug_assert!(self.first_start_pointer > 0 && start_pointer >= self.first_start_pointer);

        self.doc_base_deltas[self.block_chunks] = num_docs as i32;
        self.start_pointer_deltas[self.block_chunks] = start_pointer - self.max_start_pointer;
        self.block_chunks += 1;

        self.block_docs += num_docs;
        self.total_docs += num_docs;
        self.max_start_pointer = start_pointer;

        Ok(())
    }

    pub fn finish(&mut self, num_docs: usize, max_pointer: i64) -> Result<()> {
        if num_docs != self.total_docs {
            bail!(IllegalState(format!(
                "Expected {} docs, but got {}",
                num_docs, self.total_docs
            )));
        }

        if self.block_chunks > 0 {
            self.write_block()?;
        }

        self.fields_index_output.write_vint(0)?; // end marker
        self.fields_index_output.write_vlong(max_pointer)?;
        write_footer(&mut self.fields_index_output)
    }
}

/// `StoredFieldsWriter` impl for `CompressingStoredFieldsFormat`
pub struct CompressingStoredFieldsWriter<O: IndexOutput> {
    index_writer: CompressingStoredFieldsIndexWriter<O>,
    fields_stream: O,
    compress_mode: CompressionMode,
    compressor: Compressor,
    chunk_size: usize,
    max_docs_per_chunk: usize,
    buffered_docs: GrowableByteArrayDataOutput,
    // num of stored fields
    num_stored_fields: Vec<i32>,
    // end offsets in bufferedDocs
    end_offsets: Vec<i32>,
    // doc ID at the beginning of the chunk
    doc_base: DocId,
    // docBase + numBufferedDocs == current doc ID
    num_buffered_docs: usize,
    // number of compressed blocks written
    num_chunks: usize,
    // number of incomplete compressed blocks written
    num_dirty_chunks: usize,
    num_stored_fields_in_doc: usize,
}

impl<O: IndexOutput + 'static> CompressingStoredFieldsWriter<O> {
    #[allow(clippy::too_many_arguments)]
    pub fn new<D: Directory, DW: Directory<IndexOutput = O>, C: Codec>(
        directory: Arc<DW>,
        si: &SegmentInfo<D, C>,
        segment_suffix: &str,
        context: &IOContext,
        format_name: &str,
        compress_mode: CompressionMode,
        chunk_size: usize,
        max_docs_per_chunk: usize,
        block_size: usize,
    ) -> Result<CompressingStoredFieldsWriter<DW::IndexOutput>> {
        let mut index_stream = directory.create_output(
            &segment_file_name(&si.name, segment_suffix, STORED_FIELDS_INDEX_EXTENSION),
            context,
        )?;
        let mut fields_stream = directory.create_output(
            &segment_file_name(&si.name, segment_suffix, STORED_FIELDS_EXTENSION),
            context,
        )?;
        let codec_name_idx = format!("{}{}", format_name, CODEC_SFX_IDX);
        let codec_name_dat = format!("{}{}", format_name, CODEC_SFX_DAT);

        write_index_header(
            &mut index_stream,
            &codec_name_idx,
            VERSION_CURRENT,
            si.get_id(),
            segment_suffix,
        )?;
        write_index_header(
            &mut fields_stream,
            &codec_name_dat,
            VERSION_CURRENT,
            si.get_id(),
            segment_suffix,
        )?;
        debug_assert!(
            index_header_length(&codec_name_idx, segment_suffix)
                == index_stream.file_pointer() as usize
        );
        debug_assert!(
            index_header_length(&codec_name_dat, segment_suffix)
                == fields_stream.file_pointer() as usize
        );

        let index_writer = CompressingStoredFieldsIndexWriter::new(index_stream, block_size)?;
        fields_stream.write_vint(chunk_size as i32)?;
        fields_stream.write_vint(PACKED_VERSION_CURRENT as i32)?;

        Ok(CompressingStoredFieldsWriter {
            index_writer,
            fields_stream,
            compress_mode,
            compressor: compress_mode.new_compressor(),
            chunk_size,
            max_docs_per_chunk,
            buffered_docs: GrowableByteArrayDataOutput::new(chunk_size),
            // num of stored fields
            num_stored_fields: vec![0i32; 16],
            // end offsets in bufferedDocs
            end_offsets: vec![0i32; 16],
            // doc ID at the beginning of the chunk
            doc_base: 0,
            // docBase + numBufferedDocs == current doc ID
            num_buffered_docs: 0,
            // number of compressed blocks written
            num_chunks: 0,
            // number of incomplete compressed blocks written
            num_dirty_chunks: 0,
            num_stored_fields_in_doc: 0,
        })
    }

    fn write_header(&mut self, sliced: bool) -> Result<()> {
        let sliced_bit = if sliced { 1 } else { 0 };
        // save docBase and numBufferedDocs
        self.fields_stream.write_vint(self.doc_base)?;
        self.fields_stream
            .write_vint((self.num_buffered_docs as i32) << 1 | sliced_bit)?;

        // save numStoredFields
        CompressingStoredFieldsWriter::<O>::save_ints(
            &self.num_stored_fields,
            self.num_buffered_docs,
            &mut self.fields_stream,
        )?;

        // save lengths
        CompressingStoredFieldsWriter::<O>::save_ints(
            &self.end_offsets,
            self.num_buffered_docs,
            &mut self.fields_stream,
        )
    }

    fn trigger_flush(&self) -> bool {
        // chunks of at least chunkSize bytes
        self.buffered_docs.position() >= self.chunk_size
            || self.num_buffered_docs >= self.max_docs_per_chunk
    }

    fn flush(&mut self) -> Result<()> {
        self.index_writer
            .write_index(self.num_buffered_docs, self.fields_stream.file_pointer())?;

        // transform end offsets into lengths
        let mut i = self.num_buffered_docs - 1;
        while i > 0 {
            let length = self.end_offsets[i] - self.end_offsets[i - 1];
            debug_assert!(length >= 0);
            self.end_offsets[i] = length;
            i -= 1;
        }

        let sliced = self.buffered_docs.position() >= 2 * self.chunk_size;
        self.write_header(sliced)?;

        // compress stored fields to fieldsStream
        if sliced {
            // big chunk, slice it
            let mut compressed = 0usize;
            while compressed < self.buffered_docs.position() {
                self.compressor.compress(
                    &self.buffered_docs.bytes,
                    compressed,
                    self.chunk_size
                        .min(self.buffered_docs.position() - compressed),
                    &mut self.fields_stream,
                )?;
                compressed += self.chunk_size;
            }
        } else {
            self.compressor.compress(
                &self.buffered_docs.bytes,
                0,
                self.buffered_docs.position(),
                &mut self.fields_stream,
            )?;
        }

        // reset
        self.doc_base += self.num_buffered_docs as DocId;
        self.num_buffered_docs = 0;
        self.buffered_docs.reset();
        self.num_chunks += 1;

        Ok(())
    }

    fn save_ints(values: &[i32], length: usize, out: &mut impl DataOutput) -> Result<()> {
        debug_assert!(length > 0);
        if length == 1 {
            out.write_vint(values[0])?;
        } else {
            let mut all_equal = true;
            for i in 1..length {
                if values[i] != values[0] {
                    all_equal = false;
                    break;
                }
            }

            if all_equal {
                out.write_vint(0)?;
                out.write_vint(values[0])?;
            } else {
                let mut max = 0;
                for i in 0..length {
                    max |= values[i];
                }

                let bits_required = max.bits_required() as i32;
                out.write_vint(bits_required)?;
                let mut writer = get_writer_no_header(Format::Packed, length, bits_required, 1);
                for i in 0..length {
                    writer.add(values[i] as i64, out)?;
                }
                writer.finish(out)?;
            }
        }

        Ok(())
    }

    /// Writes a float in a variable-length format.  Writes between one and
    /// five bytes. Small integral values typically take fewer bytes.
    /// <p>
    /// ZFloat --&gt; Header, Bytes*?
    /// <ul>
    /// <li>Header --&gt; {@link DataOutput#writeByte Uint8}. When it is
    /// equal to 0xFF then the value is negative and stored in the next
    /// 4 bytes. Otherwise if the first bit is set then the other bits
    /// in the header encode the value plus one and no other
    /// bytes are read. Otherwise, the value is a positive float value
    /// whose first byte is the header, and 3 bytes need to be read to
    /// complete it.
    /// <li>Bytes --&gt; Potential additional bytes to read depending on the
    /// header.
    /// </ul>
    fn write_zfloat(out: &mut impl DataOutput, f: f32) -> Result<()> {
        let int_val = f as i32;
        let float_bits = f.to_bits() as i32;
        if (f - int_val as f32).abs() < ::std::f32::EPSILON
            && int_val >= -1
            && int_val <= 0x7D
            && float_bits != NEGATIVE_ZERO_FLOAT
        {
            // small integer value [-1..125]: single byte
            out.write_byte((0x80 | (1 + int_val)) as u8)?;
        } else if float_bits.unsigned_shift(31) == 0 {
            // other positive floats: 4 bytes
            out.write_int(float_bits)?;
        } else {
            // other negative float: 5 bytes
            out.write_byte(0xFF as u8)?;
            out.write_vint(float_bits)?;
        }

        Ok(())
    }

    /// Writes a float in a variable-length format.  Writes between one and
    /// five bytes. Small integral values typically take fewer bytes.
    /// <p>
    /// ZFloat --&gt; Header, Bytes*?
    /// <ul>
    /// <li>Header --&gt; {@link DataOutput#writeByte Uint8}. When it is
    /// equal to 0xFF then the value is negative and stored in the next
    /// 8 bytes. When it is equal to 0xFE then the value is stored as a
    /// float in the next 4 bytes. Otherwise if the first bit is set
    /// then the other bits in the header encode the value plus one and
    /// no other bytes are read. Otherwise, the value is a positive float
    /// value whose first byte is the header, and 7 bytes need to be read
    /// to complete it.
    /// <li>Bytes --&gt; Potential additional bytes to read depending on the
    /// header.
    /// </ul>
    fn write_zdouble(out: &mut impl DataOutput, d: f64) -> Result<()> {
        let int_val = d as i32;
        let double_bits = d.to_bits() as i64;

        if (d - int_val as f64).abs() < ::std::f64::EPSILON
            && int_val >= -1
            && int_val <= 0x7C
            && double_bits != NEGATIVE_ZERO_DOUBLE
        {
            // small integer value [-1..124]: single byte
            out.write_byte((0x80 | (int_val + 1)) as u8)?;
        } else if (d - (d as f32) as f64).abs() < ::std::f64::EPSILON {
            // d has an accurate float representation: 5 bytes
            out.write_byte(0xFE as u8)?;
            out.write_int((d as f32).to_bits() as i32)?;
        } else if double_bits.unsigned_shift(63) == 0 {
            // other positive doubles: 8 bytes
            out.write_long(double_bits)?;
        } else {
            // other negative doubles: 9 bytes
            out.write_byte(0xFF as u8)?;
            out.write_long(double_bits)?;
        }

        Ok(())
    }

    /// Writes a long in a variable-length format.  Writes between one and
    /// ten bytes. Small values or values representing timestamps with day,
    /// hour or second precision typically require fewer bytes.
    ///
    /// <p>
    /// ZLong --> Header, Bytes*?
    /// <ul>
    /// <li>Header --&gt; The first two bits indicate the compression scheme:
    /// <ul>
    /// <li>00 - uncompressed
    /// <li>01 - multiple of 1000 (second)
    /// <li>10 - multiple of 3600000 (hour)
    /// <li>11 - multiple of 86400000 (day)
    /// </ul>
    /// Then the next bit is a continuation bit, indicating whether more
    /// bytes need to be read, and the last 5 bits are the lower bits of
    /// the encoded value. In order to reconstruct the value, you need to
    /// combine the 5 lower bits of the header with a vLong in the next
    /// bytes (if the continuation bit is set to 1). Then
    /// {@link BitUtil#zigZagDecode(int) zigzag-decode} it and finally
    /// multiply by the multiple corresponding to the compression scheme.
    /// <li>Bytes --&gt; Potential additional bytes to read depending on the
    /// header.
    /// </ul>
    // T for "timestamp"
    fn write_tlong(out: &mut impl DataOutput, l: i64) -> Result<()> {
        let mut l = l;
        let mut header: i64 = if l % SECOND != 0 {
            0
        } else if l % DAY == 0 {
            // timestamp with day precision
            l /= DAY;
            DAY_ENCODING as i64
        } else if l % HOUR == 0 {
            // timestamp with hour precision, or day precision with a timezone
            l /= HOUR;
            HOUR_ENCODING as i64
        } else {
            // timestamp with second precision
            l /= SECOND;
            SECOND_ENCODING as i64
        };

        let zig_zag_l = l.encode();
        // last 5 bits
        header |= zig_zag_l & 0x1F;
        let upper_bits = zig_zag_l.unsigned_shift(5);
        if upper_bits != 0 {
            header |= 0x20;
        }

        out.write_byte(header as u8)?;
        if upper_bits != 0 {
            out.write_vlong(upper_bits)?;
        }

        Ok(())
    }

    /// Returns true if we should recompress this reader, even though we
    /// could bulk merge compressed data
    ///
    /// The last chunk written for a segment is typically incomplete, so without
    /// recompressing, in some worst-case situations (e.g. frequent reopen with tiny
    /// flushes), over time the compression ratio can degrade. This is a safety switch.
    fn too_dirty(candidate: &CompressingStoredFieldsReader) -> bool {
        // more than 1% dirty, or more than hard limit of 1024 dirty chunks
        candidate.num_dirty_chunks() > 1024
            || candidate.num_dirty_chunks() * 100 > candidate.num_chunks()
    }
}

impl<O: IndexOutput + 'static> StoredFieldsWriter for CompressingStoredFieldsWriter<O> {
    fn start_document(&mut self) -> Result<()> {
        Ok(())
    }

    fn finish_document(&mut self) -> Result<()> {
        if self.num_buffered_docs == self.num_stored_fields.len() {
            self.num_stored_fields
                .resize(self.num_buffered_docs + 1, 0i32);
            self.end_offsets.resize(self.num_buffered_docs + 1, 0i32);
        }

        self.num_stored_fields[self.num_buffered_docs] = self.num_stored_fields_in_doc as i32;
        self.num_stored_fields_in_doc = 0;
        self.end_offsets[self.num_buffered_docs] = self.buffered_docs.position() as i32;
        self.num_buffered_docs += 1;
        if self.trigger_flush() {
            self.flush()?;
        }

        Ok(())
    }

    fn write_field(&mut self, field_info: &FieldInfo, field: &impl Fieldable) -> Result<()> {
        self.num_stored_fields_in_doc += 1;
        let bits = if let Some(v) = field.numeric_value() {
            match v {
                Numeric::Byte(_) | Numeric::Short(_) | Numeric::Int(_) => NUMERIC_INT,
                Numeric::Long(_) => NUMERIC_LONG,
                Numeric::Float(_) => NUMERIC_FLOAT,
                Numeric::Double(_) => NUMERIC_DOUBLE,
                _ => unreachable!(),
            }
        } else if field.binary_value().is_some() {
            BYTE_ARR
        } else {
            STRING
        };

        let info_and_bits = ((field_info.number as i64) << TYPE_BITS) | bits as i64;
        self.buffered_docs.write_vlong(info_and_bits)?;
        if let Some(bytes) = field.binary_value() {
            self.buffered_docs.write_vint(bytes.len() as i32)?;
            self.buffered_docs.write_bytes(bytes, 0, bytes.len())?;
        } else if let Some(string) = field.string_value() {
            self.buffered_docs.write_string(string)?;
        } else {
            match field.numeric_value().unwrap() {
                Numeric::Byte(v) => self.buffered_docs.write_zint(v as i32)?,
                Numeric::Short(v) => self.buffered_docs.write_zint(v as i32)?,
                Numeric::Int(v) => self.buffered_docs.write_zint(v as i32)?,
                Numeric::Long(v) => {
                    CompressingStoredFieldsWriter::<O>::write_tlong(&mut self.buffered_docs, v)?
                }
                Numeric::Float(v) => {
                    CompressingStoredFieldsWriter::<O>::write_zfloat(&mut self.buffered_docs, v)?
                }
                Numeric::Double(v) => {
                    CompressingStoredFieldsWriter::<O>::write_zdouble(&mut self.buffered_docs, v)?
                }
                Numeric::Null => unreachable!(),
            }
        }

        Ok(())
    }

    fn finish(&mut self, _field_infos: &FieldInfos, num_docs: usize) -> Result<()> {
        if self.num_buffered_docs > 0 {
            self.flush()?;
            // incomplete: we had to force this flush
            self.num_dirty_chunks += 1;
        } else {
            debug_assert!(self.buffered_docs.position() == 0);
        }

        if self.doc_base != num_docs as i32 {
            bail!(RuntimeError(format!(
                "Wrote {} docs, finish called with numDocs={}",
                self.doc_base, num_docs
            )));
        }

        self.index_writer
            .finish(num_docs, self.fields_stream.file_pointer())?;
        self.fields_stream.write_vlong(self.num_chunks as i64)?;
        self.fields_stream
            .write_vlong(self.num_dirty_chunks as i64)?;
        write_footer(&mut self.fields_stream)?;
        debug_assert!(self.buffered_docs.position() == 0);
        Ok(())
    }

    fn merge<D: Directory, C: Codec>(&mut self, merge_state: &mut MergeState<D, C>) -> Result<i32> {
        if merge_state.needs_index_sort {
            // TODO: can we gain back some optos even if index is sorted?
            // E.g. if sort results in large chunks of contiguous docs from one sub
            // being copied over...?
            return merge_store_fields(self, merge_state);
        }

        let mut doc_count = 0i32;
        let num_readers = merge_state.max_docs.len();
        let matching = MatchingReaders::new(merge_state);
        if merge_state.needs_index_sort {
            // If all readers are compressed and they have the same fieldinfos then we can
            // merge the serialized document directly.

            let mut subs = Vec::with_capacity(merge_state.stored_fields_readers.len());
            for i in 0..merge_state.stored_fields_readers.len() {
                if matching.matching_readers[i]
                    && merge_state.stored_fields_readers[i]
                        .as_any()
                        .is::<CompressingStoredFieldsReader>()
                {
                    let reader = merge_state.stored_fields_readers[i]
                        .as_any() // TODO: we should use `as_any_mut` here
                        .downcast_ref::<CompressingStoredFieldsReader>()
                        .unwrap();
                    let sub = CompressingStoredFieldMergeSub::new(
                        reader as *const _ as *mut _,
                        Arc::clone(&merge_state.doc_maps[i]),
                        merge_state.max_docs[i],
                    );
                    subs.push(sub);
                } else {
                    return merge_store_fields(self, merge_state);
                }
            }

            let mut doc_id_merger = doc_id_merger_of(subs, true)?;
            while let Some(sub) = doc_id_merger.next()? {
                debug_assert_eq!(sub.base.mapped_doc_id, doc_count);
                let doc = sub.doc_id;
                sub.reader().document(doc)?;
                let doc = sub.reader().current_doc_mut();
                self.start_document()?;
                self.buffered_docs.copy_bytes(&mut doc.input, doc.length)?;
                self.num_stored_fields_in_doc = doc.num_stored_fields as usize;
                self.finish_document()?;
                doc_count += 1;
            }
            self.finish(
                &merge_state.merge_field_infos.as_ref().unwrap().as_ref(),
                doc_count as usize,
            )?;
            return Ok(doc_count);
        }

        for i in 0..num_readers {
            let mut visitor = MergeVisitor::new(merge_state, i, self);

            let mut matching_fields_reader: Option<CompressingStoredFieldsReader> = None;
            if matching.matching_readers[i] {
                let fields_reader = &mut merge_state.stored_fields_readers[i];
                if let Some(r) = fields_reader
                    .as_any()
                    .downcast_ref::<CompressingStoredFieldsReader>()
                {
                    matching_fields_reader = Some(r.clone()?);
                }
            }

            let max_doc = merge_state.max_docs[i];
            let live_docs = merge_state.live_docs[i].as_ref();
            // if its some other format, or an older version of this format, or safety switch:
            if let Some(ref mut fields_reader) = matching_fields_reader {
                if fields_reader.version() != VERSION_CURRENT {
                    // naive merge:
                    for doc_id in 0..max_doc {
                        if !live_docs.get(doc_id as usize)? {
                            continue;
                        }
                        self.start_document()?;
                        fields_reader.visit_document_mut(doc_id, &mut visitor)?;
                        self.finish_document()?;
                        doc_count += 1;
                    }
                } else if fields_reader.compression_mode() == self.compress_mode &&
                    fields_reader.chunk_size() == self.chunk_size as i32 &&
                    fields_reader.packed_ints_version() == PACKED_VERSION_CURRENT &&
                    live_docs.is_empty() &&         // this indicate that live_docs is MatchAll, equal to live_docs == null in Java
                    !Self::too_dirty(fields_reader)
                {
                    // optimized merge, raw byte copy
                    // its not worth fine-graining this if there are deletions.

                    // if the format is older, its always handled by the naive merge case above
                    debug_assert_eq!(fields_reader.version(), VERSION_CURRENT);

                    // flush any pending chunks
                    if self.num_buffered_docs > 0 {
                        self.flush()?;
                        self.num_dirty_chunks += 1; // incomplete: we had to force this flush
                    }

                    // iterate over each chunk. we use the stored fields index to find chunk
                    // boundaries, read the docstart + doccount from the chunk
                    // header (we write a new header, since doc numbers will
                    // change), and just copy the bytes directly.
                    let start_index = fields_reader.index_reader().start_pointer(0)?;
                    fields_reader.fields_stream_mut().seek(start_index)?;
                    let mut doc_id = 0;
                    while doc_id < max_doc {
                        // read header
                        let base = fields_reader.fields_stream_mut().read_vint()?;
                        if base != doc_id {
                            bail!(
                                "CorruptIndex: invalid state: base={}, doc_id={}",
                                base,
                                doc_id
                            );
                        }
                        let code: i32 = fields_reader.fields_stream_mut().read_vint()?;

                        // write a new index entry and new header for this chunk.
                        let buffered_docs = code.unsigned_shift(1);
                        self.index_writer.write_index(
                            buffered_docs as usize,
                            self.fields_stream.file_pointer(),
                        )?;
                        self.fields_stream.write_vint(self.doc_base)?; // rebase
                        self.fields_stream.write_vint(code)?;
                        doc_id += buffered_docs;
                        self.doc_base += buffered_docs;
                        doc_count += buffered_docs;

                        if doc_id > max_doc {
                            bail!(
                                "CorruptIndex: invalid state: base={}, buffered_docs={}, \
                                 max_doc={}",
                                base,
                                buffered_docs,
                                max_doc
                            );
                        }

                        // copy bytes until the next chunk boundary (or end of chunk data).
                        // using the stored fields index for this isn't the most efficient, but
                        // fast enough and is a source of redundancy for
                        // detecting bad things.
                        let end = if doc_id == max_doc {
                            fields_reader.max_pointer()
                        } else {
                            fields_reader.index_reader().start_pointer(doc_id)?
                        };
                        let len = (end - fields_reader.fields_stream_mut().file_pointer()) as usize;
                        self.fields_stream
                            .copy_bytes(fields_reader.fields_stream_mut(), len)?;
                    }

                    if fields_reader.fields_stream_mut().file_pointer()
                        != fields_reader.max_pointer()
                    {
                        bail!(
                            "CorruptIndex: invalid state: raw_docs.file_pointer={}, \
                             fields_reader.max_pointer={}",
                            fields_reader.fields_stream_mut().file_pointer(),
                            fields_reader.max_pointer()
                        );
                    }

                    // since we bulk merged all chunks, we inherit any dirty ones from this segment.
                    debug_assert!(
                        fields_reader.num_chunks() >= 0 && fields_reader.num_dirty_chunks() >= 0
                    );
                    self.num_chunks += fields_reader.num_chunks() as usize;
                    self.num_dirty_chunks += fields_reader.num_dirty_chunks() as usize;
                } else {
                    // optimized merge, we copy serialized (but decompressed) bytes directly
                    // even on simple docs (1 stored field), it seems to help by about 20%

                    // if the format is older, its always handled by the naive merge case above
                    debug_assert_eq!(fields_reader.version(), VERSION_CURRENT);

                    for doc_id in 0..max_doc {
                        if !live_docs.get(doc_id as usize)? {
                            continue;
                        }
                        fields_reader.document(doc_id)?;
                        self.start_document()?;
                        let current_doc = fields_reader.current_doc_mut();
                        self.buffered_docs
                            .copy_bytes(&mut current_doc.input, current_doc.length)?;
                        self.num_stored_fields_in_doc = current_doc.num_stored_fields as usize;
                        self.finish_document()?;
                        doc_count += 1;
                    }
                }
            } else {
                // naive merge:
                for doc_id in 0..max_doc {
                    if !live_docs.get(doc_id as usize)? {
                        continue;
                    }
                    self.start_document()?;
                    merge_state.stored_fields_readers[i]
                        .visit_document_mut(doc_id, &mut visitor)?;
                    self.finish_document()?;
                    doc_count += 1;
                }
            }
        }
        self.finish(
            merge_state.merge_field_infos.as_ref().unwrap().as_ref(),
            doc_count as usize,
        )?;
        Ok(doc_count)
    }
}

struct CompressingStoredFieldMergeSub {
    base: DocIdMergerSubBase,
    reader: *mut CompressingStoredFieldsReader,
    max_doc: DocId,
    doc_id: DocId,
}

impl CompressingStoredFieldMergeSub {
    fn new(
        reader: *mut CompressingStoredFieldsReader,
        doc_map: Arc<LiveDocsDocMap>,
        max_doc: DocId,
    ) -> Self {
        let base = DocIdMergerSubBase::new(doc_map);
        Self {
            base,
            reader,
            max_doc,
            doc_id: -1,
        }
    }

    fn reader(&mut self) -> &mut CompressingStoredFieldsReader {
        unsafe { &mut *self.reader }
    }
}

impl DocIdMergerSub for CompressingStoredFieldMergeSub {
    fn next_doc(&mut self) -> Result<DocId> {
        self.doc_id += 1;
        if self.doc_id == self.max_doc {
            Ok(NO_MORE_DOCS)
        } else {
            Ok(self.doc_id)
        }
    }

    fn base(&self) -> &DocIdMergerSubBase {
        &self.base
    }

    fn base_mut(&mut self) -> &mut DocIdMergerSubBase {
        &mut self.base
    }

    fn reset(&mut self) {
        self.base.reset();
        self.doc_id = -1;
    }
}
