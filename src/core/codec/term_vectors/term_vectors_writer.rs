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

use core::codec::codec_util::index_header_length;
use core::codec::codec_util::*;
use core::codec::field_infos::{FieldInfo, FieldInfos};
use core::codec::segment_infos::segment_file_name;
use core::codec::segment_infos::SegmentInfo;
use core::codec::stored_fields::CompressingStoredFieldsIndexWriter;
use core::codec::term_vectors::term_vectors_reader::*;
use core::codec::term_vectors::{merge_term_vectors, TermVectorsReader, TermVectorsWriter};
use core::codec::Codec;
use core::codec::MatchingReaders;
use core::index::merge::MergeState;
use core::store::directory::Directory;
use core::store::io::{DataInput, DataOutput, GrowableByteArrayDataOutput, IndexOutput};
use core::store::IOContext;
use core::util::bytes_difference;
use core::util::packed::VERSION_CURRENT as PACKED_VERSION_CURRENT;
use core::util::packed::{
    get_writer_no_header, AbstractBlockPackedWriter, BlockPackedWriter, Format, Writer,
};
use core::util::BytesRef;
use core::util::{BitsRequired, UnsignedShift};
use core::util::{Compress, CompressionMode, Compressor};

use error::Result;

use std::collections::BTreeSet;

struct FieldData {
    has_positions: bool,
    has_offsets: bool,
    has_payloads: bool,
    flags: i32,
    field_num: usize,
    num_terms: usize,
    freqs: Vec<i32>,
    prefix_lengths: Vec<i32>,
    suffix_lengths: Vec<i32>,
    pos_start: usize,
    off_start: usize,
    pay_start: usize,
    total_positions: usize,
    ord: usize,
}

impl FieldData {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        field_num: usize,
        num_terms: usize,
        has_positions: bool,
        has_offsets: bool,
        has_payloads: bool,
        pos_start: usize,
        off_start: usize,
        pay_start: usize,
    ) -> FieldData {
        let mut flags = 0;
        if has_positions {
            flags |= POSITIONS;
        }
        if has_offsets {
            flags |= OFFSETS;
        }
        if has_payloads {
            flags |= PAYLOADS;
        }

        FieldData {
            has_positions,
            has_offsets,
            has_payloads,
            flags,
            field_num,
            num_terms,
            freqs: vec![0i32; num_terms],
            prefix_lengths: vec![0i32; num_terms],
            suffix_lengths: vec![0i32; num_terms],
            pos_start,
            off_start,
            pay_start,
            total_positions: 0,
            ord: 0,
        }
    }

    pub fn add_term(&mut self, freq: i32, prefix_length: i32, suffix_length: i32) {
        self.freqs[self.ord] = freq;
        self.prefix_lengths[self.ord] = prefix_length;
        self.suffix_lengths[self.ord] = suffix_length;
        self.ord += 1;
    }

    pub fn add_position(
        &mut self,
        position: i32,
        start_offset: i32,
        length: i32,
        payload_length: i32,
        positions_buf: &mut Vec<i32>,
        start_offsets_buf: &mut Vec<i32>,
        lengths_buf: &mut Vec<i32>,
        payload_length_buf: &mut Vec<i32>,
    ) {
        if self.has_positions {
            if self.pos_start + self.total_positions == positions_buf.len() {
                positions_buf.push(position);
            } else {
                positions_buf[self.pos_start + self.total_positions] = position;
            }
        }

        if self.has_offsets {
            if self.off_start + self.total_positions == start_offsets_buf.len() {
                start_offsets_buf.push(start_offset);
                lengths_buf.push(length);
            } else {
                start_offsets_buf[self.off_start + self.total_positions] = start_offset;
                lengths_buf[self.off_start + self.total_positions] = length;
            }
        }

        if self.has_payloads {
            if self.pay_start + self.total_positions == payload_length_buf.len() {
                payload_length_buf.push(payload_length);
            } else {
                payload_length_buf[self.pay_start + self.total_positions] = payload_length;
            }
        }

        self.total_positions += 1;
    }
}

struct DocData {
    num_fields: usize,
    fields: Vec<FieldData>,
    pos_start: usize,
    off_start: usize,
    pay_start: usize,
}

impl DocData {
    pub fn new(num_fields: usize, pos_start: usize, off_start: usize, pay_start: usize) -> DocData {
        DocData {
            num_fields,
            fields: vec![],
            pos_start,
            off_start,
            pay_start,
        }
    }

    pub fn add_field(
        &mut self,
        field_num: usize,
        num_terms: usize,
        has_positions: bool,
        has_offsets: bool,
        has_payloads: bool,
    ) -> usize {
        let field = if self.fields.is_empty() {
            FieldData::new(
                field_num,
                num_terms,
                has_positions,
                has_offsets,
                has_payloads,
                self.pos_start,
                self.off_start,
                self.pay_start,
            )
        } else {
            let last = self.fields.last().unwrap();
            let mut pos_start = last.pos_start;
            let mut off_start = last.off_start;
            let mut pay_start = last.pay_start;
            if last.has_positions {
                pos_start += last.total_positions;
            }
            if last.has_offsets {
                off_start += last.total_positions;
            }
            if last.has_payloads {
                pay_start += last.total_positions;
            }

            FieldData::new(
                field_num,
                num_terms,
                has_positions,
                has_offsets,
                has_payloads,
                pos_start,
                off_start,
                pay_start,
            )
        };

        self.fields.push(field);
        self.fields.len() - 1
    }

    pub fn add_doc_data<O1: IndexOutput>(
        writer: &mut CompressingTermVectorsWriter<O1>,
        num_vector_fields: usize,
    ) -> usize {
        let mut pos_start = 0;
        let mut off_start = 0;
        let mut pay_start = 0;

        if !writer.pending_docs.is_empty() {
            let mut i = writer.pending_docs.len();
            while i > 0 {
                let doc: &DocData = &writer.pending_docs[i - 1];
                if !doc.fields.is_empty() {
                    let last = doc.fields.last().unwrap();
                    if last.has_positions {
                        pos_start = last.pos_start + last.total_positions;
                    }
                    if last.has_offsets {
                        off_start = last.off_start + last.total_positions;
                    }
                    if last.has_payloads {
                        pay_start = last.pay_start + last.total_positions;
                    }

                    break;
                }

                i -= 1;
            }
        }

        let doc = DocData::new(num_vector_fields, pos_start, off_start, pay_start);

        writer.pending_docs.push(doc);
        writer.pending_docs.len() - 1
    }
}

/// `TermVectorsWriter` for `CompressingTermVectorsFormat`
pub struct CompressingTermVectorsWriter<O: IndexOutput> {
    index_writer: CompressingStoredFieldsIndexWriter<O>,
    vectors_stream: O,
    compress_mode: CompressionMode,
    compressor: Compressor,
    chunk_size: usize,
    // number of compressed blocks written
    num_chunks: usize,
    // number of incomplete compressed blocks written
    num_dirty_chunks: usize,
    num_docs: usize,
    pending_docs: Vec<DocData>,
    cur_doc: usize,
    cur_field: usize,
    last_term: Vec<u8>,
    positions_buf: Vec<i32>,
    start_offsets_buf: Vec<i32>,
    lengths_buf: Vec<i32>,
    payload_length_buf: Vec<i32>,
    term_suffixes: GrowableByteArrayDataOutput,
    payload_bytes: GrowableByteArrayDataOutput,
    writer: BlockPackedWriter,
}

impl<O: IndexOutput> CompressingTermVectorsWriter<O> {
    #[allow(clippy::too_many_arguments)]
    pub fn new<D: Directory, DW: Directory<IndexOutput = O>, C: Codec>(
        directory: &DW,
        si: &SegmentInfo<D, C>,
        segment_suffix: &str,
        context: &IOContext,
        format_name: &str,
        compress_mode: CompressionMode,
        chunk_size: usize,
        block_size: usize,
    ) -> Result<CompressingTermVectorsWriter<O>> {
        let segment = &si.name;
        let mut index_stream = directory.create_output(
            &segment_file_name(segment, segment_suffix, VECTORS_INDEX_EXTENSION),
            context,
        )?;
        let mut vectors_stream = directory.create_output(
            &segment_file_name(segment, segment_suffix, VECTORS_EXTENSION),
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
            &mut vectors_stream,
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
                == vectors_stream.file_pointer() as usize
        );

        let index_writer = CompressingStoredFieldsIndexWriter::new(index_stream, block_size)?;

        vectors_stream.write_vint(PACKED_VERSION_CURRENT as i32)?;
        vectors_stream.write_vint(chunk_size as i32)?;

        Ok(CompressingTermVectorsWriter {
            index_writer,
            vectors_stream,
            compress_mode,
            compressor: compress_mode.new_compressor(),
            chunk_size,
            // number of compressed blocks written
            num_chunks: 0,
            // number of incomplete compressed blocks written
            num_dirty_chunks: 0,
            num_docs: 0,
            pending_docs: vec![],
            cur_doc: 0,
            cur_field: 0,
            last_term: vec![0; 1024],
            positions_buf: vec![0i32; 1024],
            start_offsets_buf: vec![0i32; 1024],
            lengths_buf: vec![0i32; 1024],
            payload_length_buf: vec![0i32; 1024],
            term_suffixes: GrowableByteArrayDataOutput::new(chunk_size + 8),
            payload_bytes: GrowableByteArrayDataOutput::new(1 + 8),
            writer: BlockPackedWriter::new(PACKED_BLOCK_SIZE as usize),
        })
    }

    fn trigger_flush(&self) -> bool {
        self.term_suffixes.position() >= self.chunk_size
            || self.pending_docs.len() >= MAX_DOCUMENTS_PER_CHUNK as usize
    }

    fn flush(&mut self) -> Result<()> {
        let chunk_docs = self.pending_docs.len();
        debug_assert!(chunk_docs > 0);

        // write the index file
        self.index_writer
            .write_index(chunk_docs, self.vectors_stream.file_pointer())?;

        let doc_base = self.num_docs - chunk_docs;
        self.vectors_stream.write_vint(doc_base as i32)?;
        self.vectors_stream.write_vint(chunk_docs as i32)?;
        // total number of fields of the chunk

        let total_fields = self.flush_num_fields(chunk_docs)?;
        if total_fields > 0 {
            // unique field numbers (sorted)
            let field_nums = self.flush_field_nums()?;
            // offsets in the array of unique field numbers
            self.flush_fields(total_fields, &field_nums)?;
            // flags (does the field have positions, offsets, payloads?)
            self.flush_flags(total_fields, &field_nums)?;
            // number of terms of each field
            self.flush_num_terms(total_fields)?;
            // prefix and suffix lengths for each field
            self.flush_term_lengths()?;
            // term freqs - 1 (because termFreq is always >=1) for each term
            self.flush_term_freqs()?;
            // positions for all terms, when enabled
            self.flush_positions()?;
            // offsets for all terms, when enabled
            self.flush_offsets(&field_nums)?;
            // payload lengths for all terms, when enabled
            self.flush_payload_lengths()?;

            // compress terms and payloads and write them to the output
            self.compressor.compress(
                &self.term_suffixes.bytes,
                0,
                self.term_suffixes.position(),
                &mut self.vectors_stream,
            )?;
        }

        self.pending_docs.clear();
        self.cur_doc = 0;
        self.cur_field = 0;
        self.term_suffixes.reset();
        self.num_chunks += 1;
        Ok(())
    }

    fn flush_num_fields(&mut self, chunk_docs: usize) -> Result<usize> {
        if chunk_docs == 1 {
            let num_fields = self.pending_docs[0].num_fields;
            self.vectors_stream.write_vint(num_fields as i32)?;
            Ok(num_fields)
        } else {
            self.writer.reset();
            let mut total_fields = 0usize;
            for dd in &self.pending_docs {
                self.writer
                    .add(dd.num_fields as i64, &mut self.vectors_stream)?;
                total_fields += dd.num_fields;
            }

            self.writer.finish(&mut self.vectors_stream)?;

            Ok(total_fields)
        }
    }

    /// Returns a sorted array containing unique field numbers
    fn flush_field_nums(&mut self) -> Result<Vec<i32>> {
        let mut field_nums: BTreeSet<i32> = BTreeSet::new();
        for dd in &self.pending_docs {
            for fd in &dd.fields {
                field_nums.insert(fd.field_num as i32);
            }
        }

        let num_dist_fields = field_nums.len() as i32;
        debug_assert!(num_dist_fields > 0);
        let mut max_field = 0;
        for field_num in &field_nums {
            max_field = *field_num;
        }
        let bits_required = max_field.bits_required() as i32;
        let token = ((num_dist_fields - 1).min(0x07) << 5) | bits_required;
        self.vectors_stream.write_byte(token as u8)?;
        if num_dist_fields > 0x07 {
            self.vectors_stream.write_vint(num_dist_fields - 1 - 0x07)?;
        }

        let mut writer = get_writer_no_header(Format::Packed, field_nums.len(), bits_required, 1);
        for field_num in &field_nums {
            writer.add(*field_num as i64, &mut self.vectors_stream)?;
        }
        writer.finish(&mut self.vectors_stream)?;

        let mut fns = vec![0i32; field_nums.len()];
        let mut i = 0;
        for field_num in &field_nums {
            fns[i] = *field_num;
            i += 1;
        }

        Ok(fns)
    }

    fn flush_fields(&mut self, total_fields: usize, field_nums: &[i32]) -> Result<()> {
        let mut writer = get_writer_no_header(
            Format::Packed,
            total_fields,
            (field_nums.len() as i64 - 1).bits_required() as i32,
            1,
        );
        for dd in &self.pending_docs {
            for fd in &dd.fields {
                let mut field_num_index = -1i64;
                for (i, f) in field_nums.iter().enumerate() {
                    if fd.field_num as i32 == *f {
                        field_num_index = i as i64;
                        break;
                    }
                }
                debug_assert!(field_num_index >= 0);
                writer.add(field_num_index, &mut self.vectors_stream)?;
            }
        }

        writer.finish(&mut self.vectors_stream)
    }

    fn flush_flags(&mut self, total_fields: usize, field_nums: &[i32]) -> Result<()> {
        // check if fields always have the same flags
        let mut non_changing_flags = true;
        let mut field_flags = vec![-1i32; field_nums.len()];

        'outer: for dd in &self.pending_docs {
            for fd in &dd.fields {
                let mut field_num_index = -1i64;
                for (i, f) in field_nums.iter().enumerate() {
                    if fd.field_num as i32 == *f {
                        field_num_index = i as i64;
                        break;
                    }
                }
                debug_assert!(field_num_index >= 0);

                let field_num_off = field_num_index as usize;
                if field_flags[field_num_off] == -1 {
                    field_flags[field_num_off] = fd.flags;
                } else if field_flags[field_num_off] != fd.flags {
                    non_changing_flags = false;
                    break 'outer;
                }
            }
        }

        if non_changing_flags {
            // write one flag per field num
            self.vectors_stream.write_vint(0)?;
            let mut writer = get_writer_no_header(Format::Packed, field_flags.len(), FLAGS_BITS, 1);
            for flags in &field_flags {
                debug_assert!(*flags >= 0);
                writer.add(*flags as i64, &mut self.vectors_stream)?;
            }
            debug_assert!(writer.ord() as usize == field_flags.len() - 1);
            writer.finish(&mut self.vectors_stream)?;
        } else {
            // write one flag for every field instance
            self.vectors_stream.write_vint(1)?;
            let mut writer = get_writer_no_header(Format::Packed, total_fields, FLAGS_BITS, 1);
            for dd in &self.pending_docs {
                for fd in &dd.fields {
                    writer.add(fd.flags as i64, &mut self.vectors_stream)?;
                }
            }

            debug_assert!(writer.ord() as usize == total_fields - 1);
            writer.finish(&mut self.vectors_stream)?;
        }

        Ok(())
    }

    fn flush_num_terms(&mut self, total_fields: usize) -> Result<()> {
        let mut max_num_terms = 0usize;
        for dd in &self.pending_docs {
            for fd in &dd.fields {
                max_num_terms |= fd.num_terms;
            }
        }

        let bits_required = max_num_terms.bits_required() as i32;
        self.vectors_stream.write_vint(bits_required)?;
        let mut writer = get_writer_no_header(Format::Packed, total_fields, bits_required, 1);
        for dd in &self.pending_docs {
            for fd in &dd.fields {
                writer.add(fd.num_terms as i64, &mut self.vectors_stream)?;
            }
        }

        debug_assert!(writer.ord() as usize == total_fields - 1);
        writer.finish(&mut self.vectors_stream)
    }

    fn flush_term_lengths(&mut self) -> Result<()> {
        self.writer.reset();
        for dd in &self.pending_docs {
            for fd in &dd.fields {
                for i in 0..fd.num_terms {
                    self.writer
                        .add(fd.prefix_lengths[i] as i64, &mut self.vectors_stream)?;
                }
            }
        }
        self.writer.finish(&mut self.vectors_stream)?;

        self.writer.reset();
        for dd in &self.pending_docs {
            for fd in &dd.fields {
                for i in 0..fd.num_terms {
                    self.writer
                        .add(fd.suffix_lengths[i] as i64, &mut self.vectors_stream)?;
                }
            }
        }
        self.writer.finish(&mut self.vectors_stream)
    }

    fn flush_term_freqs(&mut self) -> Result<()> {
        self.writer.reset();
        for dd in &self.pending_docs {
            for fd in &dd.fields {
                for i in 0..fd.num_terms {
                    self.writer
                        .add(fd.freqs[i] as i64 - 1, &mut self.vectors_stream)?;
                }
            }
        }
        self.writer.finish(&mut self.vectors_stream)
    }

    fn flush_positions(&mut self) -> Result<()> {
        self.writer.reset();
        for dd in &self.pending_docs {
            for fd in &dd.fields {
                if fd.has_positions {
                    let mut pos = 0usize;
                    for i in 0..fd.num_terms {
                        let mut prev_position = 0usize;
                        for _ in 0..fd.freqs[i] {
                            let position = self.positions_buf[fd.pos_start + pos] as usize;
                            pos += 1;
                            self.writer
                                .add((position - prev_position) as i64, &mut self.vectors_stream)?;
                            prev_position = position;
                        }
                    }

                    debug_assert!(pos == fd.total_positions);
                }
            }
        }
        self.writer.finish(&mut self.vectors_stream)
    }

    fn flush_offsets(&mut self, field_nums: &[i32]) -> Result<()> {
        let mut has_offsets = false;
        let mut sum_pos = vec![0i64; field_nums.len()];
        let mut sum_offsets = vec![0i64; field_nums.len()];
        for dd in &self.pending_docs {
            for fd in &dd.fields {
                has_offsets |= fd.has_offsets;
                if fd.has_offsets && fd.has_positions {
                    let mut field_num_index = -1i64;
                    for (i, f) in field_nums.iter().enumerate() {
                        if fd.field_num as i32 == *f {
                            field_num_index = i as i64;
                            break;
                        }
                    }
                    debug_assert!(field_num_index >= 0);
                    let field_num_off = field_num_index as usize;
                    let mut pos = 0usize;
                    for i in 0..fd.num_terms {
                        let mut prev_pos = 0usize;
                        let mut prev_off = 0usize;
                        for _ in 0..fd.freqs[i] {
                            let position = self.positions_buf[fd.pos_start + pos] as usize;
                            let start_offset = self.start_offsets_buf[fd.off_start + pos] as usize;
                            sum_pos[field_num_off] += (position - prev_pos) as i64;
                            sum_offsets[field_num_off] += (start_offset - prev_off) as i64;
                            prev_pos = position;
                            prev_off = start_offset;
                            pos += 1;
                        }
                    }
                    debug_assert!(pos == fd.total_positions);
                }
            }
        }

        if !has_offsets {
            return Ok(());
        }

        let mut chars_per_term = vec![0f32; field_nums.len()];
        for i in 0..field_nums.len() {
            chars_per_term[i] = if sum_pos[i] <= 0 || sum_offsets[i] <= 0 {
                0f32
            } else {
                (sum_offsets[i] as f64 / sum_pos[i] as f64) as f32
            };
        }

        // start offsets
        for i in 0..field_nums.len() {
            self.vectors_stream
                .write_int(chars_per_term[i].to_bits() as i32)?;
        }

        self.writer.reset();
        for dd in &self.pending_docs {
            for fd in &dd.fields {
                if fd.flags & OFFSETS != 0 {
                    let mut field_num_index = -1i64;
                    for (i, f) in field_nums.iter().enumerate() {
                        if fd.field_num as i32 == *f {
                            field_num_index = i as i64;
                            break;
                        }
                    }
                    debug_assert!(field_num_index >= 0);
                    let field_num_off = field_num_index as usize;
                    let cpt = chars_per_term[field_num_off];
                    let mut pos = 0usize;
                    for i in 0..fd.num_terms {
                        let mut prev_pos = 0usize;
                        let mut prev_off = 0usize;
                        for _j in 0..fd.freqs[i] {
                            let position = if fd.has_positions {
                                self.positions_buf[fd.pos_start + pos] as usize
                            } else {
                                0
                            };
                            let start_offset = self.start_offsets_buf[fd.off_start + pos] as usize;
                            self.writer.add(
                                start_offset as i64
                                    - prev_off as i64
                                    - (cpt * (position - prev_pos) as f32) as i64,
                                &mut self.vectors_stream,
                            )?;
                            prev_pos = position;
                            prev_off = start_offset;
                            pos += 1;
                        }
                    }
                }
            }
        }
        self.writer.finish(&mut self.vectors_stream)?;

        // lengths
        self.writer.reset();
        for dd in &self.pending_docs {
            for fd in &dd.fields {
                if (fd.flags & OFFSETS) != 0 {
                    let mut pos = 0usize;
                    for i in 0..fd.num_terms {
                        for _ in 0..fd.freqs[i] {
                            self.writer.add(
                                (self.lengths_buf[fd.off_start + pos]
                                    - fd.prefix_lengths[i]
                                    - fd.suffix_lengths[i]) as i64,
                                &mut self.vectors_stream,
                            )?;
                            pos += 1;
                        }
                    }

                    debug_assert!(pos == fd.total_positions);
                }
            }
        }
        self.writer.finish(&mut self.vectors_stream)
    }

    fn flush_payload_lengths(&mut self) -> Result<()> {
        self.writer.reset();
        for dd in &self.pending_docs {
            for fd in &dd.fields {
                if fd.has_payloads {
                    for i in 0..fd.total_positions {
                        self.writer.add(
                            self.payload_length_buf[fd.pos_start + i] as i64,
                            &mut self.vectors_stream,
                        )?;
                    }
                }
            }
        }

        self.writer.finish(&mut self.vectors_stream)
    }

    /// Returns true if we should recompress this reader, even though
    /// we could bulk merge compressed data
    ///
    /// The last chunk written for a segment is typically incomplete, so without recompressing,
    /// in some worst-case situations (e.g. frequent reopen with tiny flushes), over time the
    /// compression ratio can degrade. This is a safety switch.
    fn too_dirty(candidate: &CompressingTermVectorsReader) -> bool {
        // more than 1% dirty, or more than hard limit of 1024 dirty chunks
        candidate.num_dirty_chunks() > 1024
            || candidate.num_dirty_chunks() * 100 > candidate.num_chunks()
    }
}

impl<O: IndexOutput> TermVectorsWriter for CompressingTermVectorsWriter<O> {
    fn start_document(&mut self, num_vector_fields: usize) -> Result<()> {
        self.cur_doc = DocData::add_doc_data(self, num_vector_fields);
        Ok(())
    }

    fn finish_document(&mut self) -> Result<()> {
        // append the payload bytes of the doc after its terms
        self.term_suffixes.write_bytes(
            &self.payload_bytes.bytes,
            0,
            self.payload_bytes.position(),
        )?;
        self.payload_bytes.reset();

        self.num_docs += 1;
        if self.trigger_flush() {
            self.flush()?;
        }

        self.cur_doc = 0;

        Ok(())
    }

    fn start_field(
        &mut self,
        info: &FieldInfo,
        num_terms: usize,
        has_positions: bool,
        has_offsets: bool,
        has_payloads: bool,
    ) -> Result<()> {
        let cur_doc: &mut DocData = &mut self.pending_docs[self.cur_doc];
        self.cur_field = cur_doc.add_field(
            info.number as usize,
            num_terms,
            has_positions,
            has_offsets,
            has_payloads,
        );
        self.last_term.clear();
        Ok(())
    }

    fn finish_field(&mut self) -> Result<()> {
        self.cur_field = 0;
        Ok(())
    }

    fn start_term(&mut self, term: BytesRef, freq: i32) -> Result<()> {
        debug_assert!(freq >= 1);
        let prefix = bytes_difference(&self.last_term, &term.bytes());
        let cur_field: &mut FieldData = &mut self.pending_docs[self.cur_doc].fields[self.cur_field];
        cur_field.add_term(freq, prefix, term.len() as i32 - prefix);
        self.term_suffixes.write_bytes(
            &term.bytes(),
            prefix as usize,
            term.len() - prefix as usize,
        )?;

        self.last_term.resize(term.len(), 0u8);
        self.last_term.copy_from_slice(&term.bytes()[0..term.len()]);
        Ok(())
    }

    fn add_position(
        &mut self,
        position: i32,
        start_offset: i32,
        end_offset: i32,
        payload: &[u8],
    ) -> Result<()> {
        let cur_field: &mut FieldData = &mut self.pending_docs[self.cur_doc].fields[self.cur_field];
        debug_assert!(cur_field.flags != 0);
        cur_field.add_position(
            position,
            start_offset,
            end_offset - start_offset,
            payload.len() as i32,
            &mut self.positions_buf,
            &mut self.start_offsets_buf,
            &mut self.lengths_buf,
            &mut self.payload_length_buf,
        );
        if cur_field.has_payloads && !payload.is_empty() {
            self.payload_bytes.write_bytes(payload, 0, payload.len())?;
        }

        Ok(())
    }

    fn finish(&mut self, _fis: &FieldInfos, num_docs: usize) -> Result<()> {
        if !self.pending_docs.is_empty() {
            self.flush()?;
            // incomplete: we had to force this flush
            self.num_dirty_chunks += 1;
        }

        if num_docs != self.num_docs {
            bail!(
                "Wrote {} docs, finish called with numDocs={}",
                self.num_docs,
                num_docs
            );
        }

        self.index_writer
            .finish(num_docs, self.vectors_stream.file_pointer())?;
        self.vectors_stream.write_vlong(self.num_chunks as i64)?;
        self.vectors_stream
            .write_vlong(self.num_dirty_chunks as i64)?;
        write_footer(&mut self.vectors_stream)
    }

    fn add_prox<I: DataInput + ?Sized>(
        &mut self,
        num_prox: usize,
        positions: Option<&mut I>,
        offsets: Option<&mut I>,
    ) -> Result<()> {
        let cur_field: &mut FieldData = &mut self.pending_docs[self.cur_doc].fields[self.cur_field];
        debug_assert!(cur_field.has_positions == positions.is_some());
        debug_assert!(cur_field.has_offsets == offsets.is_some());

        let positions = positions.unwrap();
        let offsets = offsets.unwrap();

        if cur_field.has_positions {
            let pos_start = cur_field.pos_start + cur_field.total_positions;
            if pos_start + num_prox > self.positions_buf.len() {
                self.positions_buf.resize(pos_start + num_prox, 0i32);
            }
            let mut position = 0;
            if cur_field.has_payloads {
                let pay_start = cur_field.pay_start + cur_field.total_positions;
                if pay_start + num_prox > self.payload_length_buf.len() {
                    self.payload_length_buf.resize(pay_start + num_prox, 0i32);
                }

                for i in 0..num_prox {
                    let code: i32 = positions.read_vint()?;
                    if (code & 1) != 0 {
                        // This position has a payload
                        let payload_length = positions.read_vint()?;
                        self.payload_length_buf[pay_start + i] = payload_length;
                        self.payload_bytes
                            .copy_bytes(positions, payload_length as usize)?;
                    } else {
                        self.payload_length_buf[pay_start + i] = 0;
                    }

                    position += code.unsigned_shift(1);
                    self.positions_buf[pos_start + i] = position;
                }
            } else {
                for i in 0..num_prox {
                    let code: i32 = positions.read_vint()?;
                    position += code.unsigned_shift(1);
                    self.positions_buf[pos_start + i] = position;
                }
            }
        }

        if cur_field.has_offsets {
            let off_start = cur_field.off_start + cur_field.total_positions;
            if off_start + num_prox > self.start_offsets_buf.len() {
                self.start_offsets_buf
                    .resize(off_start + num_prox + 8, 0i32);
                self.lengths_buf.resize(off_start + num_prox + 8, 0i32);
            }

            let mut last_offset = 0;
            let mut start_offset;
            let mut end_offset;
            for i in 0..num_prox {
                start_offset = last_offset + offsets.read_vint()?;
                end_offset = start_offset + offsets.read_vint()?;
                last_offset = end_offset;
                self.start_offsets_buf[off_start + i] = start_offset;
                self.lengths_buf[off_start + i] = end_offset - start_offset;
            }
        }

        cur_field.total_positions += num_prox;
        Ok(())
    }

    fn merge<D: Directory, C: Codec>(&mut self, merge_state: &mut MergeState<D, C>) -> Result<i32> {
        if merge_state.needs_index_sort {
            // TODO: can we gain back some optos even if index is sorted?
            // E.g. if sort results in large chunks of contiguous docs from one sub
            // being copied over...?
            return merge_term_vectors(self, merge_state);
        }

        let mut doc_count = 0;
        let num_readers = merge_state.max_docs.len();
        let matching = MatchingReaders::new(merge_state);

        for i in 0..num_readers {
            let mut matching_vectors_reader: Option<CompressingTermVectorsReader> = None;
            if matching.matching_readers[i] {
                // we can only bulk-copy if the matching reader is also a
                // CompressingTermVectorsReader
                if let Some(ref mut vectors_reader) = merge_state.term_vectors_readers[i] {
                    if let Some(r) = vectors_reader
                        .as_any()
                        .downcast_ref::<CompressingTermVectorsReader>()
                    {
                        matching_vectors_reader = Some(r.clone()?);
                    }
                }
            }

            let max_doc = merge_state.max_docs[i];
            let live_docs = merge_state.live_docs[i].as_ref();
            match matching_vectors_reader {
                Some(ref mut vectors_reader)
                    if vectors_reader.compression_mode() == self.compress_mode
                        && vectors_reader.chunk_size() == self.chunk_size as i32
                        && vectors_reader.version() == VERSION_CURRENT
                        && vectors_reader.packed_ints_version() == PACKED_VERSION_CURRENT
                        && live_docs.is_empty()
                        && !Self::too_dirty(vectors_reader) =>
                {
                    // optimized merge, raw byte copy
                    // its not worth fine-graining this if there are deletions.

                    // flush any pending chunks
                    if !self.pending_docs.is_empty() {
                        self.flush()?;
                        self.num_dirty_chunks += 1;
                    }

                    // iterate over each chunk. we use the vectors index to find chunk boundaries,
                    // read the docstart + doccount from the chunk header (we write a new header,
                    // since doc numbers will change), and just copy the bytes directly.
                    let mut raw_docs = vectors_reader.vectors_input().clone()?;
                    let start_index = vectors_reader.index_reader().start_pointer(0)?;
                    raw_docs.seek(start_index)?;
                    let mut doc_id = 0;
                    while doc_id < max_doc {
                        // read header
                        let base = raw_docs.read_vint()?;
                        if base != doc_id {
                            bail!(
                                "CorruptIndex: invalid state: base={}, doc_id={}",
                                base,
                                doc_id
                            );
                        }
                        let buffered_docs = raw_docs.read_vint()?;
                        debug_assert!(buffered_docs >= 0);

                        // write a new index entry and new header for this chunk.
                        self.index_writer.write_index(
                            buffered_docs as usize,
                            self.vectors_stream.file_pointer(),
                        )?;
                        self.vectors_stream.write_vint(doc_count)?; // rebase
                        self.vectors_stream.write_vint(buffered_docs)?;
                        doc_id += buffered_docs;
                        doc_count += buffered_docs;
                        self.num_docs += buffered_docs as usize;

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
                            vectors_reader.max_pointer()
                        } else {
                            vectors_reader.index_reader().start_pointer(doc_id)?
                        };
                        let len = (end - raw_docs.file_pointer()) as usize;
                        self.vectors_stream.copy_bytes(raw_docs.as_mut(), len)?;
                    }

                    if raw_docs.file_pointer() != vectors_reader.max_pointer() {
                        bail!(
                            "CorruptIndex: invalid state: raw_docs.file_pointer={}, \
                             fields_reader.max_pointer={}",
                            raw_docs.file_pointer(),
                            vectors_reader.max_pointer()
                        );
                    }

                    // since we bulk merged all chunks, we inherit any dirty ones from this segment.
                    debug_assert!(
                        vectors_reader.num_chunks() >= 0 && vectors_reader.num_dirty_chunks() >= 0
                    );
                    self.num_chunks += vectors_reader.num_chunks() as usize;
                    self.num_dirty_chunks += vectors_reader.num_dirty_chunks() as usize;
                }
                _ => {
                    // navie merge
                    for doc in 0..max_doc {
                        if !live_docs.get(doc as usize)? {
                            continue;
                        }
                        let vectors =
                            if let Some(ref vectors_reader) = merge_state.term_vectors_readers[i] {
                                vectors_reader.get(doc)?
                            } else {
                                None
                            };
                        self.add_all_doc_vectors(vectors.as_ref(), merge_state)?;
                        doc_count += 1;
                    }
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
