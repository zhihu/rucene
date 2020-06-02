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

use core::codec::field_infos::FieldInfo;
use core::codec::postings::blocktree::BlockTermState;
use core::codec::postings::for_util::*;
use core::codec::postings::posting_format::BLOCK_SIZE;
use core::codec::postings::posting_reader::*;
use core::codec::postings::skip_writer::Lucene50SkipWriter;
use core::codec::postings::{PostingsWriterBase, VERSION_START};
use core::codec::segment_infos::{segment_file_name, SegmentWriteState};
use core::codec::{write_footer, write_index_header, Codec, TermIterator};
use core::codec::{PostingIterator, PostingIteratorFlags};
use core::doc::IndexOptions;
use core::index::writer::INDEX_MAX_POSITION;
use core::search::{DocIterator, NO_MORE_DOCS};
use core::store::directory::Directory;
use core::store::io::{DataOutput, IndexOutput};
use core::util::packed::{SIMD128Packer, SIMDPacker, COMPACT};
use core::util::{BitSet, DocId, FixedBitSet};
use error::{ErrorKind, Result};

pub struct EfWriterMeta {
    pub ef_base_doc: DocId,
    pub ef_upper_doc: DocId,
    pub use_ef: bool,
    pub with_pf: bool,
    pub bits: FixedBitSet,
}

impl EfWriterMeta {
    pub fn new() -> Self {
        Self {
            ef_base_doc: -1,
            ef_upper_doc: 0,
            use_ef: false,
            with_pf: true,
            bits: FixedBitSet::default(),
        }
    }

    pub fn reset(&mut self) {
        self.ef_base_doc = -1;
        self.ef_upper_doc = 0;
        self.bits.clear_all();
    }
}

/// Concrete class that writes docId(maybe frq,pos,offset,payloads) list
/// with postings format.
///
/// Postings list for each term will be stored separately.
pub struct Lucene50PostingsWriter<O: IndexOutput> {
    doc_out: O,
    pos_out: Option<O>,
    pay_out: Option<O>,
    last_state: BlockTermState,
    // Holds starting file pointers for current term:
    doc_start_fp: i64,
    pos_start_fp: i64,
    pay_start_fp: i64,

    doc_delta_buffer: Vec<i32>,
    freq_buffer: Vec<i32>,
    doc_buffer_upto: usize,

    pos_delta_buffer: Vec<i32>,
    payload_length_buffer: Vec<i32>,
    offset_start_delta_buffer: Vec<i32>,
    offset_length_buffer: Vec<i32>,
    pos_buffer_upto: usize,

    payload_bytes: Vec<u8>,
    payload_byte_upto: usize,

    last_block_doc_id: DocId,
    last_block_pos_fp: i64,
    last_block_pay_fp: i64,
    last_block_pos_buffer_upto: usize,
    last_block_payload_byte_upto: usize,

    last_doc_id: DocId,
    last_position: usize,
    last_start_offset: i32,
    doc_count: i32,

    encoded: Vec<u8>,
    for_util: ForUtil,
    skip_writer: Lucene50SkipWriter,

    // fields from PushPostingsWriterBase
    // Reused in writeTerm
    // postings_iterator: Box<PostingIterator>,
    enum_flags: u16,
    // field_info: FieldInfo,
    // index_options: IndexOptions,
    write_freqs: bool,
    write_positions: bool,
    write_payloads: bool,
    write_offsets: bool,
    ef_writer_meta: EfWriterMeta,
    use_simd: bool,
}

impl<O: IndexOutput> Lucene50PostingsWriter<O> {
    pub fn new<D: Directory, DW: Directory<IndexOutput = O>, C: Codec>(
        state: &SegmentWriteState<D, DW, C>,
    ) -> Result<Self> {
        let acceptable_overhead_ratio = COMPACT;

        let doc_file_name = segment_file_name(
            &state.segment_info.name,
            &state.segment_suffix,
            DOC_EXTENSION,
        );
        let mut doc_out = state
            .directory
            .create_output(&doc_file_name, &state.context)?;
        let mut pos_out = None;
        let mut pay_out = None;
        let mut pos_delta_buffer = Vec::with_capacity(0);
        let payload_bytes = vec![0u8; 128];
        let mut payload_length_buffer = Vec::with_capacity(0);
        let mut offset_start_delta_buffer = Vec::with_capacity(0);
        let mut offset_length_buffer = Vec::with_capacity(0);
        write_index_header(
            &mut doc_out,
            DOC_CODEC,
            VERSION_CURRENT,
            state.segment_info.get_id(),
            &state.segment_suffix,
        )?;
        let for_util = ForUtil::with_output(acceptable_overhead_ratio, &mut doc_out)?;
        if state.field_infos.has_prox {
            pos_delta_buffer = vec![0i32; max_data_size()];
            let pos_file_name = segment_file_name(
                &state.segment_info.name,
                &state.segment_suffix,
                POS_EXTENSION,
            );
            pos_out = Some(
                state
                    .directory
                    .create_output(&pos_file_name, &state.context)?,
            );
            write_index_header(
                pos_out.as_mut().unwrap(),
                POS_CODEC,
                VERSION_CURRENT,
                state.segment_info.get_id(),
                &state.segment_suffix,
            )?;
            if state.field_infos.has_payloads {
                payload_length_buffer = vec![0i32; max_data_size()];
            }

            if state.field_infos.has_offsets {
                offset_start_delta_buffer = vec![0i32; max_data_size()];
                offset_length_buffer = vec![0i32; max_data_size()];
            }

            if state.field_infos.has_payloads || state.field_infos.has_offsets {
                let pay_file_name = segment_file_name(
                    &state.segment_info.name,
                    &state.segment_suffix,
                    PAY_EXTENSION,
                );
                pay_out = Some(
                    state
                        .directory
                        .create_output(&pay_file_name, &state.context)?,
                );
                write_index_header(
                    pay_out.as_mut().unwrap(),
                    PAY_CODEC,
                    VERSION_CURRENT,
                    state.segment_info.get_id(),
                    &state.segment_suffix,
                )?;
            }
        }

        let skip_writer = Lucene50SkipWriter::new(
            MAX_SKIP_LEVELS,
            BLOCK_SIZE as u32,
            state.segment_info.max_doc() as u32,
            pos_out.is_some(),
            pay_out.is_some(),
        );

        let use_simd = if VERSION_CURRENT > VERSION_START && SIMD128Packer::is_support() {
            true
        } else {
            false
        };

        Ok(Lucene50PostingsWriter {
            doc_out,
            pos_out,
            pay_out,
            last_state: BlockTermState::new(),
            doc_start_fp: 0,
            pos_start_fp: 0,
            pay_start_fp: 0,

            doc_delta_buffer: vec![0i32; max_data_size()],
            freq_buffer: vec![0i32; max_data_size()],
            doc_buffer_upto: 0,

            pos_delta_buffer,
            payload_length_buffer,
            offset_start_delta_buffer,
            offset_length_buffer,
            pos_buffer_upto: 0,

            payload_bytes,
            payload_byte_upto: 0,

            last_block_doc_id: 0,
            last_block_pos_fp: 0,
            last_block_pay_fp: 0,
            last_block_pos_buffer_upto: 0,
            last_block_payload_byte_upto: 0,

            last_doc_id: 0,
            last_position: 0,
            last_start_offset: 0,
            doc_count: 0,

            encoded: vec![0u8; MAX_ENCODED_SIZE],
            for_util,
            skip_writer,
            enum_flags: 0,
            write_freqs: false,
            write_positions: false,
            write_payloads: false,
            write_offsets: false,
            ef_writer_meta: EfWriterMeta::new(),
            use_simd,
        })
    }

    fn new_term_state(&self) -> BlockTermState {
        BlockTermState::new()
    }

    /// Sets the current field for writing, and returns the
    /// fixed length of Vec<i64> metadata (which is fixed per
    /// field), called when the writing switches to another field.
    fn set_field_base(&mut self, field_info: &FieldInfo) -> i32 {
        // self.index_options = field_info.index_options;
        self.write_freqs = field_info.index_options >= IndexOptions::DocsAndFreqs;
        self.write_positions = field_info.index_options >= IndexOptions::DocsAndFreqsAndPositions;
        self.write_offsets =
            field_info.index_options >= IndexOptions::DocsAndFreqsAndPositionsAndOffsets;
        self.write_payloads = field_info.has_store_payloads;

        self.enum_flags = if !self.write_freqs {
            0
        } else if !self.write_positions {
            PostingIteratorFlags::FREQS
        } else if !self.write_offsets {
            if self.write_payloads {
                PostingIteratorFlags::PAYLOADS
            } else {
                PostingIteratorFlags::POSITIONS
            }
        } else if self.write_payloads {
            PostingIteratorFlags::ALL
        } else {
            PostingIteratorFlags::OFFSETS
        };

        // self.field_info = field_info;

        0
    }

    pub fn start_term(&mut self) {
        self.doc_start_fp = self.doc_out.file_pointer();
        self.ef_writer_meta.reset();
        if self.write_positions {
            self.pos_start_fp = self.pos_out.as_ref().unwrap().file_pointer();
            if self.write_payloads || self.write_offsets {
                self.pay_start_fp = self.pay_out.as_ref().unwrap().file_pointer();
            }
        }
        self.last_doc_id = 0;
        self.last_block_doc_id = -1;
        self.skip_writer
            .reset_skip(self.doc_start_fp, self.pos_start_fp, self.pay_start_fp);
    }

    pub fn start_doc(&mut self, doc_id: DocId, term_doc_freq: i32) -> Result<()> {
        // Have collected a block of docs, and get a new doc.
        // Should write skip data as well as postings list for
        // current block.
        if self.last_block_doc_id != -1 && self.doc_buffer_upto == 0 {
            self.skip_writer.buffer_skip(
                self.last_block_doc_id,
                self.doc_count as u32,
                self.last_block_pos_fp,
                self.last_block_pay_fp,
                self.last_block_pos_buffer_upto,
                self.last_block_payload_byte_upto,
                self.doc_out.file_pointer(),
            )?;
        }

        let doc_delta = doc_id - self.last_doc_id;

        if doc_id < 0 || (self.doc_count > 0 && doc_delta <= 0) {
            bail!(ErrorKind::CorruptIndex("docs out of order".into()));
        }

        self.doc_delta_buffer[self.doc_buffer_upto] = doc_delta;
        if self.write_freqs {
            self.freq_buffer[self.doc_buffer_upto] = term_doc_freq;
        }

        self.doc_buffer_upto += 1;
        self.doc_count += 1;

        if self.doc_buffer_upto == BLOCK_SIZE as usize {
            self.ef_writer_meta.ef_upper_doc = doc_id;
            self.for_util.write_block(
                &self.doc_delta_buffer,
                &mut self.encoded,
                &mut self.doc_out,
                Some(&mut self.ef_writer_meta),
                self.use_simd,
            )?;
            if self.write_freqs {
                self.for_util.write_block(
                    &self.freq_buffer,
                    &mut self.encoded,
                    &mut self.doc_out,
                    None,
                    self.use_simd,
                )?;
            }
            // NOTE: don't set docBufferUpto back to 0 here;
            // finishDoc will do so (because it needs to see that
            // the block was filled so it can save skip data)
        }

        self.last_doc_id = doc_id;
        self.last_position = 0;
        self.last_start_offset = 0;
        Ok(())
    }

    pub fn add_position(
        &mut self,
        position: i32,
        payload: &[u8],
        start_offset: i32,
        end_offset: i32,
    ) -> Result<()> {
        if position > INDEX_MAX_POSITION {
            bail!(ErrorKind::CorruptIndex(
                "position is too large (> INDEX_MAX_POSITION)".into()
            ));
        }
        if position < 0 {
            bail!(ErrorKind::CorruptIndex("position < 0".into()));
        }

        self.pos_delta_buffer[self.pos_buffer_upto] = position - self.last_position as i32;
        if self.write_payloads {
            if payload.is_empty() {
                // no payload
                self.payload_length_buffer[self.pos_buffer_upto] = 0;
            } else {
                self.payload_length_buffer[self.pos_buffer_upto] = payload.len() as i32;
                let total = self.payload_byte_upto + payload.len();
                if total > self.payload_bytes.len() {
                    self.payload_bytes.resize(total, 0u8);
                }
                self.payload_bytes[self.payload_byte_upto..total].copy_from_slice(&payload);
                self.payload_byte_upto += payload.len();
            }
        }

        if self.write_offsets {
            debug_assert!(start_offset >= self.last_start_offset);
            debug_assert!(end_offset >= start_offset);

            self.offset_start_delta_buffer[self.pos_buffer_upto] =
                start_offset - self.last_start_offset;
            self.offset_length_buffer[self.pos_buffer_upto] = end_offset - start_offset;
            self.last_start_offset = start_offset;
        }

        self.pos_buffer_upto += 1;
        self.last_position = position as usize;
        if self.pos_buffer_upto == BLOCK_SIZE as usize {
            self.for_util.write_block(
                &self.pos_delta_buffer,
                &mut self.encoded,
                self.pos_out.as_mut().unwrap(),
                None,
                self.use_simd,
            )?;

            if self.write_payloads {
                self.for_util.write_block(
                    &self.payload_length_buffer,
                    &mut self.encoded,
                    self.pay_out.as_mut().unwrap(),
                    None,
                    self.use_simd,
                )?;
                self.pay_out
                    .as_mut()
                    .unwrap()
                    .write_vint(self.payload_byte_upto as i32)?;
                self.pay_out.as_mut().unwrap().write_bytes(
                    &self.payload_bytes,
                    0,
                    self.payload_byte_upto,
                )?;
                self.payload_byte_upto = 0;
            }

            if self.write_offsets {
                self.for_util.write_block(
                    &self.offset_start_delta_buffer,
                    &mut self.encoded,
                    self.pay_out.as_mut().unwrap(),
                    None,
                    self.use_simd,
                )?;
                self.for_util.write_block(
                    &self.offset_length_buffer,
                    &mut self.encoded,
                    self.pay_out.as_mut().unwrap(),
                    None,
                    self.use_simd,
                )?;
            }
            self.pos_buffer_upto = 0;
        }
        Ok(())
    }

    pub fn finish_doc(&mut self) {
        // Since we don't know df for current term, we had to buffer
        // those skip data for each block, and when a new doc comes,
        // write them to skip file.
        if self.doc_buffer_upto == BLOCK_SIZE as usize {
            self.last_block_doc_id = self.last_doc_id;
            if self.pos_out.is_some() {
                if self.pay_out.is_some() {
                    self.last_block_pay_fp = self.pay_out.as_ref().unwrap().file_pointer();
                }
                self.last_block_pos_fp = self.pos_out.as_ref().unwrap().file_pointer();
                self.last_block_pos_buffer_upto = self.pos_buffer_upto;
                self.last_block_payload_byte_upto = self.payload_byte_upto;
            }
            self.doc_buffer_upto = 0;
            self.ef_writer_meta.ef_base_doc = self.last_block_doc_id;
        }
    }

    // Called when we are done adding docs to this term
    pub fn finish_term(&mut self, state: &mut BlockTermState) -> Result<()> {
        assert!(state.doc_freq > 0);

        // TODO: wasteful we are counting this (counting # docs
        // for this term) in two places?
        assert_eq!(state.doc_freq, self.doc_count);

        // docFreq == 1, don't write the single docid/freq to a separate file along with a pointer
        // to it.
        let singleton_doc_id = if state.doc_freq == 1 {
            // pulse the singleton docid into the term dictionary, freq is implicitly totalTermFreq
            self.doc_delta_buffer[0]
        } else {
            // vInt encode the remaining doc deltas and freqs
            for i in 0..self.doc_buffer_upto {
                let doc_delta = self.doc_delta_buffer[i];
                let freq = self.freq_buffer[i];
                if !self.write_freqs {
                    self.doc_out.write_vint(doc_delta)?;
                } else if self.freq_buffer[i] == 1 {
                    self.doc_out.write_vint(doc_delta << 1 | 1)?;
                } else {
                    self.doc_out.write_vint(doc_delta << 1)?;
                    self.doc_out.write_vint(freq)?;
                }
            }
            -1
        };

        let mut last_pos_block_offset = -1;
        if self.write_positions {
            // totalTermFreq is just total number of positions(or payloads, or offsets)
            // associated with current term.
            debug_assert!(state.total_term_freq != -1);
            if state.total_term_freq > BLOCK_SIZE as i64 {
                // record file offset for last pos in last block
                last_pos_block_offset =
                    self.pos_out.as_ref().unwrap().file_pointer() - self.pos_start_fp;
            }

            if self.pos_buffer_upto > 0 {
                // TODO: should we send offsets/payloads to
                // .pay...?  seems wasteful (have to store extra
                // vLong for low (< BLOCK_SIZE) DF terms = vast vast
                // majority)

                // vInt encode the remaining positions/payloads/offsets:
                let mut last_payload_length = -1; // force first payload length to be written
                let mut last_offset_length = -1; // force first offset length to be written
                let mut payload_bytes_read_upto = 0;
                for i in 0..self.pos_buffer_upto {
                    let pos_delta = self.pos_delta_buffer[i];
                    if self.write_payloads {
                        let payload_length = self.payload_length_buffer[i];
                        if payload_length != last_payload_length {
                            last_payload_length = payload_length;
                            self.pos_out
                                .as_mut()
                                .unwrap()
                                .write_vint(pos_delta << 1 | 1)?;
                            self.pos_out.as_mut().unwrap().write_vint(payload_length)?;
                        } else {
                            self.pos_out.as_mut().unwrap().write_vint(pos_delta << 1)?;
                        }

                        if payload_length != 0 {
                            self.pos_out.as_mut().unwrap().write_bytes(
                                &self.payload_bytes,
                                payload_bytes_read_upto,
                                payload_length as usize,
                            )?;
                            payload_bytes_read_upto += payload_length as usize;
                        }
                    } else {
                        self.pos_out.as_mut().unwrap().write_vint(pos_delta)?;
                    }

                    if self.write_offsets {
                        let delta = self.offset_start_delta_buffer[i];
                        let length = self.offset_length_buffer[i];
                        if length == last_offset_length {
                            self.pos_out.as_mut().unwrap().write_vint(delta << 1)?;
                        } else {
                            self.pos_out.as_mut().unwrap().write_vint(delta << 1 | 1)?;
                            self.pos_out.as_mut().unwrap().write_vint(length)?;
                            last_offset_length = length;
                        }
                    }
                }

                if self.write_payloads {
                    debug_assert_eq!(payload_bytes_read_upto, self.payload_byte_upto);
                    self.payload_byte_upto = 0;
                }
            }
        }

        let skip_offset = if self.doc_count > BLOCK_SIZE {
            self.skip_writer.write_skip(&mut self.doc_out)? - self.doc_start_fp
        } else {
            -1
        };

        state.doc_start_fp = self.doc_start_fp;
        state.pos_start_fp = self.pos_start_fp;
        state.pay_start_fp = self.pay_start_fp;
        state.singleton_doc_id = singleton_doc_id;
        state.skip_offset = skip_offset;
        state.last_pos_block_offset = last_pos_block_offset;
        self.doc_buffer_upto = 0;
        self.pos_buffer_upto = 0;
        self.last_doc_id = 0;
        self.doc_count = 0;
        Ok(())
    }
}

impl<O: IndexOutput> PostingsWriterBase for Lucene50PostingsWriter<O> {
    fn init<D: Directory, DW: Directory, C: Codec>(
        &mut self,
        terms_out: &mut impl IndexOutput,
        state: &SegmentWriteState<D, DW, C>,
    ) -> Result<()> {
        write_index_header(
            terms_out,
            TERMS_CODEC,
            VERSION_CURRENT,
            state.segment_info.get_id(),
            &state.segment_suffix,
        )?;
        terms_out.write_vint(BLOCK_SIZE)
    }

    fn close(&mut self) -> Result<()> {
        write_footer(&mut self.doc_out)?;
        if let Some(ref mut pos_out) = self.pos_out {
            write_footer(pos_out)?;
        }
        if let Some(ref mut pay_out) = self.pay_out {
            write_footer(pay_out)?;
        }
        Ok(())
    }

    fn write_term(
        &mut self,
        _term: &[u8],
        terms: &mut impl TermIterator,
        docs_seen: &mut FixedBitSet,
        doc_freq_limit: i32,
        term_freq_limit: i32,
    ) -> Result<Option<BlockTermState>> {
        self.start_term();
        let mut postings_enum = terms.postings_with_flags(self.enum_flags)?;
        let mut doc_freq = 0;
        let mut total_term_freq = 0i32;
        loop {
            let doc_id = postings_enum.next()?;
            if doc_id == NO_MORE_DOCS {
                break;
            }
            doc_freq += 1;
            docs_seen.set(doc_id as usize);
            let freq = if self.write_freqs {
                let f = postings_enum.freq()?.min(term_freq_limit);
                total_term_freq += f;
                f
            } else {
                -1
            };
            self.start_doc(doc_id, freq)?;

            if self.write_positions {
                for _ in 0..freq {
                    let pos = postings_enum.next_position()?;
                    let payload = if self.write_payloads {
                        postings_enum.payload()?
                    } else {
                        Vec::with_capacity(0)
                    };
                    let (start_offset, end_offset) = if self.write_offsets {
                        (postings_enum.start_offset()?, postings_enum.end_offset()?)
                    } else {
                        (-1, -1)
                    };
                    self.add_position(pos, &payload, start_offset, end_offset)?;
                }
            }

            self.finish_doc();

            if doc_freq > doc_freq_limit {
                break;
            }
        }

        if doc_freq == 0 {
            Ok(None)
        } else {
            let mut st = self.new_term_state();
            st.doc_freq = doc_freq;
            st.total_term_freq = if self.write_freqs {
                total_term_freq as i64
            } else {
                -1
            };
            self.finish_term(&mut st)?;
            Ok(Some(st))
        }
    }

    fn encode_term(
        &mut self,
        longs: &mut [i64],
        out: &mut impl DataOutput,
        _field_info: &FieldInfo,
        state: &BlockTermState,
        absolute: bool,
    ) -> Result<()> {
        if absolute {
            self.last_state = BlockTermState::new();
        }
        longs[0] = state.doc_start_fp - self.last_state.doc_start_fp;
        if self.write_positions {
            longs[1] = state.pos_start_fp - self.last_state.pos_start_fp;
            if self.write_payloads || self.write_offsets {
                longs[2] = state.pay_start_fp - self.last_state.pay_start_fp;
            }
        }

        if state.singleton_doc_id != -1 {
            out.write_vint(state.singleton_doc_id)?;
        }
        if self.write_positions && state.last_pos_block_offset != -1 {
            out.write_vlong(state.last_pos_block_offset)?;
        }
        if state.skip_offset != -1 {
            out.write_vlong(state.skip_offset)?;
        }
        self.last_state = state.clone();
        Ok(())
    }

    fn set_field(&mut self, field_info: &FieldInfo) -> i32 {
        self.set_field_base(field_info);
        self.skip_writer.set_field(
            self.write_positions,
            self.write_offsets,
            self.write_payloads,
        );
        self.last_state = BlockTermState::new();
        if self.write_positions {
            if self.write_payloads || self.write_offsets {
                3 // doc + pos + pay FP
            } else {
                2 // doc + pos FP
            }
        } else {
            1 // doc FP
        }
    }
}
