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
use core::codec::postings::skip_reader::*;
use core::codec::segment_infos::{segment_file_name, SegmentReadState};
use core::codec::{codec_util, Codec};
use core::codec::{PostingIterator, PostingIteratorFlags};
use core::search::{DocIterator, Payload, NO_MORE_DOCS};
use core::store::directory::Directory;
use core::store::io::{DataInput, IndexInput};
use core::util::UnsignedShift;
use core::util::{Bits, DocId, FixedBitSet, ImmutableBitSet};

use error::{ErrorKind::IllegalState, Result};

use core::codec::postings::{PartialBlockDecoder, SIMDBlockDecoder};
use core::util::packed::{EliasFanoDecoder, SIMD128Packer, SIMDPacker, NO_MORE_VALUES};
use std::intrinsics::{likely, unlikely};
use std::sync::Arc;

/// Filename extension for document number, frequencies, and skip data.
/// See chapter: <a href="#Frequencies">Frequencies and Skip Data</a>
pub const DOC_EXTENSION: &str = "doc";

/// Filename extension for positions.
/// See chapter: <a href="#Positions">Positions</a>
pub const POS_EXTENSION: &str = "pos";

/// Filename extension for payloads and offsets.
/// See chapter: <a href="#Payloads">Payloads and Offsets</a>
pub const PAY_EXTENSION: &str = "pay";

/// Expert: The maximum number of skip levels. Smaller values result in
/// slightly smaller indexes, but slower skipping in big posting lists.
pub const MAX_SKIP_LEVELS: usize = 10;

pub const TERMS_CODEC: &str = "Lucene50PostingsWriterTerms";
pub const DOC_CODEC: &str = "Lucene50PostingsWriterDoc";
pub const POS_CODEC: &str = "Lucene50PostingsWriterPos";
pub const PAY_CODEC: &str = "Lucene50PostingsWriterPay";

// Increment version to change it
pub const VERSION_START: i32 = 0;
pub const VERSION_CURRENT: i32 = VERSION_START + 1;

fn clone_option_index_input(input: &Option<Box<dyn IndexInput>>) -> Result<Box<dyn IndexInput>> {
    debug_assert!(input.is_some());
    (*input.as_ref().unwrap()).clone()
}

/// Concrete class that reads docId(maybe frq,pos,offset,payloads) list
/// with postings format.
///
/// @lucene.experimental
pub struct Lucene50PostingsReader {
    doc_in: Box<dyn IndexInput>,
    pos_in: Option<Box<dyn IndexInput>>,
    pay_in: Option<Box<dyn IndexInput>>,
    pub version: i32,
    pub for_util: ForUtil,
    use_simd: bool,
}

impl Lucene50PostingsReader {
    fn clone_pos_in(&self) -> Result<Box<dyn IndexInput>> {
        clone_option_index_input(&self.pos_in)
    }
    fn clone_pay_in(&self) -> Result<Box<dyn IndexInput>> {
        clone_option_index_input(&self.pay_in)
    }
    pub fn open<D: Directory, DW: Directory, C: Codec>(
        state: &SegmentReadState<'_, D, DW, C>,
    ) -> Result<Lucene50PostingsReader> {
        let doc_name = segment_file_name(
            &state.segment_info.name,
            &state.segment_suffix,
            DOC_EXTENSION,
        );
        let mut doc_in = state.directory.open_input(&doc_name, state.context)?;
        let version = codec_util::check_index_header(
            doc_in.as_mut(),
            DOC_CODEC,
            VERSION_START,
            VERSION_CURRENT,
            &state.segment_info.id,
            &state.segment_suffix,
        )?;

        let use_simd = if version > VERSION_START && SIMD128Packer::is_support() {
            true
        } else {
            false
        };

        let for_util = ForUtil::with_input(doc_in.as_mut())?;
        codec_util::retrieve_checksum(doc_in.as_mut())?;
        let mut pos_in = None;
        let mut pay_in = None;
        let doc_in = doc_in;
        if state.field_infos.has_prox {
            let prox_name = segment_file_name(
                &state.segment_info.name,
                &state.segment_suffix,
                POS_EXTENSION,
            );
            let mut input = state.directory.open_input(&prox_name, state.context)?;
            codec_util::check_index_header(
                input.as_mut(),
                POS_CODEC,
                version,
                version,
                &state.segment_info.id,
                &state.segment_suffix,
            )?;
            codec_util::retrieve_checksum(input.as_mut())?;
            pos_in = Some(input);
            if state.field_infos.has_payloads || state.field_infos.has_offsets {
                let pay_name = segment_file_name(
                    &state.segment_info.name,
                    &state.segment_suffix,
                    PAY_EXTENSION,
                );
                input = state.directory.open_input(&pay_name, state.context)?;
                codec_util::check_index_header(
                    input.as_mut(),
                    PAY_CODEC,
                    version,
                    version,
                    &state.segment_info.id,
                    &state.segment_suffix,
                )?;
                codec_util::retrieve_checksum(input.as_mut())?;
                pay_in = Some(input)
            }
        }
        Ok(Lucene50PostingsReader {
            doc_in,
            pos_in,
            pay_in,
            version,
            for_util,
            use_simd,
        })
    }

    pub fn init<D: Directory, DW: Directory, C: Codec>(
        &self,
        terms_in: &mut dyn IndexInput,
        state: &SegmentReadState<'_, D, DW, C>,
    ) -> Result<()> {
        codec_util::check_index_header(
            terms_in,
            TERMS_CODEC,
            VERSION_START,
            VERSION_CURRENT,
            &state.segment_info.id,
            &state.segment_suffix,
        )?;
        let index_block_size = terms_in.read_vint()?;
        if index_block_size != BLOCK_SIZE {
            bail!(IllegalState(format!(
                "index-time BLOCK_SIZE ({}) != read-time BLOCK_SIZE ({})",
                index_block_size, BLOCK_SIZE
            )))
        } else {
            Ok(())
        }
    }

    /// Return a newly created empty TermState
    pub fn new_term_state(&self) -> BlockTermState {
        BlockTermState::new()
    }

    pub fn postings(
        &self,
        field_info: &FieldInfo,
        state: &BlockTermState,
        flags: u16,
    ) -> Result<Lucene50PostingIterator> {
        let options = &field_info.index_options;
        let index_has_positions = options.has_positions();
        let index_has_offsets = options.has_offsets();
        let index_has_payloads = field_info.has_store_payloads;

        if !index_has_positions
            || !PostingIteratorFlags::feature_requested(flags, PostingIteratorFlags::POSITIONS)
        {
            Ok(Lucene50PostingIterator(Lucene50PostingIterEnum::Doc(
                BlockDocIterator::new(
                    self.doc_in.clone()?,
                    field_info,
                    state,
                    flags,
                    self.for_util.clone(),
                    self.use_simd,
                )?,
            )))
        } else if (!index_has_offsets
            || !PostingIteratorFlags::feature_requested(flags, PostingIteratorFlags::OFFSETS))
            && (!index_has_payloads
                || !PostingIteratorFlags::feature_requested(flags, PostingIteratorFlags::PAYLOADS))
        {
            Ok(Lucene50PostingIterator(Lucene50PostingIterEnum::Posting(
                BlockPostingIterator::new(
                    self.doc_in.clone()?,
                    self.clone_pos_in()?,
                    field_info,
                    state,
                    flags,
                    self.for_util.clone(),
                    self.use_simd,
                )?,
            )))
        } else {
            debug_assert!(self.pos_in.is_some());
            debug_assert!(self.pay_in.is_some());
            Ok(Lucene50PostingIterator(
                Lucene50PostingIterEnum::Everything(EverythingIterator::new(
                    self.doc_in.clone()?,
                    self.clone_pos_in()?,
                    self.clone_pay_in()?,
                    field_info,
                    state,
                    flags,
                    self.for_util.clone(),
                    self.use_simd,
                )?),
            ))
        }
    }

    pub fn check_integrity(&self) -> Result<()> {
        //        codec_util::checksum_entire_file(self.doc_in.as_ref())?;
        //
        //        if let Some(ref pos_in) = self.pos_in {
        //            codec_util::checksum_entire_file(pos_in.as_ref())?;
        //        }
        //        if let Some(ref pay_in) = self.pay_in {
        //            codec_util::checksum_entire_file(pay_in.as_ref())?;
        //        }
        Ok(())
    }
}

pub type Lucene50PostingsReaderRef = Arc<Lucene50PostingsReader>;

/// Actually decode metadata for next term
/// @see PostingsWriterBase#encodeTerm
pub fn lucene50_decode_term<T: DataInput + ?Sized>(
    longs: &[i64],
    input: &mut T,
    field_info: &FieldInfo,
    state: &mut BlockTermState,
    absolute: bool,
) -> Result<()> {
    let options = &field_info.index_options;
    let field_has_positions = options.has_positions();
    let field_has_offsets = options.has_offsets();
    let field_has_payloads = field_info.has_store_payloads;
    if absolute {
        state.doc_start_fp = 0;
        state.pos_start_fp = 0;
        state.pay_start_fp = 0;
    };

    state.doc_start_fp += longs[0];
    if field_has_positions {
        state.pos_start_fp += longs[1];
        if field_has_offsets || field_has_payloads {
            state.pay_start_fp += longs[2];
        }
    }
    state.singleton_doc_id = if state.doc_freq == 1 {
        input.read_vint()?
    } else {
        -1
    };
    if field_has_positions {
        state.last_pos_block_offset = if state.total_term_freq > i64::from(BLOCK_SIZE) {
            input.read_vlong()?
        } else {
            -1
        }
    }
    state.skip_offset = if state.doc_freq > BLOCK_SIZE {
        input.read_vlong()?
    } else {
        -1
    };
    Ok(())
}

fn read_vint_block(
    doc_in: &mut dyn IndexInput,
    doc_buffer: &mut [i32],
    freq_buffer: &mut [i32],
    num: i32,
    index_has_freq: bool,
) -> Result<()> {
    let num = num as usize;
    if index_has_freq {
        for i in 0..num {
            let code = doc_in.read_vint()? as u32;
            doc_buffer[i] = (code >> 1) as i32;
            if (code & 1) != 0 {
                freq_buffer[i] = 1;
            } else {
                freq_buffer[i] = doc_in.read_vint()?;
            }
        }
    } else {
        for item in doc_buffer.iter_mut().take(num) {
            *item = doc_in.read_vint()?;
        }
    }

    Ok(())
}

#[derive(Debug, Copy, Clone)]
pub enum EncodeType {
    PF,
    EF,
    BITSET,
    FULL,
}

struct BlockDocIterator {
    encoded: [u8; MAX_ENCODED_SIZE],

    doc_delta_buffer: [i32; MAX_DATA_SIZE],
    freq_buffer: [i32; MAX_DATA_SIZE],

    doc_buffer_upto: i32,

    pub skipper: Option<Lucene50SkipReader>,
    skipped: bool,

    start_doc_in: Box<dyn IndexInput>,

    doc_in: Option<Box<dyn IndexInput>>,
    index_has_freq: bool,
    index_has_pos: bool,
    pub index_has_offsets: bool,
    index_has_payloads: bool,

    /// number of docs in this posting list
    doc_freq: i32,
    /// sum of freqs in this posting list (or docFreq when omitted)
    total_term_freq: i64,
    /// how many docs we've read
    doc_upto: i32,
    /// doc we last read
    doc: DocId,
    /// accumulator for doc deltas
    accum: i32,
    /// freq we last read
    pub freq: i32,

    /// Where this term's postings start in the .doc file:
    doc_term_start_fp: i64,

    /// Where this term's skip data starts (after
    /// docTermStartFP) in the .doc file (or -1 if there is
    /// no skip data for this term):
    skip_offset: i64,

    /// docID for next skip point, we won't use skipper if
    /// target docID is not larger than this
    next_skip_doc: i32,

    /// true if the caller actually needs frequencies
    needs_freq: bool,
    /// docid when there is a single pulsed posting, otherwise -1
    singleton_doc_id: DocId,

    for_util: ForUtil,
    /// PF/EF/BITSET/FULL
    encode_type: EncodeType,
    ef_decoder: Option<EliasFanoDecoder>,
    ef_base_doc: DocId,
    ef_base_total: i32,
    doc_bits: FixedBitSet,
    bits_min_doc: DocId,
    bits_index: i32,
    partial_decode: bool,
    #[allow(dead_code)]
    partial_doc_deltas: PartialBlockDecoder,
    #[allow(dead_code)]
    partial_freqs: PartialBlockDecoder,
    use_simd: bool,
}

impl BlockDocIterator {
    pub fn new(
        start_doc_in: Box<dyn IndexInput>,
        field_info: &FieldInfo,
        term_state: &BlockTermState,
        flags: u16,
        for_util: ForUtil,
        use_simd: bool,
    ) -> Result<BlockDocIterator> {
        let options = &field_info.index_options;
        let mut iterator = BlockDocIterator {
            encoded: [0u8; MAX_ENCODED_SIZE],
            start_doc_in,
            doc_delta_buffer: [0i32; MAX_DATA_SIZE],
            freq_buffer: [0i32; MAX_DATA_SIZE],
            doc_buffer_upto: 0,
            skipped: false,
            skipper: None,
            doc_in: None,
            doc_freq: 0,
            total_term_freq: 0,
            doc_upto: 0,
            doc: 0,
            accum: 0,
            freq: 0,
            doc_term_start_fp: 0,
            skip_offset: 0,
            next_skip_doc: 0,
            needs_freq: false,
            singleton_doc_id: 0,
            index_has_freq: options.has_freqs(),
            index_has_pos: options.has_positions(),
            index_has_offsets: options.has_offsets(),
            index_has_payloads: field_info.has_store_payloads,
            for_util,
            encode_type: EncodeType::PF,
            ef_decoder: None,
            ef_base_doc: -1,
            ef_base_total: 0,
            doc_bits: FixedBitSet::default(),
            bits_min_doc: 0,
            bits_index: 0,
            partial_decode: false,
            partial_doc_deltas: PartialBlockDecoder::new(),
            partial_freqs: PartialBlockDecoder::new(),
            use_simd,
        };
        iterator.reset(term_state, flags)?;
        Ok(iterator)
    }

    pub fn reset(&mut self, term_state: &BlockTermState, flags: u16) -> Result<()> {
        self.doc_freq = term_state.doc_freq;
        self.total_term_freq = if self.index_has_freq {
            term_state.total_term_freq
        } else {
            i64::from(self.doc_freq)
        };
        self.doc_term_start_fp = term_state.doc_start_fp;
        self.skip_offset = term_state.skip_offset;
        self.singleton_doc_id = term_state.singleton_doc_id;
        if self.doc_freq > 1 {
            if self.doc_in.is_none() {
                // lazy init
                self.doc_in = Some(self.start_doc_in.clone()?);
            }
            let start = self.doc_term_start_fp;
            self.doc_in.as_mut().unwrap().seek(start)?;
        }

        self.doc = -1;
        self.needs_freq =
            PostingIteratorFlags::feature_requested(flags, PostingIteratorFlags::FREQS);
        if !self.index_has_freq || !self.needs_freq {
            self.freq_buffer.iter_mut().map(|x| *x = 1).count();
        }
        self.accum = 0;
        self.doc_upto = 0;
        self.next_skip_doc = BLOCK_SIZE - 1; // we won't skip if target is found in first block
        self.doc_buffer_upto = BLOCK_SIZE;
        self.skipped = false;

        self.encode_type = EncodeType::PF;
        self.ef_decoder = None;
        self.ef_base_doc = -1;
        self.ef_base_total = 0;
        self.doc_bits.clear_all();
        self.bits_index = 0;

        Ok(())
    }

    fn refill_docs(&mut self) -> Result<()> {
        // EF & PF compatible
        if self.accum > 0 {
            self.ef_base_doc = self.accum;
        }
        self.ef_base_total = self.doc_upto;
        self.encode_type = EncodeType::PF;
        self.bits_index = 0;
        self.partial_decode = false;

        let left = self.doc_freq - self.doc_upto;
        debug_assert!(left > 0);
        if left >= BLOCK_SIZE {
            self.partial_decode = true;
            let doc_in = self.doc_in.as_mut().unwrap();
            self.for_util.read_block(
                doc_in.as_mut(),
                &mut self.encoded,
                &mut self.doc_delta_buffer,
                Some(&mut self.encode_type),
                self.use_simd,
            )?;

            ForUtil::read_other_encode_block(
                doc_in.as_mut(),
                &mut self.ef_decoder,
                &self.encode_type,
                &mut self.doc_bits,
                &mut self.bits_min_doc,
            )?;

            if self.index_has_freq {
                if self.needs_freq {
                    self.for_util.read_block(
                        doc_in.as_mut(),
                        &mut self.encoded,
                        &mut self.freq_buffer,
                        None,
                        self.use_simd,
                    )?;
                } else {
                    self.for_util.skip_block(doc_in.as_mut())?; // skip over freqs
                }
            }
        } else if self.doc_freq == 1 {
            self.doc_delta_buffer[0] = self.singleton_doc_id;
            self.freq_buffer[0] = self.total_term_freq as i32;
        } else {
            let doc_in = self.doc_in.as_mut().unwrap();
            // Read vInts:
            read_vint_block(
                doc_in.as_mut(),
                &mut self.doc_delta_buffer,
                &mut self.freq_buffer,
                left,
                self.index_has_freq,
            )?;
        }
        self.doc_buffer_upto = 0;
        Ok(())
    }

    #[allow(dead_code)]
    #[inline(always)]
    fn decode_current_doc_delta(&mut self) -> i32 {
        if likely(self.partial_decode) {
            self.partial_doc_deltas.next()
        } else {
            self.doc_delta_buffer[self.doc_buffer_upto as usize]
        }
    }

    #[allow(dead_code)]
    #[inline(always)]
    fn decode_current_freq(&mut self) -> i32 {
        if likely(self.partial_decode && self.needs_freq) {
            self.partial_freqs.get(self.doc_buffer_upto as usize)
        } else {
            self.freq_buffer[self.doc_buffer_upto as usize]
        }
    }
}

impl PostingIterator for BlockDocIterator {
    fn freq(&self) -> Result<i32> {
        debug_assert!(self.freq > 0);
        Ok(self.freq)
    }

    fn next_position(&mut self) -> Result<i32> {
        Ok(-1)
    }

    fn start_offset(&self) -> Result<i32> {
        Ok(-1)
    }

    fn end_offset(&self) -> Result<i32> {
        Ok(-1)
    }

    fn payload(&self) -> Result<Payload> {
        Ok(Payload::new())
    }
}

impl DocIterator for BlockDocIterator {
    fn doc_id(&self) -> DocId {
        self.doc
    }

    fn next(&mut self) -> Result<DocId> {
        if self.doc_upto == self.doc_freq {
            self.doc = NO_MORE_DOCS;
            return Ok(self.doc);
        }
        if self.doc_buffer_upto == BLOCK_SIZE {
            self.refill_docs()?;
        }

        // set doc id
        self.doc = match self.encode_type {
            EncodeType::PF => self.accum + self.doc_delta_buffer[self.doc_buffer_upto as usize],
            EncodeType::EF => {
                self.ef_decoder.as_mut().unwrap().next_value() as i32 + 1 + self.ef_base_doc
            }

            EncodeType::BITSET => {
                self.bits_index = self.doc_bits.next_set_bit(self.bits_index as usize);
                let doc = self.bits_min_doc + self.bits_index;
                self.bits_index += 1;
                doc
            }

            _ => {
                unimplemented!();
            }
        };

        self.accum = self.doc;
        self.doc_upto += 1;

        // set doc freq
        self.freq = self.freq_buffer[self.doc_buffer_upto as usize];
        self.doc_buffer_upto += 1;
        Ok(self.doc)
    }

    fn advance(&mut self, target: DocId) -> Result<DocId> {
        if unlikely(target == NO_MORE_DOCS) {
            self.doc = NO_MORE_DOCS;
            return Ok(self.doc);
        }
        // TODO: make frq block load lazy/skippable
        // current skip docID < docIDs generated from current buffer <= next skip docID
        // we don't need to skip if target is buffered already
        if self.doc_freq > BLOCK_SIZE && target > self.next_skip_doc {
            if self.skipper.is_none() {
                // Lazy init: first time this enum has ever been used for skipping
                self.skipper = Some(Lucene50SkipReader::new(
                    clone_option_index_input(&self.doc_in)?,
                    MAX_SKIP_LEVELS,
                    self.index_has_pos,
                    self.index_has_offsets,
                    self.index_has_payloads,
                ));
            }

            let skipper = self.skipper.as_mut().unwrap();

            if !self.skipped {
                debug_assert_ne!(self.skip_offset, -1);
                // This is the first time this enum has skipped
                // since reset() was called; load the skip data:
                skipper.init(
                    self.doc_term_start_fp + self.skip_offset,
                    self.doc_term_start_fp,
                    0,
                    0,
                    self.doc_freq,
                )?;
                self.skipped = true;
            }

            // always plus one to fix the result, since skip position in Lucene50SkipReader
            // is a little different from MultiLevelSkipListReader
            let new_doc_upto = skipper.skip_to(target)? + 1;

            if new_doc_upto > self.doc_upto {
                // Skipper moved
                debug_assert_eq!(new_doc_upto % BLOCK_SIZE, 0);
                self.doc_upto = new_doc_upto;

                // Force to read next block
                self.doc_buffer_upto = BLOCK_SIZE;
                self.accum = skipper.doc(); // actually, this is just lastSkipEntry
                self.doc_in.as_mut().unwrap().seek(skipper.doc_pointer())?; // now point to the
                                                                            // block we want to
                                                                            // search
            }
            // next time we call advance, this is used to
            // foresee whether skipper is necessary.
            self.next_skip_doc = skipper.next_skip_doc();
        }
        if self.doc_upto == self.doc_freq {
            self.doc = NO_MORE_DOCS;
            return Ok(self.doc);
        }
        if self.doc_buffer_upto == BLOCK_SIZE {
            self.refill_docs()?;
        }

        self.doc = match self.encode_type {
            EncodeType::PF => {
                // Now scan... this is an inlined/pared down version
                // of nextDoc():
                loop {
                    self.accum += self.doc_delta_buffer[self.doc_buffer_upto as usize];
                    self.doc_upto += 1;

                    if self.accum >= target {
                        break;
                    }
                    self.doc_buffer_upto += 1;
                    if self.doc_upto == self.doc_freq {
                        self.doc = NO_MORE_DOCS;
                        return Ok(self.doc);
                    }
                }
                self.accum
            }

            EncodeType::EF => {
                let decoder = self.ef_decoder.as_mut().unwrap();
                let doc = decoder.advance_to_value((target - 1 - self.ef_base_doc) as i64);
                if doc == NO_MORE_VALUES {
                    self.doc = NO_MORE_DOCS;
                    return Ok(self.doc);
                }
                self.doc_buffer_upto = decoder.current_index()? as i32;
                self.doc_upto = self.ef_base_total + self.doc_buffer_upto + 1;
                self.accum = doc as i32 + 1 + self.ef_base_doc;
                self.accum
            }

            EncodeType::BITSET => {
                if target < self.bits_min_doc {
                    self.accum = self.bits_min_doc;
                    self.bits_index = 1;
                    self.doc_buffer_upto = 0;
                } else {
                    let mut index = target - self.bits_min_doc;
                    if index >= self.doc_bits.num_bits as i32 {
                        self.doc = NO_MORE_DOCS;
                        return Ok(NO_MORE_DOCS);
                    }
                    let find = self.doc_bits.get(index as usize)?;
                    self.accum = if find {
                        target
                    } else {
                        index = self.doc_bits.next_set_bit(index as usize);
                        if index == NO_MORE_DOCS {
                            self.doc = NO_MORE_DOCS;
                            return Ok(NO_MORE_DOCS);
                        }
                        self.bits_min_doc + index
                    };
                    self.doc_buffer_upto = self.doc_bits.count_ones_before_index2(
                        self.doc_buffer_upto,
                        self.bits_index as usize,
                        index as usize,
                    ) as i32;
                    // self.doc_buffer_upto =
                    // self.doc_bits.count_ones_before_index(index as usize) as i32;
                    self.bits_index = index + 1;
                }
                self.doc_upto = self.ef_base_total + self.doc_buffer_upto + 1;
                self.accum
            }

            _ => {
                unimplemented!();
            }
        };

        self.freq = self.freq_buffer[self.doc_buffer_upto as usize];
        self.doc_buffer_upto += 1;
        Ok(self.doc)
    }

    fn cost(&self) -> usize {
        self.doc_freq as usize
    }
}

struct SIMDBlockDocIterator {
    doc_iter: BlockDocIterator,
    simd_doc_deltas: SIMDBlockDecoder,
    total_base: i32,
}

impl SIMDBlockDocIterator {
    #[allow(dead_code)]
    pub fn new(
        start_doc_in: Box<dyn IndexInput>,
        field_info: &FieldInfo,
        term_state: &BlockTermState,
        flags: u16,
        for_util: ForUtil,
    ) -> Result<Self> {
        Ok(Self {
            doc_iter: BlockDocIterator::new(
                start_doc_in,
                field_info,
                term_state,
                flags,
                for_util,
                false,
            )?,
            simd_doc_deltas: SIMDBlockDecoder::new(),
            total_base: 0,
        })
    }

    fn refill_docs(&mut self) -> Result<()> {
        self.doc_iter.partial_decode = false;

        let left = self.doc_iter.doc_freq - self.doc_iter.doc_upto;
        debug_assert!(left > 0);
        if left >= BLOCK_SIZE {
            self.doc_iter.partial_decode = true;
            let doc_in = self.doc_iter.doc_in.as_mut().unwrap();
            self.doc_iter
                .for_util
                .read_block_by_simd(doc_in.as_mut(), &mut self.simd_doc_deltas)?;

            if self.doc_iter.index_has_freq {
                if self.doc_iter.needs_freq {
                    self.doc_iter.for_util.read_block_only(
                        doc_in.as_mut(),
                        &mut self.doc_iter.encoded,
                        &mut self.doc_iter.freq_buffer,
                        None,
                        &mut self.doc_iter.partial_freqs,
                    )?;
                } else {
                    self.doc_iter.for_util.skip_block(doc_in.as_mut())?; // skip over freqs
                }
            }
        } else if self.doc_iter.doc_freq == 1 {
            self.doc_iter.doc_delta_buffer[0] = self.doc_iter.singleton_doc_id;
            self.doc_iter.freq_buffer[0] = self.doc_iter.total_term_freq as i32;
        } else {
            let doc_in = self.doc_iter.doc_in.as_mut().unwrap();
            // Read vInts:
            read_vint_block(
                doc_in.as_mut(),
                &mut self.doc_iter.doc_delta_buffer,
                &mut self.doc_iter.freq_buffer,
                left,
                self.doc_iter.index_has_freq,
            )?;
        }
        self.doc_iter.doc_buffer_upto = 0;
        Ok(())
    }
}

impl DocIterator for SIMDBlockDocIterator {
    fn doc_id(&self) -> DocId {
        self.doc_iter.doc_id()
    }

    fn next(&mut self) -> Result<DocId> {
        if unlikely(self.doc_iter.doc_upto == self.doc_iter.doc_freq) {
            self.doc_iter.doc = NO_MORE_DOCS;
            return Ok(self.doc_iter.doc);
        }
        if unlikely(self.doc_iter.doc_buffer_upto == BLOCK_SIZE) {
            if unlikely(self.doc_iter.doc < 0) {
                self.doc_iter.accum = 0;
                self.total_base = 0;
                self.simd_doc_deltas.reset_delta_base(0);
            } else {
                self.doc_iter.accum = self.doc_iter.doc;
                self.total_base = self.doc_iter.doc_upto;
                self.simd_doc_deltas.reset_delta_base(self.doc_iter.accum);
            }
            self.refill_docs()?;
        }
        self.doc_iter.doc = if likely(self.doc_iter.partial_decode) {
            self.doc_iter.accum = self.simd_doc_deltas.next();
            self.doc_iter.accum
        } else {
            self.doc_iter.accum = self.doc_iter.accum
                + self.doc_iter.doc_delta_buffer[self.doc_iter.doc_buffer_upto as usize];
            self.doc_iter.accum
        };

        self.doc_iter.freq = self.doc_iter.decode_current_freq();
        self.doc_iter.doc_buffer_upto += 1;
        self.doc_iter.doc_upto += 1;
        Ok(self.doc_iter.doc)
    }

    fn advance(&mut self, target: i32) -> Result<DocId> {
        if unlikely(target == NO_MORE_DOCS) {
            self.doc_iter.doc = NO_MORE_DOCS;
            return Ok(self.doc_iter.doc);
        }
        // TODO: make frq block load lazy/skippable
        // current skip docID < docIDs generated from current buffer <= next skip docID
        // we don't need to skip if target is buffered already
        if self.doc_iter.doc_freq > BLOCK_SIZE && target > self.doc_iter.next_skip_doc {
            if self.doc_iter.skipper.is_none() {
                // Lazy init: first time this enum has ever been used for skipping
                self.doc_iter.skipper = Some(Lucene50SkipReader::new(
                    clone_option_index_input(&self.doc_iter.doc_in)?,
                    MAX_SKIP_LEVELS,
                    self.doc_iter.index_has_pos,
                    self.doc_iter.index_has_offsets,
                    self.doc_iter.index_has_payloads,
                ));
            }

            let skipper = self.doc_iter.skipper.as_mut().unwrap();

            if !self.doc_iter.skipped {
                debug_assert_ne!(self.doc_iter.skip_offset, -1);
                // This is the first time this enum has skipped
                // since reset() was called; load the skip data:
                skipper.init(
                    self.doc_iter.doc_term_start_fp + self.doc_iter.skip_offset,
                    self.doc_iter.doc_term_start_fp,
                    0,
                    0,
                    self.doc_iter.doc_freq,
                )?;
                self.doc_iter.skipped = true;
            }

            // always plus one to fix the result, since skip position in Lucene50SkipReader
            // is a little different from MultiLevelSkipListReader
            let new_doc_upto = skipper.skip_to(target)? + 1;

            if new_doc_upto >= self.doc_iter.doc_upto {
                // Skipper moved
                debug_assert_eq!(new_doc_upto % BLOCK_SIZE, 0);
                self.doc_iter.doc_upto = new_doc_upto;

                // Force to read next block
                self.doc_iter.doc_buffer_upto = BLOCK_SIZE;
                self.doc_iter.accum = skipper.doc(); // actually, this is just lastSkipEntry
                self.doc_iter
                    .doc_in
                    .as_mut()
                    .unwrap()
                    .seek(skipper.doc_pointer())?; // now point to the
                                                   // block we want to
                                                   // search
            }
            // next time we call advance, this is used to
            // foresee whether skipper is necessary.
            self.doc_iter.next_skip_doc = skipper.next_skip_doc();
        }
        if self.doc_iter.doc_upto == self.doc_iter.doc_freq {
            self.doc_iter.doc = NO_MORE_DOCS;
            return Ok(self.doc_iter.doc);
        }

        if unlikely(self.doc_iter.doc_buffer_upto == BLOCK_SIZE) {
            if unlikely(self.doc_iter.accum <= 0) {
                // self.doc_iter.accum = 0;
                self.total_base = 0;
                self.simd_doc_deltas.reset_delta_base(0);
            } else {
                // self.doc_iter.accum = self.doc_iter.skipper.as_ref().unwrap().doc();
                self.total_base = self.doc_iter.doc_upto;
                self.simd_doc_deltas.reset_delta_base(self.doc_iter.accum);
            }
            self.refill_docs()?;
        }

        if likely(self.doc_iter.partial_decode) {
            // let (doc, pos) = self.simd_doc_deltas.advance_by_binary_search(target);
            let (doc, pos) = self.simd_doc_deltas.advance(target);
            self.doc_iter.doc = doc;
            if unlikely(doc == NO_MORE_DOCS) {
                return Ok(NO_MORE_DOCS);
            }
            self.doc_iter.doc_buffer_upto = pos as i32;
            self.doc_iter.doc_upto = self.total_base + pos as i32 + 1;
        } else {
            loop {
                self.doc_iter.accum +=
                    self.doc_iter.doc_delta_buffer[self.doc_iter.doc_buffer_upto as usize];
                self.doc_iter.doc_upto += 1;

                if self.doc_iter.accum >= target {
                    break;
                }
                self.doc_iter.doc_buffer_upto += 1;
                if self.doc_iter.doc_upto == self.doc_iter.doc_freq {
                    self.doc_iter.doc = NO_MORE_DOCS;
                    return Ok(self.doc_iter.doc);
                }
            }
            self.doc_iter.doc = self.doc_iter.accum;
        }
        self.doc_iter.freq = self.doc_iter.decode_current_freq();
        self.doc_iter.doc_buffer_upto += 1;
        Ok(self.doc_iter.doc)
    }

    fn cost(&self) -> usize {
        self.doc_iter.cost()
    }
}

impl PostingIterator for SIMDBlockDocIterator {
    fn freq(&self) -> Result<i32> {
        self.doc_iter.freq()
    }

    fn next_position(&mut self) -> Result<i32> {
        self.doc_iter.next_position()
    }

    fn start_offset(&self) -> Result<i32> {
        self.doc_iter.start_offset()
    }

    fn end_offset(&self) -> Result<i32> {
        self.doc_iter.end_offset()
    }

    fn payload(&self) -> Result<Payload> {
        self.doc_iter.payload()
    }
}

struct BlockPostingIterator {
    encoded: [u8; MAX_ENCODED_SIZE],

    doc_delta_buffer: [i32; MAX_DATA_SIZE],
    freq_buffer: [i32; MAX_DATA_SIZE],
    pos_delta_buffer: [i32; MAX_DATA_SIZE],

    doc_buffer_upto: i32,
    pos_buffer_upto: i32,

    skipper: Option<Lucene50SkipReader>,
    skipped: bool,

    start_doc_in: Box<dyn IndexInput>,

    doc_in: Option<Box<dyn IndexInput>>,
    pos_in: Box<dyn IndexInput>,

    index_has_pos: bool,
    index_has_offsets: bool,
    index_has_payloads: bool,

    /// number of docs in this posting list
    doc_freq: i32,
    /// number of positions in this posting list
    total_term_freq: i64,
    /// how many docs we've read
    doc_upto: i32,
    /// doc we last read
    doc: DocId,
    /// accumulator for doc deltas
    accum: i32,
    /// freq we last read
    freq: i32,
    /// current position
    position: i32,

    /// how many positions "behind" we are; nextPosition must
    /// skip these to "catch up":
    pos_pending_count: i32,

    /// Lazy pos seek: if != -1 then we must seek to this FP
    /// before reading positions:
    pos_pending_fp: i64,

    /// Where this term's postings start in the .doc file:
    doc_term_start_fp: i64,

    /// Where this term's postings start in the .pos file:
    pos_term_start_fp: i64,

    /// Where this term's payloads/offsets start in the .pay
    /// file:
    pay_term_start_fp: i64,

    /// File pointer where the last (vInt encoded) pos delta
    /// block is.  We need this to know whether to bulk
    /// decode vs vInt decode the block:
    last_pos_block_fp: i64,

    /// Where this term's skip data starts (after
    /// docTermStartFP) in the .doc file (or -1 if there is
    /// no skip data for this term):
    skip_offset: i64,

    next_skip_doc: i32,

    /// docid when there is a single pulsed posting, otherwise -1
    singleton_doc_id: i32,

    for_util: ForUtil,
    /// PF/EF/BITSET/FULL
    encode_type: EncodeType,
    ef_decoder: Option<EliasFanoDecoder>,
    ef_base_doc: DocId,
    ef_base_total: i32,
    doc_bits: FixedBitSet,
    bits_min_doc: DocId,
    bits_index: i32,
    use_simd: bool,
}

impl BlockPostingIterator {
    pub fn new(
        start_doc_in: Box<dyn IndexInput>,
        pos_in: Box<dyn IndexInput>,
        field_info: &FieldInfo,
        term_state: &BlockTermState,
        _flags: u16,
        for_util: ForUtil,
        use_simd: bool,
    ) -> Result<BlockPostingIterator> {
        let options = &field_info.index_options;
        let mut iterator = BlockPostingIterator {
            encoded: [0; MAX_ENCODED_SIZE],
            start_doc_in,
            doc_delta_buffer: [0i32; MAX_DATA_SIZE],
            freq_buffer: [0i32; MAX_DATA_SIZE],
            pos_delta_buffer: [0i32; MAX_DATA_SIZE],
            doc_buffer_upto: 0,
            pos_buffer_upto: 0,
            skipped: false,
            skipper: None,
            doc_in: None,
            doc_freq: 0,
            pos_in,
            total_term_freq: 0,
            doc_upto: 0,
            doc: 0,
            accum: 0,
            freq: 0,
            position: 0,
            pos_pending_count: 0,
            pos_pending_fp: 0,
            doc_term_start_fp: 0,
            pos_term_start_fp: 0,
            pay_term_start_fp: 0,
            last_pos_block_fp: 0,
            skip_offset: 0,
            next_skip_doc: 0,
            singleton_doc_id: 0,
            index_has_pos: options.has_positions(),
            index_has_offsets: options.has_offsets(),
            index_has_payloads: field_info.has_store_payloads,
            for_util,
            encode_type: EncodeType::PF,
            ef_decoder: None,
            ef_base_doc: -1,
            ef_base_total: 0,
            doc_bits: FixedBitSet::default(),
            bits_min_doc: 0,
            bits_index: 0,
            use_simd,
        };
        iterator.reset(term_state)?;
        Ok(iterator)
    }

    pub fn reset(&mut self, term_state: &BlockTermState) -> Result<()> {
        self.doc_freq = term_state.doc_freq;
        self.doc_term_start_fp = term_state.doc_start_fp;
        self.pos_term_start_fp = term_state.pos_start_fp;
        self.pay_term_start_fp = term_state.pay_start_fp;
        self.skip_offset = term_state.skip_offset;
        self.total_term_freq = term_state.total_term_freq;
        self.singleton_doc_id = term_state.singleton_doc_id;
        if self.doc_freq > 1 {
            if self.doc_in.is_none() {
                // lazy init
                self.doc_in = Some(self.start_doc_in.clone()?);
            }
            let start = self.doc_term_start_fp;
            if let Some(ref mut doc_in) = self.doc_in {
                doc_in.seek(start)?;
            }
        }
        self.pos_pending_fp = self.pos_term_start_fp;
        self.pos_pending_count = 0;
        self.last_pos_block_fp = if term_state.total_term_freq < i64::from(BLOCK_SIZE) {
            self.pos_term_start_fp
        } else if term_state.total_term_freq == i64::from(BLOCK_SIZE) {
            -1
        } else {
            self.pos_term_start_fp + term_state.last_pos_block_offset
        };

        self.doc = -1;
        self.accum = 0;
        self.doc_upto = 0;
        self.next_skip_doc = if self.doc_freq > BLOCK_SIZE {
            // we won't skip if target is found in first block
            BLOCK_SIZE - 1
        } else {
            // not enough docs for skipping
            NO_MORE_DOCS
        };
        self.doc_buffer_upto = BLOCK_SIZE;
        self.skipped = false;

        self.encode_type = EncodeType::PF;
        self.ef_decoder = None;
        self.ef_base_doc = -1;
        self.ef_base_total = 0;
        // self.doc_bits.clear_batch(0, self.doc_bits.len());
        self.doc_bits.clear_all();
        self.bits_index = 0;

        Ok(())
    }

    fn refill_docs(&mut self) -> Result<()> {
        // EF & PF compatible
        if self.accum > 0 {
            self.ef_base_doc = self.accum;
        }
        self.ef_base_total = self.doc_upto;
        self.encode_type = EncodeType::PF;
        self.bits_index = 0;

        let left = self.doc_freq - self.doc_upto;
        if left >= BLOCK_SIZE {
            let doc_in = self.doc_in.as_mut().unwrap();
            self.for_util.read_block(
                doc_in.as_mut(),
                &mut self.encoded,
                &mut self.doc_delta_buffer,
                Some(&mut self.encode_type),
                self.use_simd,
            )?;

            ForUtil::read_other_encode_block(
                doc_in.as_mut(),
                &mut self.ef_decoder,
                &self.encode_type,
                &mut self.doc_bits,
                &mut self.bits_min_doc,
            )?;

            self.for_util.read_block(
                doc_in.as_mut(),
                &mut self.encoded,
                &mut self.freq_buffer,
                None,
                self.use_simd,
            )?;
        } else if self.doc_freq == 1 {
            self.doc_delta_buffer[0] = self.singleton_doc_id;
            self.freq_buffer[0] = self.total_term_freq as i32;
        } else {
            // Read vInts:
            let doc_in = self.doc_in.as_mut().unwrap();
            read_vint_block(
                doc_in.as_mut(),
                &mut self.doc_delta_buffer,
                &mut self.freq_buffer,
                left,
                true,
            )?;
        }
        self.doc_buffer_upto = 0;
        Ok(())
    }

    fn refill_positions(&mut self) -> Result<()> {
        let pos_in = &mut self.pos_in;
        if pos_in.file_pointer() == self.last_pos_block_fp {
            let count = (self.total_term_freq % i64::from(BLOCK_SIZE)) as usize;
            let mut payload_length = 0;
            for i in 0..count {
                let code = pos_in.read_vint()?;
                if self.index_has_payloads {
                    if (code & 1) != 0 {
                        payload_length = pos_in.read_vint()?;
                    }
                    self.pos_delta_buffer[i] = code.unsigned_shift(1);
                    if payload_length != 0 {
                        let fp = pos_in.file_pointer() + i64::from(payload_length);
                        pos_in.seek(fp)?;
                    }
                } else {
                    self.pos_delta_buffer[i] = code;
                }
                if self.index_has_offsets && pos_in.read_vint()? & 1 != 0 {
                    // offset length changed
                    pos_in.read_vint()?;
                }
            }
        } else {
            self.for_util.read_block(
                pos_in.as_mut(),
                &mut self.encoded,
                &mut self.pos_delta_buffer,
                None,
                self.use_simd,
            )?;
        }

        Ok(())
    }

    // TODO: in theory we could avoid loading frq block
    // when not needed, ie, use skip data to load how far to
    // seek the pos pointer ... instead of having to load frq
    // blocks only to sum up how many positions to skip
    fn skip_positions(&mut self) -> Result<()> {
        // Skip positions now:
        let mut to_skip = self.pos_pending_count - self.freq;

        let left_in_block = BLOCK_SIZE - self.pos_buffer_upto;
        if to_skip < left_in_block {
            self.pos_buffer_upto += to_skip;
        } else {
            to_skip -= left_in_block;
            {
                let pos_in = &mut self.pos_in;
                while to_skip >= BLOCK_SIZE {
                    debug_assert!(pos_in.file_pointer() != self.last_pos_block_fp);
                    self.for_util.skip_block(pos_in.as_mut())?;
                    to_skip -= BLOCK_SIZE;
                }
            }
            self.refill_positions()?;
            self.pos_buffer_upto = to_skip;
        }

        self.position = 0;
        Ok(())
    }
}

impl PostingIterator for BlockPostingIterator {
    fn freq(&self) -> Result<i32> {
        Ok(self.freq)
    }

    fn next_position(&mut self) -> Result<i32> {
        debug_assert!(self.pos_pending_count > 0);
        if self.pos_pending_fp != -1 {
            self.pos_in.seek(self.pos_pending_fp)?;
            self.pos_pending_fp = -1;

            // Force buffer refill:
            self.pos_buffer_upto = BLOCK_SIZE;
        }

        if self.pos_pending_count > self.freq {
            self.skip_positions()?;
            self.pos_pending_count = self.freq;
        }

        if self.pos_buffer_upto == BLOCK_SIZE {
            self.refill_positions()?;
            self.pos_buffer_upto = 0;
        }
        self.position += self.pos_delta_buffer[self.pos_buffer_upto as usize];
        self.pos_buffer_upto += 1;
        self.pos_pending_count -= 1;
        Ok(self.position)
    }

    fn start_offset(&self) -> Result<i32> {
        Ok(-1)
    }

    fn end_offset(&self) -> Result<i32> {
        Ok(-1)
    }

    fn payload(&self) -> Result<Payload> {
        Ok(Payload::new())
    }
}

impl DocIterator for BlockPostingIterator {
    fn doc_id(&self) -> DocId {
        self.doc
    }

    fn next(&mut self) -> Result<DocId> {
        if self.doc_upto == self.doc_freq {
            self.doc = NO_MORE_DOCS;
            return Ok(self.doc);
        }
        if self.doc_buffer_upto == BLOCK_SIZE {
            self.refill_docs()?;
        }
        // set doc id
        self.doc = match self.encode_type {
            EncodeType::PF => self.accum + self.doc_delta_buffer[self.doc_buffer_upto as usize],

            EncodeType::EF => {
                self.ef_decoder.as_mut().unwrap().next_value() as i32 + 1 + self.ef_base_doc
            }

            EncodeType::BITSET => {
                self.bits_index = self.doc_bits.next_set_bit(self.bits_index as usize);
                let doc = self.bits_min_doc + self.bits_index;
                self.bits_index += 1;
                doc
            }

            _ => {
                unimplemented!();
            }
        };

        self.accum = self.doc;

        self.freq = self.freq_buffer[self.doc_buffer_upto as usize];
        self.pos_pending_count += self.freq;
        self.doc_buffer_upto += 1;
        self.doc_upto += 1;

        self.position = 0;
        Ok(self.doc)
    }

    fn advance(&mut self, target: DocId) -> Result<i32> {
        if unlikely(target == NO_MORE_DOCS) {
            self.doc = NO_MORE_DOCS;
            return Ok(self.doc);
        }
        // TODO: make frq block load lazy/skippable

        if target > self.next_skip_doc {
            if self.skipper.is_none() {
                // Lazy init: first time this enum has ever been used for skipping
                self.skipper = Some(Lucene50SkipReader::new(
                    clone_option_index_input(&self.doc_in)?,
                    MAX_SKIP_LEVELS,
                    self.index_has_pos,
                    self.index_has_offsets,
                    self.index_has_payloads,
                ));
            }

            let skipper = self.skipper.as_mut().unwrap();

            if !self.skipped {
                // This is the first time this enum has skipped
                // since reset() was called; load the skip data:
                skipper.init(
                    self.doc_term_start_fp + self.skip_offset,
                    self.doc_term_start_fp,
                    self.pos_term_start_fp,
                    self.pay_term_start_fp,
                    self.doc_freq,
                )?;
                self.skipped = true;
            }

            // always plus one to fix the result, since skip position in Lucene50SkipReader
            // is a little different from MultiLevelSkipListReader
            let new_doc_upto: i32 = skipper.skip_to(target)? + 1;

            if new_doc_upto > self.doc_upto {
                // Skipper moved
                debug_assert!(new_doc_upto % BLOCK_SIZE == 0);
                self.doc_upto = new_doc_upto;

                // Force to read next block
                self.doc_buffer_upto = BLOCK_SIZE;
                self.accum = skipper.doc(); // actually, this is just lastSkipEntry
                self.doc_in.as_mut().unwrap().seek(skipper.doc_pointer())?; // now point to the block we want to search
                self.pos_pending_fp = skipper.pos_pointer();
                self.pos_pending_count = skipper.pos_buffer_upto();
            }
            // next time we call advance, this is used to
            // foresee whether skipper is necessary.
            self.next_skip_doc = skipper.next_skip_doc();
        }
        if self.doc_upto == self.doc_freq {
            self.doc = NO_MORE_DOCS;
            return Ok(self.doc);
        }
        if self.doc_buffer_upto == BLOCK_SIZE {
            self.refill_docs()?;
        }

        self.doc = match self.encode_type {
            EncodeType::PF => {
                // Now scan... this is an inlined/pared down version
                // of nextDoc():
                loop {
                    self.accum += self.doc_delta_buffer[self.doc_buffer_upto as usize];
                    self.freq = self.freq_buffer[self.doc_buffer_upto as usize];
                    self.pos_pending_count += self.freq;
                    self.doc_buffer_upto += 1;
                    self.doc_upto += 1;

                    if self.accum >= target {
                        break;
                    }
                    if self.doc_upto == self.doc_freq {
                        self.doc = NO_MORE_DOCS;
                        return Ok(self.doc);
                    }
                }
                self.accum
            }

            EncodeType::EF => {
                let decoder = self.ef_decoder.as_mut().unwrap();
                let doc = decoder.advance_to_value((target - 1 - self.ef_base_doc) as i64);
                if doc == NO_MORE_VALUES {
                    self.doc = NO_MORE_DOCS;
                    return Ok(self.doc);
                }

                let current_index = decoder.current_index()? as i32;
                for i in self.doc_buffer_upto..=current_index {
                    self.pos_pending_count += self.freq_buffer[i as usize];
                }
                self.freq = self.freq_buffer[current_index as usize];
                self.doc_buffer_upto = current_index + 1;
                self.doc_upto = self.ef_base_total + self.doc_buffer_upto;
                self.accum = doc as i32 + 1 + self.ef_base_doc;
                self.accum
            }

            EncodeType::BITSET => {
                if target < self.bits_min_doc {
                    self.accum = self.bits_min_doc;
                    self.bits_index = 1;
                    self.doc_buffer_upto = 0;
                } else {
                    let mut index = target - self.bits_min_doc;
                    if index >= self.doc_bits.num_bits as i32 {
                        self.doc = NO_MORE_DOCS;
                        return Ok(NO_MORE_DOCS);
                    }
                    let find = self.doc_bits.get(index as usize)?;
                    // self.bits_index = index + 1;
                    self.accum = if find {
                        target
                    } else {
                        index = self.doc_bits.next_set_bit(index as usize);
                        if index == NO_MORE_DOCS {
                            self.doc = NO_MORE_DOCS;
                            return Ok(NO_MORE_DOCS);
                        }
                        self.bits_min_doc + index
                    };
                    self.doc_buffer_upto = self.doc_bits.count_ones_before_index2(
                        self.doc_buffer_upto,
                        self.bits_index as usize,
                        index as usize,
                    ) as i32;
                    // self.doc_buffer_upto =
                    // self.doc_bits.count_ones_before_index(index as usize) as i32;
                    self.bits_index = index + 1;
                }
                self.freq = self.freq_buffer[self.doc_buffer_upto as usize];
                self.doc_buffer_upto += 1;
                self.doc_upto = self.ef_base_total + self.doc_buffer_upto;
                self.accum
            }

            _ => {
                unimplemented!();
            }
        };

        self.position = 0;
        Ok(self.doc)
    }

    fn cost(&self) -> usize {
        self.doc_freq as usize
    }
}

// Also handles payloads + offsets
struct EverythingIterator {
    encoded: [u8; MAX_ENCODED_SIZE],

    doc_delta_buffer: [i32; MAX_DATA_SIZE],
    freq_buffer: [i32; MAX_DATA_SIZE],
    pos_delta_buffer: [i32; MAX_DATA_SIZE],

    payload_length_buffer: [i32; MAX_DATA_SIZE],
    offset_start_delta_buffer: [i32; MAX_DATA_SIZE],
    offset_length_buffer: [i32; MAX_DATA_SIZE],

    payload_bytes: Vec<u8>,
    payload_byte_upto: i32,
    payload_length: i32,

    last_start_offset: i32,
    start_offset: i32,
    end_offset: i32,

    doc_buffer_upto: i32,
    pos_buffer_upto: i32,

    skipper: Option<Lucene50SkipReader>,
    skipped: bool,

    start_doc_in: Box<dyn IndexInput>,

    doc_in: Option<Box<dyn IndexInput>>,
    pos_in: Box<dyn IndexInput>,
    pay_in: Box<dyn IndexInput>,

    index_has_offsets: bool,
    index_has_payloads: bool,

    // number of docs in this posting list
    doc_freq: i32,
    // number of positions in this posting list
    total_term_freq: i64,
    // how many docs we've read
    doc_upto: i32,
    // doc we last read
    doc: DocId,
    // accumulator for doc deltas
    accum: i32,
    // freq we last read
    freq: i32,
    // current position
    position: i32,

    // how many positions "behind" we are, nextPosition must
    // skip these to "catch up":
    pos_pending_count: i32,

    // Lazy pos seek: if != -1 then we must seek to this FP
    // before reading positions:
    pos_pending_fp: i64,

    // Lazy pay seek: if != -1 then we must seek to this FP
    // before reading payloads/offsets:
    pay_pending_fp: i64,

    // Where this term's postings start in the .doc file:
    doc_term_start_fp: i64,

    // Where this term's postings start in the .pos file:
    pos_term_start_fp: i64,

    // Where this term's payloads/offsets start in the .pay
    // file:
    pay_term_start_fp: i64,

    // File pointer where the last (vInt encoded) pos delta
    // block is.  We need this to know whether to bulk
    // decode vs vInt decode the block:
    last_pos_block_fp: i64,

    // Where this term's skip data starts (after
    // docTermStartFP) in the .doc file (or -1 if there is
    // no skip data for this term):
    skip_offset: i64,

    next_skip_doc: i32,

    needs_offsets: bool,
    // true if we actually need offsets
    needs_payloads: bool,
    // true if we actually need payloads
    singleton_doc_id: i32,
    // docid when there is a single pulsed posting, otherwise -1
    for_util: ForUtil,
    /// PF/EF/BITSET/FULL
    encode_type: EncodeType,
    ef_decoder: Option<EliasFanoDecoder>,
    ef_base_doc: DocId,
    ef_base_total: i32,
    doc_bits: FixedBitSet,
    bits_min_doc: DocId,
    bits_index: i32,
    use_simd: bool,
}

impl<'a> EverythingIterator {
    //#[allow(too_many_arguments)]
    pub fn new(
        start_doc_in: Box<dyn IndexInput>,
        pos_in: Box<dyn IndexInput>,
        pay_in: Box<dyn IndexInput>,
        field_info: &FieldInfo,
        term_state: &BlockTermState,
        flags: u16,
        for_util: ForUtil,
        use_simd: bool,
    ) -> Result<EverythingIterator> {
        let encoded = [0u8; MAX_ENCODED_SIZE];
        let index_has_offsets = field_info.index_options.has_offsets();
        let offset_start_delta_buffer = [0; MAX_DATA_SIZE];
        let offset_length_buffer = [0; MAX_DATA_SIZE];
        let (start_offset, end_offset) = if index_has_offsets { (0, 0) } else { (-1, -1) };

        let index_has_payloads = field_info.has_store_payloads;
        let payload_bytes = if index_has_payloads {
            vec![0 as u8; 128]
        } else {
            vec![]
        };
        let payload_length_buffer = [0; MAX_DATA_SIZE];

        let doc_delta_buffer = [0i32; MAX_DATA_SIZE];
        let freq_buffer = [0i32; MAX_DATA_SIZE];
        let pos_delta_buffer = [0i32; MAX_DATA_SIZE];

        let mut iterator = EverythingIterator {
            start_doc_in,
            pay_in,
            pos_in,
            encoded,
            index_has_offsets,
            offset_start_delta_buffer,
            offset_length_buffer,
            start_offset,
            end_offset,
            index_has_payloads,
            payload_length_buffer,
            payload_bytes,
            doc_delta_buffer,
            freq_buffer,
            pos_delta_buffer,
            doc_in: None,
            accum: 0,
            doc: 0,
            doc_buffer_upto: 0,
            doc_term_start_fp: 0,
            doc_upto: 0,
            doc_freq: 0,
            freq: 0,
            last_pos_block_fp: 0,
            last_start_offset: 0,
            needs_offsets: false,
            needs_payloads: false,
            next_skip_doc: 0,
            pay_pending_fp: 0,
            pay_term_start_fp: 0,
            payload_byte_upto: 0,
            payload_length: 0,
            pos_buffer_upto: 0,
            pos_pending_count: 0,
            pos_pending_fp: 0,
            pos_term_start_fp: 0,
            position: 0,
            singleton_doc_id: 0,
            skip_offset: 0,
            skipper: None,
            skipped: false,
            total_term_freq: 0,
            for_util,
            encode_type: EncodeType::PF,
            ef_decoder: None,
            ef_base_doc: -1,
            ef_base_total: 0,
            doc_bits: FixedBitSet::default(),
            bits_min_doc: 0,
            bits_index: 0,
            use_simd,
        };

        iterator.reset(term_state, flags)?;
        Ok(iterator)
    }

    pub fn reset(&mut self, term_state: &BlockTermState, flags: u16) -> Result<()> {
        self.doc_freq = term_state.doc_freq;
        self.doc_term_start_fp = term_state.doc_start_fp;
        self.pos_term_start_fp = term_state.pos_start_fp;
        self.pay_term_start_fp = term_state.pay_start_fp;
        self.skip_offset = term_state.skip_offset;
        self.total_term_freq = term_state.total_term_freq;
        self.singleton_doc_id = term_state.singleton_doc_id;
        if self.doc_freq > 1 {
            if self.doc_in.is_none() {
                // lazy init
                self.doc_in = Some(self.start_doc_in.clone()?);
            }
            if let Some(ref mut doc_in) = self.doc_in {
                doc_in.seek(self.doc_term_start_fp)?;
            }
        }
        self.pos_pending_fp = self.pos_term_start_fp;
        self.pay_pending_fp = self.pay_term_start_fp;
        self.pos_pending_count = 0;
        if term_state.total_term_freq < i64::from(BLOCK_SIZE) {
            self.last_pos_block_fp = self.pos_term_start_fp;
        } else if term_state.total_term_freq == i64::from(BLOCK_SIZE) {
            self.last_pos_block_fp = -1;
        } else {
            self.last_pos_block_fp = self.pos_term_start_fp + term_state.last_pos_block_offset;
        }

        self.needs_offsets =
            PostingIteratorFlags::feature_requested(flags, PostingIteratorFlags::OFFSETS);
        self.needs_payloads =
            PostingIteratorFlags::feature_requested(flags, PostingIteratorFlags::PAYLOADS);

        self.doc = -1;
        self.accum = 0;
        self.doc_upto = 0;
        self.next_skip_doc = if self.doc_freq > BLOCK_SIZE {
            // we won't skip if target is found in first block
            BLOCK_SIZE - 1
        } else {
            // not enough docs for skipping
            NO_MORE_DOCS
        };
        self.doc_buffer_upto = BLOCK_SIZE;
        self.skipped = false;

        self.encode_type = EncodeType::PF;
        self.ef_decoder = None;
        self.ef_base_doc = -1;
        self.ef_base_total = 0;
        // self.doc_bits.clear_batch(0, self.doc_bits.len());
        self.doc_bits.clear_all();
        self.bits_index = 0;

        Ok(())
    }

    pub fn refill_docs(&mut self) -> Result<()> {
        // EF & PF compatible
        if self.accum > 0 {
            self.ef_base_doc = self.accum;
        }
        self.ef_base_total = self.doc_upto;
        self.encode_type = EncodeType::PF;
        self.bits_index = 0;

        let left = self.doc_freq - self.doc_upto;
        if left >= BLOCK_SIZE {
            let doc_in = self.doc_in.as_mut().unwrap();
            self.for_util.read_block(
                doc_in.as_mut(),
                &mut self.encoded,
                &mut self.doc_delta_buffer,
                Some(&mut self.encode_type),
                self.use_simd,
            )?;

            ForUtil::read_other_encode_block(
                doc_in.as_mut(),
                &mut self.ef_decoder,
                &self.encode_type,
                &mut self.doc_bits,
                &mut self.bits_min_doc,
            )?;

            self.for_util.read_block(
                doc_in.as_mut(),
                &mut self.encoded,
                &mut self.freq_buffer,
                None,
                self.use_simd,
            )?;
        } else if self.doc_freq == 1 {
            self.doc_delta_buffer[0] = self.singleton_doc_id;
            self.freq_buffer[0] = self.total_term_freq as i32;
        } else {
            let doc_in = self.doc_in.as_mut().unwrap();
            read_vint_block(
                doc_in.as_mut(),
                &mut self.doc_delta_buffer,
                &mut self.freq_buffer,
                left,
                true,
            )?;
        }
        self.doc_buffer_upto = 0;
        Ok(())
    }

    pub fn refill_positions(&mut self) -> Result<()> {
        let pos_in = &mut self.pos_in;
        if pos_in.file_pointer() == self.last_pos_block_fp {
            let count = (self.total_term_freq % i64::from(BLOCK_SIZE)) as usize;
            let mut payload_length = 0i32;
            let mut offset_length = 0i32;
            self.payload_byte_upto = 0;
            for i in 0..count {
                let code = pos_in.read_vint()?;
                if self.index_has_payloads {
                    if (code & 1) != 0 {
                        payload_length = pos_in.read_vint()?;
                    }
                    self.payload_length_buffer[i] = payload_length;
                    self.pos_delta_buffer[i] = ((code as u32) >> 1) as i32;
                    if payload_length != 0 {
                        if self.payload_byte_upto + payload_length > self.payload_bytes.len() as i32
                        {
                            self.payload_bytes
                                .resize((self.payload_byte_upto + payload_length) as usize, 0);
                        }
                        pos_in.read_exact(
                            &mut self.payload_bytes[self.payload_byte_upto as usize
                                ..(self.payload_byte_upto + payload_length) as usize],
                        )?;
                        self.payload_byte_upto += payload_length;
                    }
                } else {
                    self.pos_delta_buffer[i] = code;
                }

                if self.index_has_offsets {
                    let delta_code = pos_in.read_vint()?;
                    if delta_code & 1 != 0 {
                        offset_length = pos_in.read_vint()?;
                    }
                    self.offset_start_delta_buffer[i] = ((delta_code as u32) >> 1) as i32;
                    self.offset_length_buffer[i] = offset_length;
                }
            }
            self.payload_byte_upto = 0;
        } else {
            self.for_util.read_block(
                pos_in.as_mut(),
                self.encoded.as_mut(),
                self.pos_delta_buffer.as_mut(),
                None,
                self.use_simd,
            )?;

            let pay_in = &mut self.pay_in;
            if self.index_has_payloads {
                if self.needs_payloads {
                    self.for_util.read_block(
                        pay_in.as_mut(),
                        &mut self.encoded,
                        self.payload_length_buffer.as_mut(),
                        None,
                        self.use_simd,
                    )?;
                    let num_bytes = pay_in.read_vint()? as usize;
                    if num_bytes > self.payload_bytes.len() {
                        self.payload_bytes.resize(num_bytes, 0);
                    }
                    pay_in.read_exact(&mut self.payload_bytes[0..num_bytes])?;
                } else {
                    // this works, because when writing a vint block we always force the first
                    // length to be written
                    self.for_util.skip_block(pay_in.as_mut())?; // skip over lengths
                    let num_bytes = pay_in.read_vint()?; // read length of payload_bytes
                    let fp = pay_in.file_pointer();
                    pay_in.seek(fp + i64::from(num_bytes))?; // skip over payload_bytes
                }
                self.payload_byte_upto = 0;
            }

            if self.index_has_offsets {
                if self.needs_offsets {
                    self.for_util.read_block(
                        pay_in.as_mut(),
                        &mut self.encoded,
                        self.offset_start_delta_buffer.as_mut(),
                        None,
                        self.use_simd,
                    )?;
                    self.for_util.read_block(
                        pay_in.as_mut(),
                        &mut self.encoded,
                        self.offset_length_buffer.as_mut(),
                        None,
                        self.use_simd,
                    )?;
                } else {
                    // this works, because when writing a vint block we always force the first
                    // length to be written
                    self.for_util.skip_block(pay_in.as_mut())?; // skip over starts
                    self.for_util.skip_block(pay_in.as_mut())?; // skip over lengths
                }
            }
        }
        Ok(())
    }

    // TODO: in theory we could avoid loading frq block
    // when not needed, ie, use skip data to load how far to
    // seek the pos pointer ... instead of having to load frq
    // blocks only to sum up how many positions to skip
    pub fn skip_positions(&mut self) -> Result<()> {
        // Skip positions now:
        let mut to_skip = self.pos_pending_count - self.freq;

        let left_in_block = BLOCK_SIZE - self.pos_buffer_upto;
        if to_skip < left_in_block {
            let end = self.pos_buffer_upto + to_skip;
            while self.pos_buffer_upto < end {
                if self.index_has_payloads {
                    self.payload_byte_upto +=
                        self.payload_length_buffer[self.pos_buffer_upto as usize];
                }
                self.pos_buffer_upto += 1;
            }
        } else {
            to_skip -= left_in_block;
            while to_skip >= BLOCK_SIZE {
                self.for_util.skip_block(self.pos_in.as_mut())?;

                if self.index_has_payloads {
                    // Skip payload_length block:
                    let pay_in = &mut self.pay_in;
                    self.for_util.skip_block(pay_in.as_mut())?;

                    // Skip payload_bytes block:
                    let num_bytes = pay_in.read_vint()?;
                    let fp = pay_in.file_pointer();
                    pay_in.seek(fp + i64::from(num_bytes))?;
                }

                if self.index_has_offsets {
                    let pay_in = &mut self.pay_in;
                    self.for_util.skip_block(pay_in.as_mut())?;
                    self.for_util.skip_block(pay_in.as_mut())?;
                }
                to_skip -= BLOCK_SIZE;
            }
            self.refill_positions()?;
            self.payload_byte_upto = 0;
            self.pos_buffer_upto = 0;
            while self.pos_buffer_upto < to_skip {
                if self.index_has_payloads {
                    self.payload_byte_upto +=
                        self.payload_length_buffer[self.pos_buffer_upto as usize];
                }
                self.pos_buffer_upto += 1;
            }
        }

        self.position = 0;
        self.last_start_offset = 0;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn check_integrity(&self) -> Result<()> {
        //        if let Some(ref doc_in) = self.doc_in {
        //            codec_util::checksum_entire_file(doc_in.as_ref())?;
        //        }
        //
        //        codec_util::checksum_entire_file(self.pos_in.as_ref())?;
        //        codec_util::checksum_entire_file(self.pay_in.as_ref())?;
        Ok(())
    }
}

impl PostingIterator for EverythingIterator {
    fn freq(&self) -> Result<i32> {
        Ok(self.freq)
    }

    fn next_position(&mut self) -> Result<i32> {
        debug_assert!(self.pos_pending_count > 0);

        if self.pos_pending_fp != -1 {
            self.pos_in.seek(self.pos_pending_fp)?;
            self.pos_pending_fp = -1;

            if self.pay_pending_fp != -1 {
                self.pay_in.seek(self.pay_pending_fp)?;
                self.pay_pending_fp = -1;
            }

            // Force buffer refill:
            self.pos_buffer_upto = BLOCK_SIZE;
        }

        if self.pos_pending_count > self.freq {
            self.skip_positions()?;
            self.pos_pending_count = self.freq;
        }

        if self.pos_buffer_upto == BLOCK_SIZE {
            self.refill_positions()?;
            self.pos_buffer_upto = 0;
        }

        self.position += self.pos_delta_buffer[self.pos_buffer_upto as usize];

        if self.index_has_payloads {
            debug_assert!(!self.payload_length_buffer.is_empty());
            // debug_assert!(!self.payload_bytes.is_empty());
            self.payload_length = self.payload_length_buffer[self.pos_buffer_upto as usize];
            self.payload_byte_upto += self.payload_length;
        }

        if self.index_has_offsets {
            debug_assert!(!self.offset_start_delta_buffer.is_empty());
            debug_assert!(!self.offset_length_buffer.is_empty());
            self.start_offset = self.last_start_offset
                + self.offset_start_delta_buffer[self.pos_buffer_upto as usize];
            self.end_offset =
                self.start_offset + self.offset_length_buffer[self.pos_buffer_upto as usize];
            self.last_start_offset = self.start_offset;
        }

        self.pos_buffer_upto += 1;
        self.pos_pending_count -= 1;
        Ok(self.position)
    }

    fn start_offset(&self) -> Result<i32> {
        Ok(self.start_offset)
    }

    fn end_offset(&self) -> Result<i32> {
        Ok(self.end_offset)
    }

    fn payload(&self) -> Result<Payload> {
        if self.payload_length == 0 || self.payload_byte_upto < self.payload_length {
            Ok(vec![])
        } else {
            let end = self.payload_byte_upto as usize;
            let start = end - self.payload_length as usize;
            Ok(self.payload_bytes[start..end].to_vec())
        }
    }
}

impl DocIterator for EverythingIterator {
    fn doc_id(&self) -> DocId {
        self.doc
    }

    fn next(&mut self) -> Result<DocId> {
        if self.doc_upto == self.doc_freq {
            self.doc = NO_MORE_DOCS;
            return Ok(self.doc);
        }
        if self.doc_buffer_upto == BLOCK_SIZE {
            self.refill_docs()?;
        }
        // set doc id
        self.doc = match self.encode_type {
            EncodeType::PF => self.accum + self.doc_delta_buffer[self.doc_buffer_upto as usize],

            EncodeType::EF => {
                self.ef_decoder.as_mut().unwrap().next_value() as i32 + 1 + self.ef_base_doc
            }

            EncodeType::BITSET => {
                self.bits_index = self.doc_bits.next_set_bit(self.bits_index as usize);
                let doc = self.bits_min_doc + self.bits_index;
                self.bits_index += 1;
                doc
            }

            _ => {
                unimplemented!();
            }
        };

        self.accum = self.doc;

        self.freq = self.freq_buffer[self.doc_buffer_upto as usize];
        self.pos_pending_count += self.freq;
        self.doc_buffer_upto += 1;
        self.doc_upto += 1;

        self.position = 0;
        self.last_start_offset = 0;
        Ok(self.doc)
    }

    fn advance(&mut self, target: DocId) -> Result<DocId> {
        if unlikely(target == NO_MORE_DOCS) {
            self.doc = NO_MORE_DOCS;
            return Ok(self.doc);
        }
        // TODO: make frq block load lazy/skippable

        if target > self.next_skip_doc {
            if self.skipper.is_none() {
                // Lazy init: first time this enum has ever been used for skipping
                self.skipper = Some(Lucene50SkipReader::new(
                    IndexInput::clone(self.doc_in.as_mut().unwrap().as_mut())?,
                    MAX_SKIP_LEVELS,
                    true,
                    self.index_has_offsets,
                    self.index_has_payloads,
                ));
            }

            if !self.skipped {
                // This is the first time this enum has skipped
                // since reset() was called; load the skip data:
                if let Some(ref mut skipper) = self.skipper {
                    skipper.init(
                        self.doc_term_start_fp + self.skip_offset,
                        self.doc_term_start_fp,
                        self.pos_term_start_fp,
                        self.pay_term_start_fp,
                        self.doc_freq,
                    )?;
                }
                self.skipped = true;
            }

            let new_doc_upto: i32 = self.skipper.as_mut().unwrap().skip_to(target)? + 1;
            if new_doc_upto > self.doc_upto {
                // Skipper moved
                self.doc_upto = new_doc_upto;

                // Force to read next block
                self.doc_buffer_upto = BLOCK_SIZE;
                let skipper = self.skipper.as_ref().unwrap();
                self.accum = skipper.doc();
                self.doc_in.as_mut().unwrap().seek(skipper.doc_pointer())?;
                self.pos_pending_fp = skipper.pos_pointer();
                self.pay_pending_fp = skipper.pay_pointer();
                self.pos_pending_count = skipper.pos_buffer_upto();
                self.last_start_offset = 0; // new document
                self.payload_byte_upto = skipper.payload_byte_upto();
            }
            self.next_skip_doc = self.skipper.as_ref().unwrap().next_skip_doc();
        }
        if self.doc_upto == self.doc_freq {
            self.doc = NO_MORE_DOCS;
            return Ok(self.doc);
        }
        if self.doc_buffer_upto == BLOCK_SIZE {
            self.refill_docs()?;
        }

        self.doc = match self.encode_type {
            EncodeType::PF => {
                // Now scan... this is an inlined/pared down version
                // of nextDoc():
                loop {
                    self.accum += self.doc_delta_buffer[self.doc_buffer_upto as usize];
                    self.freq = self.freq_buffer[self.doc_buffer_upto as usize];
                    self.pos_pending_count += self.freq;
                    self.doc_buffer_upto += 1;
                    self.doc_upto += 1;

                    if self.accum >= target {
                        break;
                    }
                    if self.doc_upto == self.doc_freq {
                        self.doc = NO_MORE_DOCS;
                        return Ok(self.doc);
                    }
                }
                self.accum
            }

            EncodeType::EF => {
                let decoder = self.ef_decoder.as_mut().unwrap();
                let doc = decoder.advance_to_value((target - 1 - self.ef_base_doc) as i64);
                if doc == NO_MORE_VALUES {
                    self.doc = NO_MORE_DOCS;
                    return Ok(self.doc);
                }

                let current_index = decoder.current_index()? as i32;
                for i in self.doc_buffer_upto..=current_index {
                    self.pos_pending_count += self.freq_buffer[i as usize];
                }
                self.freq = self.freq_buffer[current_index as usize];
                self.doc_buffer_upto = current_index + 1;
                self.doc_upto = self.ef_base_total + self.doc_buffer_upto;
                self.accum = doc as i32 + 1 + self.ef_base_doc;
                self.accum
            }

            EncodeType::BITSET => {
                if target < self.bits_min_doc {
                    self.accum = self.bits_min_doc;
                    self.bits_index = 1;
                    self.doc_buffer_upto = 0;
                } else {
                    let mut index = target - self.bits_min_doc;
                    if index >= self.doc_bits.num_bits as i32 {
                        self.doc = NO_MORE_DOCS;
                        return Ok(NO_MORE_DOCS);
                    }
                    let find = self.doc_bits.get(index as usize)?;
                    // self.bits_index = index + 1;
                    self.accum = if find {
                        target
                    } else {
                        index = self.doc_bits.next_set_bit(index as usize);
                        if index == NO_MORE_DOCS {
                            self.doc = NO_MORE_DOCS;
                            return Ok(NO_MORE_DOCS);
                        }
                        self.bits_min_doc + index
                    };
                    self.doc_buffer_upto = self.doc_bits.count_ones_before_index2(
                        self.doc_buffer_upto,
                        self.bits_index as usize,
                        index as usize,
                    ) as i32;
                    // self.doc_buffer_upto =
                    // self.doc_bits.count_ones_before_index(index as usize) as i32;
                    self.bits_index = index + 1;
                }
                self.freq = self.freq_buffer[self.doc_buffer_upto as usize];
                self.doc_buffer_upto += 1;
                self.doc_upto = self.ef_base_total + self.doc_buffer_upto;
                self.accum
            }

            _ => {
                unimplemented!();
            }
        };

        self.position = 0;
        self.last_start_offset = 0;
        Ok(self.doc)
    }

    fn cost(&self) -> usize {
        self.doc_freq as usize
    }
}

/// `PostingIterator` impl for `Lucene50PostingsReader`
pub struct Lucene50PostingIterator(Lucene50PostingIterEnum);

enum Lucene50PostingIterEnum {
    Doc(BlockDocIterator),
    #[allow(dead_code)]
    SDoc(SIMDBlockDocIterator),
    Posting(BlockPostingIterator),
    Everything(EverythingIterator),
}

impl PostingIterator for Lucene50PostingIterator {
    fn freq(&self) -> Result<i32> {
        match &self.0 {
            Lucene50PostingIterEnum::Doc(i) => i.freq(),
            Lucene50PostingIterEnum::SDoc(i) => i.freq(),
            Lucene50PostingIterEnum::Posting(i) => i.freq(),
            Lucene50PostingIterEnum::Everything(i) => i.freq(),
        }
    }

    fn next_position(&mut self) -> Result<i32> {
        match &mut self.0 {
            Lucene50PostingIterEnum::Doc(i) => i.next_position(),
            Lucene50PostingIterEnum::SDoc(i) => i.next_position(),
            Lucene50PostingIterEnum::Posting(i) => i.next_position(),
            Lucene50PostingIterEnum::Everything(i) => i.next_position(),
        }
    }

    fn start_offset(&self) -> Result<i32> {
        match &self.0 {
            Lucene50PostingIterEnum::Doc(i) => i.start_offset(),
            Lucene50PostingIterEnum::SDoc(i) => i.start_offset(),
            Lucene50PostingIterEnum::Posting(i) => i.start_offset(),
            Lucene50PostingIterEnum::Everything(i) => i.start_offset(),
        }
    }

    fn end_offset(&self) -> Result<i32> {
        match &self.0 {
            Lucene50PostingIterEnum::Doc(i) => i.end_offset(),
            Lucene50PostingIterEnum::SDoc(i) => i.end_offset(),
            Lucene50PostingIterEnum::Posting(i) => i.end_offset(),
            Lucene50PostingIterEnum::Everything(i) => i.end_offset(),
        }
    }

    fn payload(&self) -> Result<Payload> {
        match &self.0 {
            Lucene50PostingIterEnum::Doc(i) => i.payload(),
            Lucene50PostingIterEnum::SDoc(i) => i.payload(),
            Lucene50PostingIterEnum::Posting(i) => i.payload(),
            Lucene50PostingIterEnum::Everything(i) => i.payload(),
        }
    }
}

impl DocIterator for Lucene50PostingIterator {
    fn doc_id(&self) -> DocId {
        match &self.0 {
            Lucene50PostingIterEnum::Doc(i) => i.doc_id(),
            Lucene50PostingIterEnum::SDoc(i) => i.doc_id(),
            Lucene50PostingIterEnum::Posting(i) => i.doc_id(),
            Lucene50PostingIterEnum::Everything(i) => i.doc_id(),
        }
    }

    fn next(&mut self) -> Result<DocId> {
        match &mut self.0 {
            Lucene50PostingIterEnum::Doc(i) => i.next(),
            Lucene50PostingIterEnum::SDoc(i) => i.next(),
            Lucene50PostingIterEnum::Posting(i) => i.next(),
            Lucene50PostingIterEnum::Everything(i) => i.next(),
        }
    }

    fn advance(&mut self, target: i32) -> Result<DocId> {
        match &mut self.0 {
            Lucene50PostingIterEnum::Doc(i) => i.advance(target),
            Lucene50PostingIterEnum::SDoc(i) => i.advance(target),
            Lucene50PostingIterEnum::Posting(i) => i.advance(target),
            Lucene50PostingIterEnum::Everything(i) => i.advance(target),
        }
    }

    fn slow_advance(&mut self, target: i32) -> Result<DocId> {
        match &mut self.0 {
            Lucene50PostingIterEnum::Doc(i) => i.slow_advance(target),
            Lucene50PostingIterEnum::SDoc(i) => i.slow_advance(target),
            Lucene50PostingIterEnum::Posting(i) => i.slow_advance(target),
            Lucene50PostingIterEnum::Everything(i) => i.slow_advance(target),
        }
    }

    fn cost(&self) -> usize {
        match &self.0 {
            Lucene50PostingIterEnum::Doc(i) => i.cost(),
            Lucene50PostingIterEnum::SDoc(i) => i.cost(),
            Lucene50PostingIterEnum::Posting(i) => i.cost(),
            Lucene50PostingIterEnum::Everything(i) => i.cost(),
        }
    }

    fn matches(&mut self) -> Result<bool> {
        match &mut self.0 {
            Lucene50PostingIterEnum::Doc(i) => i.matches(),
            Lucene50PostingIterEnum::SDoc(i) => i.matches(),
            Lucene50PostingIterEnum::Posting(i) => i.matches(),
            Lucene50PostingIterEnum::Everything(i) => i.matches(),
        }
    }

    fn match_cost(&self) -> f32 {
        match &self.0 {
            Lucene50PostingIterEnum::Doc(i) => i.match_cost(),
            Lucene50PostingIterEnum::SDoc(i) => i.match_cost(),
            Lucene50PostingIterEnum::Posting(i) => i.match_cost(),
            Lucene50PostingIterEnum::Everything(i) => i.match_cost(),
        }
    }

    fn approximate_next(&mut self) -> Result<DocId> {
        match &mut self.0 {
            Lucene50PostingIterEnum::Doc(i) => i.approximate_next(),
            Lucene50PostingIterEnum::SDoc(i) => i.approximate_next(),
            Lucene50PostingIterEnum::Posting(i) => i.approximate_next(),
            Lucene50PostingIterEnum::Everything(i) => i.approximate_next(),
        }
    }

    fn approximate_advance(&mut self, target: i32) -> Result<DocId> {
        match &mut self.0 {
            Lucene50PostingIterEnum::Doc(i) => i.approximate_advance(target),
            Lucene50PostingIterEnum::SDoc(i) => i.approximate_advance(target),
            Lucene50PostingIterEnum::Posting(i) => i.approximate_advance(target),
            Lucene50PostingIterEnum::Everything(i) => i.approximate_advance(target),
        }
    }
}
