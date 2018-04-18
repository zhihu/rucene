use core::codec::blocktree::BlockTreeTermsReader;
use core::codec::codec_util;
use core::codec::format::PostingsFormat;
use core::codec::lucene50::skip::*;
use core::codec::lucene50::util::*;
use core::codec::BlockTermState;
use core::codec::FieldsProducer;
use core::index::FieldInfo;
use core::index::{segment_file_name, SegmentReadState};
use core::search::posting_iterator::*;
use core::search::*;
use core::store::IndexInput;
use core::util::bit_util::UnsignedShift;
use core::util::DocId;
use error::*;
use std::fmt;
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
const MAX_SKIP_LEVELS: i32 = 10;

pub const TERMS_CODEC: &str = "Lucene50PostingsWriterTerms";
pub const DOC_CODEC: &str = "Lucene50PostingsWriterDoc";
pub const POS_CODEC: &str = "Lucene50PostingsWriterPos";
pub const PAY_CODEC: &str = "Lucene50PostingsWriterPay";

// Increment version to change it
const VERSION_START: i32 = 0;
const VERSION_CURRENT: i32 = VERSION_START;

/// Fixed packed block size, number of integers encoded in
/// a single packed block.
///
// NOTE: must be multiple of 64 because of PackedInts long-aligned encoding/decoding
pub const BLOCK_SIZE: i32 = 128;

fn clone_option_index_input(input: &Option<Box<IndexInput>>) -> Result<Box<IndexInput>> {
    debug_assert!(input.is_some());
    (*input.as_ref().unwrap()).clone()
}

pub struct Lucene50PostingsFormat {
    name: &'static str,
    pub min_term_block_size: i32,
    pub max_term_block_size: i32,
}

const DEFAULT_MIN_BLOCK_SIZE: i32 = 25;
const DEFAULT_MAX_BLOCK_SIZE: i32 = 48;

impl fmt::Display for Lucene50PostingsFormat {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}(blocksize={})", self.name, BLOCK_SIZE)
    }
}

impl Default for Lucene50PostingsFormat {
    fn default() -> Lucene50PostingsFormat {
        Self::with_block_size(DEFAULT_MIN_BLOCK_SIZE, DEFAULT_MAX_BLOCK_SIZE)
    }
}

impl Lucene50PostingsFormat {
    pub fn with_block_size(
        min_term_block_size: i32,
        max_term_block_size: i32,
    ) -> Lucene50PostingsFormat {
        Lucene50PostingsFormat {
            name: "Lucene50",
            min_term_block_size,
            max_term_block_size,
        }
    }
}

impl PostingsFormat for Lucene50PostingsFormat {
    fn fields_producer(&self, state: &SegmentReadState) -> Result<Box<FieldsProducer>> {
        let reader = Lucene50PostingsReader::open(&state)?;
        Ok(Box::new(BlockTreeTermsReader::new(reader, state)?))
    }
}

/// Concrete class that reads docId(maybe frq,pos,offset,payloads) list
/// with postings format.
///
/// @lucene.experimental
pub struct Lucene50PostingsReader {
    doc_in: Box<IndexInput>,
    pos_in: Option<Box<IndexInput>>,
    pay_in: Option<Box<IndexInput>>,
    pub version: i32,
    pub for_util: ForUtil,
}

impl Lucene50PostingsReader {
    fn clone_pos_in(&self) -> Result<Box<IndexInput>> {
        clone_option_index_input(&self.pos_in)
    }
    fn clone_pay_in(&self) -> Result<Box<IndexInput>> {
        clone_option_index_input(&self.pay_in)
    }
    pub fn open(state: &SegmentReadState) -> Result<Lucene50PostingsReader> {
        let doc_name = segment_file_name(
            &state.segment_info.name,
            &state.segment_suffix,
            DOC_EXTENSION,
        );
        let mut doc_in = state.directory.open_input(&doc_name, &state.context)?;
        let version = codec_util::check_index_header(
            doc_in.as_mut(),
            DOC_CODEC,
            VERSION_START,
            VERSION_CURRENT,
            &state.segment_info.id,
            &state.segment_suffix,
        )?;
        let for_util = ForUtil::with_input(doc_in.clone()?)?;
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
            let mut input = state.directory.open_input(&prox_name, &state.context)?;
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
                input = state.directory.open_input(&pay_name, &state.context)?;
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
        })
    }

    pub fn init(&self, terms_in: &mut IndexInput, state: &SegmentReadState) -> Result<()> {
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
            bail!(
                "index-time BLOCK_SIZE ({}) != read-time BLOCK_SIZE ({})",
                index_block_size,
                BLOCK_SIZE
            )
        } else {
            Ok(())
        }
    }

    /// Return a newly created empty TermState
    pub fn new_term_state(&mut self) -> BlockTermState {
        BlockTermState::new()
    }

    pub fn postings(
        &self,
        field_info: &FieldInfo,
        state: &BlockTermState,
        flags: i16,
        segment: Arc<String>,
        term: &[u8],
    ) -> Result<Box<PostingIterator>> {
        let options = &field_info.index_options;
        let index_has_positions = options.has_positions();
        let index_has_offsets = options.has_offsets();
        let index_has_payloads = field_info.has_store_payloads;

        Ok(if !index_has_positions
            || !posting_feature_requested(flags, POSTING_ITERATOR_FLAG_POSITIONS)
        {
            Box::new(BlockDocIterator::new(
                self.doc_in.clone()?,
                field_info,
                state,
                flags,
                self.for_util.clone(),
                segment,
                term,
            )?)
        } else if (!index_has_offsets
            || !posting_feature_requested(flags, POSTING_ITERATOR_FLAG_OFFSETS))
            && (!index_has_payloads
                || !posting_feature_requested(flags, POSTING_ITERATOR_FLAG_PAYLOADS))
        {
            Box::new(BlockPostingIterator::new(
                self.doc_in.clone()?,
                self.clone_pos_in()?,
                field_info,
                state,
                flags,
                self.for_util.clone(),
                segment,
                term,
            )?)
        } else {
            debug_assert!(self.pos_in.is_some());
            debug_assert!(self.pay_in.is_some());
            Box::new(EverythingIterator::new(
                self.doc_in.clone()?,
                self.clone_pos_in()?,
                self.clone_pay_in()?,
                field_info,
                state,
                flags,
                self.for_util.clone(),
                segment,
                term,
            )?)
        })
    }

    pub fn check_integrity(&self) -> Result<()> {
        codec_util::checksum_entire_file(self.doc_in.as_ref())?;

        if let Some(ref pos_in) = self.pos_in {
            codec_util::checksum_entire_file(pos_in.as_ref())?;
        }
        if let Some(ref pay_in) = self.pay_in {
            codec_util::checksum_entire_file(pay_in.as_ref())?;
        }
        Ok(())
    }
}

pub type Lucene50PostingsReaderRef = Arc<Lucene50PostingsReader>;

/// Actually decode metadata for next term
/// @see PostingsWriterBase#encodeTerm
pub fn lucene50_decode_term(
    longs: &[i64],
    input: &mut IndexInput,
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
    doc_in: &mut IndexInput,
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

pub struct BlockDocIterator {
    pub encoded: Vec<u8>,

    pub doc_delta_buffer: Vec<i32>,
    freq_buffer: Vec<i32>,

    doc_buffer_upto: i32,

    pub skipper: Option<Lucene50SkipReader>,
    skipped: bool,

    start_doc_in: Box<IndexInput>,

    doc_in: Option<Box<IndexInput>>,
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

    segment: Arc<String>,
    term: Vec<u8>,
}

impl BlockDocIterator {
    pub fn new(
        start_doc_in: Box<IndexInput>,
        field_info: &FieldInfo,
        term_state: &BlockTermState,
        flags: i16,
        for_util: ForUtil,
        segment: Arc<String>,
        term: &[u8],
    ) -> Result<BlockDocIterator> {
        let options = &field_info.index_options;
        let term = term.into();
        let mut iterator = BlockDocIterator {
            encoded: vec![0 as u8; MAX_ENCODED_SIZE],
            start_doc_in,
            doc_delta_buffer: vec![0 as i32; max_data_size()],
            freq_buffer: vec![0 as i32; max_data_size()],
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
            segment,
            term,
        };
        iterator.reset(term_state, flags)?;
        Ok(iterator)
    }

    pub fn reset(&mut self, term_state: &BlockTermState, flags: i16) -> Result<()> {
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
            if let Some(ref mut doc_in) = self.doc_in {
                doc_in.seek(start)?;
            }
        }

        self.doc = -1;
        self.needs_freq = posting_feature_requested(flags, POSTING_ITERATOR_FLAG_FREQS);
        if !self.index_has_freq || !self.needs_freq {
            self.freq_buffer.iter_mut().map(|x| *x = 1).count();
        }
        self.accum = 0;
        self.doc_upto = 0;
        self.next_skip_doc = BLOCK_SIZE - 1; // we won't skip if target is found in first block
        self.doc_buffer_upto = BLOCK_SIZE;
        self.skipped = false;
        Ok(())
    }

    fn refill_docs(&mut self) -> Result<()> {
        let left = self.doc_freq - self.doc_upto;
        debug_assert!(left > 0);
        if left >= BLOCK_SIZE {
            let doc_in = self.doc_in.as_mut().unwrap();
            self.for_util.read_block(
                doc_in.as_mut(),
                &mut self.encoded,
                &mut self.doc_delta_buffer,
            )?;

            if self.index_has_freq {
                if self.needs_freq {
                    self.for_util.read_block(
                        doc_in.as_mut(),
                        &mut self.encoded,
                        &mut self.freq_buffer,
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

    fn clone(&self) -> Result<Self> {
        let doc_in = match self.doc_in {
            Some(ref doc_in) => Some((*doc_in).clone()?),
            _ => None,
        };
        let skipper = match self.skipper {
            Some(ref skipper) => Some((*skipper).clone_reader()?),
            _ => None,
        };
        Ok(BlockDocIterator {
            encoded: self.encoded.clone(),
            doc_delta_buffer: self.doc_delta_buffer.clone(),
            freq_buffer: self.freq_buffer.clone(),

            doc_buffer_upto: self.doc_buffer_upto,

            skipper,
            skipped: self.skipped,

            start_doc_in: self.start_doc_in.clone()?,

            doc_in,
            index_has_freq: self.index_has_freq,
            index_has_pos: self.index_has_pos,
            index_has_offsets: self.index_has_offsets,
            index_has_payloads: self.index_has_payloads,

            doc_freq: self.doc_freq,
            total_term_freq: self.total_term_freq,
            doc_upto: self.doc_upto,
            doc: self.doc,
            accum: self.accum,
            freq: self.freq,

            doc_term_start_fp: self.doc_term_start_fp,

            skip_offset: self.skip_offset,

            next_skip_doc: self.next_skip_doc,

            needs_freq: self.needs_freq,
            singleton_doc_id: self.singleton_doc_id,

            for_util: self.for_util.clone(),

            segment: self.segment.clone(),
            term: self.term.clone(),
        })
    }
}

impl PostingIterator for BlockDocIterator {
    fn clone_as_doc_iterator(&self) -> Result<Box<DocIterator>> {
        Ok(Box::new(self.clone()?))
    }

    fn freq(&self) -> Result<i32> {
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

        self.accum += self.doc_delta_buffer[self.doc_buffer_upto as usize];
        self.doc_upto += 1;

        self.doc = self.accum;
        self.freq = self.freq_buffer[self.doc_buffer_upto as usize];
        self.doc_buffer_upto += 1;
        Ok(self.doc)
    }

    fn advance(&mut self, target: DocId) -> Result<DocId> {
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
                self.doc_in.as_mut().unwrap().seek(skipper.doc_pointer())?; // now point to the block we want to search
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

        self.freq = self.freq_buffer[self.doc_buffer_upto as usize];
        self.doc_buffer_upto += 1;
        self.doc = self.accum;
        Ok(self.doc)
    }

    fn cost(&self) -> usize {
        self.doc_freq as usize
    }
}

pub struct BlockPostingIterator {
    encoded: Vec<u8>,

    doc_delta_buffer: Vec<i32>,
    freq_buffer: Vec<i32>,
    pub pos_delta_buffer: Vec<i32>,

    doc_buffer_upto: i32,
    pub pos_buffer_upto: i32,

    pub skipper: Option<Lucene50SkipReader>,
    skipped: bool,

    start_doc_in: Box<IndexInput>,

    doc_in: Option<Box<IndexInput>>,
    pos_in: Box<IndexInput>,

    index_has_freq: bool,
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
    pub position: i32,

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

    segment: Arc<String>,
    term: Vec<u8>,
}

impl BlockPostingIterator {
    #[allow(too_many_arguments)]
    pub fn new(
        start_doc_in: Box<IndexInput>,
        pos_in: Box<IndexInput>,
        field_info: &FieldInfo,
        term_state: &BlockTermState,
        _flags: i16,
        for_util: ForUtil,
        segment: Arc<String>,
        term: &[u8],
    ) -> Result<BlockPostingIterator> {
        let options = &field_info.index_options;
        let mut iterator = BlockPostingIterator {
            encoded: vec![0 as u8; MAX_ENCODED_SIZE],
            start_doc_in,
            doc_delta_buffer: vec![0 as i32; max_data_size()],
            freq_buffer: vec![0 as i32; max_data_size()],
            pos_delta_buffer: vec![0 as i32; max_data_size()],
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
            index_has_freq: options.has_freqs(),
            index_has_pos: options.has_positions(),
            index_has_offsets: options.has_offsets(),
            index_has_payloads: field_info.has_store_payloads,
            for_util,
            segment,
            term: term.into(),
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
        Ok(())
    }

    fn refill_docs(&mut self) -> Result<()> {
        let left = self.doc_freq - self.doc_upto;
        if let Some(ref mut doc_in) = self.doc_in {
            if left >= BLOCK_SIZE {
                self.for_util.read_block(
                    doc_in.as_mut(),
                    &mut self.encoded,
                    &mut self.doc_delta_buffer,
                )?;
                self.for_util.read_block(
                    doc_in.as_mut(),
                    &mut self.encoded,
                    &mut self.freq_buffer,
                )?;
            } else if self.doc_freq == 1 {
                self.doc_delta_buffer[0] = self.singleton_doc_id;
                self.freq_buffer[0] = self.total_term_freq as i32;
            } else {
                // Read vInts:
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
        } else {
            bail!("Not initalized")
        }
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

    fn clone(&self) -> Result<Self> {
        let doc_in = match self.doc_in {
            Some(ref doc_in) => Some((*doc_in).clone()?),
            _ => None,
        };
        let skipper = match self.skipper {
            Some(ref skipper) => Some((*skipper).clone_reader()?),
            _ => None,
        };
        Ok(BlockPostingIterator {
            encoded: self.encoded.clone(),
            doc_delta_buffer: self.doc_delta_buffer.clone(),
            freq_buffer: self.freq_buffer.clone(),
            pos_delta_buffer: self.pos_delta_buffer.clone(),
            doc_buffer_upto: self.doc_buffer_upto,
            pos_buffer_upto: self.pos_buffer_upto,
            skipper,
            skipped: self.skipped,
            start_doc_in: self.start_doc_in.clone()?,
            doc_in,
            pos_in: self.pos_in.clone()?,
            index_has_freq: self.index_has_freq,
            index_has_pos: self.index_has_pos,
            index_has_offsets: self.index_has_offsets,
            index_has_payloads: self.index_has_payloads,
            doc_freq: self.doc_freq,
            total_term_freq: self.total_term_freq,
            doc_upto: self.doc_upto,
            doc: self.doc,
            accum: self.accum,
            freq: self.freq,
            position: self.position,
            pos_pending_count: self.pos_pending_count,
            pos_pending_fp: self.pos_pending_fp,
            doc_term_start_fp: self.doc_term_start_fp,
            pos_term_start_fp: self.pos_term_start_fp,
            pay_term_start_fp: self.pay_term_start_fp,
            last_pos_block_fp: self.last_pos_block_fp,
            skip_offset: self.skip_offset,
            next_skip_doc: self.next_skip_doc,
            singleton_doc_id: self.singleton_doc_id,
            for_util: self.for_util.clone(),
            segment: Arc::clone(&self.segment),
            term: self.term.clone(),
        })
    }
}

impl PostingIterator for BlockPostingIterator {
    fn clone_as_doc_iterator(&self) -> Result<Box<DocIterator>> {
        Ok(Box::new(self.clone()?))
    }

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

        self.accum += self.doc_delta_buffer[self.doc_buffer_upto as usize];
        self.freq = self.freq_buffer[self.doc_buffer_upto as usize];
        self.pos_pending_count += self.freq;
        self.doc_buffer_upto += 1;
        self.doc_upto += 1;

        self.doc = self.accum;
        self.position = 0;
        Ok(self.doc)
    }

    fn advance(&mut self, target: DocId) -> Result<i32> {
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

        self.position = 0;
        self.doc = self.accum;
        Ok(self.doc)
    }

    fn cost(&self) -> usize {
        self.doc_freq as usize
    }
}

// Also handles payloads + offsets
pub struct EverythingIterator {
    encoded: Vec<u8>,

    doc_delta_buffer: Vec<i32>,
    freq_buffer: Vec<i32>,
    pos_delta_buffer: Vec<i32>,

    payload_length_buffer: Option<Vec<i32>>,
    offset_start_delta_buffer: Option<Vec<i32>>,
    offset_length_buffer: Option<Vec<i32>>,

    payload_bytes: Option<Vec<u8>>,
    payload_byte_upto: i32,
    payload_length: i32,

    last_start_offset: i32,
    start_offset: i32,
    end_offset: i32,

    doc_buffer_upto: i32,
    pos_buffer_upto: i32,

    skipper: Option<Lucene50SkipReader>,
    skipped: bool,

    start_doc_in: Box<IndexInput>,

    doc_in: Option<Box<IndexInput>>,
    pos_in: Box<IndexInput>,
    pay_in: Box<IndexInput>,
    payload: Option<Vec<u8>>,

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

    segment: Arc<String>,
    term: Vec<u8>,
}

impl<'a> EverythingIterator {
    #[allow(too_many_arguments)]
    pub fn new(
        start_doc_in: Box<IndexInput>,
        pos_in: Box<IndexInput>,
        pay_in: Box<IndexInput>,
        field_info: &FieldInfo,
        term_state: &BlockTermState,
        flags: i16,
        for_util: ForUtil,
        segment: Arc<String>,
        term: &[u8],
    ) -> Result<EverythingIterator> {
        let encoded = vec![0 as u8; MAX_ENCODED_SIZE];
        let index_has_offsets = field_info.index_options.has_offsets();
        let (offset_start_delta_buffer, offset_length_buffer, start_offset, end_offset) =
            if index_has_offsets {
                (
                    Some(vec![0 as i32; max_data_size()]),
                    Some(vec![0 as i32; max_data_size()]),
                    0 as i32,
                    0 as i32,
                )
            } else {
                (None, None, -1, -1)
            };

        let index_has_payloads = field_info.has_store_payloads;
        let empty: Vec<u8> = Vec::new();
        let (payload_length_buffer, payload_bytes, payload) = if index_has_payloads {
            (
                Some(vec![0 as i32; max_data_size()]),
                Some(vec![0 as u8; 128]),
                Some(empty),
            )
        } else {
            (None, None, None)
        };

        let doc_delta_buffer = vec![0 as i32; max_data_size()];
        let freq_buffer = vec![0 as i32; max_data_size()];
        let pos_delta_buffer = vec![0 as i32; max_data_size()];

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
            payload,
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
            segment,
            term: term.into(),
        };

        iterator.reset(term_state, flags)?;
        Ok(iterator)
    }

    pub fn encoded(&self) -> &[u8] {
        &self.encoded
    }

    pub fn doc_delta_buffer(&self) -> &[i32] {
        &self.doc_delta_buffer
    }
    pub fn freq_buffer(&self) -> &[i32] {
        &self.freq_buffer
    }
    pub fn pos_delta_buffer(&self) -> &[i32] {
        &self.pos_delta_buffer
    }

    pub fn payload_length_buffer(&self) -> Option<&[i32]> {
        match self.payload_length_buffer {
            Some(ref buffer) => Some(&buffer),
            None => None,
        }
    }

    pub fn offset_start_delta_buffer(&self) -> Option<&[i32]> {
        match self.offset_start_delta_buffer {
            Some(ref buffer) => Some(&buffer),
            None => None,
        }
    }
    pub fn offset_length_buffer(&self) -> Option<&[i32]> {
        match self.offset_length_buffer {
            Some(ref buffer) => Some(&buffer),
            None => None,
        }
    }

    pub fn payload_bytes(&self) -> Option<&[u8]> {
        match self.payload_bytes {
            Some(ref buffer) => Some(&buffer),
            None => None,
        }
    }
    pub fn payload_byte_upto(&self) -> i32 {
        self.payload_byte_upto
    }
    pub fn payload_length(&self) -> i32 {
        self.payload_length
    }

    pub fn last_start_offset(&self) -> i32 {
        self.last_start_offset
    }
    pub fn start_offset(&self) -> i32 {
        self.start_offset
    }
    pub fn end_offset(&self) -> i32 {
        self.end_offset
    }

    pub fn doc_buffer_upto(&self) -> i32 {
        self.doc_buffer_upto
    }
    pub fn pos_buffer_upto(&self) -> i32 {
        self.pos_buffer_upto
    }

    pub fn skipper(&self) -> Option<&Lucene50SkipReader> {
        self.skipper.as_ref()
    }
    pub fn skipped(&self) -> bool {
        self.skipped
    }

    pub fn payload(&self) -> Option<&[u8]> {
        match self.payload {
            Some(ref payload) => Some(&payload),
            None => None,
        }
    }

    pub fn is_index_has_offsets(&self) -> bool {
        self.index_has_offsets
    }
    pub fn is_index_has_payloads(&self) -> bool {
        self.index_has_payloads
    }

    pub fn doc_freq(&self) -> i32 {
        self.doc_freq
    }
    pub fn total_term_freq(&self) -> i64 {
        self.total_term_freq
    }
    pub fn doc_upto(&self) -> i32 {
        self.doc_upto
    }
    pub fn doc(&self) -> DocId {
        self.doc
    }
    pub fn accum(&self) -> i32 {
        self.accum
    }
    pub fn position(&self) -> i32 {
        self.position
    }

    pub fn pos_pending_count(&self) -> i32 {
        self.pos_pending_count
    }

    pub fn pos_pending_fp(&self) -> i64 {
        self.pos_pending_fp
    }

    pub fn pay_pending_fp(&self) -> i64 {
        self.pay_pending_fp
    }

    pub fn doc_term_start_fp(&self) -> i64 {
        self.doc_term_start_fp
    }

    pub fn pos_term_start_fp(&self) -> i64 {
        self.pos_term_start_fp
    }

    pub fn pay_term_start_fp(&self) -> i64 {
        self.pay_term_start_fp
    }

    pub fn last_pos_block_fp(&self) -> i64 {
        self.last_pos_block_fp
    }

    pub fn skip_offset(&self) -> i64 {
        self.skip_offset
    }

    pub fn next_skip_doc(&self) -> i32 {
        self.next_skip_doc
    }

    pub fn needs_offsets(&self) -> bool {
        self.needs_offsets
    }
    pub fn needs_payloads(&self) -> bool {
        self.needs_payloads
    }
    pub fn singleton_doc_id(&self) -> i32 {
        self.singleton_doc_id
    }

    pub fn reset(&mut self, term_state: &BlockTermState, flags: i16) -> Result<()> {
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

        self.needs_offsets = posting_feature_requested(flags, POSTING_ITERATOR_FLAG_OFFSETS);
        self.needs_payloads = posting_feature_requested(flags, POSTING_ITERATOR_FLAG_PAYLOADS);

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
        Ok(())
    }

    pub fn refill_docs(&mut self) -> Result<()> {
        let left = self.doc_freq - self.doc_upto;
        if left >= BLOCK_SIZE {
            let doc_in = self.doc_in.as_mut().unwrap();
            self.for_util.read_block(
                doc_in.as_mut(),
                &mut self.encoded,
                &mut self.doc_delta_buffer,
            )?;
            self.for_util
                .read_block(doc_in.as_mut(), &mut self.encoded, &mut self.freq_buffer)?;
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
            let payload_byte_upto = 0i32;
            for i in 0..count {
                let code = pos_in.read_vint()?;
                if self.index_has_payloads {
                    if (code & 1) != 0 {
                        payload_length = pos_in.read_vint()?;
                    }
                    self.payload_length_buffer.as_mut().unwrap()[i] = payload_length;
                    self.pos_delta_buffer[i] = ((code as u32) >> 1) as i32;
                    if payload_length != 0 {
                        let payload_bytes = self.payload_bytes.as_mut().unwrap();
                        if self.payload_byte_upto + payload_length > payload_bytes.len() as i32 {
                            payload_bytes.resize((payload_byte_upto + payload_length) as usize, 0);
                        }
                        pos_in.read_exact(
                            &mut payload_bytes[self.payload_byte_upto as usize
                                                   ..(self.payload_byte_upto + payload_length)
                                                       as usize],
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
                    self.offset_start_delta_buffer.as_mut().unwrap()[i] =
                        ((delta_code as u32) >> 1) as i32;
                    self.offset_length_buffer.as_mut().unwrap()[i] = offset_length;
                }
            }
            self.payload_byte_upto = 0;
        } else {
            self.for_util.read_block(
                pos_in.as_mut(),
                self.encoded.as_mut(),
                self.pos_delta_buffer.as_mut(),
            )?;

            let pay_in = &mut self.pay_in;
            if self.index_has_payloads {
                if self.needs_payloads {
                    self.for_util.read_block(
                        pay_in.as_mut(),
                        &mut self.encoded,
                        self.payload_length_buffer.as_mut().unwrap(),
                    )?;
                    let num_bytes = pay_in.read_vint()? as usize;
                    let payload_bytes = self.payload_bytes.as_mut().unwrap();
                    if num_bytes > payload_bytes.len() {
                        payload_bytes.resize(num_bytes, 0);
                    }
                    pay_in.read_exact(&mut payload_bytes[0..num_bytes])?;
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
                        self.offset_start_delta_buffer.as_mut().unwrap(),
                    )?;
                    self.for_util.read_block(
                        pay_in.as_mut(),
                        &mut self.encoded,
                        self.offset_length_buffer.as_mut().unwrap(),
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
                        self.payload_length_buffer.as_mut().unwrap()[self.pos_buffer_upto as usize];
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
                        self.payload_length_buffer.as_mut().unwrap()[self.pos_buffer_upto as usize];
                }
                self.pos_buffer_upto += 1;
            }
        }

        self.position = 0;
        self.last_start_offset = 0;
        Ok(())
    }

    pub fn get_payload(&'a self) -> Option<&'a [u8]> {
        if self.payload_length == 0 {
            None
        } else {
            Some(self.payload.as_ref().unwrap())
        }
    }

    pub fn cost(&self) -> i64 {
        i64::from(self.doc_freq)
    }

    pub fn check_integrity(&self) -> Result<()> {
        if let Some(ref doc_in) = self.doc_in {
            codec_util::checksum_entire_file(doc_in.as_ref())?;
        }

        codec_util::checksum_entire_file(self.pos_in.as_ref())?;
        codec_util::checksum_entire_file(self.pay_in.as_ref())?;
        Ok(())
    }

    fn clone(&self) -> Result<Self> {
        let doc_in = match self.doc_in {
            Some(ref doc_in) => Some((*doc_in).clone()?),
            _ => None,
        };
        let skipper = match self.skipper {
            Some(ref skipper) => Some((*skipper).clone_reader()?),
            _ => None,
        };
        Ok(EverythingIterator {
            encoded: self.encoded.clone(),
            doc_delta_buffer: self.doc_delta_buffer.clone(),
            freq_buffer: self.freq_buffer.clone(),
            pos_delta_buffer: self.pos_delta_buffer.clone(),
            payload_length_buffer: self.payload_length_buffer.clone(),
            offset_start_delta_buffer: self.offset_start_delta_buffer.clone(),
            offset_length_buffer: self.offset_length_buffer.clone(),
            payload_bytes: self.payload_bytes.clone(),
            payload_byte_upto: self.payload_byte_upto,
            payload_length: self.payload_length,
            last_start_offset: self.last_start_offset,
            start_offset: self.start_offset,
            end_offset: self.end_offset,
            doc_buffer_upto: self.doc_buffer_upto,
            pos_buffer_upto: self.pos_buffer_upto,
            skipper,
            skipped: self.skipped,
            start_doc_in: self.start_doc_in.clone()?,
            doc_in,
            pos_in: self.pos_in.clone()?,
            pay_in: self.pay_in.clone()?,
            payload: self.payload.clone(),
            index_has_offsets: self.index_has_offsets,
            index_has_payloads: self.index_has_payloads,
            doc_freq: self.doc_freq,
            total_term_freq: self.total_term_freq,
            doc_upto: self.doc_upto,
            doc: self.doc,
            accum: self.accum,
            freq: self.freq,
            position: self.position,
            pos_pending_count: self.pos_pending_count,
            pos_pending_fp: self.pos_pending_fp,
            pay_pending_fp: self.pay_pending_fp,
            doc_term_start_fp: self.doc_term_start_fp,
            pos_term_start_fp: self.pos_term_start_fp,
            pay_term_start_fp: self.pay_term_start_fp,
            last_pos_block_fp: self.last_pos_block_fp,
            skip_offset: self.skip_offset,
            next_skip_doc: self.next_skip_doc,
            needs_offsets: self.needs_offsets,
            needs_payloads: self.needs_payloads,
            singleton_doc_id: self.singleton_doc_id,
            for_util: self.for_util.clone(),
            segment: self.segment.clone(),
            term: self.term.clone(),
        })
    }
}

impl PostingIterator for EverythingIterator {
    fn clone_as_doc_iterator(&self) -> Result<Box<DocIterator>> {
        Ok(Box::new(self.clone()?))
    }

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
            debug_assert!(self.payload_length_buffer.is_some());
            debug_assert!(self.payload_bytes.is_some());
            self.payload_length =
                self.payload_length_buffer.as_ref().unwrap()[self.pos_buffer_upto as usize];
            let payload_offset = self.payload_byte_upto as usize;
            let payload_end = payload_offset + self.payload_length as usize;
            self.payload =
                Some(self.payload_bytes.as_ref().unwrap()[payload_offset..payload_end].to_vec());
            self.payload_byte_upto += self.payload_length;
        }

        if self.index_has_offsets {
            debug_assert!(self.offset_start_delta_buffer.is_some());
            debug_assert!(self.offset_length_buffer.is_some());
            self.start_offset = self.last_start_offset
                + self.offset_start_delta_buffer.as_ref().unwrap()[self.pos_buffer_upto as usize];
            self.end_offset = self.start_offset
                + self.offset_length_buffer.as_ref().unwrap()[self.pos_buffer_upto as usize];
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
        Ok(if self.payload_length == 0 {
            Payload::new()
        } else {
            debug_assert!(self.payload.is_some());
            self.payload.as_ref().unwrap().clone()
        })
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

        self.accum += self.doc_delta_buffer[self.doc_buffer_upto as usize];
        self.freq = self.freq_buffer[self.doc_buffer_upto as usize];
        self.pos_pending_count += self.freq;
        self.doc_buffer_upto += 1;
        self.doc_upto += 1;

        self.doc = self.accum;
        self.position = 0;
        self.last_start_offset = 0;
        Ok(self.doc)
    }

    fn advance(&mut self, target: DocId) -> Result<DocId> {
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

        // Now scan:
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

        self.position = 0;
        self.last_start_offset = 0;
        self.doc = self.accum;
        Ok(self.doc)
    }

    fn cost(&self) -> usize {
        self.doc_freq as usize
    }
}
