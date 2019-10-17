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

use core::codec::postings::blocktree::{BlockTermState, SegmentTermIteratorInner, MAX_LONGS_SIZE};
use core::codec::postings::lucene50_decode_term;
use core::codec::SeekStatus;
use core::doc::IndexOptions;
use core::store::io::{ByteArrayDataInput, DataInput};
use core::util::fst::{Arc, ByteSequenceOutput};
use core::util::BytesRef;
use core::util::UnsignedShift;

use error::Result;

use std::cmp::Ordering;
use std::io::Read;
use std::ptr;

pub struct SegmentTermsIterFrame {
    pub ord: usize,
    pub has_terms: bool,
    pub has_terms_orig: bool,
    pub is_floor: bool,
    pub arc: Option<Arc<ByteSequenceOutput>>,
    version_auto_prefix: bool,
    // File pointer where this block was loaded from
    pub fp: i64,
    pub fp_orig: i64,
    pub fp_end: i64,
    suffix_bytes: Vec<u8>,
    pub suffixes_reader: ByteArrayDataInput<BytesRef>,
    stat_bytes: Vec<u8>,
    pub stats_reader: ByteArrayDataInput<BytesRef>,
    floor_data: Vec<u8>,
    floor_data_reader: ByteArrayDataInput<BytesRef>,
    // Length of prefix shared by all terms in this block
    pub prefix: usize,
    // Number of entries (term or sub-block) in this block
    pub ent_count: i32,
    // Which term we will next read, or -1 if the block isn't loaded yet
    pub next_ent: i32,
    // True if this block is either not a floor block,
    // or, it's the last sub-block of a floor block
    pub is_last_in_floor: bool,
    // True if all entries are terms
    pub is_leaf_block: bool,
    pub last_sub_fp: i64,
    next_floor_label: i32,
    num_follow_floor_blocks: i32,
    // Next term to decode metadata; we decode metadata lazily so that
    // scanning fo find the matching term is fast and only if you find
    // a match and app wants the stats or docs/positions iterators,
    // will we decode the metadata
    pub metadata_upto: i32,
    pub state: BlockTermState,
    // metadata bufffer, holding monotonic values
    longs: [i64; MAX_LONGS_SIZE],
    bytes: Vec<u8>,
    bytes_reader: ByteArrayDataInput<BytesRef>,
    // NOTE: this is pointer to parent, because of set is always Boxed,
    // thus this pointer will be safe
    pub ste: *mut SegmentTermIteratorInner,
    start_byte_pos: usize,
    suffix: usize,
    sub_code: i64,
}

impl Default for SegmentTermsIterFrame {
    fn default() -> Self {
        SegmentTermsIterFrame {
            ord: 0,
            has_terms: false,
            has_terms_orig: false,
            is_floor: false,
            arc: None,
            version_auto_prefix: false,
            fp: 0,
            fp_orig: 0,
            fp_end: 0,
            suffix_bytes: vec![0; 128],
            suffixes_reader: ByteArrayDataInput::new(BytesRef::default()),
            stat_bytes: vec![0; 64],
            stats_reader: ByteArrayDataInput::new(BytesRef::default()),
            floor_data: vec![0; 32],
            floor_data_reader: ByteArrayDataInput::new(BytesRef::default()),
            prefix: 0,
            ent_count: 0,
            next_ent: 0,
            is_last_in_floor: false,
            is_leaf_block: false,
            last_sub_fp: 0,
            next_floor_label: 0,
            num_follow_floor_blocks: 0,
            metadata_upto: 0,
            state: BlockTermState::new(),
            longs: [0; MAX_LONGS_SIZE],
            bytes: vec![0; 32],
            bytes_reader: ByteArrayDataInput::new(BytesRef::default()),
            ste: ptr::null_mut(),
            start_byte_pos: 0,
            suffix: 0,
            sub_code: 0,
        }
    }
}

impl SegmentTermsIterFrame {
    pub fn new(ste: &mut SegmentTermIteratorInner, ord: usize) -> Self {
        let mut frame = Self::default();
        frame.init(ste, ord);
        frame
    }

    pub fn init(&mut self, ste: &mut SegmentTermIteratorInner, ord: usize) {
        {
            let fr = ste.field_reader();
            let mut state = fr.parent().postings_reader().new_term_state();
            state.total_term_freq = -1;
            self.state = state;
            self.version_auto_prefix = fr.parent().is_any_auto_prefix_terms();
        }
        self.ste = ste;
        self.ord = ord;
    }

    #[allow(clippy::mut_from_ref)]
    fn terms_iter(&self) -> &mut SegmentTermIteratorInner {
        unsafe { &mut *self.ste }
    }

    pub fn set_floor_data<'a>(
        &mut self,
        input: &mut ByteArrayDataInput<&'a [u8]>,
        source: &[u8],
    ) -> Result<()> {
        let pos = input.position();
        let num_bytes = source.len() - pos;
        debug_assert!(num_bytes > 0);
        self.floor_data.resize(num_bytes, 0);
        self.floor_data.copy_from_slice(&source[pos..]);
        self.floor_data_reader
            .reset(BytesRef::new(&self.floor_data));
        self.num_follow_floor_blocks = self.floor_data_reader.read_vint()?;
        self.next_floor_label = self.floor_data_reader.read_byte()? as u32 as i32;
        Ok(())
    }

    pub fn get_term_block_ord(&self) -> i32 {
        if self.is_leaf_block {
            self.next_ent
        } else {
            self.state.term_block_ord
        }
    }

    pub fn load_next_floor_block(&mut self) -> Result<()> {
        debug_assert!(self.arc.is_none() || self.is_floor);
        self.fp = self.fp_end;
        self.next_ent = -1;
        self.load_block()
    }

    // Does initial decode of next block of terms; this
    // doesn't actually decode the docFreq, totalTermFreq,
    // postings details (frq/prx offset, etc.) metadata;
    // it just loads them as bytes blobs which are then
    // decoded on-demand if the metadata is ever requested
    // for any term in this block.  This enables terms-only
    // intensive consumes (eg certain MTQs, respelling) to
    // not pay the price of decoding metadata they won't
    // use.
    pub fn load_block(&mut self) -> Result<()> {
        // Clone the IndexInput lazily, so that consumers
        // that just pull a TermsEnum to
        // seekExact(TermState) don't pay this cost:
        self.terms_iter().init_index_input()?;
        if self.next_ent != -1 {
            // Already loaded
            return Ok(());
        }
        self.terms_iter().input.as_mut().unwrap().seek(self.fp)?;
        let mut code = self.terms_iter().input.as_mut().unwrap().read_vint()?;
        self.ent_count = code.unsigned_shift(1);
        debug_assert!(self.ent_count > 0);
        self.is_last_in_floor = (code & 1) != 0;

        debug_assert!(self.arc.is_none() || (self.is_floor || self.is_last_in_floor));

        // TODO: if suffixes were stored in random-access
        // array structure, then we could do binary search
        // instead of linear scan to find target term; eg
        // we could have simple array of offsets

        // term suffixes:
        code = self.terms_iter().input.as_mut().unwrap().read_vint()?;
        self.is_leaf_block = (code & 1) != 0;
        let num_bytes = code.unsigned_shift(1) as usize;
        self.suffix_bytes.resize(num_bytes, 0);
        unsafe {
            (*self.ste)
                .input
                .as_mut()
                .unwrap()
                .read_exact(&mut self.suffix_bytes)?;
        }
        self.suffixes_reader
            .reset(BytesRef::new(&self.suffix_bytes));

        // stats
        let num_bytes = self.terms_iter().input.as_mut().unwrap().read_vint()? as usize;
        self.stat_bytes.resize(num_bytes, 0);
        unsafe {
            (*self.ste)
                .input
                .as_mut()
                .unwrap()
                .read_exact(&mut self.stat_bytes)?;
        }
        self.stats_reader.reset(BytesRef::new(&self.stat_bytes));
        self.metadata_upto = 0;

        self.state.term_block_ord = 0;
        self.next_ent = 0;
        self.last_sub_fp = -1;

        // TODO: we could skip this if !hasTerms; but
        // that's rare so won't help much
        // metadata
        let num_bytes = self.terms_iter().input.as_mut().unwrap().read_vint()? as usize;
        self.bytes.resize(num_bytes, 0);
        unsafe {
            (*self.ste)
                .input
                .as_mut()
                .unwrap()
                .read_exact(&mut self.bytes)?;
        }
        self.bytes_reader.reset(BytesRef::new(&self.bytes));

        // Sub-blocks of a single floor block are always
        // written one after another -- tail recurse:
        self.fp_end = self.terms_iter().input.as_ref().unwrap().file_pointer();
        Ok(())
    }

    pub fn rewind(&mut self) {
        self.fp = self.fp_orig;
        self.next_ent = -1;
        self.has_terms = self.has_terms_orig;
        if self.is_floor {
            self.floor_data_reader.rewind();
            self.num_follow_floor_blocks = self.floor_data_reader.read_vint().unwrap();
            debug_assert!(self.num_follow_floor_blocks > 0);
            self.next_floor_label = self.floor_data_reader.read_byte().unwrap() as u32 as i32;
        }
    }

    // Decodes next entry; returns true if it's a sub-block
    pub fn next(&mut self) -> Result<bool> {
        if self.is_leaf_block {
            self.next_leaf()?;
            Ok(false)
        } else {
            self.next_non_leaf()
        }
    }

    pub fn next_leaf(&mut self) -> Result<()> {
        debug_assert!(self.next_ent != -1 && self.next_ent < self.ent_count);
        self.next_ent += 1;
        self.suffix = self.suffixes_reader.read_vint()? as usize;
        self.start_byte_pos = self.suffixes_reader.position();
        self.terms_iter().resize_term(self.prefix + self.suffix);
        self.suffixes_reader.read_bytes(
            unsafe { &mut (*self.ste).term },
            self.prefix,
            self.suffix,
        )?;
        self.terms_iter().term_exists = true;
        Ok(())
    }

    pub fn next_non_leaf(&mut self) -> Result<bool> {
        loop {
            if self.next_ent == self.ent_count {
                debug_assert!(self.arc.is_none() || (self.is_floor && !self.is_last_in_floor));
                self.load_next_floor_block()?;
                if self.is_leaf_block {
                    self.next_leaf()?;
                    return Ok(false);
                } else {
                    continue;
                }
            }

            debug_assert!(self.next_ent != -1 && self.next_ent < self.ent_count);
            self.next_ent += 1;
            let code = self.suffixes_reader.read_vint()?;
            if !self.version_auto_prefix {
                self.suffix = code.unsigned_shift(1) as usize;
                self.start_byte_pos = self.suffixes_reader.position();
                self.terms_iter().resize_term(self.prefix + self.suffix);
                self.suffixes_reader.read_bytes(
                    unsafe { &mut (*self.ste).term },
                    self.prefix,
                    self.suffix,
                )?;
                if (code & 1) == 0 {
                    // A normal term
                    self.terms_iter().term_exists = true;
                    self.sub_code = 0;
                    self.state.term_block_ord += 1;
                    return Ok(false);
                } else {
                    // A sub-block; make sub-FP absolute:
                    self.terms_iter().term_exists = false;
                    self.sub_code = self.suffixes_reader.read_vlong()?;
                    self.last_sub_fp = self.fp - self.sub_code;
                    return Ok(true);
                }
            } else {
                self.suffix = code.unsigned_shift(2) as usize;
                self.start_byte_pos = self.suffixes_reader.position();
                self.terms_iter().resize_term(self.prefix + self.suffix);
                self.suffixes_reader.read_bytes(
                    unsafe { &mut (*self.ste).term },
                    self.prefix,
                    self.suffix,
                )?;
                match code & 3 {
                    0 => {
                        // A normal term
                        self.terms_iter().term_exists = true;
                        self.sub_code = 0;
                        self.state.term_block_ord += 1;
                        return Ok(false);
                    }
                    1 => {
                        // A sub-block; make sub-FP absolute:
                        self.terms_iter().term_exists = false;
                        self.sub_code = self.suffixes_reader.read_vlong()?;
                        self.last_sub_fp = self.fp - self.sub_code;
                        return Ok(true);
                    }
                    2 | 3 => {
                        // A prefix term: skip it
                        self.state.term_block_ord += 1;
                        self.suffixes_reader.read_byte()?;
                    }
                    // just to make the compiler happy
                    _ => unreachable!(),
                }
            }
        }
    }

    // TODO: make this array'd so we can do bin search?
    // likely not worth it?  need to measure how many
    // floor blocks we "typically" get
    pub fn scan_to_floor_frame(&mut self, target: &[u8]) -> Result<()> {
        if !self.is_floor || target.len() <= self.prefix {
            return Ok(());
        }

        let target_label = target[self.prefix] as u32 as i32;

        if target_label < self.next_floor_label {
            return Ok(());
        }

        debug_assert!(self.num_follow_floor_blocks > 0);
        let mut new_fp;
        loop {
            let code = self.floor_data_reader.read_vlong()?;
            new_fp = self.fp_orig + (code.unsigned_shift(1));
            self.has_terms = (code & 1) != 0;
            self.is_last_in_floor = self.num_follow_floor_blocks == 1;
            self.num_follow_floor_blocks -= 1;
            if self.is_last_in_floor {
                self.next_floor_label = 256;
                break;
            } else {
                self.next_floor_label = self.floor_data_reader.read_byte()? as u32 as i32;
                if target_label < self.next_floor_label {
                    break;
                }
            }
        }

        if new_fp != self.fp {
            self.next_ent = -1;
            self.fp = new_fp;
        }

        Ok(())
    }

    pub fn decode_metadata(&mut self) -> Result<()> {
        // lazily catch up on metadata decode:
        let limit = self.get_term_block_ord();
        let mut absolute = self.metadata_upto == 0;
        debug_assert!(limit > 0);

        // TODO: better API would be "jump straight to term=N"???
        while self.metadata_upto < limit {
            // TODO: we could make "tiers" of metadata, ie,
            // decode docFreq/totalTF but don't decode postings
            // metadata; this way caller could get
            // docFreq/totalTF w/o paying decode cost for
            // postings

            // TODO: if docFreq were bulk decoded we could
            // just skipN here:

            // stats
            self.state.doc_freq = self.stats_reader.read_vint()?;
            if self.terms_iter().field_reader().field_info().index_options != IndexOptions::Docs {
                self.state.total_term_freq =
                    self.state.doc_freq as i64 + self.stats_reader.read_vlong()?;
            }

            // metadata
            for i in 0..self.terms_iter().field_reader().longs_size() {
                self.longs[i] = self.bytes_reader.read_vlong()?;
            }
            lucene50_decode_term(
                self.longs.as_ref(),
                &mut self.bytes_reader,
                unsafe { (*self.ste).field_reader().field_info() },
                &mut self.state,
                absolute,
            )?;
            self.metadata_upto += 1;
            absolute = false;
        }
        self.state.term_block_ord = self.metadata_upto;
        Ok(())
    }

    // Used only by assert
    fn prefix_matches(&self, target: &[u8]) -> bool {
        target[..self.prefix] == self.terms_iter().term[..self.prefix]
    }

    // Scans to sub-block that has this target fp; only
    // called by next(); NOTE: does not set
    // startBytePos/suffix as a side effect
    pub fn scan_to_sub_block(&mut self, sub_fp: i64) -> Result<()> {
        debug_assert!(!self.is_leaf_block);
        if self.last_sub_fp == sub_fp {
            return Ok(());
        }

        debug_assert!(sub_fp < self.fp);
        let target_sub_code = self.fp - sub_fp;
        loop {
            debug_assert!(self.next_ent < self.ent_count);
            self.next_ent += 1;
            let code = self.suffixes_reader.read_vint()?;
            if !self.version_auto_prefix {
                self.suffixes_reader
                    .skip_bytes(code.unsigned_shift(1) as usize)?;
                if (code & 1) != 0 {
                    let sub_code = self.suffixes_reader.read_vlong()?;
                    if target_sub_code == sub_code {
                        self.last_sub_fp = sub_fp;
                        return Ok(());
                    }
                } else {
                    self.state.term_block_ord += 1;
                }
            } else {
                let flag = code & 3;
                self.suffixes_reader
                    .skip_bytes(code.unsigned_shift(2) as usize)?;
                if flag == 1 {
                    let sub_code = self.suffixes_reader.read_vlong()?;
                    if target_sub_code == sub_code {
                        self.last_sub_fp = sub_fp;
                        return Ok(());
                    }
                } else {
                    self.state.term_block_ord += 1;
                    if flag == 2 || flag == 3 {
                        // Floor'd prefix term
                        self.suffixes_reader.read_byte()?;
                    }
                }
            }
        }
    }

    // NOTE: sets start_byte_pos/suffix as a side effect
    pub fn scan_to_term(&mut self, target: &[u8], exact_only: bool) -> Result<SeekStatus> {
        if self.is_leaf_block {
            self.scan_to_term_leaf(target, exact_only)
        } else {
            self.scan_to_term_non_leaf(target, exact_only)
        }
    }

    // Target's prefix matches this block's prefix; we
    // scan the entries check if the suffix matches.
    pub fn scan_to_term_leaf(&mut self, target: &[u8], exact_only: bool) -> Result<SeekStatus> {
        debug_assert!(self.next_ent != -1);
        self.terms_iter().term_exists = true;
        self.sub_code = 0;

        if self.next_ent == self.ent_count {
            if exact_only {
                self.fill_term();
            }
            return Ok(SeekStatus::End);
        }

        debug_assert!(self.prefix_matches(target));

        // Loop over each entry (term or sub-block) in this block:
        'next_term: loop {
            self.next_ent += 1;

            self.suffix = self.suffixes_reader.read_vint()? as usize;

            let term_len = self.prefix + self.suffix;
            self.start_byte_pos = self.suffixes_reader.position();
            self.suffixes_reader.skip_bytes(self.suffix)?;

            let target_limit = target.len().min(term_len);
            let mut target_pos = self.prefix;

            // Loop over bytes in the suffix, comparing to
            // the target
            let mut byte_pos = self.start_byte_pos;
            loop {
                let (cmp, stop) = if target_pos < target_limit {
                    let c = self.suffix_bytes[byte_pos].cmp(&target[target_pos]);
                    byte_pos += 1;
                    target_pos += 1;
                    (c, false)
                } else {
                    debug_assert_eq!(target_pos, target_limit);
                    (term_len.cmp(&target.len()), true)
                };

                if cmp == Ordering::Less {
                    // Current entry is still before the target;
                    // keep scanning
                    if self.next_ent == self.ent_count {
                        // We are done scanning this block
                        break 'next_term;
                    } else {
                        continue 'next_term;
                    }
                } else if cmp == Ordering::Greater {
                    // Done!  Current entry is after target --
                    self.fill_term();
                    return Ok(SeekStatus::NotFound);
                } else if stop {
                    // Exact match!

                    // This cannot be a sub-block because we
                    // would have followed the index to this
                    // sub-block from the start:
                    debug_assert!(self.terms_iter().term_exists);
                    self.fill_term();
                    return Ok(SeekStatus::Found);
                }
            }
        }

        // It is possible (and OK) that terms index pointed us
        // at this block, but, we scanned the entire block and
        // did not find the term to position to.  This happens
        // when the target is after the last term in the block
        // (but, before the next term in the index).  EG
        // target could be foozzz, and terms index pointed us
        // to the foo* block, but the last term in this block
        // was fooz (and, eg, first term in the next block will
        // bee fop).
        if exact_only {
            self.fill_term();
        }

        // TODO: not consistent that in the
        // not-exact case we don't next() into the next
        // frame here
        Ok(SeekStatus::End)
    }

    // Target's prefix matches this block's prefix; we
    // scan the entries check if the suffix matches.
    pub fn scan_to_term_non_leaf(&mut self, target: &[u8], exact_only: bool) -> Result<SeekStatus> {
        debug_assert_ne!(self.next_ent, -1);

        if self.next_ent == self.ent_count {
            if exact_only {
                self.fill_term();
                self.terms_iter().term_exists = self.sub_code == 0;
            }
            return Ok(SeekStatus::End);
        }

        debug_assert!(self.prefix_matches(target));

        // Loop over each entry (term or sub-block) in this block:
        'next_term: while self.next_ent < self.ent_count {
            self.next_ent += 1;

            let code = self.suffixes_reader.read_vint()?;
            self.suffix = if !self.version_auto_prefix {
                code.unsigned_shift(1) as usize
            } else {
                code.unsigned_shift(2) as usize
            };

            let term_len = self.prefix + self.suffix;
            self.start_byte_pos = self.suffixes_reader.position();
            self.suffixes_reader.skip_bytes(self.suffix)?;
            if !self.version_auto_prefix {
                self.terms_iter().term_exists = (code & 1) == 0;
                if self.terms_iter().term_exists {
                    self.state.term_block_ord += 1;
                    self.sub_code = 0;
                } else {
                    self.sub_code = self.suffixes_reader.read_vlong()?;
                    self.last_sub_fp = self.fp - self.sub_code;
                }
            } else {
                match code & 3 {
                    0 => {
                        // A normal term
                        self.terms_iter().term_exists = true;
                        self.sub_code = 0;
                        self.state.term_block_ord += 1;
                        break;
                    }
                    1 => {
                        // A sub-block; make sub-FP absolute:
                        self.terms_iter().term_exists = false;
                        self.sub_code = self.suffixes_reader.read_vlong()?;
                        self.last_sub_fp = self.fp - self.sub_code;
                        break;
                    }
                    2 | 3 => {
                        // Floor prefix term: skip it
                        self.state.term_block_ord += 1;
                        self.suffixes_reader.read_byte()?;
                        self.terms_iter().term_exists = false;
                        continue;
                    }
                    _ => unreachable!(),
                }
            }

            let target_limit = target.len().min(term_len);
            let mut target_pos = self.prefix;

            // Loop over bytes in the suffix, comparing to
            // the target
            let mut byte_pos = self.start_byte_pos;
            loop {
                let (cmp, stop) = if target_pos < target_limit {
                    let c = self.suffix_bytes[byte_pos].cmp(&target[target_pos]);
                    byte_pos += 1;
                    target_pos += 1;
                    (c, false)
                } else {
                    debug_assert_eq!(target_pos, target_limit);
                    (term_len.cmp(&target.len()), true)
                };

                if cmp == Ordering::Less {
                    // Current entry is still before the target;
                    // keep scanning
                    continue 'next_term;
                } else if cmp == Ordering::Greater {
                    // Done!  Current entry is after target --
                    self.fill_term();
                    if !exact_only && !self.terms_iter().term_exists {
                        // TODO this
                        // We are on a sub-block, and caller wants
                        // us to position to the next term after
                        // the target, so we must recurse into the
                        // sub-frame(s):
                        let terms_iter = self.terms_iter();
                        let last_sub_fp = terms_iter.current_frame().last_sub_fp;
                        terms_iter.current_frame_ord =
                            terms_iter.push_frame_by_fp(None, last_sub_fp, term_len)?;
                        debug_assert!(terms_iter.current_frame_ord > 0);
                        let mut current_idx = terms_iter.current_frame_ord as usize;
                        terms_iter.stack[current_idx].load_block()?;
                        while terms_iter.stack[current_idx].next()? {
                            let last_sub_fp = terms_iter.stack[current_idx].last_sub_fp;
                            let len = terms_iter.term_len;
                            terms_iter.current_frame_ord =
                                terms_iter.push_frame_by_fp(None, last_sub_fp, len)?;
                            current_idx = terms_iter.current_frame_ord as usize;
                            terms_iter.stack[current_idx].load_block()?;
                        }
                    }

                    return Ok(SeekStatus::NotFound);
                } else if stop {
                    // Exact match!

                    // This cannot be a sub-block because we
                    // would have followed the index to this
                    // sub-block from the start:
                    debug_assert!(self.terms_iter().term_exists);
                    self.fill_term();
                    return Ok(SeekStatus::Found);
                }
            }
        }

        // It is possible (and OK) that terms index pointed us
        // at this block, but, we scanned the entire block and
        // did not find the term to position to.  This happens
        // when the target is after the last term in the block
        // (but, before the next term in the index).  EG
        // target could be foozzz, and terms index pointed us
        // to the foo* block, but the last term in this block
        // was fooz (and, eg, first term in the next block will
        // bee fop).
        if exact_only {
            self.fill_term();
        }
        // TODO: not consistent that in the
        // not-exact case we don't next() into the next
        // frame here
        Ok(SeekStatus::End)
    }

    fn fill_term(&mut self) {
        let term_length = self.prefix + self.suffix;
        self.terms_iter().resize_term(term_length);
        self.terms_iter().term[self.prefix..].copy_from_slice(
            &self.suffix_bytes[self.start_byte_pos..self.start_byte_pos + self.suffix],
        );
    }
}
