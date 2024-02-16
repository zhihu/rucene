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

use core::codec::field_infos::{FieldInfo, FieldInfosBuilder, FieldInvertState, FieldNumbersRef};
use core::codec::norms::NormsProducer;
use core::codec::postings::{FieldsConsumer, PostingsFormat};
use core::codec::postings::{FreqProxTermsWriterPerField, TermsHashPerField};
use core::codec::segment_infos::SegmentWriteState;
use core::codec::term_vectors::TermVectorsConsumer;
use core::codec::Codec;
use core::codec::PackedLongDocMap;
use core::codec::{Fields, SeekStatus, TermIterator, Terms};
use core::codec::{PostingIterator, PostingIteratorFlags};
use core::doc::IndexOptions;
use core::index::merge::{MergePolicy, MergeScheduler};
use core::index::reader::SortingFields;
use core::index::writer::DocumentsWriterPerThread;
use core::search::{DocIterator, Payload, NO_MORE_DOCS};
use core::store::directory::Directory;
use core::store::io::DataInput;
use core::util::IntBlockPool;
use core::util::{BitSet, FixedBitSet};
use core::util::{Bits, BytesRef, DocId};
use core::util::{ByteBlockAllocator, ByteBlockPool, ByteSliceReader};

use error::{ErrorKind, Result};

use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::io::Read;
use std::ptr;
use std::sync::Arc;

/// This class is passed each token produced by the analyzer
/// on each field during indexing, and it stores these
/// tokens in a hash table, and allocates separate byte
/// streams per token.  Consumers of this class, eg {@link
/// FreqProxTermsWriter} and {@link TermVectorsConsumer},
/// write their own byte streams under each term.
pub struct TermsHashBase {
    // next_terms_hash: TermsHash,
    pub int_pool: IntBlockPool,
    pub byte_pool: ByteBlockPool,
    pub term_byte_pool: *mut ByteBlockPool,
}

impl Default for TermsHashBase {
    fn default() -> Self {
        TermsHashBase {
            int_pool: IntBlockPool::default(),
            byte_pool: ByteBlockPool::default(),
            term_byte_pool: ptr::null_mut(),
        }
    }
}

impl TermsHashBase {
    pub fn new<D, C, MS, MP>(doc_writer: &mut DocumentsWriterPerThread<D, C, MS, MP>) -> Self
    where
        D: Directory + Send + Sync + 'static,
        C: Codec,
        MS: MergeScheduler,
        MP: MergePolicy,
    {
        TermsHashBase {
            int_pool: IntBlockPool::new(doc_writer.int_block_allocator.shallow_copy()),
            byte_pool: ByteBlockPool::new(doc_writer.byte_block_allocator.shallow_copy()),
            term_byte_pool: ptr::null_mut(),
        }
    }

    // clear all state
    pub fn reset(&mut self) {
        // we don't reuse so we drop everything and don't fill with 0
        self.int_pool.reset(false, false);
        self.byte_pool.reset(false, false);
    }

    pub fn need_flush(&self) -> bool {
        self.int_pool.need_flush
    }
}

pub trait TermsHash<D: Directory, C: Codec> {
    type PerField: TermsHashPerField;
    fn base(&self) -> &TermsHashBase;
    fn base_mut(&mut self) -> &mut TermsHashBase;

    fn add_field(
        &mut self,
        field_invert_state: &FieldInvertState,
        field_info: &FieldInfo,
    ) -> Self::PerField;

    fn flush<DW: Directory>(
        &mut self,
        field_to_flush: BTreeMap<&str, &Self::PerField>,
        state: &mut SegmentWriteState<D, DW, C>,
        sort_map: Option<&Arc<PackedLongDocMap>>,
        norms: Option<&impl NormsProducer>,
    ) -> Result<()>;

    fn abort(&mut self) -> Result<()> {
        self.base_mut().reset();
        Ok(())
    }

    fn start_document(&mut self) -> Result<()>;

    fn finish_document(
        &mut self,
        field_infos: &mut FieldInfosBuilder<FieldNumbersRef>,
    ) -> Result<()>;
}

pub struct FreqProxTermsWriter<
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
> {
    pub base: TermsHashBase,
    pub next_terms_hash: TermVectorsConsumer<D, C, MS, MP>,
}

impl<D, C, MS, MP> FreqProxTermsWriter<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    pub fn new(
        doc_writer: &mut DocumentsWriterPerThread<D, C, MS, MP>,
        term_vectors: TermVectorsConsumer<D, C, MS, MP>,
    ) -> Self {
        let base = TermsHashBase::new(doc_writer);
        FreqProxTermsWriter {
            base,
            next_terms_hash: term_vectors,
        }
    }

    pub fn init(&mut self) {
        self.base.term_byte_pool = &mut self.base.byte_pool;
        self.next_terms_hash
            .set_term_bytes_pool(&mut self.base.byte_pool);
        self.next_terms_hash.set_inited(true);
    }

    pub fn need_flush(&self) -> bool {
        self.base.need_flush()
    }
}

fn apply_deletes<D: Directory, DW: Directory, C: Codec>(
    state: &mut SegmentWriteState<D, DW, C>,
    fields: &impl Fields,
) -> Result<()> {
    // process any pending Term deletes for this newly flushed segment:
    if state.seg_updates.is_some() && !state.seg_updates().deleted_terms.is_empty() {
        let mut deleted_terms = Vec::with_capacity(state.seg_updates().deleted_terms.len());
        for k in state.seg_updates().deleted_terms.keys() {
            deleted_terms.push(k.clone());
        }
        deleted_terms.sort();

        let mut last_field = "";
        let mut terms_iter = None;
        for delete_term in &deleted_terms {
            if delete_term.field() != last_field {
                last_field = delete_term.field();
                terms_iter = if let Some(terms) = fields.terms(last_field)? {
                    Some(terms.iterator()?)
                } else {
                    None
                };
            }

            if let Some(ref mut iter) = terms_iter {
                if iter.seek_exact(&delete_term.bytes)? {
                    let mut postings_iter = iter.postings_with_flags(0)?;
                    let del_doc_limit = state.seg_updates().deleted_terms[delete_term];
                    debug_assert!(del_doc_limit < NO_MORE_DOCS);
                    loop {
                        let doc = postings_iter.next()?;
                        if doc < del_doc_limit {
                            let size = state.segment_info.max_doc as usize;
                            if state.live_docs.is_empty() {
                                state.live_docs = FixedBitSet::new(size);
                                state.live_docs.batch_set(0, size);
                            }
                            debug_assert!(state.live_docs.len() >= size);
                            if state.live_docs.get(doc as usize)? {
                                state.del_count_on_flush += 1;
                                state.live_docs.clear(doc as usize);
                            }
                        } else {
                            break;
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

impl<D, C, MS, MP> TermsHash<D, C> for FreqProxTermsWriter<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    type PerField = FreqProxTermsWriterPerField<D, C, MS, MP>;
    fn base(&self) -> &TermsHashBase {
        &self.base
    }

    fn base_mut(&mut self) -> &mut TermsHashBase {
        &mut self.base
    }

    fn add_field(
        &mut self,
        field_invert_state: &FieldInvertState,
        field_info: &FieldInfo,
    ) -> FreqProxTermsWriterPerField<D, C, MS, MP> {
        let next_field = self
            .next_terms_hash
            .add_field(field_invert_state, field_info);
        FreqProxTermsWriterPerField::new(&mut self.base, field_info.clone(), next_field)
    }

    fn flush<DW: Directory>(
        &mut self,
        field_to_flush: BTreeMap<&str, &Self::PerField>,
        state: &mut SegmentWriteState<D, DW, C>,
        sort_map: Option<&Arc<PackedLongDocMap>>,
        norms: Option<&impl NormsProducer>,
    ) -> Result<()> {
        let mut next_child_fields = BTreeMap::new();
        for (k, v) in &field_to_flush {
            next_child_fields.insert(*k, &(*v).next_per_field);
        }
        self.next_terms_hash
            .flush(next_child_fields, state, sort_map, norms)?;

        // Gather all fields that saw any positions:
        let mut all_fields = Vec::with_capacity(field_to_flush.len());
        for (_, f) in field_to_flush {
            unsafe {
                if !f.base().bytes_hash.assume_init_ref().is_empty() {
                    // TODO: Hack logic, it's because it's hard to gain param `field_to_flush` as
                    // `HashMap<&str, &mut FreqProxTermsWriterPerField>`
                    // this should be fixed later
                    let pf_ptr = f as *const FreqProxTermsWriterPerField<D, C, MS, MP>
                        as *mut FreqProxTermsWriterPerField<D, C, MS, MP>;
                    (*pf_ptr).base_mut().sort_postings();

                    debug_assert_ne!(f.base().field_info.index_options, IndexOptions::Null);
                    all_fields.push(f);
                }
            }
        }

        if !all_fields.is_empty() {
            // sort by field name
            all_fields.sort();
            let fields = FreqProxFields::new(all_fields);

            apply_deletes(state, &fields)?;

            let mut consumer = state
                .segment_info
                .codec()
                .postings_format()
                .fields_consumer(state)?;

            if let Some(sort_map) = sort_map {
                let fields = SortingFields::new(
                    fields,
                    Arc::new(state.field_infos.clone()),
                    Arc::clone(sort_map),
                );
                consumer.write(&fields)
            } else {
                consumer.write(&fields)
            }
        } else {
            Ok(())
        }
    }

    fn abort(&mut self) -> Result<()> {
        self.base.reset();
        self.next_terms_hash.abort()
    }

    fn start_document(&mut self) -> Result<()> {
        self.next_terms_hash.start_document()
    }

    fn finish_document(
        &mut self,
        field_infos: &mut FieldInfosBuilder<FieldNumbersRef>,
    ) -> Result<()> {
        self.next_terms_hash.finish_document(field_infos)
    }
}

/// Implements limited (iterators only, no stats) `Fields`
/// interface over the in-RAM buffered fields/terms/postings,
/// to flush postings through the PostingsFormat
struct FreqProxFields<
    'a,
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
> {
    fields: BTreeMap<String, &'a FreqProxTermsWriterPerField<D, C, MS, MP>>,
}

impl<'a, D, C, MS, MP> FreqProxFields<'a, D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    fn new(field_list: Vec<&'a FreqProxTermsWriterPerField<D, C, MS, MP>>) -> Self {
        let mut fields = BTreeMap::new();
        for f in field_list {
            fields.insert(f.base().field_info.name.clone(), f);
        }
        FreqProxFields { fields }
    }
}

impl<'a, D, C, MS, MP> Fields for FreqProxFields<'a, D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    type Terms = FreqProxTerms<D, C, MS, MP>;
    fn fields(&self) -> Vec<String> {
        self.fields.keys().cloned().collect()
    }

    fn terms(&self, field: &str) -> Result<Option<Self::Terms>> {
        if let Some(perfield) = self.fields.get(field) {
            Ok(Some(FreqProxTerms::new(*perfield)))
        } else {
            Ok(None)
        }
    }

    fn size(&self) -> usize {
        panic!("unsupported operation")
    }
}

struct FreqProxTerms<
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
> {
    // TODO use raw pointer because we need to return box
    terms_writer: *const FreqProxTermsWriterPerField<D, C, MS, MP>,
}

impl<D, C, MS, MP> FreqProxTerms<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    fn new(terms_writer: &FreqProxTermsWriterPerField<D, C, MS, MP>) -> Self {
        FreqProxTerms { terms_writer }
    }

    fn terms(&self) -> &FreqProxTermsWriterPerField<D, C, MS, MP> {
        unsafe { &*self.terms_writer }
    }
}

impl<D, C, MS, MP> Terms for FreqProxTerms<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    type Iterator = FreqProxTermsIterator<D, C, MS, MP>;
    fn iterator(&self) -> Result<Self::Iterator> {
        let mut terms_iter = FreqProxTermsIterator::new(self.terms());
        terms_iter.reset();
        Ok(terms_iter)
    }

    fn size(&self) -> Result<i64> {
        bail!(ErrorKind::UnsupportedOperation(Cow::Borrowed("")))
    }

    fn sum_total_term_freq(&self) -> Result<i64> {
        bail!(ErrorKind::UnsupportedOperation(Cow::Borrowed("")))
    }

    fn sum_doc_freq(&self) -> Result<i64> {
        bail!(ErrorKind::UnsupportedOperation(Cow::Borrowed("")))
    }

    fn doc_count(&self) -> Result<i32> {
        bail!(ErrorKind::UnsupportedOperation(Cow::Borrowed("")))
    }

    fn has_freqs(&self) -> Result<bool> {
        Ok(self.terms().base.field_info.index_options >= IndexOptions::DocsAndFreqs)
    }

    fn has_offsets(&self) -> Result<bool> {
        Ok(self.terms().base.field_info.index_options
            >= IndexOptions::DocsAndFreqsAndPositionsAndOffsets)
    }

    fn has_positions(&self) -> Result<bool> {
        Ok(self.terms().base.field_info.index_options >= IndexOptions::DocsAndFreqsAndPositions)
    }

    fn has_payloads(&self) -> Result<bool> {
        Ok(self.terms().saw_payloads)
    }
}

struct FreqProxTermsIterator<
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
> {
    // TODO use raw pointer because we need to return box
    terms_writer: *const FreqProxTermsWriterPerField<D, C, MS, MP>,
    num_terms: usize,
    ord: isize,
    scratch: BytesRef,
}

impl<D, C, MS, MP> FreqProxTermsIterator<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    fn new(terms_writer: &FreqProxTermsWriterPerField<D, C, MS, MP>) -> Self {
        FreqProxTermsIterator {
            terms_writer,
            num_terms: unsafe { terms_writer.base.bytes_hash.assume_init_ref().len() },
            ord: -1,
            scratch: BytesRef::default(),
        }
    }

    fn terms(&self) -> &FreqProxTermsWriterPerField<D, C, MS, MP> {
        unsafe { &(*self.terms_writer) }
    }

    fn reset(&mut self) {
        self.ord = -1;
    }

    fn set_bytes(&mut self, term_id: usize) {
        let idx = unsafe { self.terms().base.bytes_hash.assume_init_ref().ids[term_id] as usize };
        let text_start = self.terms().base.postings_array.base.text_starts[idx];
        self.scratch = self
            .terms()
            .base
            .byte_pool()
            .set_bytes_ref(text_start as usize);
    }
}

impl<D, C, MS, MP> TermIterator for FreqProxTermsIterator<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    type Postings = FreqProxPostingIterEnum<D, C, MS, MP>;
    type TermState = ();

    fn next(&mut self) -> Result<Option<Vec<u8>>> {
        self.ord += 1;
        if self.ord >= self.num_terms as isize {
            Ok(None)
        } else {
            let ord = self.ord as usize;
            self.set_bytes(ord);
            Ok(Some(self.scratch.bytes().to_vec()))
        }
    }

    fn seek_ceil(&mut self, text: &[u8]) -> Result<SeekStatus> {
        // TODO: we could instead keep the BytesRefHash
        // intact so this is a hash lookup

        // binary search:
        let mut lo = 0;
        let mut hi = self.num_terms as i32 - 1;
        while hi >= lo {
            let mid = (lo + hi) >> 1;
            self.set_bytes(mid as usize);
            match self.scratch.bytes().cmp(text) {
                Ordering::Less => lo = mid + 1,
                Ordering::Greater => hi = mid - 1,
                Ordering::Equal => {
                    // Found:
                    self.ord = mid as isize;
                    return Ok(SeekStatus::Found);
                }
            }
        }

        // not found:
        self.ord = lo as isize;
        if self.ord >= self.num_terms as isize {
            Ok(SeekStatus::End)
        } else {
            let ord = self.ord as usize;
            self.set_bytes(ord);
            debug_assert!(self.scratch.bytes() > text);
            Ok(SeekStatus::NotFound)
        }
    }

    fn seek_exact_ord(&mut self, ord: i64) -> Result<()> {
        self.ord = ord as isize;
        self.set_bytes(ord as usize);
        Ok(())
    }

    fn term(&self) -> Result<&[u8]> {
        Ok(self.scratch.bytes())
    }

    fn ord(&self) -> Result<i64> {
        Ok(self.ord as i64)
    }

    fn doc_freq(&mut self) -> Result<i32> {
        unimplemented!()
    }

    fn total_term_freq(&mut self) -> Result<i64> {
        unimplemented!()
    }

    fn postings_with_flags(&mut self, flags: u16) -> Result<Self::Postings> {
        if PostingIteratorFlags::feature_requested(flags, PostingIteratorFlags::POSITIONS) {
            if !self.terms().has_prox {
                // Caller wants positions but we didn't index them;
                // don't lie:
                bail!(ErrorKind::IllegalArgument("did not index positions".into()));
            }
            if !self.terms().has_offsets
                && PostingIteratorFlags::feature_requested(flags, PostingIteratorFlags::OFFSETS)
            {
                bail!(ErrorKind::IllegalArgument("did not index offsets".into()));
            }

            let mut pos_iter = FreqProxPostingsIterator::new(self.terms());
            unsafe {
                pos_iter
                    .reset(self.terms().base.bytes_hash.assume_init_ref().ids[self.ord as usize] as usize);
            }
            Ok(FreqProxPostingIterEnum::Postings(pos_iter))
        } else {
            if !self.terms().has_freq
                && PostingIteratorFlags::feature_requested(flags, PostingIteratorFlags::FREQS)
            {
                // Caller wants freqs but we didn't index them;
                // don't lie:
                bail!(ErrorKind::IllegalArgument("did not index freq".into()));
            }

            let mut pos_iter = FreqProxDocsIterator::new(self.terms());
            unsafe {
                pos_iter
                    .reset(self.terms().base.bytes_hash.assume_init_ref().ids[self.ord as usize] as usize);
            }
            Ok(FreqProxPostingIterEnum::Docs(pos_iter))
        }
    }
}

struct FreqProxDocsIterator<
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
> {
    // TODO use raw pointer because we need to return box
    terms_writer: *const FreqProxTermsWriterPerField<D, C, MS, MP>,
    reader: ByteSliceReader,
    read_term_freq: bool,
    doc_id: DocId,
    freq: u32,
    ended: bool,
    term_id: usize,
}

impl<D, C, MS, MP> Clone for FreqProxDocsIterator<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    fn clone(&self) -> Self {
        FreqProxDocsIterator {
            terms_writer: self.terms_writer,
            reader: self.reader.clone(),
            read_term_freq: self.read_term_freq,
            doc_id: self.doc_id,
            freq: self.freq,
            ended: self.ended,
            term_id: self.term_id,
        }
    }
}

impl<D, C, MS, MP> FreqProxDocsIterator<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    fn new(terms_writer: &FreqProxTermsWriterPerField<D, C, MS, MP>) -> Self {
        FreqProxDocsIterator {
            terms_writer,
            reader: ByteSliceReader::default(),
            read_term_freq: terms_writer.has_freq,
            doc_id: -1,
            freq: 0,
            ended: false,
            term_id: 0,
        }
    }

    fn terms(&self) -> &FreqProxTermsWriterPerField<D, C, MS, MP> {
        unsafe { &*self.terms_writer }
    }

    fn reset(&mut self, term_id: usize) {
        self.term_id = term_id;
        unsafe {
            (&*self.terms_writer).init_reader(&mut self.reader, term_id, 0);
        }
        self.ended = false;
        self.doc_id = -1;
    }
}

unsafe impl<D, C, MS, MP> Send for FreqProxDocsIterator<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
}

impl<D, C, MS, MP> PostingIterator for FreqProxDocsIterator<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    fn freq(&self) -> Result<i32> {
        // Don't lie here ... don't want codecs writings lots
        // of wasted 1s into the index:
        if !self.read_term_freq {
            bail!(ErrorKind::IllegalState("freq was not indexed".into()))
        } else {
            Ok(self.freq as i32)
        }
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
        Ok(Vec::with_capacity(0))
    }
}

impl<D, C, MS, MP> DocIterator for FreqProxDocsIterator<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    fn doc_id(&self) -> i32 {
        self.doc_id
    }

    fn next(&mut self) -> Result<i32> {
        if self.doc_id == -1 {
            self.doc_id = 0;
        }
        if self.reader.eof() {
            if self.ended {
                return Ok(NO_MORE_DOCS);
            } else {
                self.ended = true;
                self.doc_id = self.terms().base.postings_array.last_doc_ids[self.term_id];
                if self.read_term_freq {
                    self.freq = self.terms().base.postings_array.term_freqs[self.term_id];
                }
            }
        } else {
            let code = self.reader.read_vint()?;
            if !self.read_term_freq {
                self.doc_id += code;
            } else {
                self.doc_id += code >> 1;
                self.freq = if code & 1 != 0 {
                    1
                } else {
                    self.reader.read_vint()? as u32
                };
            }
            debug_assert_ne!(
                self.doc_id,
                self.terms().base.postings_array.last_doc_ids[self.term_id]
            );
        }
        Ok(self.doc_id)
    }

    fn advance(&mut self, _target: i32) -> Result<i32> {
        unreachable!()
    }

    fn cost(&self) -> usize {
        unreachable!()
    }
}

struct FreqProxPostingsIterator<
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
> {
    // TODO use raw pointer because we need to return box
    terms_writer: *const FreqProxTermsWriterPerField<D, C, MS, MP>,
    reader: ByteSliceReader,
    pos_reader: ByteSliceReader,
    read_offsets: bool,
    doc_id: DocId,
    freq: u32,
    pos: i32,
    start_offset: i32,
    end_offset: i32,
    pos_left: i32,
    term_id: usize,
    ended: bool,
    has_payload: bool,
    payload: Vec<u8>,
}

impl<D, C, MS, MP> FreqProxPostingsIterator<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    fn new(terms_writer: &FreqProxTermsWriterPerField<D, C, MS, MP>) -> Self {
        FreqProxPostingsIterator {
            terms_writer,
            reader: ByteSliceReader::default(),
            pos_reader: ByteSliceReader::default(),
            read_offsets: terms_writer.has_offsets,
            doc_id: -1,
            freq: 0,
            pos: 0,
            start_offset: 0,
            end_offset: 0,
            pos_left: 0,
            term_id: 0,
            ended: false,
            has_payload: false,
            payload: vec![],
        }
    }

    fn terms(&self) -> &FreqProxTermsWriterPerField<D, C, MS, MP> {
        unsafe { &*self.terms_writer }
    }

    fn reset(&mut self, term_id: usize) {
        self.term_id = term_id;
        unsafe {
            (&*self.terms_writer).init_reader(&mut self.reader, term_id, 0);
            (&*self.terms_writer).init_reader(&mut self.pos_reader, term_id, 1);
        }
        self.ended = false;
        self.doc_id = -1;
        self.pos_left = 0;
    }
}

impl<D, C, MS, MP> Clone for FreqProxPostingsIterator<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    fn clone(&self) -> Self {
        FreqProxPostingsIterator {
            terms_writer: self.terms_writer,
            reader: self.reader.clone(),
            pos_reader: self.pos_reader.clone(),
            read_offsets: self.read_offsets,
            doc_id: self.doc_id,
            freq: self.freq,
            pos: self.pos,
            start_offset: self.start_offset,
            end_offset: self.end_offset,
            pos_left: self.pos_left,
            term_id: self.term_id,
            ended: self.ended,
            has_payload: self.has_payload,
            payload: self.payload.clone(),
        }
    }
}

unsafe impl<D, C, MS, MP> Send for FreqProxPostingsIterator<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
}

impl<D, C, MS, MP> PostingIterator for FreqProxPostingsIterator<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    fn freq(&self) -> Result<i32> {
        Ok(self.freq as i32)
    }

    fn next_position(&mut self) -> Result<i32> {
        debug_assert!(self.pos_left > 0);
        self.pos_left -= 1;
        let code = self.pos_reader.read_vint()?;
        self.pos += code >> 1;
        if code & 1 != 0 {
            self.has_payload = true;
            // has a payload
            let payload_len = self.pos_reader.read_vint()? as usize;
            self.payload.resize(payload_len, 0u8);
            self.pos_reader.read_exact(&mut self.payload)?;
        } else {
            self.has_payload = false;
        }

        if self.read_offsets {
            self.start_offset += self.pos_reader.read_vint()?;
            self.end_offset = self.start_offset + self.pos_reader.read_vint()?;
        }
        Ok(self.pos)
    }

    fn start_offset(&self) -> Result<i32> {
        if !self.read_offsets {
            bail!(ErrorKind::IllegalState("offsets were not indexed".into()));
        }
        Ok(self.start_offset)
    }

    fn end_offset(&self) -> Result<i32> {
        if !self.read_offsets {
            bail!(ErrorKind::IllegalState("offsets were not indexed".into()));
        }
        Ok(self.end_offset)
    }

    fn payload(&self) -> Result<Payload> {
        // if self.has_payload is false, just return empty Vec<u8>
        Ok(self.payload.clone())
    }
}

impl<D, C, MS, MP> DocIterator for FreqProxPostingsIterator<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    fn doc_id(&self) -> i32 {
        self.doc_id
    }

    fn next(&mut self) -> Result<i32> {
        if self.doc_id == -1 {
            self.doc_id = 0;
        }

        while self.pos_left != 0 {
            self.next_position()?;
        }

        if self.reader.eof() {
            if self.ended {
                return Ok(NO_MORE_DOCS);
            } else {
                self.ended = true;
                self.doc_id = self.terms().base.postings_array.last_doc_ids[self.term_id];
                self.freq = self.terms().base.postings_array.term_freqs[self.term_id];
            }
        } else {
            let code = self.reader.read_vint()?;
            self.doc_id += code >> 1;
            self.freq = if code & 1 != 0 {
                1
            } else {
                self.reader.read_vint()? as u32
            };
            debug_assert_ne!(
                self.doc_id,
                self.terms().base.postings_array.last_doc_ids[self.term_id]
            );
        }

        self.pos_left = self.freq as i32;
        self.pos = 0;
        self.start_offset = 0;

        Ok(self.doc_id)
    }

    fn advance(&mut self, _target: i32) -> Result<i32> {
        unreachable!()
    }

    fn cost(&self) -> usize {
        unreachable!()
    }
}

enum FreqProxPostingIterEnum<
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
> {
    Postings(FreqProxPostingsIterator<D, C, MS, MP>),
    Docs(FreqProxDocsIterator<D, C, MS, MP>),
}

impl<D, C, MS, MP> PostingIterator for FreqProxPostingIterEnum<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    fn freq(&self) -> Result<i32> {
        match self {
            FreqProxPostingIterEnum::Postings(i) => i.freq(),
            FreqProxPostingIterEnum::Docs(i) => i.freq(),
        }
    }

    fn next_position(&mut self) -> Result<i32> {
        match self {
            FreqProxPostingIterEnum::Postings(i) => i.next_position(),
            FreqProxPostingIterEnum::Docs(i) => i.next_position(),
        }
    }

    fn start_offset(&self) -> Result<i32> {
        match self {
            FreqProxPostingIterEnum::Postings(i) => i.start_offset(),
            FreqProxPostingIterEnum::Docs(i) => i.start_offset(),
        }
    }

    fn end_offset(&self) -> Result<i32> {
        match self {
            FreqProxPostingIterEnum::Postings(i) => i.end_offset(),
            FreqProxPostingIterEnum::Docs(i) => i.end_offset(),
        }
    }

    fn payload(&self) -> Result<Payload> {
        match self {
            FreqProxPostingIterEnum::Postings(i) => i.payload(),
            FreqProxPostingIterEnum::Docs(i) => i.payload(),
        }
    }
}

impl<D, C, MS, MP> DocIterator for FreqProxPostingIterEnum<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    fn doc_id(&self) -> DocId {
        match self {
            FreqProxPostingIterEnum::Postings(i) => i.doc_id(),
            FreqProxPostingIterEnum::Docs(i) => i.doc_id(),
        }
    }

    fn next(&mut self) -> Result<DocId> {
        match self {
            FreqProxPostingIterEnum::Postings(i) => i.next(),
            FreqProxPostingIterEnum::Docs(i) => i.next(),
        }
    }

    fn advance(&mut self, target: i32) -> Result<DocId> {
        match self {
            FreqProxPostingIterEnum::Postings(i) => i.advance(target),
            FreqProxPostingIterEnum::Docs(i) => i.advance(target),
        }
    }

    fn slow_advance(&mut self, target: i32) -> Result<DocId> {
        match self {
            FreqProxPostingIterEnum::Postings(i) => i.slow_advance(target),
            FreqProxPostingIterEnum::Docs(i) => i.slow_advance(target),
        }
    }

    fn cost(&self) -> usize {
        match self {
            FreqProxPostingIterEnum::Postings(i) => i.cost(),
            FreqProxPostingIterEnum::Docs(i) => i.cost(),
        }
    }

    fn matches(&mut self) -> Result<bool> {
        match self {
            FreqProxPostingIterEnum::Postings(i) => i.matches(),
            FreqProxPostingIterEnum::Docs(i) => i.matches(),
        }
    }

    fn match_cost(&self) -> f32 {
        match self {
            FreqProxPostingIterEnum::Postings(i) => i.match_cost(),
            FreqProxPostingIterEnum::Docs(i) => i.match_cost(),
        }
    }

    fn approximate_next(&mut self) -> Result<DocId> {
        match self {
            FreqProxPostingIterEnum::Postings(i) => i.approximate_next(),
            FreqProxPostingIterEnum::Docs(i) => i.approximate_next(),
        }
    }

    fn approximate_advance(&mut self, target: i32) -> Result<DocId> {
        match self {
            FreqProxPostingIterEnum::Postings(i) => i.approximate_advance(target),
            FreqProxPostingIterEnum::Docs(i) => i.approximate_advance(target),
        }
    }
}
