use core::index::byte_slice_reader::ByteSliceReader;
use core::index::term::{SeekStatus, TermIterator, Terms, TermsRef};
use core::index::term_vector::TermVectorsConsumer;
use core::index::terms_hash_per_field::{FreqProxTermsWriterPerField, TermsHashPerField};
use core::index::thread_doc_writer::DocumentsWriterPerThread;
use core::index::SegmentWriteState;
use core::index::{FieldInfo, FieldInfosBuilder, FieldInvertState, FieldNumbersRef, Fields,
                  IndexOptions};
use core::search::posting_iterator::POSTING_ITERATOR_FLAG_FREQS;
use core::search::posting_iterator::POSTING_ITERATOR_FLAG_OFFSETS;
use core::search::posting_iterator::POSTING_ITERATOR_FLAG_POSITIONS;
use core::search::posting_iterator::{posting_feature_requested, PostingIterator};
use core::search::{DocIterator, Payload, NO_MORE_DOCS};
use core::store::DataInput;
use core::util::bit_set::FixedBitSet;
use core::util::byte_block_pool::{ByteBlockAllocator, ByteBlockPool};
use core::util::byte_ref::BytesRef;
use core::util::int_block_pool::IntBlockPool;
use core::util::{Counter, DocId};

use std::any::Any;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::ptr;
use std::sync::Arc;

use error::{ErrorKind, Result};

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
    pub bytes_used: Counter,
    track_allocations: bool,
}

impl Default for TermsHashBase {
    fn default() -> Self {
        TermsHashBase {
            int_pool: IntBlockPool::default(),
            byte_pool: ByteBlockPool::default(),
            term_byte_pool: ptr::null_mut(),
            bytes_used: Counter::default(),
            track_allocations: false,
        }
    }
}

impl TermsHashBase {
    pub fn new(doc_writer: &mut DocumentsWriterPerThread, track_allocations: bool) -> Self {
        let bytes_used = if track_allocations {
            Counter::borrow(&mut doc_writer.bytes_used)
        } else {
            Counter::new(false)
        };
        let byte_pool =
            unsafe { ByteBlockPool::new(doc_writer.byte_block_allocator.copy_unsafe()) };

        TermsHashBase {
            int_pool: IntBlockPool::new(doc_writer.int_block_allocator.shallow_copy()),
            byte_pool,
            term_byte_pool: ptr::null_mut(),
            bytes_used,
            track_allocations,
        }
    }

    // TODO: this must be call anytime after self's address won't move anymore
    // other the term_byte_pool pointer is like point to an invalid address
    pub fn init(&mut self) {
        self.term_byte_pool = &mut self.byte_pool;
    }

    pub fn abort(&mut self) {
        self.reset();
    }

    // clear all state
    pub fn reset(&mut self) {
        // we don't reuse so we drop everything and don't fill with 0
        self.int_pool.reset(false, false);
        self.byte_pool.reset(false, false);
    }
}

pub trait TermsHash {
    type PerField: TermsHashPerField;
    fn base(&self) -> &TermsHashBase;
    fn base_mut(&mut self) -> &mut TermsHashBase;

    fn add_field(
        &mut self,
        field_invert_state: &FieldInvertState,
        field_info: &FieldInfo,
    ) -> Self::PerField;

    fn flush(
        &mut self,
        field_to_flush: BTreeMap<&str, &Self::PerField>,
        state: &mut SegmentWriteState,
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

pub struct FreqProxTermsWriter {
    pub base: TermsHashBase,
    pub next_terms_hash: TermVectorsConsumer,
}

impl Default for FreqProxTermsWriter {
    fn default() -> Self {
        FreqProxTermsWriter {
            base: TermsHashBase::default(),
            next_terms_hash: TermVectorsConsumer::default(),
        }
    }
}

impl FreqProxTermsWriter {
    pub fn new(
        doc_writer: &mut DocumentsWriterPerThread,
        term_vectors: TermVectorsConsumer,
    ) -> Self {
        let base = TermsHashBase::new(doc_writer, true);
        FreqProxTermsWriter {
            base,
            next_terms_hash: term_vectors,
        }
    }

    pub fn init(&mut self) {
        self.base.term_byte_pool = &mut self.base.byte_pool;
        self.next_terms_hash.base.term_byte_pool = &mut self.base.byte_pool;
        self.next_terms_hash.inited = true;
        // self.base.init();
        // self.next_terms_hash.init();
    }

    fn apply_deletes(state: &mut SegmentWriteState, fields: &Fields) -> Result<()> {
        // process any pending Term deletes for this newly flushed segment:
        if state.seg_updates.is_some() {
            if !state.seg_updates().deleted_terms.is_empty() {
                let mut deleted_terms = Vec::with_capacity(state.seg_updates().deleted_terms.len());
                for (k, _) in state.seg_updates().deleted_terms.as_ref() {
                    deleted_terms.push(k.clone());
                }
                deleted_terms.sort();

                let mut last_field = "";
                let mut terms_iter: Option<Box<TermIterator>> = None;
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
                                let mut doc = postings_iter.next()?;
                                if doc < del_doc_limit {
                                    let size = state.segment_info.max_doc as usize;
                                    if state.live_docs.len() == 0 {
                                        state.live_docs = Box::new(FixedBitSet::new(size));
                                        state.live_docs.as_bit_set_mut().batch_set(0, size);
                                    }
                                    debug_assert!(state.live_docs.len() >= size);
                                    if state.live_docs.get(doc as usize)? {
                                        state.del_count_on_flush += 1;
                                        state.live_docs.as_bit_set_mut().clear(doc as usize);
                                    }
                                } else {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

impl TermsHash for FreqProxTermsWriter {
    type PerField = FreqProxTermsWriterPerField;
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
    ) -> FreqProxTermsWriterPerField {
        let next_field = self
            .next_terms_hash
            .add_field(field_invert_state, field_info);
        FreqProxTermsWriterPerField::new(&mut self.base, field_info.clone(), next_field)
    }

    fn flush(
        &mut self,
        field_to_flush: BTreeMap<&str, &FreqProxTermsWriterPerField>,
        state: &mut SegmentWriteState,
    ) -> Result<()> {
        let mut next_child_fields = BTreeMap::new();
        for (k, v) in &field_to_flush {
            next_child_fields.insert(*k, &(*v).next_per_field);
        }
        self.next_terms_hash.flush(next_child_fields, state)?;

        // Gather all fields that saw any positions:
        let mut all_fields = Vec::with_capacity(field_to_flush.len());
        for (_, f) in field_to_flush {
            if f.base().bytes_hash.len() > 0 {
                // TODO: Hack logic, it's because it's hard to gain param `field_to_flush` as
                // `HashMap<&str, &mut FreqProxTermsWriterPerField>`
                // this should be fixed later
                unsafe {
                    let pf_ptr =
                        f as *const FreqProxTermsWriterPerField as *mut FreqProxTermsWriterPerField;
                    (*pf_ptr).base_mut().sort_postings();
                }

                debug_assert_ne!(f.base().field_info.index_options, IndexOptions::Null);
                all_fields.push(f);
            }
        }

        if all_fields.len() > 0 {
            // sort by field name
            all_fields.sort();
            let fields = FreqProxFields::new(all_fields);

            FreqProxTermsWriter::apply_deletes(state, &fields)?;

            let mut consumer = state
                .segment_info
                .codec()
                .postings_format()
                .fields_consumer(state)?;

            consumer.write(&fields)
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
struct FreqProxFields<'a> {
    fields: BTreeMap<String, &'a FreqProxTermsWriterPerField>,
}

impl<'a> FreqProxFields<'a> {
    fn new(field_list: Vec<&'a FreqProxTermsWriterPerField>) -> Self {
        let mut fields = BTreeMap::new();
        for f in field_list {
            fields.insert(f.base().field_info.name.clone(), f);
        }
        FreqProxFields { fields }
    }
}

unsafe impl<'a> Send for FreqProxFields<'a> {}

unsafe impl<'a> Sync for FreqProxFields<'a> {}

impl<'a> Fields for FreqProxFields<'a> {
    fn fields(&self) -> Vec<String> {
        self.fields.keys().cloned().collect()
    }

    fn terms(&self, field: &str) -> Result<Option<TermsRef>> {
        if let Some(perfield) = self.fields.get(field) {
            Ok(Some(Arc::new(FreqProxTerms::new(*perfield))))
        } else {
            Ok(None)
        }
    }

    fn size(&self) -> usize {
        panic!("unsupported operation")
    }
}

struct FreqProxTerms {
    // TODO use raw pointer because we need to return box
    terms_writer: *const FreqProxTermsWriterPerField,
}

impl FreqProxTerms {
    fn new(terms_writer: &FreqProxTermsWriterPerField) -> Self {
        FreqProxTerms { terms_writer }
    }

    fn terms(&self) -> &FreqProxTermsWriterPerField {
        unsafe { &*self.terms_writer }
    }
}

unsafe impl Send for FreqProxTerms {}

unsafe impl Sync for FreqProxTerms {}

impl Terms for FreqProxTerms {
    fn iterator(&self) -> Result<Box<TermIterator>> {
        let mut terms_iter = Box::new(FreqProxTermsIterator::new(self.terms()));
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

struct FreqProxTermsIterator {
    // TODO use raw pointer because we need to return box
    terms_writer: *const FreqProxTermsWriterPerField,
    num_terms: usize,
    ord: isize,
    scratch: BytesRef,
}

impl FreqProxTermsIterator {
    fn new(terms_writer: &FreqProxTermsWriterPerField) -> Self {
        FreqProxTermsIterator {
            terms_writer,
            num_terms: terms_writer.base.bytes_hash.len(),
            ord: -1,
            scratch: BytesRef::default(),
        }
    }

    fn terms(&self) -> &FreqProxTermsWriterPerField {
        unsafe { &(*self.terms_writer) }
    }

    fn reset(&mut self) {
        self.ord = -1;
    }

    fn set_bytes(&mut self, term_id: usize) {
        let idx = self.terms().base.bytes_hash.ids[term_id] as usize;
        let text_start = self.terms().base.postings_array.base.text_starts[idx];
        self.scratch = self
            .terms()
            .base
            .byte_block_pool()
            .set_bytes_ref(text_start as usize);
    }
}

impl TermIterator for FreqProxTermsIterator {
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

    fn postings_with_flags(&mut self, flags: i16) -> Result<Box<PostingIterator>> {
        if posting_feature_requested(flags, POSTING_ITERATOR_FLAG_POSITIONS) {
            if !self.terms().has_prox {
                // Caller wants positions but we didn't index them;
                // don't lie:
                bail!(ErrorKind::IllegalArgument("did not index positions".into()));
            }
            if !self.terms().has_offsets
                && posting_feature_requested(flags, POSTING_ITERATOR_FLAG_OFFSETS)
            {
                bail!(ErrorKind::IllegalArgument("did not index offsets".into()));
            }

            let mut pos_iter = Box::new(FreqProxPostingsIterator::new(self.terms()));
            pos_iter.reset(self.terms().base.bytes_hash.ids[self.ord as usize] as usize);
            Ok(pos_iter)
        } else {
            if !self.terms().has_freq
                && posting_feature_requested(flags, POSTING_ITERATOR_FLAG_FREQS)
            {
                // Caller wants freqs but we didn't index them;
                // don't lie:
                bail!(ErrorKind::IllegalArgument("did not index freq".into()));
            }

            let mut pos_iter = Box::new(FreqProxDocsIterator::new(self.terms()));
            pos_iter.reset(self.terms().base.bytes_hash.ids[self.ord as usize] as usize);
            Ok(pos_iter)
        }
    }

    fn as_any(&self) -> &Any {
        self
    }
}

struct FreqProxDocsIterator {
    // TODO use raw pointer because we need to return box
    terms_writer: *const FreqProxTermsWriterPerField,
    reader: ByteSliceReader,
    read_term_freq: bool,
    doc_id: DocId,
    freq: u32,
    ended: bool,
    term_id: usize,
}

impl Clone for FreqProxDocsIterator {
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

impl FreqProxDocsIterator {
    fn new(terms_writer: &FreqProxTermsWriterPerField) -> Self {
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

    fn terms(&self) -> &FreqProxTermsWriterPerField {
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

unsafe impl Send for FreqProxDocsIterator {}

unsafe impl Sync for FreqProxDocsIterator {}

impl PostingIterator for FreqProxDocsIterator {
    fn clone_as_doc_iterator(&self) -> Result<Box<DocIterator>> {
        Ok(Box::new(self.clone()))
    }

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

    fn as_any_mut(&mut self) -> &mut Any {
        self
    }
}

impl DocIterator for FreqProxDocsIterator {
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

struct FreqProxPostingsIterator {
    // TODO use raw pointer because we need to return box
    terms_writer: *const FreqProxTermsWriterPerField,
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

impl FreqProxPostingsIterator {
    fn new(terms_writer: &FreqProxTermsWriterPerField) -> Self {
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

    fn terms(&self) -> &FreqProxTermsWriterPerField {
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

impl Clone for FreqProxPostingsIterator {
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

unsafe impl Send for FreqProxPostingsIterator {}

unsafe impl Sync for FreqProxPostingsIterator {}

impl PostingIterator for FreqProxPostingsIterator {
    fn clone_as_doc_iterator(&self) -> Result<Box<DocIterator>> {
        Ok(Box::new(self.clone()))
    }

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
            self.pos_reader
                .read_bytes(&mut self.payload, 0, payload_len)?;
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

    fn as_any_mut(&mut self) -> &mut Any {
        self
    }
}

impl DocIterator for FreqProxPostingsIterator {
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
