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

use core::analysis::TokenStream;
use core::codec::{Codec, TermVectorsFormat, TermVectorsWriter, TermVectorsWriterEnum};
use core::index::byte_slice_reader::ByteSliceReader;
use core::index::merge_policy::MergePolicy;
use core::index::merge_scheduler::MergeScheduler;
use core::index::postings_array::{ParallelPostingsArray, PostingsArray};
use core::index::terms_hash::{TermsHash, TermsHashBase};
use core::index::terms_hash_per_field::{TermsHashPerField, TermsHashPerFieldBase};
use core::index::thread_doc_writer::DocumentsWriterPerThread;
use core::index::{
    FieldInfo, FieldInfosBuilder, FieldInvertState, FieldNumbersRef, Fieldable, IndexOptions,
    SegmentWriteState,
};
use core::store::{Directory, FlushInfo, IOContext};
use core::util::DocId;

use error::{ErrorKind, Result};

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::ptr;

pub struct TermVectorsConsumer<
    D: Directory + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
> {
    pub base: TermsHashBase,
    // format: Box<TermVectorsFormat>,
    // directory: DirectoryRc,
    // segment_info: SegmentInfo,
    writer: Option<TermVectorsWriterEnum<D::IndexOutput>>,
    vector_slice_reader_pos: ByteSliceReader,
    vector_slice_reader_off: ByteSliceReader,
    has_vectors: bool,
    pub num_vector_fields: u32,
    last_doc_id: DocId,
    doc_writer: *const DocumentsWriterPerThread<D, C, MS, MP>,
    pub per_fields: Vec<*mut TermVectorsConsumerPerField<D, C, MS, MP>>,
    pub inited: bool,
}

impl<D, C, MS, MP> TermVectorsConsumer<D, C, MS, MP>
where
    D: Directory + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    // only used for init
    pub fn new_default() -> Self {
        let base = TermsHashBase::default();
        TermVectorsConsumer {
            base,
            writer: None,
            vector_slice_reader_off: ByteSliceReader::default(),
            vector_slice_reader_pos: ByteSliceReader::default(),
            has_vectors: false,
            num_vector_fields: 0,
            last_doc_id: 0,
            doc_writer: ptr::null(),
            per_fields: vec![],
            inited: false,
        }
    }
    pub fn new(doc_writer: &mut DocumentsWriterPerThread<D, C, MS, MP>) -> Self {
        let base = TermsHashBase::new(doc_writer, false);
        TermVectorsConsumer {
            base,
            writer: None,
            vector_slice_reader_off: ByteSliceReader::default(),
            vector_slice_reader_pos: ByteSliceReader::default(),
            has_vectors: false,
            num_vector_fields: 0,
            last_doc_id: 0,
            doc_writer,
            per_fields: vec![],
            inited: false,
        }
    }

    pub fn init(&mut self) {
        // self.base.init();
        self.inited = true;
    }

    pub fn terms_writer(&mut self) -> &mut TermVectorsWriterEnum<D::IndexOutput> {
        debug_assert!(self.writer.is_some());
        self.writer.as_mut().unwrap()
    }

    fn init_term_vectors_writer(&mut self) -> Result<()> {
        debug_assert!(self.inited);
        if self.writer.is_none() {
            let doc_writer = unsafe { &*self.doc_writer };
            let context = IOContext::Flush(FlushInfo::new(
                doc_writer.num_docs_in_ram,
                doc_writer.bytes_used() as u64,
            ));
            self.writer = Some(doc_writer.codec().term_vectors_format().tv_writer(
                &*doc_writer.directory,
                &doc_writer.segment_info,
                &context,
            )?);
            self.last_doc_id = 0;
        }
        Ok(())
    }

    /// Fills in no-term-vectors for all docs we haven't seen
    /// since the last doc that had term vectors.
    fn fill(&mut self, doc_id: DocId) -> Result<()> {
        loop {
            if self.last_doc_id >= doc_id {
                break;
            }
            if let Some(ref mut writer) = self.writer {
                writer.start_document(0)?;
                writer.finish_document()?;
            }
            self.last_doc_id += 1;
        }
        Ok(())
    }

    fn do_flush<DW: Directory>(
        &mut self,
        _field_to_flush: BTreeMap<&str, &TermVectorsConsumerPerField<D, C, MS, MP>>,
        state: &mut SegmentWriteState<D, DW, C>,
    ) -> Result<()> {
        if self.writer.is_some() {
            let num_docs = state.segment_info.max_doc;
            debug_assert!(num_docs > 0);
            // At least one doc in this run had term vectors enabled
            self.fill(num_docs)?;
            self.writer
                .as_mut()
                .unwrap()
                .finish(&state.field_infos, num_docs as usize)?;
        }
        Ok(())
    }

    fn reset_field(&mut self) {
        self.per_fields.truncate(0); // don't hang onto stuff from previous doc
        self.num_vector_fields = 0;
    }

    fn add_field_to_flush(&mut self, field_to_flush: &TermVectorsConsumerPerField<D, C, MS, MP>) {
        self.per_fields.push(
            field_to_flush as *const TermVectorsConsumerPerField<D, C, MS, MP>
                as *mut TermVectorsConsumerPerField<D, C, MS, MP>,
        );
        self.num_vector_fields += 1;
    }
}

impl<D, C, MS, MP> TermsHash<D, C> for TermVectorsConsumer<D, C, MS, MP>
where
    D: Directory + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    type PerField = TermVectorsConsumerPerField<D, C, MS, MP>;

    fn base(&self) -> &TermsHashBase {
        &self.base
    }

    fn base_mut(&mut self) -> &mut TermsHashBase {
        &mut self.base
    }

    fn add_field(
        &mut self,
        _field_invert_state: &FieldInvertState,
        field_info: &FieldInfo,
    ) -> TermVectorsConsumerPerField<D, C, MS, MP> {
        TermVectorsConsumerPerField::new(self, field_info.clone())
    }

    fn flush<DW: Directory>(
        &mut self,
        field_to_flush: BTreeMap<&str, &TermVectorsConsumerPerField<D, C, MS, MP>>,
        state: &mut SegmentWriteState<D, DW, C>,
    ) -> Result<()> {
        let res = self.do_flush(field_to_flush, state);
        self.writer = None;
        self.last_doc_id = 0;
        self.has_vectors = false;
        res
    }

    fn abort(&mut self) -> Result<()> {
        self.has_vectors = false;
        self.writer = None;
        self.last_doc_id = 0;
        self.base_mut().reset();
        Ok(())
    }

    fn start_document(&mut self) -> Result<()> {
        self.reset_field();
        self.num_vector_fields = 0;
        Ok(())
    }

    fn finish_document(
        &mut self,
        field_infos: &mut FieldInfosBuilder<FieldNumbersRef>,
    ) -> Result<()> {
        if !self.has_vectors {
            return Ok(());
        }

        let mut pf_idxs: BTreeMap<String, usize> = BTreeMap::new();
        for i in 0..self.num_vector_fields as usize {
            unsafe {
                let pf: &TermVectorsConsumerPerField<D, C, MS, MP> = &(*self.per_fields[i]);
                pf_idxs.insert(pf.base().field_info.name.clone(), i);
            }
        }

        self.init_term_vectors_writer()?;

        let doc_id = unsafe { (*self.doc_writer).doc_state.doc_id };

        self.fill(doc_id)?;

        {
            debug_assert!(self.writer.is_some());
            let writer = self.writer.as_mut().unwrap();
            writer.start_document(self.num_vector_fields as usize)?;
            for (_, i) in pf_idxs {
                unsafe {
                    let pf: &mut TermVectorsConsumerPerField<D, C, MS, MP> =
                        &mut (*self.per_fields[i]);
                    pf.finish_document(
                        writer,
                        &mut self.vector_slice_reader_pos,
                        &mut self.vector_slice_reader_off,
                        field_infos,
                    )?;
                }
            }
            writer.finish_document()?;
        }

        debug_assert!(self.last_doc_id == doc_id);
        self.last_doc_id += 1;

        self.base_mut().reset();
        self.reset_field();

        Ok(())
    }
}

pub struct TermVectorsConsumerPerField<
    D: Directory + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
> {
    base: TermsHashPerFieldBase<TermVectorPostingsArray>,
    do_vectors: bool,
    do_vector_positions: bool,
    do_vector_offsets: bool,
    do_vector_payloads: bool,
    has_payloads: bool,
    // if enabled, and we actually saw any for this field
    inited: bool,
    parent: *mut TermVectorsConsumer<D, C, MS, MP>,
}

impl<D: Directory + 'static, C: Codec, MS: MergeScheduler, MP: MergePolicy>
    TermVectorsConsumerPerField<D, C, MS, MP>
{
    pub fn new(
        terms_writer: &mut TermVectorsConsumer<D, C, MS, MP>,
        field_info: FieldInfo,
    ) -> Self {
        let base = TermsHashPerFieldBase::new(
            2,
            &mut terms_writer.base,
            field_info,
            TermVectorPostingsArray::default(),
        );

        TermVectorsConsumerPerField {
            base,
            do_vectors: false,
            do_vector_positions: false,
            do_vector_offsets: false,
            do_vector_payloads: false,
            has_payloads: false,
            inited: false,
            parent: terms_writer,
        }
    }

    fn term_vectors_writer(&self) -> &mut TermVectorsConsumer<D, C, MS, MP> {
        unsafe { &mut *self.parent }
    }

    fn finish_document<T: TermVectorsWriter>(
        &mut self,
        tv: &mut T,
        pos_reader: &mut ByteSliceReader,
        off_reader: &mut ByteSliceReader,
        field_infos: &mut FieldInfosBuilder<FieldNumbersRef>,
    ) -> Result<()> {
        if !self.do_vectors {
            return Ok(());
        }

        self.do_vectors = false;
        let num_postings = self.base.bytes_hash.len();

        // This is called once, after inverting all occurrences
        // of a given field in the doc.  At this point we flush
        // our hash into the DocWriter.
        self.base.bytes_hash.sort();
        self.term_vectors_writer()
            .writer
            .as_mut()
            .unwrap()
            .start_field(
                &self.base.field_info,
                num_postings,
                self.do_vector_positions,
                self.do_vector_offsets,
                self.has_payloads,
            )?;
        for j in 0..num_postings {
            let term_id = self.base.bytes_hash.ids[j] as usize;
            let freq = self.base.postings_array.freqs[term_id];

            // Get BytesPtr
            let flush_term = self
                .base
                .term_pool()
                .set_bytes_ref(self.base.postings_array.base.text_starts[term_id] as usize);
            tv.start_term(&flush_term, freq as i32)?;

            if self.do_vector_positions || self.do_vector_offsets {
                if self.do_vector_positions {
                    self.init_reader(pos_reader, term_id, 0);
                }
                if self.do_vector_offsets {
                    self.init_reader(off_reader, term_id, 1);
                }
                tv.add_prox(freq as usize, Some(pos_reader), Some(off_reader))?;
            }
            tv.finish_term()?;
        }
        tv.finish_field()?;

        self.reset();
        let fi = field_infos.get_or_add(&self.base.field_info.name)?;
        fi.has_store_term_vector = true;

        Ok(())
    }

    fn reset(&mut self) {
        self.base.bytes_hash.clear(false);
    }

    fn write_prox(
        &mut self,
        term_id: usize,
        field_state: &FieldInvertState,
        token_stream: &TokenStream,
    ) {
        if self.do_vector_offsets {
            let start_offset = field_state.offset + token_stream.offset_attribute().start_offset();
            let end_offset = field_state.offset + token_stream.offset_attribute().end_offset();

            let delta = start_offset as i32 - self.base.postings_array.last_offsets[term_id] as i32;
            self.base.write_vint(1, delta);
            self.base.write_vint(1, (end_offset - start_offset) as i32);
            self.base.postings_array.last_offsets[term_id] = end_offset as u32;
        }

        if self.do_vector_positions {
            let mut payload: &[u8] = &[0u8; 0];
            if let Some(attr) = token_stream.payload_attribute() {
                payload = attr.get_payload();
            }
            let pos =
                field_state.position - self.base.postings_array.last_positions[term_id] as i32;
            if payload.is_empty() {
                self.base.write_vint(0, pos << 1);
            } else {
                self.base.write_vint(0, (pos << 1) | 1);
                self.base.write_vint(0, payload.len() as i32);
                self.base.write_bytes(0, payload);
            }
            self.base.postings_array.last_positions[term_id] = field_state.position as u32;
        }
    }
}

impl<D: Directory + 'static, C: Codec, MS: MergeScheduler, MP: MergePolicy> TermsHashPerField
    for TermVectorsConsumerPerField<D, C, MS, MP>
{
    type P = TermVectorPostingsArray;

    fn base(&self) -> &TermsHashPerFieldBase<TermVectorPostingsArray> {
        &self.base
    }

    fn base_mut(&mut self) -> &mut TermsHashPerFieldBase<TermVectorPostingsArray> {
        &mut self.base
    }

    fn init(&mut self) {
        self.base.init();
        self.inited = true;
    }

    fn reset_ptr(&mut self, parent: &mut TermsHashBase) {
        self.base.reset_ptr(parent);
    }

    fn start(
        &mut self,
        _field_state: &FieldInvertState,
        field: &impl Fieldable,
        first: bool,
    ) -> Result<bool> {
        debug_assert!(self.inited);
        debug_assert_ne!(field.field_type().index_options(), IndexOptions::Null);
        if first {
            if self.base.bytes_hash.len() != 0 {
                // Only necessary if previous doc hit a
                // non-aborting exception while writing vectors in
                // this field:
                self.reset();
            }

            self.base.bytes_hash.reinit();
            self.has_payloads = false;
            self.do_vectors = field.field_type().store_term_vectors();
            if self.do_vectors {
                self.term_vectors_writer().has_vectors = true;

                self.do_vector_positions = field.field_type().store_term_vector_positions();
                // Somewhat confusingly, unlike postings, you are
                // allowed to index TV offsets without TV positions:
                self.do_vector_offsets = field.field_type().store_term_vector_offsets();

                if self.do_vector_positions {
                    self.do_vector_payloads = field.field_type().store_term_vector_payloads();
                } else {
                    self.do_vector_payloads = false;
                    if field.field_type().store_term_vector_payloads() {
                        bail!(ErrorKind::IllegalArgument(
                            "cannot index term vector payloads without term vector positions"
                                .into()
                        ));
                    }
                }
            } else {
                if field.field_type().store_term_vector_offsets() {
                    bail!(ErrorKind::IllegalArgument(
                        "cannot index term vector offsets when term vectors are not indexed".into()
                    ));
                }
                if field.field_type().store_term_vector_positions() {
                    bail!(ErrorKind::IllegalArgument(
                        "cannot index term vector positions when term vectors are not indexed"
                            .into()
                    ));
                }
                if field.field_type().store_term_vector_payloads() {
                    bail!(ErrorKind::IllegalArgument(
                        "cannot index term vector payloads when term vectors are not indexed"
                            .into()
                    ));
                }
            }
        } else {
            if self.do_vectors != field.field_type().store_term_vectors() {
                bail!(ErrorKind::IllegalArgument(
                    "all instances of a given field name must have the same term vectors settings \
                     (storeTermVectors changed)"
                        .into()
                ));
            }
            if self.do_vector_positions != field.field_type().store_term_vector_positions() {
                bail!(ErrorKind::IllegalArgument(
                    "all instances of a given field name must have the same term vectors settings \
                     (store_term_vector_positions changed)"
                        .into()
                ));
            }
            if self.do_vector_offsets != field.field_type().store_term_vector_offsets() {
                bail!(ErrorKind::IllegalArgument(
                    "all instances of a given field name must have the same term vectors settings \
                     (store_term_vector_offsets changed)"
                        .into()
                ));
            }
            if self.do_vector_payloads != field.field_type().store_term_vector_payloads() {
                bail!(ErrorKind::IllegalArgument(
                    "all instances of a given field name must have the same term vectors settings \
                     (store_term_vector_payloads changed)"
                        .into()
                ));
            }
        }

        Ok(self.do_vectors)
    }

    /// Called once per field per document if term vectors
    /// are enabled, to write the vectors to
    /// RAMOutputStream, which is then quickly flushed to
    /// the real term vectors files in the Directory.
    fn finish(&mut self, _field_state: &FieldInvertState) -> Result<()> {
        if self.do_vectors && self.base.bytes_hash.len() > 0 {
            self.term_vectors_writer().add_field_to_flush(self);
        }
        Ok(())
    }

    fn new_term(
        &mut self,
        term_id: usize,
        field_state: &mut FieldInvertState,
        token_stream: &TokenStream,
        _doc_id: i32,
    ) -> Result<()> {
        self.base.postings_array.freqs[term_id] = 1;
        self.base.postings_array.last_offsets[term_id] = 0;
        self.base.postings_array.last_positions[term_id] = 0;

        self.write_prox(term_id, field_state, token_stream);
        Ok(())
    }

    fn add_term(
        &mut self,
        term_id: usize,
        field_state: &mut FieldInvertState,
        token_stream: &TokenStream,
        _doc_id: i32,
    ) -> Result<()> {
        self.base.postings_array.freqs[term_id] += 1;
        self.write_prox(term_id, field_state, token_stream);
        Ok(())
    }

    fn create_postings_array(&self, size: usize) -> TermVectorPostingsArray {
        TermVectorPostingsArray::new(size)
    }
}

impl<D: Directory + 'static, C: Codec, MS: MergeScheduler, MP: MergePolicy> Eq
    for TermVectorsConsumerPerField<D, C, MS, MP>
{
}

impl<D: Directory + 'static, C: Codec, MS: MergeScheduler, MP: MergePolicy> PartialEq
    for TermVectorsConsumerPerField<D, C, MS, MP>
{
    fn eq(&self, other: &Self) -> bool {
        self.base.field_info.name.eq(&other.base.field_info.name)
    }
}

impl<D: Directory + 'static, C: Codec, MS: MergeScheduler, MP: MergePolicy> Ord
    for TermVectorsConsumerPerField<D, C, MS, MP>
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.base.field_info.name.cmp(&other.base.field_info.name)
    }
}

impl<D: Directory + 'static, C: Codec, MS: MergeScheduler, MP: MergePolicy> PartialOrd
    for TermVectorsConsumerPerField<D, C, MS, MP>
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub struct TermVectorPostingsArray {
    base: ParallelPostingsArray,
    freqs: Vec<u32>,
    // How many times this term occurred in the current doc
    last_offsets: Vec<u32>,
    // Last offset we saw
    last_positions: Vec<u32>,
    // Last position where this term occurred
}

impl Default for TermVectorPostingsArray {
    fn default() -> Self {
        TermVectorPostingsArray {
            base: ParallelPostingsArray::default(),
            freqs: vec![0u32; 1024],
            last_offsets: vec![0u32; 1024],
            last_positions: vec![0u32; 1024],
        }
    }
}

impl TermVectorPostingsArray {
    fn new(size: usize) -> Self {
        let base = ParallelPostingsArray::new(size);
        TermVectorPostingsArray {
            base,
            freqs: vec![0u32; size],
            last_offsets: vec![0u32; size],
            last_positions: vec![0u32; size],
        }
    }
}

impl PostingsArray for TermVectorPostingsArray {
    fn parallel_array(&self) -> &ParallelPostingsArray {
        &self.base
    }

    fn parallel_array_mut(&mut self) -> &mut ParallelPostingsArray {
        &mut self.base
    }

    fn bytes_per_posting(&self) -> usize {
        self.base.bytes_per_posting() + 3 * 4
    }

    fn grow(&mut self) {
        self.base.grow();
        let new_size = self.base.size;
        self.freqs.resize(new_size, 0u32);
        self.last_offsets.resize(new_size, 0u32);
        self.last_positions.resize(new_size, 0u32);
    }

    fn clear(&mut self) {
        self.base.clear();
        self.freqs = Vec::with_capacity(0);
        self.last_offsets = Vec::with_capacity(0);
        self.last_positions = Vec::with_capacity(0);
    }
}
