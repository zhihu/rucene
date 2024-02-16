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
use core::codec::field_infos::{
    FieldInfo, FieldInfos, FieldInfosBuilder, FieldInvertState, FieldNumbersRef,
};
use core::codec::norms::NormsProducer;
use core::codec::postings::{
    ParallelPostingsArray, PostingsArray, TermsHash, TermsHashBase, TermsHashPerField,
    TermsHashPerFieldBase,
};
use core::codec::segment_infos::SegmentWriteState;
use core::codec::term_vectors::{
    TermVectorsFormat, TermVectorsReader, TermVectorsWriter, TermVectorsWriterEnum,
};
use core::codec::Codec;
use core::codec::{Fields, TermIterator, Terms};
use core::codec::{PackedLongDocMap, SorterDocMap};
use core::codec::{PostingIterator, PostingIteratorFlags};
use core::doc::Fieldable;
use core::doc::IndexOptions;
use core::index::merge::{MergePolicy, MergeScheduler};
use core::index::writer::{
    DocumentsWriterPerThread, TrackingTmpDirectory, TrackingTmpOutputDirectoryWrapper,
    TrackingValidDirectory,
};
use core::search::{DocIterator, NO_MORE_DOCS};
use core::store::directory::Directory;
use core::store::{FlushInfo, IOContext};
use core::util::{ByteBlockPool, ByteSliceReader, BytesRef, DocId};

use error::{ErrorKind, Result};

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::Arc;

pub struct TermVectorsConsumerImpl<
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
    DW: Directory + Send + Sync + 'static,
> {
    pub base: TermsHashBase,
    // format: Box<TermVectorsFormat>,
    // directory: DirectoryRc,
    // segment_info: SegmentInfo,
    writer: Option<TermVectorsWriterEnum<DW::IndexOutput>>,
    out_dir: Arc<DW>,
    vector_slice_reader_pos: ByteSliceReader,
    vector_slice_reader_off: ByteSliceReader,
    has_vectors: bool,
    pub num_vector_fields: u32,
    last_doc_id: DocId,
    doc_writer: *const DocumentsWriterPerThread<D, C, MS, MP>,
    pub per_fields: Vec<*mut TermVectorsConsumerPerField<D, C, MS, MP>>,
    pub inited: bool,
}

impl<D, C, MS, MP, DW> TermVectorsConsumerImpl<D, C, MS, MP, DW>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
    DW: Directory + Send + Sync + 'static,
{
    pub fn new(doc_writer: &mut DocumentsWriterPerThread<D, C, MS, MP>, out_dir: Arc<DW>) -> Self {
        let base = TermsHashBase::new(doc_writer);
        TermVectorsConsumerImpl {
            base,
            writer: None,
            out_dir,
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

    pub fn terms_writer(&mut self) -> &mut TermVectorsWriterEnum<DW::IndexOutput> {
        debug_assert!(self.writer.is_some());
        self.writer.as_mut().unwrap()
    }

    fn init_term_vectors_writer(&mut self) -> Result<()> {
        debug_assert!(self.inited);
        if self.writer.is_none() {
            let doc_writer = unsafe { &*self.doc_writer };
            let context = IOContext::Flush(FlushInfo::new(doc_writer.num_docs_in_ram));
            self.writer = Some(doc_writer.codec().term_vectors_format().tv_writer(
                self.out_dir.as_ref(),
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

    fn do_flush<DW1: Directory>(
        &mut self,
        _field_to_flush: BTreeMap<&str, &TermVectorsConsumerPerField<D, C, MS, MP>>,
        state: &mut SegmentWriteState<D, DW1, C>,
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

impl<D, C, MS, MP, DW> TermVectorsConsumerImpl<D, C, MS, MP, DW>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
    DW: Directory + Send + Sync + 'static,
{
    fn flush<DW1: Directory>(
        &mut self,
        field_to_flush: BTreeMap<&str, &TermVectorsConsumerPerField<D, C, MS, MP>>,
        state: &mut SegmentWriteState<D, DW1, C>,
        _sort_map: Option<&Arc<PackedLongDocMap>>,
        _norms: Option<&impl NormsProducer>,
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
        self.base.reset();
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

        self.base.reset();
        self.reset_field();

        Ok(())
    }
}

pub struct SortingTermVectorsConsumerImpl<
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
> {
    consumer: TermVectorsConsumerImpl<D, C, MS, MP, TrackingTmpDirectory<D>>,
    tmp_directory: Arc<TrackingTmpDirectory<D>>,
}

impl<D, C, MS, MP> SortingTermVectorsConsumerImpl<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    fn new(doc_writer: &mut DocumentsWriterPerThread<D, C, MS, MP>) -> Self {
        let dir = Arc::new(TrackingTmpOutputDirectoryWrapper::new(Arc::clone(
            &doc_writer.directory,
        )));
        let consumer = TermVectorsConsumerImpl::new(doc_writer, Arc::clone(&dir));
        Self {
            consumer,
            tmp_directory: dir,
        }
    }

    fn finish_document(
        &mut self,
        field_infos: &mut FieldInfosBuilder<FieldNumbersRef>,
    ) -> Result<()> {
        self.consumer.finish_document(field_infos)
    }

    fn flush<DW: Directory>(
        &mut self,
        field_to_flush: BTreeMap<&str, &TermVectorsConsumerPerField<D, C, MS, MP>>,
        state: &mut SegmentWriteState<D, DW, C>,
        sort_map: Option<&Arc<PackedLongDocMap>>,
        norms: Option<&impl NormsProducer>,
    ) -> Result<()> {
        let skip_flush = self.consumer.writer.is_none();
        self.consumer
            .flush(field_to_flush, state, sort_map, norms)?;
        if skip_flush {
            return Ok(());
        }

        if let Some(sort_map) = sort_map {
            let res = self.flush_sorted(state, sort_map.as_ref());
            self.tmp_directory.delete_temp_files();
            res
        } else {
            // we're lucky the index is already sorted, just rename the temporary file and return
            for (k, v) in &*self.tmp_directory.file_names.lock().unwrap() {
                self.tmp_directory.rename(v, k)?;
            }
            Ok(())
        }
    }

    fn flush_sorted<DW: Directory>(
        &mut self,
        flush_state: &SegmentWriteState<D, DW, C>,
        sort_map: &impl SorterDocMap,
    ) -> Result<()> {
        let doc_writer = unsafe { &*self.consumer.doc_writer };
        let reader = doc_writer.codec().term_vectors_format().tv_reader(
            self.tmp_directory.as_ref(),
            &flush_state.segment_info,
            Arc::new(flush_state.field_infos.clone()),
            &IOContext::Default,
        )?;
        let mut writer = doc_writer.codec().term_vectors_format().tv_writer(
            flush_state.directory.as_ref(),
            &flush_state.segment_info,
            &IOContext::Default,
        )?;
        for i in 0..flush_state.segment_info.max_doc {
            let vectors = reader.get(sort_map.new_to_old(i))?;
            Self::write_term_vectors(&mut writer, vectors, &flush_state.field_infos)?;
        }
        writer.finish(
            &flush_state.field_infos,
            flush_state.segment_info.max_doc as usize,
        )
    }

    fn write_term_vectors(
        writer: &mut impl TermVectorsWriter,
        vectors: Option<impl Fields>,
        fields_infos: &FieldInfos,
    ) -> Result<()> {
        if let Some(vectors) = vectors {
            let num_fields = vectors.size();
            writer.start_document(num_fields)?;

            let mut last_field_name = String::new();
            let mut field_count = 0;
            for field in vectors.fields() {
                field_count += 1;
                debug_assert!(field > last_field_name);
                let field_info = fields_infos.field_info_by_name(&field).unwrap();
                if let Some(terms) = vectors.terms(&field)? {
                    let has_positions = terms.has_positions()?;
                    let has_offsets = terms.has_offsets()?;
                    let has_payloads = terms.has_payloads()?;
                    debug_assert!(!has_payloads || has_positions);

                    let mut num_terms = terms.size()?;
                    if num_terms == -1 {
                        // count manually. It is stupid, but needed, as Terms.size() is not a
                        // mandatory statistics function
                        num_terms = 0;
                        let mut term_iter = terms.iterator()?;
                        while term_iter.next()?.is_some() {
                            num_terms += 1;
                        }
                    }

                    writer.start_field(
                        field_info,
                        num_terms as usize,
                        has_positions,
                        has_offsets,
                        has_payloads,
                    )?;
                    let mut terms_iter = terms.iterator()?;

                    let mut term_count = 0;
                    while let Some(term) = terms_iter.next()? {
                        term_count += 1;

                        let freq = terms_iter.total_term_freq()? as i32;
                        writer.start_term(BytesRef::new(&term), freq)?;

                        if has_positions || has_offsets {
                            let mut docs_and_pos_iter = terms_iter.postings_with_flags(
                                PostingIteratorFlags::OFFSETS | PostingIteratorFlags::PAYLOADS,
                            )?;
                            let doc = docs_and_pos_iter.next()?;
                            debug_assert_ne!(doc, NO_MORE_DOCS);
                            debug_assert_eq!(docs_and_pos_iter.freq()?, freq);

                            for _pos_upto in 0..freq {
                                let pos = docs_and_pos_iter.next_position()?;
                                let start_offset = docs_and_pos_iter.start_offset()?;
                                let end_offset = docs_and_pos_iter.end_offset()?;
                                let payloads = docs_and_pos_iter.payload()?;

                                debug_assert!(!has_positions || pos >= 0);
                                writer.add_position(pos, start_offset, end_offset, &payloads)?;
                            }
                        }
                        writer.finish_term()?;
                    }
                    debug_assert_eq!(term_count, num_terms);
                    writer.finish_field()?;
                    last_field_name = field;
                }
            }
            assert_eq!(field_count, num_fields);
        } else {
            writer.start_document(0)?;
        }
        writer.finish_document()
    }

    fn abort(&mut self) -> Result<()> {
        self.consumer.abort()?;
        self.tmp_directory.delete_temp_files();
        Ok(())
    }
}

enum TermVectorsConsumerEnum<
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
> {
    Raw(TermVectorsConsumerImpl<D, C, MS, MP, TrackingValidDirectory<D>>),
    Sorting(SortingTermVectorsConsumerImpl<D, C, MS, MP>),
}

pub struct TermVectorsConsumer<
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
>(TermVectorsConsumerEnum<D, C, MS, MP>);

impl<D, C, MS, MP> TermVectorsConsumer<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    pub fn new_raw(dwpt: &mut DocumentsWriterPerThread<D, C, MS, MP>) -> Self {
        let dir = Arc::clone(&dwpt.directory);
        let raw = TermVectorsConsumerImpl::new(dwpt, dir);
        TermVectorsConsumer(TermVectorsConsumerEnum::Raw(raw))
    }

    pub fn new_sorting(dwpt: &mut DocumentsWriterPerThread<D, C, MS, MP>) -> Self {
        let c = SortingTermVectorsConsumerImpl::new(dwpt);
        TermVectorsConsumer(TermVectorsConsumerEnum::Sorting(c))
    }

    pub fn reset_doc_writer(&mut self, parent: *const DocumentsWriterPerThread<D, C, MS, MP>) {
        match &mut self.0 {
            TermVectorsConsumerEnum::Raw(c) => c.doc_writer = parent,
            TermVectorsConsumerEnum::Sorting(c) => c.consumer.doc_writer = parent,
        }
    }

    pub fn set_term_bytes_pool(&mut self, byte_pool: *mut ByteBlockPool) {
        match &mut self.0 {
            TermVectorsConsumerEnum::Raw(c) => {
                c.base.term_byte_pool = byte_pool;
            }
            TermVectorsConsumerEnum::Sorting(c) => {
                c.consumer.base.term_byte_pool = byte_pool;
            }
        }
    }

    pub fn set_inited(&mut self, init: bool) {
        match &mut self.0 {
            TermVectorsConsumerEnum::Raw(c) => {
                c.inited = init;
            }
            TermVectorsConsumerEnum::Sorting(c) => {
                c.consumer.inited = init;
            }
        }
    }

    fn set_has_vectors(&mut self, has_vectors: bool) {
        match &mut self.0 {
            TermVectorsConsumerEnum::Raw(c) => c.has_vectors = has_vectors,
            TermVectorsConsumerEnum::Sorting(c) => c.consumer.has_vectors = has_vectors,
        }
    }

    fn add_field_to_flush(&mut self, field_to_flush: &TermVectorsConsumerPerField<D, C, MS, MP>) {
        match &mut self.0 {
            TermVectorsConsumerEnum::Raw(c) => c.add_field_to_flush(field_to_flush),
            TermVectorsConsumerEnum::Sorting(c) => c.consumer.add_field_to_flush(field_to_flush),
        }
    }

    fn base(&mut self) -> &mut TermsHashBase {
        match &mut self.0 {
            TermVectorsConsumerEnum::Raw(c) => &mut c.base,
            TermVectorsConsumerEnum::Sorting(c) => &mut c.consumer.base,
        }
    }
}

impl<D, C, MS, MP> TermsHash<D, C> for TermVectorsConsumer<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    type PerField = TermVectorsConsumerPerField<D, C, MS, MP>;

    fn base(&self) -> &TermsHashBase {
        match &self.0 {
            TermVectorsConsumerEnum::Raw(c) => &c.base,
            TermVectorsConsumerEnum::Sorting(s) => &s.consumer.base,
        }
    }

    fn base_mut(&mut self) -> &mut TermsHashBase {
        match &mut self.0 {
            TermVectorsConsumerEnum::Raw(c) => &mut c.base,
            TermVectorsConsumerEnum::Sorting(s) => &mut s.consumer.base,
        }
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
        field_to_flush: BTreeMap<&str, &Self::PerField>,
        state: &mut SegmentWriteState<D, DW, C>,
        sort_map: Option<&Arc<PackedLongDocMap>>,
        norms: Option<&impl NormsProducer>,
    ) -> Result<()> {
        match &mut self.0 {
            TermVectorsConsumerEnum::Raw(c) => c.flush(field_to_flush, state, sort_map, norms),
            TermVectorsConsumerEnum::Sorting(s) => s.flush(field_to_flush, state, sort_map, norms),
        }
    }

    fn abort(&mut self) -> Result<()> {
        match &mut self.0 {
            TermVectorsConsumerEnum::Raw(c) => c.abort(),
            TermVectorsConsumerEnum::Sorting(s) => s.abort(),
        }
    }

    fn start_document(&mut self) -> Result<()> {
        match &mut self.0 {
            TermVectorsConsumerEnum::Raw(c) => c.start_document(),
            TermVectorsConsumerEnum::Sorting(s) => s.consumer.start_document(),
        }
    }

    fn finish_document(
        &mut self,
        field_infos: &mut FieldInfosBuilder<FieldNumbersRef>,
    ) -> Result<()> {
        match &mut self.0 {
            TermVectorsConsumerEnum::Raw(c) => c.finish_document(field_infos),
            TermVectorsConsumerEnum::Sorting(s) => s.finish_document(field_infos),
        }
    }
}

pub struct TermVectorsConsumerPerField<
    D: Directory + Send + Sync + 'static,
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

impl<D, C, MS, MP> TermVectorsConsumerPerField<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    fn new(terms_writer: &mut TermVectorsConsumer<D, C, MS, MP>, field_info: FieldInfo) -> Self {
        let base = TermsHashPerFieldBase::new(
            2,
            terms_writer.base(),
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

    #[allow(clippy::mut_from_ref)]
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
        let num_postings = unsafe { self.base.bytes_hash.assume_init_ref().len() };

        // This is called once, after inverting all occurrences
        // of a given field in the doc.  At this point we flush
        // our hash into the DocWriter.
        unsafe {
            self.base.bytes_hash.assume_init_mut().sort();
        }
        match &mut self.term_vectors_writer().0 {
            TermVectorsConsumerEnum::Raw(r) => {
                r.terms_writer().start_field(
                    &self.base.field_info,
                    num_postings,
                    self.do_vector_positions,
                    self.do_vector_offsets,
                    self.has_payloads,
                )?;
            }
            TermVectorsConsumerEnum::Sorting(r) => {
                r.consumer.terms_writer().start_field(
                    &self.base.field_info,
                    num_postings,
                    self.do_vector_positions,
                    self.do_vector_offsets,
                    self.has_payloads,
                )?;
            }
        }
        for j in 0..num_postings {
            let term_id = unsafe { self.base.bytes_hash.assume_init_ref().ids[j] as usize };
            let freq = self.base.postings_array.freqs[term_id];

            // Get BytesPtr
            let flush_term = self
                .base
                .term_pool()
                .set_bytes_ref(self.base.postings_array.base.text_starts[term_id] as usize);
            tv.start_term(flush_term, freq as i32)?;

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
        unsafe {
            self.base.bytes_hash.assume_init_mut().clear(false);
        }
    }

    fn write_prox(
        &mut self,
        term_id: usize,
        field_state: &FieldInvertState,
        token_stream: &dyn TokenStream,
    ) {
        if self.do_vector_offsets {
            let start_offset = field_state.offset + token_stream.token().start_offset;
            let end_offset = field_state.offset + token_stream.token().end_offset;

            let delta = start_offset as i32 - self.base.postings_array.last_offsets[term_id] as i32;
            self.base.write_vint(1, delta);
            self.base.write_vint(1, (end_offset - start_offset) as i32);
            self.base.postings_array.last_offsets[term_id] = end_offset as u32;
        }

        if self.do_vector_positions {
            let mut payload: &[u8] = &[0u8; 0];
            if token_stream.token().payload.len() > 0 {
                payload = &token_stream.token().payload;
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

impl<D, C, MS, MP> TermsHashPerField for TermVectorsConsumerPerField<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
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
            unsafe {
                if !self.base.bytes_hash.assume_init_ref().is_empty() {
                    // Only necessary if previous doc hit a
                    // non-aborting exception while writing vectors in
                    // this field:
                    self.reset();
                }

                self.base.bytes_hash.assume_init_mut().reinit();
            }
            self.has_payloads = false;
            self.do_vectors = field.field_type().store_term_vectors();
            if self.do_vectors {
                self.term_vectors_writer().set_has_vectors(true);

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
        if self.do_vectors && unsafe { !self.base.bytes_hash.assume_init_ref().is_empty() } {
            self.term_vectors_writer().add_field_to_flush(self);
        }
        Ok(())
    }

    fn new_term(
        &mut self,
        term_id: usize,
        field_state: &mut FieldInvertState,
        token_stream: &dyn TokenStream,
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
        token_stream: &dyn TokenStream,
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

impl<D, C, MS, MP> Eq for TermVectorsConsumerPerField<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
}

impl<D, C, MS, MP> PartialEq for TermVectorsConsumerPerField<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    fn eq(&self, other: &Self) -> bool {
        self.base.field_info.name.eq(&other.base.field_info.name)
    }
}

impl<D, C, MS, MP> Ord for TermVectorsConsumerPerField<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.base.field_info.name.cmp(&other.base.field_info.name)
    }
}

impl<D, C, MS, MP> PartialOrd for TermVectorsConsumerPerField<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
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
