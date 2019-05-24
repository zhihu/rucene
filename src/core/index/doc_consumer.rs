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
use core::codec::{
    Codec, DocValuesFormat, FieldInfosFormat, NormsFormat, PointsFormat, PointsWriter,
    StoredFieldsFormat, StoredFieldsWriter, StoredFieldsWriterEnum,
};
use core::doc::FieldType;
use core::index::doc_values_type::DocValuesType;
use core::index::doc_values_writer::BinaryDocValuesWriter;
use core::index::doc_values_writer::NumericDocValuesWriter;
use core::index::doc_values_writer::SortedDocValuesWriter;
use core::index::doc_values_writer::SortedNumericDocValuesWriter;
use core::index::doc_values_writer::SortedSetDocValuesWriter;
use core::index::doc_values_writer::{DocValuesWriter, DocValuesWriterEnum};
use core::index::index_writer;
use core::index::merge_policy::MergePolicy;
use core::index::norm_values_writer::NormValuesWriter;
use core::index::point_values_writer::PointValuesWriter;
use core::index::term_vector::TermVectorsConsumer;
use core::index::terms_hash::{FreqProxTermsWriter, TermsHash};
use core::index::terms_hash_per_field::{FreqProxTermsWriterPerField, TermsHashPerField};
use core::index::thread_doc_writer::{DocState, DocumentsWriterPerThread};
use core::index::{
    FieldInfo, FieldInfosBuilder, FieldInvertState, FieldNumbersRef, Fieldable, IndexOptions,
    SegmentWriteState,
};
use core::store::{Directory, IOContext};
use core::util::{BytesRef, Counter, DocId, VariantValue};

use core::search::bm25_similarity::BM25Similarity;

use error::{
    ErrorKind::{IllegalArgument, UnsupportedOperation},
    Result,
};

use core::index::merge_scheduler::MergeScheduler;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::ptr;

const MAX_FIELD_COUNT: usize = 65536;

pub trait DocConsumer<D: Directory, C: Codec> {
    fn process_document<F: Fieldable>(
        &mut self,
        doc_state: &mut DocState,
        doc: &mut [F],
    ) -> Result<()>;

    fn flush<DW>(&mut self, state: &mut SegmentWriteState<D, DW, C>) -> Result<()>
    where
        DW: Directory + 'static;

    fn abort(&mut self) -> Result<()>;
}

pub struct DefaultIndexingChain<
    D: Directory + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
> {
    pub bytes_used: Counter,
    pub field_infos: FieldInfosBuilder<FieldNumbersRef>,
    // Writes postings and term vectors
    pub terms_hash: FreqProxTermsWriter<D, C, MS, MP>,
    // TODO, maybe we should use `TermsHash` instead
    // lazy init:
    stored_fields_writer: Option<StoredFieldsWriterEnum<D::IndexOutput>>,
    last_stored_doc_id: DocId,
    // NOTE: I tried using Hash Map<String,PerField>
    // but it was ~2% slower on Wiki and Geonames with Java
    // but we will use may anyway.
    // TODO, maybe we should use `TermsHashPerField` instead
    pub field_hash: Vec<PerField<FreqProxTermsWriterPerField<D, C, MS, MP>>>,
    total_field_count: u32,
    next_field_gen: i64,
    inited: bool,
    // Holds fields seen in each document
    fields: Vec<usize>,
    parent: *mut DocumentsWriterPerThread<D, C, MS, MP>,
}

impl<D, C, MS, MP> Default for DefaultIndexingChain<D, C, MS, MP>
where
    D: Directory + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    fn default() -> Self {
        DefaultIndexingChain {
            bytes_used: Counter::default(),
            field_infos: FieldInfosBuilder::new(FieldNumbersRef::default()),
            terms_hash: FreqProxTermsWriter::new_default(),
            stored_fields_writer: None,
            last_stored_doc_id: 0,
            field_hash: Vec::with_capacity(MAX_FIELD_COUNT),
            total_field_count: 0,
            next_field_gen: 0,
            inited: false,
            fields: vec![],
            parent: ptr::null_mut(),
        }
    }
}

impl<D, C, MS, MP> DefaultIndexingChain<D, C, MS, MP>
where
    D: Directory + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    pub fn new(
        doc_writer: &mut DocumentsWriterPerThread<D, C, MS, MP>,
        field_infos: FieldInfosBuilder<FieldNumbersRef>,
    ) -> Self {
        let term_vectors_writer = TermVectorsConsumer::new(doc_writer);
        let terms_hash = FreqProxTermsWriter::new(doc_writer, term_vectors_writer);
        let bytes_used = unsafe { doc_writer.bytes_used.shallow_copy() };

        DefaultIndexingChain {
            bytes_used,
            field_infos,
            terms_hash,
            stored_fields_writer: None,
            last_stored_doc_id: 0,
            field_hash: Vec::with_capacity(MAX_FIELD_COUNT),
            total_field_count: 0,
            next_field_gen: 0,
            inited: false,
            fields: vec![],
            parent: doc_writer,
        }
    }

    pub fn init(&mut self) {
        self.terms_hash.init();
        self.inited = true;
    }

    pub fn reset_doc_writer(&mut self, parent: *mut DocumentsWriterPerThread<D, C, MS, MP>) {
        debug_assert!(self.inited);
        self.parent = parent;
    }

    fn doc_writer(&self) -> &mut DocumentsWriterPerThread<D, C, MS, MP> {
        unsafe { &mut *self.parent }
    }

    /// Writes all buffered points.
    fn write_points<DW: Directory + 'static>(
        &mut self,
        state: &SegmentWriteState<D, DW, C>,
    ) -> Result<()> {
        let mut points_writer = None;
        for per_field in &mut self.field_hash {
            if per_field.point_values_writer.is_some() {
                debug_assert!(per_field.field_info().point_dimension_count > 0);
                if points_writer.is_none() {
                    // lazy init
                    points_writer = Some(
                        state
                            .segment_info
                            .codec()
                            .points_format()
                            .fields_writer(state)?,
                    );
                }
                per_field
                    .point_values_writer
                    .as_mut()
                    .unwrap()
                    .flush(state, points_writer.as_mut().unwrap())?;
                per_field.point_values_writer = None;
            } else {
                debug_assert_eq!(per_field.field_info().point_dimension_count, 0);
            }
        }
        if let Some(ref mut writer) = points_writer {
            writer.finish()?;
        }
        Ok(())
    }

    /// Writes all buffered doc values (called from {@link #flush}).
    fn write_doc_values<DW: Directory>(
        &mut self,
        state: &SegmentWriteState<D, DW, C>,
    ) -> Result<()> {
        let max_doc = state.segment_info.max_doc;
        let mut dv_consumer = None;
        for per_field in &mut self.field_hash {
            if per_field.doc_values_writer.is_some() {
                debug_assert_ne!(per_field.field_info().doc_values_type, DocValuesType::Null);
                if dv_consumer.is_none() {
                    dv_consumer = Some(
                        state
                            .segment_info
                            .codec()
                            .doc_values_format()
                            .fields_consumer(state)?,
                    );
                }
                per_field
                    .doc_values_writer
                    .as_mut()
                    .unwrap()
                    .finish(max_doc);
                per_field
                    .doc_values_writer
                    .as_mut()
                    .unwrap()
                    .flush(state, dv_consumer.as_mut().unwrap())?;
                per_field.doc_values_writer = None;
            } else {
                debug_assert_eq!(per_field.field_info().doc_values_type, DocValuesType::Null);
            }
        }

        debug_assert_eq!(state.field_infos.has_doc_values, dv_consumer.is_some());
        Ok(())
    }

    /// Catch up for all docs before us that had no stored fields,
    /// or hit non-aborting exceptions before writing stored fields
    fn fill_stored_fields(&mut self, doc_id: DocId) -> Result<()> {
        loop {
            if self.last_stored_doc_id >= doc_id {
                break;
            }
            self.start_stored_fields()?;
            self.finished_stored_fields()?;
        }
        Ok(())
    }

    /// Calls StoredFieldsWriter.startDocument, aborting the
    /// segment if it hits any exception
    fn start_stored_fields(&mut self) -> Result<()> {
        self.init_stored_fields_writer()?;
        self.stored_fields_writer
            .as_mut()
            .unwrap()
            .start_document()?;
        self.last_stored_doc_id += 1;
        Ok(())
    }

    /// Calls StoredFieldsWriter.finish_document, aborting
    /// the segment if it hits any exception
    fn finished_stored_fields(&mut self) -> Result<()> {
        self.stored_fields_writer
            .as_mut()
            .unwrap()
            .finish_document()
    }

    fn init_stored_fields_writer(&mut self) -> Result<()> {
        if self.stored_fields_writer.is_none() {
            self.stored_fields_writer = Some(
                self.doc_writer()
                    .codec()
                    .stored_fields_format()
                    .fields_writer(
                        self.doc_writer().directory.clone(),
                        &mut self.doc_writer().segment_info,
                        &IOContext::Default,
                    )?,
            );
        }

        Ok(())
    }

    fn write_norms<D1: Directory, D2: Directory, C1: Codec>(
        &mut self,
        state: &SegmentWriteState<D1, D2, C1>,
    ) -> Result<()> {
        let max_doc = state.segment_info.max_doc;
        if state.field_infos.has_norms {
            let norms_format = state.segment_info.codec().norms_format();
            let mut norms_consumer = norms_format.norms_consumer(state)?;

            for pf in &mut self.field_hash {
                let name = pf.name.as_str();
                if let Some(fi) = self.field_infos.by_name.get(name) {
                    if fi.omit_norms == false && fi.index_options != IndexOptions::Null {
                        debug_assert!(pf.norms.is_some());
                        if pf.norms.is_some() {
                            pf.norms.as_mut().unwrap().finish(max_doc);
                            pf.norms
                                .as_mut()
                                .unwrap()
                                .flush(state, &mut norms_consumer)?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    unsafe fn process_field(
        &mut self,
        field: &mut impl Fieldable,
        doc_state: &DocState,
        field_gen: i64,
        field_count: usize,
    ) -> Result<usize> {
        // debug_assert!(self.inited);
        // Invert indexed fields:
        let mut per_field = None;
        let mut field_count = field_count;
        if field.field_type().index_options != IndexOptions::Null {
            // if the field omits norms, the boost cannot be indexed.
            if field.field_type().omit_norms && field.boost() != 1.0f32 {
                bail!(UnsupportedOperation(Cow::Borrowed(
                    "You cannot set an index-time boost: norms are omitted"
                )));
            }

            let idx = self.get_or_add_field(field.name(), field.field_type(), true)?;
            let first = self.field_hash[idx].field_gen != field_gen;

            let ptr = self as *mut DefaultIndexingChain<D, C, MS, MP>;
            self.field_hash[idx].invert(field, doc_state, first, &mut *ptr)?;

            if first {
                self.fields.push(idx);
                field_count += 1;
                self.field_hash[idx].field_gen = field_gen;
            }
            per_field = Some(idx);
        } else {
            Self::verify_uninverted_field_type(field.name(), field.field_type())?;
        }

        // Add stored fields:
        if field.field_type().stored() {
            if per_field.is_none() {
                per_field = Some(self.get_or_add_field(field.name(), field.field_type(), false)?);
            }
            if field.field_type().stored() {
                if let Some(VariantValue::VString(ref s)) = field.fields_data() {
                    if s.len() > 16 * 1024 * 1024 {
                        bail!(IllegalArgument("stored field is too large".into()));
                    }
                }
                self.stored_fields_writer
                    .as_mut()
                    .unwrap()
                    .write_field(self.field_hash[per_field.unwrap()].field_info(), field)?;
            }
        }

        let dv_type = field.field_type().doc_values_type;
        if dv_type != DocValuesType::Null {
            if per_field.is_none() {
                per_field = Some(self.get_or_add_field(field.name(), field.field_type(), false)?);
            }
            self.index_doc_value(per_field.unwrap(), dv_type, field, doc_state)?;
        }

        if field.field_type().dimension_count > 0 {
            if per_field.is_none() {
                per_field = Some(self.get_or_add_field(field.name(), field.field_type(), false)?);
            }
            self.index_point(per_field.unwrap(), field, doc_state)?;
        }

        Ok(field_count)
    }

    fn verify_uninverted_field_type(_name: &str, ft: &FieldType) -> Result<()> {
        if ft.store_term_vectors {
            bail!(IllegalArgument(
                "cannot store term vectors for not indexed field!".into()
            ));
        }
        if ft.store_term_vector_positions {
            bail!(IllegalArgument(
                "cannot store term vectors positions for not indexed field!".into()
            ));
        }
        if ft.store_term_vector_offsets {
            bail!(IllegalArgument(
                "cannot store term vectors offsets for not indexed field!".into()
            ));
        }
        if ft.store_term_vector_payloads {
            bail!(IllegalArgument(
                "cannot store term vectors payloads for not indexed field!".into()
            ));
        }
        Ok(())
    }

    fn reset_field_info_ptr(&mut self) -> Result<()> {
        for fp in &mut self.field_hash {
            fp.field_info = self.field_infos.get_or_add(&fp.name)?;
        }

        Ok(())
    }

    /// Returns a previously created `PerField`
    /// absorbing the type information from `FieldType` and creates a new
    /// `PerField` if this field name wasn't seen yet
    fn get_or_add_field(
        &mut self,
        name: &str,
        field_type: &FieldType,
        invert: bool,
    ) -> Result<usize> {
        let mut idx = self.field_hash.len();
        for i in 0..self.field_hash.len() {
            unsafe {
                if name == (*self.field_hash[i].field_info).name {
                    idx = i;
                    break;
                }
            }
        }

        // Make sure we have a PerField allocated
        if idx == self.field_hash.len() {
            // First time we are seeing this field in this segment
            let mut fi = self.field_infos.get_or_add(name)?;
            // Messy: must set this here because e.g. FreqProxTermsWriterPerField looks at the
            // initial IndexOptions to decide what arrays it must create).  Then, we
            // also must set it in PerField.invert to allow for later downgrading of
            // the index options:
            fi.set_index_options(field_type.index_options);

            let fp = PerField::new(&mut fi, invert, &mut self.terms_hash);
            self.field_hash.push(fp);

            if let Some(fp) = self.field_hash.last_mut() {
                if let Some(ref mut term_hash_per_field) = fp.term_hash_per_field {
                    term_hash_per_field.init();
                }
            }

            self.total_field_count += 1;
        } else {
            // Messy: must set this here because e.g. FreqProxTermsWriterPerField looks at the
            // initial IndexOptions to decide what arrays it must create).  Then, we
            // also must set it in PerField.invert to allow for later downgrading of
            // the index options:
            let pf = self.field_hash.get_mut(idx).unwrap();
            if invert && !pf.invert {
                pf.field_info_mut()
                    .set_index_options(field_type.index_options);
                pf.set_invert_state(&mut self.terms_hash);
            }
        }

        self.reset_field_info_ptr()?;

        Ok(idx)
    }

    /// Called from processDocument to index one field's doc value
    fn index_doc_value(
        &mut self,
        field_idx: usize,
        dv_type: DocValuesType,
        field: &impl Fieldable,
        doc_state: &DocState,
    ) -> Result<()> {
        let per_field = &mut self.field_hash[field_idx];

        if per_field.field_info().doc_values_type == DocValuesType::Null {
            // This is the first time we are seeing this field indexed with doc values, so we
            // now record the DV type so that any future attempt to (illegally) change
            // the DV type of this field, will throw an IllegalArgExc:
            self.field_infos
                .global_field_numbers
                .as_ref()
                .set_doc_values_type(
                    per_field.field_info().number,
                    &per_field.field_info().name,
                    dv_type,
                )?;
        }
        per_field.field_info_mut().set_doc_values_type(dv_type)?;
        let doc_id = doc_state.doc_id;

        match dv_type {
            DocValuesType::Numeric => {
                if per_field.doc_values_writer.is_none() {
                    per_field.doc_values_writer = Some(DocValuesWriterEnum::Numeric(
                        NumericDocValuesWriter::new(per_field.field_info()),
                    ));
                }
                let doc_value_writer = per_field.doc_values_writer.as_mut().unwrap();
                debug_assert_eq!(doc_value_writer.doc_values_type(), DocValuesType::Numeric);
                if let DocValuesWriterEnum::Numeric(ref mut n) = doc_value_writer {
                    n.add_value(doc_id, field.numeric_value().unwrap().long_value())?;
                }
            }
            DocValuesType::Binary => {
                if per_field.doc_values_writer.is_none() {
                    per_field.doc_values_writer = Some(DocValuesWriterEnum::Binary(
                        BinaryDocValuesWriter::new(per_field.field_info())?,
                    ));
                }
                let doc_value_writer = per_field.doc_values_writer.as_mut().unwrap();
                debug_assert_eq!(doc_value_writer.doc_values_type(), DocValuesType::Binary);
                if let DocValuesWriterEnum::Binary(ref mut b) = doc_value_writer {
                    b.add_value(doc_id, &BytesRef::new(field.binary_value().unwrap()))?;
                }
            }
            DocValuesType::Sorted => {
                if per_field.doc_values_writer.is_none() {
                    per_field.doc_values_writer = Some(DocValuesWriterEnum::Sorted(
                        SortedDocValuesWriter::new(per_field.field_info(), unsafe {
                            self.bytes_used.shallow_copy()
                        }),
                    ))
                };
                let doc_value_writer = per_field.doc_values_writer.as_mut().unwrap();
                debug_assert_eq!(doc_value_writer.doc_values_type(), DocValuesType::Sorted);
                if let DocValuesWriterEnum::Sorted(ref mut s) = doc_value_writer {
                    s.add_value(doc_id, &BytesRef::new(field.binary_value().unwrap()))?;
                }
            }
            DocValuesType::SortedNumeric => {
                if per_field.doc_values_writer.is_none() {
                    per_field.doc_values_writer = Some(DocValuesWriterEnum::SortedNumeric(
                        SortedNumericDocValuesWriter::new(per_field.field_info()),
                    ));
                }
                let doc_value_writer = per_field.doc_values_writer.as_mut().unwrap();
                debug_assert_eq!(
                    doc_value_writer.doc_values_type(),
                    DocValuesType::SortedNumeric
                );
                if let DocValuesWriterEnum::SortedNumeric(ref mut s) = doc_value_writer {
                    s.add_value(doc_id, field.numeric_value().unwrap().long_value());
                }
            }
            DocValuesType::SortedSet => {
                if per_field.doc_values_writer.is_none() {
                    per_field.doc_values_writer = unsafe {
                        Some(DocValuesWriterEnum::SortedSet(
                            SortedSetDocValuesWriter::new(
                                per_field.field_info(),
                                self.bytes_used.shallow_copy(),
                            ),
                        ))
                    };
                }
                let doc_value_writer = per_field.doc_values_writer.as_mut().unwrap();
                debug_assert_eq!(doc_value_writer.doc_values_type(), DocValuesType::SortedSet);
                if let DocValuesWriterEnum::SortedSet(ref mut s) = doc_value_writer {
                    s.add_value(doc_id, &BytesRef::new(field.binary_value().unwrap()))?;
                }
            }
            _ => {
                unreachable!();
            }
        }
        Ok(())
    }

    /// Called from process_document to index one field's point
    fn index_point(
        &mut self,
        field_idx: usize,
        field: &impl Fieldable,
        doc_state: &DocState,
    ) -> Result<()> {
        let doc_writer = unsafe { &mut (*self.parent) };
        let per_field = &mut self.field_hash[field_idx];
        let point_dimension_count = field.field_type().dimension_count;
        let dimension_num_bytes = field.field_type().dimension_num_bytes;

        // Record dimensions for this field; this setter will throw IllegalArgExc if
        // the dimensions were already set to something different:
        if per_field.field_info().point_dimension_count == 0 {
            self.field_infos
                .global_field_numbers
                .as_ref()
                .set_dimensions(
                    per_field.field_info().number,
                    &per_field.field_info().name,
                    point_dimension_count,
                    dimension_num_bytes,
                )?;
            self.field_infos
                .get_or_add(&per_field.field_info().name)?
                .set_dimensions(point_dimension_count, dimension_num_bytes)?;
        }

        if per_field.point_values_writer.is_none() {
            // FIXME
            per_field.point_values_writer =
                Some(PointValuesWriter::new(doc_writer, per_field.field_info()));
        }
        per_field
            .point_values_writer
            .as_mut()
            .unwrap()
            .add_packed_value(
                doc_state.doc_id,
                &BytesRef::new(field.binary_value().unwrap()),
            )
    }
}

impl<D, C, MS, MP> DocConsumer<D, C> for DefaultIndexingChain<D, C, MS, MP>
where
    D: Directory + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    fn process_document<F: Fieldable>(
        &mut self,
        doc_state: &mut DocState,
        doc: &mut [F],
    ) -> Result<()> {
        // debug_assert!(self.inited);
        // How many indexed field names we've seen (collapses
        // multiple field instances by the same name):
        let mut field_count = 0;
        self.fields.clear();
        let field_gen = self.next_field_gen;
        self.next_field_gen += 1;

        // NOTE: we need two passes here, in case there are
        // multi-valued fields, because we must process all
        // instances of a given field at once, since the
        // analyzer is free to reuse TokenStream across fields
        // (i.e., we cannot have more than one TokenStream
        // running "at once"):
        self.terms_hash.start_document()?;

        self.fill_stored_fields(doc_state.doc_id)?;
        self.start_stored_fields()?;
        for field in doc {
            field_count = unsafe { self.process_field(field, doc_state, field_gen, field_count)? };
        }
        // Finish each indexed field name seen in the document:
        for i in 0..field_count {
            let idx = self.fields[i];
            self.field_hash[idx].finish(doc_state)?;
        }

        self.finished_stored_fields()?;

        let res = self.terms_hash.finish_document(&mut self.field_infos);

        res
    }

    fn flush<DW: Directory + 'static>(
        &mut self,
        state: &mut SegmentWriteState<D, DW, C>,
    ) -> Result<()> {
        debug_assert!(self.inited);
        // NOTE: caller (DocumentsWriterPerThread) handles
        // aborting on any exception from this method
        let max_doc = state.segment_info.max_doc();
        self.write_norms(state)?;

        for per_field in &mut self.field_hash {
            let field_info = self.field_infos.get_or_add(&per_field.invert_state.name)?;
            per_field.reset_field_info_ptr(field_info);
        }

        self.write_doc_values(state)?;
        self.write_points(state)?;

        // it's possible all docs hit non-aborting exceptions...
        self.init_stored_fields_writer()?;
        self.fill_stored_fields(max_doc)?;
        self.stored_fields_writer
            .as_mut()
            .unwrap()
            .finish(&state.field_infos, max_doc as usize)?;
        self.stored_fields_writer = None;

        {
            let mut fields_to_flush = BTreeMap::new();
            for per_field in &mut self.field_hash {
                if per_field.invert {
                    per_field
                        .term_hash_per_field
                        .as_mut()
                        .unwrap()
                        .reset_ptr(&mut self.terms_hash.base);

                    fields_to_flush.insert(
                        per_field.field_info().name.as_ref(),
                        per_field.term_hash_per_field.as_ref().unwrap(),
                    );
                }
            }

            // Important to save after asking consumer to flush so
            // consumer can alter the FieldInfo* if necessary.  EG,
            // FreqProxTermsWriter does this with
            // FieldInfo.storePayload.

            self.terms_hash.flush(fields_to_flush, state)?;
        }

        let codec = self.doc_writer().codec();
        codec.field_infos_format().write(
            state.directory.as_ref(),
            &state.segment_info,
            "",
            &state.field_infos,
            &IOContext::Default,
        )
    }

    fn abort(&mut self) -> Result<()> {
        let res = self.terms_hash.abort();
        self.field_hash.clear();
        res
    }
}

pub struct PerField<T: TermsHashPerField> {
    name: String,
    field_info: *mut FieldInfo,
    // similarity: Similarity,
    invert_state: FieldInvertState,
    pub term_hash_per_field: Option<T>,
    // Non-null if this field ever had doc values in this segment:
    doc_values_writer: Option<DocValuesWriterEnum>,
    // Non-null if this field ever had points in this segment:
    point_values_writer: Option<PointValuesWriter>,
    /// We use this to know when a PerField is seen for the
    /// first time in the current document
    field_gen: i64,
    norms: Option<NormValuesWriter>,
    invert: bool,
}

impl<T: TermsHashPerField> PerField<T> {
    fn new<D: Directory, C: Codec, TH: TermsHash<D, C, PerField = T>>(
        field_info: &mut FieldInfo,
        invert: bool,
        terms_hash: &mut TH,
    ) -> Self {
        let term_hash_per_field: Option<T> = None;
        let invert_state = FieldInvertState::with_name(field_info.name.clone());
        let mut per_field = PerField {
            name: field_info.name.clone(),
            field_info,
            invert_state,
            term_hash_per_field,
            doc_values_writer: None,
            point_values_writer: None,
            field_gen: -1,
            norms: None,
            invert,
        };

        if invert {
            per_field.set_invert_state(terms_hash);
        }

        per_field
    }

    fn field_info(&self) -> &FieldInfo {
        unsafe { &*self.field_info }
    }

    fn field_info_mut(&self) -> &mut FieldInfo {
        unsafe { &mut *self.field_info }
    }

    fn reset_field_info_ptr(&mut self, field_info: &mut FieldInfo) {
        self.field_info = field_info;
    }

    fn set_invert_state<D: Directory, C: Codec, TH: TermsHash<D, C, PerField = T>>(
        &mut self,
        terms_hash: &mut TH,
    ) {
        // self.invert_state.name = self.field_info.name.clone();
        let pf = terms_hash.add_field(&self.invert_state, self.field_info());
        self.term_hash_per_field = Some(pf);

        if !self.field_info().omit_norms {
            self.norms = Some(NormValuesWriter::new(self.field_info()));
        }
        self.invert = true;
    }

    fn finish(&mut self, doc_state: &DocState) -> Result<()> {
        if self.field_info().omit_norms == false && self.invert_state.length != 0 {
            debug_assert!(self.norms.is_some());
            let doc_id = doc_state.doc_id;
            self.norms
                .as_mut()
                .unwrap()
                .add_value(doc_id, BM25Similarity::compute_norm(&self.invert_state));
        }

        self.term_hash_per_field
            .as_mut()
            .unwrap()
            .finish(&mut self.invert_state)?;
        if self
            .term_hash_per_field
            .as_ref()
            .unwrap()
            .base()
            .field_info
            .has_store_payloads
        {
            self.field_info_mut().set_store_payloads();
        }

        Ok(())
    }

    fn invert<D: Directory, C: Codec, MS: MergeScheduler, MP: MergePolicy>(
        &mut self,
        field: &mut impl Fieldable,
        doc_state: &DocState,
        first: bool,
        index_chain: &mut DefaultIndexingChain<D, C, MS, MP>,
    ) -> Result<()> {
        if first {
            // First time we're seeing this field (indexed) in
            // this document:
            self.invert_state.reset();
        }

        let index_options = field.field_type().index_options;
        index_chain
            .field_infos
            .by_name
            .get_mut(&self.field_info().name)
            .unwrap()
            .index_options = index_options;

        if field.field_type().omit_norms {
            index_chain
                .field_infos
                .by_name
                .get_mut(&self.field_info().name)
                .unwrap()
                .omit_norms = true;
        }

        // let analyzed = field.field_type().tokenized() && doc_state.analyzer.is_some();
        // only bother checking offsets if something will consume them.
        // TODO: after we fix analyzers, also check if termVectorOffsets will be indexed.
        let check_offset = index_options == IndexOptions::DocsAndFreqsAndPositionsAndOffsets;

        // To assist people in tracking down problems in analysis components, we wish to
        // write the field name to the infostream when we fail. We expect some caller to
        // eventually deal with the real exception, so we don't want any 'catch' clauses,
        // but rather a finally that takes note of the problem.
        let mut token_stream: Box<dyn TokenStream> = field.token_stream()?;
        token_stream.reset()?;

        self.term_hash_per_field
            .as_mut()
            .unwrap()
            .start(&self.invert_state, field, first)?;

        loop {
            let end = token_stream.increment_token()?;
            if !end {
                break;
            }

            // If we hit an exception in stream.next below
            // (which is fairly common, e.g. if analyzer
            // chokes on a given document), then it's
            // non-aborting and (above) this one document
            // will be marked as deleted, but still
            // consume a docID
            let pos_incr = token_stream
                .position_attribute_mut()
                .get_position_increment();
            self.invert_state.position += pos_incr as i32;
            if self.invert_state.position < self.invert_state.last_position {
                if pos_incr == 0 {
                    bail!(IllegalArgument(
                        "first position increment must be > 0 (got 0)".into()
                    ));
                } else {
                    bail!(IllegalArgument(
                        "position overflowed Integer.MAX_VALUE".into()
                    ));
                }
            } else if self.invert_state.position > index_writer::INDEX_MAX_POSITION {
                bail!(IllegalArgument(
                    "position is exceed field max allowed position".into()
                ));
            }
            self.invert_state.last_position = self.invert_state.position;
            if pos_incr == 0 {
                self.invert_state.num_overlap += 1;
            }

            if check_offset {
                let start_offset =
                    self.invert_state.offset + token_stream.offset_attribute_mut().start_offset();
                let end_offset =
                    self.invert_state.offset + token_stream.offset_attribute_mut().end_offset();
                if (start_offset as i32) < self.invert_state.last_start_offset
                    || end_offset < start_offset
                {
                    bail!(IllegalArgument(
                        "startOffset must be non-negative, and endOffset must be >= startOffset, \
                         and offsets must not go backwards"
                            .into()
                    ));
                }
                self.invert_state.last_start_offset = start_offset as i32;
            }

            self.invert_state.length += 1;
            if self.invert_state.length < 0 {
                bail!(IllegalArgument("too many tokens in field".into()));
            }

            // If we hit an exception in here, we abort
            // all buffered documents since the last
            // flush, on the likelihood that the
            // internal state of the terms hash is now
            // corrupt and should not be flushed to a
            // new segment:
            self.term_hash_per_field.as_mut().unwrap().add(
                &mut self.invert_state,
                token_stream.as_ref(),
                doc_state.doc_id,
            )?;
        }
        // trigger streams to perform end-of-stream operations
        token_stream.end()?;

        // TODO: maybe add some safety? then again, it's already checked
        // when we come back around to the field...
        self.invert_state.position += token_stream
            .position_attribute_mut()
            .get_position_increment() as i32;
        self.invert_state.offset += token_stream.offset_attribute_mut().end_offset();

        //        if analyzed {
        //            self.invert_state.position += doc_state.analyzer.get_position_increment_gap();
        //            self.invert_state.offset += doc_state.analyzer.get_offset_gap();
        //        }

        self.invert_state.boost *= field.boost();

        Ok(())
    }
}

impl<T: TermsHashPerField> Eq for PerField<T> {}

impl<T: TermsHashPerField> PartialEq for PerField<T> {
    fn eq(&self, other: &Self) -> bool {
        self.field_info().name.eq(&other.field_info().name)
    }
}

impl<T: TermsHashPerField> Ord for PerField<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.field_info().name.cmp(&other.field_info().name)
    }
}

impl<T: TermsHashPerField> PartialOrd for PerField<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
