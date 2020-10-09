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
use core::codec::field_infos::FieldInfo;
use core::codec::segment_infos::SegmentWriteState;
use core::codec::stored_fields::{
    StoredFieldsFormat, StoredFieldsReader, StoredFieldsWriter, StoredFieldsWriterEnum,
};
use core::codec::Codec;
use core::codec::SorterDocMap;
use core::doc::{FieldType, Fieldable, Status, StoredFieldVisitor, STORE_FIELD_TYPE};
use core::index::merge::MergePolicy;
use core::index::merge::MergeScheduler;
use core::index::writer::{
    DocumentsWriterPerThread, TrackingTmpDirectory, TrackingTmpOutputDirectoryWrapper,
    TrackingValidDirectory,
};
use core::store::directory::Directory;
use core::store::IOContext;
use core::util::Numeric;
use core::util::{DocId, VariantValue};

use std::ptr;
use std::sync::Arc;

use error::Result;

pub struct StoredFieldsConsumerImpl<
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
    DW: Directory + Send + Sync + 'static,
> {
    doc_writer: *mut DocumentsWriterPerThread<D, C, MS, MP>,
    writer: Option<StoredFieldsWriterEnum<DW::IndexOutput>>,
    last_doc: i32,
    dir: Arc<DW>,
}

impl<D, C, MS, MP, DW> StoredFieldsConsumerImpl<D, C, MS, MP, DW>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
    DW: Directory + Send + Sync + 'static,
{
    pub fn new(doc_writer: *mut DocumentsWriterPerThread<D, C, MS, MP>, dir: Arc<DW>) -> Self {
        Self {
            doc_writer,
            writer: None,
            last_doc: -1,
            dir,
        }
    }

    fn dwpt(&mut self) -> &mut DocumentsWriterPerThread<D, C, MS, MP> {
        unsafe { &mut *self.doc_writer }
    }

    pub fn init_stored_fields_writer(&mut self) -> Result<()> {
        if self.writer.is_none() {
            let dir = Arc::clone(&self.dir);
            let dwpt = self.dwpt();
            let writer = dwpt.codec().stored_fields_format().fields_writer(
                dir,
                &mut dwpt.segment_info,
                &IOContext::Default,
            )?;
            self.writer = Some(writer);
        }
        Ok(())
    }

    pub fn start_document(&mut self, doc_id: DocId) -> Result<()> {
        self.init_stored_fields_writer()?;
        self.do_start_document(doc_id)
    }

    fn do_start_document(&mut self, doc_id: DocId) -> Result<()> {
        debug_assert!(self.last_doc < doc_id);
        loop {
            self.last_doc += 1;
            if self.last_doc >= doc_id {
                break;
            }
            self.writer.as_mut().unwrap().start_document()?;
            self.writer.as_mut().unwrap().finish_document()?;
        }
        self.writer.as_mut().unwrap().start_document()
    }

    pub fn write_field(&mut self, field_info: &FieldInfo, field: &impl Fieldable) -> Result<()> {
        self.writer.as_mut().unwrap().write_field(field_info, field)
    }

    pub fn finish_document(&mut self) -> Result<()> {
        self.writer.as_mut().unwrap().finish_document()
    }

    pub fn finish(&mut self, max_doc: i32) -> Result<()> {
        while self.last_doc < max_doc - 1 {
            self.do_start_document(self.last_doc)?;
            self.finish_document()?;
            self.last_doc += 1;
        }
        Ok(())
    }

    pub fn flush<DW1: Directory>(
        &mut self,
        flush_state: &SegmentWriteState<D, DW1, C>,
        _sort_map: Option<&impl SorterDocMap>,
    ) -> Result<()> {
        let res = self.writer.as_mut().unwrap().finish(
            &flush_state.field_infos,
            flush_state.segment_info.max_doc as usize,
        );
        self.writer = None;
        res
    }

    pub fn abort(&mut self) {
        self.writer = None;
    }
}

pub struct SortingStoredFieldsConsumerImpl<
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
> {
    consumer: StoredFieldsConsumerImpl<D, C, MS, MP, TrackingTmpDirectory<D>>,
    tmp_directory: Arc<TrackingTmpDirectory<D>>,
}

impl<D, C, MS, MP> SortingStoredFieldsConsumerImpl<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    pub fn new(doc_writer: *mut DocumentsWriterPerThread<D, C, MS, MP>) -> Self {
        let dwpt = unsafe { &*doc_writer };
        let dir = Arc::new(TrackingTmpOutputDirectoryWrapper::new(Arc::clone(
            &dwpt.directory,
        )));
        let consumer = StoredFieldsConsumerImpl::new(doc_writer, Arc::clone(&dir));
        Self {
            consumer,
            tmp_directory: dir,
        }
    }

    #[inline]
    pub fn start_document(&mut self, doc_id: DocId) -> Result<()> {
        self.consumer.start_document(doc_id)
    }
    #[inline]
    pub fn write_field(&mut self, field_info: &FieldInfo, field: &impl Fieldable) -> Result<()> {
        self.consumer.write_field(field_info, field)
    }
    #[inline]
    pub fn finish_document(&mut self) -> Result<()> {
        self.consumer.finish_document()
    }

    #[inline]
    pub fn finish(&mut self, max_doc: i32) -> Result<()> {
        self.consumer.finish(max_doc)
    }

    pub fn flush<DW>(
        &mut self,
        flush_state: &mut SegmentWriteState<D, DW, C>,
        sort_map: Option<&impl SorterDocMap>,
    ) -> Result<()>
    where
        DW: Directory,
        <DW as Directory>::IndexOutput: 'static,
    {
        self.consumer.flush(flush_state, sort_map)?;
        if let Some(sort_map) = sort_map {
            let res = self.flush_sorted(flush_state, sort_map);
            self.delete_tmp_files();
            res
        } else {
            // we're lucky the index is already sorted, just rename the temporary file and return.
            for (k, v) in &*self.tmp_directory.file_names.lock().unwrap() {
                self.tmp_directory.rename(v, k)?;
            }
            Ok(())
        }
    }

    fn flush_sorted<DW>(
        &mut self,
        flush_state: &mut SegmentWriteState<D, DW, C>,
        sort_map: &dyn SorterDocMap,
    ) -> Result<()>
    where
        DW: Directory,
        <DW as Directory>::IndexOutput: 'static,
    {
        let reader = self
            .consumer
            .dwpt()
            .codec()
            .stored_fields_format()
            .fields_reader(
                self.tmp_directory.as_ref(),
                &flush_state.segment_info,
                Arc::new(flush_state.field_infos.clone()),
                &IOContext::Default,
            )?;
        let mut merge_reader = reader.get_merge_instance()?;
        let mut sort_writer = self
            .consumer
            .dwpt()
            .codec()
            .stored_fields_format()
            .fields_writer(
                Arc::clone(&flush_state.directory),
                &mut flush_state.segment_info,
                &IOContext::Default,
            )?;
        let mut visitor = CopyVisitor::new(&mut sort_writer);
        for i in 0..flush_state.segment_info.max_doc {
            visitor.writer.start_document()?;
            merge_reader.visit_document_mut(sort_map.new_to_old(i), &mut visitor)?;
            visitor.writer.finish_document()?;
        }
        sort_writer.finish(
            &flush_state.field_infos,
            flush_state.segment_info.max_doc as usize,
        )
    }

    #[inline]
    pub fn abort(&mut self) {
        self.consumer.abort();
        // self.delete_tmp_files();
    }
    #[inline]
    fn delete_tmp_files(&self) {
        self.tmp_directory.delete_temp_files();
    }
}

/// A visitor that copies every field it sees in the provided `StoredFieldsWriter`
struct CopyVisitor<'a, W: StoredFieldsWriter + 'a> {
    writer: &'a mut W,
    field: CopyField,
}

impl<'a, W: StoredFieldsWriter + 'a> CopyVisitor<'a, W> {
    fn new(writer: &'a mut W) -> Self {
        Self {
            writer,
            field: CopyField::new(),
        }
    }

    fn reset(&mut self, field_info: &FieldInfo) {
        self.field.reset(field_info);
    }

    fn write(&mut self) -> Result<()> {
        unsafe {
            self.writer
                .write_field(&*self.field.current_field, &self.field)
        }
    }
}

impl<'a, W: StoredFieldsWriter + 'a> StoredFieldVisitor for CopyVisitor<'a, W> {
    fn add_binary_field(&mut self, field_info: &FieldInfo, value: Vec<u8>) -> Result<()> {
        self.reset(field_info);
        self.field.binary_value = Some(value);
        self.write()
    }

    fn add_string_field(&mut self, field_info: &FieldInfo, value: Vec<u8>) -> Result<()> {
        self.reset(field_info);
        self.field.string_value = unsafe { Some(String::from_utf8_unchecked(value)) };
        self.write()
    }

    fn add_int_field(&mut self, field_info: &FieldInfo, value: i32) -> Result<()> {
        self.reset(field_info);
        self.field.numeric_value = Some(Numeric::Int(value));
        self.write()
    }
    fn add_long_field(&mut self, field_info: &FieldInfo, value: i64) -> Result<()> {
        self.reset(field_info);
        self.field.numeric_value = Some(Numeric::Long(value));
        self.write()
    }
    fn add_float_field(&mut self, field_info: &FieldInfo, value: f32) -> Result<()> {
        self.reset(field_info);
        self.field.numeric_value = Some(Numeric::Float(value));
        self.write()
    }
    fn add_double_field(&mut self, field_info: &FieldInfo, value: f64) -> Result<()> {
        self.reset(field_info);
        self.field.numeric_value = Some(Numeric::Double(value));
        self.write()
    }

    fn needs_field(&self, _field_info: &FieldInfo) -> Status {
        Status::Yes
    }
}

struct CopyField {
    binary_value: Option<Vec<u8>>,
    string_value: Option<String>,
    numeric_value: Option<Numeric>,
    current_field: *const FieldInfo,
}

impl CopyField {
    fn new() -> Self {
        Self {
            binary_value: None,
            string_value: None,
            numeric_value: None,
            current_field: ptr::null(),
        }
    }

    fn reset(&mut self, field: &FieldInfo) {
        self.current_field = field;
        self.binary_value = None;
        self.numeric_value = None;
        self.string_value = None;
    }
}

impl Fieldable for CopyField {
    fn name(&self) -> &str {
        unsafe { &(*self.current_field).name }
    }

    fn field_type(&self) -> &FieldType {
        &STORE_FIELD_TYPE
    }

    fn boost(&self) -> f32 {
        1.0
    }

    fn field_data(&self) -> Option<&VariantValue> {
        unreachable!()
    }

    fn token_stream(&mut self) -> Result<Box<dyn TokenStream>> {
        unreachable!()
    }

    fn binary_value(&self) -> Option<&[u8]> {
        self.binary_value.as_ref().map(|b| b.as_slice())
    }

    fn string_value(&self) -> Option<&str> {
        self.string_value.as_ref().map(|b| b.as_ref())
    }

    fn numeric_value(&self) -> Option<Numeric> {
        self.numeric_value
    }
}

pub enum StoredFieldsConsumer<
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
> {
    Raw(StoredFieldsConsumerImpl<D, C, MS, MP, TrackingValidDirectory<D>>),
    Sorting(SortingStoredFieldsConsumerImpl<D, C, MS, MP>),
}

impl<D, C, MS, MP> StoredFieldsConsumer<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    pub fn new_raw(dwpt: *mut DocumentsWriterPerThread<D, C, MS, MP>) -> Self {
        let dir = unsafe { Arc::clone(&(*dwpt).directory) };
        StoredFieldsConsumer::Raw(StoredFieldsConsumerImpl::new(dwpt, dir))
    }

    pub fn new_sorting(dwpt: *mut DocumentsWriterPerThread<D, C, MS, MP>) -> Self {
        StoredFieldsConsumer::Sorting(SortingStoredFieldsConsumerImpl::new(dwpt))
    }

    pub fn reset_doc_writer(&mut self, parent: *mut DocumentsWriterPerThread<D, C, MS, MP>) {
        match self {
            StoredFieldsConsumer::Raw(c) => c.doc_writer = parent,
            StoredFieldsConsumer::Sorting(c) => c.consumer.doc_writer = parent,
        }
    }

    pub fn start_document(&mut self, doc_id: DocId) -> Result<()> {
        match self {
            StoredFieldsConsumer::Raw(c) => c.start_document(doc_id),
            StoredFieldsConsumer::Sorting(c) => c.start_document(doc_id),
        }
    }
    #[inline]
    pub fn write_field(&mut self, field_info: &FieldInfo, field: &impl Fieldable) -> Result<()> {
        match self {
            StoredFieldsConsumer::Raw(c) => c.write_field(field_info, field),
            StoredFieldsConsumer::Sorting(c) => c.write_field(field_info, field),
        }
    }
    #[inline]
    pub fn finish_document(&mut self) -> Result<()> {
        match self {
            StoredFieldsConsumer::Raw(c) => c.finish_document(),
            StoredFieldsConsumer::Sorting(c) => c.finish_document(),
        }
    }

    #[inline]
    pub fn finish(&mut self, max_doc: i32) -> Result<()> {
        match self {
            StoredFieldsConsumer::Raw(c) => c.finish(max_doc),
            StoredFieldsConsumer::Sorting(c) => c.finish(max_doc),
        }
    }

    pub fn flush<DW>(
        &mut self,
        flush_state: &mut SegmentWriteState<D, DW, C>,
        sort_map: Option<&impl SorterDocMap>,
    ) -> Result<()>
    where
        DW: Directory,
        <DW as Directory>::IndexOutput: 'static,
    {
        match self {
            StoredFieldsConsumer::Raw(c) => c.flush(flush_state, sort_map),
            StoredFieldsConsumer::Sorting(c) => c.flush(flush_state, sort_map),
        }
    }

    pub fn abort(&mut self) {
        match self {
            StoredFieldsConsumer::Raw(c) => c.abort(),
            StoredFieldsConsumer::Sorting(c) => c.abort(),
        }
    }
}
