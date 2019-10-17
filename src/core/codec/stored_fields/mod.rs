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

mod stored_fields_reader;

pub use self::stored_fields_reader::*;

mod stored_fields_writer;

pub use self::stored_fields_writer::*;

mod stored_fields;

pub use self::stored_fields::*;

mod stored_fields_consumer;

pub use self::stored_fields_consumer::*;

use core::analysis::TokenStream;
use core::codec::field_infos::{FieldInfo, FieldInfos};
use core::codec::segment_infos::SegmentInfo;
use core::codec::stored_fields::CompressingStoredFieldsWriter;
use core::codec::Codec;
use core::doc::{FieldType, Fieldable, STORE_FIELD_TYPE};
use core::doc::{Status, StoredFieldVisitor};
use core::index::merge::doc_id_merger_of;
use core::index::merge::{DocIdMerger, DocIdMergerSub, DocIdMergerSubBase};
use core::index::merge::{LiveDocsDocMap, MergeState};
use core::search::NO_MORE_DOCS;
use core::store::directory::Directory;
use core::store::io::IndexOutput;
use core::util::{DocId, Numeric, VariantValue};

use error::Result;

use core::store::IOContext;
use std::any::Any;
use std::mem;
use std::ptr;
use std::sync::Arc;

/// Controls the format of stored fields
pub trait StoredFieldsFormat {
    type Reader: StoredFieldsReader;
    /// Returns a {@link StoredFieldsReader} to load stored
    /// fields. */
    fn fields_reader<D: Directory, DW: Directory, C: Codec>(
        &self,
        directory: &DW,
        si: &SegmentInfo<D, C>,
        field_info: Arc<FieldInfos>,
        ioctx: &IOContext,
    ) -> Result<Self::Reader>;

    /// Returns a {@link StoredFieldsWriter} to write stored
    /// fields
    fn fields_writer<D, DW, C>(
        &self,
        directory: Arc<DW>,
        si: &mut SegmentInfo<D, C>,
        ioctx: &IOContext,
    ) -> Result<StoredFieldsWriterEnum<DW::IndexOutput>>
    where
        D: Directory,
        DW: Directory,
        DW::IndexOutput: 'static,
        C: Codec;
}

pub trait StoredFieldsReader: Sized {
    // NOTE: we can't use generic for `StoredFieldVisitor` because of IndexReader#docment can't use
    // generic
    fn visit_document(&self, doc_id: DocId, visitor: &mut dyn StoredFieldVisitor) -> Result<()>;

    // a mutable version for `Self::visit_document` that maybe more fast
    fn visit_document_mut(
        &mut self,
        doc_id: DocId,
        visitor: &mut dyn StoredFieldVisitor,
    ) -> Result<()>;

    fn get_merge_instance(&self) -> Result<Self>;

    // used for type Downcast
    fn as_any(&self) -> &dyn Any;
}

impl<T: StoredFieldsReader + 'static> StoredFieldsReader for Arc<T> {
    fn visit_document(&self, doc_id: DocId, visitor: &mut dyn StoredFieldVisitor) -> Result<()> {
        (**self).visit_document(doc_id, visitor)
    }

    fn visit_document_mut(
        &mut self,
        doc_id: DocId,
        visitor: &mut dyn StoredFieldVisitor,
    ) -> Result<()> {
        debug_assert_eq!(Arc::strong_count(self), 1);
        Arc::get_mut(self)
            .unwrap()
            .visit_document_mut(doc_id, visitor)
    }

    fn get_merge_instance(&self) -> Result<Self> {
        Ok(Arc::new((**self).get_merge_instance()?))
    }

    fn as_any(&self) -> &dyn Any {
        &**self
    }
}

/// Codec API for writing stored fields:
/// <ol>
/// <li>For every document, {@link #startDocument()} is called,
/// informing the Codec that a new document has started.
/// <li>{@link #writeField(FieldInfo, IndexableField)} is called for
/// each field in the document.
/// <li>After all documents have been written, {@link #finish(FieldInfos, int)}
/// is called for verification/sanity-checks.
/// <li>Finally the writer is closed ({@link #close()})
/// </ol>
///
/// @lucene.experimental
pub trait StoredFieldsWriter {
    /// Called before writing the stored fields of the document.
    /// {@link #writeField(FieldInfo, IndexableField)} will be called
    /// for each stored field. Note that this is
    /// called even if the document has no stored fields.
    fn start_document(&mut self) -> Result<()>;

    /// Called when a document and all its fields have been added.
    fn finish_document(&mut self) -> Result<()>;

    /// Writes a single stored field.
    fn write_field(&mut self, field_info: &FieldInfo, field: &impl Fieldable) -> Result<()>;

    /// Called before {@link #close()}, passing in the number
    /// of documents that were written. Note that this is
    /// intentionally redundant (equivalent to the number of
    /// calls to {@link #startDocument()}, but a Codec should
    /// check that this is the case to detect the JRE bug described
    /// in LUCENE-1282.
    fn finish(&mut self, field_infos: &FieldInfos, num_docs: usize) -> Result<()>;

    /// Merges in the stored fields from the readers in
    /// <code>mergeState</code>. The default implementation skips
    /// over deleted documents, and uses {@link #startDocument()},
    /// {@link #writeField(FieldInfo, IndexableField)}, and {@link #finish(FieldInfos, int)},
    /// returning the number of documents that were written.
    /// Implementations can override this method for more sophisticated
    /// merging (bulk-byte copying, etc).
    fn merge<D: Directory, C: Codec>(&mut self, merge_state: &mut MergeState<D, C>) -> Result<i32>;
}

pub enum StoredFieldsWriterEnum<O: IndexOutput + 'static> {
    Compressing(CompressingStoredFieldsWriter<O>),
}

impl<O: IndexOutput + 'static> StoredFieldsWriter for StoredFieldsWriterEnum<O> {
    fn start_document(&mut self) -> Result<()> {
        match self {
            StoredFieldsWriterEnum::Compressing(w) => w.start_document(),
        }
    }

    fn finish_document(&mut self) -> Result<()> {
        match self {
            StoredFieldsWriterEnum::Compressing(w) => w.finish_document(),
        }
    }

    fn write_field(&mut self, field_info: &FieldInfo, field: &impl Fieldable) -> Result<()> {
        match self {
            StoredFieldsWriterEnum::Compressing(w) => w.write_field(field_info, field),
        }
    }

    fn finish(&mut self, field_infos: &FieldInfos, num_docs: usize) -> Result<()> {
        match self {
            StoredFieldsWriterEnum::Compressing(w) => w.finish(field_infos, num_docs),
        }
    }

    fn merge<D: Directory, C: Codec>(&mut self, merge_state: &mut MergeState<D, C>) -> Result<i32> {
        match self {
            StoredFieldsWriterEnum::Compressing(w) => w.merge(merge_state),
        }
    }
}

pub fn merge_store_fields<D: Directory, C: Codec, S: StoredFieldsWriter + 'static>(
    writer: &mut S,
    state: &mut MergeState<D, C>,
) -> Result<i32> {
    let fields_readers = mem::replace(&mut state.stored_fields_readers, Vec::with_capacity(0));
    let mut subs = Vec::with_capacity(fields_readers.len());
    let mut i = 0;
    for reader in fields_readers {
        let sub = StoredFieldsMergeSub::new(
            MergeVisitor::new(state, i, writer),
            Arc::clone(&state.doc_maps[i]),
            reader,
            state.max_docs[i],
        );
        subs.push(sub);
        i += 1;
    }
    let mut doc_id_merger = doc_id_merger_of(subs, state.needs_index_sort)?;

    let mut doc_count = 0;
    while let Some(sub) = doc_id_merger.next()? {
        debug_assert_eq!(sub.base().mapped_doc_id, doc_count);
        writer.start_document()?;
        sub.reader
            .visit_document_mut(sub.doc_id, &mut sub.visitor)?;
        writer.finish_document()?;
        doc_count += 1;
    }
    writer.finish(
        state.merge_field_infos.as_ref().unwrap().as_ref(),
        doc_count as usize,
    )?;
    Ok(doc_count)
}

struct StoredFieldsMergeSub<S: StoredFieldsWriter, R: StoredFieldsReader> {
    reader: R,
    max_doc: i32,
    visitor: MergeVisitor<S>,
    base: DocIdMergerSubBase,
    doc_id: DocId,
}

impl<S: StoredFieldsWriter, R: StoredFieldsReader> StoredFieldsMergeSub<S, R> {
    fn new(
        visitor: MergeVisitor<S>,
        doc_map: Arc<LiveDocsDocMap>,
        reader: R,
        max_doc: i32,
    ) -> Self {
        let base = DocIdMergerSubBase::new(doc_map);
        StoredFieldsMergeSub {
            reader,
            max_doc,
            visitor,
            base,
            doc_id: -1,
        }
    }
}

impl<S: StoredFieldsWriter, R: StoredFieldsReader> DocIdMergerSub for StoredFieldsMergeSub<S, R> {
    fn next_doc(&mut self) -> Result<DocId> {
        self.doc_id += 1;
        Ok(if self.doc_id == self.max_doc {
            NO_MORE_DOCS
        } else {
            self.doc_id
        })
    }

    fn base(&self) -> &DocIdMergerSubBase {
        &self.base
    }

    fn base_mut(&mut self) -> &mut DocIdMergerSubBase {
        &mut self.base
    }

    fn reset(&mut self) {
        self.base.reset();
        self.doc_id = -1;
    }
}

pub struct MergeVisitor<S: StoredFieldsWriter> {
    value: Option<VariantValue>,
    current_field: *const FieldInfo,
    fields_writer: *mut S,
    remapper: Option<Arc<FieldInfos>>,
}

impl<S: StoredFieldsWriter> MergeVisitor<S> {
    pub fn new<D: Directory, C: Codec>(
        merge_state: &MergeState<D, C>,
        reader_index: usize,
        fields_writer: &mut S,
    ) -> Self {
        // if field numbers are aligned, we can save hash lookups
        // on every field access. Otherwise, we need to lookup
        // fieldname each time, and remap to a new number.
        let mut remapper = None;
        for fi in merge_state.fields_infos[reader_index].by_number.values() {
            if let Some(info) = merge_state
                .merge_field_infos
                .as_ref()
                .unwrap()
                .field_info_by_number(fi.number)
            {
                if info.name != fi.name {
                    remapper = merge_state.merge_field_infos.as_ref().map(Arc::clone);
                    break;
                }
            } else {
                remapper = merge_state.merge_field_infos.as_ref().map(Arc::clone);
                break;
            }
        }
        MergeVisitor {
            value: None,
            current_field: ptr::null(),
            remapper,
            fields_writer,
        }
    }

    fn reset(&mut self, field: &FieldInfo) {
        if let Some(ref remapper) = self.remapper {
            self.current_field = remapper.field_info_by_name(&field.name).unwrap();
        } else {
            self.current_field = field;
        }
        self.value = None;
    }

    fn write(&mut self) -> Result<()> {
        unsafe { (*self.fields_writer).write_field(&*self.current_field, self) }
    }
}

impl<S: StoredFieldsWriter> StoredFieldVisitor for MergeVisitor<S> {
    fn add_binary_field(&mut self, field_info: &FieldInfo, value: Vec<u8>) -> Result<()> {
        self.reset(field_info);
        self.value = Some(VariantValue::Binary(value));
        self.write()
    }

    fn add_string_field(&mut self, field_info: &FieldInfo, value: Vec<u8>) -> Result<()> {
        self.reset(field_info);
        self.value = Some(VariantValue::VString(String::from_utf8(value)?));
        self.write()
    }

    fn add_int_field(&mut self, field_info: &FieldInfo, value: i32) -> Result<()> {
        self.reset(field_info);
        self.value = Some(VariantValue::Int(value));
        self.write()
    }

    fn add_long_field(&mut self, field_info: &FieldInfo, value: i64) -> Result<()> {
        self.reset(field_info);
        self.value = Some(VariantValue::Long(value));
        self.write()
    }

    fn add_float_field(&mut self, field_info: &FieldInfo, value: f32) -> Result<()> {
        self.reset(field_info);
        self.value = Some(VariantValue::Float(value));
        self.write()
    }

    fn add_double_field(&mut self, field_info: &FieldInfo, value: f64) -> Result<()> {
        self.reset(field_info);
        self.value = Some(VariantValue::Double(value));
        self.write()
    }

    fn needs_field(&self, _field_info: &FieldInfo) -> Status {
        Status::Yes
    }
}

impl<S: StoredFieldsWriter> Fieldable for MergeVisitor<S> {
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
        debug_assert!(self.value.is_some());
        self.value.as_ref()
    }
    fn token_stream(&mut self) -> Result<Box<dyn TokenStream>> {
        unreachable!()
    }

    fn binary_value(&self) -> Option<&[u8]> {
        if let Some(VariantValue::Binary(ref b)) = self.value {
            Some(b.as_ref())
        } else {
            None
        }
    }

    // fn binary_value(&self) -> Option<&[u8]>;
    fn string_value(&self) -> Option<&str> {
        if let Some(VariantValue::VString(s)) = &self.value {
            Some(&s)
        } else {
            None
        }
    }

    fn numeric_value(&self) -> Option<Numeric> {
        self.value.as_ref().and_then(|v| v.get_numeric())
    }
}
