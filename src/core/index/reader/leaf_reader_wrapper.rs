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

use core::codec::doc_values::lucene54::DocValuesTermIterator;
use core::codec::doc_values::*;
use core::codec::field_infos::{FieldInfo, FieldInfos};
use core::codec::norms::NormsProducer;
use core::codec::points::{IntersectVisitor, PointValues, Relation};
use core::codec::postings::FieldsProducer;
use core::codec::stored_fields::StoredFieldsReader;
use core::codec::term_vectors::TermVectorsReader;
use core::codec::*;
use core::codec::{Fields, SeekStatus, TermIterator, Terms};
use core::codec::{PackedLongDocMap, SorterDocMap};
use core::codec::{PostingIterator, PostingIteratorFlags};
use core::doc::{IndexOptions, StoredFieldVisitor};
use core::index::reader::{LeafReader, SegmentReader};
use core::search::sort_field::Sort;
use core::search::{DocIterator, Payload, NO_MORE_DOCS};
use core::store::directory::Directory;
use core::store::io::{DataInput, IndexInput, IndexOutput, RAMOutputStream};
use core::util::external::Deferred;
use core::util::fst::{BytesStore, StoreBytesReader};
use core::util::{Bits, BitsMut, BitsRef, DocId};

use error::{ErrorKind::IllegalArgument, Result};

use core::util::FixedBitSet;
use std::any::Any;
use std::io::Read;
use std::mem;
use std::ops::DerefMut;
use std::sync::Arc;

/// This is a hack to make index sorting fast, with a `LeafReader` that
/// always returns merge instances when you ask for the codec readers.
pub struct MergeReaderWrapper<D: Directory + 'static, C: Codec> {
    reader: Arc<SegmentReader<D, C>>,
    fields: CodecFieldsProducer<C>,
    norms: Option<Arc<CodecNormsProducer<C>>>,
    doc_values: Option<Box<dyn DocValuesProducer>>,
    store: Arc<CodecStoredFieldsReader<C>>,
    vectors: Option<Arc<CodecTVReader<C>>>,
}

impl<D: Directory + 'static, C: Codec> MergeReaderWrapper<D, C> {
    pub fn new(reader: Arc<SegmentReader<D, C>>) -> Result<MergeReaderWrapper<D, C>> {
        let fields = reader.postings_reader()?;
        let norms = reader.norms_reader()?;
        let doc_values = match reader.doc_values_reader()? {
            Some(p) => Some(p.get_merge_instance()?),
            None => None,
        };
        let store = reader.store_fields_reader()?.get_merge_instance()?;
        let vectors = reader.term_vectors_reader()?;

        Ok(MergeReaderWrapper {
            reader,
            fields,
            norms,
            doc_values,
            store,
            vectors,
        })
    }

    fn check_bounds(&self, doc_id: DocId) -> Result<()> {
        if doc_id < 0 || doc_id > self.max_doc() {
            bail!(IllegalArgument(format!(
                "doc_id must be >= 0 and < max_doc={}, got {}",
                self.max_doc(),
                doc_id
            )));
        }
        Ok(())
    }
}

impl<D: Directory + 'static, C: Codec> LeafReader for MergeReaderWrapper<D, C> {
    type Codec = C;
    type FieldsProducer = CodecFieldsProducer<C>;
    type TVFields = CodecTVFields<C>;
    type TVReader = Arc<CodecTVReader<C>>;
    type StoredReader = Arc<CodecStoredFieldsReader<C>>;
    type NormsReader = Arc<CodecNormsProducer<C>>;
    type PointsReader = Arc<CodecPointsReader<C>>;

    fn codec(&self) -> &Self::Codec {
        self.reader.codec()
    }

    fn fields(&self) -> Result<CodecFieldsProducer<C>> {
        Ok(self.fields.clone())
    }

    fn name(&self) -> &str {
        self.reader.name()
    }

    fn term_vector(&self, doc_id: i32) -> Result<Option<CodecTVFields<C>>> {
        self.check_bounds(doc_id)?;
        if let Some(ref vectors) = self.vectors {
            vectors.get(doc_id)
        } else {
            Ok(None)
        }
    }

    fn document(&self, doc_id: i32, visitor: &mut dyn StoredFieldVisitor) -> Result<()> {
        self.check_bounds(doc_id)?;
        self.store.visit_document(doc_id, visitor)
    }

    fn live_docs(&self) -> BitsRef {
        self.reader.live_docs()
    }

    fn field_info(&self, field: &str) -> Option<&FieldInfo> {
        self.reader.field_info(field)
    }

    fn field_infos(&self) -> &FieldInfos {
        self.reader.field_infos()
    }

    fn clone_field_infos(&self) -> Arc<FieldInfos> {
        self.reader.clone_field_infos()
    }

    fn max_doc(&self) -> i32 {
        self.reader.max_doc()
    }

    fn num_docs(&self) -> i32 {
        self.reader.num_docs()
    }

    fn get_numeric_doc_values(&self, field: &str) -> Result<Box<dyn NumericDocValues>> {
        if let Some(field_info) = self.field_info(field) {
            self.doc_values
                .as_ref()
                .unwrap()
                .get_numeric(field_info)?
                .get()
        } else {
            bail!(IllegalArgument(format!("field '{}' not exist!", field)))
        }
    }

    fn get_binary_doc_values(&self, field: &str) -> Result<Box<dyn BinaryDocValues>> {
        if let Some(field_info) = self.field_info(field) {
            self.doc_values
                .as_ref()
                .unwrap()
                .get_binary(field_info)?
                .get()
        } else {
            bail!(IllegalArgument(format!("field '{}' not exist!", field)))
        }
    }

    fn get_sorted_doc_values(&self, field: &str) -> Result<Box<dyn SortedDocValues>> {
        if let Some(field_info) = self.field_info(field) {
            self.doc_values
                .as_ref()
                .unwrap()
                .get_sorted(field_info)?
                .get()
        } else {
            bail!(IllegalArgument(format!("field '{}' not exist!", field)))
        }
    }

    fn get_sorted_numeric_doc_values(
        &self,
        field: &str,
    ) -> Result<Box<dyn SortedNumericDocValues>> {
        if let Some(field_info) = self.field_info(field) {
            self.doc_values
                .as_ref()
                .unwrap()
                .get_sorted_numeric(field_info)?
                .get()
        } else {
            bail!(IllegalArgument(format!("field '{}' not exist!", field)))
        }
    }

    fn get_sorted_set_doc_values(&self, field: &str) -> Result<Box<dyn SortedSetDocValues>> {
        if let Some(field_info) = self.field_info(field) {
            self.doc_values
                .as_ref()
                .unwrap()
                .get_sorted_set(field_info)?
                .get()
        } else {
            bail!(IllegalArgument(format!("field '{}' not exist!", field)))
        }
    }

    fn norm_values(&self, field: &str) -> Result<Option<Box<dyn NumericDocValues>>> {
        if let Some(ref norms) = self.norms {
            if let Some(field_info) = self.field_info(field) {
                return Ok(Some(norms.norms(field_info)?));
            }
        }
        Ok(None)
    }

    fn get_docs_with_field(&self, field: &str) -> Result<Box<dyn BitsMut>> {
        if let Some(field_info) = self.field_info(field) {
            self.doc_values
                .as_ref()
                .unwrap()
                .get_docs_with_field(field_info)
        } else {
            bail!(IllegalArgument(format!("field '{}' not exist!", field)))
        }
    }

    fn point_values(&self) -> Option<Self::PointsReader> {
        self.reader.point_values()
    }

    fn core_cache_key(&self) -> &str {
        self.reader.core_cache_key()
    }

    fn index_sort(&self) -> Option<&Sort> {
        self.reader.index_sort()
    }

    fn add_core_drop_listener(&self, listener: Deferred) {
        self.reader.add_core_drop_listener(listener)
    }

    fn is_codec_reader(&self) -> bool {
        false
    }

    fn store_fields_reader(&self) -> Result<Self::StoredReader> {
        unreachable!()
    }

    fn term_vectors_reader(&self) -> Result<Option<Self::TVReader>> {
        unreachable!()
    }

    fn norms_reader(&self) -> Result<Option<Self::NormsReader>> {
        unreachable!()
    }

    fn doc_values_reader(&self) -> Result<Option<Arc<dyn DocValuesProducer>>> {
        unreachable!()
    }

    fn postings_reader(&self) -> Result<CodecFieldsProducer<C>> {
        unreachable!()
    }
}

/// A `LeafReader` which supports sorting documents by a given `Sort`. This
/// is package private and is only used by Rucene whne it needs to merge a
/// newly fluhsed (unsorted) segment.
pub struct SortingLeafReader<T: LeafReader> {
    doc_map: Arc<PackedLongDocMap>,
    reader: T,
}

impl<T: LeafReader> SortingLeafReader<T> {
    pub fn new(reader: T, doc_map: Arc<PackedLongDocMap>) -> Self {
        debug_assert_eq!(reader.max_doc(), doc_map.len() as i32);
        SortingLeafReader { reader, doc_map }
    }
}

impl<T: LeafReader + 'static> LeafReader for SortingLeafReader<T> {
    type Codec = T::Codec;
    type FieldsProducer = Arc<SortingFields<T::FieldsProducer>>;
    type TVFields = T::TVFields;
    type TVReader = T::TVReader;
    type StoredReader = T::StoredReader;
    type NormsReader = T::NormsReader;
    type PointsReader = SortingPointValues<T::PointsReader>;

    fn codec(&self) -> &Self::Codec {
        self.reader.codec()
    }

    fn fields(&self) -> Result<Self::FieldsProducer> {
        Ok(Arc::new(SortingFields::new(
            self.reader.fields()?,
            self.reader.clone_field_infos(),
            Arc::clone(&self.doc_map),
        )))
    }

    fn name(&self) -> &str {
        self.reader.name()
    }

    fn term_vector(&self, doc_id: DocId) -> Result<Option<Self::TVFields>> {
        self.reader.term_vector(self.doc_map.new_to_old(doc_id))
    }

    fn document(&self, doc_id: DocId, visitor: &mut dyn StoredFieldVisitor) -> Result<()> {
        self.reader
            .document(self.doc_map.new_to_old(doc_id), visitor)
    }

    fn live_docs(&self) -> BitsRef {
        Arc::new(SortingBits::new(
            self.reader.live_docs(),
            Arc::clone(&self.doc_map),
        ))
    }

    fn field_info(&self, field: &str) -> Option<&FieldInfo> {
        self.reader.field_info(field)
    }

    fn field_infos(&self) -> &FieldInfos {
        self.reader.field_infos()
    }

    fn clone_field_infos(&self) -> Arc<FieldInfos> {
        self.reader.clone_field_infos()
    }

    fn max_doc(&self) -> DocId {
        self.reader.max_doc()
    }

    fn num_docs(&self) -> i32 {
        self.reader.num_docs()
    }

    fn get_numeric_doc_values(&self, field: &str) -> Result<Box<dyn NumericDocValues>> {
        Ok(Box::new(SortingNumericDocValues::new(
            self.reader.get_numeric_doc_values(field)?,
            Arc::clone(&self.doc_map),
        )))
    }

    fn get_binary_doc_values(&self, field: &str) -> Result<Box<dyn BinaryDocValues>> {
        Ok(Box::new(SortingBinaryDocValues::new(
            self.reader.get_binary_doc_values(field)?,
            Arc::clone(&self.doc_map),
        )))
    }

    fn get_sorted_doc_values(&self, field: &str) -> Result<Box<dyn SortedDocValues>> {
        Ok(Box::new(SortingSortedDocValues::new(
            self.reader.get_sorted_doc_values(field)?,
            Arc::clone(&self.doc_map),
        )))
    }

    fn get_sorted_numeric_doc_values(
        &self,
        field: &str,
    ) -> Result<Box<dyn SortedNumericDocValues>> {
        Ok(Box::new(SortingSortedNumericDocValues::new(
            self.reader.get_sorted_numeric_doc_values(field)?,
            Arc::clone(&self.doc_map),
        )))
    }

    fn get_sorted_set_doc_values(&self, field: &str) -> Result<Box<dyn SortedSetDocValues>> {
        Ok(Box::new(SortingSortedSetDocValues::new(
            self.reader.get_sorted_set_doc_values(field)?,
            Arc::clone(&self.doc_map),
        )))
    }

    fn norm_values(&self, field: &str) -> Result<Option<Box<dyn NumericDocValues>>> {
        match self.reader.norm_values(field)? {
            Some(n) => Ok(Some(Box::new(SortingNumericDocValues::new(
                n,
                Arc::clone(&self.doc_map),
            )))),
            None => Ok(None),
        }
    }

    fn get_docs_with_field(&self, field: &str) -> Result<Box<dyn BitsMut>> {
        Ok(Box::new(SortingBitsMut::new(
            self.reader.get_docs_with_field(field)?,
            Arc::clone(&self.doc_map),
        )))
    }

    /// Returns the `PointValues` used for numeric or
    /// spatial searches, or None if there are no point fields.
    fn point_values(&self) -> Option<Self::PointsReader> {
        match self.reader.point_values() {
            Some(p) => Some(SortingPointValues::new(p, Arc::clone(&self.doc_map))),
            None => None,
        }
    }

    /// Expert: Returns a key for this IndexReader, so CachingWrapperFilter can find
    // it again.
    // This key must not have equals()/hashCode() methods, so &quot;equals&quot; means
    // &quot;identical&quot;.
    fn core_cache_key(&self) -> &str {
        self.reader.core_cache_key()
    }

    /// Returns null if this leaf is unsorted, or the `Sort` that it was sorted by
    fn index_sort(&self) -> Option<&Sort> {
        self.reader.index_sort()
    }

    fn add_core_drop_listener(&self, listener: Deferred) {
        self.reader.add_core_drop_listener(listener)
    }

    fn is_codec_reader(&self) -> bool {
        false
    }

    // following methods are from `CodecReader`
    fn store_fields_reader(&self) -> Result<Self::StoredReader> {
        unreachable!()
    }

    fn term_vectors_reader(&self) -> Result<Option<Self::TVReader>> {
        unreachable!()
    }

    fn norms_reader(&self) -> Result<Option<Self::NormsReader>> {
        unreachable!()
    }

    fn doc_values_reader(&self) -> Result<Option<Arc<dyn DocValuesProducer>>> {
        unreachable!()
    }

    fn postings_reader(&self) -> Result<Self::FieldsProducer> {
        unreachable!()
    }
}

pub struct CachedBinaryDVs {
    pub values: Vec<Vec<u8>>,
    pub docs_with_field: FixedBitSet,
}

impl CachedBinaryDVs {
    pub fn new(values: Vec<Vec<u8>>, docs_with_field: FixedBitSet) -> Self {
        Self {
            values,
            docs_with_field,
        }
    }
}

pub struct CachedNumericDVs {
    pub values: Vec<i64>,
    pub docs_with_field: FixedBitSet,
}

impl CachedNumericDVs {
    pub fn new(values: Vec<i64>, docs_with_field: FixedBitSet) -> Self {
        Self {
            values,
            docs_with_field,
        }
    }
}

pub struct SortingFields<T: Fields> {
    fields: T,
    doc_map: Arc<PackedLongDocMap>,
    infos: Arc<FieldInfos>,
}

impl<T: Fields> SortingFields<T> {
    pub fn new(fields: T, infos: Arc<FieldInfos>, doc_map: Arc<PackedLongDocMap>) -> Self {
        SortingFields {
            fields,
            doc_map,
            infos,
        }
    }
}

impl<T: Fields> Fields for SortingFields<T> {
    type Terms = SortingTerms<T::Terms>;
    fn fields(&self) -> Vec<String> {
        self.fields.fields()
    }

    fn terms(&self, field: &str) -> Result<Option<Self::Terms>> {
        match self.fields.terms(field)? {
            Some(terms) => Ok(Some(SortingTerms::new(
                terms,
                self.infos.field_info_by_name(field).unwrap().index_options,
                Arc::clone(&self.doc_map),
            ))),
            None => Ok(None),
        }
    }

    fn size(&self) -> usize {
        self.fields.size()
    }
}

impl<T: FieldsProducer> FieldsProducer for SortingFields<T> {
    fn check_integrity(&self) -> Result<()> {
        Ok(())
    }
}

pub struct SortingTerms<T: Terms> {
    terms: T,
    doc_map: Arc<PackedLongDocMap>,
    index_options: IndexOptions,
}

impl<T: Terms> SortingTerms<T> {
    fn new(terms: T, index_options: IndexOptions, doc_map: Arc<PackedLongDocMap>) -> Self {
        SortingTerms {
            terms,
            index_options,
            doc_map,
        }
    }
}

impl<T: Terms> Terms for SortingTerms<T> {
    type Iterator = SortingTermsIterator<T::Iterator>;
    fn iterator(&self) -> Result<Self::Iterator> {
        Ok(SortingTermsIterator::new(
            self.terms.iterator()?,
            Arc::clone(&self.doc_map),
            self.index_options,
            self.has_positions()?,
        ))
    }

    fn size(&self) -> Result<i64> {
        self.terms.size()
    }

    fn sum_total_term_freq(&self) -> Result<i64> {
        self.terms.sum_total_term_freq()
    }

    fn sum_doc_freq(&self) -> Result<i64> {
        self.terms.sum_doc_freq()
    }

    fn doc_count(&self) -> Result<i32> {
        self.terms.doc_count()
    }

    fn has_freqs(&self) -> Result<bool> {
        self.terms.has_freqs()
    }

    fn has_offsets(&self) -> Result<bool> {
        self.terms.has_offsets()
    }

    fn has_positions(&self) -> Result<bool> {
        self.terms.has_positions()
    }

    fn has_payloads(&self) -> Result<bool> {
        self.terms.has_payloads()
    }
}

pub struct SortingTermsIterator<T: TermIterator> {
    iter: T,
    doc_map: Arc<PackedLongDocMap>,
    index_options: IndexOptions,
    has_positions: bool,
}

impl<T: TermIterator> SortingTermsIterator<T> {
    fn new(
        iter: T,
        doc_map: Arc<PackedLongDocMap>,
        index_options: IndexOptions,
        has_positions: bool,
    ) -> Self {
        SortingTermsIterator {
            iter,
            doc_map,
            index_options,
            has_positions,
        }
    }
}

impl<T: TermIterator> TermIterator for SortingTermsIterator<T> {
    type Postings = SortingPostingIterEnum<T::Postings>;
    type TermState = T::TermState;
    fn next(&mut self) -> Result<Option<Vec<u8>>> {
        self.iter.next()
    }

    fn seek_ceil(&mut self, text: &[u8]) -> Result<SeekStatus> {
        self.iter.seek_ceil(text)
    }

    fn seek_exact_ord(&mut self, ord: i64) -> Result<()> {
        self.iter.seek_exact_ord(ord)
    }

    fn term(&self) -> Result<&[u8]> {
        self.iter.term()
    }

    fn ord(&self) -> Result<i64> {
        self.iter.ord()
    }

    fn doc_freq(&mut self) -> Result<i32> {
        self.iter.doc_freq()
    }

    fn total_term_freq(&mut self) -> Result<i64> {
        self.iter.total_term_freq()
    }

    fn postings_with_flags(&mut self, flags: u16) -> Result<Self::Postings> {
        if self.has_positions
            && PostingIteratorFlags::feature_requested(flags, PostingIteratorFlags::POSITIONS)
        {
            let docs_and_positions = self.iter.postings_with_flags(flags)?;
            // we ignore the fact that offsets may be stored but not asked for,
            // since this code is expected to be used during addIndexes which will
            // ask for everything. if that assumption changes in the future, we can
            // factor in whether 'flags' says offsets are not required.
            let store_offsets =
                self.index_options >= IndexOptions::DocsAndFreqsAndPositionsAndOffsets;
            return Ok(SortingPostingIterEnum::Posting(
                SortingPostingsIterator::new(
                    self.doc_map.len() as i32,
                    docs_and_positions,
                    self.doc_map.as_ref(),
                    store_offsets,
                )?,
            ));
        }

        let docs = self.iter.postings_with_flags(flags)?;
        let with_freqs = self.index_options >= IndexOptions::DocsAndFreqs
            && PostingIteratorFlags::feature_requested(flags, PostingIteratorFlags::FREQS);
        Ok(SortingPostingIterEnum::Doc(SortingDocsIterator::new(
            self.doc_map.len() as i32,
            docs,
            with_freqs,
            self.doc_map.as_ref(),
        )?))
    }
}

pub struct SortingPostingsIterator<T: PostingIterator> {
    postings: T,
    _max_doc: i32,
    docs_and_offsets: Vec<DocAndOffset>,
    upto: i32,
    posting_input: StoreBytesReader,
    store_offsets: bool,
    doc_it: i32,
    pos: i32,
    start_offset: i32,
    end_offset: i32,
    payload: Vec<u8>,
    curr_freq: i32,
    // file: BytesStore,
}

struct DocAndOffset {
    doc: DocId,
    offset: i64,
}

impl<T: PostingIterator> SortingPostingsIterator<T> {
    fn new(
        max_doc: i32,
        mut postings: T,
        doc_map: &PackedLongDocMap,
        store_offsets: bool,
    ) -> Result<Self> {
        let mut docs_and_offsets = Vec::with_capacity(32);
        let mut file = BytesStore::with_block_bits(10);
        let upto: i32;
        file = {
            let mut output = RAMOutputStream::from_store(file);
            let mut i = 0;
            loop {
                let doc = postings.next()?;
                if doc == NO_MORE_DOCS {
                    break;
                }
                let doc_offset = DocAndOffset {
                    doc: doc_map.old_to_new(doc),
                    offset: output.file_pointer(),
                };
                if i == docs_and_offsets.len() {
                    docs_and_offsets.push(doc_offset);
                } else {
                    docs_and_offsets[i] = doc_offset;
                }
                i += 1;
                Self::add_positions(&mut postings, &mut output, store_offsets)?;
            }
            upto = i as i32;
            mem::replace(&mut output.store, BytesStore::with_block_bits(1))
        };
        docs_and_offsets.sort_by(|d1, d2| d1.doc.cmp(&d2.doc));

        let posting_input = StoreBytesReader::from_bytes_store(file, false);

        Ok(SortingPostingsIterator {
            postings,
            _max_doc: max_doc,
            docs_and_offsets,
            upto,
            posting_input,
            store_offsets,
            doc_it: -1,
            pos: 0,
            start_offset: -1,
            end_offset: -1,
            payload: Vec::with_capacity(0),
            curr_freq: 0,
        })
    }

    fn add_positions(
        input: &mut dyn PostingIterator,
        output: &mut impl IndexOutput,
        store_offsets: bool,
    ) -> Result<()> {
        let freq = input.freq()?;
        output.write_vint(freq)?;

        let mut prev_position = 0;
        let mut prev_end_offset = 0;
        for _i in 0..freq {
            let pos = input.next_position()?;
            let payload = input.payload()?;
            // The low-order bit of token is set only if there is a payload, the
            // previous bits are the delta-encoded position.
            let token = (pos - prev_position) << 1 | if payload.is_empty() { 0 } else { 1 };
            output.write_vint(token)?;
            prev_position = pos;
            if store_offsets {
                // don't encode offsets if the are not stored
                let start_offset = input.start_offset()?;
                let end_offset = input.end_offset()?;
                output.write_vint(start_offset - prev_end_offset)?;
                output.write_vint(end_offset - start_offset)?;
                prev_end_offset = end_offset;
            }
            if !payload.is_empty() {
                output.write_vint(payload.len() as i32)?;
                output.write_bytes(&payload, 0, payload.len())?;
            }
        }
        Ok(())
    }
}

impl<T: PostingIterator> DocIterator for SortingPostingsIterator<T> {
    fn doc_id(&self) -> i32 {
        if self.doc_it < 0 {
            -1
        } else if self.doc_it >= self.upto {
            NO_MORE_DOCS
        } else {
            self.docs_and_offsets[self.doc_it as usize].doc
        }
    }

    fn next(&mut self) -> Result<i32> {
        self.doc_it += 1;
        if self.doc_it >= self.upto {
            Ok(NO_MORE_DOCS)
        } else {
            self.posting_input
                .seek(self.docs_and_offsets[self.doc_it as usize].offset)?;
            self.curr_freq = self.posting_input.read_vint()?;
            // reset variables used in next_position
            self.pos = 0;
            self.end_offset = 0;
            Ok(self.docs_and_offsets[self.doc_it as usize].doc)
        }
    }

    fn advance(&mut self, target: i32) -> Result<i32> {
        // need to support it for checkIndex, but in practice it won't be called, so
        // don't bother to implement efficiently for now.
        self.slow_advance(target)
    }

    fn cost(&self) -> usize {
        self.postings.cost()
    }
}

impl<T: PostingIterator> PostingIterator for SortingPostingsIterator<T> {
    fn freq(&self) -> Result<i32> {
        Ok(self.curr_freq)
    }

    fn next_position(&mut self) -> Result<i32> {
        let token = self.posting_input.read_vint()?;
        debug_assert!(token >= 0);
        self.pos += token >> 1;
        if self.store_offsets {
            self.start_offset = self.end_offset + self.posting_input.read_vint()?;
            self.end_offset = self.start_offset + self.posting_input.read_vint()?;
        }
        if token & 1 != 0 {
            let payload_len = self.posting_input.read_vint()? as usize;
            self.payload.resize(payload_len, 0);
            self.posting_input.read_exact(&mut self.payload)?;
        } else {
            self.payload.clear();
        }
        Ok(self.pos)
    }

    fn start_offset(&self) -> Result<i32> {
        Ok(self.start_offset)
    }

    fn end_offset(&self) -> Result<i32> {
        Ok(self.end_offset)
    }

    fn payload(&self) -> Result<Vec<u8>> {
        Ok(self.payload.clone())
    }
}

struct DocAndFreq {
    doc: DocId,
    freq: i32,
}

pub struct SortingDocsIterator<T: PostingIterator> {
    posting: T,
    _max_doc: i32,
    docs_and_freqs: Vec<DocAndFreq>,
    doc_it: DocId,
    upto: i32,
    with_freqs: bool,
}

impl<T: PostingIterator> SortingDocsIterator<T> {
    fn new(
        max_doc: i32,
        mut posting: T,
        with_freqs: bool,
        doc_map: &PackedLongDocMap,
    ) -> Result<Self> {
        let mut i = 0;
        let mut docs_and_freqs = Vec::with_capacity(64);
        loop {
            let doc = posting.next()?;
            if doc == NO_MORE_DOCS {
                break;
            }
            let new_doc = doc_map.old_to_new(doc);
            let freq = if with_freqs { posting.freq()? } else { 0 };
            docs_and_freqs.push(DocAndFreq { doc: new_doc, freq });
            i += 1;
        }
        // TODO: TimSort can save much time compared to other sorts in case of
        // reverse sorting, or when sorting a concatenation of sorted readers
        docs_and_freqs.sort_by(|d1, d2| d1.doc.cmp(&d2.doc));
        let upto = i;
        Ok(SortingDocsIterator {
            _max_doc: max_doc,
            posting,
            docs_and_freqs,
            doc_it: -1,
            upto,
            with_freqs,
        })
    }
}

impl<T: PostingIterator> PostingIterator for SortingDocsIterator<T> {
    fn freq(&self) -> Result<i32> {
        Ok(if self.with_freqs && self.doc_it < self.upto {
            self.docs_and_freqs[self.doc_it as usize].freq
        } else {
            -1
        })
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

    fn payload(&self) -> Result<Vec<u8>> {
        Ok(Vec::with_capacity(0))
    }
}

impl<T: PostingIterator> DocIterator for SortingDocsIterator<T> {
    fn doc_id(&self) -> i32 {
        if self.doc_it < 0 {
            -1
        } else if self.doc_it >= self.upto {
            NO_MORE_DOCS
        } else {
            self.docs_and_freqs[self.doc_it as usize].doc
        }
    }

    fn next(&mut self) -> Result<i32> {
        self.doc_it += 1;
        if self.doc_it >= self.upto {
            Ok(NO_MORE_DOCS)
        } else {
            Ok(self.docs_and_freqs[self.doc_it as usize].doc)
        }
    }

    fn advance(&mut self, target: i32) -> Result<i32> {
        // need to support it for checkIndex, but in practice it won't be called, so
        // don't bother to implement efficiently for now.
        self.slow_advance(target)
    }

    fn cost(&self) -> usize {
        self.posting.cost()
    }
}

pub enum SortingPostingIterEnum<T: PostingIterator> {
    Posting(SortingPostingsIterator<T>),
    Doc(SortingDocsIterator<T>),
    Raw(T),
}

impl<T: PostingIterator> PostingIterator for SortingPostingIterEnum<T> {
    fn freq(&self) -> Result<i32> {
        match self {
            SortingPostingIterEnum::Posting(i) => i.freq(),
            SortingPostingIterEnum::Doc(i) => i.freq(),
            SortingPostingIterEnum::Raw(i) => i.freq(),
        }
    }

    fn next_position(&mut self) -> Result<i32> {
        match self {
            SortingPostingIterEnum::Posting(i) => i.next_position(),
            SortingPostingIterEnum::Doc(i) => i.next_position(),
            SortingPostingIterEnum::Raw(i) => i.next_position(),
        }
    }

    fn start_offset(&self) -> Result<i32> {
        match self {
            SortingPostingIterEnum::Posting(i) => i.start_offset(),
            SortingPostingIterEnum::Doc(i) => i.start_offset(),
            SortingPostingIterEnum::Raw(i) => i.start_offset(),
        }
    }

    fn end_offset(&self) -> Result<i32> {
        match self {
            SortingPostingIterEnum::Posting(i) => i.end_offset(),
            SortingPostingIterEnum::Doc(i) => i.end_offset(),
            SortingPostingIterEnum::Raw(i) => i.end_offset(),
        }
    }

    fn payload(&self) -> Result<Payload> {
        match self {
            SortingPostingIterEnum::Posting(i) => i.payload(),
            SortingPostingIterEnum::Doc(i) => i.payload(),
            SortingPostingIterEnum::Raw(i) => i.payload(),
        }
    }
}

impl<T: PostingIterator> DocIterator for SortingPostingIterEnum<T> {
    fn doc_id(&self) -> DocId {
        match self {
            SortingPostingIterEnum::Posting(i) => i.doc_id(),
            SortingPostingIterEnum::Doc(i) => i.doc_id(),
            SortingPostingIterEnum::Raw(i) => i.doc_id(),
        }
    }

    fn next(&mut self) -> Result<DocId> {
        match self {
            SortingPostingIterEnum::Posting(i) => i.next(),
            SortingPostingIterEnum::Doc(i) => i.next(),
            SortingPostingIterEnum::Raw(i) => i.next(),
        }
    }

    fn advance(&mut self, target: i32) -> Result<DocId> {
        match self {
            SortingPostingIterEnum::Posting(i) => i.advance(target),
            SortingPostingIterEnum::Doc(i) => i.advance(target),
            SortingPostingIterEnum::Raw(i) => i.advance(target),
        }
    }

    fn slow_advance(&mut self, target: i32) -> Result<DocId> {
        match self {
            SortingPostingIterEnum::Posting(i) => i.slow_advance(target),
            SortingPostingIterEnum::Doc(i) => i.slow_advance(target),
            SortingPostingIterEnum::Raw(i) => i.slow_advance(target),
        }
    }

    fn cost(&self) -> usize {
        match self {
            SortingPostingIterEnum::Posting(i) => i.cost(),
            SortingPostingIterEnum::Doc(i) => i.cost(),
            SortingPostingIterEnum::Raw(i) => i.cost(),
        }
    }

    fn matches(&mut self) -> Result<bool> {
        match self {
            SortingPostingIterEnum::Posting(i) => i.matches(),
            SortingPostingIterEnum::Doc(i) => i.matches(),
            SortingPostingIterEnum::Raw(i) => i.matches(),
        }
    }

    fn match_cost(&self) -> f32 {
        match self {
            SortingPostingIterEnum::Posting(i) => i.match_cost(),
            SortingPostingIterEnum::Doc(i) => i.match_cost(),
            SortingPostingIterEnum::Raw(i) => i.match_cost(),
        }
    }

    fn approximate_next(&mut self) -> Result<DocId> {
        match self {
            SortingPostingIterEnum::Posting(i) => i.approximate_next(),
            SortingPostingIterEnum::Doc(i) => i.approximate_next(),
            SortingPostingIterEnum::Raw(i) => i.approximate_next(),
        }
    }

    fn approximate_advance(&mut self, target: i32) -> Result<DocId> {
        match self {
            SortingPostingIterEnum::Posting(i) => i.approximate_advance(target),
            SortingPostingIterEnum::Doc(i) => i.approximate_advance(target),
            SortingPostingIterEnum::Raw(i) => i.approximate_advance(target),
        }
    }
}

pub struct SortingBinaryDocValues {
    doc_values: Box<dyn BinaryDocValues>,
    doc_map: Arc<PackedLongDocMap>,
}

impl SortingBinaryDocValues {
    fn new(doc_values: Box<dyn BinaryDocValues>, doc_map: Arc<PackedLongDocMap>) -> Self {
        SortingBinaryDocValues {
            doc_map,
            doc_values,
        }
    }
}

impl BinaryDocValues for SortingBinaryDocValues {
    fn get(&mut self, doc_id: i32) -> Result<Vec<u8>> {
        self.doc_values.get(self.doc_map.new_to_old(doc_id))
    }
}

pub struct SortingNumericDocValues<T> {
    doc_values: T,
    doc_map: Arc<PackedLongDocMap>,
}

impl<T> SortingNumericDocValues<T> {
    fn new(doc_values: T, doc_map: Arc<PackedLongDocMap>) -> Self {
        SortingNumericDocValues {
            doc_map,
            doc_values,
        }
    }
}

impl<T: DerefMut<Target = dyn NumericDocValues> + Send + Sync> NumericDocValues
    for SortingNumericDocValues<T>
{
    fn get(&self, doc_id: DocId) -> Result<i64> {
        (*self.doc_values).get(self.doc_map.new_to_old(doc_id))
    }

    fn get_mut(&mut self, doc_id: DocId) -> Result<i64> {
        (*self.doc_values).get_mut(self.doc_map.new_to_old(doc_id))
    }
}

struct SortingSortedNumericDocValues {
    doc_values: Box<dyn SortedNumericDocValues>,
    doc_map: Arc<PackedLongDocMap>,
}

impl SortingSortedNumericDocValues {
    fn new(doc_values: Box<dyn SortedNumericDocValues>, doc_map: Arc<PackedLongDocMap>) -> Self {
        SortingSortedNumericDocValues {
            doc_map,
            doc_values,
        }
    }
}

impl SortedNumericDocValues for SortingSortedNumericDocValues {
    fn set_document(&mut self, doc: i32) -> Result<()> {
        let doc_id = self.doc_map.new_to_old(doc);
        self.doc_values.set_document(doc_id)
    }

    fn value_at(&mut self, index: usize) -> Result<i64> {
        self.doc_values.value_at(index)
    }

    fn count(&self) -> usize {
        self.doc_values.count()
    }

    fn get_numeric_doc_values(&self) -> Option<Box<dyn NumericDocValues>> {
        self.doc_values.get_numeric_doc_values()
    }
}

struct SortingBits {
    bits: Arc<dyn Bits>,
    doc_map: Arc<PackedLongDocMap>,
}

impl SortingBits {
    fn new(bits: Arc<dyn Bits>, doc_map: Arc<PackedLongDocMap>) -> Self {
        SortingBits { bits, doc_map }
    }
}

impl Bits for SortingBits {
    fn get(&self, index: usize) -> Result<bool> {
        self.bits
            .get(self.doc_map.new_to_old(index as i32) as usize)
    }

    fn len(&self) -> usize {
        self.bits.len()
    }

    fn is_empty(&self) -> bool {
        self.bits.is_empty()
    }
}

struct SortingBitsMut {
    bits: Box<dyn BitsMut>,
    doc_map: Arc<PackedLongDocMap>,
}

impl SortingBitsMut {
    fn new(bits: Box<dyn BitsMut>, doc_map: Arc<PackedLongDocMap>) -> Self {
        SortingBitsMut { bits, doc_map }
    }
}

impl BitsMut for SortingBitsMut {
    fn get(&mut self, index: usize) -> Result<bool> {
        let idx = self.doc_map.new_to_old(index as i32) as usize;
        self.bits.get(idx)
    }

    fn len(&self) -> usize {
        self.bits.len()
    }
}

#[derive(Clone)]
pub struct SortingPointValues<P: PointValues> {
    point_values: P,
    doc_map: Arc<PackedLongDocMap>,
}

impl<P: PointValues> SortingPointValues<P> {
    fn new(point_values: P, doc_map: Arc<PackedLongDocMap>) -> Self {
        SortingPointValues {
            point_values,
            doc_map,
        }
    }
}

impl<P: PointValues + 'static> PointValues for SortingPointValues<P> {
    fn intersect(&self, field_name: &str, visitor: &mut impl IntersectVisitor) -> Result<()> {
        let mut sort_visitor = SortingIntersectVisitor {
            visitor,
            doc_map: Arc::clone(&self.doc_map),
        };
        self.point_values.intersect(field_name, &mut sort_visitor)
    }

    fn min_packed_value(&self, field_name: &str) -> Result<Vec<u8>> {
        self.point_values.min_packed_value(field_name)
    }

    fn max_packed_value(&self, field_name: &str) -> Result<Vec<u8>> {
        self.point_values.max_packed_value(field_name)
    }

    fn num_dimensions(&self, field_name: &str) -> Result<usize> {
        self.point_values.num_dimensions(field_name)
    }

    fn bytes_per_dimension(&self, field_name: &str) -> Result<usize> {
        self.point_values.bytes_per_dimension(field_name)
    }

    fn size(&self, field_name: &str) -> Result<i64> {
        self.point_values.size(field_name)
    }

    fn doc_count(&self, field_name: &str) -> Result<i32> {
        self.point_values.doc_count(field_name)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

struct SortingIntersectVisitor<'a, IV: IntersectVisitor> {
    visitor: &'a mut IV,
    doc_map: Arc<PackedLongDocMap>,
}

impl<'a, IV: IntersectVisitor> IntersectVisitor for SortingIntersectVisitor<'a, IV> {
    fn visit(&mut self, doc_id: i32) -> Result<()> {
        let new_doc = self.doc_map.old_to_new(doc_id);
        self.visitor.visit(new_doc)
    }

    fn visit_by_packed_value(&mut self, doc_id: i32, packed_value: &[u8]) -> Result<()> {
        let new_doc = self.doc_map.old_to_new(doc_id);
        self.visitor.visit_by_packed_value(new_doc, packed_value)
    }

    fn compare(&self, min_packed_value: &[u8], max_packed_value: &[u8]) -> Relation {
        self.visitor.compare(min_packed_value, max_packed_value)
    }
}

struct SortingSortedDocValues {
    doc_values: Box<dyn SortedDocValues>,
    doc_map: Arc<PackedLongDocMap>,
}

impl SortingSortedDocValues {
    fn new(doc_values: Box<dyn SortedDocValues>, doc_map: Arc<PackedLongDocMap>) -> Self {
        SortingSortedDocValues {
            doc_map,
            doc_values,
        }
    }
}

impl SortedDocValues for SortingSortedDocValues {
    fn get_ord(&mut self, doc_id: i32) -> Result<i32> {
        self.doc_values.get_ord(self.doc_map.new_to_old(doc_id))
    }

    fn lookup_ord(&mut self, ord: i32) -> Result<Vec<u8>> {
        self.doc_values.lookup_ord(ord)
    }

    fn value_count(&self) -> usize {
        self.doc_values.value_count()
    }

    fn term_iterator(&self) -> Result<DocValuesTermIterator> {
        self.doc_values.term_iterator()
    }
}

impl BinaryDocValues for SortingSortedDocValues {
    fn get(&mut self, doc_id: DocId) -> Result<Vec<u8>> {
        self.doc_values.get(self.doc_map.new_to_old(doc_id))
    }
}

struct SortingSortedSetDocValues {
    doc_values: Box<dyn SortedSetDocValues>,
    doc_map: Arc<PackedLongDocMap>,
}

impl SortingSortedSetDocValues {
    fn new(doc_values: Box<dyn SortedSetDocValues>, doc_map: Arc<PackedLongDocMap>) -> Self {
        SortingSortedSetDocValues {
            doc_map,
            doc_values,
        }
    }
}

impl SortedSetDocValues for SortingSortedSetDocValues {
    fn set_document(&mut self, doc: DocId) -> Result<()> {
        self.doc_values.set_document(self.doc_map.new_to_old(doc))
    }

    fn next_ord(&mut self) -> Result<i64> {
        self.doc_values.next_ord()
    }

    fn lookup_ord(&mut self, ord: i64) -> Result<Vec<u8>> {
        self.doc_values.lookup_ord(ord)
    }

    fn get_value_count(&self) -> usize {
        self.doc_values.get_value_count()
    }

    fn lookup_term(&mut self, key: &[u8]) -> Result<i64> {
        self.doc_values.lookup_term(key)
    }

    fn term_iterator(&self) -> Result<DocValuesTermIterator> {
        self.doc_values.term_iterator()
    }
}

/// Wraps arbitrary readers for merging. Note that this can cause slow
/// and memory-intensive merges. Consider using `FilterCodecReader` instead
pub struct SlowCodecReaderWrapper<T: LeafReader> {
    reader: Arc<T>,
}

impl<T: LeafReader> SlowCodecReaderWrapper<T> {
    pub fn new(reader: Arc<T>) -> Self {
        SlowCodecReaderWrapper { reader }
    }
}

impl<T: LeafReader + 'static> LeafReader for SlowCodecReaderWrapper<T> {
    type Codec = T::Codec;
    type FieldsProducer = T::FieldsProducer;
    type TVFields = T::TVFields;
    type TVReader = LeafReaderAsTermVectorsReaderWrapper<T>;
    type StoredReader = LeafReaderAsStoreFieldsReader<T>;
    type NormsReader = LeafReaderAsNormsProducer<T>;
    type PointsReader = T::PointsReader;

    fn codec(&self) -> &Self::Codec {
        self.reader.codec()
    }

    fn fields(&self) -> Result<Self::FieldsProducer> {
        self.reader.fields()
    }

    fn name(&self) -> &str {
        self.reader.name()
    }

    fn term_vector(&self, doc_id: DocId) -> Result<Option<Self::TVFields>> {
        self.reader.term_vector(doc_id)
    }

    fn document(&self, doc_id: DocId, visitor: &mut dyn StoredFieldVisitor) -> Result<()> {
        self.reader.document(doc_id, visitor)
    }

    fn live_docs(&self) -> BitsRef {
        self.reader.live_docs()
    }

    fn field_info(&self, field: &str) -> Option<&FieldInfo> {
        self.reader.field_info(field)
    }

    fn field_infos(&self) -> &FieldInfos {
        self.reader.field_infos()
    }

    fn clone_field_infos(&self) -> Arc<FieldInfos> {
        self.reader.clone_field_infos()
    }

    fn max_doc(&self) -> DocId {
        self.reader.max_doc()
    }

    fn num_docs(&self) -> i32 {
        self.reader.num_docs()
    }

    fn get_numeric_doc_values(&self, field: &str) -> Result<Box<dyn NumericDocValues>> {
        self.reader.get_numeric_doc_values(field)
    }

    fn get_binary_doc_values(&self, field: &str) -> Result<Box<dyn BinaryDocValues>> {
        self.reader.get_binary_doc_values(field)
    }

    fn get_sorted_doc_values(&self, field: &str) -> Result<Box<dyn SortedDocValues>> {
        self.reader.get_sorted_doc_values(field)
    }

    fn get_sorted_numeric_doc_values(
        &self,
        field: &str,
    ) -> Result<Box<dyn SortedNumericDocValues>> {
        self.reader.get_sorted_numeric_doc_values(field)
    }

    fn get_sorted_set_doc_values(&self, field: &str) -> Result<Box<dyn SortedSetDocValues>> {
        self.reader.get_sorted_set_doc_values(field)
    }

    fn norm_values(&self, field: &str) -> Result<Option<Box<dyn NumericDocValues>>> {
        self.reader.norm_values(field)
    }

    fn get_docs_with_field(&self, field: &str) -> Result<Box<dyn BitsMut>> {
        self.reader.get_docs_with_field(field)
    }

    /// Returns the `PointValues` used for numeric or
    /// spatial searches, or None if there are no point fields.
    fn point_values(&self) -> Option<Self::PointsReader> {
        self.reader.point_values()
    }

    /// Expert: Returns a key for this IndexReader, so CachingWrapperFilter can find
    // it again.
    // This key must not have equals()/hashCode() methods, so &quot;equals&quot; means
    // &quot;identical&quot;.
    fn core_cache_key(&self) -> &str {
        self.reader.core_cache_key()
    }

    /// Returns null if this leaf is unsorted, or the `Sort` that it was sorted by
    fn index_sort(&self) -> Option<&Sort> {
        self.reader.index_sort()
    }

    fn add_core_drop_listener(&self, listener: Deferred) {
        self.reader.add_core_drop_listener(listener)
    }

    fn is_codec_reader(&self) -> bool {
        true
    }

    // following methods are from `CodecReader`
    fn store_fields_reader(&self) -> Result<Self::StoredReader> {
        Ok(LeafReaderAsStoreFieldsReader::new(Arc::clone(&self.reader)))
    }

    fn term_vectors_reader(&self) -> Result<Option<Self::TVReader>> {
        Ok(Some(LeafReaderAsTermVectorsReaderWrapper::new(Arc::clone(
            &self.reader,
        ))))
    }

    fn norms_reader(&self) -> Result<Option<Self::NormsReader>> {
        Ok(Some(LeafReaderAsNormsProducer::new(Arc::clone(
            &self.reader,
        ))))
    }

    fn doc_values_reader(&self) -> Result<Option<Arc<dyn DocValuesProducer>>> {
        Ok(Some(Arc::new(LeafReaderAsDocValuesProducer::new(
            Arc::clone(&self.reader),
        ))))
    }

    fn postings_reader(&self) -> Result<Self::FieldsProducer> {
        self.reader.fields()
    }
}

pub struct LeafReaderAsStoreFieldsReader<T: LeafReader> {
    reader: Arc<T>,
}

impl<T: LeafReader> LeafReaderAsStoreFieldsReader<T> {
    fn new(reader: Arc<T>) -> Self {
        Self { reader }
    }
}

impl<T: LeafReader> Clone for LeafReaderAsStoreFieldsReader<T> {
    fn clone(&self) -> Self {
        Self {
            reader: Arc::clone(&self.reader),
        }
    }
}

impl<T: LeafReader + 'static> StoredFieldsReader for LeafReaderAsStoreFieldsReader<T> {
    fn visit_document(&self, doc_id: DocId, visitor: &mut dyn StoredFieldVisitor) -> Result<()> {
        self.reader.document(doc_id, visitor)
    }

    // a mutable version for `Self::visit_document` that maybe more fast
    fn visit_document_mut(
        &mut self,
        doc_id: DocId,
        visitor: &mut dyn StoredFieldVisitor,
    ) -> Result<()> {
        self.visit_document(doc_id, visitor)
    }

    fn get_merge_instance(&self) -> Result<Self> {
        Ok(LeafReaderAsStoreFieldsReader::new(Arc::clone(&self.reader)))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub struct LeafReaderAsTermVectorsReaderWrapper<T: LeafReader> {
    reader: Arc<T>,
}

impl<T: LeafReader> LeafReaderAsTermVectorsReaderWrapper<T> {
    fn new(reader: Arc<T>) -> Self {
        LeafReaderAsTermVectorsReaderWrapper { reader }
    }
}

impl<T: LeafReader + 'static> TermVectorsReader for LeafReaderAsTermVectorsReaderWrapper<T> {
    type Fields = T::TVFields;
    fn get(&self, doc: i32) -> Result<Option<Self::Fields>> {
        self.reader.term_vector(doc)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl<T: LeafReader> Clone for LeafReaderAsTermVectorsReaderWrapper<T> {
    fn clone(&self) -> Self {
        Self {
            reader: Arc::clone(&self.reader),
        }
    }
}

pub struct LeafReaderAsNormsProducer<T: LeafReader> {
    reader: Arc<T>,
}

impl<T: LeafReader> LeafReaderAsNormsProducer<T> {
    fn new(reader: Arc<T>) -> Self {
        Self { reader }
    }
}

impl<T: LeafReader> NormsProducer for LeafReaderAsNormsProducer<T> {
    fn norms(&self, field: &FieldInfo) -> Result<Box<dyn NumericDocValues>> {
        Ok(self.reader.norm_values(&field.name)?.unwrap())
    }
}

impl<T: LeafReader> Clone for LeafReaderAsNormsProducer<T> {
    fn clone(&self) -> Self {
        Self {
            reader: Arc::clone(&self.reader),
        }
    }
}

struct LeafReaderAsDVFieldProvider<T: LeafReader> {
    reader: Arc<T>,
    field_name: String,
}

unsafe impl<L: LeafReader> Send for LeafReaderAsDVFieldProvider<L> {}

unsafe impl<L: LeafReader> Sync for LeafReaderAsDVFieldProvider<L> {}

impl<T: LeafReader> BinaryDocValuesProvider for LeafReaderAsDVFieldProvider<T> {
    fn get(&self) -> Result<Box<dyn BinaryDocValues>> {
        self.reader.get_binary_doc_values(&self.field_name)
    }
}

impl<T: LeafReader> NumericDocValuesProvider for LeafReaderAsDVFieldProvider<T> {
    fn get(&self) -> Result<Box<dyn NumericDocValues>> {
        self.reader.get_numeric_doc_values(&self.field_name)
    }
}

impl<T: LeafReader> SortedDocValuesProvider for LeafReaderAsDVFieldProvider<T> {
    fn get(&self) -> Result<Box<dyn SortedDocValues>> {
        self.reader.get_sorted_doc_values(&self.field_name)
    }
}

impl<T: LeafReader> SortedNumericDocValuesProvider for LeafReaderAsDVFieldProvider<T> {
    fn get(&self) -> Result<Box<dyn SortedNumericDocValues>> {
        self.reader.get_sorted_numeric_doc_values(&self.field_name)
    }
}

impl<T: LeafReader> SortedSetDocValuesProvider for LeafReaderAsDVFieldProvider<T> {
    fn get(&self) -> Result<Box<dyn SortedSetDocValues>> {
        self.reader.get_sorted_set_doc_values(&self.field_name)
    }
}

struct LeafReaderAsDocValuesProducer<T: LeafReader> {
    reader: Arc<T>,
}

impl<T: LeafReader> LeafReaderAsDocValuesProducer<T> {
    fn new(reader: Arc<T>) -> Self {
        Self { reader }
    }
}

impl<T: LeafReader + 'static> DocValuesProducer for LeafReaderAsDocValuesProducer<T> {
    fn get_numeric(&self, field_info: &FieldInfo) -> Result<Arc<dyn NumericDocValuesProvider>> {
        Ok(Arc::new(LeafReaderAsDVFieldProvider {
            reader: Arc::clone(&self.reader),
            field_name: field_info.name.clone(),
        }))
    }
    fn get_binary(&self, field_info: &FieldInfo) -> Result<Arc<dyn BinaryDocValuesProvider>> {
        Ok(Arc::new(LeafReaderAsDVFieldProvider {
            reader: Arc::clone(&self.reader),
            field_name: field_info.name.clone(),
        }))
    }
    fn get_sorted(&self, field: &FieldInfo) -> Result<Arc<dyn SortedDocValuesProvider>> {
        Ok(Arc::new(LeafReaderAsDVFieldProvider {
            reader: Arc::clone(&self.reader),
            field_name: field.name.clone(),
        }))
    }

    fn get_sorted_numeric(
        &self,
        field: &FieldInfo,
    ) -> Result<Arc<dyn SortedNumericDocValuesProvider>> {
        Ok(Arc::new(LeafReaderAsDVFieldProvider {
            reader: Arc::clone(&self.reader),
            field_name: field.name.clone(),
        }))
    }

    fn get_sorted_set(&self, field: &FieldInfo) -> Result<Arc<dyn SortedSetDocValuesProvider>> {
        Ok(Arc::new(LeafReaderAsDVFieldProvider {
            reader: Arc::clone(&self.reader),
            field_name: field.name.clone(),
        }))
    }
    /// Returns a `bits` at the size of `reader.max_doc()`, with turned on bits for each doc_id
    /// that does have a value for this field.
    /// The returned instance need not be thread-safe: it will only be used by a single thread.
    fn get_docs_with_field(&self, field: &FieldInfo) -> Result<Box<dyn BitsMut>> {
        self.reader.get_docs_with_field(&field.name)
    }
    /// Checks consistency of this producer
    /// Note that this may be costly in terms of I/O, e.g.
    /// may involve computing a checksum value against large data files.
    fn check_integrity(&self) -> Result<()> {
        Ok(())
    }

    fn get_merge_instance(&self) -> Result<Box<dyn DocValuesProducer>> {
        Ok(Box::new(Self {
            reader: Arc::clone(&self.reader),
        }))
    }
}

// TODO: hack logic, when merge, the dv producer need not be Send or Sync, but
// the dv producer has this restrict
unsafe impl<T: LeafReader> Send for LeafReaderAsDocValuesProducer<T> {}

unsafe impl<T: LeafReader> Sync for LeafReaderAsDocValuesProducer<T> {}
