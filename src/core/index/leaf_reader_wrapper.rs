use core::codec::{DocValuesProducer, FieldsProducer, FieldsProducerRef, NormsProducer};
use core::codec::{StoredFieldsReader, TermVectorsReader};
use core::index::sorter::{PackedLongDocMap, SorterDocMap};
use core::index::term::TermsRef;
use core::index::LeafReader;
use core::index::SegmentReader;
use core::index::StoredFieldVisitor;
use core::index::{BinaryDocValues, BinaryDocValuesRef};
use core::index::{FieldInfo, FieldInfos, Fields, IndexOptions};
use core::index::{IntersectVisitor, PointValues, PointValuesRef, Relation};
use core::index::{NumericDocValues, NumericDocValuesRef};
use core::index::{SeekStatus, TermIterator, Terms};
use core::index::{SortedDocValues, SortedDocValuesRef};
use core::index::{SortedNumericDocValues, SortedNumericDocValuesContext, SortedNumericDocValuesRef};
use core::index::{SortedSetDocValues, SortedSetDocValuesContext, SortedSetDocValuesRef};
use core::search::posting_iterator::posting_feature_requested;
use core::search::posting_iterator::PostingIterator;
use core::search::posting_iterator::{POSTING_ITERATOR_FLAG_FREQS, POSTING_ITERATOR_FLAG_POSITIONS};
use core::search::sort::Sort;
use core::search::{DocIterator, NO_MORE_DOCS};
use core::store::{DataInput, IndexInput, IndexOutput, RAMOutputStream};
use core::util::external::deferred::Deferred;
use core::util::fst::bytes_store::{BytesStore, StoreBytesReader};
use core::util::{Bits, BitsContext, BitsRef, DocId};

use error::Result;

use std::any::Any;
use std::mem;
use std::sync::Arc;

/// This is a hack to make index sorting fast, with a `LeafReader` that
/// always returns merge instances when you ask for the codec readers.
pub struct MergeReaderWrapper {
    reader: Arc<SegmentReader>,
    fields: FieldsProducerRef,
    norms: Option<Arc<NormsProducer>>,
    doc_values: Option<Box<DocValuesProducer>>,
    store: Box<StoredFieldsReader>,
    vectors: Option<Arc<TermVectorsReader>>,
}

impl MergeReaderWrapper {
    pub fn new(reader: Arc<SegmentReader>) -> Result<MergeReaderWrapper> {
        let fields = reader.postings_reader();
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
            bail!(
                "doc_id must be >= 0 and < max_doc={}, got {}",
                self.max_doc(),
                doc_id
            );
        }
        Ok(())
    }
}

impl LeafReader for MergeReaderWrapper {
    fn fields(&self) -> Result<FieldsProducerRef> {
        Ok(Arc::clone(&self.fields))
    }

    fn name(&self) -> &str {
        self.reader.name()
    }

    fn term_vector(&self, doc_id: i32) -> Result<Option<Box<Fields>>> {
        self.check_bounds(doc_id)?;
        if let Some(ref vectors) = self.vectors {
            vectors.get(doc_id)
        } else {
            bail!("reader does not have term vectors")
        }
    }

    fn document(&self, doc_id: i32, visitor: &mut StoredFieldVisitor) -> Result<()> {
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

    fn get_numeric_doc_values(&self, field: &str) -> Result<NumericDocValuesRef> {
        if let Some(field_info) = self.field_info(field) {
            self.doc_values.as_ref().unwrap().get_numeric(field_info)
        } else {
            bail!("field '{}' not exist!")
        }
    }

    fn get_binary_doc_values(&self, field: &str) -> Result<BinaryDocValuesRef> {
        if let Some(field_info) = self.field_info(field) {
            Ok(Arc::from(
                self.doc_values.as_ref().unwrap().get_binary(field_info)?,
            ))
        } else {
            bail!("field '{}' not exist!")
        }
    }

    fn get_sorted_doc_values(&self, field: &str) -> Result<SortedDocValuesRef> {
        if let Some(field_info) = self.field_info(field) {
            Ok(Arc::from(
                self.doc_values.as_ref().unwrap().get_sorted(field_info)?,
            ))
        } else {
            bail!("field '{}' not exist!")
        }
    }

    fn get_sorted_numeric_doc_values(&self, field: &str) -> Result<SortedNumericDocValuesRef> {
        if let Some(field_info) = self.field_info(field) {
            Ok(Arc::from(
                self.doc_values
                    .as_ref()
                    .unwrap()
                    .get_sorted_numeric(field_info)?,
            ))
        } else {
            bail!("field '{}' not exist!")
        }
    }

    fn get_sorted_set_doc_values(&self, field: &str) -> Result<SortedSetDocValuesRef> {
        if let Some(field_info) = self.field_info(field) {
            Ok(Arc::from(
                self.doc_values
                    .as_ref()
                    .unwrap()
                    .get_sorted_set(field_info)?,
            ))
        } else {
            bail!("field '{}' not exist!")
        }
    }

    fn norm_values(&self, field: &str) -> Result<Option<Box<NumericDocValues>>> {
        if let Some(ref norms) = self.norms {
            if let Some(field_info) = self.field_info(field) {
                return Ok(Some(norms.norms(field_info)?));
            }
        }
        Ok(None)
    }

    fn get_docs_with_field(&self, field: &str) -> Result<BitsRef> {
        if let Some(field_info) = self.field_info(field) {
            self.doc_values
                .as_ref()
                .unwrap()
                .get_docs_with_field(field_info)
        } else {
            bail!("field '{}' not exist!")
        }
    }

    fn point_values(&self) -> Option<PointValuesRef> {
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

    fn store_fields_reader(&self) -> Result<Arc<StoredFieldsReader>> {
        unreachable!()
    }

    fn term_vectors_reader(&self) -> Result<Option<Arc<TermVectorsReader>>> {
        unreachable!()
    }

    fn norms_reader(&self) -> Result<Option<Arc<NormsProducer>>> {
        unreachable!()
    }

    fn doc_values_reader(&self) -> Result<Option<Arc<DocValuesProducer>>> {
        unreachable!()
    }

    fn postings_reader(&self) -> Result<Arc<FieldsProducer>> {
        unreachable!()
    }
}

/// A `LeafReader` which supports sorting documents by a given `Sort`. This
/// is package private and is only used by Rucene whne it needs to merge a
/// newly fluhsed (unsorted) segment.
pub(crate) struct SortingLeafReader<T: LeafReader> {
    doc_map: Arc<PackedLongDocMap>,
    reader: T,
}

impl<T: LeafReader> SortingLeafReader<T> {
    pub fn new(reader: T, doc_map: Arc<PackedLongDocMap>) -> Self {
        debug_assert_eq!(reader.max_doc(), doc_map.len() as i32);
        SortingLeafReader { reader, doc_map }
    }
}

impl<T: LeafReader> LeafReader for SortingLeafReader<T> {
    fn fields(&self) -> Result<FieldsProducerRef> {
        Ok(Arc::new(SortingFields::new(
            self.reader.fields()?,
            self.reader.clone_field_infos(),
            Arc::clone(&self.doc_map),
        )))
    }

    fn name(&self) -> &str {
        self.reader.name()
    }

    fn term_vector(&self, doc_id: DocId) -> Result<Option<Box<Fields>>> {
        self.reader.term_vector(self.doc_map.new_to_old(doc_id))
    }

    fn document(&self, doc_id: DocId, visitor: &mut StoredFieldVisitor) -> Result<()> {
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

    fn get_numeric_doc_values(&self, field: &str) -> Result<NumericDocValuesRef> {
        Ok(Arc::new(SortingNumericDocValues::new(
            self.reader.get_numeric_doc_values(field)?,
            Arc::clone(&self.doc_map),
        )))
    }

    fn get_binary_doc_values(&self, field: &str) -> Result<BinaryDocValuesRef> {
        Ok(Arc::new(SortingBinaryDocValues::new(
            self.reader.get_binary_doc_values(field)?,
            Arc::clone(&self.doc_map),
        )))
    }

    fn get_sorted_doc_values(&self, field: &str) -> Result<SortedDocValuesRef> {
        Ok(Arc::new(SortingSortedDocValues::new(
            self.reader.get_sorted_doc_values(field)?,
            Arc::clone(&self.doc_map),
        )))
    }

    fn get_sorted_numeric_doc_values(&self, field: &str) -> Result<SortedNumericDocValuesRef> {
        Ok(Arc::new(SortingSortedNumericDocValues::new(
            self.reader.get_sorted_numeric_doc_values(field)?,
            Arc::clone(&self.doc_map),
        )))
    }

    fn get_sorted_set_doc_values(&self, field: &str) -> Result<SortedSetDocValuesRef> {
        Ok(Arc::new(SortingSortedSetDocValues::new(
            self.reader.get_sorted_set_doc_values(field)?,
            Arc::clone(&self.doc_map),
        )))
    }

    fn norm_values(&self, field: &str) -> Result<Option<Box<NumericDocValues>>> {
        match self.reader.norm_values(field)? {
            Some(n) => Ok(Some(Box::new(SortingNumericDocValues::new(
                n,
                Arc::clone(&self.doc_map),
            )))),
            None => Ok(None),
        }
    }

    fn get_docs_with_field(&self, field: &str) -> Result<BitsRef> {
        Ok(Arc::new(SortingBits::new(
            self.reader.get_docs_with_field(field)?,
            Arc::clone(&self.doc_map),
        )))
    }

    /// Returns the `PointValuesRef` used for numeric or
    /// spatial searches, or None if there are no point fields.
    fn point_values(&self) -> Option<PointValuesRef> {
        match self.reader.point_values() {
            Some(p) => Some(Arc::new(SortingPointValues::new(
                p,
                Arc::clone(&self.doc_map),
            ))),
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
    fn store_fields_reader(&self) -> Result<Arc<StoredFieldsReader>> {
        unreachable!()
    }

    fn term_vectors_reader(&self) -> Result<Option<Arc<TermVectorsReader>>> {
        unreachable!()
    }

    fn norms_reader(&self) -> Result<Option<Arc<NormsProducer>>> {
        unreachable!()
    }

    fn doc_values_reader(&self) -> Result<Option<Arc<DocValuesProducer>>> {
        unreachable!()
    }

    fn postings_reader(&self) -> Result<Arc<FieldsProducer>> {
        unreachable!()
    }
}

struct SortingFields {
    fields: FieldsProducerRef,
    doc_map: Arc<PackedLongDocMap>,
    infos: Arc<FieldInfos>,
}

impl SortingFields {
    fn new(
        fields: FieldsProducerRef,
        infos: Arc<FieldInfos>,
        doc_map: Arc<PackedLongDocMap>,
    ) -> Self {
        SortingFields {
            fields,
            doc_map,
            infos,
        }
    }
}

impl Fields for SortingFields {
    fn fields(&self) -> Vec<String> {
        self.fields.fields()
    }

    fn terms(&self, field: &str) -> Result<Option<TermsRef>> {
        match self.fields.terms(field)? {
            Some(terms) => Ok(Some(Arc::new(SortingTerms::new(
                terms,
                self.infos.field_info_by_name(field).unwrap().index_options,
                Arc::clone(&self.doc_map),
            )))),
            None => Ok(None),
        }
    }

    fn size(&self) -> usize {
        self.fields.size()
    }
}

impl FieldsProducer for SortingFields {
    fn check_integrity(&self) -> Result<()> {
        Ok(())
    }
}

struct SortingTerms {
    terms: TermsRef,
    doc_map: Arc<PackedLongDocMap>,
    index_options: IndexOptions,
}

impl SortingTerms {
    fn new(terms: TermsRef, index_options: IndexOptions, doc_map: Arc<PackedLongDocMap>) -> Self {
        SortingTerms {
            terms,
            index_options,
            doc_map,
        }
    }
}

impl Terms for SortingTerms {
    fn iterator(&self) -> Result<Box<TermIterator>> {
        Ok(Box::new(SortingTermsIterator::new(
            self.terms.iterator()?,
            Arc::clone(&self.doc_map),
            self.index_options,
            self.has_positions()?,
        )))
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

struct SortingTermsIterator {
    iter: Box<TermIterator>,
    doc_map: Arc<PackedLongDocMap>,
    index_options: IndexOptions,
    has_positions: bool,
}

impl SortingTermsIterator {
    fn new(
        iter: Box<TermIterator>,
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

impl TermIterator for SortingTermsIterator {
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

    fn postings_with_flags(&mut self, flags: i16) -> Result<Box<PostingIterator>> {
        if self.has_positions && posting_feature_requested(flags, POSTING_ITERATOR_FLAG_POSITIONS) {
            let docs_and_positions = self.iter.postings_with_flags(flags)?;
            // we ignore the fact that offsets may be stored but not asked for,
            // since this code is expected to be used during addIndexes which will
            // ask for everything. if that assumption changes in the future, we can
            // factor in whether 'flags' says offsets are not required.
            let store_offsets =
                self.index_options >= IndexOptions::DocsAndFreqsAndPositionsAndOffsets;
            return Ok(Box::new(SortingPostingsIterator::new(
                self.doc_map.len() as i32,
                docs_and_positions,
                self.doc_map.as_ref(),
                store_offsets,
            )?));
        }

        let docs = self.iter.postings_with_flags(flags)?;
        let with_freqs = self.index_options >= IndexOptions::DocsAndFreqs
            && posting_feature_requested(flags, POSTING_ITERATOR_FLAG_FREQS);
        Ok(Box::new(SortingDocsIterator::new(
            self.doc_map.len() as i32,
            docs,
            with_freqs,
            self.doc_map.as_ref(),
        )?))
    }

    fn as_any(&self) -> &Any {
        self
    }
}

struct SortingPostingsIterator {
    postings: Box<PostingIterator>,
    max_doc: i32,
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

impl SortingPostingsIterator {
    fn new(
        max_doc: i32,
        mut postings: Box<PostingIterator>,
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
                Self::add_positions(postings.as_mut(), &mut output, store_offsets)?;
            }
            upto = i as i32;
            mem::replace(&mut output.store, BytesStore::with_block_bits(1))
        };
        docs_and_offsets.sort_by(|d1, d2| d1.doc.cmp(&d2.doc));

        let posting_input = StoreBytesReader::from_bytes_store(file, false);

        Ok(SortingPostingsIterator {
            postings,
            max_doc,
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
        input: &mut PostingIterator,
        output: &mut IndexOutput,
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

impl DocIterator for SortingPostingsIterator {
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

impl PostingIterator for SortingPostingsIterator {
    fn clone_as_doc_iterator(&self) -> Result<Box<DocIterator>> {
        self.postings.clone_as_doc_iterator()
    }

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
            self.posting_input
                .read_bytes(&mut self.payload, 0, payload_len)?;
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

    fn as_any_mut(&mut self) -> &mut Any {
        self
    }
}

struct DocAndFreq {
    doc: DocId,
    freq: i32,
}

pub struct SortingDocsIterator {
    posting: Box<PostingIterator>,
    max_doc: i32,
    docs_and_freqs: Vec<DocAndFreq>,
    doc_it: DocId,
    upto: i32,
    with_freqs: bool,
}

impl SortingDocsIterator {
    fn new(
        max_doc: i32,
        mut posting: Box<PostingIterator>,
        with_freqs: bool,
        doc_map: &PackedLongDocMap,
    ) -> Result<SortingDocsIterator> {
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
            max_doc,
            posting,
            docs_and_freqs,
            doc_it: -1,
            upto,
            with_freqs,
        })
    }
}

impl PostingIterator for SortingDocsIterator {
    fn clone_as_doc_iterator(&self) -> Result<Box<DocIterator>> {
        self.posting.clone_as_doc_iterator()
    }

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

    fn as_any_mut(&mut self) -> &mut Any {
        self
    }
}

impl DocIterator for SortingDocsIterator {
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

pub struct SortingBinaryDocValues {
    doc_values: BinaryDocValuesRef,
    doc_map: Arc<PackedLongDocMap>,
}

impl SortingBinaryDocValues {
    fn new(doc_values: BinaryDocValuesRef, doc_map: Arc<PackedLongDocMap>) -> Self {
        SortingBinaryDocValues {
            doc_map,
            doc_values,
        }
    }
}

impl BinaryDocValues for SortingBinaryDocValues {
    fn get(&self, doc_id: i32) -> Result<Vec<u8>> {
        self.doc_values.get(self.doc_map.new_to_old(doc_id))
    }
}

pub struct SortingNumericDocValues<T: AsRef<NumericDocValues> + Send + Sync> {
    doc_values: T,
    doc_map: Arc<PackedLongDocMap>,
}

impl<T: AsRef<NumericDocValues> + Send + Sync> SortingNumericDocValues<T> {
    fn new(doc_values: T, doc_map: Arc<PackedLongDocMap>) -> Self {
        SortingNumericDocValues {
            doc_map,
            doc_values,
        }
    }
}

impl<T: AsRef<NumericDocValues> + Send + Sync> NumericDocValues for SortingNumericDocValues<T> {
    fn get_with_ctx(&self, ctx: Option<[u8; 64]>, doc_id: i32) -> Result<(i64, Option<[u8; 64]>)> {
        self.doc_values
            .as_ref()
            .get_with_ctx(ctx, self.doc_map.new_to_old(doc_id))
    }
}

struct SortingSortedNumericDocValues {
    doc_values: SortedNumericDocValuesRef,
    doc_map: Arc<PackedLongDocMap>,
}

impl SortingSortedNumericDocValues {
    fn new(doc_values: SortedNumericDocValuesRef, doc_map: Arc<PackedLongDocMap>) -> Self {
        SortingSortedNumericDocValues {
            doc_map,
            doc_values,
        }
    }
}

impl SortedNumericDocValues for SortingSortedNumericDocValues {
    fn set_document(
        &self,
        ctx: Option<SortedNumericDocValuesContext>,
        doc: i32,
    ) -> Result<SortedNumericDocValuesContext> {
        self.doc_values
            .set_document(ctx, self.doc_map.new_to_old(doc))
    }

    fn value_at(&self, ctx: &SortedNumericDocValuesContext, index: usize) -> Result<i64> {
        self.doc_values.value_at(ctx, index)
    }

    fn count(&self, ctx: &SortedNumericDocValuesContext) -> usize {
        self.doc_values.count(ctx)
    }

    fn get_numeric_doc_values(&self) -> Option<Arc<NumericDocValues>> {
        self.doc_values.get_numeric_doc_values()
    }
}

struct SortingBits {
    bits: BitsRef,
    doc_map: Arc<PackedLongDocMap>,
}

impl SortingBits {
    fn new(bits: BitsRef, doc_map: Arc<PackedLongDocMap>) -> Self {
        SortingBits { bits, doc_map }
    }
}

impl Bits for SortingBits {
    fn get_with_ctx(&self, ctx: BitsContext, index: usize) -> Result<(bool, BitsContext)> {
        self.bits
            .get_with_ctx(ctx, self.doc_map.new_to_old(index as i32) as usize)
    }

    fn len(&self) -> usize {
        self.bits.len()
    }

    fn is_empty(&self) -> bool {
        self.bits.is_empty()
    }
}

struct SortingPointValues {
    point_values: PointValuesRef,
    doc_map: Arc<PackedLongDocMap>,
}

impl SortingPointValues {
    fn new(point_values: PointValuesRef, doc_map: Arc<PackedLongDocMap>) -> Self {
        SortingPointValues {
            point_values,
            doc_map,
        }
    }
}

impl PointValues for SortingPointValues {
    fn intersect(&self, field_name: &str, visitor: &mut IntersectVisitor) -> Result<()> {
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

    fn as_any(&self) -> &Any {
        self
    }
}

struct SortingIntersectVisitor<'a> {
    visitor: &'a mut IntersectVisitor,
    doc_map: Arc<PackedLongDocMap>,
}

impl<'a> IntersectVisitor for SortingIntersectVisitor<'a> {
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
    doc_values: SortedDocValuesRef,
    doc_map: Arc<PackedLongDocMap>,
}

impl SortingSortedDocValues {
    fn new(doc_values: SortedDocValuesRef, doc_map: Arc<PackedLongDocMap>) -> Self {
        SortingSortedDocValues {
            doc_map,
            doc_values,
        }
    }
}

impl SortedDocValues for SortingSortedDocValues {
    fn get_ord(&self, doc_id: i32) -> Result<i32> {
        self.doc_values.get_ord(self.doc_map.new_to_old(doc_id))
    }

    fn lookup_ord(&self, ord: i32) -> Result<Vec<u8>> {
        self.doc_values.lookup_ord(ord)
    }

    fn get_value_count(&self) -> usize {
        self.doc_values.get_value_count()
    }

    fn term_iterator(&self) -> Result<Box<TermIterator>> {
        self.doc_values.term_iterator()
    }
}

impl BinaryDocValues for SortingSortedDocValues {
    fn get(&self, doc_id: DocId) -> Result<Vec<u8>> {
        self.doc_values.get(self.doc_map.new_to_old(doc_id))
    }
}

struct SortingSortedSetDocValues {
    doc_values: SortedSetDocValuesRef,
    doc_map: Arc<PackedLongDocMap>,
}

impl SortingSortedSetDocValues {
    fn new(doc_values: SortedSetDocValuesRef, doc_map: Arc<PackedLongDocMap>) -> Self {
        SortingSortedSetDocValues {
            doc_map,
            doc_values,
        }
    }
}

impl SortedSetDocValues for SortingSortedSetDocValues {
    fn set_document(&self, doc: DocId) -> Result<SortedSetDocValuesContext> {
        self.doc_values.set_document(self.doc_map.new_to_old(doc))
    }

    fn next_ord(&self, ctx: &mut SortedSetDocValuesContext) -> Result<i64> {
        self.doc_values.next_ord(ctx)
    }

    fn lookup_ord(&self, ord: i64) -> Result<Vec<u8>> {
        self.doc_values.lookup_ord(ord)
    }

    fn get_value_count(&self) -> usize {
        self.doc_values.get_value_count()
    }

    fn lookup_term(&self, key: &[u8]) -> Result<i64> {
        self.doc_values.lookup_term(key)
    }

    fn term_iterator(&self) -> Result<Box<TermIterator>> {
        self.doc_values.term_iterator()
    }
}

/// Wraps arbitrary readers for merging. Note that this can cause slow
/// and memory-intensive merges. Consider using `FilterCodecReader` instead
pub struct SlowCodecReaderWrapper {
    reader: Arc<LeafReader>,
}

impl SlowCodecReaderWrapper {
    pub fn new(reader: Arc<LeafReader>) -> Self {
        SlowCodecReaderWrapper { reader }
    }
}

impl LeafReader for SlowCodecReaderWrapper {
    fn fields(&self) -> Result<FieldsProducerRef> {
        self.reader.fields()
    }

    fn name(&self) -> &str {
        self.reader.name()
    }

    fn term_vector(&self, doc_id: DocId) -> Result<Option<Box<Fields>>> {
        self.reader.term_vector(doc_id)
    }

    fn document(&self, doc_id: DocId, visitor: &mut StoredFieldVisitor) -> Result<()> {
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

    fn get_numeric_doc_values(&self, field: &str) -> Result<NumericDocValuesRef> {
        self.reader.get_numeric_doc_values(field)
    }

    fn get_binary_doc_values(&self, field: &str) -> Result<BinaryDocValuesRef> {
        self.reader.get_binary_doc_values(field)
    }

    fn get_sorted_doc_values(&self, field: &str) -> Result<SortedDocValuesRef> {
        self.reader.get_sorted_doc_values(field)
    }

    fn get_sorted_numeric_doc_values(&self, field: &str) -> Result<SortedNumericDocValuesRef> {
        self.reader.get_sorted_numeric_doc_values(field)
    }

    fn get_sorted_set_doc_values(&self, field: &str) -> Result<SortedSetDocValuesRef> {
        self.reader.get_sorted_set_doc_values(field)
    }

    fn norm_values(&self, field: &str) -> Result<Option<Box<NumericDocValues>>> {
        self.reader.norm_values(field)
    }

    fn get_docs_with_field(&self, field: &str) -> Result<BitsRef> {
        self.reader.get_docs_with_field(field)
    }

    /// Returns the `PointValuesRef` used for numeric or
    /// spatial searches, or None if there are no point fields.
    fn point_values(&self) -> Option<PointValuesRef> {
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
    fn store_fields_reader(&self) -> Result<Arc<StoredFieldsReader>> {
        Ok(Arc::new(LeafReaderAsStoreFieldsReader {
            reader: Arc::clone(&self.reader),
        }))
    }

    fn term_vectors_reader(&self) -> Result<Option<Arc<TermVectorsReader>>> {
        Ok(Some(Arc::new(LeafReaderAsTermVectorsReaderWrapper {
            reader: Arc::clone(&self.reader),
        })))
    }

    fn norms_reader(&self) -> Result<Option<Arc<NormsProducer>>> {
        Ok(Some(Arc::new(LeafReaderAsNormsProducer {
            reader: Arc::clone(&self.reader),
        })))
    }

    fn doc_values_reader(&self) -> Result<Option<Arc<DocValuesProducer>>> {
        Ok(Some(Arc::new(LeafReaderAsDocValuesProducer {
            reader: Arc::clone(&self.reader),
        })))
    }

    fn postings_reader(&self) -> Result<FieldsProducerRef> {
        self.reader.fields()
    }
}

struct LeafReaderAsStoreFieldsReader {
    reader: Arc<LeafReader>,
}

impl StoredFieldsReader for LeafReaderAsStoreFieldsReader {
    fn visit_document(&self, doc_id: DocId, visitor: &mut StoredFieldVisitor) -> Result<()> {
        self.reader.document(doc_id, visitor)
    }

    // a mutable version for `Self::visit_document` that maybe more fast
    fn visit_document_mut(
        &mut self,
        doc_id: DocId,
        visitor: &mut StoredFieldVisitor,
    ) -> Result<()> {
        self.visit_document(doc_id, visitor)
    }

    fn get_merge_instance(&self) -> Result<Box<StoredFieldsReader>> {
        Ok(Box::new(LeafReaderAsStoreFieldsReader {
            reader: Arc::clone(&self.reader),
        }))
    }

    fn as_any(&self) -> &Any {
        self
    }
}

struct LeafReaderAsTermVectorsReaderWrapper {
    reader: Arc<LeafReader>,
}

impl TermVectorsReader for LeafReaderAsTermVectorsReaderWrapper {
    fn get(&self, doc: i32) -> Result<Option<Box<Fields>>> {
        self.reader.term_vector(doc)
    }

    fn as_any(&self) -> &Any {
        self
    }
}

struct LeafReaderAsNormsProducer {
    reader: Arc<LeafReader>,
}

impl NormsProducer for LeafReaderAsNormsProducer {
    fn norms(&self, field: &FieldInfo) -> Result<Box<NumericDocValues>> {
        Ok(self.reader.norm_values(&field.name)?.unwrap())
    }
}

struct LeafReaderAsDocValuesProducer {
    reader: Arc<LeafReader>,
}

impl DocValuesProducer for LeafReaderAsDocValuesProducer {
    fn get_numeric(&self, field_info: &FieldInfo) -> Result<Arc<NumericDocValues>> {
        self.reader.get_numeric_doc_values(&field_info.name)
    }
    fn get_binary(&self, field_info: &FieldInfo) -> Result<Arc<BinaryDocValues>> {
        self.reader.get_binary_doc_values(&field_info.name)
    }
    fn get_sorted(&self, field: &FieldInfo) -> Result<Arc<SortedDocValues>> {
        self.reader.get_sorted_doc_values(&field.name)
    }
    fn get_sorted_numeric(&self, field: &FieldInfo) -> Result<Arc<SortedNumericDocValues>> {
        self.reader.get_sorted_numeric_doc_values(&field.name)
    }
    fn get_sorted_set(&self, field: &FieldInfo) -> Result<Arc<SortedSetDocValues>> {
        self.reader.get_sorted_set_doc_values(&field.name)
    }
    /// Returns a `bits` at the size of `reader.max_doc()`, with turned on bits for each doc_id
    /// that does have a value for this field.
    /// The returned instance need not be thread-safe: it will only be used by a single thread.
    fn get_docs_with_field(&self, field: &FieldInfo) -> Result<BitsRef> {
        self.reader.get_docs_with_field(&field.name)
    }
    /// Checks consistency of this producer
    /// Note that this may be costly in terms of I/O, e.g.
    /// may involve computing a checksum value against large data files.
    fn check_integrity(&self) -> Result<()> {
        Ok(())
    }

    fn get_merge_instance(&self) -> Result<Box<DocValuesProducer>> {
        Ok(Box::new(Self {
            reader: Arc::clone(&self.reader),
        }))
    }
}
