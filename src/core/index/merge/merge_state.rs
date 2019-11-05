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

use core::codec::doc_values::{
    BinaryDocValues, DocValuesProducer, NumericDocValues, SortedDocValues, SortedNumericDocValues,
    SortedSetDocValues,
};
use core::codec::field_infos::{FieldInfo, FieldInfos};
use core::codec::norms::NormsProducer;
use core::codec::points::{IntersectVisitor, PointValues};
use core::codec::postings::FieldsProducer;
use core::codec::segment_infos::SegmentInfo;
use core::codec::stored_fields::StoredFieldsReader;
use core::codec::term_vectors::TermVectorsReader;
use core::codec::{
    Codec, CodecFieldsProducer, CodecNormsProducer, CodecPointsReader, CodecStoredFieldsReader,
    CodecTVFields, CodecTVReader,
};
use core::codec::{Fields, SeekStatus, TermIterator, Terms};
use core::codec::{MultiSorter, PackedLongDocMap, Sorter, SorterDocMap};
use core::doc::StoredFieldVisitor;
use core::index::reader::*;
use core::search::sort_field::Sort;
use core::util::external::Deferred;
use core::util::packed::COMPACT;
use core::util::packed::{
    PackedLongValues, PackedLongValuesBuilder, PackedLongValuesBuilderType, DEFAULT_PAGE_SIZE,
};
use core::util::{Bits, BitsMut, BitsRef, DocId};

use error::ErrorKind::IllegalArgument;
use error::Result;

use core::store::directory::Directory;
use std::any::Any;
use std::sync::Arc;

/// Holds common state used during segment merging.
pub struct MergeState<D: Directory + 'static, C: Codec> {
    /// Maps document IDs from old segments to document IDs in the new segment
    pub doc_maps: Vec<Arc<LiveDocsDocMap>>,
    /// Only used by IW when it must remap deletes that arrived against the
    /// merging segments while a merge was running:
    pub leaf_doc_maps: Vec<MergeDocMap>,
    /// `SegmentInfo` of the newly merged segment.
    // WARN: pointer to OneMerge.info.info, must used carefully
    pub segment_info: *mut SegmentInfo<D, C>,
    /// `FieldInfos` of the newly merged segment.
    pub merge_field_infos: Option<Arc<FieldInfos>>,
    pub stored_fields_readers: Vec<
        MergeStoredReaderEnum<
            CodecStoredFieldsReader<C>,
            SortingLeafReader<MergeReaderWrapper<D, C>>,
        >,
    >,
    pub term_vectors_readers: Vec<
        Option<MergeTVReaderEnum<CodecTVReader<C>, SortingLeafReader<MergeReaderWrapper<D, C>>>>,
    >,
    pub norms_producers: Vec<
        Option<
            MergeNormsReaderEnum<
                CodecNormsProducer<C>,
                SortingLeafReader<MergeReaderWrapper<D, C>>,
            >,
        >,
    >,
    pub doc_values_producers: Vec<Option<Box<dyn DocValuesProducer>>>,
    pub fields_infos: Vec<Arc<FieldInfos>>,
    pub live_docs: Vec<BitsRef>,
    pub fields_producers: Vec<MergeFieldsProducer<CodecFieldsProducer<C>>>,
    pub points_readers: Vec<Option<MergePointValuesEnum<Arc<CodecPointsReader<C>>>>>,
    pub max_docs: Vec<i32>,
    /// Indicates if the index needs to be sorted
    pub needs_index_sort: bool,
}

impl<D: Directory + 'static, C: Codec> MergeState<D, C> {
    pub fn new(
        seg_readers: Vec<Arc<SegmentReader<D, C>>>,
        segment_info: &SegmentInfo<D, C>,
    ) -> Result<Self> {
        let num_readers = seg_readers.len();

        let mut leaf_doc_maps = Vec::with_capacity(num_readers);
        for _ in 0..seg_readers.len() {
            leaf_doc_maps.push(MergeDocMap(DocMapEnum::Default(DefaultDocMap::default())));
        }
        let mut needs_index_sort = false;
        let readers: Vec<ReaderWrapperEnum<D, C>> = Self::maybe_sort_readers(
            seg_readers,
            segment_info,
            &mut leaf_doc_maps,
            &mut needs_index_sort,
        )?;
        let doc_maps =
            Self::build_doc_maps(&readers, segment_info.index_sort(), &mut needs_index_sort)?
                .into_iter()
                .map(Arc::new)
                .collect();
        let mut max_docs = Vec::with_capacity(num_readers);
        let mut fields_producers = Vec::with_capacity(num_readers);
        let mut norms_producers = Vec::with_capacity(num_readers);
        let mut stored_fields_readers = Vec::with_capacity(num_readers);
        let mut term_vectors_readers = Vec::with_capacity(num_readers);
        let mut doc_values_producers = Vec::with_capacity(num_readers);
        let mut points_readers = Vec::with_capacity(num_readers);
        let mut fields_infos = Vec::with_capacity(num_readers);
        let mut live_docs = Vec::with_capacity(num_readers);

        let mut num_docs = 0;
        for reader in &readers {
            max_docs.push(reader.max_doc());
            live_docs.push(reader.live_docs());
            fields_infos.push(reader.clone_field_infos());

            norms_producers.push(reader.norms_reader()?);
            let doc_value_producer = if let Some(producer) = reader.doc_values_reader()? {
                Some(producer.get_merge_instance()?)
            } else {
                None
            };
            doc_values_producers.push(doc_value_producer);
            stored_fields_readers.push(reader.store_fields_reader()?.get_merge_instance()?);
            term_vectors_readers.push(reader.term_vectors_reader()?);
            fields_producers.push(reader.postings_reader()?);
            points_readers.push(reader.point_values());
            num_docs += reader.num_docs();
        }
        // TODO: hack logic
        let segment_info_ptr = segment_info as *const SegmentInfo<D, C> as *mut SegmentInfo<D, C>;
        unsafe {
            (*segment_info_ptr).set_max_doc(num_docs)?;
        }
        Ok(MergeState {
            doc_maps,
            leaf_doc_maps,
            segment_info: segment_info_ptr,
            merge_field_infos: None,
            stored_fields_readers,
            term_vectors_readers,
            norms_producers,
            doc_values_producers,
            fields_infos,
            live_docs,
            fields_producers,
            points_readers,
            max_docs,
            needs_index_sort,
        })
    }

    #[allow(clippy::mut_from_ref)]
    pub fn segment_info(&self) -> &mut SegmentInfo<D, C> {
        unsafe { &mut *self.segment_info }
    }

    fn maybe_sort_readers(
        seg_readers: Vec<Arc<SegmentReader<D, C>>>,
        segment_info: &SegmentInfo<D, C>,
        leaf_doc_maps: &mut Vec<MergeDocMap>,
        needs_index_sort: &mut bool,
    ) -> Result<Vec<ReaderWrapperEnum<D, C>>> {
        if segment_info.index_sort.is_none() {
            let mut readers = Vec::with_capacity(seg_readers.len());
            for leaf in seg_readers {
                readers.push(ReaderWrapperEnum::Segment(leaf));
            }
            return Ok(readers);
        }
        let index_sort = segment_info.index_sort.as_ref().unwrap().clone();
        let sorter = Sorter::new(index_sort);

        let mut readers = Vec::with_capacity(seg_readers.len());
        for leaf in seg_readers {
            let leaf_wrapper = if leaf.index_sort().is_some() {
                if leaf.index_sort() != segment_info.index_sort() {
                    error!(
                        "leaf: {:?}, seg: {:?}",
                        leaf.index_sort(),
                        segment_info.index_sort()
                    );
                    bail!(IllegalArgument("index sort mismatch".into()))
                } else {
                    ReaderWrapperEnum::Segment(leaf)
                }
            } else {
                // TODO: fix IW to also sort when flushing?  It's somewhat tricky because of stored
                // fields and term vectors, which write "live" to their index files
                // on each indexed document:

                // This segment was written by flush, so documents are not yet sorted, so we sort
                // them now:
                let sort_doc_map = sorter.sort_leaf_reader(&leaf.leaf_context())?;
                if let Some(sort_doc_map) = sort_doc_map {
                    *needs_index_sort = true;
                    let doc_map_ref = Arc::new(sort_doc_map);
                    leaf_doc_maps[readers.len()] = MergeDocMap(DocMapEnum::Sorted(
                        DocMapBySortDocMap::new(Arc::clone(&doc_map_ref)),
                    ));
                    let reader = SlowCodecReaderWrapper::new(Arc::new(SortingLeafReader::new(
                        MergeReaderWrapper::new(leaf)?,
                        doc_map_ref,
                    )));
                    ReaderWrapperEnum::SortedSegment(reader)
                } else {
                    ReaderWrapperEnum::Segment(leaf)
                }
            };
            readers.push(leaf_wrapper);
        }
        Ok(readers)
    }

    fn build_doc_maps<D1: Directory>(
        readers: &[ReaderWrapperEnum<D1, C>],
        index_sort: Option<&Sort>,
        needs_index_sort: &mut bool,
    ) -> Result<Vec<LiveDocsDocMap>> {
        if let Some(sort) = index_sort {
            // do a merge sort of the incoming leaves:
            let res = MultiSorter::sort(sort, readers)?;
            if !res.is_empty() {
                *needs_index_sort = true;
                return Ok(res);
            }
        }
        Self::build_deletion_doc_maps(readers)
    }

    /// remap doc_ids around deletions
    fn build_deletion_doc_maps<D1: Directory>(
        readers: &[ReaderWrapperEnum<D1, C>],
    ) -> Result<Vec<LiveDocsDocMap>> {
        let mut total_docs = 0;
        let num_readers = readers.len();
        let mut doc_maps = Vec::with_capacity(num_readers);
        for reader in readers {
            let live_docs = reader.live_docs();
            let del_doc_map = Self::remove_deletes(reader.max_doc(), live_docs.as_ref())?;
            let doc_map = LiveDocsDocMap::new(live_docs, del_doc_map, total_docs);
            doc_maps.push(doc_map);
            total_docs += reader.num_docs();
        }
        Ok(doc_maps)
    }

    fn remove_deletes(max_doc: i32, live_docs: &dyn Bits) -> Result<PackedLongValues> {
        debug_assert!(max_doc >= 0);
        let mut doc_map_builder = PackedLongValuesBuilder::new(
            DEFAULT_PAGE_SIZE,
            COMPACT,
            PackedLongValuesBuilderType::Monotonic,
        );
        let mut del = 0;
        for i in 0..max_doc {
            doc_map_builder.add((i - del) as i64);
            if !live_docs.get(i as usize)? {
                del += 1;
            }
        }
        Ok(doc_map_builder.build())
    }
}

pub enum ReaderWrapperEnum<D: Directory + 'static, C: Codec> {
    Segment(Arc<SegmentReader<D, C>>),
    SortedSegment(SlowCodecReaderWrapper<SortingLeafReader<MergeReaderWrapper<D, C>>>),
}

impl<D: Directory + 'static, C: Codec> LeafReader for ReaderWrapperEnum<D, C> {
    type Codec = C;
    type FieldsProducer = MergeFieldsProducer<CodecFieldsProducer<C>>;
    type TVFields = CodecTVFields<C>;
    type TVReader =
        MergeTVReaderEnum<CodecTVReader<C>, SortingLeafReader<MergeReaderWrapper<D, C>>>;
    type StoredReader = MergeStoredReaderEnum<
        CodecStoredFieldsReader<C>,
        SortingLeafReader<MergeReaderWrapper<D, C>>,
    >;
    type NormsReader =
        MergeNormsReaderEnum<CodecNormsProducer<C>, SortingLeafReader<MergeReaderWrapper<D, C>>>;
    type PointsReader = MergePointValuesEnum<Arc<CodecPointsReader<C>>>;

    fn codec(&self) -> &Self::Codec {
        match self {
            ReaderWrapperEnum::Segment(s) => s.codec(),
            ReaderWrapperEnum::SortedSegment(s) => s.codec(),
        }
    }

    fn fields(&self) -> Result<Self::FieldsProducer> {
        match self {
            ReaderWrapperEnum::Segment(s) => Ok(MergeFieldsProducer(MergeFieldsProducerEnum::Raw(
                s.fields()?,
            ))),
            ReaderWrapperEnum::SortedSegment(s) => Ok(MergeFieldsProducer(
                MergeFieldsProducerEnum::Sort(s.fields()?),
            )),
        }
    }

    fn name(&self) -> &str {
        match self {
            ReaderWrapperEnum::Segment(s) => s.name(),
            ReaderWrapperEnum::SortedSegment(s) => s.name(),
        }
    }

    fn term_vector(&self, doc_id: DocId) -> Result<Option<CodecTVFields<C>>> {
        match self {
            ReaderWrapperEnum::Segment(s) => LeafReader::term_vector(s.as_ref(), doc_id),
            ReaderWrapperEnum::SortedSegment(s) => s.term_vector(doc_id),
        }
    }

    fn document(&self, doc_id: DocId, visitor: &mut dyn StoredFieldVisitor) -> Result<()> {
        match self {
            ReaderWrapperEnum::Segment(s) => LeafReader::document(s.as_ref(), doc_id, visitor),
            ReaderWrapperEnum::SortedSegment(s) => s.document(doc_id, visitor),
        }
    }

    fn live_docs(&self) -> BitsRef {
        match self {
            ReaderWrapperEnum::Segment(s) => s.live_docs(),
            ReaderWrapperEnum::SortedSegment(s) => s.live_docs(),
        }
    }

    fn field_info(&self, field: &str) -> Option<&FieldInfo> {
        match self {
            ReaderWrapperEnum::Segment(s) => s.field_info(field),
            ReaderWrapperEnum::SortedSegment(s) => s.field_info(field),
        }
    }

    fn field_infos(&self) -> &FieldInfos {
        match self {
            ReaderWrapperEnum::Segment(s) => s.field_infos(),
            ReaderWrapperEnum::SortedSegment(s) => s.field_infos(),
        }
    }

    fn clone_field_infos(&self) -> Arc<FieldInfos> {
        match self {
            ReaderWrapperEnum::Segment(s) => s.clone_field_infos(),
            ReaderWrapperEnum::SortedSegment(s) => s.clone_field_infos(),
        }
    }

    fn max_doc(&self) -> DocId {
        match self {
            ReaderWrapperEnum::Segment(s) => LeafReader::max_doc(s.as_ref()),
            ReaderWrapperEnum::SortedSegment(s) => s.max_doc(),
        }
    }

    fn num_docs(&self) -> i32 {
        match self {
            ReaderWrapperEnum::Segment(s) => s.num_docs(),
            ReaderWrapperEnum::SortedSegment(s) => s.num_docs(),
        }
    }

    fn get_numeric_doc_values(&self, field: &str) -> Result<Box<dyn NumericDocValues>> {
        match self {
            ReaderWrapperEnum::Segment(s) => s.get_numeric_doc_values(field),
            ReaderWrapperEnum::SortedSegment(s) => s.get_numeric_doc_values(field),
        }
    }

    fn get_binary_doc_values(&self, field: &str) -> Result<Box<dyn BinaryDocValues>> {
        match self {
            ReaderWrapperEnum::Segment(s) => s.get_binary_doc_values(field),
            ReaderWrapperEnum::SortedSegment(s) => s.get_binary_doc_values(field),
        }
    }

    fn get_sorted_doc_values(&self, field: &str) -> Result<Box<dyn SortedDocValues>> {
        match self {
            ReaderWrapperEnum::Segment(s) => s.get_sorted_doc_values(field),
            ReaderWrapperEnum::SortedSegment(s) => s.get_sorted_doc_values(field),
        }
    }

    fn get_sorted_numeric_doc_values(
        &self,
        field: &str,
    ) -> Result<Box<dyn SortedNumericDocValues>> {
        match self {
            ReaderWrapperEnum::Segment(s) => s.get_sorted_numeric_doc_values(field),
            ReaderWrapperEnum::SortedSegment(s) => s.get_sorted_numeric_doc_values(field),
        }
    }

    fn get_sorted_set_doc_values(&self, field: &str) -> Result<Box<dyn SortedSetDocValues>> {
        match self {
            ReaderWrapperEnum::Segment(s) => s.get_sorted_set_doc_values(field),
            ReaderWrapperEnum::SortedSegment(s) => s.get_sorted_set_doc_values(field),
        }
    }

    fn norm_values(&self, field: &str) -> Result<Option<Box<dyn NumericDocValues>>> {
        match self {
            ReaderWrapperEnum::Segment(s) => s.norm_values(field),
            ReaderWrapperEnum::SortedSegment(s) => s.norm_values(field),
        }
    }

    fn get_docs_with_field(&self, field: &str) -> Result<Box<dyn BitsMut>> {
        match self {
            ReaderWrapperEnum::Segment(s) => s.get_docs_with_field(field),
            ReaderWrapperEnum::SortedSegment(s) => s.get_docs_with_field(field),
        }
    }

    /// Returns the `PointValues` used for numeric or
    /// spatial searches, or None if there are no point fields.
    fn point_values(&self) -> Option<Self::PointsReader> {
        match self {
            ReaderWrapperEnum::Segment(s) => s.point_values().map(MergePointValuesEnum::Raw),
            ReaderWrapperEnum::SortedSegment(s) => {
                s.point_values().map(MergePointValuesEnum::Sorting)
            }
        }
    }

    /// Expert: Returns a key for this IndexReader, so CachingWrapperFilter can find
    // it again.
    // This key must not have equals()/hashCode() methods, so &quot;equals&quot; means
    // &quot;identical&quot;.
    fn core_cache_key(&self) -> &str {
        match self {
            ReaderWrapperEnum::Segment(s) => s.core_cache_key(),
            ReaderWrapperEnum::SortedSegment(s) => s.core_cache_key(),
        }
    }

    /// Returns null if this leaf is unsorted, or the `Sort` that it was sorted by
    fn index_sort(&self) -> Option<&Sort> {
        match self {
            ReaderWrapperEnum::Segment(s) => s.index_sort(),
            ReaderWrapperEnum::SortedSegment(s) => s.index_sort(),
        }
    }

    fn add_core_drop_listener(&self, listener: Deferred) {
        match self {
            ReaderWrapperEnum::Segment(s) => s.add_core_drop_listener(listener),
            ReaderWrapperEnum::SortedSegment(s) => s.add_core_drop_listener(listener),
        }
    }

    fn is_codec_reader(&self) -> bool {
        match self {
            ReaderWrapperEnum::Segment(s) => s.is_codec_reader(),
            ReaderWrapperEnum::SortedSegment(s) => s.is_codec_reader(),
        }
    }

    // following methods are from `CodecReader`
    fn store_fields_reader(&self) -> Result<Self::StoredReader> {
        match self {
            ReaderWrapperEnum::Segment(s) => {
                Ok(MergeStoredReaderEnum::Raw(s.store_fields_reader()?))
            }
            ReaderWrapperEnum::SortedSegment(s) => {
                Ok(MergeStoredReaderEnum::LeafReader(s.store_fields_reader()?))
            }
        }
    }

    fn term_vectors_reader(&self) -> Result<Option<Self::TVReader>> {
        match self {
            ReaderWrapperEnum::Segment(s) => {
                if let Some(tv_reader) = s.term_vectors_reader()? {
                    Ok(Some(MergeTVReaderEnum::Raw(tv_reader)))
                } else {
                    Ok(None)
                }
            }
            ReaderWrapperEnum::SortedSegment(s) => {
                if let Some(tv_reader) = s.term_vectors_reader()? {
                    Ok(Some(MergeTVReaderEnum::LeafReader(tv_reader)))
                } else {
                    Ok(None)
                }
            }
        }
    }

    fn norms_reader(&self) -> Result<Option<Self::NormsReader>> {
        match self {
            ReaderWrapperEnum::Segment(s) => {
                if let Some(norm_reader) = s.norms_reader()? {
                    Ok(Some(MergeNormsReaderEnum::Raw(norm_reader)))
                } else {
                    Ok(None)
                }
            }
            ReaderWrapperEnum::SortedSegment(s) => {
                if let Some(norm_reader) = s.norms_reader()? {
                    Ok(Some(MergeNormsReaderEnum::LeafReader(norm_reader)))
                } else {
                    Ok(None)
                }
            }
        }
    }

    fn doc_values_reader(&self) -> Result<Option<Arc<dyn DocValuesProducer>>> {
        match self {
            ReaderWrapperEnum::Segment(s) => s.doc_values_reader(),
            ReaderWrapperEnum::SortedSegment(s) => s.doc_values_reader(),
        }
    }

    fn postings_reader(&self) -> Result<Self::FieldsProducer> {
        self.fields()
    }
}

pub enum MergeStoredReaderEnum<R: StoredFieldsReader, L: LeafReader> {
    Raw(Arc<R>),
    LeafReader(LeafReaderAsStoreFieldsReader<L>),
}

impl<R: StoredFieldsReader, L: LeafReader> Clone for MergeStoredReaderEnum<R, L> {
    fn clone(&self) -> Self {
        match self {
            MergeStoredReaderEnum::Raw(s) => MergeStoredReaderEnum::Raw(Arc::clone(s)),
            MergeStoredReaderEnum::LeafReader(s) => MergeStoredReaderEnum::LeafReader(s.clone()),
        }
    }
}

impl<R: StoredFieldsReader + 'static, L: LeafReader + 'static> StoredFieldsReader
    for MergeStoredReaderEnum<R, L>
{
    fn visit_document(&self, doc_id: DocId, visitor: &mut dyn StoredFieldVisitor) -> Result<()> {
        match self {
            MergeStoredReaderEnum::Raw(s) => s.visit_document(doc_id, visitor),
            MergeStoredReaderEnum::LeafReader(s) => s.visit_document(doc_id, visitor),
        }
    }

    fn visit_document_mut(
        &mut self,
        doc_id: DocId,
        visitor: &mut dyn StoredFieldVisitor,
    ) -> Result<()> {
        match self {
            MergeStoredReaderEnum::Raw(s) => s.visit_document_mut(doc_id, visitor),
            MergeStoredReaderEnum::LeafReader(s) => s.visit_document_mut(doc_id, visitor),
        }
    }

    fn get_merge_instance(&self) -> Result<Self> {
        match self {
            MergeStoredReaderEnum::Raw(s) => {
                Ok(MergeStoredReaderEnum::Raw(s.get_merge_instance()?))
            }
            MergeStoredReaderEnum::LeafReader(s) => {
                Ok(MergeStoredReaderEnum::LeafReader(s.get_merge_instance()?))
            }
        }
    }

    fn as_any(&self) -> &dyn Any {
        match self {
            MergeStoredReaderEnum::Raw(s) => s.as_any(),
            MergeStoredReaderEnum::LeafReader(s) => s.as_any(),
        }
    }
}

pub enum MergeTVReaderEnum<TR: TermVectorsReader, L: LeafReader<TVFields = TR::Fields>> {
    Raw(Arc<TR>),
    LeafReader(LeafReaderAsTermVectorsReaderWrapper<L>),
}

impl<TR, L> TermVectorsReader for MergeTVReaderEnum<TR, L>
where
    TR: TermVectorsReader + 'static,
    L: LeafReader<TVFields = TR::Fields> + 'static,
{
    type Fields = TR::Fields;

    fn get(&self, doc: DocId) -> Result<Option<Self::Fields>> {
        match self {
            MergeTVReaderEnum::Raw(tr) => tr.get(doc),
            MergeTVReaderEnum::LeafReader(tr) => tr.get(doc),
        }
    }

    fn as_any(&self) -> &dyn Any {
        match self {
            MergeTVReaderEnum::Raw(tr) => tr.as_any(),
            MergeTVReaderEnum::LeafReader(tr) => tr.as_any(),
        }
    }
}

impl<TR: TermVectorsReader, L: LeafReader<TVFields = TR::Fields>> Clone
    for MergeTVReaderEnum<TR, L>
{
    fn clone(&self) -> Self {
        match self {
            MergeTVReaderEnum::Raw(tr) => MergeTVReaderEnum::Raw(Arc::clone(tr)),
            MergeTVReaderEnum::LeafReader(tr) => MergeTVReaderEnum::LeafReader(tr.clone()),
        }
    }
}

pub enum MergeNormsReaderEnum<R: NormsProducer, L: LeafReader> {
    Raw(Arc<R>),
    LeafReader(LeafReaderAsNormsProducer<L>),
}

impl<R: NormsProducer, L: LeafReader> NormsProducer for MergeNormsReaderEnum<R, L> {
    fn norms(&self, field: &FieldInfo) -> Result<Box<dyn NumericDocValues>> {
        match self {
            MergeNormsReaderEnum::Raw(n) => n.norms(field),
            MergeNormsReaderEnum::LeafReader(n) => n.norms(field),
        }
    }

    fn check_integrity(&self) -> Result<()> {
        match self {
            MergeNormsReaderEnum::Raw(n) => n.check_integrity(),
            MergeNormsReaderEnum::LeafReader(n) => n.check_integrity(),
        }
    }
}

impl<R: NormsProducer, L: LeafReader> Clone for MergeNormsReaderEnum<R, L> {
    fn clone(&self) -> Self {
        match self {
            MergeNormsReaderEnum::Raw(n) => MergeNormsReaderEnum::Raw(Arc::clone(n)),
            MergeNormsReaderEnum::LeafReader(n) => MergeNormsReaderEnum::LeafReader(n.clone()),
        }
    }
}

#[derive(Clone)]
pub enum MergePointValuesEnum<P: PointValues + Clone> {
    Raw(P),
    Sorting(SortingPointValues<P>),
}

impl<P: PointValues + Clone + 'static> PointValues for MergePointValuesEnum<P> {
    fn intersect(&self, field_name: &str, visitor: &mut impl IntersectVisitor) -> Result<()> {
        match self {
            MergePointValuesEnum::Raw(p) => p.intersect(field_name, visitor),
            MergePointValuesEnum::Sorting(p) => p.intersect(field_name, visitor),
        }
    }

    fn min_packed_value(&self, field_name: &str) -> Result<Vec<u8>> {
        match self {
            MergePointValuesEnum::Raw(p) => p.min_packed_value(field_name),
            MergePointValuesEnum::Sorting(p) => p.min_packed_value(field_name),
        }
    }

    fn max_packed_value(&self, field_name: &str) -> Result<Vec<u8>> {
        match self {
            MergePointValuesEnum::Raw(p) => p.max_packed_value(field_name),
            MergePointValuesEnum::Sorting(p) => p.max_packed_value(field_name),
        }
    }

    fn num_dimensions(&self, field_name: &str) -> Result<usize> {
        match self {
            MergePointValuesEnum::Raw(p) => p.num_dimensions(field_name),
            MergePointValuesEnum::Sorting(p) => p.num_dimensions(field_name),
        }
    }

    fn bytes_per_dimension(&self, field_name: &str) -> Result<usize> {
        match self {
            MergePointValuesEnum::Raw(p) => p.bytes_per_dimension(field_name),
            MergePointValuesEnum::Sorting(p) => p.bytes_per_dimension(field_name),
        }
    }

    fn size(&self, field_name: &str) -> Result<i64> {
        match self {
            MergePointValuesEnum::Raw(p) => p.size(field_name),
            MergePointValuesEnum::Sorting(p) => p.size(field_name),
        }
    }

    fn doc_count(&self, field_name: &str) -> Result<i32> {
        match self {
            MergePointValuesEnum::Raw(p) => p.doc_count(field_name),
            MergePointValuesEnum::Sorting(p) => p.doc_count(field_name),
        }
    }

    fn as_any(&self) -> &dyn Any {
        match self {
            MergePointValuesEnum::Raw(p) => p.as_any(),
            MergePointValuesEnum::Sorting(p) => p.as_any(),
        }
    }
}

#[derive(Clone)]
pub struct MergeFieldsProducer<T: FieldsProducer>(MergeFieldsProducerEnum<T>);

impl<T: FieldsProducer> FieldsProducer for MergeFieldsProducer<T> {
    fn check_integrity(&self) -> Result<()> {
        match &self.0 {
            MergeFieldsProducerEnum::Raw(f) => f.check_integrity(),
            MergeFieldsProducerEnum::Sort(f) => f.check_integrity(),
        }
    }
}

impl<T: FieldsProducer> Fields for MergeFieldsProducer<T> {
    type Terms = MergeTerms<T::Terms>;
    fn fields(&self) -> Vec<String> {
        match &self.0 {
            MergeFieldsProducerEnum::Raw(f) => f.fields(),
            MergeFieldsProducerEnum::Sort(f) => f.fields(),
        }
    }

    fn terms(&self, field: &str) -> Result<Option<Self::Terms>> {
        match &self.0 {
            MergeFieldsProducerEnum::Raw(f) => {
                if let Some(terms) = f.terms(field)? {
                    Ok(Some(MergeTerms(MergeTermsEnum::Raw(terms))))
                } else {
                    Ok(None)
                }
            }
            MergeFieldsProducerEnum::Sort(f) => {
                if let Some(terms) = f.terms(field)? {
                    Ok(Some(MergeTerms(MergeTermsEnum::Sort(terms))))
                } else {
                    Ok(None)
                }
            }
        }
    }

    fn size(&self) -> usize {
        match &self.0 {
            MergeFieldsProducerEnum::Raw(f) => f.size(),
            MergeFieldsProducerEnum::Sort(f) => f.size(),
        }
    }

    fn terms_freq(&self, field: &str) -> usize {
        match &self.0 {
            MergeFieldsProducerEnum::Raw(f) => f.terms_freq(field),
            MergeFieldsProducerEnum::Sort(f) => f.terms_freq(field),
        }
    }
}

enum MergeFieldsProducerEnum<T: FieldsProducer> {
    Raw(T),
    Sort(Arc<SortingFields<T>>),
}

impl<T> Clone for MergeFieldsProducerEnum<T>
where
    T: FieldsProducer + Clone,
{
    fn clone(&self) -> Self {
        match self {
            MergeFieldsProducerEnum::Raw(f) => MergeFieldsProducerEnum::Raw(f.clone()),
            MergeFieldsProducerEnum::Sort(f) => MergeFieldsProducerEnum::Sort(Arc::clone(f)),
        }
    }
}

enum MergeTermsEnum<T: Terms> {
    Raw(T),
    Sort(SortingTerms<T>),
}

pub struct MergeTerms<T: Terms>(MergeTermsEnum<T>);

impl<T: Terms> Terms for MergeTerms<T> {
    type Iterator = MergeTermIterator<T::Iterator>;
    fn iterator(&self) -> Result<Self::Iterator> {
        match &self.0 {
            MergeTermsEnum::Raw(t) => {
                Ok(MergeTermIterator(MergeTermIteratorEnum::Raw(t.iterator()?)))
            }
            MergeTermsEnum::Sort(t) => Ok(MergeTermIterator(MergeTermIteratorEnum::Sorting(
                t.iterator()?,
            ))),
        }
    }

    fn size(&self) -> Result<i64> {
        match &self.0 {
            MergeTermsEnum::Raw(t) => t.size(),
            MergeTermsEnum::Sort(t) => t.size(),
        }
    }

    fn sum_total_term_freq(&self) -> Result<i64> {
        match &self.0 {
            MergeTermsEnum::Raw(t) => t.sum_total_term_freq(),
            MergeTermsEnum::Sort(t) => t.sum_total_term_freq(),
        }
    }

    fn sum_doc_freq(&self) -> Result<i64> {
        match &self.0 {
            MergeTermsEnum::Raw(t) => t.sum_doc_freq(),
            MergeTermsEnum::Sort(t) => t.sum_doc_freq(),
        }
    }

    fn doc_count(&self) -> Result<i32> {
        match &self.0 {
            MergeTermsEnum::Raw(t) => t.doc_count(),
            MergeTermsEnum::Sort(t) => t.doc_count(),
        }
    }

    fn has_freqs(&self) -> Result<bool> {
        match &self.0 {
            MergeTermsEnum::Raw(t) => t.has_freqs(),
            MergeTermsEnum::Sort(t) => t.has_freqs(),
        }
    }

    fn has_offsets(&self) -> Result<bool> {
        match &self.0 {
            MergeTermsEnum::Raw(t) => t.has_offsets(),
            MergeTermsEnum::Sort(t) => t.has_offsets(),
        }
    }

    fn has_positions(&self) -> Result<bool> {
        match &self.0 {
            MergeTermsEnum::Raw(t) => t.has_positions(),
            MergeTermsEnum::Sort(t) => t.has_positions(),
        }
    }

    fn has_payloads(&self) -> Result<bool> {
        match &self.0 {
            MergeTermsEnum::Raw(t) => t.has_payloads(),
            MergeTermsEnum::Sort(t) => t.has_payloads(),
        }
    }

    fn min(&self) -> Result<Option<Vec<u8>>> {
        match &self.0 {
            MergeTermsEnum::Raw(t) => t.min(),
            MergeTermsEnum::Sort(t) => t.min(),
        }
    }

    fn max(&self) -> Result<Option<Vec<u8>>> {
        match &self.0 {
            MergeTermsEnum::Raw(t) => t.max(),
            MergeTermsEnum::Sort(t) => t.max(),
        }
    }

    fn stats(&self) -> Result<String> {
        match &self.0 {
            MergeTermsEnum::Raw(t) => t.stats(),
            MergeTermsEnum::Sort(t) => t.stats(),
        }
    }
}

pub struct MergeTermIterator<T: TermIterator>(MergeTermIteratorEnum<T>);

impl<T: TermIterator> TermIterator for MergeTermIterator<T> {
    type Postings = SortingPostingIterEnum<T::Postings>;
    type TermState = T::TermState;

    fn next(&mut self) -> Result<Option<Vec<u8>>> {
        match &mut self.0 {
            MergeTermIteratorEnum::Raw(t) => t.next(),
            MergeTermIteratorEnum::Sorting(t) => t.next(),
        }
    }

    fn seek_exact(&mut self, text: &[u8]) -> Result<bool> {
        match &mut self.0 {
            MergeTermIteratorEnum::Raw(t) => t.seek_exact(text),
            MergeTermIteratorEnum::Sorting(t) => t.seek_exact(text),
        }
    }

    fn seek_ceil(&mut self, text: &[u8]) -> Result<SeekStatus> {
        match &mut self.0 {
            MergeTermIteratorEnum::Raw(t) => t.seek_ceil(text),
            MergeTermIteratorEnum::Sorting(t) => t.seek_ceil(text),
        }
    }

    fn seek_exact_ord(&mut self, ord: i64) -> Result<()> {
        match &mut self.0 {
            MergeTermIteratorEnum::Raw(t) => t.seek_exact_ord(ord),
            MergeTermIteratorEnum::Sorting(t) => t.seek_exact_ord(ord),
        }
    }

    fn seek_exact_state(&mut self, text: &[u8], state: &Self::TermState) -> Result<()> {
        match &mut self.0 {
            MergeTermIteratorEnum::Raw(t) => t.seek_exact_state(text, state),
            MergeTermIteratorEnum::Sorting(t) => t.seek_exact_state(text, state),
        }
    }

    fn term(&self) -> Result<&[u8]> {
        match &self.0 {
            MergeTermIteratorEnum::Raw(t) => t.term(),
            MergeTermIteratorEnum::Sorting(t) => t.term(),
        }
    }

    fn ord(&self) -> Result<i64> {
        match &self.0 {
            MergeTermIteratorEnum::Raw(t) => t.ord(),
            MergeTermIteratorEnum::Sorting(t) => t.ord(),
        }
    }

    fn doc_freq(&mut self) -> Result<i32> {
        match &mut self.0 {
            MergeTermIteratorEnum::Raw(t) => t.doc_freq(),
            MergeTermIteratorEnum::Sorting(t) => t.doc_freq(),
        }
    }

    fn total_term_freq(&mut self) -> Result<i64> {
        match &mut self.0 {
            MergeTermIteratorEnum::Raw(t) => t.total_term_freq(),
            MergeTermIteratorEnum::Sorting(t) => t.total_term_freq(),
        }
    }

    fn postings(&mut self) -> Result<Self::Postings> {
        match &mut self.0 {
            MergeTermIteratorEnum::Raw(t) => Ok(SortingPostingIterEnum::Raw(t.postings()?)),
            MergeTermIteratorEnum::Sorting(t) => t.postings(),
        }
    }

    fn postings_with_flags(&mut self, flags: u16) -> Result<Self::Postings> {
        match &mut self.0 {
            MergeTermIteratorEnum::Raw(t) => {
                Ok(SortingPostingIterEnum::Raw(t.postings_with_flags(flags)?))
            }
            MergeTermIteratorEnum::Sorting(t) => t.postings_with_flags(flags),
        }
    }

    fn term_state(&mut self) -> Result<Self::TermState> {
        match &mut self.0 {
            MergeTermIteratorEnum::Raw(t) => t.term_state(),
            MergeTermIteratorEnum::Sorting(t) => t.term_state(),
        }
    }

    fn is_empty(&self) -> bool {
        match &self.0 {
            MergeTermIteratorEnum::Raw(t) => t.is_empty(),
            MergeTermIteratorEnum::Sorting(t) => t.is_empty(),
        }
    }
}

enum MergeTermIteratorEnum<T: TermIterator> {
    Raw(T),
    Sorting(SortingTermsIterator<T>),
}

/// A map of doc IDs use for merge.
pub trait DocMap {
    /// Return the mapped doc_id or -1 if the given doc is not mapped
    fn get(&self, doc_id: DocId) -> Result<DocId>;
}

/// Only used by IW when it must remap deletes that arrived against the
/// merging segments while a merge was running:
pub struct MergeDocMap(DocMapEnum);

impl DocMap for MergeDocMap {
    fn get(&self, doc_id: i32) -> Result<DocId> {
        match &self.0 {
            DocMapEnum::Default(d) => d.get(doc_id),
            // DocMapEnum::LiveDocs(l) => l.get(doc_id),
            DocMapEnum::Sorted(s) => s.get(doc_id),
        }
    }
}

enum DocMapEnum {
    Default(DefaultDocMap),
    // LiveDocs(LiveDocsDocMap),
    Sorted(DocMapBySortDocMap),
}

#[derive(Default)]
pub struct DefaultDocMap;

impl DocMap for DefaultDocMap {
    fn get(&self, doc_id: DocId) -> Result<DocId> {
        Ok(doc_id)
    }
}

pub struct DocMapBySortDocMap {
    doc_map: Arc<PackedLongDocMap>,
}

impl DocMapBySortDocMap {
    pub fn new(doc_map: Arc<PackedLongDocMap>) -> Self {
        DocMapBySortDocMap { doc_map }
    }
}

impl DocMap for DocMapBySortDocMap {
    fn get(&self, doc_id: DocId) -> Result<DocId> {
        Ok(self.doc_map.old_to_new(doc_id))
    }
}

/// `DocMap` based on live docs `Bits`
pub struct LiveDocsDocMap {
    live_docs: BitsRef,
    del_docs: PackedLongValues,
    doc_base: DocId,
}

impl LiveDocsDocMap {
    pub fn new(live_docs: BitsRef, del_docs: PackedLongValues, doc_base: DocId) -> Self {
        LiveDocsDocMap {
            live_docs,
            del_docs,
            doc_base,
        }
    }
}

impl DocMap for LiveDocsDocMap {
    fn get(&self, doc_id: i32) -> Result<DocId> {
        if self.live_docs.get(doc_id as usize)? {
            Ok(self.doc_base + self.del_docs.get(doc_id)? as DocId)
        } else {
            Ok(-1)
        }
    }
}
