use core::codec::FieldsProducerRef;
use core::codec::{DocValuesProducer, NormsProducer};
use core::codec::{StoredFieldsReader, TermVectorsReader};
use core::index::leaf_reader_wrapper::{MergeReaderWrapper, SlowCodecReaderWrapper,
                                       SortingLeafReader};
use core::index::sorter::{MultiSorter, PackedLongDocMap, Sorter, SorterDocMap};
use core::index::BinaryDocValuesRef;
use core::index::SegmentInfo;
use core::index::SortedDocValuesRef;
use core::index::SortedNumericDocValuesRef;
use core::index::SortedSetDocValuesRef;
use core::index::StoredFieldVisitor;
use core::index::{FieldInfo, FieldInfos, Fields};
use core::index::{LeafReader, SegmentReader};
use core::index::{NumericDocValues, NumericDocValuesRef};
use core::index::{PointValues, PointValuesRef};
use core::search::sort::Sort;
use core::util::external::deferred::Deferred;
use core::util::packed::{PackedLongValuesBuilder, PackedLongValuesBuilderType, DEFAULT_PAGE_SIZE};
use core::util::packed_misc::COMPACT;
use core::util::{Bits, BitsRef, DocId};

use error::ErrorKind::IllegalArgument;
use error::Result;

use std::sync::Arc;

/// Holds common state used during segment merging.
pub struct MergeState {
    /// Maps document IDs from old segments to document IDs in the new segment
    pub doc_maps: Vec<Arc<LiveDocsDocMap>>,
    /// Only used by IW when it must remap deletes that arrived against the
    /// merging segments while a merge was running:
    pub leaf_doc_maps: Vec<DocMapEnum>,
    /// `SegmentInfo` of the newly merged segment.
    // WARN: pointer to OneMerge.info.info, must used carefully
    pub segment_info: *mut SegmentInfo,
    /// `FieldInfos` of the newly merged segment.
    pub merge_field_infos: Option<Arc<FieldInfos>>,
    pub stored_fields_readers: Vec<Box<StoredFieldsReader>>,
    pub term_vectors_readers: Vec<Option<Arc<TermVectorsReader>>>,
    pub norms_producers: Vec<Option<Arc<NormsProducer>>>,
    pub doc_values_producers: Vec<Option<Box<DocValuesProducer>>>,
    pub fields_infos: Vec<Arc<FieldInfos>>,
    pub live_docs: Vec<BitsRef>,
    pub fields_producers: Vec<FieldsProducerRef>,
    pub points_readers: Vec<Option<Arc<PointValues>>>,
    pub max_docs: Vec<i32>,
    /// Indicates if the index needs to be sorted
    pub needs_index_sort: bool,
}

impl MergeState {
    pub fn new(seg_readers: Vec<Arc<SegmentReader>>, segment_info: &SegmentInfo) -> Result<Self> {
        let num_readers = seg_readers.len();

        let mut leaf_doc_maps = Vec::with_capacity(num_readers);
        for _ in 0..seg_readers.len() {
            leaf_doc_maps.push(DocMapEnum::Default(DefaultDocMap::default()));
        }
        let mut needs_index_sort = false;
        let readers: Vec<ReaderWrapperEnum> = Self::maybe_sort_readers(
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
        let segment_info_ptr = segment_info as *const SegmentInfo as *mut SegmentInfo;
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

    pub fn segment_info(&self) -> &mut SegmentInfo {
        unsafe { &mut *self.segment_info }
    }

    fn maybe_sort_readers(
        seg_readers: Vec<Arc<SegmentReader>>,
        segment_info: &SegmentInfo,
        leaf_doc_maps: &mut Vec<DocMapEnum>,
        needs_index_sort: &mut bool,
    ) -> Result<Vec<ReaderWrapperEnum>> {
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
                    leaf_doc_maps[readers.len()] =
                        DocMapEnum::Sorted(DocMapBySortDocMap::new(Arc::clone(&doc_map_ref)));
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

    fn build_doc_maps(
        readers: &[ReaderWrapperEnum],
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
    fn build_deletion_doc_maps(readers: &[ReaderWrapperEnum]) -> Result<Vec<LiveDocsDocMap>> {
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

    fn remove_deletes(max_doc: i32, live_docs: &Bits) -> Result<PackedLongValuesBuilder> {
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
        doc_map_builder.build();
        Ok(doc_map_builder)
    }
}

pub enum ReaderWrapperEnum {
    Segment(Arc<SegmentReader>),
    SortedSegment(SlowCodecReaderWrapper),
}

impl LeafReader for ReaderWrapperEnum {
    fn fields(&self) -> Result<FieldsProducerRef> {
        match self {
            ReaderWrapperEnum::Segment(s) => s.fields(),
            ReaderWrapperEnum::SortedSegment(s) => s.fields(),
        }
    }

    fn name(&self) -> &str {
        match self {
            ReaderWrapperEnum::Segment(s) => s.name(),
            ReaderWrapperEnum::SortedSegment(s) => s.name(),
        }
    }

    fn term_vector(&self, doc_id: DocId) -> Result<Option<Box<Fields>>> {
        match self {
            ReaderWrapperEnum::Segment(s) => LeafReader::term_vector(s.as_ref(), doc_id),
            ReaderWrapperEnum::SortedSegment(s) => s.term_vector(doc_id),
        }
    }

    fn document(&self, doc_id: DocId, visitor: &mut StoredFieldVisitor) -> Result<()> {
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

    fn get_numeric_doc_values(&self, field: &str) -> Result<NumericDocValuesRef> {
        match self {
            ReaderWrapperEnum::Segment(s) => s.get_numeric_doc_values(field),
            ReaderWrapperEnum::SortedSegment(s) => s.get_numeric_doc_values(field),
        }
    }

    fn get_binary_doc_values(&self, field: &str) -> Result<BinaryDocValuesRef> {
        match self {
            ReaderWrapperEnum::Segment(s) => s.get_binary_doc_values(field),
            ReaderWrapperEnum::SortedSegment(s) => s.get_binary_doc_values(field),
        }
    }

    fn get_sorted_doc_values(&self, field: &str) -> Result<SortedDocValuesRef> {
        match self {
            ReaderWrapperEnum::Segment(s) => s.get_sorted_doc_values(field),
            ReaderWrapperEnum::SortedSegment(s) => s.get_sorted_doc_values(field),
        }
    }

    fn get_sorted_numeric_doc_values(&self, field: &str) -> Result<SortedNumericDocValuesRef> {
        match self {
            ReaderWrapperEnum::Segment(s) => s.get_sorted_numeric_doc_values(field),
            ReaderWrapperEnum::SortedSegment(s) => s.get_sorted_numeric_doc_values(field),
        }
    }

    fn get_sorted_set_doc_values(&self, field: &str) -> Result<SortedSetDocValuesRef> {
        match self {
            ReaderWrapperEnum::Segment(s) => s.get_sorted_set_doc_values(field),
            ReaderWrapperEnum::SortedSegment(s) => s.get_sorted_set_doc_values(field),
        }
    }

    fn norm_values(&self, field: &str) -> Result<Option<Box<NumericDocValues>>> {
        match self {
            ReaderWrapperEnum::Segment(s) => s.norm_values(field),
            ReaderWrapperEnum::SortedSegment(s) => s.norm_values(field),
        }
    }

    fn get_docs_with_field(&self, field: &str) -> Result<BitsRef> {
        match self {
            ReaderWrapperEnum::Segment(s) => s.get_docs_with_field(field),
            ReaderWrapperEnum::SortedSegment(s) => s.get_docs_with_field(field),
        }
    }

    /// Returns the `PointValuesRef` used for numeric or
    /// spatial searches, or None if there are no point fields.
    fn point_values(&self) -> Option<PointValuesRef> {
        match self {
            ReaderWrapperEnum::Segment(s) => s.point_values(),
            ReaderWrapperEnum::SortedSegment(s) => s.point_values(),
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
    fn store_fields_reader(&self) -> Result<Arc<StoredFieldsReader>> {
        match self {
            ReaderWrapperEnum::Segment(s) => s.store_fields_reader(),
            ReaderWrapperEnum::SortedSegment(s) => s.store_fields_reader(),
        }
    }

    fn term_vectors_reader(&self) -> Result<Option<Arc<TermVectorsReader>>> {
        match self {
            ReaderWrapperEnum::Segment(s) => s.term_vectors_reader(),
            ReaderWrapperEnum::SortedSegment(s) => s.term_vectors_reader(),
        }
    }

    fn norms_reader(&self) -> Result<Option<Arc<NormsProducer>>> {
        match self {
            ReaderWrapperEnum::Segment(s) => s.norms_reader(),
            ReaderWrapperEnum::SortedSegment(s) => s.norms_reader(),
        }
    }

    fn doc_values_reader(&self) -> Result<Option<Arc<DocValuesProducer>>> {
        match self {
            ReaderWrapperEnum::Segment(s) => s.doc_values_reader(),
            ReaderWrapperEnum::SortedSegment(s) => s.doc_values_reader(),
        }
    }

    fn postings_reader(&self) -> Result<FieldsProducerRef> {
        match self {
            ReaderWrapperEnum::Segment(s) => Ok(s.postings_reader()),
            ReaderWrapperEnum::SortedSegment(s) => s.postings_reader(),
        }
    }
}

pub trait DocMap {
    /// Return the mapped doc_id or -1 if the given doc is not mapped
    fn get(&self, doc_id: DocId) -> Result<DocId>;
}

pub enum DocMapEnum {
    Default(DefaultDocMap),
    LiveDocs(LiveDocsDocMap),
    Sorted(DocMapBySortDocMap),
}

impl DocMap for DocMapEnum {
    fn get(&self, doc_id: i32) -> Result<DocId> {
        match self {
            DocMapEnum::Default(d) => d.get(doc_id),
            DocMapEnum::LiveDocs(l) => l.get(doc_id),
            DocMapEnum::Sorted(s) => s.get(doc_id),
        }
    }
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

pub struct LiveDocsDocMap {
    live_docs: BitsRef,
    del_docs: PackedLongValuesBuilder,
    doc_base: DocId,
}

impl LiveDocsDocMap {
    pub fn new(live_docs: BitsRef, del_docs: PackedLongValuesBuilder, doc_base: DocId) -> Self {
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
