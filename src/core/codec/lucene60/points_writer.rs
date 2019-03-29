use core::codec::lucene60::points_reader::{DATA_CODEC_NAME, DATA_EXTENSION, DATA_VERSION_CURRENT,
                                           INDEX_EXTENSION, INDEX_VERSION_CURRENT, META_CODEC_NAME};
use core::codec::BoxedPointsReader;
use core::codec::Lucene60PointsReader;
use core::codec::{merge_point_values, PointsWriter};
use core::codec::{write_footer, write_index_header};
use core::index::segment_file_name;
use core::index::FieldInfo;
use core::index::LiveDocsDocMap;
use core::index::{IntersectVisitor, PointValues, Relation};
use core::index::{MergeState, SegmentWriteState};
use core::store::IndexOutput;
use core::util::bkd::{BKDWriter, DEFAULT_MAX_MB_SORT_IN_HEAP, DEFAULT_MAX_POINTS_IN_LEAF_NODE};
use core::util::DocId;

use error::Result;
use std::collections::btree_map::BTreeMap;
use std::sync::Arc;

// Writes dimensional values
pub struct Lucene60PointsWriter {
    // Output used to write the BKD tree data file
    data_out: Box<IndexOutput>,
    // Maps field name to file pointer in the data file where the BKD index is located.
    index_fps: BTreeMap<String, i64>,
    write_state: SegmentWriteState,
    max_points_in_leaf_node: i32,
    max_mb_sort_in_heap: f64,
    finished: bool,
}

impl Lucene60PointsWriter {
    pub fn new(write_state: &SegmentWriteState) -> Result<Lucene60PointsWriter> {
        let write_state = write_state.clone();
        debug_assert!(write_state.field_infos.has_doc_values);
        let data_file_name = segment_file_name(
            &write_state.segment_info.name,
            &write_state.segment_suffix,
            DATA_EXTENSION,
        );
        let mut data_out = write_state
            .directory
            .create_output(&data_file_name, &write_state.context)?;
        write_index_header(
            data_out.as_mut(),
            DATA_CODEC_NAME,
            DATA_VERSION_CURRENT,
            write_state.segment_info.get_id(),
            &write_state.segment_suffix,
        )?;

        Ok(Lucene60PointsWriter {
            data_out,
            index_fps: BTreeMap::new(),
            write_state,
            max_points_in_leaf_node: DEFAULT_MAX_POINTS_IN_LEAF_NODE,
            max_mb_sort_in_heap: DEFAULT_MAX_MB_SORT_IN_HEAP as f64,
            finished: false,
        })
    }
}

pub struct ValuesIntersectVisitor<'a> {
    writer: &'a mut BKDWriter,
}

impl<'a> ValuesIntersectVisitor<'a> {
    pub fn new(writer: &'a mut BKDWriter) -> ValuesIntersectVisitor<'a> {
        ValuesIntersectVisitor { writer }
    }
}

impl<'a> IntersectVisitor for ValuesIntersectVisitor<'a> {
    fn visit(&mut self, _doc_id: DocId) -> Result<()> {
        bail!("IllegalStateException");
    }

    fn visit_by_packed_value(&mut self, doc_id: DocId, packed_value: &[u8]) -> Result<()> {
        self.writer.add(packed_value, doc_id)
    }

    fn compare(&self, _min_packed_value: &[u8], _max_packed_value: &[u8]) -> Relation {
        Relation::CellCrossesQuery
    }
}

impl PointsWriter for Lucene60PointsWriter {
    fn write_field(&mut self, field_info: &FieldInfo, values: BoxedPointsReader) -> Result<()> {
        let single_value_per_doc =
            values.size(&field_info.name)? == values.doc_count(&field_info.name)? as i64;
        let mut writer = BKDWriter::new(
            self.write_state.segment_info.max_doc(),
            Arc::clone(&self.write_state.directory),
            &self.write_state.segment_info.name,
            field_info.point_dimension_count as usize,
            field_info.point_num_bytes as usize,
            self.max_points_in_leaf_node,
            self.max_mb_sort_in_heap,
            values.size(&field_info.name)?,
            single_value_per_doc,
        )?;

        match values {
            BoxedPointsReader::Mutable(s) => {
                let fp = writer.write_field(self.data_out.as_mut(), &field_info.name, s)?;
                if fp != -1 {
                    self.index_fps.insert(field_info.name.clone(), fp);
                }
            }
            BoxedPointsReader::Simple(m) => {
                {
                    let mut visitor = ValuesIntersectVisitor::new(&mut writer);
                    m.intersect(&field_info.name, &mut visitor)?;
                }
                // We could have 0 points on merge since all docs with dimensional fields may be
                // deleted:
                if writer.point_count > 0 {
                    let fp = writer.finish(self.data_out.as_mut())?;
                    self.index_fps.insert(field_info.name.clone(), fp);
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, merge_state: &MergeState) -> Result<()> {
        if merge_state.needs_index_sort {
            // TODO: can we gain back some optos even if index is sorted?
            // E.g. if sort results in large chunks of contiguous docs from one sub
            // being copied over...?
            return merge_point_values(self, merge_state);
        }

        for reader_opt in &merge_state.points_readers {
            if let Some(reader) = reader_opt {
                if !reader.as_any().is::<Lucene60PointsReader>() {
                    // We can only bulk merge when all to-be-merged segments use our format:
                    return merge_point_values(self, merge_state);
                }
            }
        }

        for field_info in merge_state
            .merge_field_infos
            .as_ref()
            .unwrap()
            .by_number
            .values()
        {
            if field_info.point_dimension_count > 0 {
                if field_info.point_dimension_count == 1 {
                    let mut single_value_per_doc = false;
                    // Worst case total maximum size (if none of the points are deleted):
                    let mut total_max_size = 0;
                    for i in 0..merge_state.points_readers.len() {
                        if let Some(reader) = &merge_state.points_readers[i] {
                            if let Some(reader_field_info) =
                                merge_state.fields_infos[i].field_info_by_name(&field_info.name)
                            {
                                if reader_field_info.point_dimension_count > 0 {
                                    let max_size = reader.size(&field_info.name)?;
                                    total_max_size += max_size;
                                    single_value_per_doc &=
                                        max_size == reader.doc_count(&field_info.name)? as i64;
                                }
                            }
                        }
                    }

                    // Optimize the 1D case to use BKDWriter.merge, which does a single merge sort
                    // of the already sorted incoming segments, instead of
                    // trying to sort all points again as if we were simply
                    // reindexing them:
                    let mut writer: BKDWriter = BKDWriter::new(
                        self.write_state.segment_info.max_doc,
                        Arc::clone(&self.write_state.directory),
                        &self.write_state.segment_info.name,
                        field_info.point_dimension_count as usize,
                        field_info.point_num_bytes as usize,
                        self.max_points_in_leaf_node,
                        self.max_mb_sort_in_heap,
                        total_max_size,
                        single_value_per_doc,
                    )?;
                    let mut bkd_readers = vec![];
                    let mut doc_maps: Vec<&LiveDocsDocMap> = vec![];
                    for i in 0..merge_state.points_readers.len() {
                        if let Some(reader) = &merge_state.points_readers[i] {
                            // we confirmed this up above
                            let reader60: &Lucene60PointsReader = reader
                                .as_any()
                                .downcast_ref::<Lucene60PointsReader>()
                                .unwrap();

                            // NOTE: we cannot just use the merged fieldInfo.number (instead of
                            // resolving to this reader's FieldInfo as
                            // we do below) because field numbers can easily be different
                            // when addIndexes(Directory...) copies over segments from another
                            // index:
                            if let Some(reader_field_info) =
                                merge_state.fields_infos[i].field_info_by_name(&field_info.name)
                            {
                                if reader_field_info.point_dimension_count > 0 {
                                    if let Some(bkd_reader) =
                                        reader60.readers.get(&(reader_field_info.number as i32))
                                    {
                                        bkd_readers.push(bkd_reader);
                                        doc_maps.push(merge_state.doc_maps[i].as_ref());
                                    }
                                }
                            }
                        }
                    }

                    let fp = writer.merge(self.data_out.as_mut(), doc_maps, bkd_readers)?;
                    if fp != -1 {
                        self.index_fps.insert(field_info.name.clone(), fp);
                    }
                } else {
                    self.merge_one_field(merge_state, field_info)?;
                }
            }
        }

        self.finish()
    }

    fn finish(&mut self) -> Result<()> {
        if self.finished {
            bail!("Lucene60PointsWriter already finished");
        }

        self.finished = true;
        write_footer(self.data_out.as_mut())?;
        let index_file_name = segment_file_name(
            &self.write_state.segment_info.name,
            &self.write_state.segment_suffix,
            INDEX_EXTENSION,
        );
        let mut index_output = self
            .write_state
            .directory
            .create_output(&index_file_name, &self.write_state.context)?;
        write_index_header(
            index_output.as_mut(),
            META_CODEC_NAME,
            INDEX_VERSION_CURRENT,
            self.write_state.segment_info.get_id(),
            &self.write_state.segment_suffix,
        )?;

        let count = self.index_fps.len();
        index_output.write_vint(count as i32)?;
        for (key, value) in &self.index_fps {
            if let Some(field_info) = self.write_state.field_infos.field_info_by_name(key) {
                index_output.write_vint(field_info.number as i32)?;
                index_output.write_vlong(*value)?;
            } else {
                bail!(
                    "wrote field={}, but that field doesn't exist in FieldInfos",
                    key
                );
            }
        }

        write_footer(index_output.as_mut())
    }
}
