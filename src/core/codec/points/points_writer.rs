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

use core::codec::field_infos::FieldInfo;
use core::codec::points::points_reader::{
    DATA_CODEC_NAME, DATA_EXTENSION, DATA_VERSION_CURRENT, INDEX_EXTENSION, INDEX_VERSION_CURRENT,
    META_CODEC_NAME,
};
use core::codec::points::Lucene60PointsReader;
use core::codec::points::{
    merge_point_values, IntersectVisitor, MutablePointsReader, PointValues, PointsReader,
    PointsReaderEnum, PointsWriter, Relation,
};
use core::codec::segment_infos::{segment_file_name, SegmentWriteState};
use core::codec::{codec_util, Codec};
use core::index::merge::{LiveDocsDocMap, MergeState};
use core::store::directory::Directory;
use core::store::io::DataOutput;
use core::util::bkd::{BKDWriter, DEFAULT_MAX_MB_SORT_IN_HEAP, DEFAULT_MAX_POINTS_IN_LEAF_NODE};
use core::util::DocId;

use error::{ErrorKind::IllegalState, Result};
use std::collections::btree_map::BTreeMap;
use std::sync::Arc;

// Writes dimensional values
pub struct Lucene60PointsWriter<D: Directory, DW: Directory, C: Codec> {
    // Output used to write the BKD tree data file
    data_out: DW::IndexOutput,
    // Maps field name to file pointer in the data file where the BKD index is located.
    index_fps: BTreeMap<String, i64>,
    write_state: SegmentWriteState<D, DW, C>,
    max_points_in_leaf_node: i32,
    max_mb_sort_in_heap: f64,
    finished: bool,
}

impl<D: Directory, DW: Directory, C: Codec> Lucene60PointsWriter<D, DW, C> {
    pub fn new(
        write_state: &SegmentWriteState<D, DW, C>,
    ) -> Result<Lucene60PointsWriter<D, DW, C>> {
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
        codec_util::write_index_header(
            &mut data_out,
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

impl<D: Directory, DW: Directory, C: Codec> PointsWriter for Lucene60PointsWriter<D, DW, C> {
    fn write_field<P, MP>(
        &mut self,
        field_info: &FieldInfo,
        values: PointsReaderEnum<P, MP>,
    ) -> Result<()>
    where
        P: PointsReader,
        MP: MutablePointsReader,
    {
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
            PointsReaderEnum::Mutable(s) => {
                let fp = writer.write_field(&mut self.data_out, &field_info.name, s)?;
                if fp != -1 {
                    self.index_fps.insert(field_info.name.clone(), fp);
                }
            }
            PointsReaderEnum::Simple(m) => {
                {
                    let mut visitor = ValuesIntersectVisitor::new(&mut writer);
                    m.intersect(&field_info.name, &mut visitor)?;
                }
                // We could have 0 points on merge since all docs with dimensional fields may be
                // deleted:
                if writer.point_count > 0 {
                    let fp = writer.finish(&mut self.data_out)?;
                    self.index_fps.insert(field_info.name.clone(), fp);
                }
            }
        }
        Ok(())
    }

    fn merge<D1: Directory, C1: Codec>(&mut self, merge_state: &MergeState<D1, C1>) -> Result<()> {
        // If indexSort is activated and some of the leaves are not sorted the next test will catch
        // that and the non-optimized merge will run. If the readers are all sorted then
        // it's safe to perform a bulk merge of the points.

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
                    let mut writer = BKDWriter::new(
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

                    let fp = writer.merge(&mut self.data_out, doc_maps, bkd_readers)?;
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
            bail!(IllegalState("already finished".into()));
        }

        self.finished = true;
        codec_util::write_footer(&mut self.data_out)?;
        let index_file_name = segment_file_name(
            &self.write_state.segment_info.name,
            &self.write_state.segment_suffix,
            INDEX_EXTENSION,
        );
        let mut index_output = self
            .write_state
            .directory
            .create_output(&index_file_name, &self.write_state.context)?;
        codec_util::write_index_header(
            &mut index_output,
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

        codec_util::write_footer(&mut index_output)
    }
}

struct ValuesIntersectVisitor<'a, D: Directory> {
    writer: &'a mut BKDWriter<D>,
}

impl<'a, D: Directory> ValuesIntersectVisitor<'a, D> {
    pub fn new(writer: &'a mut BKDWriter<D>) -> ValuesIntersectVisitor<'a, D> {
        ValuesIntersectVisitor { writer }
    }
}

impl<'a, D: Directory> IntersectVisitor for ValuesIntersectVisitor<'a, D> {
    fn visit(&mut self, _doc_id: DocId) -> Result<()> {
        bail!(IllegalState("".into()))
    }

    fn visit_by_packed_value(&mut self, doc_id: DocId, packed_value: &[u8]) -> Result<()> {
        self.writer.add(packed_value, doc_id)
    }

    fn compare(&self, _min_packed_value: &[u8], _max_packed_value: &[u8]) -> Relation {
        Relation::CellCrossesQuery
    }
}
