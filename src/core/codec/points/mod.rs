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

mod points_reader;

pub use self::points_reader::*;

mod points_writer;

pub use self::points_writer::*;

mod point_values_writer;

pub use self::point_values_writer::*;

mod point_values;

pub use self::point_values::*;

use core::util::DocId;

use core::codec::field_infos::{FieldInfo, FieldInfos};
use core::codec::segment_infos::{SegmentReadState, SegmentWriteState};
use core::codec::{Codec, CodecPointsReader};
use core::index::merge::DocMap;
use core::index::merge::{LiveDocsDocMap, MergePointValuesEnum, MergeState};
use core::store::directory::Directory;

use error::ErrorKind::{IllegalArgument, IllegalState, UnsupportedOperation};
use error::Result;
use std::any::Any;
use std::borrow::Cow;
use std::sync::Arc;

/// trait for visit point values.
pub trait PointsReader: PointValues {
    fn check_integrity(&self) -> Result<()>;
    fn as_any(&self) -> &dyn Any;
}

/// `PointsReader` whose order of points can be changed.
///
/// This trait is useful for codecs to optimize flush.
pub trait MutablePointsReader: PointsReader {
    fn value(&self, i: i32, packed_value: &mut Vec<u8>);
    fn byte_at(&self, i: i32, k: i32) -> u8;
    fn doc_id(&self, i: i32) -> DocId;
    fn swap(&mut self, i: i32, j: i32);
    fn clone(&self) -> Self;
}

/// Encodes/decodes indexed points.
/// @lucene.experimental */
pub trait PointsFormat {
    //    type Writer<D, DW>: PointsWriter<D, DW>;
    type Reader: PointValues;

    /// Writes a new segment
    //    fn fields_writer<D: Directory, DW: Directory>(
    //        &self,
    //        state: &SegmentWriteState<D, DW, C>
    //    ) -> Result<Self::Writer<D, DW>>;
    // TODO we need GAT to make this interface possible
    fn fields_writer<D: Directory, DW: Directory, C: Codec>(
        &self,
        state: &SegmentWriteState<D, DW, C>,
    ) -> Result<Lucene60PointsWriter<D, DW, C>>;

    /// Reads a segment.  NOTE: by the time this call
    /// returns, it must hold open any files it will need to
    /// use; else, those files may be deleted.
    /// Additionally, required files may be deleted during the execution of
    /// this call before there is a chance to open them. Under these
    /// circumstances an IOException should be thrown by the implementation.
    /// IOExceptions are expected and will automatically cause a retry of the
    /// segment opening logic with the newly revised segments.
    fn fields_reader<D: Directory, DW: Directory, C: Codec>(
        &self,
        state: &SegmentReadState<'_, D, DW, C>,
    ) -> Result<Self::Reader>;
}

pub enum PointsReaderEnum<P: PointsReader, MP: MutablePointsReader> {
    Simple(P),
    Mutable(MP),
}

impl<P: PointsReader, MP: MutablePointsReader> PointsReader for PointsReaderEnum<P, MP> {
    fn check_integrity(&self) -> Result<()> {
        match self {
            PointsReaderEnum::Simple(s) => s.check_integrity(),
            PointsReaderEnum::Mutable(m) => m.check_integrity(),
        }
    }

    fn as_any(&self) -> &dyn Any {
        match self {
            PointsReaderEnum::Simple(s) => PointsReader::as_any(s),
            PointsReaderEnum::Mutable(m) => PointsReader::as_any(m),
        }
    }
}

impl<P: PointsReader, MP: MutablePointsReader> PointValues for PointsReaderEnum<P, MP> {
    fn intersect(&self, field_name: &str, visitor: &mut impl IntersectVisitor) -> Result<()> {
        match self {
            PointsReaderEnum::Simple(s) => s.intersect(field_name, visitor),
            PointsReaderEnum::Mutable(m) => m.intersect(field_name, visitor),
        }
    }

    fn min_packed_value(&self, field_name: &str) -> Result<Vec<u8>> {
        match self {
            PointsReaderEnum::Simple(s) => s.min_packed_value(field_name),
            PointsReaderEnum::Mutable(m) => m.min_packed_value(field_name),
        }
    }

    fn max_packed_value(&self, field_name: &str) -> Result<Vec<u8>> {
        match self {
            PointsReaderEnum::Simple(s) => s.max_packed_value(field_name),
            PointsReaderEnum::Mutable(m) => m.max_packed_value(field_name),
        }
    }

    fn num_dimensions(&self, field_name: &str) -> Result<usize> {
        match self {
            PointsReaderEnum::Simple(s) => s.num_dimensions(field_name),
            PointsReaderEnum::Mutable(m) => m.num_dimensions(field_name),
        }
    }

    fn bytes_per_dimension(&self, field_name: &str) -> Result<usize> {
        match self {
            PointsReaderEnum::Simple(s) => s.bytes_per_dimension(field_name),
            PointsReaderEnum::Mutable(m) => m.bytes_per_dimension(field_name),
        }
    }

    fn size(&self, field_name: &str) -> Result<i64> {
        match self {
            PointsReaderEnum::Simple(s) => s.size(field_name),
            PointsReaderEnum::Mutable(m) => m.size(field_name),
        }
    }

    fn doc_count(&self, field_name: &str) -> Result<i32> {
        match self {
            PointsReaderEnum::Simple(s) => s.doc_count(field_name),
            PointsReaderEnum::Mutable(m) => m.doc_count(field_name),
        }
    }

    fn as_any(&self) -> &dyn Any {
        match self {
            PointsReaderEnum::Simple(s) => PointValues::as_any(s),
            PointsReaderEnum::Mutable(m) => PointValues::as_any(m),
        }
    }
}

pub trait PointsWriter {
    /// Write all values contained in the provided reader
    fn write_field<P, MP>(
        &mut self,
        field_info: &FieldInfo,
        values: PointsReaderEnum<P, MP>,
    ) -> Result<()>
    where
        P: PointsReader,
        MP: MutablePointsReader;

    /// Default naive merge implementation for one field: it just re-indexes all the values
    /// from the incoming segment.  The default codec overrides this for 1D fields and uses
    /// a faster but more complex implementation.
    fn merge_one_field<D: Directory, C: Codec>(
        &mut self,
        merge_state: &MergeState<D, C>,
        field_info: &FieldInfo,
    ) -> Result<()> {
        let mut max_point_count = 0;
        let mut doc_count = 0;
        let mut i = 0;
        for reader_opt in &merge_state.points_readers {
            if let Some(reader) = reader_opt {
                if let Some(reader_field_info) =
                    merge_state.fields_infos[i].field_info_by_name(&field_info.name)
                {
                    if reader_field_info.point_dimension_count > 0 {
                        max_point_count += reader.size(&field_info.name)?;
                        doc_count += reader.doc_count(&field_info.name)?;
                    }
                }
            }
            i += 1;
        }

        let reader: PointsReaderEnum<MergePointsReader<C>, TempMutablePointsReader> =
            PointsReaderEnum::Simple(MergePointsReader::new(
                field_info.clone(),
                merge_state,
                max_point_count,
                doc_count,
            ));
        self.write_field(field_info, reader)
    }

    /// Default merge implementation to merge incoming points readers by visiting all their points
    /// and adding to this writer
    fn merge<D: Directory, C: Codec>(&mut self, merge_state: &MergeState<D, C>) -> Result<()>;

    /// Called once at the end before close
    fn finish(&mut self) -> Result<()>;
}

pub fn merge_point_values<D: Directory, C: Codec, P: PointsWriter>(
    writer: &mut P,
    merge_state: &MergeState<D, C>,
) -> Result<()> {
    // merge field at a time
    for field_info in merge_state
        .merge_field_infos
        .as_ref()
        .unwrap()
        .by_number
        .values()
    {
        if field_info.point_dimension_count > 0 {
            writer.merge_one_field(merge_state, field_info.as_ref())?;
        }
    }
    writer.finish()
}

pub struct MergePointsReader<C: Codec> {
    field_info: FieldInfo,
    points_readers: Vec<Option<MergePointValuesEnum<Arc<CodecPointsReader<C>>>>>,
    fields_infos: Vec<Arc<FieldInfos>>,
    doc_maps: Vec<Arc<LiveDocsDocMap>>,
    max_point_count: i64,
    doc_count: i32,
}

impl<C: Codec> MergePointsReader<C> {
    fn new<D: Directory>(
        field_info: FieldInfo,
        merge_state: &MergeState<D, C>,
        max_point_count: i64,
        doc_count: i32,
    ) -> Self {
        MergePointsReader {
            field_info,
            points_readers: merge_state.points_readers.clone(),
            fields_infos: merge_state.fields_infos.clone(),
            doc_maps: merge_state.doc_maps.clone(),
            max_point_count,
            doc_count,
        }
    }
}

impl<C: Codec> PointsReader for MergePointsReader<C> {
    fn check_integrity(&self) -> Result<()> {
        bail!(UnsupportedOperation(Cow::Borrowed("")))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl<C: Codec> PointValues for MergePointsReader<C> {
    fn intersect(&self, field_name: &str, visitor: &mut impl IntersectVisitor) -> Result<()> {
        if field_name != self.field_info.name {
            bail!(IllegalArgument(
                "field name must match the field being merged".into()
            ));
        }

        for i in 0..self.points_readers.len() {
            if let Some(reader) = &self.points_readers[i] {
                if let Some(reader_field_info) = self.fields_infos[i].field_info_by_name(field_name)
                {
                    if reader_field_info.point_dimension_count == 0 {
                        continue;
                    }

                    reader.intersect(
                        &self.field_info.name,
                        &mut MergeIntersectVisitorWrapper {
                            visitor,
                            doc_map: self.doc_maps[i].as_ref(),
                        },
                    )?;
                }
            }
        }
        Ok(())
    }

    fn min_packed_value(&self, _field_name: &str) -> Result<Vec<u8>> {
        bail!(UnsupportedOperation(Cow::Borrowed("")))
    }

    fn max_packed_value(&self, _field_name: &str) -> Result<Vec<u8>> {
        bail!(UnsupportedOperation(Cow::Borrowed("")))
    }

    fn num_dimensions(&self, _field_name: &str) -> Result<usize> {
        bail!(UnsupportedOperation(Cow::Borrowed("")))
    }

    fn bytes_per_dimension(&self, _field_name: &str) -> Result<usize> {
        bail!(UnsupportedOperation(Cow::Borrowed("")))
    }

    fn size(&self, _field_name: &str) -> Result<i64> {
        Ok(self.max_point_count)
    }

    fn doc_count(&self, _field_name: &str) -> Result<i32> {
        Ok(self.doc_count)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

struct MergeIntersectVisitorWrapper<'a, V: IntersectVisitor> {
    visitor: &'a mut V,
    doc_map: &'a LiveDocsDocMap,
}

impl<'a, V: IntersectVisitor + 'a> IntersectVisitor for MergeIntersectVisitorWrapper<'a, V> {
    fn visit(&mut self, _doc_id: DocId) -> Result<()> {
        // Should never be called because our compare method never returns
        // Relation.CELL_INSIDE_QUERY
        bail!(IllegalState("".into()))
    }

    fn visit_by_packed_value(&mut self, doc_id: DocId, packed_value: &[u8]) -> Result<()> {
        let new_doc_id = self.doc_map.get(doc_id)?;
        if new_doc_id != -1 {
            // not deleted
            self.visitor.visit_by_packed_value(new_doc_id, packed_value)
        } else {
            Ok(())
        }
    }

    fn compare(&self, _min_packed_value: &[u8], _max_packed_value: &[u8]) -> Relation {
        // Forces this segment's PointsReader to always visit all docs + values:
        Relation::CellCrossesQuery
    }
}
