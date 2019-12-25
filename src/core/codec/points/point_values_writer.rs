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
use core::codec::points::{
    IntersectVisitor, MergePointsReader, MutablePointsReader, PointValues, PointsReader,
    PointsReaderEnum, PointsWriter, Relation,
};
use core::codec::segment_infos::SegmentWriteState;
use core::codec::{Codec, SorterDocMap};
use core::index::merge::MergePolicy;
use core::index::merge::MergeScheduler;
use core::index::writer::DocumentsWriterPerThread;
use core::store::directory::Directory;
use core::util::{ByteBlockAllocator, ByteBlockPool};
use core::util::{BytesRef, DocId};

use error::Result;

use std::any::Any;

pub struct PointValuesWriter {
    field_info: FieldInfo,
    bytes: ByteBlockPool,
    doc_ids: Vec<DocId>,
    num_points: usize,
    num_docs: usize,
    last_doc_id: DocId,
    packed_bytes_length: usize,
}

impl PointValuesWriter {
    pub fn new<D, C, MS, MP>(
        doc_writer: &mut DocumentsWriterPerThread<D, C, MS, MP>,
        field_info: &FieldInfo,
    ) -> PointValuesWriter
    where
        D: Directory + Send + Sync + 'static,
        C: Codec,
        MS: MergeScheduler,
        MP: MergePolicy,
    {
        let bytes = ByteBlockPool::new(doc_writer.byte_block_allocator.shallow_copy());
        PointValuesWriter {
            field_info: field_info.clone(),
            bytes,
            doc_ids: vec![],
            num_points: 0,
            num_docs: 0,
            last_doc_id: -1,
            packed_bytes_length: (field_info.point_dimension_count * field_info.point_num_bytes)
                as usize,
        }
    }

    pub fn add_packed_value(&mut self, doc_id: DocId, value: &BytesRef) -> Result<()> {
        if value.is_empty() {
            bail!(
                "field={}: point value must not be null",
                self.field_info.name
            );
        }
        if value.len() != self.packed_bytes_length {
            bail!(
                "field={}: this field's value has length={} but should be {}",
                self.field_info.name,
                value.len(),
                self.packed_bytes_length
            );
        }

        self.bytes.append(value);
        self.doc_ids.push(doc_id);
        self.num_points += 1;
        if doc_id != self.last_doc_id {
            self.num_docs += 1;
            self.last_doc_id = doc_id;
        }

        Ok(())
    }

    pub fn flush<
        D: Directory,
        C: Codec,
        DW: Directory,
        W: PointsWriter,
        M: SorterDocMap + 'static,
    >(
        &mut self,
        _state: &SegmentWriteState<D, DW, C>,
        sort_map: Option<&M>,
        writer: &mut W,
    ) -> Result<()> {
        let points_reader = TempMutablePointsReader::new(self);
        if let Some(sort_map) = sort_map {
            let reader: PointsReaderEnum<
                MergePointsReader<C>,
                MutableSortingPointValues<TempMutablePointsReader, M>,
            > = PointsReaderEnum::Mutable(MutableSortingPointValues::new(points_reader, sort_map));
            writer.write_field(&self.field_info, reader)
        } else {
            let reader: PointsReaderEnum<MergePointsReader<C>, TempMutablePointsReader> =
                PointsReaderEnum::Mutable(points_reader);
            writer.write_field(&self.field_info, reader)
        }
    }
}

pub struct TempMutablePointsReader {
    point_values_writer: *const PointValuesWriter,
    ords: Vec<usize>,
}

impl TempMutablePointsReader {
    pub fn new(point_values_writer: &PointValuesWriter) -> TempMutablePointsReader {
        let ords: Vec<usize> = (0..point_values_writer.num_points).collect();

        TempMutablePointsReader {
            point_values_writer,
            ords,
        }
    }

    #[inline]
    pub fn point_values_writer(&self) -> &PointValuesWriter {
        unsafe { &*self.point_values_writer }
    }
}

impl PointValues for TempMutablePointsReader {
    fn intersect(&self, field_name: &str, visitor: &mut impl IntersectVisitor) -> Result<()> {
        let point_values_writer = self.point_values_writer();

        if field_name != point_values_writer.field_info.name {
            bail!(
                "fieldName must be the same, got: {}, expected: {}",
                field_name,
                point_values_writer.field_info.name
            );
        }

        let mut packed_value = vec![0u8; point_values_writer.packed_bytes_length];
        for i in 0..point_values_writer.num_points {
            self.value(i as i32, &mut packed_value);
            visitor.visit_by_packed_value(self.doc_id(i as i32), &packed_value)?;
        }

        Ok(())
    }

    fn min_packed_value(&self, _field_name: &str) -> Result<Vec<u8>> {
        unimplemented!()
    }

    fn max_packed_value(&self, _field_name: &str) -> Result<Vec<u8>> {
        unimplemented!()
    }

    fn num_dimensions(&self, _field_name: &str) -> Result<usize> {
        unimplemented!()
    }

    fn bytes_per_dimension(&self, _field_name: &str) -> Result<usize> {
        unimplemented!()
    }

    fn size(&self, field_name: &str) -> Result<i64> {
        if field_name != self.point_values_writer().field_info.name {
            bail!(
                "fieldName must be the same, got: {}, expected: {}",
                field_name,
                self.point_values_writer().field_info.name
            );
        }

        Ok(self.point_values_writer().num_points as i64)
    }

    fn doc_count(&self, field_name: &str) -> Result<i32> {
        if field_name != self.point_values_writer().field_info.name {
            bail!(
                "fieldName must be the same, got: {}, expected: {}",
                field_name,
                self.point_values_writer().field_info.name
            );
        }

        Ok(self.point_values_writer().num_docs as i32)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl PointsReader for TempMutablePointsReader {
    fn check_integrity(&self) -> Result<()> {
        unimplemented!()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl MutablePointsReader for TempMutablePointsReader {
    fn value(&self, i: i32, packed_value: &mut Vec<u8>) {
        let point_values_writer = self.point_values_writer();

        let offset = point_values_writer.packed_bytes_length * self.ords[i as usize];
        packed_value.resize(point_values_writer.packed_bytes_length, 0);
        point_values_writer
            .bytes
            .set_raw_bytes_ref(packed_value.as_mut(), offset);
    }

    fn byte_at(&self, i: i32, k: i32) -> u8 {
        let offset =
            self.point_values_writer().packed_bytes_length * self.ords[i as usize] + k as usize;
        self.point_values_writer().bytes.read_byte(offset)
    }

    fn doc_id(&self, i: i32) -> DocId {
        self.point_values_writer().doc_ids[self.ords[i as usize]]
    }

    fn swap(&mut self, i: i32, j: i32) {
        self.ords.swap(i as usize, j as usize);
    }

    fn clone(&self) -> Self {
        TempMutablePointsReader {
            point_values_writer: self.point_values_writer,
            ords: self.ords.clone(),
        }
    }
}

pub struct MutableSortingPointValues<PV: MutablePointsReader, M: SorterDocMap> {
    point_values: PV,
    doc_map: *const M,
}

impl<PR, M> MutableSortingPointValues<PR, M>
where
    PR: MutablePointsReader,
    M: SorterDocMap,
{
    pub fn new(point_values: PR, doc_map: &M) -> Self {
        Self {
            point_values,
            doc_map,
        }
    }
}

impl<PR, M> PointValues for MutableSortingPointValues<PR, M>
where
    PR: MutablePointsReader + 'static,
    M: SorterDocMap + 'static,
{
    fn intersect(&self, field_name: &str, visitor: &mut impl IntersectVisitor) -> Result<()> {
        let doc_map = unsafe { &*self.doc_map };
        let mut v = SorterIntersectVisitor { visitor, doc_map };
        self.point_values.intersect(field_name, &mut v)
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

impl<PR, M> PointsReader for MutableSortingPointValues<PR, M>
where
    PR: MutablePointsReader + 'static,
    M: SorterDocMap + 'static,
{
    fn check_integrity(&self) -> Result<()> {
        self.point_values.check_integrity()
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl<PR, M> MutablePointsReader for MutableSortingPointValues<PR, M>
where
    PR: MutablePointsReader + 'static,
    M: SorterDocMap + 'static,
{
    fn value(&self, i: i32, packed_value: &mut Vec<u8>) {
        self.point_values.value(i, packed_value)
    }
    fn byte_at(&self, i: i32, k: i32) -> u8 {
        self.point_values.byte_at(i, k)
    }
    fn doc_id(&self, i: i32) -> DocId {
        let doc = self.point_values.doc_id(i);
        unsafe { (*self.doc_map).old_to_new(doc) }
    }
    fn swap(&mut self, i: i32, j: i32) {
        self.point_values.swap(i, j)
    }
    fn clone(&self) -> Self {
        Self {
            point_values: self.point_values.clone(),
            doc_map: self.doc_map,
        }
    }
}

struct SorterIntersectVisitor<'a, IV: IntersectVisitor + 'a, M: SorterDocMap + 'a> {
    visitor: &'a mut IV,
    doc_map: &'a M,
}

impl<'a, IV: IntersectVisitor + 'a, M: SorterDocMap + 'a> IntersectVisitor
    for SorterIntersectVisitor<'a, IV, M>
{
    fn visit(&mut self, doc_id: i32) -> Result<()> {
        self.visitor.visit(self.doc_map.old_to_new(doc_id))
    }

    fn visit_by_packed_value(&mut self, doc_id: i32, packed_value: &[u8]) -> Result<()> {
        self.visitor
            .visit_by_packed_value(self.doc_map.old_to_new(doc_id), packed_value)
    }

    fn compare(&self, min_packed_value: &[u8], max_packed_value: &[u8]) -> Relation {
        self.visitor.compare(min_packed_value, max_packed_value)
    }
}
