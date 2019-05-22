use core::codec::{
    Codec, MergePointsReader, MutablePointsReader, PointsReader, PointsReaderEnum, PointsWriter,
};
use core::index::merge_policy::MergePolicy;
use core::index::merge_scheduler::MergeScheduler;
use core::index::thread_doc_writer::DocumentsWriterPerThread;
use core::index::FieldInfo;
use core::index::IntersectVisitor;
use core::index::PointValues;
use core::index::SegmentWriteState;
use core::util::byte_block_pool::{ByteBlockAllocator, ByteBlockPool};
use core::util::{BytesRef, DocId};

use error::Result;

use core::store::Directory;
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
    pub fn new<D: Directory, C: Codec, MS: MergeScheduler, MP: MergePolicy>(
        doc_writer: &mut DocumentsWriterPerThread<D, C, MS, MP>,
        field_info: &FieldInfo,
    ) -> PointValuesWriter {
        let bytes = unsafe { ByteBlockPool::new(doc_writer.byte_block_allocator.copy_unsafe()) };
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
        if value.len() == 0 {
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

    pub fn flush<D: Directory, C: Codec, DW: Directory, W: PointsWriter>(
        &mut self,
        _state: &SegmentWriteState<D, DW, C>,
        writer: &mut W,
    ) -> Result<()> {
        let reader: PointsReaderEnum<MergePointsReader<C>, TempMutablePointsReader> =
            PointsReaderEnum::Mutable(TempMutablePointsReader::new(self));
        writer.write_field(&self.field_info, reader)
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

        if field_name != &point_values_writer.field_info.name {
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
        if field_name != &self.point_values_writer().field_info.name {
            bail!(
                "fieldName must be the same, got: {}, expected: {}",
                field_name,
                self.point_values_writer().field_info.name
            );
        }

        Ok(self.point_values_writer().num_points as i64)
    }

    fn doc_count(&self, field_name: &str) -> Result<i32> {
        if field_name != &self.point_values_writer().field_info.name {
            bail!(
                "fieldName must be the same, got: {}, expected: {}",
                field_name,
                self.point_values_writer().field_info.name
            );
        }

        Ok(self.point_values_writer().num_docs as i32)
    }

    fn as_any(&self) -> &Any {
        self
    }
}

impl PointsReader for TempMutablePointsReader {
    fn check_integrity(&self) -> Result<()> {
        unimplemented!()
    }

    fn as_any(&self) -> &Any {
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
