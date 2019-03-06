use core::index::field_info::Fields;
use core::index::PointValues;
use core::index::StoredFieldVisitor;
use core::util::DocId;

use error::Result;

use std::any::Any;

pub trait StoredFieldsReader: Send + Sync {
    fn visit_document(&self, doc_id: DocId, visitor: &mut StoredFieldVisitor) -> Result<()>;

    // a mutable version for `Self::visit_document` that maybe more fast
    fn visit_document_mut(&mut self, doc_id: DocId, visitor: &mut StoredFieldVisitor)
        -> Result<()>;

    fn get_merge_instance(&self) -> Result<Box<StoredFieldsReader>>;

    // used for type Downcast
    fn as_any(&self) -> &Any;
}

pub trait TermVectorsReader: Send + Sync {
    fn get(&self, doc: DocId) -> Result<Option<Box<Fields>>>;
    fn as_any(&self) -> &Any;
}

pub trait PointsReader: PointValues {
    fn check_integrity(&self) -> Result<()>;
    fn as_any(&self) -> &Any;
}

pub trait MutablePointsReader: PointsReader {
    fn value(&self, i: i32, packed_value: &mut Vec<u8>);
    fn byte_at(&self, i: i32, k: i32) -> u8;
    fn doc_id(&self, i: i32) -> DocId;
    fn swap(&mut self, i: i32, j: i32);
    fn clone(&self) -> Box<MutablePointsReader>;
}
