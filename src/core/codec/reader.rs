use core::index::field_info::Fields;
use core::index::PointValues;
use core::index::StoredFieldVisitor;
use core::util::DocId;

use error::Result;

use std::any::Any;
use std::sync::Arc;

pub trait StoredFieldsReader: Sized {
    // NOTE: we can't use generic for `StoredFieldVisitor` because of IndexReader#docment can't use
    // generic
    fn visit_document(&self, doc_id: DocId, visitor: &mut dyn StoredFieldVisitor) -> Result<()>;

    // a mutable version for `Self::visit_document` that maybe more fast
    fn visit_document_mut(&mut self, doc_id: DocId, visitor: &mut StoredFieldVisitor)
        -> Result<()>;

    fn get_merge_instance(&self) -> Result<Self>;

    // used for type Downcast
    fn as_any(&self) -> &Any;
}

impl<T: StoredFieldsReader + 'static> StoredFieldsReader for Arc<T> {
    fn visit_document(&self, doc_id: DocId, visitor: &mut dyn StoredFieldVisitor) -> Result<()> {
        (**self).visit_document(doc_id, visitor)
    }

    fn visit_document_mut(
        &mut self,
        doc_id: DocId,
        visitor: &mut StoredFieldVisitor,
    ) -> Result<()> {
        debug_assert_eq!(Arc::strong_count(self), 1);
        Arc::get_mut(self)
            .unwrap()
            .visit_document_mut(doc_id, visitor)
    }

    fn get_merge_instance(&self) -> Result<Self> {
        Ok(Arc::new((**self).get_merge_instance()?))
    }

    fn as_any(&self) -> &Any {
        &**self
    }
}

pub trait TermVectorsReader {
    type Fields: Fields;
    fn get(&self, doc: DocId) -> Result<Option<Self::Fields>>;
    fn as_any(&self) -> &Any;
}

impl<T: TermVectorsReader + 'static> TermVectorsReader for Arc<T> {
    type Fields = T::Fields;

    fn get(&self, doc: DocId) -> Result<Option<Self::Fields>> {
        (**self).get(doc)
    }

    fn as_any(&self) -> &Any {
        (**self).as_any()
    }
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
    fn clone(&self) -> Self;
}
