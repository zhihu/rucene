use core::index::field_info::Fields;
use core::index::stored_field_visitor::StoredFieldVisitor;
use core::util::DocId;

use error::Result;

pub trait StoredFieldsReader: Send + Sync {
    fn visit_document(&self, doc_id: DocId, visitor: &mut StoredFieldVisitor) -> Result<()>;
}

pub trait TermVectorsReader: Send + Sync {
    fn get(&self, doc: DocId) -> Result<Box<Fields>>;
}
