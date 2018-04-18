use core::index::BinaryDocValues;
use core::index::NumericDocValues;
use core::index::SortedDocValues;
use core::index::SortedNumericDocValues;
use core::index::SortedSetDocValues;
use core::index::{FieldInfo, Fields};
use core::util::Bits;
use error::Result;

use std::sync::Arc;

pub trait FieldsProducer: Fields {
    /// Checks consistency of this reader.
    /// Note that this may be costly in terms of I/O, e.g.
    /// may involve computing a checksum value against large data files.
    fn check_integrity(&self) -> Result<()>;

    /// Returns an instance optimized for merging.
    fn get_merge_instance(&self) -> Result<FieldsProducerRef>;
}

pub type FieldsProducerRef = Arc<FieldsProducer>;

pub trait DocValuesProducer: Send + Sync {
    fn get_numeric(&self, field_info: &FieldInfo) -> Result<Box<NumericDocValues>>;
    fn get_binary(&mut self, field_info: &FieldInfo) -> Result<Box<BinaryDocValues>>;
    fn get_sorted(&mut self, field: &FieldInfo) -> Result<Box<SortedDocValues>>;
    fn get_sorted_numeric(&self, field: &FieldInfo) -> Result<Box<SortedNumericDocValues>>;
    fn get_sorted_set(&mut self, field: &FieldInfo) -> Result<Box<SortedSetDocValues>>;
    /// Returns a `bits` at the size of `reader.max_doc()`, with turned on bits for each doc_id
    /// that does have a value for this field.
    /// The returned instance need not be thread-safe: it will only be used by a single thread.
    fn get_docs_with_field(&mut self, field: &FieldInfo) -> Result<Bits>;
    /// Checks consistency of this producer
    /// Note that this may be costly in terms of I/O, e.g.
    /// may involve computing a checksum value against large data files.
    fn check_integrity(&mut self) -> Result<()>;
}

pub trait NormsProducer: Send + Sync {
    fn norms(&self, field: &FieldInfo) -> Result<Box<NumericDocValues>>;
}
