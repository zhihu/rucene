use core::index::BinaryDocValues;
use core::index::NumericDocValues;
use core::index::SortedDocValues;
use core::index::SortedNumericDocValues;
use core::index::SortedSetDocValues;
use core::index::{FieldInfo, Fields};
use core::util::BitsRef;
use error::Result;

use std::sync::Arc;

pub trait FieldsProducer: Fields {
    /// Checks consistency of this reader.
    /// Note that this may be costly in terms of I/O, e.g.
    /// may involve computing a checksum value against large data files.
    fn check_integrity(&self) -> Result<()>;

    // Returns an instance optimized for merging.
    // fn get_merge_instance(&self) -> Result<FieldsProducerRef>;
}

pub type FieldsProducerRef = Arc<FieldsProducer>;

pub trait DocValuesProducer: Send + Sync {
    fn get_numeric(&self, field_info: &FieldInfo) -> Result<Arc<NumericDocValues>>;
    fn get_binary(&self, field_info: &FieldInfo) -> Result<Arc<BinaryDocValues>>;
    fn get_sorted(&self, field: &FieldInfo) -> Result<Arc<SortedDocValues>>;
    fn get_sorted_numeric(&self, field: &FieldInfo) -> Result<Arc<SortedNumericDocValues>>;
    fn get_sorted_set(&self, field: &FieldInfo) -> Result<Arc<SortedSetDocValues>>;
    /// Returns a `bits` at the size of `reader.max_doc()`, with turned on bits for each doc_id
    /// that does have a value for this field.
    /// The returned instance need not be thread-safe: it will only be used by a single thread.
    fn get_docs_with_field(&self, field: &FieldInfo) -> Result<BitsRef>;
    /// Checks consistency of this producer
    /// Note that this may be costly in terms of I/O, e.g.
    /// may involve computing a checksum value against large data files.
    fn check_integrity(&self) -> Result<()>;

    fn get_merge_instance(&self) -> Result<Box<DocValuesProducer>>;
}

pub type DocValuesProducerRef = Arc<DocValuesProducer>;

pub trait NormsProducer: Send + Sync {
    fn norms(&self, field: &FieldInfo) -> Result<Box<NumericDocValues>>;
    fn check_integrity(&self) -> Result<()> {
        // codec_util::checksum_entire_file(input)?;
        Ok(())
    }
}
