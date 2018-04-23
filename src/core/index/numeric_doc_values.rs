use core::util::DocId;
use error::Result;

use std::sync::Arc;

pub trait NumericDocValues: Send + Sync {
    fn get(&self, doc_id: DocId) -> Result<i64>;
}

pub type NumericDocValuesRef = Arc<Box<NumericDocValues>>;
