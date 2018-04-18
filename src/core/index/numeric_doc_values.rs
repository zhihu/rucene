use core::util::DocId;
use error::Result;

use std::sync::{Arc, Mutex};

pub trait NumericDocValues: Send {
    fn get(&mut self, doc_id: DocId) -> Result<i64>;
}

pub type NumericDocValuesRef = Arc<Mutex<Box<NumericDocValues>>>;
