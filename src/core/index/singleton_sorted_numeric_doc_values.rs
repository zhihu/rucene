use core::index::{NumericDocValues, NumericDocValuesRef};

use core::index::SortedNumericDocValues;
use core::util::Bits;
use core::util::DocId;
use error::Result;

use std::sync::{Arc, Mutex};

pub struct SingletonSortedNumericDocValues {
    numeric_doc_values_in: NumericDocValuesRef,
    docs_with_field: Bits,
    value: i64,
    count: usize,
}

impl SingletonSortedNumericDocValues {
    pub fn new(numeric_doc_values_in: Box<NumericDocValues>, docs_with_field: Bits) -> Self {
        SingletonSortedNumericDocValues {
            numeric_doc_values_in: Arc::new(Mutex::new(numeric_doc_values_in)),
            docs_with_field,
            value: 0,
            count: 0,
        }
    }

    pub fn get_numeric_doc_values_impl(&self) -> NumericDocValuesRef {
        Arc::clone(&self.numeric_doc_values_in)
    }
    // pub fn get_docs_with_field(&self) -> &Bits {
    //    self.docs_with_field
    // }
}

impl SortedNumericDocValues for SingletonSortedNumericDocValues {
    fn set_document(&mut self, doc: DocId) -> Result<()> {
        let value = self.numeric_doc_values_in.lock()?.get(doc)?;
        self.value = value;
        let id = self.docs_with_field.id();
        self.count = if id != 1 && value == 0 && !(self.docs_with_field.get(doc as usize)?) {
            0
        } else {
            1
        };
        Ok(())
    }

    fn value_at(&mut self, _index: usize) -> Result<i64> {
        Ok(self.value)
    }

    fn count(&self) -> usize {
        self.count
    }

    fn get_numeric_doc_values(&self) -> Option<NumericDocValuesRef> {
        Some(self.get_numeric_doc_values_impl())
    }
}
