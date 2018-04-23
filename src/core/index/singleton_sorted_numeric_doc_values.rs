use core::index::{NumericDocValues, NumericDocValuesRef, SortedNumericDocValues,
                  SortedNumericDocValuesContext};
use core::util::{Bits, DocId};
use error::Result;

use std::sync::Arc;

pub struct SingletonSortedNumericDocValues {
    numeric_doc_values_in: NumericDocValuesRef,
    docs_with_field: Bits,
}

impl SingletonSortedNumericDocValues {
    pub fn new(numeric_doc_values_in: Box<NumericDocValues>, docs_with_field: Bits) -> Self {
        SingletonSortedNumericDocValues {
            numeric_doc_values_in: Arc::new(numeric_doc_values_in),
            docs_with_field,
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
    fn set_document(&self, doc: DocId) -> Result<SortedNumericDocValuesContext> {
        let value = self.numeric_doc_values_in.get(doc)?;
        let id = self.docs_with_field.id();
        let count = if id != 1 && value == 0 && !(self.docs_with_field.get(doc as usize)?) {
            0
        } else {
            1
        };
        Ok((value, i64::from(count)))
    }

    fn value_at(&self, ctx: &SortedNumericDocValuesContext, _index: usize) -> Result<i64> {
        Ok(ctx.0)
    }

    fn count(&self, ctx: &SortedNumericDocValuesContext) -> usize {
        ctx.1 as usize
    }

    fn get_numeric_doc_values(&self) -> Option<NumericDocValuesRef> {
        Some(self.get_numeric_doc_values_impl())
    }
}
