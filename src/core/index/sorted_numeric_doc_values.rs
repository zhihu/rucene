use core::index::NumericDocValuesRef;
use core::util::{BitsContext, DocId, LongValues};
use error::Result;

use std::sync::Arc;

pub type SortedNumericDocValuesContext = (i64, i64, BitsContext);

pub trait SortedNumericDocValues: Send + Sync {
    /// positions to the specified document
    fn set_document(
        &self,
        ctx: Option<SortedNumericDocValuesContext>,
        doc: DocId,
    ) -> Result<SortedNumericDocValuesContext>;
    /// Retrieve the value for the current document at the specified index.
    /// An index ranges from 0 to count() - 1.
    fn value_at(&self, ctx: &SortedNumericDocValuesContext, index: usize) -> Result<i64>;
    /// Retrieves the count of values for the current document
    /// There may be zero if a document has no values
    fn count(&self, ctx: &SortedNumericDocValuesContext) -> usize;
    fn get_numeric_doc_values(&self) -> Option<NumericDocValuesRef> {
        None
    }
}

pub type SortedNumericDocValuesRef = Arc<SortedNumericDocValues>;

pub struct AddressedSortedNumericDocValues {
    values: Box<LongValues>,
    ord_index: Box<LongValues>,
}

impl AddressedSortedNumericDocValues {
    pub fn new(values: Box<LongValues>, ord_index: Box<LongValues>) -> Self {
        AddressedSortedNumericDocValues { values, ord_index }
    }
}

impl SortedNumericDocValues for AddressedSortedNumericDocValues {
    fn set_document(
        &self,
        ctx: Option<SortedNumericDocValuesContext>,
        doc: DocId,
    ) -> Result<SortedNumericDocValuesContext> {
        let bc = ctx.and_then(|c| c.2);
        Ok((self.ord_index.get(doc)?, self.ord_index.get(doc + 1)?, bc))
    }

    fn value_at(&self, ctx: &SortedNumericDocValuesContext, index: usize) -> Result<i64> {
        let offset = ctx.0 + index as i64;
        self.values.get64(offset)
    }

    fn count(&self, ctx: &SortedNumericDocValuesContext) -> usize {
        (ctx.1 - ctx.0) as usize
    }
}

pub struct TabledSortedNumericDocValues {
    ordinals: Box<LongValues>,
    table: Vec<i64>,
    offsets: Vec<i32>,
}

impl TabledSortedNumericDocValues {
    pub fn new(ordinals: Box<LongValues>, table: &[i64], offsets: &[i32]) -> Self {
        TabledSortedNumericDocValues {
            ordinals,
            table: table.to_vec(),
            offsets: offsets.to_vec(),
        }
    }
}

impl SortedNumericDocValues for TabledSortedNumericDocValues {
    fn set_document(
        &self,
        ctx: Option<SortedNumericDocValuesContext>,
        doc: DocId,
    ) -> Result<SortedNumericDocValuesContext> {
        let ord = self.ordinals.get(doc)? as usize;
        let bc = ctx.and_then(|c| c.2);
        Ok((
            i64::from(self.offsets[ord]),
            i64::from(self.offsets[ord + 1]),
            bc,
        ))
    }

    fn value_at(&self, ctx: &SortedNumericDocValuesContext, index: usize) -> Result<i64> {
        let offset = ctx.0 + index as i64;
        Ok(self.table[offset as usize])
    }

    fn count(&self, ctx: &SortedNumericDocValuesContext) -> usize {
        (ctx.1 - ctx.0) as usize
    }
}
