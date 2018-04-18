use core::index::NumericDocValuesRef;
use core::util::DocId;
use core::util::LongValues;
use error::Result;

use std::sync::{Arc, Mutex};

pub trait SortedNumericDocValues: Send {
    /// positions to the specified document
    fn set_document(&mut self, doc: DocId) -> Result<()>;
    /// Retrieve the value for the current document at the specified index.
    /// An index ranges from 0 to count() - 1.
    fn value_at(&mut self, index: usize) -> Result<i64>;
    /// Retrieves the count of values for the current document
    /// There may be zero if a document has no values
    fn count(&self) -> usize;
    fn get_numeric_doc_values(&self) -> Option<NumericDocValuesRef> {
        None
    }
}

pub type SortedNumericDocValuesRef = Arc<Mutex<Box<SortedNumericDocValues>>>;

pub struct AddressedSortedNumericDocValues {
    values: Box<LongValues>,
    ord_index: Box<LongValues>,
    start_offset: i64,
    end_offset: i64,
}

impl AddressedSortedNumericDocValues {
    pub fn new(values: Box<LongValues>, ord_index: Box<LongValues>) -> Self {
        AddressedSortedNumericDocValues {
            values,
            ord_index,
            start_offset: 0,
            end_offset: 0,
        }
    }
}

impl SortedNumericDocValues for AddressedSortedNumericDocValues {
    fn set_document(&mut self, doc: DocId) -> Result<()> {
        self.start_offset = self.ord_index.get(doc)?;
        self.end_offset = self.ord_index.get(doc + 1)?;
        Ok(())
    }

    fn value_at(&mut self, index: usize) -> Result<i64> {
        let offset = self.start_offset + index as i64;
        self.values.get64(offset)
    }

    fn count(&self) -> usize {
        (self.end_offset - self.start_offset) as usize
    }
}

pub struct TabledSortedNumericDocValues {
    ordinals: Box<LongValues>,
    table: Vec<i64>,
    offsets: Vec<i32>,
    start_offset: i32,
    end_offset: i32,
}

impl TabledSortedNumericDocValues {
    pub fn new(ordinals: Box<LongValues>, table: &[i64], offsets: &[i32]) -> Self {
        TabledSortedNumericDocValues {
            ordinals,
            table: table.to_vec(),
            offsets: offsets.to_vec(),
            start_offset: 0,
            end_offset: 0,
        }
    }
}

impl SortedNumericDocValues for TabledSortedNumericDocValues {
    fn set_document(&mut self, doc: DocId) -> Result<()> {
        let ord = self.ordinals.get(doc)? as usize;
        self.start_offset = self.offsets[ord];
        self.end_offset = self.offsets[ord + 1];
        Ok(())
    }

    fn value_at(&mut self, index: usize) -> Result<i64> {
        let offset = self.start_offset + index as i32;
        Ok(self.table[offset as usize])
    }

    fn count(&self) -> usize {
        (self.end_offset - self.start_offset) as usize
    }
}
