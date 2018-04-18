use core::index::term::TermIterator;
use core::index::BoxedBinaryDocValuesEnum;
use core::index::SortedDocValuesTermIterator;
use core::index::{BinaryDocValues, CompressedBinaryDocValues, LongBinaryDocValues};
use core::util::bit_util;
use core::util::DocId;
use core::util::LongValues;
use error::Result;

use std::sync::{Arc, Mutex};

pub trait SortedDocValues: BinaryDocValues {
    fn get_ord(&mut self, doc_id: DocId) -> Result<i32>;

    fn lookup_ord(&mut self, ord: i32) -> Result<&[u8]>;

    fn get_value_count(&self) -> usize;

    /// if key exists, return its ordinal, else return
    /// - insertion_point - 1.
    fn lookup_term(&mut self, key: &[u8]) -> Result<i32> {
        let mut low = 0;
        let mut high = self.get_value_count() as i32 - 1;
        while low <= high {
            let mid = low + (high - low) / 2;
            let term = self.lookup_ord(mid)?;
            let cmp = bit_util::bcompare(term, key);
            if cmp < 0 {
                low = mid + 1;
            } else if cmp > 0 {
                high = mid - 1;
            } else {
                return Ok(mid); // key found
            }
        }
        Ok(-(low + 1)) // key not found
    }

    fn term_iterator<'a, 'b: 'a>(&'b mut self) -> Result<Box<TermIterator + 'a>>;
}

pub type SortedDocValuesRef = Arc<Mutex<Box<SortedDocValues>>>;

pub struct TailoredSortedDocValues {
    ordinals: Box<LongValues>,
    binary: BoxedBinaryDocValuesEnum,
    value_count: usize,
    dummy: Vec<u8>,
}

impl TailoredSortedDocValues {
    pub fn new(
        ordinals: Box<LongValues>,
        binary: Box<LongBinaryDocValues>,
        value_count: usize,
    ) -> Self {
        TailoredSortedDocValues {
            ordinals,
            binary: BoxedBinaryDocValuesEnum::General(binary),
            value_count,
            dummy: vec![0u8; 1],
        }
    }

    pub fn with_compression(
        ordinals: Box<LongValues>,
        binary: Box<CompressedBinaryDocValues>,
        value_count: usize,
    ) -> Self {
        TailoredSortedDocValues {
            ordinals,
            binary: BoxedBinaryDocValuesEnum::Compressed(binary),
            value_count,
            dummy: vec![0u8; 1],
        }
    }
}

impl SortedDocValues for TailoredSortedDocValues {
    fn get_ord(&mut self, doc_id: DocId) -> Result<i32> {
        let value = self.ordinals.get(doc_id)?;
        Ok(value as i32)
    }

    fn lookup_ord(&mut self, ord: i32) -> Result<&[u8]> {
        match self.binary {
            BoxedBinaryDocValuesEnum::General(ref mut binary) => binary.get(ord),
            BoxedBinaryDocValuesEnum::Compressed(ref mut binary) => binary.get(ord),
        }
    }

    fn get_value_count(&self) -> usize {
        self.value_count
    }

    fn lookup_term(&mut self, key: &[u8]) -> Result<i32> {
        match self.binary {
            BoxedBinaryDocValuesEnum::Compressed(ref mut binary) => {
                let val = binary.lookup_term(key)? as i32;
                Ok(val)
            }
            _ => <Self as SortedDocValues>::lookup_term(self, key),
        }
    }
    fn term_iterator<'a, 'b: 'a>(&'b mut self) -> Result<Box<TermIterator + 'a>> {
        match self.binary {
            BoxedBinaryDocValuesEnum::Compressed(ref mut bin) => {
                let boxed = bin.get_term_iterator()?;
                Ok(Box::new(boxed))
            }
            _ => {
                let ti = SortedDocValuesTermIterator::new(self);
                Ok(Box::new(ti))
            }
        }
    }
}

impl BinaryDocValues for TailoredSortedDocValues {
    fn get(&mut self, doc_id: DocId) -> Result<&[u8]> {
        let ord = self.get_ord(doc_id)?;
        if ord == -1 {
            Ok(&self.dummy[0..0])
        } else {
            self.lookup_ord(ord)
        }
    }
}
