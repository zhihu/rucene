use core::index::term::TermIterator;
use core::index::BoxedBinaryDocValuesEnum;
use core::index::SortedSetDocValuesTermIterator;
use core::index::{CompressedBinaryDocValues, LongBinaryDocValues};

use core::util::bit_util;
use core::util::DocId;
use core::util::LongValues;
use error::Result;

use std::sync::{Arc, Mutex};

pub const NO_MORE_ORDS: i64 = -1;

pub trait SortedSetDocValues: Send {
    /// positions to the specified document
    fn set_document(&mut self, doc: DocId) -> Result<()>;
    fn next_ord(&mut self) -> Result<i64>;
    fn lookup_ord(&mut self, ord: i64) -> Result<&[u8]>;
    fn get_value_count(&self) -> usize;

    fn lookup_term(&mut self, key: &[u8]) -> Result<i64> {
        let mut low = 0_i64;
        let mut high = self.get_value_count() as i64 - 1;
        while low < high {
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

pub type SortedSetDocValuesRef = Arc<Mutex<Box<SortedSetDocValues>>>;

pub trait RandomAccessOrds: SortedSetDocValues {
    fn ord_at(&mut self, index: i32) -> Result<i64>;
    fn cardinality(&self) -> i32;
}

pub struct AddressedRandomAccessOrds {
    binary: BoxedBinaryDocValuesEnum,
    ordinals: Box<LongValues>,
    ord_index: Box<LongValues>,
    value_count: usize,
    start_offset: i64,
    end_offset: i64,
    offset: i64,
}

impl AddressedRandomAccessOrds {
    pub fn new(
        binary: Box<LongBinaryDocValues>,
        ordinals: Box<LongValues>,
        ord_index: Box<LongValues>,
        value_count: usize,
    ) -> Self {
        AddressedRandomAccessOrds {
            binary: BoxedBinaryDocValuesEnum::General(binary),
            ordinals,
            ord_index,
            value_count,
            start_offset: 0,
            end_offset: 0,
            offset: 0,
        }
    }

    pub fn with_compression(
        binary: Box<CompressedBinaryDocValues>,
        ordinals: Box<LongValues>,
        ord_index: Box<LongValues>,
        value_count: usize,
    ) -> Self {
        AddressedRandomAccessOrds {
            binary: BoxedBinaryDocValuesEnum::Compressed(binary),
            ordinals,
            ord_index,
            value_count,
            start_offset: 0,
            end_offset: 0,
            offset: 0,
        }
    }
}

impl SortedSetDocValues for AddressedRandomAccessOrds {
    fn set_document(&mut self, doc: DocId) -> Result<()> {
        self.start_offset = self.ord_index.get(doc)?;
        self.offset = self.start_offset;
        self.end_offset = self.ord_index.get(doc + 1)?;
        Ok(())
    }

    fn next_ord(&mut self) -> Result<i64> {
        let value = if self.offset == self.end_offset {
            NO_MORE_ORDS
        } else {
            let ord = self.ordinals.get64(self.offset)?;
            self.offset += 1;
            ord
        };
        Ok(value)
    }

    fn lookup_ord(&mut self, ord: i64) -> Result<&[u8]> {
        match self.binary {
            BoxedBinaryDocValuesEnum::General(ref mut long_binary) => long_binary.get64(ord),
            BoxedBinaryDocValuesEnum::Compressed(ref mut compressed_binary) => {
                compressed_binary.get64(ord)
            }
        }
    }

    fn get_value_count(&self) -> usize {
        self.value_count
    }

    fn lookup_term(&mut self, key: &[u8]) -> Result<i64> {
        match self.binary {
            BoxedBinaryDocValuesEnum::Compressed(ref mut compressed_binary) => {
                let value = compressed_binary.lookup_term(key)?;
                Ok(value as i64)
            }
            _ => <Self as SortedSetDocValues>::lookup_term(self, key),
        }
    }

    fn term_iterator<'a, 'b: 'a>(&'b mut self) -> Result<Box<TermIterator + 'a>> {
        match self.binary {
            BoxedBinaryDocValuesEnum::Compressed(ref mut bin) => {
                let boxed = bin.get_term_iterator()?;
                Ok(Box::new(boxed))
            }
            _ => {
                let ti = SortedSetDocValuesTermIterator::new(self);
                Ok(Box::new(ti))
            }
        }
    }
}

impl RandomAccessOrds for AddressedRandomAccessOrds {
    fn ord_at(&mut self, index: i32) -> Result<i64> {
        let position = self.start_offset + i64::from(index);
        self.ordinals.get64(position)
    }
    fn cardinality(&self) -> i32 {
        (self.end_offset - self.start_offset) as i32
    }
}

pub struct TabledRandomAccessOrds {
    binary: BoxedBinaryDocValuesEnum,
    ordinals: Box<LongValues>,
    table: Vec<i64>,
    table_offsets: Vec<i32>,
    value_count: usize,
    start_offset: i32,
    end_offset: i32,
    offset: i32,
}

impl TabledRandomAccessOrds {
    pub fn new(
        binary: Box<LongBinaryDocValues>,
        ordinals: Box<LongValues>,
        table: Vec<i64>,
        table_offsets: Vec<i32>,
        value_count: usize,
    ) -> Self {
        TabledRandomAccessOrds {
            binary: BoxedBinaryDocValuesEnum::General(binary),
            ordinals,
            table,
            table_offsets,
            value_count,
            start_offset: 0,
            end_offset: 0,
            offset: 0,
        }
    }

    pub fn with_compression(
        binary: Box<CompressedBinaryDocValues>,
        ordinals: Box<LongValues>,
        table: Vec<i64>,
        table_offsets: Vec<i32>,
        value_count: usize,
    ) -> Self {
        TabledRandomAccessOrds {
            binary: BoxedBinaryDocValuesEnum::Compressed(binary),
            ordinals,
            table,
            table_offsets,
            value_count,
            start_offset: 0,
            end_offset: 0,
            offset: 0,
        }
    }
}

impl SortedSetDocValues for TabledRandomAccessOrds {
    fn set_document(&mut self, doc: DocId) -> Result<()> {
        let ord = self.ordinals.get(doc)?;
        let val = self.table_offsets[ord as usize];
        self.start_offset = val;
        self.offset = val;
        self.end_offset = self.table_offsets[ord as usize + 1];
        Ok(())
    }

    fn next_ord(&mut self) -> Result<i64> {
        let value = if self.offset == self.end_offset {
            NO_MORE_ORDS
        } else {
            let val = self.table[self.offset as usize];
            self.offset += 1;
            val
        };
        Ok(value)
    }

    fn lookup_ord(&mut self, ord: i64) -> Result<&[u8]> {
        match self.binary {
            BoxedBinaryDocValuesEnum::General(ref mut binary) => binary.get64(ord),
            BoxedBinaryDocValuesEnum::Compressed(ref mut binary) => binary.get64(ord),
        }
    }

    fn get_value_count(&self) -> usize {
        self.value_count
    }

    fn lookup_term(&mut self, key: &[u8]) -> Result<i64> {
        match self.binary {
            BoxedBinaryDocValuesEnum::Compressed(ref mut binary) => {
                let val = binary.lookup_term(key)?;
                Ok(val as i64)
            }
            _ => <Self as SortedSetDocValues>::lookup_term(self, key),
        }
    }

    fn term_iterator<'a, 'b: 'a>(&'b mut self) -> Result<Box<TermIterator + 'a>> {
        match self.binary {
            BoxedBinaryDocValuesEnum::Compressed(ref mut bin) => {
                let boxed = bin.get_term_iterator()?;
                Ok(Box::new(boxed))
            }
            _ => {
                let ti = SortedSetDocValuesTermIterator::new(self);
                Ok(Box::new(ti))
            }
        }
    }
}

impl RandomAccessOrds for TabledRandomAccessOrds {
    fn ord_at(&mut self, index: i32) -> Result<i64> {
        let position = self.start_offset + index;
        let value = self.table[position as usize];
        Ok(value)
    }

    fn cardinality(&self) -> i32 {
        self.end_offset - self.start_offset
    }
}
