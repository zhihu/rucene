use core::index::term::TermIterator;
use core::index::BoxedBinaryDocValuesEnum;
use core::index::SortedSetDocValuesTermIterator;
use core::index::{CompressedBinaryDocValues, LongBinaryDocValues};

use core::util::bit_util;
use core::util::DocId;
use core::util::LongValues;
use error::Result;

use std::sync::Arc;

pub const NO_MORE_ORDS: i64 = -1;

pub type SortedSetDocValuesContext = (i64, i64, i64);

pub trait SortedSetDocValues: Send + Sync {
    /// positions to the specified document
    fn set_document(&self, doc: DocId) -> Result<SortedSetDocValuesContext>;
    fn next_ord(&self, ctx: &mut SortedSetDocValuesContext) -> Result<i64>;
    fn lookup_ord(&self, ord: i64) -> Result<Vec<u8>>;
    fn get_value_count(&self) -> usize;

    fn lookup_term(&self, key: &[u8]) -> Result<i64> {
        let mut low = 0_i64;
        let mut high = self.get_value_count() as i64 - 1;
        while low < high {
            let mid = low + (high - low) / 2;
            let term = self.lookup_ord(mid)?;
            let cmp = bit_util::bcompare(&term, key);
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

    fn term_iterator<'a, 'b: 'a>(&'b self) -> Result<Box<TermIterator + 'a>>;
}

pub type SortedSetDocValuesRef = Arc<SortedSetDocValues>;

pub trait RandomAccessOrds: SortedSetDocValues {
    fn ord_at(&self, ctx: &SortedSetDocValuesContext, index: i32) -> Result<i64>;
    fn cardinality(&self, ctx: &SortedSetDocValuesContext) -> i32;
}

pub struct AddressedRandomAccessOrds {
    binary: BoxedBinaryDocValuesEnum,
    ordinals: Box<LongValues>,
    ord_index: Box<LongValues>,
    value_count: usize,
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
        }
    }
}

impl SortedSetDocValues for AddressedRandomAccessOrds {
    fn set_document(&self, doc: DocId) -> Result<SortedSetDocValuesContext> {
        let start_offset = self.ord_index.get(doc)?;
        let offset = start_offset;
        let end_offset = self.ord_index.get(doc + 1)?;
        Ok((start_offset, offset, end_offset))
    }

    fn next_ord(&self, ctx: &mut SortedSetDocValuesContext) -> Result<i64> {
        let value = if ctx.1 == ctx.2 {
            NO_MORE_ORDS
        } else {
            let ord = self.ordinals.get64(ctx.1)?;
            ctx.1 += 1;
            ord
        };
        Ok(value)
    }

    fn lookup_ord(&self, ord: i64) -> Result<Vec<u8>> {
        match self.binary {
            BoxedBinaryDocValuesEnum::General(ref long_binary) => long_binary.get64(ord),
            BoxedBinaryDocValuesEnum::Compressed(ref compressed_binary) => {
                compressed_binary.get64(ord)
            }
        }
    }

    fn get_value_count(&self) -> usize {
        self.value_count
    }

    fn lookup_term(&self, key: &[u8]) -> Result<i64> {
        match self.binary {
            BoxedBinaryDocValuesEnum::Compressed(ref compressed_binary) => {
                let value = compressed_binary.lookup_term(key)?;
                Ok(value as i64)
            }
            _ => <Self as SortedSetDocValues>::lookup_term(self, key),
        }
    }

    fn term_iterator<'a, 'b: 'a>(&'b self) -> Result<Box<TermIterator + 'a>> {
        match self.binary {
            BoxedBinaryDocValuesEnum::Compressed(ref bin) => {
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
    fn ord_at(&self, ctx: &SortedSetDocValuesContext, index: i32) -> Result<i64> {
        let position = ctx.0 + i64::from(index);
        self.ordinals.get64(position)
    }
    fn cardinality(&self, ctx: &SortedSetDocValuesContext) -> i32 {
        (ctx.2 - ctx.0) as i32
    }
}

pub struct TabledRandomAccessOrds {
    binary: BoxedBinaryDocValuesEnum,
    ordinals: Box<LongValues>,
    table: Vec<i64>,
    table_offsets: Vec<i32>,
    value_count: usize,
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
        }
    }
}

impl SortedSetDocValues for TabledRandomAccessOrds {
    fn set_document(&self, doc: DocId) -> Result<SortedSetDocValuesContext> {
        let ord = self.ordinals.get(doc)?;
        let start_offset = self.table_offsets[ord as usize];
        let offset = start_offset;
        let end_offset = self.table_offsets[ord as usize + 1];
        Ok((start_offset.into(), offset.into(), end_offset.into()))
    }

    fn next_ord(&self, ctx: &mut SortedSetDocValuesContext) -> Result<i64> {
        let value = if ctx.1 == ctx.2 {
            NO_MORE_ORDS
        } else {
            let val = self.table[ctx.1 as usize];
            ctx.1 += 1;
            val
        };
        Ok(value)
    }

    fn lookup_ord(&self, ord: i64) -> Result<Vec<u8>> {
        match self.binary {
            BoxedBinaryDocValuesEnum::General(ref binary) => binary.get64(ord),
            BoxedBinaryDocValuesEnum::Compressed(ref binary) => binary.get64(ord),
        }
    }

    fn get_value_count(&self) -> usize {
        self.value_count
    }

    fn lookup_term(&self, key: &[u8]) -> Result<i64> {
        match self.binary {
            BoxedBinaryDocValuesEnum::Compressed(ref binary) => {
                let val = binary.lookup_term(key)?;
                Ok(val as i64)
            }
            _ => <Self as SortedSetDocValues>::lookup_term(self, key),
        }
    }

    fn term_iterator<'a, 'b: 'a>(&'b self) -> Result<Box<TermIterator + 'a>> {
        match self.binary {
            BoxedBinaryDocValuesEnum::Compressed(ref bin) => {
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
    fn ord_at(&self, ctx: &SortedSetDocValuesContext, index: i32) -> Result<i64> {
        let position = ctx.0 + i64::from(index);
        let value = self.table[position as usize];
        Ok(value)
    }

    fn cardinality(&self, ctx: &SortedSetDocValuesContext) -> i32 {
        (ctx.2 - ctx.0) as i32
    }
}
