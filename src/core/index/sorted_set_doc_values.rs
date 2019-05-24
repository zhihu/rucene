// Copyright 2019 Zhizhesihai (Beijing) Technology Limited.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use core::index::{
    BoxedBinaryDocValuesEnum, CompressedBinaryDocValues, DocValuesTermIterator,
    LongBinaryDocValues, NumericDocValues, SortedSetDocValuesTermIterator,
};

use core::util::bit_util;
use core::util::DocId;
use core::util::LongValues;
use error::Result;

use core::util::packed::MixinMonotonicLongValues;
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
        while low <= high {
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

    fn term_iterator(&self) -> Result<DocValuesTermIterator>;
}

pub type SortedSetDocValuesRef = Arc<dyn SortedSetDocValues>;

pub struct EmptySortedSetDocValues;

impl SortedSetDocValues for EmptySortedSetDocValues {
    fn set_document(&self, _doc: DocId) -> Result<SortedSetDocValuesContext> {
        Ok((0, 0, 0))
    }
    fn next_ord(&self, _ctx: &mut SortedSetDocValuesContext) -> Result<i64> {
        Ok(NO_MORE_ORDS)
    }
    fn lookup_ord(&self, _ord: i64) -> Result<Vec<u8>> {
        Ok(Vec::with_capacity(0))
    }
    fn get_value_count(&self) -> usize {
        0
    }

    fn lookup_term(&self, _key: &[u8]) -> Result<i64> {
        Ok(-1)
    }

    fn term_iterator(&self) -> Result<DocValuesTermIterator> {
        Ok(DocValuesTermIterator::empty())
    }
}

pub trait RandomAccessOrds: SortedSetDocValues {
    fn ord_at(&self, ctx: &SortedSetDocValuesContext, index: i32) -> Result<i64>;
    fn cardinality(&self, ctx: &SortedSetDocValuesContext) -> i32;
}

#[derive(Clone)]
pub struct AddressedRandomAccessOrds {
    inner: Arc<AddressedRandomAccessOrdsInner>,
}

impl AddressedRandomAccessOrds {
    pub fn new(
        binary: Box<dyn LongBinaryDocValues>,
        ordinals: Box<dyn LongValues>,
        ord_index: MixinMonotonicLongValues,
        value_count: usize,
    ) -> Self {
        let inner = AddressedRandomAccessOrdsInner::new(binary, ordinals, ord_index, value_count);
        AddressedRandomAccessOrds {
            inner: Arc::new(inner),
        }
    }

    pub fn with_compression(
        binary: CompressedBinaryDocValues,
        ordinals: Box<dyn LongValues>,
        ord_index: MixinMonotonicLongValues,
        value_count: usize,
    ) -> Self {
        let inner = AddressedRandomAccessOrdsInner::with_compression(
            binary,
            ordinals,
            ord_index,
            value_count,
        );
        AddressedRandomAccessOrds {
            inner: Arc::new(inner),
        }
    }
}

impl SortedSetDocValues for AddressedRandomAccessOrds {
    fn set_document(&self, doc: DocId) -> Result<SortedSetDocValuesContext> {
        self.inner.set_document(doc)
    }

    fn next_ord(&self, ctx: &mut SortedSetDocValuesContext) -> Result<i64> {
        self.inner.next_ord(ctx)
    }

    fn lookup_ord(&self, ord: i64) -> Result<Vec<u8>> {
        self.inner.lookup_ord(ord)
    }

    fn get_value_count(&self) -> usize {
        self.inner.value_count
    }

    fn lookup_term(&self, key: &[u8]) -> Result<i64> {
        self.inner.lookup_term(key)
    }

    fn term_iterator(&self) -> Result<DocValuesTermIterator> {
        match self.inner.binary {
            BoxedBinaryDocValuesEnum::Compressed(ref bin) => {
                let boxed = bin.get_term_iterator()?;
                Ok(DocValuesTermIterator::comp_bin(boxed))
            }
            _ => {
                let ti = SortedSetDocValuesTermIterator::new(self.clone());
                Ok(DocValuesTermIterator::sorted_set_addr(ti))
            }
        }
    }
}

impl RandomAccessOrds for AddressedRandomAccessOrds {
    fn ord_at(&self, ctx: &SortedSetDocValuesContext, index: i32) -> Result<i64> {
        let position = ctx.0 + i64::from(index);
        self.inner.ordinals.get64(position)
    }
    fn cardinality(&self, ctx: &SortedSetDocValuesContext) -> i32 {
        (ctx.2 - ctx.0) as i32
    }
}

pub struct AddressedRandomAccessOrdsInner {
    binary: BoxedBinaryDocValuesEnum,
    ordinals: Box<dyn LongValues>,
    ord_index: MixinMonotonicLongValues,
    value_count: usize,
}

impl AddressedRandomAccessOrdsInner {
    fn new(
        binary: Box<dyn LongBinaryDocValues>,
        ordinals: Box<dyn LongValues>,
        ord_index: MixinMonotonicLongValues,
        value_count: usize,
    ) -> Self {
        AddressedRandomAccessOrdsInner {
            binary: BoxedBinaryDocValuesEnum::General(binary),
            ordinals,
            ord_index,
            value_count,
        }
    }

    fn with_compression(
        binary: CompressedBinaryDocValues,
        ordinals: Box<dyn LongValues>,
        ord_index: MixinMonotonicLongValues,
        value_count: usize,
    ) -> Self {
        AddressedRandomAccessOrdsInner {
            binary: BoxedBinaryDocValuesEnum::Compressed(binary),
            ordinals,
            ord_index,
            value_count,
        }
    }

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

    fn lookup_term(&self, key: &[u8]) -> Result<i64> {
        match self.binary {
            BoxedBinaryDocValuesEnum::Compressed(ref compressed_binary) => {
                let value = compressed_binary.lookup_term(key)?;
                Ok(value as i64)
            }
            _ => {
                let mut low = 0_i64;
                let mut high = self.value_count as i64 - 1;
                while low <= high {
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
        }
    }
}

#[derive(Clone)]
pub struct TabledRandomAccessOrds {
    inner: Arc<TabledRandomAccessOrdsInner>,
}

impl TabledRandomAccessOrds {
    pub fn new(
        binary: Box<dyn LongBinaryDocValues>,
        ordinals: Box<dyn LongValues>,
        table: Vec<i64>,
        table_offsets: Vec<i32>,
        value_count: usize,
    ) -> Self {
        let inner =
            TabledRandomAccessOrdsInner::new(binary, ordinals, table, table_offsets, value_count);
        TabledRandomAccessOrds {
            inner: Arc::new(inner),
        }
    }

    pub fn with_compression(
        binary: CompressedBinaryDocValues,
        ordinals: Box<dyn LongValues>,
        table: Vec<i64>,
        table_offsets: Vec<i32>,
        value_count: usize,
    ) -> Self {
        let inner = TabledRandomAccessOrdsInner::with_compression(
            binary,
            ordinals,
            table,
            table_offsets,
            value_count,
        );
        TabledRandomAccessOrds {
            inner: Arc::new(inner),
        }
    }
}
impl SortedSetDocValues for TabledRandomAccessOrds {
    fn set_document(&self, doc: DocId) -> Result<SortedSetDocValuesContext> {
        self.inner.set_document(doc)
    }

    fn next_ord(&self, ctx: &mut SortedSetDocValuesContext) -> Result<i64> {
        self.inner.next_ord(ctx)
    }

    fn lookup_ord(&self, ord: i64) -> Result<Vec<u8>> {
        self.inner.lookup_ord(ord)
    }

    fn get_value_count(&self) -> usize {
        self.inner.value_count
    }

    fn lookup_term(&self, key: &[u8]) -> Result<i64> {
        self.inner.lookup_term(key)
    }
    fn term_iterator(&self) -> Result<DocValuesTermIterator> {
        match self.inner.binary {
            BoxedBinaryDocValuesEnum::Compressed(ref bin) => {
                let boxed = bin.get_term_iterator()?;
                Ok(DocValuesTermIterator::comp_bin(boxed))
            }
            _ => {
                let ti = SortedSetDocValuesTermIterator::new(self.clone());
                Ok(DocValuesTermIterator::sorted_set_table(ti))
            }
        }
    }
}

impl RandomAccessOrds for TabledRandomAccessOrds {
    fn ord_at(&self, ctx: &SortedSetDocValuesContext, index: i32) -> Result<i64> {
        let position = ctx.0 + i64::from(index);
        let value = self.inner.table[position as usize];
        Ok(value)
    }

    fn cardinality(&self, ctx: &SortedSetDocValuesContext) -> i32 {
        (ctx.2 - ctx.0) as i32
    }
}

struct TabledRandomAccessOrdsInner {
    binary: BoxedBinaryDocValuesEnum,
    ordinals: Box<dyn LongValues>,
    table: Vec<i64>,
    table_offsets: Vec<i32>,
    value_count: usize,
}

impl TabledRandomAccessOrdsInner {
    pub fn new(
        binary: Box<dyn LongBinaryDocValues>,
        ordinals: Box<dyn LongValues>,
        table: Vec<i64>,
        table_offsets: Vec<i32>,
        value_count: usize,
    ) -> Self {
        TabledRandomAccessOrdsInner {
            binary: BoxedBinaryDocValuesEnum::General(binary),
            ordinals,
            table,
            table_offsets,
            value_count,
        }
    }

    pub fn with_compression(
        binary: CompressedBinaryDocValues,
        ordinals: Box<dyn LongValues>,
        table: Vec<i64>,
        table_offsets: Vec<i32>,
        value_count: usize,
    ) -> Self {
        TabledRandomAccessOrdsInner {
            binary: BoxedBinaryDocValuesEnum::Compressed(binary),
            ordinals,
            table,
            table_offsets,
            value_count,
        }
    }

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

    fn lookup_term(&self, key: &[u8]) -> Result<i64> {
        match self.binary {
            BoxedBinaryDocValuesEnum::Compressed(ref binary) => {
                let val = binary.lookup_term(key)?;
                Ok(val as i64)
            }
            _ => {
                let mut low = 0_i64;
                let mut high = self.value_count as i64 - 1;
                while low <= high {
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
        }
    }
}
