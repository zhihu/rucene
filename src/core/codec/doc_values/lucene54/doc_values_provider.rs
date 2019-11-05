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

use core::codec::doc_values::lucene54::{
    DocValuesTermIterator, NumericLongValuesEnum, SortedDocValuesTermIterator,
    SortedSetDocValuesTermIterator, TailoredBoxedBinaryDocValuesEnum,
};
use core::codec::doc_values::{
    BinaryDocValues, BinaryDocValuesProvider, EmptySortedSetDocValues, NumericDocValues,
    SortedDocValues, SortedDocValuesProvider, SortedNumericDocValues,
    SortedNumericDocValuesProvider, SortedSetDocValues, SortedSetDocValuesProvider, NO_MORE_ORDS,
};
use core::store::io::IndexInput;
use core::util::packed::MixinMonotonicLongValues;
use core::util::LongValues;
use core::util::{BitsMut, DocId, UnsignedShift};

use error::Result;
use std::cmp::Ordering;
use std::io::Read;
use std::ops::Deref;
use std::sync::Arc;

/// ################ BinaryDocValuesProvider
/// // internally we compose complex dv (sorted/sortedset) from other ones
pub trait LongBinaryDocValues: BinaryDocValues {
    fn get64(&mut self, doc_id: i64) -> Result<Vec<u8>>;

    fn clone_long(&self) -> Result<Box<dyn LongBinaryDocValues>>;
}

pub struct FixedBinaryDocValues {
    data: Box<dyn IndexInput>,
    buffer_len: usize,
}

impl FixedBinaryDocValues {
    pub fn new(data: Box<dyn IndexInput>, buffer_len: usize) -> Self {
        FixedBinaryDocValues { data, buffer_len }
    }

    pub fn clone(&self) -> Result<Self> {
        self.data.clone().map(|data| Self {
            data,
            buffer_len: self.buffer_len,
        })
    }
}

impl BinaryDocValuesProvider for FixedBinaryDocValues {
    fn get(&self) -> Result<Box<dyn BinaryDocValues>> {
        let data = self.data.as_ref().clone()?;
        Ok(Box::new(Self {
            data,
            buffer_len: self.buffer_len,
        }))
    }
}

impl LongBinaryDocValues for FixedBinaryDocValues {
    fn get64(&mut self, id: i64) -> Result<Vec<u8>> {
        let length = self.buffer_len;
        self.data.seek(id * length as i64)?;
        let mut buffer = vec![0u8; length];
        self.data.read_exact(&mut buffer)?;
        Ok(buffer)
    }

    fn clone_long(&self) -> Result<Box<dyn LongBinaryDocValues>> {
        Ok(Box::new(self.clone()?))
    }
}

impl BinaryDocValues for FixedBinaryDocValues {
    fn get(&mut self, doc_id: DocId) -> Result<Vec<u8>> {
        FixedBinaryDocValues::get64(self, i64::from(doc_id))
    }
}

pub struct VariableBinaryDocValues<T: LongValues + Clone + 'static> {
    addresses: T,
    data: Box<dyn IndexInput>,
}

impl<T: LongValues + Clone + 'static> VariableBinaryDocValues<T> {
    pub fn new(addresses: T, data: Box<dyn IndexInput>, _length: usize) -> Self {
        VariableBinaryDocValues { addresses, data }
    }

    pub fn clone(&self) -> Result<Self> {
        self.data.clone().map(|data| Self {
            addresses: self.addresses.clone(),
            data,
        })
    }
}

impl<T: LongValues + Clone + 'static> BinaryDocValuesProvider for VariableBinaryDocValues<T> {
    fn get(&self) -> Result<Box<dyn BinaryDocValues>> {
        let data = self.data.clone()?;
        Ok(Box::new(Self {
            addresses: self.addresses.clone(),
            data,
        }))
    }
}

impl<T: LongValues + Clone + 'static> LongBinaryDocValues for VariableBinaryDocValues<T> {
    fn get64(&mut self, id: i64) -> Result<Vec<u8>> {
        let start_address = self.addresses.get64(id)?;
        let end_address = self.addresses.get64(id + 1)?;
        let length = (end_address - start_address) as usize;
        self.data.seek(start_address)?;
        let mut buffer = vec![0u8; length];
        self.data.read_exact(&mut buffer)?;
        Ok(buffer)
    }

    fn clone_long(&self) -> Result<Box<dyn LongBinaryDocValues>> {
        Ok(Box::new(self.clone()?))
    }
}

impl<T: LongValues + Clone + 'static> BinaryDocValues for VariableBinaryDocValues<T> {
    fn get(&mut self, doc_id: DocId) -> Result<Vec<u8>> {
        self.get64(i64::from(doc_id))
    }
}

/// ################ BinaryDocValuesProvider
pub trait CloneableNumericDocValues: NumericDocValues {
    fn clone_box(&self) -> Box<dyn NumericDocValues>;
}

impl<T: CloneableNumericDocValues + ?Sized> CloneableNumericDocValues for Box<T> {
    fn clone_box(&self) -> Box<dyn NumericDocValues> {
        (**self).clone_box()
    }
}

pub trait CloneableNumericDocValuesProvider: Send + Sync {
    fn get(&self) -> Result<Box<dyn CloneableNumericDocValues>>;
}

impl<T: CloneableNumericDocValues + Clone + 'static> CloneableNumericDocValuesProvider for T {
    fn get(&self) -> Result<Box<dyn CloneableNumericDocValues>> {
        Ok(Box::new(self.clone()))
    }
}

/// ################ SortedNumericDocValuesProvider
#[derive(Clone)]
pub struct AddressedSortedNumericDocValues<T: LongValues + Clone> {
    values: NumericLongValuesEnum,
    ord_index: T,
    start_offset: i64,
    end_offset: i64,
}

impl<T: LongValues + Clone> AddressedSortedNumericDocValues<T> {
    pub fn new(values: NumericLongValuesEnum, ord_index: T) -> Self {
        Self {
            values,
            ord_index,
            start_offset: 0,
            end_offset: 0,
        }
    }
}

impl<T: LongValues + Clone> SortedNumericDocValues for AddressedSortedNumericDocValues<T> {
    fn set_document(&mut self, doc: i32) -> Result<()> {
        self.start_offset = self.ord_index.get_mut(doc)?;
        self.end_offset = self.ord_index.get_mut(doc + 1)?;
        Ok(())
    }

    fn value_at(&mut self, index: usize) -> Result<i64> {
        let idx = self.start_offset + index as i64;
        self.values.get64(idx)
    }

    fn count(&self) -> usize {
        (self.end_offset - self.start_offset) as usize
    }
}

impl<T: LongValues + Clone + 'static> SortedNumericDocValuesProvider
    for AddressedSortedNumericDocValues<T>
{
    fn get(&self) -> Result<Box<dyn SortedNumericDocValues>> {
        Ok(Box::new(self.clone()))
    }
}

#[derive(Clone)]
pub struct TabledSortedNumericDocValues {
    ordinals: NumericLongValuesEnum,
    table: Vec<i64>,
    offsets: Vec<i32>,
    start_offset: i32,
    end_offset: i32,
}

impl TabledSortedNumericDocValues {
    pub fn new(ordinals: NumericLongValuesEnum, table: &[i64], offsets: &[i32]) -> Self {
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
        let ord = self.ordinals.get_mut(doc)? as usize;
        self.start_offset = self.offsets[ord];
        self.end_offset = self.offsets[ord + 1];

        Ok(())
    }

    fn value_at(&mut self, index: usize) -> Result<i64> {
        let offset = self.start_offset as usize + index;
        Ok(self.table[offset])
    }

    fn count(&self) -> usize {
        (self.end_offset - self.start_offset) as usize
    }
}

impl SortedNumericDocValuesProvider for TabledSortedNumericDocValues {
    fn get(&self) -> Result<Box<dyn SortedNumericDocValues>> {
        Ok(Box::new(self.clone()))
    }
}

/// ################ SortedDocValuesProvider
impl<T: Deref<Target = dyn SortedDocValuesProvider> + Send + Sync> SortedDocValuesProvider for T {
    fn get(&self) -> Result<Box<dyn SortedDocValues>> {
        (**self).get()
    }
}

pub struct TailoredSortedDocValues {
    ordinals: NumericLongValuesEnum,
    binary: TailoredBoxedBinaryDocValuesEnum,
    value_count: usize,
}

impl TailoredSortedDocValues {
    pub fn new(
        ordinals: NumericLongValuesEnum,
        binary: TailoredBoxedBinaryDocValuesEnum,
        value_count: usize,
    ) -> Self {
        Self {
            ordinals,
            binary,
            value_count,
        }
    }
}

impl SortedDocValuesProvider for TailoredSortedDocValues {
    fn get(&self) -> Result<Box<dyn SortedDocValues>> {
        let doc_values = Self {
            ordinals: self.ordinals.clone(),
            binary: self.binary.clone()?,
            value_count: self.value_count,
        };
        Ok(Box::new(doc_values))
    }
}

impl SortedDocValues for TailoredSortedDocValues {
    fn get_ord(&mut self, doc_id: DocId) -> Result<i32> {
        self.ordinals.get_mut(doc_id).map(|v| v as i32)
    }

    fn lookup_ord(&mut self, ord: i32) -> Result<Vec<u8>> {
        self.binary.get(ord)
    }

    fn value_count(&self) -> usize {
        self.value_count
    }

    fn lookup_term(&mut self, key: &[u8]) -> Result<i32> {
        match self.binary {
            TailoredBoxedBinaryDocValuesEnum::Compressed(ref mut binary) => {
                let val = binary.lookup_term(key)? as i32;
                Ok(val)
            }
            _ => {
                // TODO: Copy from SortedDocValues#lookup_term
                let mut low = 0;
                let mut high = self.value_count as i32 - 1;
                while low <= high {
                    let mid = low + (high - low) / 2;
                    let term = self.lookup_ord(mid)?;
                    match term.as_slice().cmp(key) {
                        Ordering::Less => {
                            low = mid + 1;
                        }
                        Ordering::Greater => {
                            high = mid - 1;
                        }
                        Ordering::Equal => {
                            return Ok(mid);
                        }
                    }
                }
                Ok(-(low + 1)) // key not found
            }
        }
    }

    fn term_iterator(&self) -> Result<DocValuesTermIterator> {
        match self.binary {
            TailoredBoxedBinaryDocValuesEnum::Compressed(ref bin) => {
                let boxed = bin.get_term_iterator()?;
                Ok(DocValuesTermIterator::comp_bin(boxed))
            }
            _ => {
                let ti = SortedDocValuesTermIterator::new(self);
                Ok(DocValuesTermIterator::sorted(ti))
            }
        }
    }
}

impl BinaryDocValues for TailoredSortedDocValues {
    fn get(&mut self, doc_id: DocId) -> Result<Vec<u8>> {
        let ord = self.get_ord(doc_id)?;
        if ord == -1 {
            Ok(Vec::with_capacity(0))
        } else {
            self.lookup_ord(ord)
        }
    }
}

/// ################ SortedSetDocValuesProvider
pub struct EmptySortedSetDocValuesProvider;

impl SortedSetDocValuesProvider for EmptySortedSetDocValuesProvider {
    fn get(&self) -> Result<Box<dyn SortedSetDocValues>> {
        Ok(Box::new(EmptySortedSetDocValues))
    }
}

/// Extension of `SortedSetDocValues` that supports random access to the ordinals of a document.
///
/// Operations via this API are independent of the iterator api next_ord()
/// and do not impact its state.
///
/// Codecs can optionally extend this API if they support constant-time access
/// to ordinals for the document.
pub trait RandomAccessOrds: SortedSetDocValues {
    fn ord_at(&mut self, index: i32) -> Result<i64>;
    fn cardinality(&self) -> i32;
}

pub struct AddressedRandomAccessOrds {
    binary: TailoredBoxedBinaryDocValuesEnum,
    ordinals: NumericLongValuesEnum,
    ord_index: MixinMonotonicLongValues,
    value_count: usize,

    start_offset: i64,
    current_offset: i64,
    end_offset: i64,
}

impl AddressedRandomAccessOrds {
    pub fn new(
        binary: TailoredBoxedBinaryDocValuesEnum,
        ordinals: NumericLongValuesEnum,
        ord_index: MixinMonotonicLongValues,
        value_count: usize,
    ) -> Self {
        Self {
            binary,
            ordinals,
            ord_index,
            value_count,
            start_offset: 0,
            current_offset: 0,
            end_offset: 0,
        }
    }
}

impl SortedSetDocValues for AddressedRandomAccessOrds {
    fn set_document(&mut self, doc: DocId) -> Result<()> {
        self.start_offset = self.ord_index.get_mut(doc)?;
        self.current_offset = self.start_offset;
        self.end_offset = self.ord_index.get_mut(doc + 1)?;
        Ok(())
    }

    fn next_ord(&mut self) -> Result<i64> {
        if self.current_offset == self.end_offset {
            Ok(NO_MORE_ORDS)
        } else {
            let ord = self.ordinals.get64_mut(self.current_offset)?;
            self.current_offset += 1;
            Ok(ord)
        }
    }

    fn lookup_ord(&mut self, ord: i64) -> Result<Vec<u8>> {
        self.binary.get64(ord)
    }

    fn get_value_count(&self) -> usize {
        self.value_count
    }

    fn lookup_term(&mut self, key: &[u8]) -> Result<i64> {
        match self.binary {
            TailoredBoxedBinaryDocValuesEnum::Compressed(ref mut compressed_binary) => {
                let value = compressed_binary.lookup_term(key)?;
                Ok(value as i64)
            }
            _ => {
                let mut low = 0_i64;
                let mut high = self.value_count as i64 - 1;
                while low <= high {
                    let mid = low + (high - low) / 2;
                    let term = self.lookup_ord(mid)?;
                    match term.as_slice().cmp(key) {
                        Ordering::Less => {
                            low = mid + 1;
                        }
                        Ordering::Greater => {
                            high = mid - 1;
                        }
                        Ordering::Equal => {
                            return Ok(mid);
                        }
                    }
                }
                Ok(-(low + 1)) // key not found
            }
        }
    }

    fn term_iterator(&self) -> Result<DocValuesTermIterator> {
        match self.binary {
            TailoredBoxedBinaryDocValuesEnum::Compressed(ref bin) => {
                let boxed = bin.get_term_iterator()?;
                Ok(DocValuesTermIterator::comp_bin(boxed))
            }
            _ => {
                let ti = SortedSetDocValuesTermIterator::new(self);
                Ok(DocValuesTermIterator::sorted_set_addr(ti))
            }
        }
    }
}

impl RandomAccessOrds for AddressedRandomAccessOrds {
    fn ord_at(&mut self, index: i32) -> Result<i64> {
        let position = self.start_offset + i64::from(index);
        self.ordinals.get64_mut(position)
    }

    fn cardinality(&self) -> i32 {
        (self.end_offset - self.start_offset) as i32
    }
}

impl SortedSetDocValuesProvider for AddressedRandomAccessOrds {
    fn get(&self) -> Result<Box<dyn SortedSetDocValues>> {
        let binary = self.binary.clone()?;
        let dv = Self {
            binary,
            ordinals: self.ordinals.clone(),
            ord_index: self.ord_index.clone(),
            value_count: self.value_count,
            start_offset: self.start_offset,
            current_offset: self.current_offset,
            end_offset: self.end_offset,
        };
        Ok(Box::new(dv))
    }
}

pub struct TabledRandomAccessOrds {
    binary: TailoredBoxedBinaryDocValuesEnum,
    ordinals: NumericLongValuesEnum,
    table: Arc<[i64]>,
    table_offsets: Arc<[i32]>,
    value_count: usize,

    start_offset: i32,
    current_offset: i32,
    end_offset: i32,
}

impl TabledRandomAccessOrds {
    pub fn new(
        binary: TailoredBoxedBinaryDocValuesEnum,
        ordinals: NumericLongValuesEnum,
        table: Vec<i64>,
        table_offsets: Vec<i32>,
        value_count: usize,
    ) -> Self {
        Self {
            binary,
            ordinals,
            table: Arc::from(table),
            table_offsets: Arc::from(table_offsets),
            value_count,
            start_offset: 0,
            current_offset: 0,
            end_offset: 0,
        }
    }
}

impl SortedSetDocValues for TabledRandomAccessOrds {
    fn set_document(&mut self, doc: DocId) -> Result<()> {
        let ord = self.ordinals.get_mut(doc)? as usize;
        self.start_offset = self.table_offsets[ord];
        self.current_offset = self.start_offset;
        self.end_offset = self.table_offsets[ord as usize + 1];
        Ok(())
    }

    fn next_ord(&mut self) -> Result<i64> {
        if self.current_offset == self.end_offset {
            Ok(NO_MORE_ORDS)
        } else {
            let ord = self.table[self.current_offset as usize];
            self.current_offset += 1;
            Ok(ord)
        }
    }

    fn lookup_ord(&mut self, ord: i64) -> Result<Vec<u8>> {
        self.binary.get64(ord)
    }

    fn get_value_count(&self) -> usize {
        self.value_count
    }

    fn lookup_term(&mut self, key: &[u8]) -> Result<i64> {
        match self.binary {
            TailoredBoxedBinaryDocValuesEnum::Compressed(ref mut binary) => binary.lookup_term(key),
            _ => {
                let mut low = 0_i64;
                let mut high = self.value_count as i64 - 1;
                while low <= high {
                    let mid = low + (high - low) / 2;
                    let term = self.lookup_ord(mid)?;
                    match term.as_slice().cmp(key) {
                        Ordering::Less => {
                            low = mid + 1;
                        }
                        Ordering::Greater => {
                            high = mid - 1;
                        }
                        Ordering::Equal => {
                            return Ok(mid);
                        }
                    }
                }
                Ok(-(low + 1)) // key not found
            }
        }
    }

    fn term_iterator(&self) -> Result<DocValuesTermIterator> {
        match self.binary {
            TailoredBoxedBinaryDocValuesEnum::Compressed(ref bin) => {
                bin.get_term_iterator().map(DocValuesTermIterator::comp_bin)
            }
            _ => {
                let ti = SortedSetDocValuesTermIterator::new(self);
                Ok(DocValuesTermIterator::sorted_set_table(ti))
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

impl SortedSetDocValuesProvider for TabledRandomAccessOrds {
    fn get(&self) -> Result<Box<dyn SortedSetDocValues>> {
        let dv = Self {
            binary: self.binary.clone()?,
            ordinals: self.ordinals.clone(),
            table: self.table.clone(),
            table_offsets: self.table_offsets.clone(),
            value_count: self.value_count,
            start_offset: self.start_offset,
            current_offset: self.current_offset,
            end_offset: self.end_offset,
        };
        Ok(Box::new(dv))
    }
}

/// ################ SingletonSortedNumericDVProvider
pub struct SingletonSortedNumericDVProvider<
    P: CloneableNumericDocValuesProvider,
    B: BitsMut + Clone,
> {
    provider: P,
    bits: B,
}

impl<P, B> SingletonSortedNumericDVProvider<P, B>
where
    P: CloneableNumericDocValuesProvider,
    B: BitsMut + Clone,
{
    pub fn new(provider: P, bits: B) -> Self {
        Self { provider, bits }
    }
}

impl<P, B> SortedNumericDocValuesProvider for SingletonSortedNumericDVProvider<P, B>
where
    P: CloneableNumericDocValuesProvider,
    B: BitsMut + Clone + 'static,
{
    fn get(&self) -> Result<Box<dyn SortedNumericDocValues>> {
        let dv = self.provider.get()?;
        Ok(Box::new(SingletonSortedNumericDocValues::new(
            dv,
            self.bits.clone(),
        )))
    }
}

pub struct SingletonSortedNumericDocValues<DV: CloneableNumericDocValues, B: BitsMut> {
    numeric_doc_values_in: DV,
    docs_with_field: B,
    value: i64,
    count: usize,
}

impl<DV: CloneableNumericDocValues, B: BitsMut> SingletonSortedNumericDocValues<DV, B> {
    pub fn new(numeric_doc_values_in: DV, docs_with_field: B) -> Self {
        SingletonSortedNumericDocValues {
            numeric_doc_values_in,
            docs_with_field,
            value: 0,
            count: 0,
        }
    }

    pub fn get_numeric_doc_values_impl(&self) -> Box<dyn NumericDocValues> {
        self.numeric_doc_values_in.clone_box()
    }
}

impl<DV: CloneableNumericDocValues, B: BitsMut> SortedNumericDocValues
    for SingletonSortedNumericDocValues<DV, B>
{
    fn set_document(&mut self, doc: i32) -> Result<()> {
        let value = self.numeric_doc_values_in.get(doc)?;
        let id = self.docs_with_field.id();
        self.count = if id != 1 && value == 0 {
            let result = self.docs_with_field.get(doc as usize)?;
            if result {
                1
            } else {
                0
            }
        } else {
            1
        };
        self.value = value;
        Ok(())
    }

    fn value_at(&mut self, _index: usize) -> Result<i64> {
        Ok(self.value)
    }

    fn count(&self) -> usize {
        self.count
    }

    fn get_numeric_doc_values(&self) -> Option<Box<dyn NumericDocValues>> {
        Some(self.numeric_doc_values_in.clone_box())
    }
}

/// ################ SingletonSortedSetDVProvider
pub struct SingletonSortedSetDVProvider<T: SortedDocValuesProvider> {
    provider: T,
}

impl<T: SortedDocValuesProvider> SingletonSortedSetDVProvider<T> {
    pub fn new(provider: T) -> Self {
        Self { provider }
    }
}

impl<T: SortedDocValuesProvider> SortedSetDocValuesProvider for SingletonSortedSetDVProvider<T> {
    fn get(&self) -> Result<Box<dyn SortedSetDocValues>> {
        let sorted = self.provider.get()?;
        Ok(Box::new(SingletonSortedSetDocValues::new(sorted)))
    }
}

/// Exposes multi-valued view over a single-valued instance.
///
/// This can be used if you want to have one multi-valued implementation
/// that works for single or multi-valued types.
pub struct SingletonSortedSetDocValues<T: SortedDocValues> {
    dv_in: T,
    current_ord: i64,
    ord: i64,
}

impl<T: SortedDocValues> SingletonSortedSetDocValues<T> {
    pub fn new(dv_in: T) -> Self {
        Self {
            dv_in,
            current_ord: 0,
            ord: 0,
        }
    }
    pub fn get_sorted_doc_values(&self) -> &T {
        &self.dv_in
    }
}

impl<T: SortedDocValues> RandomAccessOrds for SingletonSortedSetDocValues<T> {
    fn ord_at(&mut self, _index: i32) -> Result<i64> {
        Ok(self.ord)
    }

    fn cardinality(&self) -> i32 {
        self.ord.unsigned_shift(63) as i32 ^ 1
    }
}

impl<T: SortedDocValues> SortedSetDocValues for SingletonSortedSetDocValues<T> {
    fn set_document(&mut self, doc: DocId) -> Result<()> {
        let v = i64::from(self.dv_in.get_ord(doc)?);
        self.ord = v;
        self.current_ord = v;
        Ok(())
    }

    fn next_ord(&mut self) -> Result<i64> {
        let v = self.current_ord;
        self.current_ord = NO_MORE_ORDS;
        Ok(v)
    }

    fn lookup_ord(&mut self, ord: i64) -> Result<Vec<u8>> {
        // cast is ok: single-valued cannot exceed i32::MAX
        self.dv_in.lookup_ord(ord as i32)
    }

    fn get_value_count(&self) -> usize {
        self.dv_in.value_count()
    }

    fn lookup_term(&mut self, key: &[u8]) -> Result<i64> {
        self.dv_in.lookup_term(key).map(i64::from)
    }

    fn term_iterator(&self) -> Result<DocValuesTermIterator> {
        self.dv_in.term_iterator()
    }
}
