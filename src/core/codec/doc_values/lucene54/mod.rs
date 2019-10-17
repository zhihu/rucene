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

mod lucene54_doc_values_consumer;

pub use self::lucene54_doc_values_consumer::*;

mod lucene54_doc_values_format;

pub use self::lucene54_doc_values_format::*;

mod lucene54_doc_values_producer;

pub use self::lucene54_doc_values_producer::*;

mod doc_values_provider;

pub use self::doc_values_provider::*;

mod doc_values_term_iterator;

pub use self::doc_values_term_iterator::*;

use core::codec::doc_values::{
    BinaryDocValues, BinaryDocValuesProvider, NumericDocValues, NumericDocValuesProvider,
    SortedDocValues, SortedNumericDocValues, SortedSetDocValues, NO_MORE_ORDS,
};
use core::codec::posting_iterator::EmptyPostingIterator;
use core::codec::terms::{EmptyTermIterator, OrdTermState, SeekStatus, TermIterator};
use core::codec::Codec;
use core::index::reader::SearchLeafReader;
use core::store::io::IndexInput;
use core::util::packed::{
    DirectMonotonicMeta, DirectPackedReader, MixinMonotonicLongValues, MonotonicBlockPackedReader,
};
use core::util::{
    Bits, BitsMut, CloneableLongValues, DocId, LiveBits, LongValues, MatchAllBits, MatchNoBits,
    PagedBytesReader, SparseBits,
};
use error::Result;
use std::sync::Arc;

/// provide utility methods and constants for DocValues
pub struct DocValues;

impl DocValues {
    pub fn singleton_sorted_doc_values<T: SortedDocValues>(
        dv: T,
    ) -> SingletonSortedSetDocValues<T> {
        SingletonSortedSetDocValues::new(dv)
    }

    pub fn singleton_sorted_numeric_doc_values<DV: CloneableNumericDocValues, B: BitsMut>(
        numeric_doc_values_in: DV,
        docs_with_field: B,
    ) -> SingletonSortedNumericDocValues<DV, B> {
        SingletonSortedNumericDocValues::new(numeric_doc_values_in, docs_with_field)
    }

    pub fn docs_with_value_sorted(dv: Box<dyn SortedDocValues>, max_doc: i32) -> Box<dyn BitsMut> {
        Box::new(SortedDocValuesBits { dv, max_doc })
    }

    pub fn docs_with_value_sorted_set(
        dv: Box<dyn SortedSetDocValues>,
        max_doc: i32,
    ) -> Box<dyn BitsMut> {
        Box::new(SortedSetDocValuesBits { dv, max_doc })
    }

    pub fn docs_with_value_sorted_numeric(
        dv: Box<dyn SortedNumericDocValues>,
        max_doc: i32,
    ) -> Box<dyn BitsMut> {
        Box::new(SortedNumericDocValuesBits { dv, max_doc })
    }

    pub fn get_docs_with_field<C: Codec>(
        reader: &SearchLeafReader<C>,
        field: &str,
    ) -> Result<Box<dyn BitsMut>> {
        reader.get_docs_with_field(field)
    }

    pub fn get_numeric<C: Codec>(
        reader: &SearchLeafReader<C>,
        field: &str,
    ) -> Result<Box<dyn NumericDocValues>> {
        reader.get_numeric_doc_values(field)
    }

    pub fn get_binary<C: Codec>(
        reader: &SearchLeafReader<C>,
        field: &str,
    ) -> Result<Box<dyn BinaryDocValues>> {
        reader.get_binary_doc_values(field)
    }

    pub fn get_sorted<C: Codec>(
        reader: &SearchLeafReader<C>,
        field: &str,
    ) -> Result<Box<dyn SortedDocValues>> {
        reader.get_sorted_doc_values(field)
    }

    pub fn get_sorted_numeric<C: Codec>(
        reader: &SearchLeafReader<C>,
        field: &str,
    ) -> Result<Box<dyn SortedNumericDocValues>> {
        reader.get_sorted_numeric_doc_values(field)
    }

    pub fn get_sorted_set<C: Codec>(
        reader: &SearchLeafReader<C>,
        field: &str,
    ) -> Result<Box<dyn SortedSetDocValues>> {
        reader.get_sorted_set_doc_values(field)
    }

    pub fn unwrap_singleton<DV: SortedNumericDocValues>(
        dv: &DV,
    ) -> Option<Box<dyn NumericDocValues>> {
        dv.get_numeric_doc_values()
    }
}

struct SortedDocValuesBits {
    dv: Box<dyn SortedDocValues>,
    max_doc: i32,
}

impl BitsMut for SortedDocValuesBits {
    fn get(&mut self, index: usize) -> Result<bool> {
        let ord = self.dv.get_ord(index as DocId)?;
        Ok(ord >= 0)
    }

    fn len(&self) -> usize {
        self.max_doc as usize
    }
}

struct SortedSetDocValuesBits {
    dv: Box<dyn SortedSetDocValues>,
    max_doc: i32,
}

impl BitsMut for SortedSetDocValuesBits {
    fn get(&mut self, index: usize) -> Result<bool> {
        self.dv.set_document(index as DocId)?;
        let ord = self.dv.next_ord()?;
        Ok(ord != NO_MORE_ORDS)
    }

    fn len(&self) -> usize {
        self.max_doc as usize
    }
}

struct SortedNumericDocValuesBits {
    dv: Box<dyn SortedNumericDocValues>,
    max_doc: i32,
}

impl BitsMut for SortedNumericDocValuesBits {
    fn get(&mut self, index: usize) -> Result<bool> {
        self.dv.set_document(index as DocId)?;
        Ok(self.dv.count() > 0)
    }

    fn len(&self) -> usize {
        self.max_doc as usize
    }
}

#[derive(Debug, Copy, Clone)]
pub enum NumberType {
    // Dense ordinals
    ORDINAL,
    // Random long values
    VALUE,
}

pub struct EmptyLongValues;

impl LongValues for EmptyLongValues {
    fn get64(&self, _index: i64) -> Result<i64> {
        Ok(0)
    }
}

impl NumericDocValues for EmptyLongValues {
    fn get(&self, _doc_id: DocId) -> Result<i64> {
        Ok(0)
    }
}

#[derive(Clone)]
pub struct LiveLongValues {
    live: LiveBitsEnum,
    constant: i64,
}

impl LiveLongValues {
    pub fn new(live: LiveBitsEnum, constant: i64) -> Self {
        LiveLongValues { live, constant }
    }
}

impl LongValues for LiveLongValues {
    fn get64(&self, index: i64) -> Result<i64> {
        if self.live.get(index as usize)? {
            Ok(self.constant)
        } else {
            Ok(0)
        }
    }
}

impl NumericDocValues for LiveLongValues {
    fn get(&self, doc_id: DocId) -> Result<i64> {
        self.get64(i64::from(doc_id))
    }
}

#[derive(Clone)]
pub struct DeltaLongValues {
    values: DirectPackedReader,
    delta: i64,
}

impl DeltaLongValues {
    pub fn new(values: DirectPackedReader, delta: i64) -> Self {
        DeltaLongValues { values, delta }
    }
}

impl LongValues for DeltaLongValues {
    fn get64(&self, index: i64) -> Result<i64> {
        let packed = self.values.get64(index)?;
        Ok(self.delta + packed)
    }
}

impl NumericDocValues for DeltaLongValues {
    fn get(&self, doc_id: DocId) -> Result<i64> {
        self.get64(doc_id as i64)
    }
}

#[derive(Clone)]
pub struct GcdLongValues {
    quotient_reader: DirectPackedReader,
    base: i64,
    mult: i64,
}

impl GcdLongValues {
    pub fn new(quotient_reader: DirectPackedReader, base: i64, mult: i64) -> Self {
        GcdLongValues {
            quotient_reader,
            base,
            mult,
        }
    }
}

impl LongValues for GcdLongValues {
    fn get64(&self, index: i64) -> Result<i64> {
        let val = self.quotient_reader.get64(index)?;
        Ok(self.base + self.mult * val)
    }
}

impl NumericDocValues for GcdLongValues {
    fn get(&self, doc_id: DocId) -> Result<i64> {
        self.get64(i64::from(doc_id))
    }
}

#[derive(Clone)]
pub struct TableLongValues {
    ords: DirectPackedReader,
    table: Arc<[i64]>,
}

impl TableLongValues {
    pub fn new(ords: DirectPackedReader, table: Vec<i64>) -> TableLongValues {
        TableLongValues {
            ords,
            table: Arc::from(Box::from(table)),
        }
    }
}

impl LongValues for TableLongValues {
    fn get64(&self, index: i64) -> Result<i64> {
        self.ords.get64(index).map(|val| self.table[val as usize])
    }
}

impl NumericDocValues for TableLongValues {
    fn get(&self, doc_id: DocId) -> Result<i64> {
        self.get64(i64::from(doc_id))
    }
}

pub struct SparseLongValues<T: LongValues + Clone> {
    docs_with_field: SparseBits<T>,
    values: Box<dyn CloneableLongValues>,
    missing_value: i64,
}

impl<T: LongValues + Clone> Clone for SparseLongValues<T> {
    fn clone(&self) -> Self {
        Self {
            docs_with_field: self.docs_with_field.clone(),
            values: self.values.cloned(),
            missing_value: self.missing_value,
        }
    }
}

impl<T: LongValues + Clone> SparseLongValues<T> {
    pub fn new(
        docs_with_field: SparseBits<T>,
        values: Box<dyn CloneableLongValues>,
        missing_value: i64,
    ) -> Self {
        SparseLongValues {
            docs_with_field,
            values,
            missing_value,
        }
    }

    pub fn docs_with_field_clone(&self) -> SparseBits<T> {
        self.docs_with_field.clone()
    }
}

impl<T: LongValues + Clone + 'static> LongValues for SparseLongValues<T> {
    fn get64(&self, index: i64) -> Result<i64> {
        let mut ctx = self.docs_with_field.context();
        let exists = self.docs_with_field.get64(&mut ctx, index)?;
        if exists {
            self.values.get64(ctx.index)
        } else {
            Ok(self.missing_value)
        }
    }

    fn get64_mut(&mut self, index: i64) -> Result<i64> {
        let exists = BitsMut::get(&mut self.docs_with_field, index as usize)?;
        if exists {
            self.values.get64_mut(self.docs_with_field.ctx.index)
        } else {
            Ok(self.missing_value)
        }
    }
}

impl<T: LongValues + Clone + 'static> NumericDocValues for SparseLongValues<T> {
    fn get(&self, doc_id: DocId) -> Result<i64> {
        self.get64(i64::from(doc_id))
    }

    fn get_mut(&mut self, doc_id: DocId) -> Result<i64> {
        self.get64_mut(i64::from(doc_id))
    }
}

impl<T: LongValues + Clone + 'static> CloneableNumericDocValues for SparseLongValues<T> {
    fn clone_box(&self) -> Box<dyn NumericDocValues> {
        Box::new(self.clone())
    }
}

impl<T: LongValues + Clone + 'static> NumericDocValuesProvider for SparseLongValues<T> {
    fn get(&self) -> Result<Box<dyn NumericDocValues>> {
        Ok(Box::new(self.clone()))
    }
}

#[derive(Clone)]
pub enum LiveBitsEnum {
    Bits(LiveBits),
    All(MatchAllBits),
    None(MatchNoBits),
}

impl LiveBitsEnum {
    fn into_bits_mut(self) -> Box<dyn BitsMut> {
        match self {
            LiveBitsEnum::Bits(b) => Box::new(b),
            LiveBitsEnum::All(b) => Box::new(b),
            LiveBitsEnum::None(b) => Box::new(b),
        }
    }
}

impl Bits for LiveBitsEnum {
    fn get(&self, index: usize) -> Result<bool> {
        match self {
            LiveBitsEnum::Bits(b) => b.get(index),
            LiveBitsEnum::All(b) => b.get(index),
            LiveBitsEnum::None(b) => b.get(index),
        }
    }

    fn len(&self) -> usize {
        match self {
            LiveBitsEnum::Bits(b) => Bits::len(b),
            LiveBitsEnum::All(b) => Bits::len(b),
            LiveBitsEnum::None(b) => Bits::len(b),
        }
    }
    fn is_empty(&self) -> bool {
        match self {
            LiveBitsEnum::Bits(b) => b.is_empty(),
            LiveBitsEnum::All(b) => b.is_empty(),
            LiveBitsEnum::None(b) => b.is_empty(),
        }
    }
}

pub struct ReverseTermsIndex {
    pub term_addresses: MonotonicBlockPackedReader,
    pub terms: PagedBytesReader,
}

/// meta-data entry for a numeric docvalues field
struct NumericEntry {
    /// offset to the bitset representing docsWithField, or -1 if no documents have missing
    /// values
    missing_offset: i64,
    offset: i64,
    end_offset: i64,
    count: i64,
    bits_per_value: i32,
    format: i32,
    min_value: i64,
    gcd: i64,
    num_docs_with_value: i64,
    number_type: NumberType,
    table: Vec<i64>,
    monotonic_meta: Option<Arc<DirectMonotonicMeta>>,
    non_missing_values: Option<Arc<NumericEntry>>,
}

impl NumericEntry {
    pub fn new() -> Self {
        NumericEntry {
            missing_offset: 0,
            offset: 0,
            end_offset: 0,
            count: 0,
            bits_per_value: 0,
            format: 0,
            min_value: 0,
            gcd: 0,
            num_docs_with_value: 0,
            number_type: NumberType::VALUE,
            table: Vec::new(),
            monotonic_meta: None,
            non_missing_values: None,
        }
    }
}

/// metadata entry for a binary docvalues field
#[derive(Clone)]
pub struct BinaryEntry {
    missing_offset: i64,
    offset: i64,
    pub count: i64,
    min_length: i32,
    pub max_length: i32,
    // offset to the addressing data that maps a value to its slice of the [u8]
    addresses_offset: i64,
    addresses_end_offset: i64,

    reverse_index_offset: i64,

    // packed ints version used to encode addressing information
    packed_ints_version: i32,
    // packed ints blocksize
    block_size: i32,

    format: i32,
    addresses_meta: Option<Arc<DirectMonotonicMeta>>,
}

impl Default for BinaryEntry {
    fn default() -> Self {
        BinaryEntry {
            missing_offset: 0,
            offset: 0,
            count: 0,
            min_length: 0,
            max_length: 0,
            addresses_offset: 0,
            addresses_end_offset: 0,
            reverse_index_offset: 0,
            packed_ints_version: 0,
            block_size: 0,
            format: 0,
            addresses_meta: None,
        }
    }
}

#[derive(Clone)]
struct SortedSetEntry {
    format: i32,
    table: Vec<i64>,
    table_offsets: Vec<i32>,
}

impl SortedSetEntry {
    pub fn new() -> Self {
        SortedSetEntry {
            format: 0,
            table: Vec::new(),
            table_offsets: Vec::new(),
        }
    }
}

pub struct CompressedBinaryDocValues {
    iterator: CompressedBinaryTermIterator,
}

impl CompressedBinaryDocValues {
    fn new(
        bytes: &BinaryEntry,
        addresses: Arc<MonotonicBlockPackedReader>,
        reverse_index: Arc<ReverseTermsIndex>,
        data: Box<dyn IndexInput>,
    ) -> Result<Self> {
        let num_reverse_index_values = reverse_index.term_addresses.size() as i64;
        let num_index_values = addresses.size() as i64;

        let iterator = CompressedBinaryTermIterator::new(
            data,
            bytes.max_length as usize,
            num_reverse_index_values,
            reverse_index,
            addresses,
            bytes.count,
            num_index_values,
        )?;
        Ok(Self { iterator })
    }
}

impl CompressedBinaryDocValues {
    pub fn lookup_term(&mut self, key: &[u8]) -> Result<i64> {
        match self.iterator.seek_ceil(key)? {
            SeekStatus::Found => self.iterator.ord(),
            SeekStatus::NotFound => {
                let val = -self.iterator.ord()? - 1;
                Ok(val)
            }
            _ => Ok(-self.iterator.num_values() - 1),
        }
    }

    pub fn get_term_iterator(&self) -> Result<CompressedBinaryTermIterator> {
        self.iterator.clone()
    }

    fn clone(&self) -> Result<Self> {
        self.iterator.clone().map(|iterator| Self { iterator })
    }
}

impl LongBinaryDocValues for CompressedBinaryDocValues {
    fn get64(&mut self, id: i64) -> Result<Vec<u8>> {
        self.iterator.seek_exact_ord(id)?;
        self.iterator.term().map(|t| t.to_vec())
    }

    fn clone_long(&self) -> Result<Box<dyn LongBinaryDocValues>> {
        Ok(Box::new(self.clone()?))
    }
}

impl BinaryDocValues for CompressedBinaryDocValues {
    fn get(&mut self, doc_id: DocId) -> Result<Vec<u8>> {
        self.get64(i64::from(doc_id))
    }
}

impl BinaryDocValuesProvider for CompressedBinaryDocValues {
    fn get(&self) -> Result<Box<dyn BinaryDocValues>> {
        Ok(Box::new(self.clone()?))
    }
}

pub enum TailoredBoxedBinaryDocValuesEnum {
    Fixed(FixedBinaryDocValues),
    Variable(VariableBinaryDocValues<MixinMonotonicLongValues>),
    Compressed(CompressedBinaryDocValues),
}

impl TailoredBoxedBinaryDocValuesEnum {
    pub fn clone(&self) -> Result<Self> {
        match self {
            TailoredBoxedBinaryDocValuesEnum::Fixed(b) => {
                b.clone().map(TailoredBoxedBinaryDocValuesEnum::Fixed)
            }
            TailoredBoxedBinaryDocValuesEnum::Variable(b) => {
                b.clone().map(TailoredBoxedBinaryDocValuesEnum::Variable)
            }
            TailoredBoxedBinaryDocValuesEnum::Compressed(b) => {
                b.clone().map(TailoredBoxedBinaryDocValuesEnum::Compressed)
            }
        }
    }
}

impl LongBinaryDocValues for TailoredBoxedBinaryDocValuesEnum {
    fn get64(&mut self, doc_id: i64) -> Result<Vec<u8>> {
        match self {
            TailoredBoxedBinaryDocValuesEnum::Fixed(b) => b.get64(doc_id),
            TailoredBoxedBinaryDocValuesEnum::Variable(b) => b.get64(doc_id),
            TailoredBoxedBinaryDocValuesEnum::Compressed(b) => b.get64(doc_id),
        }
    }

    fn clone_long(&self) -> Result<Box<dyn LongBinaryDocValues>> {
        match self {
            TailoredBoxedBinaryDocValuesEnum::Fixed(b) => Ok(Box::new(b.clone()?)),
            TailoredBoxedBinaryDocValuesEnum::Variable(b) => Ok(Box::new(b.clone()?)),
            TailoredBoxedBinaryDocValuesEnum::Compressed(b) => Ok(Box::new(b.clone()?)),
        }
    }
}

impl BinaryDocValues for TailoredBoxedBinaryDocValuesEnum {
    fn get(&mut self, doc_id: DocId) -> Result<Vec<u8>> {
        match self {
            TailoredBoxedBinaryDocValuesEnum::Fixed(b) => b.get(doc_id),
            TailoredBoxedBinaryDocValuesEnum::Variable(b) => b.get(doc_id),
            TailoredBoxedBinaryDocValuesEnum::Compressed(b) => b.get(doc_id),
        }
    }
}

#[derive(Clone)]
pub enum NumericLongValuesEnum {
    Live(LiveLongValues),
    Delta(DeltaLongValues),
    Gcd(GcdLongValues),
    Table(TableLongValues),
    Sparse(SparseLongValues<MixinMonotonicLongValues>),
}

impl LongValues for NumericLongValuesEnum {
    fn get64(&self, index: i64) -> Result<i64> {
        match self {
            NumericLongValuesEnum::Live(l) => l.get64(index),
            NumericLongValuesEnum::Delta(l) => l.get64(index),
            NumericLongValuesEnum::Gcd(l) => l.get64(index),
            NumericLongValuesEnum::Table(l) => l.get64(index),
            NumericLongValuesEnum::Sparse(l) => l.get64(index),
        }
    }

    fn get64_mut(&mut self, index: i64) -> Result<i64> {
        match self {
            NumericLongValuesEnum::Live(l) => l.get64_mut(index),
            NumericLongValuesEnum::Delta(l) => l.get64_mut(index),
            NumericLongValuesEnum::Gcd(l) => l.get64_mut(index),
            NumericLongValuesEnum::Table(l) => l.get64_mut(index),
            NumericLongValuesEnum::Sparse(l) => l.get64_mut(index),
        }
    }
}

impl NumericDocValuesProvider for NumericLongValuesEnum {
    fn get(&self) -> Result<Box<dyn NumericDocValues>> {
        match self {
            NumericLongValuesEnum::Live(l) => Ok(Box::new(l.clone())),
            NumericLongValuesEnum::Delta(l) => Ok(Box::new(l.clone())),
            NumericLongValuesEnum::Gcd(l) => Ok(Box::new(l.clone())),
            NumericLongValuesEnum::Table(l) => Ok(Box::new(l.clone())),
            NumericLongValuesEnum::Sparse(l) => Ok(l.clone_box()),
        }
    }
}

impl NumericDocValues for NumericLongValuesEnum {
    fn get(&self, doc_id: DocId) -> Result<i64> {
        match self {
            NumericLongValuesEnum::Live(l) => l.get(doc_id),
            NumericLongValuesEnum::Delta(l) => l.get(doc_id),
            NumericLongValuesEnum::Gcd(l) => l.get(doc_id),
            NumericLongValuesEnum::Table(l) => l.get(doc_id),
            NumericLongValuesEnum::Sparse(l) => NumericDocValues::get(l, doc_id),
        }
    }

    fn get_mut(&mut self, doc_id: DocId) -> Result<i64> {
        match self {
            NumericLongValuesEnum::Live(l) => l.get_mut(doc_id),
            NumericLongValuesEnum::Delta(l) => l.get_mut(doc_id),
            NumericLongValuesEnum::Gcd(l) => l.get_mut(doc_id),
            NumericLongValuesEnum::Table(l) => l.get_mut(doc_id),
            NumericLongValuesEnum::Sparse(l) => NumericDocValues::get_mut(l, doc_id),
        }
    }
}

impl CloneableNumericDocValues for NumericLongValuesEnum {
    fn clone_box(&self) -> Box<dyn NumericDocValues> {
        match self {
            NumericLongValuesEnum::Live(l) => Box::new(l.clone()),
            NumericLongValuesEnum::Delta(l) => Box::new(l.clone()),
            NumericLongValuesEnum::Gcd(l) => Box::new(l.clone()),
            NumericLongValuesEnum::Table(l) => Box::new(l.clone()),
            NumericLongValuesEnum::Sparse(l) => l.clone_box(),
        }
    }
}

pub enum DocValuesTermIteratorEnum {
    CompBin(CompressedBinaryTermIterator),
    Sorted(SortedDocValuesTermIterator<TailoredSortedDocValues>),
    SortedSetAddr(SortedSetDocValuesTermIterator<AddressedRandomAccessOrds>),
    SortedSetTable(SortedSetDocValuesTermIterator<TabledRandomAccessOrds>),
    Empty(EmptyTermIterator),
}

/// implements a `TermIterator` wrapping a provided `SortedDocValues`
pub struct DocValuesTermIterator(DocValuesTermIteratorEnum);

impl DocValuesTermIterator {
    pub fn comp_bin(d: CompressedBinaryTermIterator) -> Self {
        DocValuesTermIterator(DocValuesTermIteratorEnum::CompBin(d))
    }
    pub fn sorted(d: SortedDocValuesTermIterator<TailoredSortedDocValues>) -> Self {
        DocValuesTermIterator(DocValuesTermIteratorEnum::Sorted(d))
    }
    pub fn sorted_set_addr(d: SortedSetDocValuesTermIterator<AddressedRandomAccessOrds>) -> Self {
        DocValuesTermIterator(DocValuesTermIteratorEnum::SortedSetAddr(d))
    }
    pub fn sorted_set_table(d: SortedSetDocValuesTermIterator<TabledRandomAccessOrds>) -> Self {
        DocValuesTermIterator(DocValuesTermIteratorEnum::SortedSetTable(d))
    }
    pub fn empty() -> Self {
        DocValuesTermIterator(DocValuesTermIteratorEnum::Empty(EmptyTermIterator {}))
    }
}

impl TermIterator for DocValuesTermIterator {
    type Postings = EmptyPostingIterator;
    type TermState = OrdTermState;
    fn next(&mut self) -> Result<Option<Vec<u8>>> {
        match &mut self.0 {
            DocValuesTermIteratorEnum::CompBin(t) => t.next(),
            DocValuesTermIteratorEnum::Sorted(t) => t.next(),
            DocValuesTermIteratorEnum::SortedSetAddr(t) => t.next(),
            DocValuesTermIteratorEnum::SortedSetTable(t) => t.next(),
            DocValuesTermIteratorEnum::Empty(t) => t.next(),
        }
    }

    fn seek_exact(&mut self, text: &[u8]) -> Result<bool> {
        match &mut self.0 {
            DocValuesTermIteratorEnum::CompBin(t) => t.seek_exact(text),
            DocValuesTermIteratorEnum::Sorted(t) => t.seek_exact(text),
            DocValuesTermIteratorEnum::SortedSetAddr(t) => t.seek_exact(text),
            DocValuesTermIteratorEnum::SortedSetTable(t) => t.seek_exact(text),
            DocValuesTermIteratorEnum::Empty(t) => t.seek_exact(text),
        }
    }

    fn seek_ceil(&mut self, text: &[u8]) -> Result<SeekStatus> {
        match &mut self.0 {
            DocValuesTermIteratorEnum::CompBin(t) => t.seek_ceil(text),
            DocValuesTermIteratorEnum::Sorted(t) => t.seek_ceil(text),
            DocValuesTermIteratorEnum::SortedSetAddr(t) => t.seek_ceil(text),
            DocValuesTermIteratorEnum::SortedSetTable(t) => t.seek_ceil(text),
            DocValuesTermIteratorEnum::Empty(t) => t.seek_ceil(text),
        }
    }

    fn seek_exact_ord(&mut self, ord: i64) -> Result<()> {
        match &mut self.0 {
            DocValuesTermIteratorEnum::CompBin(t) => t.seek_exact_ord(ord),
            DocValuesTermIteratorEnum::Sorted(t) => t.seek_exact_ord(ord),
            DocValuesTermIteratorEnum::SortedSetAddr(t) => t.seek_exact_ord(ord),
            DocValuesTermIteratorEnum::SortedSetTable(t) => t.seek_exact_ord(ord),
            DocValuesTermIteratorEnum::Empty(t) => t.seek_exact_ord(ord),
        }
    }

    fn seek_exact_state(&mut self, text: &[u8], state: &Self::TermState) -> Result<()> {
        match &mut self.0 {
            DocValuesTermIteratorEnum::CompBin(_) => unreachable!(),
            DocValuesTermIteratorEnum::Sorted(t) => t.seek_exact_state(text, state),
            DocValuesTermIteratorEnum::SortedSetAddr(t) => t.seek_exact_state(text, state),
            DocValuesTermIteratorEnum::SortedSetTable(t) => t.seek_exact_state(text, state),
            DocValuesTermIteratorEnum::Empty(_) => unreachable!(),
        }
    }

    fn term(&self) -> Result<&[u8]> {
        match &self.0 {
            DocValuesTermIteratorEnum::CompBin(t) => t.term(),
            DocValuesTermIteratorEnum::Sorted(t) => t.term(),
            DocValuesTermIteratorEnum::SortedSetAddr(t) => t.term(),
            DocValuesTermIteratorEnum::SortedSetTable(t) => t.term(),
            DocValuesTermIteratorEnum::Empty(t) => t.term(),
        }
    }

    fn ord(&self) -> Result<i64> {
        match &self.0 {
            DocValuesTermIteratorEnum::CompBin(t) => t.ord(),
            DocValuesTermIteratorEnum::Sorted(t) => t.ord(),
            DocValuesTermIteratorEnum::SortedSetAddr(t) => t.ord(),
            DocValuesTermIteratorEnum::SortedSetTable(t) => t.ord(),
            DocValuesTermIteratorEnum::Empty(t) => t.ord(),
        }
    }

    fn doc_freq(&mut self) -> Result<i32> {
        match &mut self.0 {
            DocValuesTermIteratorEnum::CompBin(t) => t.doc_freq(),
            DocValuesTermIteratorEnum::Sorted(t) => t.doc_freq(),
            DocValuesTermIteratorEnum::SortedSetAddr(t) => t.doc_freq(),
            DocValuesTermIteratorEnum::SortedSetTable(t) => t.doc_freq(),
            DocValuesTermIteratorEnum::Empty(t) => t.doc_freq(),
        }
    }

    fn total_term_freq(&mut self) -> Result<i64> {
        match &mut self.0 {
            DocValuesTermIteratorEnum::CompBin(t) => t.total_term_freq(),
            DocValuesTermIteratorEnum::Sorted(t) => t.total_term_freq(),
            DocValuesTermIteratorEnum::SortedSetAddr(t) => t.total_term_freq(),
            DocValuesTermIteratorEnum::SortedSetTable(t) => t.total_term_freq(),
            DocValuesTermIteratorEnum::Empty(t) => t.total_term_freq(),
        }
    }

    fn postings(&mut self) -> Result<Self::Postings> {
        match &mut self.0 {
            DocValuesTermIteratorEnum::CompBin(t) => t.postings(),
            DocValuesTermIteratorEnum::Sorted(t) => t.postings(),
            DocValuesTermIteratorEnum::SortedSetAddr(t) => t.postings(),
            DocValuesTermIteratorEnum::SortedSetTable(t) => t.postings(),
            DocValuesTermIteratorEnum::Empty(t) => t.postings(),
        }
    }

    fn postings_with_flags(&mut self, flags: u16) -> Result<Self::Postings> {
        match &mut self.0 {
            DocValuesTermIteratorEnum::CompBin(t) => t.postings_with_flags(flags),
            DocValuesTermIteratorEnum::Sorted(t) => t.postings_with_flags(flags),
            DocValuesTermIteratorEnum::SortedSetAddr(t) => t.postings_with_flags(flags),
            DocValuesTermIteratorEnum::SortedSetTable(t) => t.postings_with_flags(flags),
            DocValuesTermIteratorEnum::Empty(t) => t.postings_with_flags(flags),
        }
    }

    fn term_state(&mut self) -> Result<Self::TermState> {
        match &mut self.0 {
            DocValuesTermIteratorEnum::CompBin(_) => unimplemented!(),
            DocValuesTermIteratorEnum::Sorted(t) => t.term_state(),
            DocValuesTermIteratorEnum::SortedSetAddr(t) => t.term_state(),
            DocValuesTermIteratorEnum::SortedSetTable(t) => t.term_state(),
            DocValuesTermIteratorEnum::Empty(_) => unimplemented!(),
        }
    }

    fn is_empty(&self) -> bool {
        match &self.0 {
            DocValuesTermIteratorEnum::CompBin(t) => t.is_empty(),
            DocValuesTermIteratorEnum::Sorted(t) => t.is_empty(),
            DocValuesTermIteratorEnum::SortedSetAddr(t) => t.is_empty(),
            DocValuesTermIteratorEnum::SortedSetTable(t) => t.is_empty(),
            DocValuesTermIteratorEnum::Empty(t) => t.is_empty(),
        }
    }
}
