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

use core::codec::doc_values::{DocValuesConsumer, NumericDocValues};
use core::codec::field_infos::FieldInfo;
use core::codec::segment_infos::SegmentWriteState;
use core::codec::{Codec, INT_BYTES, LONG_BYTES};
use core::codec::{DVSortDocComparator, SorterDocComparator, SorterDocMap};
use core::doc::DocValuesType;
use core::doc::Term;
use core::index::reader::{CachedBinaryDVs, CachedNumericDVs};
use core::search::sort_field::{SortField, SortFieldType, SortedNumericSelectorType};
use core::search::NO_MORE_DOCS;
use core::store::directory::Directory;
use core::store::io::DataOutput;
use core::util::packed::{
    LongValuesIterator, PackedLongValues, PackedLongValuesBuilder, PackedLongValuesBuilderType,
    PagedGrowableWriter, PagedMutableHugeWriter, PagedMutableWriter, DEFAULT_PAGE_SIZE,
};
use core::util::packed::{COMPACT, FAST};
use core::util::BitsRequired;
use core::util::{BitSet, BitSetIterator, FixedBitSet};
use core::util::{
    Bits, BytesRef, DocId, Numeric, PagedBytes, PagedBytesDataInput, ReusableIterator, VariantValue,
};
use core::util::{ByteBlockPool, DirectTrackingAllocator};
use core::util::{BytesRefHash, DirectByteStartArray, DEFAULT_CAPACITY};
use core::util::{Sorter, BINARY_SORT_THRESHOLD};

use error::{
    ErrorKind::{IllegalArgument, IllegalState},
    Result,
};

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::io::Read;
use std::mem;

pub trait DocValuesWriter {
    fn finish(&mut self, num_doc: i32);

    fn flush<D: Directory, DW: Directory, C: Codec, W: DocValuesConsumer>(
        &mut self,
        state: &SegmentWriteState<D, DW, C>,
        sort_map: Option<&impl SorterDocMap>,
        consumer: &mut W,
    ) -> Result<()>;

    fn get_doc_comparator(
        &mut self,
        num_doc: i32,
        sort_field: &SortField,
    ) -> Result<Box<dyn SorterDocComparator>>;
}

pub enum DocValuesWriterEnum {
    Numeric(NumericDocValuesWriter),
    Binary(BinaryDocValuesWriter),
    Sorted(SortedDocValuesWriter),
    SortedNumeric(SortedNumericDocValuesWriter),
    SortedSet(SortedSetDocValuesWriter),
}

impl DocValuesWriterEnum {
    pub fn doc_values_type(&self) -> DocValuesType {
        match self {
            DocValuesWriterEnum::Numeric(_) => DocValuesType::Numeric,
            DocValuesWriterEnum::Binary(_) => DocValuesType::Binary,
            DocValuesWriterEnum::Sorted(_) => DocValuesType::Sorted,
            DocValuesWriterEnum::SortedNumeric(_) => DocValuesType::SortedNumeric,
            DocValuesWriterEnum::SortedSet(_) => DocValuesType::SortedSet,
        }
    }
}

impl DocValuesWriter for DocValuesWriterEnum {
    fn finish(&mut self, num_doc: i32) {
        match self {
            DocValuesWriterEnum::Numeric(n) => n.finish(num_doc),
            DocValuesWriterEnum::Binary(b) => b.finish(num_doc),
            DocValuesWriterEnum::Sorted(s) => s.finish(num_doc),
            DocValuesWriterEnum::SortedNumeric(s) => s.finish(num_doc),
            DocValuesWriterEnum::SortedSet(s) => s.finish(num_doc),
        }
    }

    fn flush<D: Directory, DW: Directory, C: Codec, W: DocValuesConsumer>(
        &mut self,
        state: &SegmentWriteState<D, DW, C>,
        sort_map: Option<&impl SorterDocMap>,
        consumer: &mut W,
    ) -> Result<()> {
        match self {
            DocValuesWriterEnum::Numeric(n) => n.flush(state, sort_map, consumer),
            DocValuesWriterEnum::Binary(b) => b.flush(state, sort_map, consumer),
            DocValuesWriterEnum::Sorted(s) => s.flush(state, sort_map, consumer),
            DocValuesWriterEnum::SortedNumeric(s) => s.flush(state, sort_map, consumer),
            DocValuesWriterEnum::SortedSet(s) => s.flush(state, sort_map, consumer),
        }
    }

    fn get_doc_comparator(
        &mut self,
        num_doc: i32,
        sort_field: &SortField,
    ) -> Result<Box<dyn SorterDocComparator>> {
        match self {
            DocValuesWriterEnum::Numeric(n) => n.get_doc_comparator(num_doc, sort_field),
            DocValuesWriterEnum::Binary(b) => b.get_doc_comparator(num_doc, sort_field),
            DocValuesWriterEnum::Sorted(s) => s.get_doc_comparator(num_doc, sort_field),
            DocValuesWriterEnum::SortedNumeric(s) => s.get_doc_comparator(num_doc, sort_field),
            DocValuesWriterEnum::SortedSet(s) => s.get_doc_comparator(num_doc, sort_field),
        }
    }
}

/// Buffers up pending bytes per doc, then flushes when
/// segment flushes.
const MAX_ARRAY_LENGTH: usize = (i32::max_value() - 1024) as usize;
// 32 KB block sizes for PagedBytes storage:
const BLOCK_BITS: usize = 15;

/// Buffers up pending bytes per doc, then flushes when segment flushes.
pub struct BinaryDocValuesWriter {
    bytes: PagedBytes,
    lengths: PackedLongValuesBuilder,
    docs_with_field: FixedBitSet,
    field_info: FieldInfo,
    added_values: DocId,
}

impl BinaryDocValuesWriter {
    pub fn new(field_info: &FieldInfo) -> Result<BinaryDocValuesWriter> {
        let bytes = PagedBytes::new(BLOCK_BITS);
        Ok(BinaryDocValuesWriter {
            bytes,
            lengths: PackedLongValuesBuilder::new(
                DEFAULT_PAGE_SIZE,
                COMPACT as f32,
                PackedLongValuesBuilderType::Delta,
            ),
            docs_with_field: FixedBitSet::new(64),
            field_info: field_info.clone(),
            added_values: 0,
        })
    }

    pub fn add_value(&mut self, doc_id: DocId, value: &BytesRef) -> Result<()> {
        if doc_id < self.added_values {
            bail!(IllegalArgument(format!(
                "DocValuesField {} appears more than once in this document (only one value is \
                 allowed per field)",
                self.field_info.name
            )));
        }
        if value.is_empty() {
            bail!(IllegalArgument(format!(
                "field={}: null value not allowed",
                self.field_info.name
            )));
        }
        if value.len() > MAX_ARRAY_LENGTH {
            bail!(IllegalArgument(format!(
                "DocValuesField {} is too large, must be <={}",
                self.field_info.name, MAX_ARRAY_LENGTH
            )));
        }
        // Fill in any holes:
        while self.added_values < doc_id {
            self.added_values += 1;
            self.lengths.add(0);
        }

        self.added_values += 1;
        self.lengths.add(value.len() as i64);
        self.bytes
            .get_output()?
            .write_bytes(value.bytes(), 0, value.len())?;
        self.docs_with_field.ensure_capacity(doc_id as usize);
        self.docs_with_field.set(doc_id as usize);

        Ok(())
    }

    fn sort_doc_values(
        &self,
        max_doc: i32,
        sort_map: &dyn SorterDocMap,
        mut iter: BinaryBytesIterator,
    ) -> Result<CachedBinaryDVs> {
        let mut docs_with_fields = FixedBitSet::new(max_doc as usize);
        let mut values = vec![vec![]; max_doc as usize];
        // TODO: we should use the doc values iterator API as define in `LUCENE-7407`
        let mut i = 0;
        while let Some(v) = iter.next() {
            let value = v?;
            if !value.is_empty() {
                let new_id = sort_map.old_to_new(i) as usize;
                docs_with_fields.set(new_id);
                values[new_id] = value.as_ref().to_vec();
            }
            i += 1;
        }
        debug_assert_eq!(i, max_doc);

        Ok(CachedBinaryDVs::new(values, docs_with_fields))
    }
}

impl DocValuesWriter for BinaryDocValuesWriter {
    fn finish(&mut self, _num_doc: i32) {}

    fn flush<D: Directory, DW: Directory, C: Codec, W: DocValuesConsumer>(
        &mut self,
        state: &SegmentWriteState<D, DW, C>,
        sort_map: Option<&impl SorterDocMap>,
        consumer: &mut W,
    ) -> Result<()> {
        self.bytes.freeze(false)?;
        let size = self.lengths.size() as usize;
        let values = self.lengths.build();
        let lengths = values.iterator();
        let max_doc = state.segment_info.max_doc;
        debug_assert!(self.bytes.frozen);
        let mut iter =
            BinaryBytesIterator::new(max_doc, lengths, size, &self.bytes, &self.docs_with_field);

        if let Some(sort_map) = sort_map {
            let sorted_values = self.sort_doc_values(max_doc, sort_map, iter)?;
            let mut iter = SortedDVIter::new(sorted_values);
            consumer.add_binary_field(&self.field_info, &mut iter)
        } else {
            consumer.add_binary_field(&self.field_info, &mut iter)
        }
    }

    fn get_doc_comparator(
        &mut self,
        _num_doc: i32,
        _sort_field: &SortField,
    ) -> Result<Box<dyn SorterDocComparator>> {
        unreachable!()
    }
}

#[allow(dead_code)]
const MISSING: i64 = 0;

pub struct NumericDocValuesWriter {
    pending: PackedLongValuesBuilder,
    final_values: Option<PackedLongValues>,
    docs_with_field: FixedBitSet,
    field_info: FieldInfo,
}

impl NumericDocValuesWriter {
    pub fn new(field_info: &FieldInfo) -> NumericDocValuesWriter {
        NumericDocValuesWriter {
            pending: PackedLongValuesBuilder::new(
                DEFAULT_PAGE_SIZE,
                COMPACT as f32,
                PackedLongValuesBuilderType::Delta,
            ),
            final_values: None,
            docs_with_field: FixedBitSet::new(64),
            field_info: field_info.clone(),
        }
    }

    pub fn add_value(&mut self, doc_id: DocId, value: i64) -> Result<()> {
        if (doc_id as i64) < self.pending.size() {
            bail!(IllegalArgument(format!(
                "DocValuesField {} appears more than once in this document (only one value is \
                 allowed per field)",
                self.field_info.name
            )));
        }

        self.pending.add(value);
        self.docs_with_field.ensure_capacity(doc_id as usize);
        self.docs_with_field.set(doc_id as usize);

        Ok(())
    }

    pub fn sort_doc_values(
        max_doc: i32,
        doc_map: &impl SorterDocMap,
        docs_with_field: &FixedBitSet,
        mut values: LongValuesIterator,
    ) -> CachedNumericDVs {
        let mut sorted_docs_with_fields = FixedBitSet::new(max_doc as usize);
        let mut data = vec![0i64; max_doc as usize];
        let iter = BitSetIterator::new(docs_with_field);
        for doc_id in iter {
            let new_id = doc_map.old_to_new(doc_id);
            data[new_id as usize] = values.next().unwrap();
            sorted_docs_with_fields.set(new_id as usize);
        }
        debug_assert!(values.next().is_none());
        CachedNumericDVs::new(data, sorted_docs_with_fields)
    }
}

impl DocValuesWriter for NumericDocValuesWriter {
    fn finish(&mut self, _num_doc: i32) {}

    fn flush<D: Directory, DW: Directory, C: Codec, W: DocValuesConsumer>(
        &mut self,
        state: &SegmentWriteState<D, DW, C>,
        sort_map: Option<&impl SorterDocMap>,
        consumer: &mut W,
    ) -> Result<()> {
        let values = self
            .final_values
            .take()
            .unwrap_or_else(|| self.pending.build());
        let max_doc = state.segment_info.max_doc();

        if let Some(sort_map) = sort_map {
            let dvs =
                Self::sort_doc_values(max_doc, sort_map, &self.docs_with_field, values.iterator());
            let mut iter = NumericDVIter::new(dvs);
            consumer.add_numeric_field(&self.field_info, &mut iter)
        } else {
            let mut iter = NumericDocValuesIter::new(
                values.iterator(),
                &self.docs_with_field,
                max_doc as usize,
            );

            consumer.add_numeric_field(&self.field_info, &mut iter)
        }
    }

    fn get_doc_comparator(
        &mut self,
        num_doc: i32,
        sort_field: &SortField,
    ) -> Result<Box<dyn SorterDocComparator>> {
        debug_assert!(self.final_values.is_none());
        let final_values = self.pending.build();
        let default_value = if let Some(d) = sort_field.missing_value() {
            match sort_field.field_type() {
                SortFieldType::Int => d.get_int().unwrap() as i64,
                SortFieldType::Long => d.get_long().unwrap(),
                _ => unreachable!(),
            }
        } else {
            0
        };
        let mut values = vec![default_value; num_doc as usize];
        let iter = BitSetIterator::new(&self.docs_with_field);
        for doc_id in iter {
            values[doc_id as usize] = final_values.get(doc_id)?;
        }

        // the float type is only 32 bits valid for sort
        let cmp_fn: fn(v1: &i64, v2: &i64) -> Ordering = if sort_field.is_reverse() {
            if sort_field.field_type() == SortFieldType::Float {
                |d1: &i64, d2: &i64| (*d2 as i32).cmp(&(*d1 as i32))
            } else {
                |d1: &i64, d2: &i64| d2.cmp(d1)
            }
        } else {
            if sort_field.field_type() == SortFieldType::Float {
                |d1: &i64, d2: &i64| (*d1 as i32).cmp(&(*d2 as i32))
            } else {
                |d1: &i64, d2: &i64| d1.cmp(d2)
            }
        };

        self.final_values = Some(final_values);

        Ok(Box::new(DVSortDocComparator::new(values, cmp_fn)))
    }
}

struct NumericDocValuesIter<'a> {
    values_iter: LongValuesIterator<'a>,
    docs_with_field: &'a FixedBitSet,
    upto: usize,
    max_doc: usize,
}

impl<'a> NumericDocValuesIter<'a> {
    fn new(
        values_iter: LongValuesIterator<'a>,
        docs_with_field: &'a FixedBitSet,
        max_doc: usize,
    ) -> Self {
        NumericDocValuesIter {
            values_iter,
            docs_with_field,
            upto: 0,
            max_doc,
        }
    }
}

impl<'a> Iterator for NumericDocValuesIter<'a> {
    type Item = Result<Numeric>;

    fn next(&mut self) -> Option<Result<Numeric>> {
        if self.upto < self.max_doc {
            let current = if self.docs_with_field.get(self.upto).unwrap() {
                let v = self.values_iter.next().unwrap();
                Numeric::Long(v)
            } else {
                Numeric::Null
            };
            self.upto += 1;
            Some(Ok(current))
        } else {
            None
        }
    }
}

impl<'a> ReusableIterator for NumericDocValuesIter<'a> {
    fn reset(&mut self) {
        self.values_iter.reset();
        self.upto = 0;
    }
}

pub struct NumericDVIter {
    dv: CachedNumericDVs,
    doc: i32,
    max_doc: i32,
}

impl NumericDVIter {
    pub fn new(dv: CachedNumericDVs) -> Self {
        let max_doc = dv.values.len() as i32;
        Self {
            dv,
            doc: -1,
            max_doc,
        }
    }
}

impl Iterator for NumericDVIter {
    type Item = Result<Numeric>;

    fn next(&mut self) -> Option<Self::Item> {
        self.doc += 1;
        if self.doc >= self.max_doc {
            None
        } else {
            let value = Numeric::Long(self.dv.values[self.doc as usize]);
            Some(Ok(value))
        }
    }
}

impl ReusableIterator for NumericDVIter {
    fn reset(&mut self) {
        self.doc = -1;
    }
}

pub struct SortedNumericDocValuesWriter {
    // stream of all values
    pending: PackedLongValuesBuilder,
    // count of values per doc
    pending_counts: PackedLongValuesBuilder,
    field_info: FieldInfo,
    current_doc: DocId,
    current_values: Vec<i64>,

    final_values: Option<PackedLongValues>,
    final_values_count: Option<PackedLongValues>,
}

impl SortedNumericDocValuesWriter {
    pub fn new(field_info: &FieldInfo) -> SortedNumericDocValuesWriter {
        SortedNumericDocValuesWriter {
            pending: PackedLongValuesBuilder::new(
                DEFAULT_PAGE_SIZE,
                COMPACT as f32,
                PackedLongValuesBuilderType::Delta,
            ),
            pending_counts: PackedLongValuesBuilder::new(
                DEFAULT_PAGE_SIZE,
                COMPACT as f32,
                PackedLongValuesBuilderType::Delta,
            ),
            field_info: field_info.clone(),
            current_doc: 0,
            current_values: vec![],

            final_values: None,
            final_values_count: None,
        }
    }

    pub fn add_value(&mut self, doc_id: DocId, value: i64) {
        if doc_id != self.current_doc {
            self.finish_current_doc();
        }

        while self.current_doc < doc_id {
            self.pending_counts.add(0);
            self.current_doc += 1;
        }

        self.add_one_value(value);
    }

    // finalize currentDoc: this sorts the values in the current doc
    fn finish_current_doc(&mut self) {
        self.current_values.sort();
        for v in &self.current_values {
            self.pending.add(*v);
        }
        self.pending_counts.add(self.current_values.len() as i64);
        self.current_values.clear();
        self.current_doc += 1;
    }

    fn add_one_value(&mut self, value: i64) {
        self.current_values.push(value);
    }

    fn sort_doc_values(
        &mut self,
        num_doc: i32,
        doc_map: &dyn SorterDocMap,
        counts_iter: LongValuesIterator,
        mut values_iter: LongValuesIterator,
    ) -> Vec<Vec<i64>> {
        let mut res = vec![vec![]; num_doc as usize];
        for (i, v) in counts_iter.enumerate() {
            if v > 0 {
                let mut values = Vec::with_capacity(v as usize);
                for _ in 0..v {
                    values.push(values_iter.next().unwrap());
                }
                let idx = doc_map.old_to_new(i as i32) as usize;
                res[idx] = values;
            }
        }
        res
    }
}

impl DocValuesWriter for SortedNumericDocValuesWriter {
    // `finish` may be called twice before `self.get_doc_comparator` and `self.flush` in
    // DefaultIndexingChain
    fn finish(&mut self, num_doc: i32) {
        if self.current_doc < num_doc {
            self.finish_current_doc();

            for _ in self.current_doc..num_doc {
                self.pending_counts.add(0);
            }
            self.current_doc = num_doc;
        }
    }

    fn flush<D: Directory, DW: Directory, C: Codec, W: DocValuesConsumer>(
        &mut self,
        state: &SegmentWriteState<D, DW, C>,
        sort_map: Option<&impl SorterDocMap>,
        consumer: &mut W,
    ) -> Result<()> {
        debug_assert_eq!(
            self.pending_counts.size() as i32,
            state.segment_info.max_doc
        );
        let pending_counts = self
            .final_values_count
            .take()
            .unwrap_or_else(|| self.pending_counts.build());
        let pending = self
            .final_values
            .take()
            .unwrap_or_else(|| self.pending.build());

        if let Some(sort_map) = sort_map {
            let values = self.sort_doc_values(
                state.segment_info.max_doc(),
                sort_map,
                pending_counts.iterator(),
                pending.iterator(),
            );
            let mut values_iter = SortSNValuesIterator::new(&values);
            let mut counts_iter = SortSNCountIterator::new(&values);
            consumer.add_sorted_numeric_field(&self.field_info, &mut values_iter, &mut counts_iter)
        } else {
            let mut values_iter = SNValuesIterator::new(&pending);
            let mut counts_iter = SNCountIterator::new(&pending_counts);

            consumer.add_sorted_numeric_field(&self.field_info, &mut values_iter, &mut counts_iter)
        }
    }

    fn get_doc_comparator(
        &mut self,
        num_doc: i32,
        sort_field: &SortField,
    ) -> Result<Box<dyn SorterDocComparator>> {
        debug_assert!(self.final_values.is_none() && self.final_values_count.is_none());
        self.final_values = Some(self.pending.build());
        self.final_values_count = Some(self.pending_counts.build());

        let counts_iter = self.final_values_count.as_ref().unwrap().iterator();
        let mut values_iter = self.final_values.as_ref().unwrap().iterator();

        let select_type = match sort_field {
            SortField::SortedNumeric(s) => s.selector(),
            _ => unreachable!(),
        };

        // TODO: currently the missing value is auto set to 0.
        // NOTE: if missing is explicitly set to none zero, then you must convert it to sorted bit
        // if type is float/double, else the compare may be in-corrent.
        let mut data = vec![0; num_doc as usize];
        for (i, v) in counts_iter.enumerate() {
            if v > 0 {
                let val = match select_type {
                    SortedNumericSelectorType::Min => {
                        let value = values_iter.next().unwrap();
                        for _ in 0..v - 1 {
                            let _ = values_iter.next().unwrap();
                        }
                        value
                    }
                    SortedNumericSelectorType::Max => {
                        for _ in 0..v - 1 {
                            let _ = values_iter.next().unwrap();
                        }
                        values_iter.next().unwrap()
                    }
                };
                data[i] = val;
            }
        }

        // the float type is only 32 bits valid for sort
        let cmp_fn: fn(v1: &i64, v2: &i64) -> Ordering = if sort_field.is_reverse() {
            if sort_field.field_type() == SortFieldType::Float {
                |d1: &i64, d2: &i64| (*d2 as i32).cmp(&(*d1 as i32))
            } else {
                |d1: &i64, d2: &i64| d2.cmp(d1)
            }
        } else {
            if sort_field.field_type() == SortFieldType::Float {
                |d1: &i64, d2: &i64| (*d1 as i32).cmp(&(*d2 as i32))
            } else {
                |d1: &i64, d2: &i64| d1.cmp(d2)
            }
        };

        Ok(Box::new(DVSortDocComparator::new(data, cmp_fn)))
    }
}

struct SNCountIterator<'a> {
    iter: LongValuesIterator<'a>,
}

impl<'a> SNCountIterator<'a> {
    fn new(builder: &'a PackedLongValues) -> Self {
        SNCountIterator {
            iter: builder.iterator(),
        }
    }
}

impl<'a> Iterator for SNCountIterator<'a> {
    type Item = Result<u32>;

    fn next(&mut self) -> Option<Result<u32>> {
        match self.iter.next() {
            Some(v) => Some(Ok(v as u32)),
            None => None,
        }
    }
}

impl<'a> ReusableIterator for SNCountIterator<'a> {
    fn reset(&mut self) {
        self.iter.reset();
    }
}

struct SNValuesIterator<'a> {
    iter: LongValuesIterator<'a>,
}

impl<'a> SNValuesIterator<'a> {
    fn new(values: &'a PackedLongValues) -> Self {
        let iter = values.iterator();
        SNValuesIterator { iter }
    }
}

impl<'a> Iterator for SNValuesIterator<'a> {
    type Item = Result<Numeric>;

    fn next(&mut self) -> Option<Result<Numeric>> {
        self.iter.next().map(|v| Ok(Numeric::Long(v)))
    }
}

impl<'a> ReusableIterator for SNValuesIterator<'a> {
    fn reset(&mut self) {
        self.iter.reset();
    }
}

struct SortSNCountIterator<'a> {
    data: &'a [Vec<i64>],
    upto: usize,
}

impl<'a> SortSNCountIterator<'a> {
    fn new(data: &'a [Vec<i64>]) -> Self {
        Self { data, upto: 0 }
    }
}

impl<'a> Iterator for SortSNCountIterator<'a> {
    type Item = Result<u32>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.upto < self.data.len() {
            let v = self.data[self.upto].len() as u32;
            self.upto += 1;
            Some(Ok(v))
        } else {
            None
        }
    }
}

impl<'a> ReusableIterator for SortSNCountIterator<'a> {
    fn reset(&mut self) {
        self.upto = 0;
    }
}

struct SortSNValuesIterator<'a> {
    data: &'a [Vec<i64>],
    upto: usize,
    idx: usize,
}

impl<'a> SortSNValuesIterator<'a> {
    fn new(data: &'a [Vec<i64>]) -> Self {
        Self {
            data,
            upto: 0,
            idx: 0,
        }
    }
}

impl<'a> Iterator for SortSNValuesIterator<'a> {
    type Item = Result<Numeric>;

    fn next(&mut self) -> Option<Result<Numeric>> {
        while self.upto < self.data.len() {
            if self.idx < self.data[self.upto].len() {
                let v = self.data[self.upto][self.idx];
                self.idx += 1;
                return Some(Ok(Numeric::Long(v)));
            } else {
                self.upto += 1;
                self.idx = 0;
            }
        }
        None
    }
}

impl<'a> ReusableIterator for SortSNValuesIterator<'a> {
    fn reset(&mut self) {
        self.upto = 0;
        self.idx = 0;
    }
}

const EMPTY_ORD: i64 = -1;

pub struct SortedDocValuesWriter {
    // stream of all values
    pending: PackedLongValuesBuilder,
    field_info: FieldInfo,
    docs_with_field: FixedBitSet,
    hash: BytesRefHash,
    // the hash.pool is pointed to this, so it must be boxed
    _bytes_block_pool: Box<ByteBlockPool>,

    final_ords: Option<PackedLongValues>,
    final_ord_map: Vec<i32>,
}

impl SortedDocValuesWriter {
    pub fn new(field_info: &FieldInfo) -> SortedDocValuesWriter {
        let mut bytes_block_pool =
            Box::new(ByteBlockPool::new(Box::new(DirectTrackingAllocator::new())));
        let hash = BytesRefHash::new(
            bytes_block_pool.as_mut(),
            DEFAULT_CAPACITY,
            Box::new(DirectByteStartArray::new(DEFAULT_CAPACITY)),
        );
        SortedDocValuesWriter {
            pending: PackedLongValuesBuilder::new(
                DEFAULT_PAGE_SIZE,
                COMPACT as f32,
                PackedLongValuesBuilderType::Delta,
            ),
            field_info: field_info.clone(),
            docs_with_field: FixedBitSet::new(64),
            hash,
            _bytes_block_pool: bytes_block_pool,
            final_ords: None,
            final_ord_map: vec![],
        }
    }

    pub fn add_value(&mut self, doc_id: DocId, value: &BytesRef) -> Result<()> {
        if doc_id < self.pending.size() as DocId {
            bail!(
                "DocValuesField {} appears more than once in this document (only one value is \
                 allowed per field)",
                self.field_info.name
            );
        }
        if value.is_empty() {
            bail!(IllegalArgument(format!(
                "DocValuesField {}: null value not allowed",
                self.field_info.name
            )));
        }
        if value.len() > ByteBlockPool::BYTE_BLOCK_SIZE - 2 {
            bail!(IllegalArgument(format!(
                "DocValuesField {} is too large, must be <= {}",
                self.field_info.name,
                ByteBlockPool::BYTE_BLOCK_SIZE - 2
            )));
        }

        self.add_one_value(value);
        self.docs_with_field.ensure_capacity(doc_id as usize);
        self.docs_with_field.set(doc_id as usize);
        Ok(())
    }

    fn add_one_value(&mut self, value: &BytesRef) {
        let mut term_id = self.hash.add(value) as i64;
        if term_id < 0 {
            term_id = -term_id - 1;
        } else {
            // reserve additional space for each unique value:
            // 1. when indexing, when hash is 50% full, rehash() suddenly needs 2*size ints.
            //    TODO: can this same OOM happen in THPF?
            // 2. when flushing, we need 1 int per value (slot in the ordMap).
        }

        self.pending.add(term_id);
    }

    fn sort_doc_values(
        &self,
        max_doc: i32,
        doc_map: &dyn SorterDocMap,
        mut iter: LongValuesIterator,
    ) -> Vec<i32> {
        let mut values = vec![-1; max_doc as usize];
        let doc_id_iter = BitSetIterator::new(&self.docs_with_field);
        for doc_id in doc_id_iter {
            let new_id = doc_map.old_to_new(doc_id);
            values[new_id as usize] = iter.next().unwrap() as i32;
        }
        debug_assert!(iter.next().is_none());
        values
    }
}

impl DocValuesWriter for SortedDocValuesWriter {
    fn finish(&mut self, _num_doc: i32) {}

    fn flush<D: Directory, DW: Directory, C: Codec, W: DocValuesConsumer>(
        &mut self,
        state: &SegmentWriteState<D, DW, C>,
        sort_map: Option<&impl SorterDocMap>,
        consumer: &mut W,
    ) -> Result<()> {
        let max_doc = state.segment_info.max_doc();
        debug_assert!(self.pending.size() == max_doc as i64);

        let value_count = self.hash.len();

        let (pending, ord_map) = if self.final_ords.is_some() {
            (
                self.final_ords.take().unwrap(),
                mem::replace(&mut self.final_ord_map, vec![]),
            )
        } else {
            self.hash.sort();
            let mut ord_map = vec![0i32; value_count];
            for ord in 0..value_count {
                let idx = self.hash.ids[ord] as usize;
                ord_map[idx] = ord as i32;
            }
            (self.pending.build(), ord_map)
        };
        let mut values_iter = SortedValuesIterator::new(&self.hash.ids, value_count, &self.hash);

        if let Some(sort_map) = sort_map {
            let values = self.sort_doc_values(max_doc, sort_map, pending.iterator());
            let mut ords_iter = SortSortedOrdsIter {
                ords: values,
                doc_upto: 0,
            };
            consumer.add_sorted_field(&self.field_info, &mut values_iter, &mut ords_iter)
        } else {
            let mut ords_iter = SortedOrdsIterator::new(
                &ord_map,
                max_doc,
                &self.docs_with_field,
                pending.iterator(),
            );
            consumer.add_sorted_field(&self.field_info, &mut values_iter, &mut ords_iter)
        }
    }

    fn get_doc_comparator(
        &mut self,
        num_doc: i32,
        sort_field: &SortField,
    ) -> Result<Box<dyn SorterDocComparator>> {
        debug_assert!(self.final_ords.is_none() && self.final_ord_map.is_empty());

        let mut data = vec![i32::min_value(); num_doc as usize];
        self.hash.sort();
        let value_count = self.hash.len();

        let mut ord_map = vec![0i32; value_count];
        for ord in 0..value_count {
            let idx = self.hash.ids[ord] as usize;
            ord_map[idx] = ord as i32;
        }
        self.final_ords = Some(self.pending.build());
        let mut value_iter = self.final_ords.as_ref().unwrap().iterator();
        let doc_id_iter = BitSetIterator::new(&self.docs_with_field);
        for doc in doc_id_iter {
            let i = value_iter.next().unwrap() as usize;
            data[doc as usize] = self.final_ord_map[i];
        }
        self.final_ord_map = ord_map;

        let cmp_fn: fn(v1: &i32, v2: &i32) -> Ordering = if sort_field.is_reverse() {
            |d1: &i32, d2: &i32| d2.cmp(d1)
        } else {
            |d1: &i32, d2: &i32| d1.cmp(d2)
        };

        Ok(Box::new(DVSortDocComparator::new(data, cmp_fn)))
    }
}

struct SortedValuesIterator<'a> {
    sorted_values: &'a [i32],
    hash: &'a BytesRefHash,
    value_count: usize,
    ord_upto: usize,
}

impl<'a> SortedValuesIterator<'a> {
    fn new(sorted_values: &'a [i32], value_count: usize, hash: &'a BytesRefHash) -> Self {
        SortedValuesIterator {
            sorted_values,
            hash,
            value_count,
            ord_upto: 0,
        }
    }
}

impl<'a> Iterator for SortedValuesIterator<'a> {
    type Item = Result<BytesRef>;

    fn next(&mut self) -> Option<Result<BytesRef>> {
        if self.ord_upto >= self.value_count {
            None
        } else {
            let res = self.hash.get(self.sorted_values[self.ord_upto] as usize);
            self.ord_upto += 1;
            Some(Ok(res))
        }
    }
}

impl<'a> ReusableIterator for SortedValuesIterator<'a> {
    fn reset(&mut self) {
        self.ord_upto = 0;
    }
}

struct SortedOrdsIterator<'a> {
    iter: LongValuesIterator<'a>,
    docs_with_field: &'a FixedBitSet,
    ord_map: &'a [i32],
    max_doc: i32,
    doc_upto: i32,
}

impl<'a> SortedOrdsIterator<'a> {
    fn new(
        ord_map: &'a [i32],
        max_doc: i32,
        docs_with_field: &'a FixedBitSet,
        iter: LongValuesIterator<'a>,
    ) -> Self {
        SortedOrdsIterator {
            ord_map,
            max_doc,
            docs_with_field,
            iter,
            doc_upto: 0,
        }
    }
}

impl<'a> Iterator for SortedOrdsIterator<'a> {
    type Item = Result<Numeric>;

    fn next(&mut self) -> Option<Result<Numeric>> {
        if self.doc_upto >= self.max_doc {
            None
        } else {
            self.doc_upto += 1;
            let ord = if self.docs_with_field.get(self.doc_upto as usize).unwrap() {
                let i = self.iter.next().unwrap();
                self.ord_map[i as usize]
            } else {
                -1
            };
            Some(Ok(Numeric::Int(ord)))
        }
    }
}

impl<'a> ReusableIterator for SortedOrdsIterator<'a> {
    fn reset(&mut self) {
        self.iter.reset();
        self.doc_upto = 0;
    }
}

struct SortSortedOrdsIter {
    ords: Vec<i32>,
    doc_upto: usize,
}

impl Iterator for SortSortedOrdsIter {
    type Item = Result<Numeric>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.doc_upto < self.ords.len() {
            let v = Numeric::Int(self.ords[self.doc_upto]);
            self.doc_upto += 1;
            Some(Ok(v))
        } else {
            None
        }
    }
}

impl ReusableIterator for SortSortedOrdsIter {
    fn reset(&mut self) {
        self.doc_upto = 0;
    }
}

pub struct SortedSetDocValuesWriter {
    // stream of all values
    pending: PackedLongValuesBuilder,
    // count of values per doc
    pending_counts: PackedLongValuesBuilder,
    field_info: FieldInfo,
    current_doc: DocId,
    current_values: Vec<i64>,
    max_count: i32,
    hash: BytesRefHash,
    // the hash.pool is pointed to this, so it must be boxed
    _bytes_block_pool: Box<ByteBlockPool>,
}

impl SortedSetDocValuesWriter {
    pub fn new(field_info: &FieldInfo) -> SortedSetDocValuesWriter {
        let mut bytes_block_pool =
            Box::new(ByteBlockPool::new(Box::new(DirectTrackingAllocator::new())));
        let hash = BytesRefHash::new(
            bytes_block_pool.as_mut(),
            DEFAULT_CAPACITY,
            Box::new(DirectByteStartArray::new(DEFAULT_CAPACITY)),
        );
        SortedSetDocValuesWriter {
            pending: PackedLongValuesBuilder::new(
                DEFAULT_PAGE_SIZE,
                COMPACT as f32,
                PackedLongValuesBuilderType::Delta,
            ),
            pending_counts: PackedLongValuesBuilder::new(
                DEFAULT_PAGE_SIZE,
                COMPACT as f32,
                PackedLongValuesBuilderType::Delta,
            ),
            field_info: field_info.clone(),
            current_doc: 0,
            current_values: vec![],
            max_count: 0,
            hash,
            _bytes_block_pool: bytes_block_pool,
        }
    }

    pub fn add_value(&mut self, doc_id: DocId, value: &BytesRef) -> Result<()> {
        if value.is_empty() {
            bail!(IllegalArgument(format!(
                "DocValuesField {}: null value not allowed",
                self.field_info.name
            )));
        }
        if value.len() > ByteBlockPool::BYTE_BLOCK_SIZE - 2 {
            bail!(IllegalArgument(format!(
                "DocValuesField {} is too large, must be <= {}",
                self.field_info.name,
                ByteBlockPool::BYTE_BLOCK_SIZE - 2
            )));
        }

        if doc_id != self.current_doc {
            self.finish_current_doc();
        }

        while self.current_doc < doc_id {
            self.pending_counts.add(0);
            self.current_doc += 1;
        }

        self.add_one_value(value);
        Ok(())
    }

    // finalize currentDoc: this sorts the values in the current doc
    fn finish_current_doc(&mut self) {
        self.current_values.sort();
        let mut last_value = EMPTY_ORD;
        let mut count = 0i32;
        for term_id in &self.current_values {
            // if it's not a duplicate
            if *term_id != last_value {
                self.pending.add(*term_id);
                count += 1;
            }
            last_value = *term_id;
        }

        // record the number of unique term ids for this doc
        self.pending_counts.add(count as i64);
        self.max_count = self.max_count.max(count);

        self.current_values.clear();
        self.current_doc += 1;
    }

    fn add_one_value(&mut self, value: &BytesRef) {
        let mut term_id = self.hash.add(value) as i64;
        if term_id < 0 {
            term_id = -term_id - 1;
        } else {
            // reserve additional space for each unique value:
            // 1. when indexing, when hash is 50% full, rehash() suddenly needs 2*size ints.
            //    TODO: can this same OOM happen in THPF?
            // 2. when flushing, we need 1 int per value (slot in the ordMap).
        }

        self.current_values.push(term_id);
    }

    fn sort_doc_values(
        &self,
        max_doc: i32,
        doc_map: &dyn SorterDocMap,
        mut ord_iter: LongValuesIterator,
        ord_count_iter: LongValuesIterator,
        ord_map: &[i32],
    ) -> Vec<Vec<i32>> {
        let mut res = vec![vec![]; max_doc as usize];
        for (i, v) in ord_count_iter.enumerate() {
            if v > 0 {
                let mut values = Vec::with_capacity(v as usize);
                for _ in 0..v {
                    let ord = ord_iter.next().unwrap();
                    values.push(ord_map[ord as usize]);
                }
                let idx = doc_map.old_to_new(i as i32) as usize;
                res[idx] = values;
            }
        }
        res
    }
}

impl DocValuesWriter for SortedSetDocValuesWriter {
    fn finish(&mut self, num_doc: i32) {
        self.finish_current_doc();

        for _ in self.current_doc..num_doc {
            self.pending_counts.add(0);
        }
    }

    fn flush<D: Directory, DW: Directory, C: Codec, W: DocValuesConsumer>(
        &mut self,
        state: &SegmentWriteState<D, DW, C>,
        sort_map: Option<&impl SorterDocMap>,
        consumer: &mut W,
    ) -> Result<()> {
        let max_doc = state.segment_info.max_doc();
        let _max_count_per_doc = self.max_count;
        debug_assert!(self.pending_counts.size() == max_doc as i64);

        let value_count = self.hash.len();
        let pending = self.pending.build();
        let pending_counts = self.pending_counts.build();

        self.hash.sort();
        let mut ord_map = vec![0i32; value_count];

        for ord in 0..value_count {
            ord_map[self.hash.ids[ord] as usize] = ord as i32;
        }

        let mut value_iter = SortedValuesIterator::new(&self.hash.ids, value_count, &self.hash);

        if let Some(sort_map) = sort_map {
            let values = self.sort_doc_values(
                max_doc,
                sort_map,
                pending.iterator(),
                pending_counts.iterator(),
                &ord_map,
            );
            let mut ords_iter = SortSSOrdsIterator::new(&values);
            let mut counts_iter = SortSSCountIterator::new(&values);

            consumer.add_sorted_set_field(
                &self.field_info,
                &mut value_iter,
                &mut counts_iter,
                &mut ords_iter,
            )
        } else {
            let mut ord_iter = SortedSetOrdsIterator::new(&ord_map, &pending, &pending_counts);
            let mut ord_count_iter =
                SortedSetOrdCountIterator::new(max_doc as i32, pending_counts.iterator());

            consumer.add_sorted_set_field(
                &self.field_info,
                &mut value_iter,
                &mut ord_count_iter,
                &mut ord_iter,
            )
        }
    }

    fn get_doc_comparator(
        &mut self,
        _num_doc: i32,
        _sort_field: &SortField,
    ) -> Result<Box<dyn SorterDocComparator>> {
        unimplemented!()
    }
}

struct SortedSetOrdsIterator<'a> {
    iter: LongValuesIterator<'a>,
    counts: LongValuesIterator<'a>,
    ord_map: &'a [i32],
    num_ords: i32,
    ord_upto: i32,
    current_doc: Vec<i32>,
    current_upto: usize,
}

impl<'a> SortedSetOrdsIterator<'a> {
    fn new(
        ord_map: &'a [i32],
        ords: &'a PackedLongValues,
        ord_counts: &'a PackedLongValues,
    ) -> Self {
        let num_ords = ords.size() as i32;
        SortedSetOrdsIterator {
            iter: ords.iterator(),
            counts: ord_counts.iterator(),
            ord_map,
            num_ords,
            ord_upto: 0,
            current_doc: vec![],
            current_upto: 0,
        }
    }
}

impl<'a> Iterator for SortedSetOrdsIterator<'a> {
    type Item = Result<Numeric>;

    fn next(&mut self) -> Option<Result<Numeric>> {
        if self.ord_upto >= self.num_ords {
            return None;
        }

        while self.current_upto == self.current_doc.len() {
            // refill next doc, and sort remapped ords within the doc.
            self.current_upto = 0;
            self.current_doc
                .resize(self.counts.next().unwrap() as usize, 0);
            for i in 0..self.current_doc.len() {
                self.current_doc[i] = self.ord_map[self.iter.next().unwrap() as usize];
            }
            self.current_doc.sort();
        }
        let ord = self.current_doc[self.current_upto];
        self.current_upto += 1;
        self.ord_upto += 1;
        Some(Ok(Numeric::Int(ord)))
    }
}

impl<'a> ReusableIterator for SortedSetOrdsIterator<'a> {
    fn reset(&mut self) {
        self.iter.reset();
        self.counts.reset();
        self.ord_upto = 0;
        self.current_upto = 0;
        self.current_doc.clear();
    }
}

struct SortedSetOrdCountIterator<'a> {
    iter: LongValuesIterator<'a>,
    max_doc: i32,
    doc_upto: i32,
}

impl<'a> SortedSetOrdCountIterator<'a> {
    fn new(max_doc: i32, iter: LongValuesIterator<'a>) -> Self {
        SortedSetOrdCountIterator {
            iter,
            max_doc,
            doc_upto: 0,
        }
    }
}

impl<'a> Iterator for SortedSetOrdCountIterator<'a> {
    type Item = Result<u32>;

    fn next(&mut self) -> Option<Result<u32>> {
        if self.doc_upto < self.max_doc {
            self.doc_upto += 1;
            Some(Ok(self.iter.next().unwrap() as u32))
        } else {
            None
        }
    }
}

impl<'a> ReusableIterator for SortedSetOrdCountIterator<'a> {
    fn reset(&mut self) {
        self.iter.reset();
        self.doc_upto = 0;
    }
}

struct SortSSOrdsIterator<'a> {
    data: &'a [Vec<i32>],
    upto: usize,
    idx: usize,
}

impl<'a> SortSSOrdsIterator<'a> {
    fn new(data: &'a [Vec<i32>]) -> Self {
        Self {
            data,
            upto: 0,
            idx: 0,
        }
    }
}

impl<'a> Iterator for SortSSOrdsIterator<'a> {
    type Item = Result<Numeric>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.upto < self.data.len() {
            if self.idx < self.data[self.upto].len() {
                let v = self.data[self.upto][self.idx];
                self.idx += 1;
                return Some(Ok(Numeric::Int(v)));
            } else {
                self.upto += 1;
                self.idx = 0;
            }
        }
        None
    }
}

impl<'a> ReusableIterator for SortSSOrdsIterator<'a> {
    fn reset(&mut self) {
        self.upto = 0;
        self.idx = 0;
    }
}

struct SortSSCountIterator<'a> {
    data: &'a [Vec<i32>],
    upto: usize,
}

impl<'a> SortSSCountIterator<'a> {
    fn new(data: &'a [Vec<i32>]) -> Self {
        Self { data, upto: 0 }
    }
}

impl<'a> Iterator for SortSSCountIterator<'a> {
    type Item = Result<u32>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.upto < self.data.len() {
            let v = self.data[self.upto].len() as u32;
            self.upto += 1;
            Some(Ok(v))
        } else {
            None
        }
    }
}

impl<'a> ReusableIterator for SortSSCountIterator<'a> {
    fn reset(&mut self) {
        self.upto = 0;
    }
}

// Rough logic: OBJ_HEADER + 3*PTR + INT
// Term: OBJ_HEADER + 2*PTR
// Term.field: 2*OBJ_HEADER + 4*INT + PTR + string.length*CHAR
// Term.bytes: 2*OBJ_HEADER + 2*INT + PTR + bytes.length
// String: 2*OBJ_HEADER + 4*INT + PTR + string.length*CHAR
/// T: OBJ_HEADER
// reference size is 4, if we have compressed oops, else 8
pub const NUM_BYTES_OBJECT_REF: i32 = 4;
pub const NUM_BYTES_OBJECT_HEADER: i32 = 8 + NUM_BYTES_OBJECT_REF;
pub const RAW_SIZE_IN_BYTES: i32 =
    8 * NUM_BYTES_OBJECT_HEADER + 8 * NUM_BYTES_OBJECT_REF + 8 * INT_BYTES;
pub const NUM_BYTES_OBJECT_ALIGNMENT: i32 = 8;

pub fn align_object_size(size: i32) -> i32 {
    let size = size + (NUM_BYTES_OBJECT_ALIGNMENT - 1);
    size - size % NUM_BYTES_OBJECT_ALIGNMENT
}

pub trait SizeInBytesCalc {
    fn value_size_in_bytes(&self) -> usize;

    fn size_in_bytes(&self) -> usize;
}

/// An in-place update to a DocValues field.
struct DocValuesUpdate {
    pub doc_values_type: DocValuesType,
    pub term: Term,
    pub field: String,
    pub value: VariantValue,
    // unassigned until applied, and confusing that it's here, when it's just used in
    // BufferedDeletes...
    pub doc_id_up_to: i32,
}

impl DocValuesUpdate {
    pub fn new(
        doc_values_type: DocValuesType,
        term: Term,
        field: String,
        value: VariantValue,
    ) -> DocValuesUpdate {
        DocValuesUpdate {
            doc_values_type,
            term,
            field,
            value,
            doc_id_up_to: -1,
        }
    }

    pub fn base_size_in_bytes(&self) -> usize {
        let mut size_in_bytes = RAW_SIZE_IN_BYTES as usize;
        size_in_bytes += self.term.field().len();
        size_in_bytes += self.term.bytes.len();
        size_in_bytes += self.field.len();

        size_in_bytes
    }
}

impl fmt::Display for DocValuesUpdate {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "type:{:?} term={:?} field={} value={:?} doc_id_up_to={}",
            self.doc_values_type, self.term, self.field, self.value, self.doc_id_up_to
        )
    }
}

#[allow(dead_code)]
struct BinaryDocValuesUpdate {
    update: DocValuesUpdate,
}

impl BinaryDocValuesUpdate {
    #[allow(dead_code)]
    fn new(term: Term, field: String, value: VariantValue) -> BinaryDocValuesUpdate {
        BinaryDocValuesUpdate {
            update: DocValuesUpdate::new(DocValuesType::Binary, term, field, value),
        }
    }
}

impl SizeInBytesCalc for BinaryDocValuesUpdate {
    fn value_size_in_bytes(&self) -> usize {
        debug_assert!(self.update.value.get_binary().is_some());
        let raw_value_size_in_bytes = align_object_size(NUM_BYTES_OBJECT_HEADER + INT_BYTES) * 2
            + 2 * INT_BYTES
            + NUM_BYTES_OBJECT_REF;

        raw_value_size_in_bytes as usize + self.update.value.get_binary().as_ref().unwrap().len()
    }

    fn size_in_bytes(&self) -> usize {
        self.update.base_size_in_bytes() + self.value_size_in_bytes()
    }
}

pub struct NumericDocValuesUpdate {
    update: DocValuesUpdate,
}

impl NumericDocValuesUpdate {
    pub fn new(term: Term, field: String, value: VariantValue) -> NumericDocValuesUpdate {
        NumericDocValuesUpdate {
            update: DocValuesUpdate::new(DocValuesType::Numeric, term, field, value),
        }
    }
}

impl SizeInBytesCalc for NumericDocValuesUpdate {
    fn value_size_in_bytes(&self) -> usize {
        LONG_BYTES as usize
    }

    fn size_in_bytes(&self) -> usize {
        self.update.base_size_in_bytes() + self.value_size_in_bytes()
    }
}

pub struct DocValuesFieldUpdatesValue {
    doc_values_type: DocValuesType,
    numeric_value: Option<Numeric>,
    binary_value: Option<BytesRef>,
}

/// An iterator over documents and their updated values. Only documents with
/// updates are returned by this iterator, and the documents are returned in
/// increasing order.
pub trait DocValuesFieldUpdatesIterator {
    /// Returns the next document which has an update, or
    /// {@link DocIdSetIterator#NO_MORE_DOCS} if there are no more documents to return.
    fn next_doc(&mut self) -> Result<DocId>;

    /// Returns the current document this iterator is on.
    fn doc(&self) -> DocId;

    /// Returns the value of the document returned from {@link #nextDoc()}. A
    /// {@code null} value means that it was unset for this document.
    fn value(&self) -> DocValuesFieldUpdatesValue;

    /// Reset the iterator's state. Should be called before {@link #nextDoc()} and {@link #value()}.
    fn reset(&mut self);
}

const PAGE_SIZE: usize = 1024;

/// Returns the estimated capacity of a {@link PagedGrowableWriter} given the actual
/// number of stored elements.
#[allow(dead_code)]
fn estimate_capacity(size: usize) -> usize {
    let extra = if size % PAGE_SIZE == 0 { 0 } else { 1 };

    (size / PAGE_SIZE + extra as usize) * PAGE_SIZE
}

#[derive(Default)]
#[allow(dead_code)]
struct Container {
    numeric_dv_updates: HashMap<String, NumericDocValuesFieldUpdates>,
    binary_dv_updates: HashMap<String, BinaryDocValuesFieldUpdates>,
}

impl Container {
    #[allow(dead_code)]
    pub fn new() -> Container {
        Default::default()
    }

    #[allow(dead_code)]
    pub fn any(&self) -> bool {
        for updates in self.numeric_dv_updates.values() {
            if updates.any() {
                return true;
            }
        }

        for updates in self.binary_dv_updates.values() {
            if updates.any() {
                return true;
            }
        }

        false
    }

    #[allow(dead_code)]
    pub fn size(&self) -> usize {
        self.numeric_dv_updates.len() + self.binary_dv_updates.len()
    }

    #[allow(dead_code)]
    pub fn get_updates(
        &self,
        field: &str,
        doc_values_type: DocValuesType,
    ) -> Result<&dyn DocValuesFieldUpdates> {
        match doc_values_type {
            DocValuesType::Numeric => Ok(self
                .numeric_dv_updates
                .get(field)
                .as_ref()
                .unwrap()
                .as_base()),
            DocValuesType::Binary => Ok(self
                .binary_dv_updates
                .get(field)
                .as_ref()
                .unwrap()
                .as_base()),
            _ => bail!(IllegalArgument(format!(
                "unsupported type: {:?}",
                doc_values_type
            ))),
        }
    }

    #[allow(dead_code)]
    pub fn new_updates(
        &mut self,
        field: &str,
        doc_values_type: DocValuesType,
        max_doc: i32,
    ) -> Result<&dyn DocValuesFieldUpdates> {
        match doc_values_type {
            DocValuesType::Numeric => {
                debug_assert!(self.numeric_dv_updates.get(field).is_none());
                let numeric_updates = NumericDocValuesFieldUpdates::new(field, max_doc);
                self.numeric_dv_updates
                    .insert(field.to_string(), numeric_updates);
                Ok(self
                    .numeric_dv_updates
                    .get(field)
                    .as_ref()
                    .unwrap()
                    .as_base())
            }
            DocValuesType::Binary => {
                debug_assert!(self.binary_dv_updates.get(field).is_none());
                let binary_updates = BinaryDocValuesFieldUpdates::new(field, max_doc);
                self.binary_dv_updates
                    .insert(field.to_string(), binary_updates);
                Ok(self
                    .binary_dv_updates
                    .get(field)
                    .as_ref()
                    .unwrap()
                    .as_base())
            }
            _ => bail!(IllegalArgument(format!(
                "unsupported type: {:?}",
                doc_values_type
            ))),
        }
    }
}

pub trait DocValuesFieldUpdates {
    /// Add an update to a document. For unsetting a value you should pass
    /// {@code null}.
    fn add(&mut self, doc: DocId, value: &DocValuesFieldUpdatesValue) -> Result<()>;

    /// Returns an {@link Iterator} over the updated documents and their values.
    fn iterator(&mut self) -> Result<Box<dyn DocValuesFieldUpdatesIterator>>;

    /// Returns true if this instance contains any updates.
    fn any(&self) -> bool;

    /// Merge with another {@link DocValuesFieldUpdates}. This is called for a
    /// segment which received updates while it was being merged. The given updates
    /// should override whatever updates are in that instance.
    fn merge(&mut self, other: &dyn DocValuesFieldUpdates) -> Result<()>;

    fn doc_values_type(&self) -> &DocValuesType;

    fn as_numeric(&self) -> &NumericDocValuesFieldUpdates {
        unimplemented!()
    }

    fn as_binary(&self) -> &BinaryDocValuesFieldUpdates {
        unimplemented!()
    }

    fn as_base(&self) -> &dyn DocValuesFieldUpdates;
}

struct NumericDocValuesFieldUpdatesIterator {
    updates: *const NumericDocValuesFieldUpdates,
    // long so we don't overflow if size == Integer.MAX_VALUE
    idx: DocId,
    doc: DocId,
    value: Option<Numeric>,
}

impl NumericDocValuesFieldUpdatesIterator {
    fn new(updates: &NumericDocValuesFieldUpdates) -> NumericDocValuesFieldUpdatesIterator {
        NumericDocValuesFieldUpdatesIterator {
            updates,
            idx: 0,
            doc: -1,
            value: None,
        }
    }
}

impl DocValuesFieldUpdatesIterator for NumericDocValuesFieldUpdatesIterator {
    fn next_doc(&mut self) -> Result<DocId> {
        let updates = unsafe { &(*self.updates) };
        if self.idx > updates.size as DocId {
            self.value = None;
            self.doc = NO_MORE_DOCS;
            return Ok(self.doc);
        }

        self.doc = updates.docs.get(self.idx)? as DocId;
        self.idx += 1;
        while self.idx < updates.size as DocId && updates.docs.get(self.idx)? == self.doc as i64 {
            self.idx += 1;
        }

        // idx points to the "next" element
        self.value = Some(Numeric::Long(updates.values.get(self.idx - 1)?));
        Ok(self.doc)
    }

    fn doc(&self) -> DocId {
        self.doc
    }

    fn value(&self) -> DocValuesFieldUpdatesValue {
        DocValuesFieldUpdatesValue {
            doc_values_type: DocValuesType::Numeric,
            numeric_value: self.value,
            binary_value: None,
        }
    }

    fn reset(&mut self) {
        self.idx = 0;
        self.doc = -1;
        self.value = None;
    }
}

struct NumericDocValuesInPlaceMergeSorter {
    updates: *mut NumericDocValuesFieldUpdates,
    pivot_index: i32,
}

impl NumericDocValuesInPlaceMergeSorter {
    pub fn new(updates: &mut NumericDocValuesFieldUpdates) -> NumericDocValuesInPlaceMergeSorter {
        NumericDocValuesInPlaceMergeSorter {
            updates,
            pivot_index: 0,
        }
    }

    fn merge_sort(&mut self, from: i32, to: i32) {
        if to - from < BINARY_SORT_THRESHOLD as i32 {
            self.binary_sort(from, to);
        } else {
            let mid = (from + to) / 2;
            self.merge_sort(from, mid);
            self.merge_sort(mid, to);
            self.merge_in_place(from, mid, to);
        }
    }
}

impl Sorter for NumericDocValuesInPlaceMergeSorter {
    fn compare(&mut self, i: i32, j: i32) -> Ordering {
        let updates = unsafe { &(*self.updates) };
        let x = updates.docs.get(i).unwrap();
        let y = updates.docs.get(j).unwrap();
        x.cmp(&y)
    }

    fn swap(&mut self, i: i32, j: i32) {
        let updates = unsafe { &mut (*self.updates) };
        let temp_doci = updates.docs.get(i).unwrap();
        let temp_docj = updates.docs.get(j).unwrap();
        updates.docs.set(i as usize, temp_docj);
        updates.docs.set(j as usize, temp_doci);

        let temp_valuei = updates.values.get(i).unwrap();
        let temp_valuej = updates.values.get(j).unwrap();
        updates.values.set(i as usize, temp_valuej);
        updates.values.set(j as usize, temp_valuei);
    }

    fn sort(&mut self, from: i32, to: i32) {
        assert!(from <= to);
        self.merge_sort(from, to);
    }

    fn set_pivot(&mut self, i: i32) {
        self.pivot_index = i;
    }

    fn compare_pivot(&mut self, j: i32) -> Ordering {
        let pivot_index = self.pivot_index;
        self.compare(pivot_index, j)
    }
}

pub struct NumericDocValuesFieldUpdates {
    pub field: String,
    pub doc_values_type: DocValuesType,
    pub bits_per_value: i32,
    pub size: usize,
    pub docs: PagedMutableHugeWriter,
    pub values: PagedGrowableWriter,
}

impl NumericDocValuesFieldUpdates {
    #[allow(dead_code)]
    pub fn new(field: &str, max_doc: i32) -> NumericDocValuesFieldUpdates {
        let bits_per_value = (max_doc - 1).bits_required() as i32;
        let docs = PagedMutableHugeWriter::new(1, PAGE_SIZE, bits_per_value, COMPACT);
        let values = PagedGrowableWriter::new(1, PAGE_SIZE, 1, FAST);

        NumericDocValuesFieldUpdates {
            field: field.to_string(),
            doc_values_type: DocValuesType::Numeric,
            bits_per_value,
            size: 0,
            docs,
            values,
        }
    }
}

impl DocValuesFieldUpdates for NumericDocValuesFieldUpdates {
    fn add(&mut self, doc: DocId, value: &DocValuesFieldUpdatesValue) -> Result<()> {
        // TODO: if the Sorter interface changes to take long indexes, we can remove that limitation
        if self.size == i32::max_value() as usize {
            bail!(IllegalState(
                "cannot support more than Integer.MAX_VALUE doc/value entries".into()
            ))
        }

        debug_assert!(
            value.doc_values_type == DocValuesType::Numeric && value.numeric_value.is_some()
        );
        let val = value.numeric_value.as_ref().unwrap();

        // grow the structures to have room for more elements
        if self.docs.size() == self.size {
            self.docs = self.docs.grow_by_size(self.size + 1);
            self.values = self.values.grow_by_size(self.size + 1);
        }

        self.docs.set(self.size, doc as i64);
        self.values.set(self.size, val.long_value());
        self.size += 1;

        Ok(())
    }

    fn iterator(&mut self) -> Result<Box<dyn DocValuesFieldUpdatesIterator>> {
        let mut sorter = NumericDocValuesInPlaceMergeSorter::new(self);
        sorter.sort(0, self.size as i32);

        Ok(Box::new(NumericDocValuesFieldUpdatesIterator::new(self)))
    }

    fn any(&self) -> bool {
        self.size > 0
    }

    fn merge(&mut self, other: &dyn DocValuesFieldUpdates) -> Result<()> {
        debug_assert!(other.doc_values_type() == self.doc_values_type());
        let other_updates = other.as_numeric();
        if other_updates.size + self.size > i32::max_value() as usize {
            bail!(IllegalState(format!(
                "cannot support more than Integer.MAX_VALUE doc/value entries; size={} \
                 other.size={}",
                self.size, other_updates.size
            )));
        }
        self.docs = self.docs.grow_by_size(self.size + other_updates.size);
        self.values = self.values.grow_by_size(self.size + other_updates.size);
        for i in 0..other_updates.size {
            self.docs
                .set(self.size, other_updates.docs.get(i as DocId)?);
            self.values
                .set(self.size, other_updates.values.get(i as DocId)?);
            self.size += 1;
        }

        Ok(())
    }

    fn doc_values_type(&self) -> &DocValuesType {
        &self.doc_values_type
    }

    fn as_numeric(&self) -> &NumericDocValuesFieldUpdates {
        self
    }

    fn as_base(&self) -> &dyn DocValuesFieldUpdates {
        self
    }
}

struct BinaryDocValuesFieldUpdatesIterator {
    updates: *mut BinaryDocValuesFieldUpdates,
    // long so we don't overflow if size == Integer.MAX_VALUE
    idx: DocId,
    doc: DocId,
    offset: i64,
    length: i64,
    value: BytesRef,
}

impl BinaryDocValuesFieldUpdatesIterator {
    #[allow(dead_code)]
    pub fn new(updates: &mut BinaryDocValuesFieldUpdates) -> BinaryDocValuesFieldUpdatesIterator {
        BinaryDocValuesFieldUpdatesIterator {
            updates,
            idx: 0,
            doc: -1,
            offset: -1,
            length: 0,
            value: BytesRef::new(&updates.values),
        }
    }
}

impl DocValuesFieldUpdatesIterator for BinaryDocValuesFieldUpdatesIterator {
    fn next_doc(&mut self) -> Result<DocId> {
        let updates = unsafe { &mut (*self.updates) };
        if self.idx > updates.size as DocId {
            self.offset = -1;
            self.doc = NO_MORE_DOCS;
            return Ok(self.doc);
        }

        self.doc = updates.docs.get(self.idx)? as DocId;
        self.idx += 1;
        while self.idx < updates.size as DocId && updates.docs.get(self.idx)? == self.doc as i64 {
            self.idx += 1;
        }

        // idx points to the "next" element
        let prev_idx = self.idx - 1;
        // cannot change 'value' here because nextDoc is called before the
        // value is used, and it's a waste to clone the BytesRef when we
        // obtain the value
        self.offset = updates.offsets.get(prev_idx)?;
        self.length = updates.lengths.get(prev_idx)?;
        Ok(self.doc)
    }

    fn doc(&self) -> DocId {
        self.doc
    }

    fn value(&self) -> DocValuesFieldUpdatesValue {
        DocValuesFieldUpdatesValue {
            doc_values_type: DocValuesType::Binary,
            numeric_value: None,
            binary_value: Some(self.value),
        }
    }

    fn reset(&mut self) {
        self.idx = 0;
        self.doc = -1;
        self.offset = -1;
    }
}

struct BinaryDocValuesInPlaceMergeSorter {
    updates: *mut BinaryDocValuesFieldUpdates,
    pivot_index: i32,
}

impl BinaryDocValuesInPlaceMergeSorter {
    fn new(updates: &mut BinaryDocValuesFieldUpdates) -> BinaryDocValuesInPlaceMergeSorter {
        BinaryDocValuesInPlaceMergeSorter {
            updates,
            pivot_index: 0,
        }
    }

    fn merge_sort(&mut self, from: i32, to: i32) {
        if to - from < BINARY_SORT_THRESHOLD as i32 {
            self.binary_sort(from, to);
        } else {
            let mid = (from + to) / 2;
            self.merge_sort(from, mid);
            self.merge_sort(mid, to);
            self.merge_in_place(from, mid, to);
        }
    }
}

impl Sorter for BinaryDocValuesInPlaceMergeSorter {
    fn compare(&mut self, i: i32, j: i32) -> Ordering {
        let updates = unsafe { &(*self.updates) };
        let x = updates.docs.get(i).unwrap();
        let y = updates.docs.get(j).unwrap();
        x.cmp(&y)
    }

    fn swap(&mut self, i: i32, j: i32) {
        let updates = unsafe { &mut (*self.updates) };
        let temp_doci = updates.docs.get(i).unwrap();
        let temp_docj = updates.docs.get(j).unwrap();
        updates.docs.set(i as usize, temp_docj);
        updates.docs.set(j as usize, temp_doci);

        let temp_offseti = updates.offsets.get(i).unwrap();
        let temp_offsetj = updates.offsets.get(j).unwrap();
        updates.offsets.set(i as usize, temp_offsetj);
        updates.offsets.set(j as usize, temp_offseti);

        let temp_lengthi = updates.lengths.get(i).unwrap();
        let temp_lengthj = updates.lengths.get(j).unwrap();
        updates.lengths.set(i as usize, temp_lengthj);
        updates.lengths.set(j as usize, temp_lengthi);
    }

    fn sort(&mut self, from: i32, to: i32) {
        assert!(from <= to);
        self.merge_sort(from, to);
    }

    fn set_pivot(&mut self, i: i32) {
        self.pivot_index = i;
    }

    fn compare_pivot(&mut self, j: i32) -> Ordering {
        let pivot_index = self.pivot_index;
        self.compare(pivot_index, j)
    }
}

pub struct BinaryDocValuesFieldUpdates {
    pub field: String,
    pub doc_values_type: DocValuesType,
    pub bits_per_value: i32,
    pub size: usize,
    pub docs: PagedMutableHugeWriter,
    pub offsets: PagedGrowableWriter,
    pub lengths: PagedGrowableWriter,
    pub values: Vec<u8>,
}

impl BinaryDocValuesFieldUpdates {
    #[allow(dead_code)]
    pub fn new(field: &str, max_doc: i32) -> BinaryDocValuesFieldUpdates {
        let bits_per_value = (max_doc - 1).bits_required() as i32;
        let docs = PagedMutableHugeWriter::new(1, PAGE_SIZE, bits_per_value, COMPACT);
        let offsets = PagedGrowableWriter::new(1, PAGE_SIZE, 1, FAST);
        let lengths = PagedGrowableWriter::new(1, PAGE_SIZE, 1, FAST);

        BinaryDocValuesFieldUpdates {
            field: field.to_string(),
            doc_values_type: DocValuesType::Binary,
            bits_per_value,
            size: 0,
            docs,
            offsets,
            lengths,
            values: vec![],
        }
    }
}

impl DocValuesFieldUpdates for BinaryDocValuesFieldUpdates {
    fn add(&mut self, doc: DocId, value: &DocValuesFieldUpdatesValue) -> Result<()> {
        // TODO: if the Sorter interface changes to take long indexes, we can remove that limitation
        if self.size == i32::max_value() as usize {
            bail!(IllegalState(
                "cannot support more than Integer.MAX_VALUE doc/value entries".into()
            ));
        }
        debug_assert!(
            value.doc_values_type == DocValuesType::Binary && value.binary_value.is_some()
        );
        let val = value.binary_value.as_ref().unwrap();

        // grow the structures to have room for more elements
        if self.docs.size() == self.size {
            self.docs = self.docs.grow_by_size(self.size + 1);
            self.offsets = self.offsets.grow_by_size(self.size + 1);
            self.lengths = self.lengths.grow_by_size(self.size + 1);
        }

        self.docs.set(self.size, doc as i64);
        self.offsets.set(self.size, self.values.len() as i64);
        self.lengths.set(self.size, val.len() as i64);
        self.values.extend_from_slice(val.bytes());
        self.size += 1;

        Ok(())
    }

    fn iterator(&mut self) -> Result<Box<dyn DocValuesFieldUpdatesIterator>> {
        let mut sorter = BinaryDocValuesInPlaceMergeSorter::new(self);
        sorter.sort(0, self.size as i32);

        Ok(Box::new(BinaryDocValuesFieldUpdatesIterator::new(self)))
    }

    fn any(&self) -> bool {
        self.size > 0
    }

    fn merge(&mut self, other: &dyn DocValuesFieldUpdates) -> Result<()> {
        debug_assert!(other.doc_values_type() == self.doc_values_type());
        let other_updates = other.as_binary();
        if other_updates.size + self.size > i32::max_value() as usize {
            bail!(IllegalState(format!(
                "cannot support more than Integer.MAX_VALUE doc/value entries; size={} \
                 other.size={}",
                self.size, other_updates.size
            )));
        }

        let new_size = self.size + other_updates.size;
        self.docs = self.docs.grow_by_size(new_size);
        self.offsets = self.offsets.grow_by_size(new_size);
        self.lengths = self.lengths.grow_by_size(new_size);
        for i in 0..other_updates.size {
            self.docs
                .set(self.size, other_updates.docs.get(i as DocId)?);
            self.offsets.set(
                self.size,
                self.values.len() as i64 + other_updates.offsets.get(i as DocId)?,
            );
            self.lengths
                .set(self.size, other_updates.lengths.get(i as DocId)?);
            self.size += 1;
        }

        self.values.extend_from_slice(&other_updates.values[..]);
        Ok(())
    }

    fn doc_values_type(&self) -> &DocValuesType {
        &self.doc_values_type
    }

    fn as_binary(&self) -> &BinaryDocValuesFieldUpdates {
        self
    }

    fn as_base(&self) -> &dyn DocValuesFieldUpdates {
        self
    }
}

struct BinaryBytesIterator<'a> {
    value: Vec<u8>,
    lengths_iter: LongValuesIterator<'a>,
    bytes: &'a PagedBytes,
    input: PagedBytesDataInput,
    docs_with_field: &'a FixedBitSet,
    size: usize,
    max_doc: i32,
    upto: usize,
}

impl<'a> BinaryBytesIterator<'a> {
    fn new(
        max_doc: i32,
        lengths_iter: LongValuesIterator<'a>,
        size: usize,
        bytes: &'a PagedBytes,
        docs_with_field: &'a FixedBitSet,
    ) -> Self {
        let input = bytes.get_input().unwrap();
        BinaryBytesIterator {
            lengths_iter,
            bytes,
            input,
            size,
            max_doc,
            docs_with_field,
            value: vec![],
            upto: 0,
        }
    }

    fn has_next(&self) -> bool {
        (self.upto as i32) < self.max_doc
    }
}

impl<'a> Iterator for BinaryBytesIterator<'a> {
    type Item = Result<BytesRef>;

    fn next(&mut self) -> Option<Result<BytesRef>> {
        if !self.has_next() {
            return None;
        }

        if self.upto < self.size {
            let length = self.lengths_iter.next().unwrap() as usize;
            self.value.resize(length, 0u8);
            if let Err(e) = self.input.read_exact(&mut self.value) {
                return Some(Err(e.into()));
            }
            match self.docs_with_field.get(self.upto) {
                Err(e) => {
                    return Some(Err(e));
                }
                Ok(false) => {
                    self.value.clear();
                }
                Ok(true) => {}
            }
        } else {
            self.value.clear();
        }

        self.upto += 1;
        Some(Ok(BytesRef::new(&self.value)))
    }
}

impl<'a> ReusableIterator for BinaryBytesIterator<'a> {
    fn reset(&mut self) {
        self.input = self.bytes.get_input().unwrap();
        self.lengths_iter.reset();
        self.upto = 0;
        self.value.clear();
    }
}

struct SortedDVIter {
    dv: CachedBinaryDVs,
    doc: i32,
    max_doc: i32,
}

impl SortedDVIter {
    fn new(dv: CachedBinaryDVs) -> Self {
        let max_doc = dv.values.len() as i32;
        Self {
            dv,
            doc: -1,
            max_doc,
        }
    }
}

impl Iterator for SortedDVIter {
    type Item = Result<BytesRef>;

    fn next(&mut self) -> Option<Self::Item> {
        self.doc += 1;
        if self.doc >= self.max_doc {
            None
        } else {
            let value = BytesRef::new(&self.dv.values[self.doc as usize]);
            Some(Ok(value))
        }
    }
}

impl ReusableIterator for SortedDVIter {
    fn reset(&mut self) {
        self.doc = -1;
    }
}
