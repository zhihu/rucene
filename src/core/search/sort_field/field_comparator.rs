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

use core::codec::doc_values::NumericDocValues;
use core::index::reader::{LeafReaderContext, SearchLeafReader};
use core::search::sort_field::{SortFieldType, SortedWrapperDocValuesSource};
use core::util::{BitsMut, DocId, VariantValue};
use error::Result;

use core::codec::Codec;
use std::cmp::Ordering;
use std::fmt;

#[derive(Copy, Clone, Debug)]
pub enum ComparatorValue {
    Doc(DocId),
    Score(f32), // this is only used in RelevanceComparator
}

impl ComparatorValue {
    fn is_doc(self) -> bool {
        match self {
            ComparatorValue::Doc(_) => true,
            _ => false,
        }
    }

    fn is_score(self) -> bool {
        match self {
            ComparatorValue::Score(_) => true,
            _ => false,
        }
    }

    fn doc(self) -> DocId {
        debug_assert!(self.is_doc());
        if let ComparatorValue::Doc(d) = self {
            d
        } else {
            unreachable!()
        }
    }

    fn score(self) -> f32 {
        debug_assert!(self.is_score());
        if let ComparatorValue::Score(s) = self {
            s
        } else {
            unreachable!()
        }
    }

    pub fn as_variant(self) -> VariantValue {
        match self {
            ComparatorValue::Doc(d) => VariantValue::Int(d),
            ComparatorValue::Score(s) => VariantValue::Float(s),
        }
    }
}

impl Eq for ComparatorValue {}

impl PartialEq for ComparatorValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ComparatorValue::Doc(d1), ComparatorValue::Doc(d2)) => *d1 == *d2,
            (ComparatorValue::Score(s1), ComparatorValue::Score(s2)) => s1.eq(s2),
            (_, _) => false,
        }
    }
}

impl Ord for ComparatorValue {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (ComparatorValue::Doc(d1), ComparatorValue::Doc(d2)) => d1.cmp(d2),
            (ComparatorValue::Score(s1), ComparatorValue::Score(s2)) => {
                (*s1).partial_cmp(s2).unwrap()
            }
            (_, _) => panic!("Non-comparable"),
        }
    }
}

impl PartialOrd for ComparatorValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Expert: a FieldComparator compares hits so as to determine their
/// sort order when collecting the top results with `TopFieldCollector`.
/// The concrete public FieldComparator
/// classes here correspond to the SortField types.
///
/// This API is designed to achieve high performance
/// sorting, by exposing a tight interaction with {@link
/// FieldValueHitQueue} as it visits hits.  Whenever a hit is
/// competitive, it's enrolled into a virtual slot, which is
/// an int ranging from 0 to numHits-1. Segment transitions are
/// handled by creating a dedicated per-segment
/// {@link LeafFieldComparator} which also needs to interact
/// with the {@link FieldValueHitQueue} but can optimize based
/// on the segment to collect.
///
/// The following functions need to be implemented:
/// * `compare()` Compare a hit at 'slot a' with hit 'slot b'.
/// * `set_top_value()` This method is called by `TopFieldCollector` to notify the FieldComparator
///   of the top most value, which is used by future calls to `LeafFieldComparator::compare_top`
/// * `leaf_comparator()` Invoked when the search is switching to the next segment. You may need to
///   update internal state of the comparator, for example retrieving new values from DocValues.
/// * `value()` Return the sort value stored in the specified slot.  This is only called at the end
///   of the search, in order to populate `FieldDoc::fields` when returning the top results.
pub trait FieldComparator: fmt::Display {
    fn compare(&self, slot1: usize, slot2: usize) -> Ordering;

    fn value(&self, slot: usize) -> VariantValue;

    fn set_bottom(&mut self, slot: usize);

    fn compare_bottom(&mut self, value: ComparatorValue) -> Result<Ordering>;

    fn copy(&mut self, slot: usize, value: ComparatorValue) -> Result<()>;

    fn get_information_from_reader<C: Codec>(
        &mut self,
        reader: &LeafReaderContext<'_, C>,
    ) -> Result<()>;

    fn get_type(&self) -> SortFieldType;
}

pub enum FieldComparatorEnum {
    Score(RelevanceComparator),
    Doc(DocComparator),
    NumericDV(NumericDocValuesComparator<DefaultDocValuesSource>),
    SortedNumericDV(NumericDocValuesComparator<SortedWrapperDocValuesSource>),
}

impl FieldComparator for FieldComparatorEnum {
    fn compare(&self, slot1: usize, slot2: usize) -> Ordering {
        match self {
            FieldComparatorEnum::Score(c) => c.compare(slot1, slot2),
            FieldComparatorEnum::Doc(c) => c.compare(slot1, slot2),
            FieldComparatorEnum::NumericDV(c) => c.compare(slot1, slot2),
            FieldComparatorEnum::SortedNumericDV(c) => c.compare(slot1, slot2),
        }
    }

    fn value(&self, slot: usize) -> VariantValue {
        match self {
            FieldComparatorEnum::Score(c) => c.value(slot),
            FieldComparatorEnum::Doc(c) => c.value(slot),
            FieldComparatorEnum::NumericDV(c) => c.value(slot),
            FieldComparatorEnum::SortedNumericDV(c) => c.value(slot),
        }
    }

    fn set_bottom(&mut self, slot: usize) {
        match self {
            FieldComparatorEnum::Score(c) => c.set_bottom(slot),
            FieldComparatorEnum::Doc(c) => c.set_bottom(slot),
            FieldComparatorEnum::NumericDV(c) => c.set_bottom(slot),
            FieldComparatorEnum::SortedNumericDV(c) => c.set_bottom(slot),
        }
    }

    fn compare_bottom(&mut self, value: ComparatorValue) -> Result<Ordering> {
        match self {
            FieldComparatorEnum::Score(c) => c.compare_bottom(value),
            FieldComparatorEnum::Doc(c) => c.compare_bottom(value),
            FieldComparatorEnum::NumericDV(c) => c.compare_bottom(value),
            FieldComparatorEnum::SortedNumericDV(c) => c.compare_bottom(value),
        }
    }

    fn copy(&mut self, slot: usize, value: ComparatorValue) -> Result<()> {
        match self {
            FieldComparatorEnum::Score(c) => c.copy(slot, value),
            FieldComparatorEnum::Doc(c) => c.copy(slot, value),
            FieldComparatorEnum::NumericDV(c) => c.copy(slot, value),
            FieldComparatorEnum::SortedNumericDV(c) => c.copy(slot, value),
        }
    }

    fn get_information_from_reader<C: Codec>(
        &mut self,
        reader: &LeafReaderContext<'_, C>,
    ) -> Result<()> {
        match self {
            FieldComparatorEnum::Score(c) => c.get_information_from_reader(reader),
            FieldComparatorEnum::Doc(c) => c.get_information_from_reader(reader),
            FieldComparatorEnum::NumericDV(c) => c.get_information_from_reader(reader),
            FieldComparatorEnum::SortedNumericDV(c) => c.get_information_from_reader(reader),
        }
    }

    fn get_type(&self) -> SortFieldType {
        match self {
            FieldComparatorEnum::Score(c) => c.get_type(),
            FieldComparatorEnum::Doc(c) => c.get_type(),
            FieldComparatorEnum::NumericDV(c) => c.get_type(),
            FieldComparatorEnum::SortedNumericDV(c) => c.get_type(),
        }
    }
}

impl fmt::Display for FieldComparatorEnum {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FieldComparatorEnum::Score(c) => write!(f, "FieldComparatorEnum({})", c),
            FieldComparatorEnum::Doc(c) => write!(f, "FieldComparatorEnum({})", c),
            FieldComparatorEnum::NumericDV(c) => write!(f, "FieldComparatorEnum({})", c),
            FieldComparatorEnum::SortedNumericDV(c) => write!(f, "FieldComparatorEnum({})", c),
        }
    }
}

/// Sorts by descending relevance.
///
/// NOTE: if you are sorting only gy descending relevance and then
/// secondarily by ascending doc_id, performance is faster using
/// `TopScoreDocCollector` directly.
pub struct RelevanceComparator {
    scores: Vec<f32>,
    bottom: f32,
}

impl RelevanceComparator {
    pub fn new(num_hits: usize) -> RelevanceComparator {
        let scores = vec![0f32; num_hits];

        RelevanceComparator {
            scores,
            bottom: 0f32,
        }
    }
}

impl FieldComparator for RelevanceComparator {
    fn compare(&self, slot1: usize, slot2: usize) -> Ordering {
        self.scores[slot2]
            .partial_cmp(&self.scores[slot1])
            .unwrap_or(Ordering::Equal)
    }

    fn value(&self, slot: usize) -> VariantValue {
        VariantValue::Float(self.scores[slot])
    }

    fn set_bottom(&mut self, slot: usize) {
        self.bottom = self.scores[slot];
    }

    fn compare_bottom(&mut self, value: ComparatorValue) -> Result<Ordering> {
        debug_assert!(value.is_score());
        Ok(value
            .score()
            .partial_cmp(&self.bottom)
            .unwrap_or(Ordering::Equal))
    }

    fn copy(&mut self, slot: usize, value: ComparatorValue) -> Result<()> {
        debug_assert!(value.is_score());
        self.scores[slot] = value.score();
        Ok(())
    }

    fn get_information_from_reader<C: Codec>(
        &mut self,
        _reader: &LeafReaderContext<'_, C>,
    ) -> Result<()> {
        Ok(())
    }

    fn get_type(&self) -> SortFieldType {
        SortFieldType::Score
    }
}

impl fmt::Display for RelevanceComparator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "bottom: {:?}\tscores: {:?}", self.bottom, self.scores)
    }
}

/// Sorts by ascending docID
pub struct DocComparator {
    doc_ids: Vec<i32>,
    bottom: i32,
    doc_base: i32,
}

impl DocComparator {
    pub fn new(num_hits: usize) -> DocComparator {
        let mut doc_ids: Vec<i32> = Vec::with_capacity(num_hits);
        for _ in 0..num_hits {
            doc_ids.push(0);
        }

        DocComparator {
            doc_ids,
            bottom: 0,
            doc_base: 0,
        }
    }
}

impl FieldComparator for DocComparator {
    fn compare(&self, slot1: usize, slot2: usize) -> Ordering {
        self.doc_ids[slot1].cmp(&self.doc_ids[slot2])
    }

    fn value(&self, slot: usize) -> VariantValue {
        VariantValue::Int(self.doc_ids[slot])
    }

    fn set_bottom(&mut self, slot: usize) {
        self.bottom = self.doc_ids[slot];
    }

    fn compare_bottom(&mut self, value: ComparatorValue) -> Result<Ordering> {
        debug_assert!(value.is_doc());
        Ok(self.bottom.cmp(&value.doc()))
    }

    fn copy(&mut self, slot: usize, value: ComparatorValue) -> Result<()> {
        debug_assert!(value.is_doc());
        self.doc_ids[slot] = value.doc() + self.doc_base;
        Ok(())
    }

    fn get_information_from_reader<C: Codec>(
        &mut self,
        reader: &LeafReaderContext<'_, C>,
    ) -> Result<()> {
        self.doc_base = reader.doc_base;
        Ok(())
    }

    fn get_type(&self) -> SortFieldType {
        SortFieldType::Doc
    }
}

impl fmt::Display for DocComparator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "bottom: {:?}\tdoc_base: {:?}\tdoc_ids: {:?}",
            self.bottom, self.doc_base, self.doc_ids
        )
    }
}

/// compare doc hit by numeric doc values field
pub struct NumericDocValuesComparator<T: DocValuesSource> {
    missing_value: Option<VariantValue>,
    field: String,
    field_type: SortFieldType,
    docs_with_fields: Option<Box<dyn BitsMut>>,
    current_read_values: Option<Box<dyn NumericDocValues>>,
    values: Vec<VariantValue>,
    bottom: VariantValue,
    top_value: VariantValue,
    doc_values_source: T,
}

impl<T: DocValuesSource> NumericDocValuesComparator<T> {
    pub fn new(
        num_hits: usize,
        field: String,
        field_type: SortFieldType,
        missing_value: Option<VariantValue>,
        doc_values_source: T,
    ) -> Self {
        NumericDocValuesComparator {
            missing_value,
            field,
            field_type,
            doc_values_source,
            docs_with_fields: None,
            current_read_values: None,
            // the following three field default value are useless, just using to
            // avoid Option
            values: vec![VariantValue::Int(0); num_hits],
            bottom: VariantValue::Int(0),
            top_value: VariantValue::Int(0),
        }
    }

    fn get_doc_value(&mut self, doc_id: DocId) -> Result<VariantValue> {
        let raw_value = self.current_read_values.as_mut().unwrap().get_mut(doc_id)?;
        let value = match self.field_type {
            SortFieldType::Int => VariantValue::Int(raw_value as i32),
            SortFieldType::Long => VariantValue::Long(raw_value),
            SortFieldType::Float => VariantValue::Float(f32::from_bits(raw_value as u32)),
            SortFieldType::Double => VariantValue::Double(f64::from_bits(raw_value as u64)),
            _ => {
                unreachable!();
            }
        };
        Ok(value)
    }
}

impl<T: DocValuesSource> FieldComparator for NumericDocValuesComparator<T> {
    fn compare(&self, slot1: usize, slot2: usize) -> Ordering {
        self.values[slot1].cmp(&self.values[slot2])
    }

    fn value(&self, slot: usize) -> VariantValue {
        self.values[slot].clone()
    }

    fn set_bottom(&mut self, slot: usize) {
        self.bottom = self.values[slot].clone();
    }

    fn compare_bottom(&mut self, value: ComparatorValue) -> Result<Ordering> {
        debug_assert!(value.is_doc());
        let doc_id = value.doc();
        let value = self.get_doc_value(doc_id)?;
        if let Some(ref mut bits) = self.docs_with_fields {
            if value.is_zero() && bits.get(doc_id as usize)? {
                return Ok(self.bottom.cmp(self.missing_value.as_ref().unwrap()));
            }
        }
        Ok(self.bottom.cmp(&value))
    }

    fn copy(&mut self, slot: usize, value: ComparatorValue) -> Result<()> {
        debug_assert!(value.is_doc());
        let doc_id = value.doc();
        let mut value = self.get_doc_value(doc_id)?;
        if let Some(ref mut bits) = self.docs_with_fields {
            if value.is_zero() && bits.get(doc_id as usize)? {
                value = self.missing_value.as_ref().unwrap().clone();
            }
        }
        self.values[slot] = value;
        Ok(())
    }

    fn get_information_from_reader<C: Codec>(
        &mut self,
        reader: &LeafReaderContext<'_, C>,
    ) -> Result<()> {
        self.current_read_values = Some(
            self.doc_values_source
                .numeric_doc_values(reader.reader, &self.field)?,
        );
        if self.missing_value.is_some() {
            self.docs_with_fields = Some(
                self.doc_values_source
                    .docs_with_fields(reader.reader, &self.field)?,
            );
        }

        Ok(())
    }

    fn get_type(&self) -> SortFieldType {
        self.field_type
    }
}

impl<T: DocValuesSource> fmt::Display for NumericDocValuesComparator<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "NumericDocValuesComparator(field: {}, type: {:?}, botton: {}, top: {})",
            self.field, self.field_type, self.bottom, self.top_value
        )
    }
}

pub trait DocValuesSource {
    fn numeric_doc_values<C: Codec>(
        &self,
        reader: &SearchLeafReader<C>,
        field: &str,
    ) -> Result<Box<dyn NumericDocValues>>;

    fn docs_with_fields<C: Codec>(
        &self,
        reader: &SearchLeafReader<C>,
        field: &str,
    ) -> Result<Box<dyn BitsMut>>;
}

#[derive(Default)]
pub struct DefaultDocValuesSource;

impl DocValuesSource for DefaultDocValuesSource {
    fn numeric_doc_values<C: Codec>(
        &self,
        reader: &SearchLeafReader<C>,
        field: &str,
    ) -> Result<Box<dyn NumericDocValues>> {
        reader.get_numeric_doc_values(field)
    }
    fn docs_with_fields<C: Codec>(
        &self,
        reader: &SearchLeafReader<C>,
        field: &str,
    ) -> Result<Box<dyn BitsMut>> {
        reader.get_docs_with_field(field)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::index::reader::IndexReader;
    use core::index::tests::*;

    #[test]
    fn test_relevance_comparator() {
        let mut comparator = RelevanceComparator::new(3);
        {
            comparator.copy(0, ComparatorValue::Score(1f32)).unwrap();
            comparator.copy(1, ComparatorValue::Score(2f32)).unwrap();
            comparator.copy(2, ComparatorValue::Score(3f32)).unwrap();
        }

        assert_eq!(comparator.compare(0, 1), Ordering::Greater);
        assert_eq!(comparator.value(1), VariantValue::Float(2f32));

        {
            comparator.set_bottom(2);
        }

        assert_eq!(
            comparator
                .compare_bottom(ComparatorValue::Score(10f32))
                .unwrap(),
            Ordering::Greater
        );
    }

    #[test]
    fn test_doc_comparator() {
        let mut comparator = DocComparator::new(3);

        let leaf_reader = MockLeafReader::new(0);
        let index_reader = MockIndexReader::new(vec![leaf_reader]);
        let leaf_reader_context = index_reader.leaves();
        {
            comparator
                .get_information_from_reader(&leaf_reader_context[0])
                .unwrap();
            comparator.copy(0, ComparatorValue::Doc(1)).unwrap();
            comparator.copy(1, ComparatorValue::Doc(2)).unwrap();
            comparator.copy(2, ComparatorValue::Doc(3)).unwrap();
        }

        assert_eq!(comparator.compare(0, 1), Ordering::Less);
        assert_eq!(comparator.value(1), VariantValue::Int(2));

        {
            comparator.set_bottom(2);
        }

        assert_eq!(
            comparator.compare_bottom(ComparatorValue::Doc(2)).unwrap(),
            Ordering::Greater
        );
    }
}
