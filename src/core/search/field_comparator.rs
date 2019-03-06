use core::index::LeafReader;
use core::index::NumericDocValuesRef;
use core::search::sort_field::SortFieldType;
use core::util::bits::BitsRef;
use core::util::{DocId, VariantValue};
use error::Result;

use std::cmp::Ordering;
use std::fmt;

#[derive(Copy, Clone, Debug)]
pub enum ComparatorValue {
    Doc(DocId),
    Score(f32), // this is only used in RelevanceComparator
}

impl ComparatorValue {
    fn is_doc(&self) -> bool {
        match self {
            ComparatorValue::Doc(_) => true,
            _ => false,
        }
    }

    fn is_score(&self) -> bool {
        match self {
            ComparatorValue::Score(_) => true,
            _ => false,
        }
    }

    fn doc(&self) -> DocId {
        debug_assert!(self.is_doc());
        if let ComparatorValue::Doc(d) = self {
            *d
        } else {
            unreachable!()
        }
    }

    fn score(&self) -> f32 {
        debug_assert!(self.is_score());
        if let ComparatorValue::Score(s) = self {
            *s
        } else {
            unreachable!()
        }
    }

    pub fn as_variant(&self) -> VariantValue {
        match self {
            ComparatorValue::Doc(d) => VariantValue::Int(*d),
            ComparatorValue::Score(s) => VariantValue::Float(*s),
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

pub trait FieldComparator: fmt::Display {
    fn compare(&self, slot1: usize, slot2: usize) -> Ordering;

    fn value(&self, slot: usize) -> VariantValue;

    fn set_bottom(&mut self, slot: usize);

    fn compare_bottom(&self, value: ComparatorValue) -> Result<Ordering>;

    fn copy(&mut self, slot: usize, value: ComparatorValue) -> Result<()>;

    fn get_information_from_reader(&mut self, reader: &LeafReader) -> Result<()>;

    fn get_type(&self) -> SortFieldType;
}

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

    fn compare_bottom(&self, value: ComparatorValue) -> Result<Ordering> {
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

    fn get_information_from_reader(&mut self, _reader: &LeafReader) -> Result<()> {
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

    fn compare_bottom(&self, value: ComparatorValue) -> Result<Ordering> {
        debug_assert!(value.is_doc());
        Ok(self.bottom.cmp(&value.doc()))
    }

    fn copy(&mut self, slot: usize, value: ComparatorValue) -> Result<()> {
        debug_assert!(value.is_doc());
        self.doc_ids[slot] = value.doc() + self.doc_base;
        Ok(())
    }

    fn get_information_from_reader(&mut self, reader: &LeafReader) -> Result<()> {
        self.doc_base = reader.doc_base();
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

pub struct NumericDocValuesComparator<T: DocValuesSource> {
    missing_value: VariantValue,
    field: String,
    field_type: SortFieldType,
    docs_with_fields: Option<BitsRef>,
    current_read_values: Option<NumericDocValuesRef>,
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
        missing_value: VariantValue,
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

    fn get_doc_value(&self, doc_id: DocId) -> Result<VariantValue> {
        let raw_value = self.current_read_values.as_ref().unwrap().get(doc_id)?;
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

    fn compare_bottom(&self, value: ComparatorValue) -> Result<Ordering> {
        debug_assert!(value.is_doc());
        let doc_id = value.doc();
        let value = self.get_doc_value(doc_id)?;
        if let Some(ref bits) = self.docs_with_fields {
            if value.is_zero() && bits.get(doc_id as usize)? {
                return Ok(self.bottom.cmp(&self.missing_value));
            }
        }
        Ok(self.bottom.cmp(&value))
    }

    fn copy(&mut self, slot: usize, value: ComparatorValue) -> Result<()> {
        debug_assert!(value.is_doc());
        let doc_id = value.doc();
        let mut value = self.get_doc_value(doc_id)?;
        if let Some(ref bits) = self.docs_with_fields {
            if value.is_zero() && bits.get(doc_id as usize)? {
                value = self.missing_value.clone();
            }
        }
        self.values[slot] = value;
        Ok(())
    }

    fn get_information_from_reader(&mut self, reader: &LeafReader) -> Result<()> {
        self.current_read_values = Some(self.doc_values_source
            .numeric_doc_values(reader, &self.field)?);
        self.docs_with_fields = Some(self.doc_values_source
            .docs_with_fields(reader, &self.field)?);
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
    fn numeric_doc_values(&self, reader: &LeafReader, field: &str) -> Result<NumericDocValuesRef>;
    fn docs_with_fields(&self, reader: &LeafReader, field: &str) -> Result<BitsRef>;
}

#[derive(Default)]
pub struct DefaultDocValuesSource;

impl DocValuesSource for DefaultDocValuesSource {
    fn numeric_doc_values(&self, reader: &LeafReader, field: &str) -> Result<NumericDocValuesRef> {
        reader.get_numeric_doc_values(field)
    }
    fn docs_with_fields(&self, reader: &LeafReader, field: &str) -> Result<BitsRef> {
        reader.get_docs_with_field(field)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::index::tests::*;

    #[test]
    fn test_relevance_comparator() {
        let mut comparator = RelevanceComparator::new(3);
        {
            comparator.copy(0, ComparatorValue::Score(1f32));
            comparator.copy(1,  ComparatorValue::Score(2f32));
            comparator.copy(2,  ComparatorValue::Score(3f32));
        }

        assert_eq!(comparator.compare(0, 1), Ordering::Greater);
        assert_eq!(comparator.value(1), VariantValue::Float(2f32));

        {
            comparator.set_bottom(2);
        }

        assert_eq!(
            comparator
                .compare_bottom( ComparatorValue::Score(10f32))
                .unwrap(),
            Ordering::Greater
        );
    }

    #[test]
    fn test_doc_comparator() {
        let mut comparator = DocComparator::new(3);

        let leaf_reader = MockLeafReader::new(0);
        {
            comparator.get_information_from_reader(&leaf_reader);
            comparator.copy(0, ComparatorValue::Doc(1));
            comparator.copy(1, ComparatorValue::Doc(2));
            comparator.copy(2, ComparatorValue::Doc(3));
        }

        assert_eq!(comparator.compare(0, 1), Ordering::Less);
        assert_eq!(comparator.value(1), VariantValue::Int(2));

        {
            comparator.set_bottom(2);
        }

        assert_eq!(
            comparator
                .compare_bottom( ComparatorValue::Doc(2))
                .unwrap(),
            Ordering::Greater
        );
    }
}
