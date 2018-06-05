use std::fmt;

use core::index::LeafReader;
use core::search::sort_field::SortFieldType;
use core::util::VariantValue;

pub trait FieldComparator: fmt::Display {
    fn compare(&self, slot1: usize, slot2: usize) -> i32;

    fn value(&self, slot: usize) -> VariantValue;

    fn set_bottom(&mut self, slot: usize);

    fn compare_bottom(&self, value: VariantValue) -> i32;

    fn copy(&mut self, slot: usize, value: &VariantValue);

    fn get_information_from_reader(&mut self, reader: &LeafReader);

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
    fn compare(&self, slot1: usize, slot2: usize) -> i32 {
        let result = self.scores[slot2].partial_cmp(&self.scores[slot1]);
        debug_assert!(result.is_some());
        result.unwrap() as i32
    }

    fn value(&self, slot: usize) -> VariantValue {
        VariantValue::Float(self.scores[slot])
    }

    fn set_bottom(&mut self, slot: usize) {
        self.bottom = self.scores[slot];
    }

    fn compare_bottom(&self, value: VariantValue) -> i32 {
        let score = match value {
            VariantValue::Float(f) => f,
            _ => unreachable!(),
        };
        score.partial_cmp(&self.bottom).unwrap() as i32
    }

    fn copy(&mut self, slot: usize, value: &VariantValue) {
        let score = match *value {
            VariantValue::Float(f) => f,
            _ => unreachable!(),
        };
        self.scores[slot] = score;
    }

    fn get_information_from_reader(&mut self, _reader: &LeafReader) {}

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
    fn compare(&self, slot1: usize, slot2: usize) -> i32 {
        self.doc_ids[slot1]
            .partial_cmp(&self.doc_ids[slot2])
            .unwrap() as i32
    }

    fn value(&self, slot: usize) -> VariantValue {
        VariantValue::Int(self.doc_ids[slot])
    }

    fn set_bottom(&mut self, slot: usize) {
        self.bottom = self.doc_ids[slot];
    }

    fn compare_bottom(&self, value: VariantValue) -> i32 {
        let doc_id = match value {
            VariantValue::Int(i) => i + self.doc_base,
            _ => unreachable!(),
        };
        self.bottom.partial_cmp(&doc_id).unwrap() as i32
    }

    fn copy(&mut self, slot: usize, value: &VariantValue) {
        let doc_id = match *value {
            VariantValue::Int(i) => i + self.doc_base,
            _ => unreachable!(),
        };
        self.doc_ids[slot] = doc_id;
    }

    fn get_information_from_reader(&mut self, reader: &LeafReader) {
        self.doc_base = reader.doc_base();
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

#[cfg(test)]
mod tests {
    use super::*;
    use core::index::tests::*;

    #[test]
    fn test_relevance_comparator() {
        let mut comparator = RelevanceComparator::new(3);
        {
            comparator.copy(0, &VariantValue::Float(1f32));
            comparator.copy(1, &VariantValue::Float(2f32));
            comparator.copy(2, &VariantValue::Float(3f32));
        }

        assert_eq!(comparator.compare(0, 1), 1);
        assert_eq!(comparator.value(1), VariantValue::Float(2f32));

        {
            comparator.set_bottom(2);
        }

        assert_eq!(comparator.compare_bottom(VariantValue::Float(10f32)), 1);
    }

    #[test]
    fn test_doc_comparator() {
        let mut comparator = DocComparator::new(3);

        let leaf_reader = MockLeafReader::new(0);
        {
            comparator.get_information_from_reader(&leaf_reader);
            comparator.copy(0, &VariantValue::Int(1));
            comparator.copy(1, &VariantValue::Int(2));
            comparator.copy(2, &VariantValue::Int(3));
        }

        assert_eq!(comparator.compare(0, 1), -1);
        assert_eq!(comparator.value(1), VariantValue::Int(2));

        {
            comparator.set_bottom(2);
        }

        assert_eq!(comparator.compare_bottom(VariantValue::Int(1)), 1);
    }
}
