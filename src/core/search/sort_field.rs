use core::search::field_comparator::*;
use core::util::VariantValue;

#[derive(PartialEq, Debug, Clone, Eq)]
pub enum SortFieldType {
    Score,
    Doc,
}

#[derive(Clone, Debug)]
pub struct SortField {
    pub field: String,
    pub field_type: SortFieldType,
    pub is_reverse: bool,
}

impl SortField {
    pub fn new(field: String, field_type: SortFieldType, is_reverse: bool) -> SortField {
        SortField {
            field,
            field_type,
            is_reverse,
        }
    }

    pub fn new_score() -> SortField {
        SortField {
            field: String::new(),
            field_type: SortFieldType::Score,
            is_reverse: false,
        }
    }

    pub fn field(&self) -> &String {
        &self.field
    }

    pub fn field_type(&self) -> &SortFieldType {
        &self.field_type
    }

    pub fn reverse(&self) -> i32 {
        if self.is_reverse {
            -1
        } else {
            1
        }
    }

    pub fn get_comparator(
        &self,
        num_hits: usize,
        _missing_value: &Option<VariantValue>,
    ) -> Option<Box<FieldComparator>> {
        match self.field_type {
            SortFieldType::Score => Some(Box::new(RelevanceComparator::new(num_hits))),
            SortFieldType::Doc => Some(Box::new(DocComparator::new(num_hits))),
        }
    }

    pub fn needs_scores(&self) -> bool {
        self.field_type == SortFieldType::Score
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sort_field_with_score_type() {
        let sort_field = SortField::new(String::from("test"), SortFieldType::Score, true);

        assert_eq!("test", sort_field.field);
        assert_eq!(SortFieldType::Score, sort_field.field_type);
        assert_eq!(true, sort_field.is_reverse);

        sort_field.get_comparator(5, &None).unwrap();
    }

    #[test]
    fn test_sort_field_with_doc_type() {
        let sort_field = SortField::new(String::from("test"), SortFieldType::Doc, true);

        assert_eq!("test", sort_field.field);
        assert_eq!(SortFieldType::Doc, sort_field.field_type);
        assert_eq!(true, sort_field.is_reverse);

        sort_field.get_comparator(5, &None).unwrap();
    }
}
