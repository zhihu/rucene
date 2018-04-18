use core::search::sort_field::*;

pub struct Sort {
    fields: Vec<SortField>,
}

impl Sort {
    pub fn new(fields: Vec<SortField>) -> Sort {
        Sort { fields }
    }

    pub fn get_sort(&self) -> &Vec<SortField> {
        &self.fields
    }

    pub fn needs_scores(&self) -> bool {
        for field in &self.fields {
            if field.needs_scores() {
                return true;
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sort() {
        let sort_fields: Vec<SortField> = vec![
            SortField::new(String::from("field_one"), SortFieldType::Score, true),
            SortField::new(String::from("field_two"), SortFieldType::Doc, false),
        ];
        let sort = Sort::new(sort_fields);

        assert!(sort.needs_scores());

        let fields = sort.get_sort();
        assert_eq!(fields.len(), 2);

        let score_field = &fields[0];
        assert_eq!(score_field.field(), &String::from("field_one"));

        let doc_field = &fields[1];
        assert_eq!(doc_field.field(), &String::from("field_two"));
    }
}
