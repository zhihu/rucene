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

use core::search::sort_field::*;

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct Sort {
    fields: Vec<SortField>,
}

impl Sort {
    pub fn new(fields: Vec<SortField>) -> Sort {
        Sort { fields }
    }

    pub fn get_sort(&self) -> &[SortField] {
        &self.fields
    }

    pub fn needs_scores(&self) -> bool {
        self.fields.iter().any(|f| f.needs_scores())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sort() {
        let sort_fields: Vec<SortField> = vec![
            SortField::Simple(SimpleSortField::new(
                String::from("field_one"),
                SortFieldType::Score,
                true,
            )),
            SortField::Simple(SimpleSortField::new(
                String::from("field_two"),
                SortFieldType::Doc,
                false,
            )),
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
