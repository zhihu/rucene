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

mod field_comparator;

pub use self::field_comparator::*;

mod sort_field;

pub use self::sort_field::*;

mod collapse_top_docs;

pub use self::collapse_top_docs::*;

mod search_group;

pub use self::search_group::*;

/// Encapsulates sort criteria for returned hits.
///
/// The fields used to determine sort order must be carefully chosen.
/// Documents must contain a single term in such a field,
/// and the value of the term should indicate the document's relative position in
/// a given sort order.  The field must be indexed, but should not be tokenized,
/// and does not need to be stored (unless you happen to want it back with the
/// rest of your document data).
///
/// ### Valid Types of Values
///
/// There are four possible kinds of term values which may be put into
/// sorting fields: Integers, Longs, Floats, or Strings.  Unless
/// `SortField` objects are specified, the type of value
/// in the field is determined by parsing the first term in the field.
///
/// Integer term values should contain only digits and an optional
/// preceding negative sign.  Values must be base 10 and in the range
/// `i32::min_value()` and `i32::max_value()` inclusive.
/// Documents which should appear first in the sort
/// should have low value integers, later documents high values
/// (i.e. the documents should be numbered `1..n` where
/// `1` is the first and `n` the last).
///
/// Long term values should contain only digits and an optional
/// preceding negative sign.  Values must be base 10 and in the range
/// `i64::min_value()` and `i64::max_value()` inclusive.
/// Documents which should appear first in the sort
/// should have low value integers, later documents high values.
///
/// Float term values should conform to values accepted by
/// {@link Float Float.valueOf(String)} (except that `NaN`
/// and `Infinity` are not supported).
/// Documents which should appear first in the sort
/// should have low values, later documents high values.
///
/// String term values can contain any valid String, but should
/// not be tokenized.  The values are sorted according to their
/// {@link Comparable natural order}.  Note that using this type
/// of term value has higher memory requirements than the other
/// two types.
///
/// ### Object Reuse
///
/// One of these objects can be
/// used multiple times and the sort order changed between usages.
///
/// This class is thread safe.
///
/// ### Memory Usage
///
/// Sorting uses of caches of term values maintained by the
/// internal HitQueue(s).  The cache is static and contains an integer
/// or float array of length `IndexReader.max_doc()` for each field
/// name for which a sort is performed.  In other words, the size of the
/// cache in bytes is:
///
/// `4 * IndexReader.max_doc() * (# of different fields actually used to sort)`
///
/// For String fields, the cache is larger: in addition to the
/// above array, the value of every term in the field is kept in memory.
/// If there are many unique terms in the field, this could
/// be quite large.
///
/// Note that the size of the cache is not affected by how many
/// fields are in the index and *might* be used to sort - only by
/// the ones actually used to sort a result set.
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
