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

use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

use core::search::sort_field::{ComparatorValue, SortFieldType};
use core::util::DocId;
use core::util::VariantValue;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WilsonInfo {
    pub doc_id: DocId,
    pub score: VariantValue,
    pub wilson_score: Option<VariantValue>,
}

impl WilsonInfo {
    pub fn new(
        doc_id: DocId,
        score: VariantValue,
        wilson_score: Option<VariantValue>,
    ) -> WilsonInfo {
        WilsonInfo {
            doc_id,
            score,
            wilson_score,
        }
    }
}

impl Ord for WilsonInfo {
    fn cmp(&self, other: &Self) -> Ordering {
        match (&self.wilson_score, &other.wilson_score) {
            (Some(w1), Some(w2)) => w1.cmp(w2),
            (Some(_), None) => Ordering::Greater,
            (None, Some(_)) => Ordering::Less,
            (None, None) => Ordering::Equal,
        }
    }
}

impl PartialOrd for WilsonInfo {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SortInfo {
    pub sort_type: SortFieldType,
    pub sort_value: ComparatorValue,
}

impl SortInfo {
    pub fn new(sort_type: SortFieldType, sort_value: ComparatorValue) -> SortInfo {
        SortInfo {
            sort_type,
            sort_value,
        }
    }
}

#[derive(Debug, Clone, Eq)]
pub struct CollectedSearchGroup {
    pub group_value: VariantValue,
    pub sort_info_list: Vec<SortInfo>,
    pub comparator_slot: usize,
    pub max_wilson_info: WilsonInfo,

    pub top_doc: DocId,
}

impl CollectedSearchGroup {
    pub fn new(
        group_value: VariantValue,
        sort_info_list: Vec<SortInfo>,
        max_wilson_info: WilsonInfo,
        comparator_slot: usize,
        top_doc: DocId,
    ) -> Self {
        CollectedSearchGroup {
            group_value,
            sort_info_list,
            max_wilson_info,
            comparator_slot,
            top_doc,
        }
    }
}

impl Ord for CollectedSearchGroup {
    fn cmp(&self, other: &CollectedSearchGroup) -> Ordering {
        debug_assert_eq!(self.sort_info_list.len(), other.sort_info_list.len());
        for (index, sort_info) in self.sort_info_list.iter().enumerate() {
            match sort_info
                .sort_value
                .cmp(&other.sort_info_list[index].sort_value)
            {
                Ordering::Equal => {
                    continue;
                }
                x => match sort_info.sort_type {
                    SortFieldType::Score => return x.reverse(),
                    _ => return x,
                },
            }
        }

        self.top_doc.cmp(&other.top_doc)
    }
}

impl PartialOrd for CollectedSearchGroup {
    fn partial_cmp(&self, other: &CollectedSearchGroup) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Hash for CollectedSearchGroup {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.group_value.hash(state)
    }
}

impl PartialEq for CollectedSearchGroup {
    fn eq(&self, other: &CollectedSearchGroup) -> bool {
        self.group_value.eq(&other.group_value) && self.cmp(other) == Ordering::Equal
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collected_search_group() {
        let collected_search_group = CollectedSearchGroup::new(
            VariantValue::Int(1),
            vec![
                SortInfo::new(SortFieldType::Score, ComparatorValue::Score(1.0)),
                SortInfo::new(SortFieldType::Doc, ComparatorValue::Doc(2)),
            ],
            WilsonInfo::new(1, VariantValue::Float(1.0), Some(VariantValue::Double(0.0))),
            3,
            0,
        );

        assert_eq!(collected_search_group.group_value, VariantValue::Int(1));
        assert_eq!(
            collected_search_group.sort_info_list,
            vec![
                SortInfo::new(SortFieldType::Score, ComparatorValue::Score(1.0)),
                SortInfo::new(SortFieldType::Doc, ComparatorValue::Doc(2)),
            ]
        );
        assert_eq!(collected_search_group.comparator_slot, 3);
        assert_eq!(collected_search_group.top_doc, 0);
    }

    #[test]
    fn test_compare_collected_search_group() {
        let collected_search_group_one = CollectedSearchGroup::new(
            VariantValue::Int(1),
            vec![
                SortInfo::new(SortFieldType::Score, ComparatorValue::Score(1.0)),
                SortInfo::new(SortFieldType::Doc, ComparatorValue::Doc(2)),
            ],
            WilsonInfo::new(1, VariantValue::Float(1.0), Some(VariantValue::Double(0.0))),
            10,
            0,
        );

        // Equal
        {
            let collected_search_group_two = CollectedSearchGroup::new(
                VariantValue::Int(1),
                vec![
                    SortInfo::new(SortFieldType::Score, ComparatorValue::Score(1.0)),
                    SortInfo::new(SortFieldType::Doc, ComparatorValue::Doc(2)),
                ],
                WilsonInfo::new(1, VariantValue::Float(1.0), Some(VariantValue::Double(0.0))),
                10,
                0,
            );

            assert_eq!(collected_search_group_one, collected_search_group_two);
        }

        // Unequal because group_values is different
        {
            let collected_search_group_two = CollectedSearchGroup::new(
                VariantValue::Int(2),
                vec![
                    SortInfo::new(SortFieldType::Score, ComparatorValue::Score(1.0)),
                    SortInfo::new(SortFieldType::Doc, ComparatorValue::Doc(2)),
                ],
                WilsonInfo::new(1, VariantValue::Float(1.0), Some(VariantValue::Double(0.0))),
                10,
                0,
            );

            assert_ne!(collected_search_group_one, collected_search_group_two);
        }

        // Less because second sort_info_list of group_two is greater
        {
            let collected_search_group_two = CollectedSearchGroup::new(
                VariantValue::Int(1),
                vec![
                    SortInfo::new(SortFieldType::Score, ComparatorValue::Score(1.0)),
                    SortInfo::new(SortFieldType::Doc, ComparatorValue::Doc(3)),
                ],
                WilsonInfo::new(1, VariantValue::Float(1.0), Some(VariantValue::Double(0.0))),
                10,
                0,
            );

            assert_eq!(
                collected_search_group_one.cmp(&collected_search_group_two),
                Ordering::Less
            );
            assert_eq!(
                collected_search_group_one.partial_cmp(&collected_search_group_two),
                Some(Ordering::Less)
            );
        }

        // Greater because second sort_info_list of group_one is greater
        {
            let collected_search_group_two = CollectedSearchGroup::new(
                VariantValue::Int(1),
                vec![
                    SortInfo::new(SortFieldType::Score, ComparatorValue::Score(2.0)),
                    SortInfo::new(SortFieldType::Doc, ComparatorValue::Doc(2)),
                ],
                WilsonInfo::new(1, VariantValue::Float(1.0), Some(VariantValue::Double(0.0))),
                10,
                0,
            );

            assert_eq!(
                collected_search_group_one.cmp(&collected_search_group_two),
                Ordering::Greater
            );
            assert_eq!(
                collected_search_group_one.partial_cmp(&collected_search_group_two),
                Some(Ordering::Greater)
            );
        }

        // Less because top_doc of group_two is greater
        {
            let collected_search_group_two = CollectedSearchGroup::new(
                VariantValue::Int(1),
                vec![
                    SortInfo::new(SortFieldType::Score, ComparatorValue::Score(1.0)),
                    SortInfo::new(SortFieldType::Doc, ComparatorValue::Doc(2)),
                ],
                WilsonInfo::new(1, VariantValue::Float(1.0), Some(VariantValue::Double(0.0))),
                10,
                1,
            );

            assert_eq!(
                collected_search_group_one.cmp(&collected_search_group_two),
                Ordering::Less
            );
            assert_eq!(
                collected_search_group_one.partial_cmp(&collected_search_group_two),
                Some(Ordering::Less)
            );
        }
    }
}
