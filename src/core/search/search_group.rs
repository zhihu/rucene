use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

use core::search::sort_field::SortFieldType;
use core::util::DocId;
use core::util::VariantValue;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SortInfo {
    pub sort_type: SortFieldType,
    pub sort_value: VariantValue,
}

impl SortInfo {
    pub fn new(sort_type: SortFieldType, sort_value: VariantValue) -> SortInfo {
        SortInfo {
            sort_type,
            sort_value,
        }
    }
}

#[derive(Debug, Clone, Eq)]
pub struct CollectedSearchGroup {
    pub group_value: Option<VariantValue>,
    pub sort_info_list: Vec<SortInfo>,

    pub top_doc: DocId,
    pub comparator_slot: usize,
}

impl Default for CollectedSearchGroup {
    fn default() -> Self {
        CollectedSearchGroup {
            group_value: None,
            sort_info_list: Vec::new(),
            top_doc: 0,
            comparator_slot: 0usize,
        }
    }
}

impl Ord for CollectedSearchGroup {
    fn cmp(&self, other: &CollectedSearchGroup) -> Ordering {
        if let Some(group_value) = self.group_value.as_ref() {
            if let Some(other_value) = other.group_value.as_ref() {
                let cmp = group_value.cmp(other_value);
                if cmp != Ordering::Equal {
                    return cmp;
                }
            } else {
                return Ordering::Greater;
            }
        } else if other.group_value.is_some() {
            return Ordering::Less;
        }
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
        if let Some(group_value) = self.group_value.as_ref() {
            group_value.hash(state)
        }
    }
}

impl PartialEq for CollectedSearchGroup {
    fn eq(&self, other: &CollectedSearchGroup) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collected_search_group() {
        let mut collected_search_group = CollectedSearchGroup::default();

        collected_search_group.group_value = Some(VariantValue::Int(1));
        collected_search_group.sort_info_list = vec![
            SortInfo::new(SortFieldType::Score, VariantValue::Int(1)),
            SortInfo::new(SortFieldType::Doc, VariantValue::Int(2)),
        ];
        collected_search_group.top_doc = 3;
        collected_search_group.comparator_slot = 0;

        assert_eq!(
            collected_search_group.group_value,
            Some(VariantValue::Int(1))
        );
        assert_eq!(
            collected_search_group.sort_info_list,
            vec![
                SortInfo::new(SortFieldType::Score, VariantValue::Int(1)),
                SortInfo::new(SortFieldType::Doc, VariantValue::Int(2)),
            ]
        );
        assert_eq!(collected_search_group.top_doc, 3);
        assert_eq!(collected_search_group.comparator_slot, 0);
    }

    #[test]
    fn test_compare_collected_search_group() {
        let mut collected_search_group_one = CollectedSearchGroup::default();
        collected_search_group_one.group_value = Some(VariantValue::Int(1));
        collected_search_group_one.sort_info_list = vec![
            SortInfo::new(SortFieldType::Score, VariantValue::Int(1)),
            SortInfo::new(SortFieldType::Doc, VariantValue::Int(2)),
        ];
        collected_search_group_one.top_doc = 10;

        // Equal
        {
            let mut collected_search_group_two = CollectedSearchGroup::default();
            collected_search_group_two.group_value = Some(VariantValue::Int(1));
            collected_search_group_two.top_doc = 10;
            collected_search_group_two.sort_info_list = vec![
                SortInfo::new(SortFieldType::Score, VariantValue::Int(1)),
                SortInfo::new(SortFieldType::Doc, VariantValue::Int(2)),
            ];

            assert_eq!(collected_search_group_one, collected_search_group_two);
        }

        // Unequal because second group_value is None
        {
            let mut collected_search_group_two = CollectedSearchGroup::default();
            collected_search_group_two.group_value = None;
            collected_search_group_two.top_doc = 10;
            collected_search_group_two.sort_info_list = vec![
                SortInfo::new(SortFieldType::Score, VariantValue::Int(1)),
                SortInfo::new(SortFieldType::Doc, VariantValue::Int(2)),
            ];

            assert_ne!(collected_search_group_one, collected_search_group_two);
        }

        // Unequal because group_values is different
        {
            let mut collected_search_group_two = CollectedSearchGroup::default();
            collected_search_group_two.group_value = Some(VariantValue::Int(2));
            collected_search_group_two.top_doc = 10;
            collected_search_group_two.sort_info_list = vec![
                SortInfo::new(SortFieldType::Score, VariantValue::Int(1)),
                SortInfo::new(SortFieldType::Doc, VariantValue::Int(2)),
            ];

            assert_ne!(collected_search_group_one, collected_search_group_two);
        }

        // Less because second sort_info_list of group_two is greater
        {
            let mut collected_search_group_two = CollectedSearchGroup::default();
            collected_search_group_two.group_value = Some(VariantValue::Int(2));
            collected_search_group_two.top_doc = 10;
            collected_search_group_two.sort_info_list = vec![
                SortInfo::new(SortFieldType::Score, VariantValue::Int(1)),
                SortInfo::new(SortFieldType::Doc, VariantValue::Int(3)),
            ];

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
            let mut collected_search_group_two = CollectedSearchGroup::default();
            collected_search_group_two.group_value = Some(VariantValue::Int(1));
            collected_search_group_two.top_doc = 10;
            collected_search_group_two.sort_info_list = vec![
                SortInfo::new(SortFieldType::Score, VariantValue::Int(2)),
                SortInfo::new(SortFieldType::Doc, VariantValue::Int(3)),
            ];

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
            let mut collected_search_group_two = CollectedSearchGroup::default();
            collected_search_group_two.group_value = Some(VariantValue::Int(1));
            collected_search_group_two.top_doc = 10;
            collected_search_group_two.sort_info_list = vec![
                SortInfo::new(SortFieldType::Score, VariantValue::Int(1)),
                SortInfo::new(SortFieldType::Doc, VariantValue::Int(2)),
            ];
            collected_search_group_two.top_doc = 11;

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
