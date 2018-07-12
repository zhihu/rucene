use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

use core::search::sort_field::SortFieldType;
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

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SortInfo {
    pub sort_type: SortFieldType,
    pub sort_value: VariantValue,
    pub wilson_infos: Vec<WilsonInfo>,
}

impl SortInfo {
    pub fn new(
        sort_type: SortFieldType,
        sort_value: VariantValue,
        wilson_infos: Vec<WilsonInfo>,
    ) -> SortInfo {
        SortInfo {
            sort_type,
            sort_value,
            wilson_infos,
        }
    }

    pub fn append_wilson_info(&mut self, wilson_info: WilsonInfo) {
        self.wilson_infos.push(wilson_info)
    }

    pub fn max_score_doc(&self) -> Option<DocId> {
        let mut top_doc = None;
        let mut max_score = 0f64;
        for info in &self.wilson_infos {
            match info.wilson_score {
                Some(ref w) => match w {
                    VariantValue::Double(s) => {
                        if *s > max_score {
                            max_score = *s;
                            top_doc = Some(info.doc_id);
                        }
                    }
                    VariantValue::Float(s) => {
                        let score = *s as f64;
                        if score > max_score {
                            max_score = score;
                            top_doc = Some(info.doc_id);
                        }
                    }
                    _ => {}
                },
                _ => {}
            }
        }

        top_doc
    }
}

#[derive(Debug, Clone, Eq)]
pub struct CollectedSearchGroup {
    pub group_value: VariantValue,
    pub sort_info_list: Vec<SortInfo>,
    pub comparator_slot: usize,

    pub top_doc: DocId,
}

impl CollectedSearchGroup {
    pub fn new(
        group_value: VariantValue,
        sort_info_list: Vec<SortInfo>,
        comparator_slot: usize,
        top_doc: DocId,
    ) -> Self {
        CollectedSearchGroup {
            group_value,
            sort_info_list,
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
        self.group_value.cmp(&other.group_value) == Ordering::Equal
            && self.cmp(other) == Ordering::Equal
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
                SortInfo::new(SortFieldType::Score, VariantValue::Int(1), vec![]),
                SortInfo::new(SortFieldType::Doc, VariantValue::Int(2), vec![]),
            ],
            3,
            0,
        );

        assert_eq!(collected_search_group.group_value, VariantValue::Int(1));
        assert_eq!(
            collected_search_group.sort_info_list,
            vec![
                SortInfo::new(SortFieldType::Score, VariantValue::Int(1), vec![]),
                SortInfo::new(SortFieldType::Doc, VariantValue::Int(2), vec![]),
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
                SortInfo::new(SortFieldType::Score, VariantValue::Int(1), vec![]),
                SortInfo::new(SortFieldType::Doc, VariantValue::Int(2), vec![]),
            ],
            10,
            0,
        );

        // Equal
        {
            let collected_search_group_two = CollectedSearchGroup::new(
                VariantValue::Int(1),
                vec![
                    SortInfo::new(SortFieldType::Score, VariantValue::Int(1), vec![]),
                    SortInfo::new(SortFieldType::Doc, VariantValue::Int(2), vec![]),
                ],
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
                    SortInfo::new(SortFieldType::Score, VariantValue::Int(1), vec![]),
                    SortInfo::new(SortFieldType::Doc, VariantValue::Int(2), vec![]),
                ],
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
                    SortInfo::new(SortFieldType::Score, VariantValue::Int(1), vec![]),
                    SortInfo::new(SortFieldType::Doc, VariantValue::Int(3), vec![]),
                ],
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
                    SortInfo::new(SortFieldType::Score, VariantValue::Int(2), vec![]),
                    SortInfo::new(SortFieldType::Doc, VariantValue::Int(2), vec![]),
                ],
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
                    SortInfo::new(SortFieldType::Score, VariantValue::Int(1), vec![]),
                    SortInfo::new(SortFieldType::Doc, VariantValue::Int(2), vec![]),
                ],
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
