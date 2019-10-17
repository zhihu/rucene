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

use core::codec::doc_values::lucene54::DocValuesTermIterator;
use core::codec::doc_values::*;
use core::codec::field_infos::FieldInfo;
use core::codec::Codec;
use core::codec::EmptyPostingIterator;
use core::codec::*;
use core::doc::DocValuesType;
use core::index::merge::{
    doc_id_merger_of, DocIdMerger, DocIdMergerEnum, DocIdMergerSub, DocIdMergerSubBase,
    LiveDocsDocMap, MergeState,
};
use core::index::reader::ReaderSlice;
use core::search::NO_MORE_DOCS;
use core::store::directory::Directory;
use core::util::bkd::LongBitSet;
use core::util::packed::{
    get_mutable_by_ratio, Mutable, MutableEnum, PackedLongValues, PackedLongValuesBuilder,
    PackedLongValuesBuilderType, Reader, COMPACT, DEFAULT_PAGE_SIZE,
};
use core::util::{
    BitsMut, BitsRequired, BytesRef, DocId, LongValues, MatchNoBits, Numeric, ReusableIterator,
};

use error::Result;

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::ptr;
use std::rc::Rc;
use std::sync::Arc;

/// Abstract API that consumes numeric, binary and sorted docvalues.
///
/// Concrete implementations of this actually do "something" with the
/// doc values (write it into the index in a specific format).
pub trait DocValuesConsumer {
    fn add_numeric_field(
        &mut self,
        field_info: &FieldInfo,
        values: &mut impl ReusableIterator<Item = Result<Numeric>>,
    ) -> Result<()>;

    fn add_binary_field(
        &mut self,
        field_info: &FieldInfo,
        values: &mut impl ReusableIterator<Item = Result<BytesRef>>,
    ) -> Result<()>;

    fn add_sorted_field(
        &mut self,
        field_info: &FieldInfo,
        values: &mut impl ReusableIterator<Item = Result<BytesRef>>,
        doc_to_ord: &mut impl ReusableIterator<Item = Result<Numeric>>,
    ) -> Result<()>;

    fn add_sorted_numeric_field(
        &mut self,
        field_info: &FieldInfo,
        values: &mut impl ReusableIterator<Item = Result<Numeric>>,
        doc_to_value_count: &mut impl ReusableIterator<Item = Result<u32>>,
    ) -> Result<()>;

    fn add_sorted_set_field(
        &mut self,
        field_info: &FieldInfo,
        values: &mut impl ReusableIterator<Item = Result<BytesRef>>,
        doc_to_ord_count: &mut impl ReusableIterator<Item = Result<u32>>,
        ords: &mut impl ReusableIterator<Item = Result<Numeric>>,
    ) -> Result<()>;

    fn merge<D: Directory, C: Codec>(&mut self, merge_state: &mut MergeState<D, C>) -> Result<()> {
        for producer in &merge_state.doc_values_producers {
            if let Some(producer) = producer.as_ref() {
                producer.check_integrity()?;
            }
        }

        for merge_field_info in merge_state
            .merge_field_infos
            .as_ref()
            .unwrap()
            .by_number
            .values()
        {
            let dv_type = merge_field_info.doc_values_type;
            match dv_type {
                DocValuesType::Null => {}
                DocValuesType::Numeric => {
                    let mut to_merge = vec![];
                    let mut docs_with_field = vec![];
                    for i in 0..merge_state.doc_values_producers.len() {
                        let mut values: Box<dyn NumericDocValues> =
                            Box::new(EmptyNumericDocValues {});
                        let mut bits: Box<dyn BitsMut> =
                            Box::new(MatchNoBits::new(merge_state.max_docs[i] as usize));
                        if let Some(ref producer) = merge_state.doc_values_producers[i] {
                            if let Some(field_info) = merge_state.fields_infos[i]
                                .field_info_by_name(&merge_field_info.name)
                            {
                                if field_info.doc_values_type == DocValuesType::Numeric {
                                    values = producer.get_numeric(field_info)?.get()?;
                                    bits = producer.get_docs_with_field(field_info)?;
                                }
                            }
                        }
                        to_merge.push(values);
                        docs_with_field.push(bits);
                    }
                    self.merge_numeric_field(
                        merge_field_info.as_ref(),
                        merge_state,
                        to_merge,
                        docs_with_field,
                    )?;
                }
                DocValuesType::Binary => {
                    let mut to_merge = vec![];
                    let mut docs_with_field = vec![];
                    for i in 0..merge_state.doc_values_producers.len() {
                        let mut values: Box<dyn BinaryDocValues> =
                            Box::new(EmptyBinaryDocValues {});
                        let mut bits: Box<dyn BitsMut> =
                            Box::new(MatchNoBits::new(merge_state.max_docs[i] as usize));
                        if let Some(ref producer) = merge_state.doc_values_producers[i] {
                            if let Some(field_info) = merge_state.fields_infos[i]
                                .field_info_by_name(&merge_field_info.name)
                            {
                                if field_info.doc_values_type == DocValuesType::Binary {
                                    values = producer.get_binary(field_info)?.get()?;
                                    bits = producer.get_docs_with_field(field_info)?;
                                }
                            }
                        }
                        to_merge.push(values);
                        docs_with_field.push(bits);
                    }

                    self.merge_binary_field(
                        merge_field_info.as_ref(),
                        merge_state,
                        to_merge,
                        docs_with_field,
                    )?;
                }
                DocValuesType::Sorted => {
                    let mut to_merge = vec![];
                    for i in 0..merge_state.doc_values_producers.len() {
                        let mut values: Box<dyn SortedDocValues> =
                            Box::new(EmptySortedDocValues {});
                        if let Some(ref producer) = merge_state.doc_values_producers[i] {
                            if let Some(field_info) = merge_state.fields_infos[i]
                                .field_info_by_name(&merge_field_info.name)
                            {
                                if field_info.doc_values_type == DocValuesType::Sorted {
                                    values = producer.get_sorted(field_info)?.get()?;
                                }
                            }
                        }
                        to_merge.push(values);
                    }
                    self.merge_sorted_field(merge_field_info.as_ref(), merge_state, to_merge)?;
                }
                DocValuesType::SortedSet => {
                    let mut to_merge = vec![];
                    for i in 0..merge_state.doc_values_producers.len() {
                        let mut values: Box<dyn SortedSetDocValues> =
                            Box::new(EmptySortedSetDocValues);
                        if let Some(ref producer) = merge_state.doc_values_producers[i] {
                            if let Some(field_info) = merge_state.fields_infos[i]
                                .field_info_by_name(&merge_field_info.name)
                            {
                                if field_info.doc_values_type == DocValuesType::SortedSet {
                                    values = producer.get_sorted_set(field_info)?.get()?;
                                }
                            }
                        }
                        to_merge.push(values);
                    }
                    self.merge_sorted_set_field(merge_field_info.as_ref(), merge_state, to_merge)?;
                }
                DocValuesType::SortedNumeric => {
                    let mut to_merge = vec![];
                    for i in 0..merge_state.doc_values_producers.len() {
                        let mut values: Box<dyn SortedNumericDocValues> =
                            Box::new(EmptySortedNumericDocValues {});
                        if let Some(ref producer) = merge_state.doc_values_producers[i] {
                            if let Some(field_info) = merge_state.fields_infos[i]
                                .field_info_by_name(&merge_field_info.name)
                            {
                                if field_info.doc_values_type == DocValuesType::SortedNumeric {
                                    values = producer.get_sorted_numeric(field_info)?.get()?;
                                }
                            }
                        }
                        to_merge.push(values);
                    }
                    self.merge_sorted_numeric_field(
                        merge_field_info.as_ref(),
                        merge_state,
                        to_merge,
                    )?;
                }
            }
        }
        Ok(())
    }

    fn merge_numeric_field<D: Directory, C: Codec>(
        &mut self,
        field_info: &FieldInfo,
        merge_state: &MergeState<D, C>,
        to_merge: Vec<Box<dyn NumericDocValues>>,
        docs_with_field: Vec<Box<dyn BitsMut>>,
    ) -> Result<()> {
        let mut iter = NumericDocValuesMergeIter::new(merge_state, to_merge, docs_with_field)?;
        self.add_numeric_field(field_info, &mut iter)
    }

    fn merge_binary_field<D: Directory, C: Codec>(
        &mut self,
        field_info: &FieldInfo,
        merge_state: &MergeState<D, C>,
        to_merge: Vec<Box<dyn BinaryDocValues>>,
        docs_with_field: Vec<Box<dyn BitsMut>>,
    ) -> Result<()> {
        let mut iter = BinaryDocValuesMergeIter::new(merge_state, to_merge, docs_with_field)?;
        self.add_binary_field(field_info, &mut iter)
    }

    fn merge_sorted_field<D: Directory, C: Codec>(
        &mut self,
        field_info: &FieldInfo,
        merge_state: &MergeState<D, C>,
        to_merge: Vec<Box<dyn SortedDocValues>>,
    ) -> Result<()> {
        let mut to_merge = to_merge;
        let num_readers = to_merge.len();
        // step 1: iterate thru each sub and mark terms still in use
        let mut live_terms = Vec::with_capacity(num_readers);
        let mut weights = Vec::with_capacity(num_readers);
        for i in 0..num_readers {
            let dv = &mut to_merge[i];
            let max_doc = merge_state.max_docs[i];
            let live_docs = &merge_state.live_docs[i];
            if live_docs.len() == 0 {
                live_terms.push(Some(MergeDVTermIterEnum::DV(dv.term_iterator()?)));
                weights.push(dv.value_count());
            } else {
                let mut bitset = LongBitSet::new(dv.value_count() as i64);
                for i in 0..max_doc {
                    if live_docs.get(i as usize)? {
                        let ord = dv.get_ord(i)?;
                        if ord >= 0 {
                            bitset.set(ord as i64);
                        }
                    }
                }
                weights.push(bitset.cardinality());
                live_terms.push(Some(MergeDVTermIterEnum::BitsFilter(
                    BitsFilteredTermIterator::new(dv.term_iterator()?, bitset),
                )));
            }
        }

        // step 2: create ordinal map (this conceptually does the "merging")
        let map = OrdinalMap::build(live_terms, weights, COMPACT)?;

        // step 3: add field
        let mut ords_iter = SortedDocValuesMergeOrdIter::new(&mut to_merge, merge_state, &map)?;
        let mut bytes_iter = SortedDocValuesMergeBytesIter::new(&map, &mut to_merge);
        self.add_sorted_field(field_info, &mut bytes_iter, &mut ords_iter)
    }

    fn merge_sorted_set_field<D: Directory, C: Codec>(
        &mut self,
        field_info: &FieldInfo,
        merge_state: &MergeState<D, C>,
        mut to_merge: Vec<Box<dyn SortedSetDocValues>>,
    ) -> Result<()> {
        let num_readers = to_merge.len();
        // step 1: iterate thru each sub and mark terms still in use
        let mut live_terms = Vec::with_capacity(num_readers);
        let mut weights = Vec::with_capacity(num_readers);
        for (i, dv) in to_merge.iter_mut().enumerate() {
            let max_doc = merge_state.max_docs[i];
            let live_docs = &merge_state.live_docs[i];
            if live_docs.len() == 0 {
                live_terms.push(Some(MergeDVTermIterEnum::DV(dv.term_iterator()?)));
                weights.push(dv.get_value_count());
            } else {
                let mut bitset = LongBitSet::new(dv.get_value_count() as i64);
                for i in 0..max_doc {
                    if live_docs.get(i as usize)? {
                        dv.set_document(i)?;
                        loop {
                            let ord = dv.next_ord()?;
                            if ord == NO_MORE_ORDS {
                                break;
                            }
                            bitset.set(ord);
                        }
                    }
                }
                weights.push(bitset.cardinality());
                live_terms.push(Some(MergeDVTermIterEnum::BitsFilter(
                    BitsFilteredTermIterator::new(dv.term_iterator()?, bitset),
                )));
            }
        }

        // step 2: create ordinal map (this conceptually does the "merging")
        let map = OrdinalMap::build(live_terms, weights, COMPACT)?;

        // step 3: add field
        self.add_sorted_set_field(
            field_info,
            &mut SortedSetDocValuesMergeBytesIter::new(&map, &mut to_merge),
            &mut SortedSetDocValuesOrdCountIter::new(&mut to_merge, merge_state, &map)?,
            &mut SortedSetDocValuesMergeOrdIter::new(&mut to_merge, merge_state, &map)?,
        )
    }

    fn merge_sorted_numeric_field<D: Directory, C: Codec>(
        &mut self,
        field_info: &FieldInfo,
        merge_state: &MergeState<D, C>,
        to_merge: Vec<Box<dyn SortedNumericDocValues>>,
    ) -> Result<()> {
        let mut to_merge = to_merge;
        let mut values_iter = SortedNumericDocValuesIter::new(&mut to_merge, merge_state)?;
        let mut doc_counts_iter = SortedNumericDocValuesCountIter::new(&mut to_merge, merge_state)?;

        self.add_sorted_numeric_field(field_info, &mut values_iter, &mut doc_counts_iter)
    }
}

// Helper: returns true if the given docToValue count contains only at most one value
pub fn is_single_valued(
    doc_to_value_count: &mut impl ReusableIterator<Item = Result<u32>>,
) -> Result<bool> {
    loop {
        let count = match doc_to_value_count.next() {
            None => {
                break;
            }
            Some(r) => r?,
        };
        if count > 1 {
            doc_to_value_count.reset();
            return Ok(false);
        }
    }
    doc_to_value_count.reset();
    Ok(true)
}

pub fn singleton_view<'a, RI1, RI2>(
    doc_to_value_count: &'a mut RI1,
    values: &'a mut RI2,
    missing_value: Numeric,
) -> SingletonViewIter<'a, RI1, RI2>
where
    RI1: ReusableIterator<Item = Result<u32>>,
    RI2: ReusableIterator<Item = Result<Numeric>>,
{
    debug_assert!(is_single_valued(doc_to_value_count).unwrap());
    // doc_to_value_count.reset();
    // debug_assert!(doc_to_value_count.len() == values.len());

    SingletonViewIter::new(doc_to_value_count, values, missing_value)
}

pub struct SingletonViewIter<'a, RI1, RI2> {
    doc_to_value_count: &'a mut RI1,
    values: &'a mut RI2,
    missing_value: Numeric,
}

impl<'a, RI1, RI2> SingletonViewIter<'a, RI1, RI2>
where
    RI1: ReusableIterator<Item = Result<u32>>,
    RI2: ReusableIterator<Item = Result<Numeric>>,
{
    fn new(doc_to_value_count: &'a mut RI1, values: &'a mut RI2, missing_value: Numeric) -> Self {
        SingletonViewIter {
            doc_to_value_count,
            values,
            missing_value,
        }
    }
}

impl<'a, RI1, RI2> ReusableIterator for SingletonViewIter<'a, RI1, RI2>
where
    RI1: ReusableIterator<Item = Result<u32>>,
    RI2: ReusableIterator<Item = Result<Numeric>>,
{
    fn reset(&mut self) {
        self.doc_to_value_count.reset();
        self.values.reset();
    }
}

impl<'a, RI1, RI2> Iterator for SingletonViewIter<'a, RI1, RI2>
where
    RI1: ReusableIterator<Item = Result<u32>>,
    RI2: ReusableIterator<Item = Result<Numeric>>,
{
    type Item = Result<Numeric>;

    fn next(&mut self) -> Option<Result<Numeric>> {
        match self.doc_to_value_count.next() {
            Some(Ok(v)) => {
                if v == 0 {
                    Some(Ok(self.missing_value))
                } else {
                    self.values.next()
                }
            }
            None => None,
            Some(Err(e)) => Some(Err(e)),
        }
    }
}

struct BitsFilteredTermIterator<T: TermIterator> {
    base: FilteredTermIterBase<T>,
    live_terms: LongBitSet,
}

impl<T: TermIterator> BitsFilteredTermIterator<T> {
    fn new(terms: T, live_terms: LongBitSet) -> Self {
        let base = FilteredTermIterBase::new(terms, false);
        BitsFilteredTermIterator { base, live_terms }
    }
}

impl<T: TermIterator> FilteredTermIterator for BitsFilteredTermIterator<T> {
    type Iter = T;
    fn base(&self) -> &FilteredTermIterBase<T> {
        &self.base
    }

    fn base_mut(&mut self) -> &mut FilteredTermIterBase<T> {
        &mut self.base
    }

    fn accept(&self, _term: &[u8]) -> Result<AcceptStatus> {
        let ord = self.ord()?;
        let status = if self.live_terms.get(ord) {
            AcceptStatus::Yes
        } else {
            AcceptStatus::No
        };
        Ok(status)
    }
}

struct NumericDocValuesMergeIter {
    doc_id_merger: DocIdMergerEnum<NumericDocValuesSub>,
    next_value: Numeric,
    next_is_set: bool,
}

impl NumericDocValuesMergeIter {
    fn new<D: Directory + 'static, C: Codec>(
        merge_state: &MergeState<D, C>,
        to_merge: Vec<Box<dyn NumericDocValues>>,
        docs_with_field: Vec<Box<dyn BitsMut>>,
    ) -> Result<Self> {
        let mut subs = Vec::with_capacity(to_merge.len());
        for (i, (dv, bits)) in to_merge.into_iter().zip(docs_with_field).enumerate() {
            subs.push(NumericDocValuesSub::new(
                Arc::clone(&merge_state.doc_maps[i]),
                dv,
                bits,
                merge_state.max_docs[i],
            ));
        }
        let doc_id_merger = doc_id_merger_of(subs, merge_state.needs_index_sort)?;
        Ok(NumericDocValuesMergeIter {
            doc_id_merger,
            next_value: Numeric::Null,
            next_is_set: false,
        })
    }
    fn has_next(&mut self) -> Result<bool> {
        Ok(self.next_is_set || self.set_next()?)
    }

    fn set_next(&mut self) -> Result<bool> {
        if let Some(sub) = self.doc_id_merger.next()? {
            self.next_is_set = true;
            let next_value = sub.values.get_mut(sub.doc_id)?;
            self.next_value = if next_value != 0 || sub.docs_with_field.get(sub.doc_id as usize)? {
                Numeric::Long(next_value)
            } else {
                Numeric::Null
            };
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

impl Iterator for NumericDocValuesMergeIter {
    type Item = Result<Numeric>;

    fn next(&mut self) -> Option<Result<Numeric>> {
        match self.has_next() {
            Err(e) => Some(Err(e)),
            Ok(true) => {
                debug_assert!(self.next_is_set);
                self.next_is_set = false;
                Some(Ok(self.next_value))
            }
            Ok(false) => None,
        }
    }
}

impl ReusableIterator for NumericDocValuesMergeIter {
    fn reset(&mut self) {
        self.doc_id_merger.reset().unwrap();
        self.next_is_set = false;
        self.next_value = Numeric::Null;
    }
}

struct NumericDocValuesSub {
    values: Box<dyn NumericDocValues>,
    docs_with_field: Box<dyn BitsMut>,
    doc_id: i32,
    max_doc: i32,
    base: DocIdMergerSubBase,
}

impl NumericDocValuesSub {
    fn new(
        doc_map: Arc<LiveDocsDocMap>,
        values: Box<dyn NumericDocValues>,
        docs_with_field: Box<dyn BitsMut>,
        max_doc: i32,
    ) -> Self {
        let base = DocIdMergerSubBase::new(doc_map);
        NumericDocValuesSub {
            values,
            docs_with_field,
            doc_id: -1,
            max_doc,
            base,
        }
    }
}

impl DocIdMergerSub for NumericDocValuesSub {
    fn next_doc(&mut self) -> Result<i32> {
        self.doc_id += 1;
        if self.doc_id == self.max_doc {
            Ok(NO_MORE_DOCS)
        } else {
            Ok(self.doc_id)
        }
    }

    fn base(&self) -> &DocIdMergerSubBase {
        &self.base
    }

    fn base_mut(&mut self) -> &mut DocIdMergerSubBase {
        &mut self.base
    }

    fn reset(&mut self) {
        self.base.reset();
        self.doc_id = -1;
    }
}

struct BinaryDocValuesMergeIter {
    doc_id_merger: DocIdMergerEnum<BinaryDocValuesSub>,
    next_value: Vec<u8>,
    next_ref: BytesRef,
    next_is_set: bool,
}

impl BinaryDocValuesMergeIter {
    fn new<D: Directory + 'static, C: Codec>(
        merge_state: &MergeState<D, C>,
        to_merge: Vec<Box<dyn BinaryDocValues>>,
        docs_with_field: Vec<Box<dyn BitsMut>>,
    ) -> Result<Self> {
        let mut subs = Vec::with_capacity(to_merge.len());
        for (i, (dv, bits)) in to_merge.into_iter().zip(docs_with_field).enumerate() {
            subs.push(BinaryDocValuesSub::new(
                Arc::clone(&merge_state.doc_maps[i]),
                dv,
                bits,
                merge_state.max_docs[i],
            ));
        }
        let doc_id_merger = doc_id_merger_of(subs, merge_state.needs_index_sort)?;
        Ok(BinaryDocValuesMergeIter {
            doc_id_merger,
            next_value: vec![],
            next_ref: BytesRef::default(),
            next_is_set: false,
        })
    }

    fn has_next(&mut self) -> Result<bool> {
        Ok(self.next_is_set || self.set_next()?)
    }

    fn set_next(&mut self) -> Result<bool> {
        if let Some(sub) = self.doc_id_merger.next()? {
            self.next_is_set = true;
            if sub.docs_with_field.get(sub.doc_id as usize)? {
                self.next_value = sub.values.get(sub.doc_id)?;
                self.next_ref = BytesRef::new(&self.next_value);
            } else {
                self.next_value.clear();
                self.next_ref = BytesRef::default();
            };
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

impl Iterator for BinaryDocValuesMergeIter {
    type Item = Result<BytesRef>;

    fn next(&mut self) -> Option<Result<BytesRef>> {
        match self.has_next() {
            Err(e) => Some(Err(e)),
            Ok(true) => {
                debug_assert!(self.next_is_set);
                self.next_is_set = false;
                Some(Ok(self.next_ref))
            }
            Ok(false) => None,
        }
    }
}

impl ReusableIterator for BinaryDocValuesMergeIter {
    fn reset(&mut self) {
        self.doc_id_merger.reset().unwrap();
        self.next_is_set = false;
        self.next_value.clear();
        self.next_ref = BytesRef::default();
    }
}

struct BinaryDocValuesSub {
    values: Box<dyn BinaryDocValues>,
    docs_with_field: Box<dyn BitsMut>,
    doc_id: i32,
    max_doc: i32,
    base: DocIdMergerSubBase,
}

impl BinaryDocValuesSub {
    fn new(
        doc_map: Arc<LiveDocsDocMap>,
        values: Box<dyn BinaryDocValues>,
        docs_with_field: Box<dyn BitsMut>,
        max_doc: i32,
    ) -> Self {
        let base = DocIdMergerSubBase::new(doc_map);
        BinaryDocValuesSub {
            values,
            docs_with_field,
            doc_id: -1,
            max_doc,
            base,
        }
    }
}

impl DocIdMergerSub for BinaryDocValuesSub {
    fn next_doc(&mut self) -> Result<i32> {
        self.doc_id += 1;
        if self.doc_id == self.max_doc {
            Ok(NO_MORE_DOCS)
        } else {
            Ok(self.doc_id)
        }
    }

    fn base(&self) -> &DocIdMergerSubBase {
        &self.base
    }

    fn base_mut(&mut self) -> &mut DocIdMergerSubBase {
        &mut self.base
    }

    fn reset(&mut self) {
        self.base.reset();
        self.doc_id = -1;
    }
}

struct SortedDocValuesMergeBytesIter<'a> {
    map: &'a OrdinalMap,
    dvs: &'a mut [Box<dyn SortedDocValues>],
    current_ord: i64,
    current: Vec<u8>,
}

impl<'a> SortedDocValuesMergeBytesIter<'a> {
    fn new(map: &'a OrdinalMap, dvs: &'a mut [Box<dyn SortedDocValues>]) -> Self {
        SortedDocValuesMergeBytesIter {
            map,
            dvs,
            current_ord: 0,
            current: Vec::with_capacity(0),
        }
    }
}

impl<'a> Iterator for SortedDocValuesMergeBytesIter<'a> {
    type Item = Result<BytesRef>;

    fn next(&mut self) -> Option<Result<BytesRef>> {
        if self.current_ord >= self.map.value_count() {
            return None;
        }
        let segment_number = self.map.first_segment_number(self.current_ord);
        let segment_ord = self.map.first_segment_ord(self.current_ord);
        match self.dvs[segment_number as usize].lookup_ord(segment_ord as i32) {
            Err(e) => Some(Err(e)),
            Ok(term) => {
                self.current = term;
                self.current_ord += 1;
                Some(Ok(BytesRef::new(&self.current)))
            }
        }
    }
}

impl<'a> ReusableIterator for SortedDocValuesMergeBytesIter<'a> {
    fn reset(&mut self) {
        self.current_ord = 0;
        self.current.clear();
    }
}

struct SortedDocValuesMergeOrdIter {
    doc_id_merger: DocIdMergerEnum<SortedDocValuesSub>,
    next_value: Numeric,
    next_is_set: bool,
}

impl SortedDocValuesMergeOrdIter {
    fn new<D: Directory + 'static, C: Codec>(
        to_merge: &mut [Box<dyn SortedDocValues>],
        merge_state: &MergeState<D, C>,
        map: &OrdinalMap,
    ) -> Result<Self> {
        let mut subs = Vec::with_capacity(to_merge.len());
        for (i, dv) in to_merge.iter_mut().enumerate() {
            subs.push(SortedDocValuesSub::new(
                Arc::clone(&merge_state.doc_maps[i]),
                &mut **dv,
                merge_state.max_docs[i],
                map.get_global_ords(i),
            ));
        }
        let doc_id_merger = doc_id_merger_of(subs, merge_state.needs_index_sort)?;
        Ok(SortedDocValuesMergeOrdIter {
            doc_id_merger,
            next_value: Numeric::Null,
            next_is_set: false,
        })
    }

    fn set_next(&mut self) -> Result<bool> {
        if let Some(sub) = self.doc_id_merger.next()? {
            self.next_is_set = true;
            let doc_id = sub.doc_id;
            let seg_ord = sub.values().get_ord(doc_id)?;
            self.next_value = if seg_ord == -1 {
                Numeric::Int(-1)
            } else {
                Numeric::Int(sub.map.get(seg_ord)? as i32)
            };
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn has_next(&mut self) -> Result<bool> {
        Ok(self.next_is_set || self.set_next()?)
    }
}

impl Iterator for SortedDocValuesMergeOrdIter {
    type Item = Result<Numeric>;

    fn next(&mut self) -> Option<Result<Numeric>> {
        match self.has_next() {
            Err(e) => Some(Err(e)),
            Ok(true) => {
                self.next_is_set = false;
                Some(Ok(self.next_value))
            }
            Ok(false) => None,
        }
    }
}

impl ReusableIterator for SortedDocValuesMergeOrdIter {
    fn reset(&mut self) {
        self.doc_id_merger.reset().unwrap();
        self.next_value = Numeric::Null;
        self.next_is_set = false;
    }
}

struct SortedDocValuesSub {
    values: *mut dyn SortedDocValues,
    doc_id: DocId,
    max_doc: i32,
    map: Rc<dyn LongValues>,
    base: DocIdMergerSubBase,
}

impl SortedDocValuesSub {
    fn new(
        doc_map: Arc<LiveDocsDocMap>,
        values: &mut (dyn SortedDocValues + 'static),
        max_doc: i32,
        map: Rc<dyn LongValues>,
    ) -> Self {
        let base = DocIdMergerSubBase::new(doc_map);
        SortedDocValuesSub {
            values,
            map,
            base,
            max_doc,
            doc_id: -1,
        }
    }

    fn values(&mut self) -> &mut dyn SortedDocValues {
        unsafe { &mut *self.values }
    }
}

impl DocIdMergerSub for SortedDocValuesSub {
    fn next_doc(&mut self) -> Result<i32> {
        self.doc_id += 1;
        if self.doc_id == self.max_doc {
            Ok(NO_MORE_DOCS)
        } else {
            Ok(self.doc_id)
        }
    }

    fn base(&self) -> &DocIdMergerSubBase {
        &self.base
    }

    fn base_mut(&mut self) -> &mut DocIdMergerSubBase {
        &mut self.base
    }

    fn reset(&mut self) {
        self.base.reset();
        self.doc_id = -1;
    }
}

struct SortedSetDocValuesMergeBytesIter<'a> {
    map: &'a OrdinalMap,
    dvs: *mut [Box<dyn SortedSetDocValues>],
    current_ord: i64,
    current: Vec<u8>,
}

impl<'a> SortedSetDocValuesMergeBytesIter<'a> {
    fn new(map: &'a OrdinalMap, dvs: &mut [Box<dyn SortedSetDocValues>]) -> Self {
        SortedSetDocValuesMergeBytesIter {
            map,
            dvs,
            current_ord: 0,
            current: Vec::with_capacity(0),
        }
    }

    fn doc_values(&mut self) -> &mut [Box<dyn SortedSetDocValues>] {
        unsafe { &mut *self.dvs }
    }
}

impl<'a> Iterator for SortedSetDocValuesMergeBytesIter<'a> {
    type Item = Result<BytesRef>;

    fn next(&mut self) -> Option<Result<BytesRef>> {
        if self.current_ord >= self.map.value_count() {
            return None;
        }
        let segment_number = self.map.first_segment_number(self.current_ord);
        let segment_ord = self.map.first_segment_ord(self.current_ord);
        match self.doc_values()[segment_number as usize].lookup_ord(segment_ord) {
            Err(e) => Some(Err(e)),
            Ok(term) => {
                self.current = term;
                self.current_ord += 1;
                Some(Ok(BytesRef::new(&self.current)))
            }
        }
    }
}

impl<'a> ReusableIterator for SortedSetDocValuesMergeBytesIter<'a> {
    fn reset(&mut self) {
        self.current_ord = 0;
        self.current.clear();
    }
}

struct SortedSetDocValuesOrdCountIter {
    doc_id_merger: DocIdMergerEnum<SortedSetDocValuesSub>,
    next_value: u32,
    next_is_set: bool,
}

impl SortedSetDocValuesOrdCountIter {
    fn new<D: Directory + 'static, C: Codec>(
        to_merge: &mut [Box<dyn SortedSetDocValues>],
        merge_state: &MergeState<D, C>,
        map: &OrdinalMap,
    ) -> Result<Self> {
        let mut subs = Vec::with_capacity(to_merge.len());
        for (i, dv) in to_merge.iter_mut().enumerate() {
            subs.push(SortedSetDocValuesSub::new(
                Arc::clone(&merge_state.doc_maps[i]),
                &mut **dv,
                merge_state.max_docs[i],
                map.get_global_ords(i),
            ));
        }
        let doc_id_merger = doc_id_merger_of(subs, merge_state.needs_index_sort)?;
        Ok(SortedSetDocValuesOrdCountIter {
            doc_id_merger,
            next_value: 0,
            next_is_set: false,
        })
    }

    fn set_next(&mut self) -> Result<bool> {
        if let Some(sub) = self.doc_id_merger.next()? {
            self.next_is_set = true;
            let doc_id = sub.doc_id;
            sub.values().set_document(doc_id)?;
            self.next_value = 0;
            while sub.values().next_ord()? != NO_MORE_ORDS {
                self.next_value += 1;
            }
            self.next_is_set = true;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn has_next(&mut self) -> Result<bool> {
        Ok(self.next_is_set || self.set_next()?)
    }
}

impl Iterator for SortedSetDocValuesOrdCountIter {
    type Item = Result<u32>;

    fn next(&mut self) -> Option<Result<u32>> {
        match self.has_next() {
            Err(e) => Some(Err(e)),
            Ok(true) => {
                self.next_is_set = false;
                Some(Ok(self.next_value))
            }
            Ok(false) => None,
        }
    }
}

impl ReusableIterator for SortedSetDocValuesOrdCountIter {
    fn reset(&mut self) {
        self.doc_id_merger.reset().unwrap();
        self.next_value = 0;
        self.next_is_set = false;
    }
}

struct SortedSetDocValuesMergeOrdIter {
    doc_id_merger: DocIdMergerEnum<SortedSetDocValuesSub>,
    next_value: Numeric,
    next_is_set: bool,
    ords: Vec<i64>,
    ord_upto: usize,
    ord_length: usize,
}

impl SortedSetDocValuesMergeOrdIter {
    fn new<D: Directory + 'static, C: Codec>(
        to_merge: &mut [Box<dyn SortedSetDocValues>],
        merge_state: &MergeState<D, C>,
        map: &OrdinalMap,
    ) -> Result<Self> {
        let mut subs = Vec::with_capacity(to_merge.len());
        for (i, dv) in to_merge.iter_mut().enumerate() {
            subs.push(SortedSetDocValuesSub::new(
                Arc::clone(&merge_state.doc_maps[i]),
                &mut **dv,
                merge_state.max_docs[i],
                map.get_global_ords(i),
            ));
        }
        let doc_id_merger = doc_id_merger_of(subs, merge_state.needs_index_sort)?;
        Ok(SortedSetDocValuesMergeOrdIter {
            doc_id_merger,
            next_value: Numeric::Null,
            next_is_set: false,
            ords: Vec::with_capacity(8),
            ord_upto: 0,
            ord_length: 0,
        })
    }

    fn set_next(&mut self) -> Result<bool> {
        loop {
            if self.ord_upto < self.ord_length {
                self.next_value = Numeric::Long(self.ords[self.ord_upto]);
                self.ord_upto += 1;
                self.next_is_set = true;
                return Ok(true);
            }

            if let Some(sub) = self.doc_id_merger.next()? {
                let doc_id = sub.doc_id;
                sub.values().set_document(doc_id)?;
                self.ord_upto = 0;
                self.ords.clear();
                loop {
                    let ord = sub.values().next_ord()?;
                    if ord == NO_MORE_ORDS {
                        break;
                    }
                    self.ords.push(sub.map.get64(ord)?);
                }
                self.ord_length = self.ords.len();
            } else {
                return Ok(false);
            }
        }
    }

    fn has_next(&mut self) -> Result<bool> {
        Ok(self.next_is_set || self.set_next()?)
    }
}

impl Iterator for SortedSetDocValuesMergeOrdIter {
    type Item = Result<Numeric>;

    fn next(&mut self) -> Option<Result<Numeric>> {
        match self.has_next() {
            Err(e) => Some(Err(e)),
            Ok(true) => {
                self.next_is_set = false;
                Some(Ok(self.next_value))
            }
            Ok(false) => None,
        }
    }
}

impl ReusableIterator for SortedSetDocValuesMergeOrdIter {
    fn reset(&mut self) {
        self.doc_id_merger.reset().unwrap();
        self.next_value = Numeric::Null;
        self.next_is_set = false;
        self.ords.clear();
        self.ord_length = 0;
        self.ord_upto = 0;
    }
}

struct SortedSetDocValuesSub {
    values: *mut dyn SortedSetDocValues,
    doc_id: DocId,
    max_doc: i32,
    map: Rc<dyn LongValues>,
    base: DocIdMergerSubBase,
}

impl SortedSetDocValuesSub {
    fn new(
        doc_map: Arc<LiveDocsDocMap>,
        values: &mut (dyn SortedSetDocValues + 'static),
        max_doc: i32,
        map: Rc<dyn LongValues>,
    ) -> Self {
        let base = DocIdMergerSubBase::new(doc_map);
        SortedSetDocValuesSub {
            values,
            map,
            base,
            max_doc,
            doc_id: -1,
        }
    }

    fn values(&mut self) -> &mut dyn SortedSetDocValues {
        unsafe { &mut *self.values }
    }
}

impl DocIdMergerSub for SortedSetDocValuesSub {
    fn next_doc(&mut self) -> Result<i32> {
        self.doc_id += 1;
        if self.doc_id == self.max_doc {
            Ok(NO_MORE_DOCS)
        } else {
            Ok(self.doc_id)
        }
    }

    fn base(&self) -> &DocIdMergerSubBase {
        &self.base
    }

    fn base_mut(&mut self) -> &mut DocIdMergerSubBase {
        &mut self.base
    }

    fn reset(&mut self) {
        self.base.reset();
        self.doc_id = -1;
    }
}

struct SortedNumericDocValuesCountIter {
    doc_id_merger: DocIdMergerEnum<SortedNumericDocValuesSub>,
    next_value: u32,
    next_is_set: bool,
}

impl SortedNumericDocValuesCountIter {
    fn new<D: Directory + 'static, C: Codec>(
        to_merge: &mut [Box<dyn SortedNumericDocValues>],
        merge_state: &MergeState<D, C>,
    ) -> Result<Self> {
        let mut subs = Vec::with_capacity(to_merge.len());
        for (i, dv) in to_merge.iter_mut().enumerate() {
            subs.push(SortedNumericDocValuesSub::new(
                Arc::clone(&merge_state.doc_maps[i]),
                &mut **dv,
                merge_state.max_docs[i],
            ));
        }
        let doc_id_merger = doc_id_merger_of(subs, merge_state.needs_index_sort)?;
        Ok(SortedNumericDocValuesCountIter {
            doc_id_merger,
            next_value: 0,
            next_is_set: false,
        })
    }

    fn set_next(&mut self) -> Result<bool> {
        if let Some(sub) = self.doc_id_merger.next()? {
            let doc_id = sub.doc_id;
            sub.values().set_document(doc_id)?;
            self.next_value = sub.values().count() as u32;
            self.next_is_set = true;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn has_next(&mut self) -> Result<bool> {
        Ok(self.next_is_set || self.set_next()?)
    }
}

impl Iterator for SortedNumericDocValuesCountIter {
    type Item = Result<u32>;

    fn next(&mut self) -> Option<Result<u32>> {
        match self.has_next() {
            Err(e) => Some(Err(e)),
            Ok(true) => {
                self.next_is_set = false;
                Some(Ok(self.next_value))
            }
            Ok(false) => None,
        }
    }
}

impl ReusableIterator for SortedNumericDocValuesCountIter {
    fn reset(&mut self) {
        self.doc_id_merger.reset().unwrap();
        self.next_value = 0;
        self.next_is_set = false;
    }
}

struct SortedNumericDocValuesIter {
    doc_id_merger: DocIdMergerEnum<SortedNumericDocValuesSub>,
    next_value: Numeric,
    next_is_set: bool,
    value_upto: usize,
    value_length: usize,
    current: *mut SortedNumericDocValuesSub,
}

impl SortedNumericDocValuesIter {
    fn new<D: Directory + 'static, C: Codec>(
        to_merge: &mut [Box<dyn SortedNumericDocValues>],
        merge_state: &MergeState<D, C>,
    ) -> Result<Self> {
        let mut subs = Vec::with_capacity(to_merge.len());
        for (i, dv) in to_merge.iter_mut().enumerate() {
            subs.push(SortedNumericDocValuesSub::new(
                Arc::clone(&merge_state.doc_maps[i]),
                &mut **dv,
                merge_state.max_docs[i],
            ));
        }
        let doc_id_merger = doc_id_merger_of(subs, merge_state.needs_index_sort)?;
        Ok(SortedNumericDocValuesIter {
            doc_id_merger,
            next_value: Numeric::Null,
            next_is_set: false,
            value_upto: 0,
            value_length: 0,
            current: ptr::null_mut(),
        })
    }

    fn set_next(&mut self) -> Result<bool> {
        loop {
            if self.value_upto < self.value_length {
                debug_assert!(!self.current.is_null());
                let value = unsafe { (*self.current).values().value_at(self.value_upto)? };
                self.next_value = Numeric::Long(value);
                self.value_upto += 1;
                self.next_is_set = true;
                return Ok(true);
            }

            if let Some(sub) = self.doc_id_merger.next()? {
                let doc_id = sub.doc_id;
                sub.values().set_document(doc_id)?;
                self.value_upto = 0;
                self.value_length = sub.values().count();
                self.current = sub;
            } else {
                self.current = ptr::null_mut();
                return Ok(false);
            }
        }
    }

    fn has_next(&mut self) -> Result<bool> {
        Ok(self.next_is_set || self.set_next()?)
    }
}

impl Iterator for SortedNumericDocValuesIter {
    type Item = Result<Numeric>;

    fn next(&mut self) -> Option<Result<Numeric>> {
        match self.has_next() {
            Ok(true) => {
                self.next_is_set = false;
                Some(Ok(self.next_value))
            }
            Ok(false) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

impl ReusableIterator for SortedNumericDocValuesIter {
    fn reset(&mut self) {
        self.doc_id_merger.reset().unwrap();
        self.next_value = Numeric::Null;
        self.next_is_set = false;
        self.value_upto = 0;
        self.value_length = 0;
        self.current = ptr::null_mut();
    }
}

struct SortedNumericDocValuesSub {
    values: *mut dyn SortedNumericDocValues,
    doc_id: DocId,
    max_doc: i32,
    base: DocIdMergerSubBase,
}

impl SortedNumericDocValuesSub {
    fn new(
        doc_map: Arc<LiveDocsDocMap>,
        values: &mut (dyn SortedNumericDocValues + 'static),
        max_doc: i32,
    ) -> Self {
        let base = DocIdMergerSubBase::new(doc_map);
        SortedNumericDocValuesSub {
            values,
            base,
            max_doc,
            doc_id: -1,
        }
    }

    #[inline]
    fn values(&mut self) -> &mut dyn SortedNumericDocValues {
        unsafe { &mut *self.values }
    }
}

impl DocIdMergerSub for SortedNumericDocValuesSub {
    fn next_doc(&mut self) -> Result<i32> {
        self.doc_id += 1;
        if self.doc_id == self.max_doc {
            Ok(NO_MORE_DOCS)
        } else {
            Ok(self.doc_id)
        }
    }

    fn base(&self) -> &DocIdMergerSubBase {
        &self.base
    }

    fn base_mut(&mut self) -> &mut DocIdMergerSubBase {
        &mut self.base
    }

    fn reset(&mut self) {
        self.base.reset();
        self.doc_id = -1;
    }
}

// the use of ReusableIterFilter is recursive in `Lucene54DocValuesConsumer#add_numeric`,
// so we must use trait object here to avoid endless type loop
pub struct ReusableIterFilter<'a, I: 'a, P> {
    iter: &'a mut dyn ReusableIterator<Item = I>,
    predicate: P,
}

impl<'a, I, P> ReusableIterFilter<'a, I, P>
where
    P: FnMut(&I) -> bool,
{
    pub fn new(
        iter: &'a mut dyn ReusableIterator<Item = I>,
        predicate: P,
    ) -> ReusableIterFilter<'a, I, P> {
        ReusableIterFilter { iter, predicate }
    }
}

impl<'a, I, P> ReusableIterator for ReusableIterFilter<'a, I, P>
where
    P: FnMut(&I) -> bool,
{
    fn reset(&mut self) {
        self.iter.reset();
    }
}

impl<'a, I, P> Iterator for ReusableIterFilter<'a, I, P>
where
    P: FnMut(&I) -> bool,
{
    type Item = I;

    fn next(&mut self) -> Option<I> {
        loop {
            match self.iter.next() {
                Some(x) => {
                    if (self.predicate)(&x) {
                        return Some(x);
                    }
                }
                None => {
                    return None;
                }
            }
        }
    }
}

pub fn hash_vec(longs: &[i64]) -> u64 {
    let mut hasher = DefaultHasher::default();
    longs.hash(&mut hasher);
    hasher.finish()
}

pub struct SetIdIter<'a, RI1, RI2> {
    doc_to_value_count: &'a mut RI1,
    values: &'a mut RI2,
    set_ids: HashMap<u64, i32>,
    doc_values: Vec<i64>,
}

impl<'a, RI1, RI2> SetIdIter<'a, RI1, RI2>
where
    RI1: ReusableIterator<Item = Result<u32>>,
    RI2: ReusableIterator<Item = Result<Numeric>>,
{
    pub fn new(
        doc_to_value_count: &'a mut RI1,
        values: &'a mut RI2,
        set_ids: HashMap<u64, i32>,
    ) -> Self {
        SetIdIter {
            doc_to_value_count,
            values,
            set_ids,
            doc_values: Vec::with_capacity(256),
        }
    }
}

impl<'a, RI1, RI2> Iterator for SetIdIter<'a, RI1, RI2>
where
    RI1: ReusableIterator<Item = Result<u32>>,
    RI2: ReusableIterator<Item = Result<Numeric>>,
{
    type Item = Result<Numeric>;

    fn next(&mut self) -> Option<Result<Numeric>> {
        match self.doc_to_value_count.next() {
            None => None,
            Some(Err(e)) => Some(Err(e)),
            Some(Ok(v)) => {
                self.doc_values.clear();
                let size = v as usize;
                for _ in 0..size {
                    match self.values.next() {
                        Some(Err(e)) => {
                            return Some(Err(e));
                        }
                        Some(Ok(s)) => {
                            self.doc_values.push(s.long_value());
                        }
                        None => unreachable!(),
                    }
                }
                let hash = hash_vec(&self.doc_values);
                let id = self.set_ids.get(&hash).unwrap();
                Some(Ok(Numeric::Int(*id)))
            }
        }
    }
}

impl<'a, RI1, RI2> ReusableIterator for SetIdIter<'a, RI1, RI2>
where
    RI1: ReusableIterator<Item = Result<u32>>,
    RI2: ReusableIterator<Item = Result<Numeric>>,
{
    fn reset(&mut self) {
        self.doc_to_value_count.reset();
        self.values.reset();
    }
}

enum MergeDVTermIterEnum {
    DV(DocValuesTermIterator),
    BitsFilter(BitsFilteredTermIterator<DocValuesTermIterator>),
}

impl TermIterator for MergeDVTermIterEnum {
    type Postings = EmptyPostingIterator;
    type TermState = OrdTermState;
    fn next(&mut self) -> Result<Option<Vec<u8>>> {
        match self {
            MergeDVTermIterEnum::DV(t) => t.next(),
            MergeDVTermIterEnum::BitsFilter(t) => t.next(),
        }
    }

    fn seek_exact(&mut self, text: &[u8]) -> Result<bool> {
        match self {
            MergeDVTermIterEnum::DV(t) => t.seek_exact(text),
            MergeDVTermIterEnum::BitsFilter(t) => t.seek_exact(text),
        }
    }

    fn seek_ceil(&mut self, text: &[u8]) -> Result<SeekStatus> {
        match self {
            MergeDVTermIterEnum::DV(t) => t.seek_ceil(text),
            MergeDVTermIterEnum::BitsFilter(t) => t.seek_ceil(text),
        }
    }

    fn seek_exact_ord(&mut self, ord: i64) -> Result<()> {
        match self {
            MergeDVTermIterEnum::DV(t) => t.seek_exact_ord(ord),
            MergeDVTermIterEnum::BitsFilter(t) => t.seek_exact_ord(ord),
        }
    }

    fn seek_exact_state(&mut self, text: &[u8], state: &Self::TermState) -> Result<()> {
        match self {
            MergeDVTermIterEnum::DV(t) => t.seek_exact_state(text, state),
            MergeDVTermIterEnum::BitsFilter(t) => t.seek_exact_state(text, state),
        }
    }

    fn term(&self) -> Result<&[u8]> {
        match self {
            MergeDVTermIterEnum::DV(t) => t.term(),
            MergeDVTermIterEnum::BitsFilter(t) => t.term(),
        }
    }

    fn ord(&self) -> Result<i64> {
        match self {
            MergeDVTermIterEnum::DV(t) => t.ord(),
            MergeDVTermIterEnum::BitsFilter(t) => t.ord(),
        }
    }

    fn doc_freq(&mut self) -> Result<i32> {
        match self {
            MergeDVTermIterEnum::DV(t) => t.doc_freq(),
            MergeDVTermIterEnum::BitsFilter(t) => t.doc_freq(),
        }
    }

    fn total_term_freq(&mut self) -> Result<i64> {
        match self {
            MergeDVTermIterEnum::DV(t) => t.total_term_freq(),
            MergeDVTermIterEnum::BitsFilter(t) => t.total_term_freq(),
        }
    }

    fn postings(&mut self) -> Result<Self::Postings> {
        match self {
            MergeDVTermIterEnum::DV(t) => t.postings(),
            MergeDVTermIterEnum::BitsFilter(t) => t.postings(),
        }
    }

    fn postings_with_flags(&mut self, flags: u16) -> Result<Self::Postings> {
        match self {
            MergeDVTermIterEnum::DV(t) => t.postings_with_flags(flags),
            MergeDVTermIterEnum::BitsFilter(t) => t.postings_with_flags(flags),
        }
    }

    fn term_state(&mut self) -> Result<Self::TermState> {
        match self {
            MergeDVTermIterEnum::DV(t) => t.term_state(),
            MergeDVTermIterEnum::BitsFilter(t) => t.term_state(),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            MergeDVTermIterEnum::DV(t) => t.is_empty(),
            MergeDVTermIterEnum::BitsFilter(t) => t.is_empty(),
        }
    }
}

pub struct IdentityLongValues;

impl LongValues for IdentityLongValues {
    fn get64(&self, index: i64) -> Result<i64> {
        Ok(index)
    }
}

impl NumericDocValues for IdentityLongValues {
    fn get(&self, doc_id: DocId) -> Result<i64> {
        Ok(doc_id as i64)
    }
}

/// maps per-segment ordinals to/from global ordinal space
// TODO: we could also have a utility method to merge Terms[] and use size() as a weight when we
// need it TODO: use more efficient packed ints structures?
// TODO: pull this out? it's pretty generic (maps between N ord()-enabled TermsEnums)
pub struct OrdinalMap {
    // globalOrd -> (globalOrd - segmentOrd) where segmentOrd is the the ordinal in
    // the first segment that contains this term
    global_ord_deltas: PackedLongValues,
    // globalOrd -> first segment container
    first_segments: PackedLongValues,
    // for every segment, segmentOrd -> globalOrd
    segment_to_global_ords: Vec<Rc<dyn LongValues>>,
    // the map from/to segment ids
    segment_map: SegmentMap,
}

impl OrdinalMap {
    pub fn build<T: TermIterator>(
        subs: Vec<Option<T>>,
        weights: Vec<usize>,
        acceptable_overhead_ratio: f32,
    ) -> Result<Self> {
        debug_assert_eq!(subs.len(), weights.len());
        let segment_map = SegmentMap::new(weights);
        Self::new(subs, segment_map, acceptable_overhead_ratio)
    }

    fn new<T: TermIterator>(
        mut subs: Vec<Option<T>>,
        segment_map: SegmentMap,
        acceptable_overhead_ratio: f32,
    ) -> Result<Self> {
        let num_subs = subs.len();
        let mut global_ord_deltas_builder = PackedLongValuesBuilder::new(
            DEFAULT_PAGE_SIZE,
            COMPACT,
            PackedLongValuesBuilderType::Monotonic,
        );
        let mut first_segments_builder = PackedLongValuesBuilder::new(
            DEFAULT_PAGE_SIZE,
            COMPACT,
            PackedLongValuesBuilderType::Default,
        );
        let mut ord_deltas = Vec::with_capacity(num_subs);
        for _i in 0..num_subs {
            ord_deltas.push(PackedLongValuesBuilder::new(
                DEFAULT_PAGE_SIZE,
                COMPACT,
                PackedLongValuesBuilderType::Monotonic,
            ));
        }
        let mut ord_delta_bits = vec![0i64; num_subs];
        let mut segment_ords = vec![0i64; num_subs];
        let mut slices = Vec::with_capacity(num_subs);
        let mut indexes = Vec::with_capacity(num_subs);
        for i in 0..num_subs {
            slices.push(ReaderSlice::new(0, 0, i));
            let idx = segment_map.new_to_old(i as i32) as usize;
            debug_assert!(subs[idx].is_some());
            let sub = subs[idx].take().unwrap();
            indexes.push(TermIteratorIndex::new(sub, i));
        }
        let mut mte = MultiTermIterator::new(slices);
        mte.reset(indexes)?;
        let mut global_ord = 0;
        loop {
            if mte.next()?.is_some() {
                let mut first_segment_index = i32::max_value() as usize;
                let mut global_ord_delta = i64::max_value();
                for i in 0..mte.num_top {
                    let segment_index = mte.subs[mte.top_indexes[i]].index;
                    let segment_ord = mte.subs[mte.top_indexes[i]].terms.as_mut().unwrap().ord()?;
                    let delta = global_ord - segment_ord;
                    // We compute the least segment where the term occurs. In case the
                    // first segment contains most (or better all) values, this will
                    // help save significant memory
                    if segment_index < first_segment_index {
                        first_segment_index = segment_index;
                        global_ord_delta = delta;
                    }

                    // for each per-segment ord, map it back to the global term.
                    while segment_ords[segment_index] <= segment_ord {
                        ord_delta_bits[segment_index] |= delta;
                        ord_deltas[segment_index].add(delta);
                        segment_ords[segment_index] += 1;
                    }
                }
                // for each unique term, just mark the first segment index/delta where it occurs
                debug_assert!(first_segment_index < segment_ords.len());
                first_segments_builder.add(first_segment_index as i64);
                global_ord_deltas_builder.add(global_ord_delta);
                global_ord += 1;
            } else {
                break;
            }
        }

        let first_segments = first_segments_builder.build();
        let global_ord_deltas = global_ord_deltas_builder.build();

        let mut segment_to_global_ords: Vec<Rc<dyn LongValues>> = Vec::with_capacity(subs.len());
        let mut i = 0;
        for mut d in ord_deltas {
            let deltas = d.build();
            if ord_delta_bits[i] == 0 {
                // segment ords perfectly match global ordinals
                // likely in case of low cardinalities and large segments
                segment_to_global_ords.push(Rc::new(IdentityLongValues {}));
            } else {
                let bits_required = if ord_delta_bits[i] < 0 {
                    64
                } else {
                    ord_delta_bits[i].bits_required() as i32
                };
                let monotonic_bits = deltas.ram_bytes_used_estimate() * 8;
                let packed_bits = bits_required as i64 * deltas.size();
                if deltas.size() < i32::max_value() as i64
                    && packed_bits as f32
                        <= monotonic_bits as f32 * (1.0 + acceptable_overhead_ratio)
                {
                    // monotonic compression mostly adds overhead, let's keep the mapping in plain
                    // packed ints
                    let size = deltas.size();
                    let mut new_deltas = get_mutable_by_ratio(
                        size as usize,
                        bits_required,
                        acceptable_overhead_ratio,
                    );
                    let mut cnt = 0;
                    for v in deltas.iterator() {
                        new_deltas.set(cnt, v);
                        cnt += 1;
                    }
                    debug_assert_eq!(cnt as i64, size);
                    segment_to_global_ords.push(Rc::new(MutableAsLongValues {
                        mutable: Arc::new(new_deltas),
                    }));
                } else {
                    segment_to_global_ords
                        .push(Rc::new(PackedLongValuesWrapper { values: deltas }));
                }
            }
            i += 1;
        }
        Ok(OrdinalMap {
            global_ord_deltas,
            first_segments,
            segment_to_global_ords,
            segment_map,
        })
    }

    pub fn value_count(&self) -> i64 {
        self.global_ord_deltas.size()
    }

    pub fn first_segment_number(&self, global_ord: i64) -> i32 {
        let new = self.first_segments.get64(global_ord).unwrap() as i32;
        self.segment_map.new_to_old(new)
    }

    pub fn first_segment_ord(&self, global_ord: i64) -> i64 {
        global_ord - self.global_ord_deltas.get64(global_ord).unwrap()
    }

    pub fn get_global_ords(&self, index: usize) -> Rc<dyn LongValues> {
        let i = self.segment_map.old_to_new(index as i32) as usize;
        Rc::clone(&self.segment_to_global_ords[i])
    }
}

#[derive(Debug)]
struct SegmentMap {
    new_to_old: Vec<i32>,
    old_to_new: Vec<i32>,
}

impl SegmentMap {
    fn new(weights: Vec<usize>) -> Self {
        let new_to_old = Self::map(&weights);
        let old_to_new = Self::inverse(&new_to_old);
        SegmentMap {
            new_to_old,
            old_to_new,
        }
    }

    fn new_to_old(&self, segment: i32) -> i32 {
        self.new_to_old[segment as usize]
    }

    fn old_to_new(&self, segment: i32) -> i32 {
        self.old_to_new[segment as usize]
    }

    fn map(weights: &[usize]) -> Vec<i32> {
        let mut new_to_old: Vec<i32> = (0..weights.len() as i32).collect();
        new_to_old.sort_by(|i, j| weights[*j as usize].cmp(&weights[*i as usize]));
        new_to_old
    }

    // inverse the map
    fn inverse(map: &[i32]) -> Vec<i32> {
        let mut inverse = vec![0i32; map.len()];
        for i in 0..map.len() {
            inverse[map[i] as usize] = i as i32;
        }
        inverse
    }
}

#[derive(Clone)]
struct MutableAsLongValues {
    mutable: Arc<MutableEnum>,
}

impl LongValues for MutableAsLongValues {
    fn get64(&self, index: i64) -> Result<i64> {
        Ok(index + self.mutable.get(index as usize))
    }
}

impl NumericDocValues for MutableAsLongValues {
    fn get(&self, doc_id: DocId) -> Result<i64> {
        self.get64(i64::from(doc_id))
    }
}

struct PackedLongValuesWrapper {
    values: PackedLongValues,
}

impl LongValues for PackedLongValuesWrapper {
    fn get64(&self, index: i64) -> Result<i64> {
        self.values.get64(index).map(|v| index + v)
    }
}

impl NumericDocValues for PackedLongValuesWrapper {
    fn get(&self, doc_id: DocId) -> Result<i64> {
        self.get64(i64::from(doc_id))
    }
}
