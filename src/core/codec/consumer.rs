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

use core::codec::{
    BlockTreeTermsWriter, Codec, FieldsProducer, Lucene50PostingsWriter, NormsProducer,
    PerFieldFieldsWriter,
};
use core::index::doc_id_merger::{
    doc_id_merger_of, DocIdMerger, DocIdMergerEnum, DocIdMergerSub, DocIdMergerSubBase,
};
use core::index::{
    AcceptStatus, BinaryDocValues, DocValuesTermIterator, DocValuesType, EmptyBinaryDocValues,
    EmptyNumericDocValues, EmptySortedDocValues, EmptySortedNumericDocValues,
    EmptySortedSetDocValues, FieldInfo, Fields, FilteredTermIterBase, FilteredTermIterator,
    LiveDocsDocMap, MappedMultiFields, MergeState, MultiFields, NumericDocValues, OrdTermState,
    OrdinalMap, ReaderSlice, SeekStatus, SortedDocValues, SortedNumericDocValues,
    SortedSetDocValues, TermIterator, NO_MORE_ORDS,
};
use core::search::posting_iterator::EmptyPostingIterator;
use core::search::NO_MORE_DOCS;
use core::store::Directory;
use core::util::bkd::LongBitSet;
use core::util::packed_misc::COMPACT;
use core::util::{
    numeric::Numeric, BitsMut, BytesRef, DocId, LongValues, MatchNoBits, ReusableIterator,
};

use error::Result;

use std::ptr;
use std::rc::Rc;
use std::sync::Arc;

/// Abstract API that consumes terms, doc, freq, prox, offset and
/// payloads postings.  Concrete implementations of this
/// actually do "something" with the postings (write it into
/// the index in a specific format).

pub trait FieldsConsumer {
    // TODO: can we somehow compute stats for you...?

    // TODO: maybe we should factor out "limited" (only
    // iterables, no counts/stats) base classes from
    // Fields/Terms/Docs/AndPositions?

    /// Write all fields, terms and postings.  This the "pull"
    /// API, allowing you to iterate more than once over the
    /// postings, somewhat analogous to using a DOM API to
    /// traverse an XML tree.
    ///
    /// Notes:
    /// - You must compute index statistics, including each Term's docFreq and totalTermFreq, as
    ///   well as the summary sumTotalTermFreq, sumTotalDocFreq and docCount.
    ///
    /// - You must skip terms that have no docs and fields that have no terms, even though the
    ///   provided Fields API will expose them; this typically requires lazily writing the field or
    ///   term until you've actually seen the first term or document.
    ///
    /// - The provided Fields instance is limited: you cannot call any methods that return
    ///   statistics/counts; you cannot pass a non-null live docs when pulling docs/positions enums.
    fn write(&mut self, fields: &impl Fields) -> Result<()>;

    /// Merges in the fields from the readers in
    /// <code>mergeState</code>. The default implementation skips
    /// and maps around deleted documents, and calls {@link #write(Fields)}.
    /// Implementations can override this method for more sophisticated
    /// merging (bulk-byte copying, etc).
    fn merge<D: Directory, C: Codec>(&mut self, merge_state: &mut MergeState<D, C>) -> Result<()> {
        let mut fields = vec![];
        let mut slices = vec![];

        let mut doc_base = 0;
        let fields_producers = merge_state.fields_producers.clone();
        // let fields_producers = mem::replace(&mut merge_state.fields_producers,
        // Vec::with_capacity(0));
        for (i, f) in fields_producers.into_iter().enumerate() {
            f.check_integrity()?;
            let max_doc = merge_state.max_docs[i];
            slices.push(ReaderSlice::new(doc_base, max_doc, i));
            fields.push(f);
            doc_base += max_doc;
        }

        let fields = MultiFields::new(fields, slices);
        let merged_fields = MappedMultiFields::new(merge_state, fields);
        self.write(&merged_fields)
    }
}

pub enum FieldsConsumerEnum<D: Directory, DW: Directory, C: Codec> {
    Lucene50(BlockTreeTermsWriter<Lucene50PostingsWriter<DW::IndexOutput>, DW::IndexOutput>),
    PerField(PerFieldFieldsWriter<D, DW, C>),
}

impl<D: Directory, DW: Directory, C: Codec> FieldsConsumer for FieldsConsumerEnum<D, DW, C> {
    fn write(&mut self, fields: &impl Fields) -> Result<()> {
        match self {
            FieldsConsumerEnum::Lucene50(w) => w.write(fields),
            FieldsConsumerEnum::PerField(w) => w.write(fields),
        }
    }

    fn merge<D1: Directory, C1: Codec>(
        &mut self,
        merge_state: &mut MergeState<D1, C1>,
    ) -> Result<()> {
        match self {
            FieldsConsumerEnum::Lucene50(w) => w.merge(merge_state),
            FieldsConsumerEnum::PerField(w) => w.merge(merge_state),
        }
    }
}

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

pub(crate) fn singleton_view<'a, RI1, RI2>(
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

pub(crate) struct SingletonViewIter<'a, RI1, RI2> {
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

pub trait NormsConsumer {
    fn add_norms_field(
        &mut self,
        field_info: &FieldInfo,
        values: &mut impl ReusableIterator<Item = Result<Numeric>>,
    ) -> Result<()>;

    fn merge_norms_field<D: Directory, C: Codec>(
        &mut self,
        field_info: &FieldInfo,
        merge_state: &MergeState<D, C>,
        to_merge: Vec<Box<dyn NumericDocValues>>,
    ) -> Result<()> {
        let mut iter = NormsValuesMergeIter::new(merge_state, to_merge)?;
        self.add_norms_field(field_info, &mut iter)
    }

    fn merge<D: Directory, C: Codec>(&mut self, merge_state: &mut MergeState<D, C>) -> Result<()> {
        for producer in &merge_state.norms_producers {
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
            if merge_field_info.has_norms() {
                let mut to_merge = vec![];
                for i in 0..merge_state.norms_producers.len() {
                    let mut norms: Box<dyn NumericDocValues> =
                        Box::new(EmptyNumericDocValues::default());
                    if let Some(ref norm_producer) = merge_state.norms_producers[i] {
                        if let Some(field_info) =
                            merge_state.fields_infos[i].field_info_by_name(&merge_field_info.name)
                        {
                            if field_info.has_norms() {
                                norms = norm_producer.norms(field_info)?;
                            }
                        }
                    }
                    to_merge.push(norms);
                }
                self.merge_norms_field(merge_field_info, merge_state, to_merge)?;
            }
        }
        Ok(())
    }
}

struct NormsValuesSub {
    values: Box<dyn NumericDocValues>,
    doc_id: i32,
    max_doc: i32,
    base: DocIdMergerSubBase,
}

impl NormsValuesSub {
    fn new(doc_map: Arc<LiveDocsDocMap>, values: Box<dyn NumericDocValues>, max_doc: i32) -> Self {
        let base = DocIdMergerSubBase::new(doc_map);
        NormsValuesSub {
            values,
            doc_id: -1,
            max_doc,
            base,
        }
    }
}

impl DocIdMergerSub for NormsValuesSub {
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

struct NormsValuesMergeIter {
    doc_id_merger: DocIdMergerEnum<NormsValuesSub>,
    next_value: Numeric,
    next_is_set: bool,
}

impl NormsValuesMergeIter {
    fn new<D: Directory + 'static, C: Codec>(
        merge_state: &MergeState<D, C>,
        to_merge: Vec<Box<dyn NumericDocValues>>,
    ) -> Result<Self> {
        let mut subs = Vec::with_capacity(to_merge.len());
        for (i, dv) in to_merge.into_iter().enumerate() {
            subs.push(NormsValuesSub::new(
                Arc::clone(&merge_state.doc_maps[i]),
                dv,
                merge_state.max_docs[i],
            ));
        }
        let doc_id_merger = doc_id_merger_of(subs, merge_state.needs_index_sort)?;
        Ok(NormsValuesMergeIter {
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
            let value = sub.values.get_mut(sub.doc_id)?;
            self.next_value = Numeric::Long(value);
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

impl Iterator for NormsValuesMergeIter {
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

impl ReusableIterator for NormsValuesMergeIter {
    fn reset(&mut self) {
        self.doc_id_merger.reset().unwrap();
        self.next_is_set = false;
        self.next_value = Numeric::Null;
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
