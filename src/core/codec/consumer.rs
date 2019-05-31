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
    AcceptStatus, BinaryDocValuesRef, DocValuesTermIterator, DocValuesType, EmptyBinaryDocValues,
    EmptyNumericDocValues, EmptySortedDocValues, EmptySortedNumericDocValues,
    EmptySortedSetDocValues, FieldInfo, Fields, FilteredTermIterBase, FilteredTermIterator,
    LiveDocsDocMap, MappedMultiFields, MergeState, MultiFields, NumericDocValues,
    NumericDocValuesContext, NumericDocValuesRef, OrdTermState, OrdinalMap, ReaderSlice,
    SeekStatus, SortedDocValues, SortedDocValuesRef, SortedNumericDocValuesContext,
    SortedNumericDocValuesRef, SortedSetDocValuesRef, TermIterator, NO_MORE_ORDS,
};
use core::search::posting_iterator::EmptyPostingIterator;
use core::search::NO_MORE_DOCS;
use core::store::Directory;
use core::util::bkd::LongBitSet;
use core::util::numeric::Numeric;
use core::util::packed_misc::COMPACT;
use core::util::{BitsRef, BytesRef, DocId, LongValues, MatchNoBits, ReusableIterator};

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

/// Abstract API that consumes numeric, binary and
/// sorted docvalues.  Concrete implementations of this
/// actually do "something" with the docvalues (write it into
/// the index in a specific format).
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
                        let mut values: Arc<NumericDocValues> = Arc::new(EmptyNumericDocValues {});
                        let mut bits: BitsRef =
                            Arc::new(MatchNoBits::new(merge_state.max_docs[i] as usize));
                        if let Some(ref producer) = merge_state.doc_values_producers[i] {
                            if let Some(field_info) = merge_state.fields_infos[i]
                                .field_info_by_name(&merge_field_info.name)
                            {
                                if field_info.doc_values_type == DocValuesType::Numeric {
                                    values = producer.get_numeric(field_info)?;
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
                        &to_merge,
                        &docs_with_field,
                    )?;
                }
                DocValuesType::Binary => {
                    let mut to_merge = vec![];
                    let mut docs_with_field = vec![];
                    for i in 0..merge_state.doc_values_producers.len() {
                        let mut values: BinaryDocValuesRef = Arc::new(EmptyBinaryDocValues {});
                        let mut bits: BitsRef =
                            Arc::new(MatchNoBits::new(merge_state.max_docs[i] as usize));
                        if let Some(ref producer) = merge_state.doc_values_producers[i] {
                            if let Some(field_info) = merge_state.fields_infos[i]
                                .field_info_by_name(&merge_field_info.name)
                            {
                                if field_info.doc_values_type == DocValuesType::Binary {
                                    values = producer.get_binary(field_info)?;
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
                        &to_merge,
                        &docs_with_field,
                    )?;
                }
                DocValuesType::Sorted => {
                    let mut to_merge = vec![];
                    for i in 0..merge_state.doc_values_producers.len() {
                        let mut values: Arc<SortedDocValues> = Arc::new(EmptySortedDocValues {});
                        if let Some(ref producer) = merge_state.doc_values_producers[i] {
                            if let Some(field_info) = merge_state.fields_infos[i]
                                .field_info_by_name(&merge_field_info.name)
                            {
                                if field_info.doc_values_type == DocValuesType::Sorted {
                                    values = producer.get_sorted(field_info)?;
                                }
                            }
                        }
                        to_merge.push(values);
                    }
                    self.merge_sorted_field(merge_field_info.as_ref(), merge_state, &to_merge)?;
                }
                DocValuesType::SortedSet => {
                    let mut to_merge = vec![];
                    for i in 0..merge_state.doc_values_producers.len() {
                        let mut values: SortedSetDocValuesRef =
                            Arc::new(EmptySortedSetDocValues {});
                        if let Some(ref producer) = merge_state.doc_values_producers[i] {
                            if let Some(field_info) = merge_state.fields_infos[i]
                                .field_info_by_name(&merge_field_info.name)
                            {
                                if field_info.doc_values_type == DocValuesType::SortedSet {
                                    values = producer.get_sorted_set(field_info)?;
                                }
                            }
                        }
                        to_merge.push(values);
                    }
                    self.merge_sorted_set_field(merge_field_info.as_ref(), merge_state, &to_merge)?;
                }
                DocValuesType::SortedNumeric => {
                    let mut to_merge = vec![];
                    for i in 0..merge_state.doc_values_producers.len() {
                        let mut values: SortedNumericDocValuesRef =
                            Arc::new(EmptySortedNumericDocValues {});
                        if let Some(ref producer) = merge_state.doc_values_producers[i] {
                            if let Some(field_info) = merge_state.fields_infos[i]
                                .field_info_by_name(&merge_field_info.name)
                            {
                                if field_info.doc_values_type == DocValuesType::SortedNumeric {
                                    values = producer.get_sorted_numeric(field_info)?;
                                }
                            }
                        }
                        to_merge.push(values);
                    }
                    self.merge_sorted_numeric_field(
                        merge_field_info.as_ref(),
                        merge_state,
                        &to_merge,
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
        to_merge: &[Arc<dyn NumericDocValues>],
        docs_with_field: &[BitsRef],
    ) -> Result<()> {
        let mut iter = NumericDocValuesMergeIter::new(merge_state, to_merge, docs_with_field)?;
        self.add_numeric_field(field_info, &mut iter)
    }

    fn merge_binary_field<D: Directory, C: Codec>(
        &mut self,
        field_info: &FieldInfo,
        merge_state: &MergeState<D, C>,
        to_merge: &[BinaryDocValuesRef],
        docs_with_field: &[BitsRef],
    ) -> Result<()> {
        let mut iter = BinaryDocValuesMergeIter::new(merge_state, to_merge, docs_with_field)?;
        self.add_binary_field(field_info, &mut iter)
    }

    fn merge_sorted_field<D: Directory, C: Codec>(
        &mut self,
        field_info: &FieldInfo,
        merge_state: &MergeState<D, C>,
        to_merge: &[SortedDocValuesRef],
    ) -> Result<()> {
        let num_readers = to_merge.len();
        // step 1: iterate thru each sub and mark terms still in use
        let mut live_terms = Vec::with_capacity(num_readers);
        let mut weights = Vec::with_capacity(num_readers);
        for i in 0..num_readers {
            let dv = &to_merge[i];
            let max_doc = merge_state.max_docs[i];
            let live_docs = &merge_state.live_docs[i];
            if live_docs.len() == 0 {
                live_terms.push(Some(MergeDVTermIterEnum::DV(dv.term_iterator()?)));
                weights.push(dv.get_value_count());
            } else {
                let mut bitset = LongBitSet::new(dv.get_value_count() as i64);
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
        let mut ords_iter = SortedDocValuesMergeOrdIter::new(to_merge, merge_state, &map)?;
        let mut bytes_iter = SortedDocValuesMergeBytesIter::new(&map, to_merge);
        self.add_sorted_field(field_info, &mut bytes_iter, &mut ords_iter)
    }

    fn merge_sorted_set_field<D: Directory, C: Codec>(
        &mut self,
        field_info: &FieldInfo,
        merge_state: &MergeState<D, C>,
        to_merge: &[SortedSetDocValuesRef],
    ) -> Result<()> {
        let num_readers = to_merge.len();
        // step 1: iterate thru each sub and mark terms still in use
        let mut live_terms = Vec::with_capacity(num_readers);
        let mut weights = Vec::with_capacity(num_readers);
        for (i, dv) in to_merge.iter().enumerate().take(num_readers) {
            let max_doc = merge_state.max_docs[i];
            let live_docs = &merge_state.live_docs[i];
            if live_docs.len() == 0 {
                live_terms.push(Some(MergeDVTermIterEnum::DV(dv.term_iterator()?)));
                weights.push(dv.get_value_count());
            } else {
                let mut bitset = LongBitSet::new(dv.get_value_count() as i64);
                for i in 0..max_doc {
                    if live_docs.get(i as usize)? {
                        let mut ctx = dv.set_document(i)?;
                        loop {
                            let ord = dv.next_ord(&mut ctx)?;
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
            &mut SortedSetDocValuesMergeBytesIter::new(&map, &to_merge),
            &mut SortedSetDocValuesOrdCountIter::new(&to_merge, merge_state, &map)?,
            &mut SortedSetDocValuesMergeOrdIter::new(&to_merge, merge_state, &map)?,
        )
    }

    fn merge_sorted_numeric_field<D: Directory, C: Codec>(
        &mut self,
        field_info: &FieldInfo,
        merge_state: &MergeState<D, C>,
        to_merge: &[SortedNumericDocValuesRef],
    ) -> Result<()> {
        let mut values_iter = SortedNumericDocValuesIter::new(to_merge, merge_state)?;
        let mut doc_counts_iter = SortedNumericDocValuesCountIter::new(to_merge, merge_state)?;

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

struct NumericDocValuesMergeIter<'a, D: Directory + 'static, C: Codec> {
    merge_state: &'a MergeState<D, C>,
    to_merge: &'a [Arc<dyn NumericDocValues>],
    docs_with_field: &'a [BitsRef],
    doc_id_merger: DocIdMergerEnum<NumericDocValuesSub>,
    next_value: Numeric,
    next_is_set: bool,
}

impl<'a, D: Directory + 'static, C: Codec> NumericDocValuesMergeIter<'a, D, C> {
    fn new(
        merge_state: &'a MergeState<D, C>,
        to_merge: &'a [Arc<dyn NumericDocValues>],
        docs_with_field: &'a [BitsRef],
    ) -> Result<Self> {
        let mut subs = Vec::with_capacity(to_merge.len());
        for i in 0..to_merge.len() {
            subs.push(NumericDocValuesSub::new(
                Arc::clone(&merge_state.doc_maps[i]),
                Arc::clone(&to_merge[i]),
                Arc::clone(&docs_with_field[i]),
                merge_state.max_docs[i],
            ));
        }
        let doc_id_merger = doc_id_merger_of(subs, merge_state.needs_index_sort)?;
        Ok(NumericDocValuesMergeIter {
            merge_state,
            to_merge,
            docs_with_field,
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
            let current_ctx = sub.ctx.take();
            let (next_value, ctx) = sub.values.get_with_ctx(current_ctx, sub.doc_id)?;
            self.next_value = if next_value != 0 || sub.docs_with_field.get(sub.doc_id as usize)? {
                Numeric::Long(next_value)
            } else {
                Numeric::Null
            };
            sub.ctx = ctx;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

impl<'a, D: Directory + 'static, C: Codec> Iterator for NumericDocValuesMergeIter<'a, D, C> {
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

impl<'a, D: Directory + 'static, C: Codec> ReusableIterator
    for NumericDocValuesMergeIter<'a, D, C>
{
    fn reset(&mut self) {
        let mut subs = Vec::with_capacity(self.to_merge.len());
        for i in 0..self.to_merge.len() {
            subs.push(NumericDocValuesSub::new(
                Arc::clone(&self.merge_state.doc_maps[i]),
                Arc::clone(&self.to_merge[i]),
                Arc::clone(&self.docs_with_field[i]),
                self.merge_state.max_docs[i],
            ));
        }
        let doc_id_merger = doc_id_merger_of(subs, self.merge_state.needs_index_sort).unwrap();
        self.doc_id_merger = doc_id_merger;
        self.next_is_set = false;
        self.next_value = Numeric::Null;
    }
}

struct NumericDocValuesSub {
    values: NumericDocValuesRef,
    docs_with_field: BitsRef,
    doc_id: i32,
    max_doc: i32,
    base: DocIdMergerSubBase,
    ctx: NumericDocValuesContext,
}

impl NumericDocValuesSub {
    fn new(
        doc_map: Arc<LiveDocsDocMap>,
        values: NumericDocValuesRef,
        docs_with_field: BitsRef,
        max_doc: i32,
    ) -> Self {
        let base = DocIdMergerSubBase::new(doc_map);
        NumericDocValuesSub {
            values,
            docs_with_field,
            doc_id: -1,
            max_doc,
            base,
            ctx: None,
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
}

struct BinaryDocValuesMergeIter<'a, D: Directory + 'static, C: Codec> {
    merge_state: &'a MergeState<D, C>,
    to_merge: &'a [BinaryDocValuesRef],
    docs_with_field: &'a [BitsRef],
    doc_id_merger: DocIdMergerEnum<BinaryDocValuesSub>,
    next_value: Vec<u8>,
    next_ref: BytesRef,
    next_is_set: bool,
}

impl<'a, D: Directory + 'static, C: Codec> BinaryDocValuesMergeIter<'a, D, C> {
    fn new(
        merge_state: &'a MergeState<D, C>,
        to_merge: &'a [BinaryDocValuesRef],
        docs_with_field: &'a [BitsRef],
    ) -> Result<Self> {
        let mut subs = Vec::with_capacity(to_merge.len());
        for i in 0..to_merge.len() {
            subs.push(BinaryDocValuesSub::new(
                Arc::clone(&merge_state.doc_maps[i]),
                Arc::clone(&to_merge[i]),
                Arc::clone(&docs_with_field[i]),
                merge_state.max_docs[i],
            ));
        }
        let doc_id_merger = doc_id_merger_of(subs, merge_state.needs_index_sort)?;
        Ok(BinaryDocValuesMergeIter {
            merge_state,
            to_merge,
            docs_with_field,
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

impl<'a, D: Directory + 'static, C: Codec> Iterator for BinaryDocValuesMergeIter<'a, D, C> {
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

impl<'a, D: Directory + 'static, C: Codec> ReusableIterator for BinaryDocValuesMergeIter<'a, D, C> {
    fn reset(&mut self) {
        let mut subs = Vec::with_capacity(self.to_merge.len());
        for i in 0..self.to_merge.len() {
            subs.push(BinaryDocValuesSub::new(
                Arc::clone(&self.merge_state.doc_maps[i]),
                Arc::clone(&self.to_merge[i]),
                Arc::clone(&self.docs_with_field[i]),
                self.merge_state.max_docs[i],
            ));
        }
        let doc_id_merger = doc_id_merger_of(subs, self.merge_state.needs_index_sort).unwrap();
        self.doc_id_merger = doc_id_merger;
        self.next_is_set = false;
        self.next_value.clear();
        self.next_ref = BytesRef::default();
    }
}

struct BinaryDocValuesSub {
    values: BinaryDocValuesRef,
    docs_with_field: BitsRef,
    doc_id: i32,
    max_doc: i32,
    base: DocIdMergerSubBase,
}

impl BinaryDocValuesSub {
    fn new(
        doc_map: Arc<LiveDocsDocMap>,
        values: BinaryDocValuesRef,
        docs_with_field: BitsRef,
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
}

struct SortedDocValuesMergeBytesIter<'a> {
    map: &'a OrdinalMap,
    dvs: &'a [SortedDocValuesRef],
    current_ord: i64,
    current: Vec<u8>,
}

impl<'a> SortedDocValuesMergeBytesIter<'a> {
    fn new(map: &'a OrdinalMap, dvs: &'a [SortedDocValuesRef]) -> Self {
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

struct SortedDocValuesMergeOrdIter<'a, D: Directory + 'static, C: Codec> {
    to_merge: &'a [SortedDocValuesRef],
    merge_state: &'a MergeState<D, C>,
    map: &'a OrdinalMap,
    doc_id_merger: DocIdMergerEnum<SortedDocValuesSub>,
    next_value: Numeric,
    next_is_set: bool,
}

impl<'a, D: Directory + 'static, C: Codec> SortedDocValuesMergeOrdIter<'a, D, C> {
    fn new(
        to_merge: &'a [SortedDocValuesRef],
        merge_state: &'a MergeState<D, C>,
        map: &'a OrdinalMap,
    ) -> Result<Self> {
        let mut subs = Vec::with_capacity(to_merge.len());
        for (i, dv) in to_merge.iter().enumerate() {
            subs.push(SortedDocValuesSub::new(
                Arc::clone(&merge_state.doc_maps[i]),
                Arc::clone(dv),
                merge_state.max_docs[i],
                map.get_global_ords(i),
            ));
        }
        let doc_id_merger = doc_id_merger_of(subs, merge_state.needs_index_sort)?;
        Ok(SortedDocValuesMergeOrdIter {
            to_merge,
            merge_state,
            map,
            doc_id_merger,
            next_value: Numeric::Null,
            next_is_set: false,
        })
    }

    fn set_next(&mut self) -> Result<bool> {
        if let Some(sub) = self.doc_id_merger.next()? {
            self.next_is_set = true;
            let seg_ord = sub.values.get_ord(sub.doc_id)?;
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

impl<'a, D: Directory + 'static, C: Codec> Iterator for SortedDocValuesMergeOrdIter<'a, D, C> {
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

impl<'a, D: Directory + 'static, C: Codec> ReusableIterator
    for SortedDocValuesMergeOrdIter<'a, D, C>
{
    fn reset(&mut self) {
        let mut subs = Vec::with_capacity(self.to_merge.len());
        for i in 0..self.to_merge.len() {
            subs.push(SortedDocValuesSub::new(
                Arc::clone(&self.merge_state.doc_maps[i]),
                Arc::clone(&self.to_merge[i]),
                self.merge_state.max_docs[i],
                self.map.get_global_ords(i),
            ));
        }
        self.doc_id_merger = doc_id_merger_of(subs, self.merge_state.needs_index_sort).unwrap();
        self.next_value = Numeric::Null;
        self.next_is_set = false;
    }
}

struct SortedDocValuesSub {
    values: SortedDocValuesRef,
    doc_id: DocId,
    max_doc: i32,
    map: Rc<dyn LongValues>,
    base: DocIdMergerSubBase,
}

impl SortedDocValuesSub {
    fn new(
        doc_map: Arc<LiveDocsDocMap>,
        values: SortedDocValuesRef,
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
}

struct SortedSetDocValuesMergeBytesIter<'a> {
    map: &'a OrdinalMap,
    dvs: &'a [SortedSetDocValuesRef],
    current_ord: i64,
    current: Vec<u8>,
}

impl<'a> SortedSetDocValuesMergeBytesIter<'a> {
    fn new(map: &'a OrdinalMap, dvs: &'a [SortedSetDocValuesRef]) -> Self {
        SortedSetDocValuesMergeBytesIter {
            map,
            dvs,
            current_ord: 0,
            current: Vec::with_capacity(0),
        }
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
        match self.dvs[segment_number as usize].lookup_ord(segment_ord) {
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

struct SortedSetDocValuesOrdCountIter<'a, D: Directory + 'static, C: Codec> {
    to_merge: &'a [SortedSetDocValuesRef],
    merge_state: &'a MergeState<D, C>,
    map: &'a OrdinalMap,
    doc_id_merger: DocIdMergerEnum<SortedSetDocValuesSub>,
    next_value: u32,
    next_is_set: bool,
}

impl<'a, D: Directory + 'static, C: Codec> SortedSetDocValuesOrdCountIter<'a, D, C> {
    fn new(
        to_merge: &'a [SortedSetDocValuesRef],
        merge_state: &'a MergeState<D, C>,
        map: &'a OrdinalMap,
    ) -> Result<Self> {
        let mut subs = Vec::with_capacity(to_merge.len());
        for (i, dv) in to_merge.iter().enumerate() {
            subs.push(SortedSetDocValuesSub::new(
                Arc::clone(&merge_state.doc_maps[i]),
                Arc::clone(dv),
                merge_state.max_docs[i],
                map.get_global_ords(i),
            ));
        }
        let doc_id_merger = doc_id_merger_of(subs, merge_state.needs_index_sort)?;
        Ok(SortedSetDocValuesOrdCountIter {
            to_merge,
            merge_state,
            map,
            doc_id_merger,
            next_value: 0,
            next_is_set: false,
        })
    }

    fn set_next(&mut self) -> Result<bool> {
        if let Some(sub) = self.doc_id_merger.next()? {
            self.next_is_set = true;
            let mut ctx = sub.values.set_document(sub.doc_id)?;
            self.next_value = 0;
            while sub.values.next_ord(&mut ctx)? != NO_MORE_ORDS {
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

impl<'a, D: Directory + 'static, C: Codec> Iterator for SortedSetDocValuesOrdCountIter<'a, D, C> {
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

impl<'a, D: Directory + 'static, C: Codec> ReusableIterator
    for SortedSetDocValuesOrdCountIter<'a, D, C>
{
    fn reset(&mut self) {
        let mut subs = Vec::with_capacity(self.to_merge.len());
        for i in 0..self.to_merge.len() {
            let sub = SortedSetDocValuesSub::new(
                Arc::clone(&self.merge_state.doc_maps[i]),
                Arc::clone(&self.to_merge[i]),
                self.merge_state.max_docs[i],
                self.map.get_global_ords(i),
            );
            subs.push(sub);
        }
        self.doc_id_merger = doc_id_merger_of(subs, self.merge_state.needs_index_sort).unwrap();
        self.next_value = 0;
        self.next_is_set = false;
    }
}

struct SortedSetDocValuesMergeOrdIter<'a, D: Directory + 'static, C: Codec> {
    to_merge: &'a [SortedSetDocValuesRef],
    merge_state: &'a MergeState<D, C>,
    map: &'a OrdinalMap,
    doc_id_merger: DocIdMergerEnum<SortedSetDocValuesSub>,
    next_value: Numeric,
    next_is_set: bool,
    ords: Vec<i64>,
    ord_upto: usize,
    ord_length: usize,
}

impl<'a, D: Directory + 'static, C: Codec> SortedSetDocValuesMergeOrdIter<'a, D, C> {
    fn new(
        to_merge: &'a [SortedSetDocValuesRef],
        merge_state: &'a MergeState<D, C>,
        map: &'a OrdinalMap,
    ) -> Result<Self> {
        let mut subs = Vec::with_capacity(to_merge.len());
        for (i, dv) in to_merge.iter().enumerate() {
            subs.push(SortedSetDocValuesSub::new(
                Arc::clone(&merge_state.doc_maps[i]),
                Arc::clone(dv),
                merge_state.max_docs[i],
                map.get_global_ords(i),
            ));
        }
        let doc_id_merger = doc_id_merger_of(subs, merge_state.needs_index_sort)?;
        Ok(SortedSetDocValuesMergeOrdIter {
            to_merge,
            merge_state,
            map,
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
                let mut ctx = sub.values.set_document(sub.doc_id)?;
                self.ord_upto = 0;
                self.ords.clear();
                loop {
                    let ord = sub.values.next_ord(&mut ctx)?;
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

impl<'a, D: Directory + 'static, C: Codec> Iterator for SortedSetDocValuesMergeOrdIter<'a, D, C> {
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

impl<'a, D: Directory + 'static, C: Codec> ReusableIterator
    for SortedSetDocValuesMergeOrdIter<'a, D, C>
{
    fn reset(&mut self) {
        let mut subs = Vec::with_capacity(self.to_merge.len());
        for i in 0..self.to_merge.len() {
            let sub = SortedSetDocValuesSub::new(
                Arc::clone(&self.merge_state.doc_maps[i]),
                Arc::clone(&self.to_merge[i]),
                self.merge_state.max_docs[i],
                self.map.get_global_ords(i),
            );
            subs.push(sub);
        }
        self.doc_id_merger = doc_id_merger_of(subs, self.merge_state.needs_index_sort).unwrap();
        self.next_value = Numeric::Null;
        self.next_is_set = false;
        self.ords.clear();
        self.ord_length = 0;
        self.ord_upto = 0;
    }
}

struct SortedSetDocValuesSub {
    values: SortedSetDocValuesRef,
    doc_id: DocId,
    max_doc: i32,
    map: Rc<dyn LongValues>,
    base: DocIdMergerSubBase,
}

impl SortedSetDocValuesSub {
    fn new(
        doc_map: Arc<LiveDocsDocMap>,
        values: SortedSetDocValuesRef,
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
}

struct SortedNumericDocValuesCountIter<'a, D: Directory + 'static, C: Codec> {
    to_merge: &'a [SortedNumericDocValuesRef],
    merge_state: &'a MergeState<D, C>,
    doc_id_merger: DocIdMergerEnum<SortedNumericDocValuesSub>,
    next_value: u32,
    next_is_set: bool,
}

impl<'a, D: Directory + 'static, C: Codec> SortedNumericDocValuesCountIter<'a, D, C> {
    fn new(
        to_merge: &'a [SortedNumericDocValuesRef],
        merge_state: &'a MergeState<D, C>,
    ) -> Result<Self> {
        let mut subs = Vec::with_capacity(to_merge.len());
        for (i, dv) in to_merge.iter().enumerate() {
            subs.push(SortedNumericDocValuesSub::new(
                Arc::clone(&merge_state.doc_maps[i]),
                Arc::clone(dv),
                merge_state.max_docs[i],
            ));
        }
        let doc_id_merger = doc_id_merger_of(subs, merge_state.needs_index_sort)?;
        Ok(SortedNumericDocValuesCountIter {
            to_merge,
            merge_state,
            doc_id_merger,
            next_value: 0,
            next_is_set: false,
        })
    }

    fn set_next(&mut self) -> Result<bool> {
        if let Some(sub) = self.doc_id_merger.next()? {
            let ctx = sub.ctx.take();
            let new_ctx = sub.values.set_document(ctx, sub.doc_id)?;
            self.next_value = sub.values.count(&new_ctx) as u32;
            self.next_is_set = true;
            sub.ctx = Some(new_ctx);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn has_next(&mut self) -> Result<bool> {
        Ok(self.next_is_set || self.set_next()?)
    }
}

impl<'a, D: Directory + 'static, C: Codec> Iterator for SortedNumericDocValuesCountIter<'a, D, C> {
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

impl<'a, D: Directory + 'static, C: Codec> ReusableIterator
    for SortedNumericDocValuesCountIter<'a, D, C>
{
    fn reset(&mut self) {
        let mut subs = Vec::with_capacity(self.to_merge.len());
        for i in 0..self.to_merge.len() {
            let sub = SortedNumericDocValuesSub::new(
                Arc::clone(&self.merge_state.doc_maps[i]),
                Arc::clone(&self.to_merge[i]),
                self.merge_state.max_docs[i],
            );
            subs.push(sub);
        }
        self.doc_id_merger = doc_id_merger_of(subs, self.merge_state.needs_index_sort).unwrap();
        self.next_value = 0;
        self.next_is_set = false;
    }
}

struct SortedNumericDocValuesIter<'a, D: Directory + 'static, C: Codec> {
    to_merge: &'a [SortedNumericDocValuesRef],
    merge_state: &'a MergeState<D, C>,
    doc_id_merger: DocIdMergerEnum<SortedNumericDocValuesSub>,
    next_value: Numeric,
    next_is_set: bool,
    value_upto: usize,
    value_length: usize,
    current: *mut SortedNumericDocValuesSub,
}

impl<'a, D: Directory + 'static, C: Codec> SortedNumericDocValuesIter<'a, D, C> {
    fn new(
        to_merge: &'a [SortedNumericDocValuesRef],
        merge_state: &'a MergeState<D, C>,
    ) -> Result<Self> {
        let mut subs = Vec::with_capacity(to_merge.len());
        for i in 0..to_merge.len() {
            subs.push(SortedNumericDocValuesSub::new(
                Arc::clone(&merge_state.doc_maps[i]),
                Arc::clone(&to_merge[i]),
                merge_state.max_docs[i],
            ));
        }
        let doc_id_merger = doc_id_merger_of(subs, merge_state.needs_index_sort)?;
        Ok(SortedNumericDocValuesIter {
            to_merge,
            merge_state,
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
                let value = unsafe {
                    (*self.current)
                        .values
                        .value_at((*self.current).ctx.as_ref().unwrap(), self.value_upto)?
                };
                self.next_value = Numeric::Long(value);
                self.value_upto += 1;
                self.next_is_set = true;
                return Ok(true);
            }

            if let Some(sub) = self.doc_id_merger.next()? {
                let current_ctx = sub.ctx.take();
                let ctx = sub.values.set_document(current_ctx, sub.doc_id)?;
                self.value_upto = 0;
                self.value_length = sub.values.count(&ctx);
                sub.ctx = Some(ctx);
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

impl<'a, D: Directory + 'static, C: Codec> Iterator for SortedNumericDocValuesIter<'a, D, C> {
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

impl<'a, D: Directory + 'static, C: Codec> ReusableIterator
    for SortedNumericDocValuesIter<'a, D, C>
{
    fn reset(&mut self) {
        let mut subs = Vec::with_capacity(self.to_merge.len());
        for i in 0..self.to_merge.len() {
            subs.push(SortedNumericDocValuesSub::new(
                Arc::clone(&self.merge_state.doc_maps[i]),
                Arc::clone(&self.to_merge[i]),
                self.merge_state.max_docs[i],
            ));
        }
        let doc_id_merger = doc_id_merger_of(subs, self.merge_state.needs_index_sort).unwrap();
        self.doc_id_merger = doc_id_merger;
        self.next_value = Numeric::Null;
        self.next_is_set = false;
        self.value_upto = 0;
        self.value_length = 0;
        self.current = ptr::null_mut();
    }
}

struct SortedNumericDocValuesSub {
    values: SortedNumericDocValuesRef,
    doc_id: DocId,
    max_doc: i32,
    base: DocIdMergerSubBase,
    ctx: Option<SortedNumericDocValuesContext>,
}

impl SortedNumericDocValuesSub {
    fn new(doc_map: Arc<LiveDocsDocMap>, values: SortedNumericDocValuesRef, max_doc: i32) -> Self {
        let base = DocIdMergerSubBase::new(doc_map);
        SortedNumericDocValuesSub {
            values,
            base,
            max_doc,
            doc_id: -1,
            ctx: None,
        }
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
        to_merge: &[Arc<dyn NumericDocValues>],
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
                    let mut norms: NumericDocValuesRef = Arc::new(EmptyNumericDocValues::default());
                    if let Some(ref norm_producer) = merge_state.norms_producers[i] {
                        if let Some(field_info) =
                            merge_state.fields_infos[i].field_info_by_name(&merge_field_info.name)
                        {
                            if field_info.has_norms() {
                                norms = Arc::from(norm_producer.norms(field_info)?);
                            }
                        }
                    }
                    to_merge.push(norms);
                }
                self.merge_norms_field(merge_field_info, merge_state, &to_merge)?;
            }
        }
        Ok(())
    }
}

struct NormsValuesSub {
    values: NumericDocValuesRef,
    ctx: NumericDocValuesContext,
    doc_id: i32,
    max_doc: i32,
    base: DocIdMergerSubBase,
}

impl NormsValuesSub {
    fn new(doc_map: Arc<LiveDocsDocMap>, values: NumericDocValuesRef, max_doc: i32) -> Self {
        let base = DocIdMergerSubBase::new(doc_map);
        NormsValuesSub {
            values,
            doc_id: -1,
            max_doc,
            base,
            ctx: None,
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
}

struct NormsValuesMergeIter<'a, D: Directory + 'static, C: Codec> {
    merge_state: &'a MergeState<D, C>,
    to_merge: &'a [Arc<dyn NumericDocValues>],
    doc_id_merger: DocIdMergerEnum<NormsValuesSub>,
    next_value: Numeric,
    next_is_set: bool,
}

impl<'a, D: Directory + 'static, C: Codec> NormsValuesMergeIter<'a, D, C> {
    fn new(
        merge_state: &'a MergeState<D, C>,
        to_merge: &'a [Arc<dyn NumericDocValues>],
    ) -> Result<Self> {
        let mut subs = Vec::with_capacity(to_merge.len());
        for (i, dv) in to_merge.iter().enumerate() {
            subs.push(NormsValuesSub::new(
                Arc::clone(&merge_state.doc_maps[i]),
                Arc::clone(dv),
                merge_state.max_docs[i],
            ));
        }
        let doc_id_merger = doc_id_merger_of(subs, merge_state.needs_index_sort)?;
        Ok(NormsValuesMergeIter {
            merge_state,
            to_merge,
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
            let current_ctx = sub.ctx.take();
            let (value, ctx) = sub.values.get_with_ctx(current_ctx, sub.doc_id)?;
            self.next_value = Numeric::Long(value);
            sub.ctx = ctx;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

impl<'a, D: Directory + 'static, C: Codec> Iterator for NormsValuesMergeIter<'a, D, C> {
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

impl<'a, D: Directory + 'static, C: Codec> ReusableIterator for NormsValuesMergeIter<'a, D, C> {
    fn reset(&mut self) {
        let mut subs = Vec::with_capacity(self.to_merge.len());
        for i in 0..self.to_merge.len() {
            subs.push(NormsValuesSub::new(
                Arc::clone(&self.merge_state.doc_maps[i]),
                Arc::clone(&self.to_merge[i]),
                self.merge_state.max_docs[i],
            ));
        }
        let doc_id_merger = doc_id_merger_of(subs, self.merge_state.needs_index_sort).unwrap();
        self.doc_id_merger = doc_id_merger;
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
