use core::index::doc_id_merger::{doc_id_merger_of, DocIdMergerEnum};
use core::index::doc_id_merger::{DocIdMerger, DocIdMergerSub, DocIdMergerSubBase};
use core::index::SortedNumericDocValuesRef;
use core::index::{AcceptStatus, FilteredTermIterBase, FilteredTermIterator, TermIterator};
use core::index::{BinaryDocValuesRef, EmptyBinaryDocValues};
use core::index::{DocValuesType, EmptyNumericDocValues, NumericDocValues, NumericDocValuesRef};
use core::index::{EmptySortedDocValues, SortedDocValues, SortedDocValuesRef};
use core::index::{EmptySortedNumericDocValues, SortedNumericDocValuesContext};
use core::index::{EmptySortedSetDocValues, SortedSetDocValuesRef, NO_MORE_ORDS};
use core::index::{FieldInfo, Fields, MergeState, ReaderSlice};
use core::index::{LiveDocsDocMap, MappedMultiFields, MultiFields};
use core::index::{NumericDocValuesContext, OrdinalMap};
use core::search::NO_MORE_DOCS;
use core::util::bkd::LongBitSet;
use core::util::byte_ref::BytesRef;
use core::util::numeric::Numeric;
use core::util::packed_misc::COMPACT;
use core::util::{BitsRef, LongValues, MatchNoBits};
use core::util::{DocId, ReusableIterator};

use error::Result;

use std::mem;
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
    fn write(&mut self, fields: &Fields) -> Result<()>;

    /// Merges in the fields from the readers in
    /// <code>mergeState</code>. The default implementation skips
    /// and maps around deleted documents, and calls {@link #write(Fields)}.
    /// Implementations can override this method for more sophisticated
    /// merging (bulk-byte copying, etc).
    fn merge(&mut self, merge_state: &mut MergeState) -> Result<()> {
        let mut fields = vec![];
        let mut slices = vec![];

        let mut doc_base = 0;
        for i in 0..merge_state.fields_producers.len() {
            let f = Arc::clone(&merge_state.fields_producers[i]);
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

/// Abstract API that consumes numeric, binary and
/// sorted docvalues.  Concrete implementations of this
/// actually do "something" with the docvalues (write it into
/// the index in a specific format).
pub trait DocValuesConsumer {
    fn add_numeric_field(
        &mut self,
        field_info: &FieldInfo,
        values: &mut ReusableIterator<Item = Result<Numeric>>,
    ) -> Result<()>;
    fn add_binary_field(
        &mut self,
        field_info: &FieldInfo,
        values: &mut ReusableIterator<Item = Result<BytesRef>>,
    ) -> Result<()>;
    fn add_sorted_field(
        &mut self,
        field_info: &FieldInfo,
        values: &mut ReusableIterator<Item = Result<BytesRef>>,
        doc_to_ord: &mut ReusableIterator<Item = Result<Numeric>>,
    ) -> Result<()>;
    fn add_sorted_numeric_field(
        &mut self,
        field_info: &FieldInfo,
        values: &mut ReusableIterator<Item = Result<Numeric>>,
        doc_to_value_count: &mut ReusableIterator<Item = Result<u32>>,
    ) -> Result<()>;
    fn add_sorted_set_field(
        &mut self,
        field_info: &FieldInfo,
        values: &mut ReusableIterator<Item = Result<BytesRef>>,
        doc_to_ord_count: &mut ReusableIterator<Item = Result<u32>>,
        ords: &mut ReusableIterator<Item = Result<Numeric>>,
    ) -> Result<()>;

    fn merge(&mut self, merge_state: &mut MergeState) -> Result<()> {
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

    fn merge_numeric_field(
        &mut self,
        field_info: &FieldInfo,
        merge_state: &MergeState,
        to_merge: &[Arc<NumericDocValues>],
        docs_with_field: &[BitsRef],
    ) -> Result<()> {
        let mut iter = NumericDocValuesMergeIter::new(merge_state, to_merge, docs_with_field)?;
        self.add_numeric_field(field_info, &mut iter)
    }

    fn merge_binary_field(
        &mut self,
        field_info: &FieldInfo,
        merge_state: &MergeState,
        to_merge: &[BinaryDocValuesRef],
        docs_with_field: &[BitsRef],
    ) -> Result<()> {
        let mut iter = BinaryDocValuesMergeIter::new(merge_state, to_merge, docs_with_field)?;
        self.add_binary_field(field_info, &mut iter)
    }

    fn merge_sorted_field(
        &mut self,
        field_info: &FieldInfo,
        merge_state: &MergeState,
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
                live_terms.push(dv.term_iterator()?);
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
                live_terms.push(Box::new(BitsFilteredTermIterator::new(
                    dv.term_iterator()?,
                    bitset,
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

    fn merge_sorted_set_field(
        &mut self,
        field_info: &FieldInfo,
        merge_state: &MergeState,
        to_merge: &[SortedSetDocValuesRef],
    ) -> Result<()> {
        let num_readers = to_merge.len();
        // step 1: iterate thru each sub and mark terms still in use
        let mut live_terms: Vec<Box<TermIterator>> = Vec::with_capacity(num_readers);
        let mut weights = Vec::with_capacity(num_readers);
        for i in 0..num_readers {
            let dv = &to_merge[i];
            let max_doc = merge_state.max_docs[i];
            let live_docs = &merge_state.live_docs[i];
            if live_docs.len() == 0 {
                live_terms.push(dv.term_iterator()?);
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
                live_terms.push(Box::new(BitsFilteredTermIterator::new(
                    dv.term_iterator()?,
                    bitset,
                )));
            }
        }

        // step 2: create ordinal map (this conceptually does the "merging")
        let map = OrdinalMap::build(live_terms, weights, COMPACT)?;

        // step 3: add field
        let mut values_iter = SortedSetDocValuesMergeBytesIter::new(&map, to_merge);
        let mut ord_counts_iter = SortedSetDocValuesOrdCountIter::new(to_merge, merge_state, &map)?;
        let mut ords_iter = SortedSetDocValuesMergeOrdIter::new(to_merge, merge_state, &map)?;

        self.add_sorted_set_field(
            field_info,
            &mut values_iter,
            &mut ord_counts_iter,
            &mut ords_iter,
        )
    }

    fn merge_sorted_numeric_field(
        &mut self,
        field_info: &FieldInfo,
        merge_state: &MergeState,
        to_merge: &[SortedNumericDocValuesRef],
    ) -> Result<()> {
        let mut values_iter = SortedNumericDocValuesIter::new(to_merge, merge_state)?;
        let mut doc_counts_iter = SortedNumericDocValuesCountIter::new(to_merge, merge_state)?;

        self.add_sorted_numeric_field(field_info, &mut values_iter, &mut doc_counts_iter)
    }
}

// Helper: returns true if the given docToValue count contains only at most one value
pub fn is_single_valued(
    doc_to_value_count: &mut ReusableIterator<Item = Result<u32>>,
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

pub fn singleton_view<'a>(
    doc_to_value_count: &'a mut ReusableIterator<Item = Result<u32>>,
    values: &'a mut ReusableIterator<Item = Result<Numeric>>,
    missing_value: Numeric,
) -> SingletonViewIter<'a> {
    debug_assert!(is_single_valued(doc_to_value_count).unwrap());
    // doc_to_value_count.reset();
    // debug_assert!(doc_to_value_count.len() == values.len());

    SingletonViewIter::new(doc_to_value_count, values, missing_value)
}

pub struct SingletonViewIter<'a> {
    doc_to_value_count: &'a mut ReusableIterator<Item = Result<u32>>,
    values: &'a mut ReusableIterator<Item = Result<Numeric>>,
    missing_value: Numeric,
}

impl<'a> SingletonViewIter<'a> {
    fn new(
        doc_to_value_count: &'a mut ReusableIterator<Item = Result<u32>>,
        values: &'a mut ReusableIterator<Item = Result<Numeric>>,
        missing_value: Numeric,
    ) -> Self {
        SingletonViewIter {
            doc_to_value_count,
            values,
            missing_value,
        }
    }
}

impl<'a> ReusableIterator for SingletonViewIter<'a> {
    fn reset(&mut self) {
        self.doc_to_value_count.reset();
        self.values.reset();
    }
}

impl<'a> Iterator for SingletonViewIter<'a> {
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

struct BitsFilteredTermIterator {
    base: FilteredTermIterBase,
    live_terms: LongBitSet,
}

impl BitsFilteredTermIterator {
    fn new(terms: Box<TermIterator>, live_terms: LongBitSet) -> Self {
        let base = FilteredTermIterBase::new(terms, false);
        BitsFilteredTermIterator { base, live_terms }
    }
}

impl FilteredTermIterator for BitsFilteredTermIterator {
    fn base(&self) -> &FilteredTermIterBase {
        &self.base
    }

    fn base_mut(&mut self) -> &mut FilteredTermIterBase {
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

struct NumericDocValuesMergeIter<'a> {
    merge_state: &'a MergeState,
    to_merge: &'a [Arc<NumericDocValues>],
    docs_with_field: &'a [BitsRef],
    doc_id_merger: DocIdMergerEnum<NumericDocValuesSub>,
    next_value: Numeric,
    next_is_set: bool,
}

impl<'a> NumericDocValuesMergeIter<'a> {
    fn new(
        merge_state: &'a MergeState,
        to_merge: &'a [Arc<NumericDocValues>],
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
            let ctx = sub.ctx.take();
            let (next_value, ctx) = sub.values.get_with_ctx(ctx, sub.doc_id)?;
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

impl<'a> Iterator for NumericDocValuesMergeIter<'a> {
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

impl<'a> ReusableIterator for NumericDocValuesMergeIter<'a> {
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

struct BinaryDocValuesMergeIter<'a> {
    merge_state: &'a MergeState,
    to_merge: &'a [BinaryDocValuesRef],
    docs_with_field: &'a [BitsRef],
    doc_id_merger: DocIdMergerEnum<BinaryDocValuesSub>,
    next_value: Vec<u8>,
    next_ref: BytesRef,
    next_is_set: bool,
}

impl<'a> BinaryDocValuesMergeIter<'a> {
    fn new(
        merge_state: &'a MergeState,
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

impl<'a> Iterator for BinaryDocValuesMergeIter<'a> {
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

impl<'a> ReusableIterator for BinaryDocValuesMergeIter<'a> {
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

struct SortedDocValuesMergeOrdIter<'a> {
    to_merge: &'a [SortedDocValuesRef],
    merge_state: &'a MergeState,
    map: &'a OrdinalMap,
    doc_id_merger: DocIdMergerEnum<SortedDocValuesSub>,
    next_value: Numeric,
    next_is_set: bool,
}

impl<'a> SortedDocValuesMergeOrdIter<'a> {
    fn new(
        to_merge: &'a [SortedDocValuesRef],
        merge_state: &'a MergeState,
        map: &'a OrdinalMap,
    ) -> Result<Self> {
        let mut subs = Vec::with_capacity(to_merge.len());
        for i in 0..to_merge.len() {
            subs.push(SortedDocValuesSub::new(
                Arc::clone(&merge_state.doc_maps[i]),
                Arc::clone(&to_merge[i]),
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

impl<'a> Iterator for SortedDocValuesMergeOrdIter<'a> {
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

impl<'a> ReusableIterator for SortedDocValuesMergeOrdIter<'a> {
    fn reset(&mut self) {
        let mut subs = Vec::with_capacity(self.to_merge.len());
        for i in 0..self.to_merge.len() {
            let sub = SortedDocValuesSub::new(
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
    }
}

struct SortedDocValuesSub {
    values: SortedDocValuesRef,
    doc_id: DocId,
    max_doc: i32,
    map: Rc<LongValues>,
    base: DocIdMergerSubBase,
}

impl SortedDocValuesSub {
    fn new(
        doc_map: Arc<LiveDocsDocMap>,
        values: SortedDocValuesRef,
        max_doc: i32,
        map: Rc<LongValues>,
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

struct SortedSetDocValuesOrdCountIter<'a> {
    to_merge: &'a [SortedSetDocValuesRef],
    merge_state: &'a MergeState,
    map: &'a OrdinalMap,
    doc_id_merger: DocIdMergerEnum<SortedSetDocValuesSub>,
    next_value: u32,
    next_is_set: bool,
}

impl<'a> SortedSetDocValuesOrdCountIter<'a> {
    fn new(
        to_merge: &'a [SortedSetDocValuesRef],
        merge_state: &'a MergeState,
        map: &'a OrdinalMap,
    ) -> Result<Self> {
        let mut subs = Vec::with_capacity(to_merge.len());
        for i in 0..to_merge.len() {
            subs.push(SortedSetDocValuesSub::new(
                Arc::clone(&merge_state.doc_maps[i]),
                Arc::clone(&to_merge[i]),
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

impl<'a> Iterator for SortedSetDocValuesOrdCountIter<'a> {
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

impl<'a> ReusableIterator for SortedSetDocValuesOrdCountIter<'a> {
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

struct SortedSetDocValuesMergeOrdIter<'a> {
    to_merge: &'a [SortedSetDocValuesRef],
    merge_state: &'a MergeState,
    map: &'a OrdinalMap,
    doc_id_merger: DocIdMergerEnum<SortedSetDocValuesSub>,
    next_value: Numeric,
    next_is_set: bool,
    ords: Vec<i64>,
    ord_upto: usize,
    ord_length: usize,
}

impl<'a> SortedSetDocValuesMergeOrdIter<'a> {
    fn new(
        to_merge: &'a [SortedSetDocValuesRef],
        merge_state: &'a MergeState,
        map: &'a OrdinalMap,
    ) -> Result<Self> {
        let mut subs = Vec::with_capacity(to_merge.len());
        for i in 0..to_merge.len() {
            subs.push(SortedSetDocValuesSub::new(
                Arc::clone(&merge_state.doc_maps[i]),
                Arc::clone(&to_merge[i]),
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

impl<'a> Iterator for SortedSetDocValuesMergeOrdIter<'a> {
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

impl<'a> ReusableIterator for SortedSetDocValuesMergeOrdIter<'a> {
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
    map: Rc<LongValues>,
    base: DocIdMergerSubBase,
}

impl SortedSetDocValuesSub {
    fn new(
        doc_map: Arc<LiveDocsDocMap>,
        values: SortedSetDocValuesRef,
        max_doc: i32,
        map: Rc<LongValues>,
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

struct SortedNumericDocValuesCountIter<'a> {
    to_merge: &'a [SortedNumericDocValuesRef],
    merge_state: &'a MergeState,
    doc_id_merger: DocIdMergerEnum<SortedNumericDocValuesSub>,
    next_value: u32,
    next_is_set: bool,
}

impl<'a> SortedNumericDocValuesCountIter<'a> {
    fn new(to_merge: &'a [SortedNumericDocValuesRef], merge_state: &'a MergeState) -> Result<Self> {
        let mut subs = Vec::with_capacity(to_merge.len());
        for i in 0..to_merge.len() {
            subs.push(SortedNumericDocValuesSub::new(
                Arc::clone(&merge_state.doc_maps[i]),
                Arc::clone(&to_merge[i]),
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
            let mut ctx = sub.ctx.take();
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

impl<'a> Iterator for SortedNumericDocValuesCountIter<'a> {
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

impl<'a> ReusableIterator for SortedNumericDocValuesCountIter<'a> {
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

struct SortedNumericDocValuesIter<'a> {
    to_merge: &'a [SortedNumericDocValuesRef],
    merge_state: &'a MergeState,
    doc_id_merger: DocIdMergerEnum<SortedNumericDocValuesSub>,
    next_value: Numeric,
    next_is_set: bool,
    value_upto: usize,
    value_length: usize,
    current: *mut SortedNumericDocValuesSub,
}

impl<'a> SortedNumericDocValuesIter<'a> {
    fn new(to_merge: &'a [SortedNumericDocValuesRef], merge_state: &'a MergeState) -> Result<Self> {
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

impl<'a> Iterator for SortedNumericDocValuesIter<'a> {
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

impl<'a> ReusableIterator for SortedNumericDocValuesIter<'a> {
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

pub trait NormsConsumer: Drop {
    fn add_norms_field(
        &mut self,
        field_info: &FieldInfo,
        values: &mut ReusableIterator<Item = Result<Numeric>>,
    ) -> Result<()>;

    fn merge_norms_field(
        &mut self,
        field_info: &FieldInfo,
        merge_state: &MergeState,
        to_merge: &[Arc<NumericDocValues>],
    ) -> Result<()> {
        let mut iter = NormsValuesMergeIter::new(merge_state, to_merge)?;
        self.add_norms_field(field_info, &mut iter)
    }

    fn merge(&mut self, merge_state: &mut MergeState) -> Result<()> {
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
                    let norms: Arc<NumericDocValues> =
                        if let Some(ref norm_producer) = merge_state.norms_producers[i] {
                            if let Some(field_info) = merge_state.fields_infos[i]
                                .field_info_by_name(&merge_field_info.name)
                            {
                                if field_info.has_norms() {
                                    Arc::from(norm_producer.norms(field_info)?)
                                } else {
                                    Arc::new(EmptyNumericDocValues {})
                                }
                            } else {
                                Arc::new(EmptyNumericDocValues {})
                            }
                        } else {
                            Arc::new(EmptyNumericDocValues {})
                        };
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

struct NormsValuesMergeIter<'a> {
    merge_state: &'a MergeState,
    to_merge: &'a [Arc<NumericDocValues>],
    doc_id_merger: DocIdMergerEnum<NormsValuesSub>,
    next_value: Numeric,
    next_is_set: bool,
}

impl<'a> NormsValuesMergeIter<'a> {
    fn new(merge_state: &'a MergeState, to_merge: &'a [Arc<NumericDocValues>]) -> Result<Self> {
        let mut subs = Vec::with_capacity(to_merge.len());
        for i in 0..to_merge.len() {
            subs.push(NormsValuesSub::new(
                Arc::clone(&merge_state.doc_maps[i]),
                Arc::clone(&to_merge[i]),
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
            self.next_value = Numeric::Long(sub.values.get(sub.doc_id)?);
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

impl<'a> Iterator for NormsValuesMergeIter<'a> {
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

impl<'a> ReusableIterator for NormsValuesMergeIter<'a> {
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
