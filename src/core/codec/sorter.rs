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

use core::codec::doc_values::NumericDocValues;
use core::codec::Codec;
use core::index::merge::{LiveDocsDocMap, ReaderWrapperEnum};
use core::index::reader::{LeafReader, LeafReaderContext};
use core::search::sort_field::Sort;
use core::search::sort_field::{ComparatorValue, FieldComparator, FieldComparatorEnum};
use core::search::sort_field::{SortField, SortFieldType, SortedNumericSelector};
use core::util::packed::COMPACT;
use core::util::packed::{
    PackedLongValues, PackedLongValuesBuilder, PackedLongValuesBuilderType, DEFAULT_PAGE_SIZE,
};
use core::util::{BitsMut, BitsRef, DocId};

use error::ErrorKind::IllegalArgument;
use error::Result;

use core::store::directory::Directory;
use std::cmp::Ordering;
use std::collections::BinaryHeap;

/// Sorts documents of a given index by returning a permutation
/// on the document IDs.
pub struct Sorter {
    sort: Sort,
}

impl Sorter {
    pub fn new(sort: Sort) -> Self {
        debug_assert!(!sort.needs_scores());
        Sorter { sort }
    }

    pub fn sort_field_type(sort: &SortField) -> SortFieldType {
        match sort {
            SortField::Simple(s) => s.field_type(),
            SortField::SortedNumeric(s) => s.numeric_type(),
        }
    }

    #[allow(dead_code)]
    /// Check consistency of a `SorterDocMap`, useful for assertions.
    fn is_consistent(doc_map: &dyn SorterDocMap) -> bool {
        let max_doc = doc_map.len() as i32;
        for i in 0..max_doc {
            let new_id = doc_map.old_to_new(i);
            let old_id = doc_map.new_to_old(new_id);
            assert!(new_id >= 0 && new_id < max_doc);
            assert_eq!(i, old_id);
        }
        true
    }

    /// Computes the old-to-new permutation over the given comparator.
    fn sort(
        max_doc: DocId,
        comparator: &mut impl SorterDocComparator,
    ) -> Result<Option<PackedLongDocMap>> {
        debug_assert!(max_doc > 0);
        // check if the index is sorted
        let mut sorted = true;
        for i in 1..max_doc {
            if comparator.compare(i - 1, i)? == Ordering::Greater {
                sorted = false;
                break;
            }
        }

        if sorted {
            return Ok(None);
        }

        // sort doc IDs
        let mut docs = vec![0i32; max_doc as usize];
        for i in 0..max_doc {
            docs[i as usize] = i;
        }

        let mut sort_res = Ok(());
        docs.sort_by(|doc1, doc2| match comparator.compare(*doc1, *doc2) {
            Err(e) => {
                sort_res = Err(e);
                Ordering::Equal
            }
            Ok(o) => o,
        });
        if let Err(e) = sort_res {
            return Err(e);
        }

        // The reason why we use MonotonicAppendingLongBuffer here is that it
        // wastes very little memory if the index is in random order but can save
        // a lot of memory if the index is already "almost" sorted
        let mut new_to_old_builder = PackedLongValuesBuilder::new(
            DEFAULT_PAGE_SIZE,
            COMPACT,
            PackedLongValuesBuilderType::Monotonic,
        );
        for doc in docs.iter().take(max_doc as usize) {
            new_to_old_builder.add(*doc as i64);
        }
        // NOTE: the #build method contain reference, but the builder will move after return,
        // so we won't use the build result
        let new_to_old = new_to_old_builder.build();

        // invert the docs mapping
        for i in 0..max_doc {
            docs[new_to_old.get(i)? as usize] = i;
        } // docs is now the old_to_new mapping

        let mut old_to_new_builder = PackedLongValuesBuilder::new(
            DEFAULT_PAGE_SIZE,
            COMPACT,
            PackedLongValuesBuilderType::Monotonic,
        );
        for doc in docs.iter().take(max_doc as usize) {
            old_to_new_builder.add(*doc as i64);
        }
        // NOTE: the #build method contain reference, but the builder will move after return,
        // so we won't use the build result
        let old_to_new = old_to_new_builder.build();

        Ok(Some(PackedLongDocMap {
            max_doc: max_doc as usize,
            old_to_new,
            new_to_old,
        }))
    }

    pub fn sort_by_comps(
        max_doc: i32,
        comparators: Vec<Box<dyn SorterDocComparator>>,
    ) -> Result<Option<PackedLongDocMap>> {
        let mut multi_cmp = MultiSorterDocComps { cmps: comparators };
        Self::sort(max_doc, &mut multi_cmp)
    }

    /// Returns a mapping from the old document ID to its new location in the
    /// sorted index. Implementations can use the auxiliary
    /// {@link #sort(int, DocComparator)} to compute the old-to-new permutation
    /// given a list of documents and their corresponding values.
    ///
    /// A return value of `None` is allowed and means that
    /// <code>reader</code> is already sorted.
    ///
    /// NOTE: deleted documents are expected to appear in the mapping as
    /// well, they will however be marked as deleted in the sorted view.
    pub fn sort_leaf_reader<C: Codec>(
        &self,
        reader: &LeafReaderContext<'_, C>,
    ) -> Result<Option<PackedLongDocMap>> {
        let fields = self.sort.get_sort();
        let mut reverses = Vec::with_capacity(fields.len());
        let mut comparators = Vec::with_capacity(fields.len());
        for field in fields {
            reverses.push(field.is_reverse());
            let mut comparator = field.get_comparator(1, None);
            comparator.get_information_from_reader(reader)?;
            comparators.push(comparator);
        }
        let mut comparator = SortFieldsDocComparator {
            comparators,
            reverses,
        };
        Self::sort(reader.reader.max_doc(), &mut comparator)
    }

    pub fn get_or_wrap_numeric<R: LeafReader + ?Sized>(
        reader: &R,
        sort_field: &SortField,
    ) -> Result<Box<dyn NumericDocValues>> {
        match sort_field {
            SortField::SortedNumeric(s) => SortedNumericSelector::wrap(
                reader.get_sorted_numeric_doc_values(sort_field.field())?,
                s.selector(),
                s.numeric_type(),
            ),
            _ => reader.get_numeric_doc_values(sort_field.field()),
        }
    }
}

struct MultiSorterDocComps {
    cmps: Vec<Box<dyn SorterDocComparator>>,
}

impl SorterDocComparator for MultiSorterDocComps {
    fn compare(&mut self, doc1: i32, doc2: i32) -> Result<Ordering> {
        for cmp in &mut self.cmps {
            let res = cmp.compare(doc1, doc2)?;
            if res != Ordering::Equal {
                return Ok(res);
            }
        }
        Ok(doc1.cmp(&doc2))
    }
}

pub struct PackedLongDocMap {
    max_doc: usize,
    old_to_new: PackedLongValues,
    new_to_old: PackedLongValues,
}

impl SorterDocMap for PackedLongDocMap {
    fn old_to_new(&self, doc_id: DocId) -> DocId {
        self.old_to_new.get(doc_id).unwrap() as DocId
    }

    fn new_to_old(&self, doc_id: i32) -> i32 {
        self.new_to_old.get(doc_id).unwrap() as DocId
    }

    fn len(&self) -> usize {
        self.max_doc
    }
}

/// A permutation of doc IDs. For every document ID between <tt>0</tt> and
/// `IndexReader#max_doc()`, `old_to_new(new_to_old(doc_id))` must
/// return `doc_id`
pub trait SorterDocMap {
    /// Given a doc ID from the original index, return its ordinal in the
    /// sorted index
    fn old_to_new(&self, doc_id: DocId) -> DocId;

    /// Given the ordinal of a doc ID, return its doc ID in the original index.
    fn new_to_old(&self, doc_id: DocId) -> DocId;

    /// Return the number of documents in this map. This must be equal to the
    /// `LeafReader#max_doc()` number of documents of the `LeafReader` which
    /// is sorted.
    fn len(&self) -> usize;
}

/// a comparator of doc IDs
pub trait SorterDocComparator {
    fn compare(&mut self, doc1: DocId, doc2: DocId) -> Result<Ordering>;
}

struct SortFieldsDocComparator {
    comparators: Vec<FieldComparatorEnum>,
    reverses: Vec<bool>,
}

impl SorterDocComparator for SortFieldsDocComparator {
    fn compare(&mut self, doc1: i32, doc2: i32) -> Result<Ordering> {
        for i in 0..self.comparators.len() {
            // TODO: would be better if copy() didnt cause a term lookup in TermOrdVal & co,
            // the segments are always the same here...
            self.comparators[i].copy(0, ComparatorValue::Doc(doc1))?;
            self.comparators[i].set_bottom(0);
            let mut comp = self.comparators[i].compare_bottom(ComparatorValue::Doc(doc2))?;
            if self.reverses[i] {
                comp = comp.reverse();
            }
            if comp != Ordering::Equal {
                return Ok(comp);
            }
        }
        Ok(doc1.cmp(&doc2))
    }
}

pub struct MultiSorter;

impl MultiSorter {
    /// Does a merge sort of the leaves of the incoming reader, returning `DocMap`
    /// to map each leaf's documents into the merged segment.  The documents for
    /// each incoming leaf reader must already be sorted by the same sort!
    /// Returns null if the merge sort is not needed (segments are already in index sort order).
    pub fn sort<D: Directory, C: Codec>(
        sort: &Sort,
        readers: &[ReaderWrapperEnum<D, C>],
    ) -> Result<Vec<LiveDocsDocMap>> {
        let fields = sort.get_sort();

        let mut comparators = Vec::with_capacity(fields.len());
        for field in fields {
            comparators.push(Self::get_comparator(readers, field)?);
        }

        let leaf_count = readers.len();

        let mut queue = BinaryHeap::with_capacity(leaf_count);
        let mut builders = Vec::with_capacity(leaf_count);

        for (i, reader) in readers.iter().enumerate().take(leaf_count) {
            queue.push(LeafAndDocId::new(
                i,
                reader.live_docs(),
                reader.max_doc(),
                &mut comparators,
            ));
            builders.push(PackedLongValuesBuilder::new(
                DEFAULT_PAGE_SIZE,
                COMPACT,
                PackedLongValuesBuilderType::Monotonic,
            ));
        }

        let mut mapped_doc_id = 0;
        let mut last_reader_index = 0;
        let mut sorted = true;
        loop {
            let mut tmp = None;
            {
                if let Some(mut top) = queue.pop() {
                    if last_reader_index > top.reader_index {
                        // merge sort is needed
                        sorted = false;
                    }
                    last_reader_index = top.reader_index;
                    builders[last_reader_index].add(mapped_doc_id);
                    if top.live_docs.get(top.doc_id as usize)? {
                        mapped_doc_id += 1;
                    }
                    top.doc_id += 1;
                    if top.doc_id < top.max_doc {
                        tmp = Some(top);
                    }
                } else {
                    break;
                }
            }
            if tmp.is_some() {
                queue.push(tmp.unwrap());
            }
        }

        if sorted {
            return Ok(Vec::with_capacity(0));
        }

        let mut i = 0;
        let mut doc_maps = Vec::with_capacity(leaf_count);
        for mut builder in builders {
            let values = builder.build();
            let live_docs = readers[i].live_docs();
            doc_maps.push(LiveDocsDocMap::new(live_docs, values, 0));
            i += 1;
        }

        Ok(doc_maps)
    }

    /// Returns {@code CrossReaderComparator} for the provided readers to represent
    /// the requested {@link SortField} sort order.
    fn get_comparator<D: Directory, C: Codec>(
        readers: &[ReaderWrapperEnum<D, C>],
        sort_field: &SortField,
    ) -> Result<CrossReaderComparatorEnum> {
        let reverse = sort_field.is_reverse();
        let field_type = Sorter::sort_field_type(sort_field);
        match field_type {
            SortFieldType::String => unimplemented!(),
            SortFieldType::Long | SortFieldType::Int => {
                let mut values = Vec::with_capacity(readers.len());
                let mut docs_with_fields = Vec::with_capacity(readers.len());
                for reader in readers {
                    values.push(Sorter::get_or_wrap_numeric(reader, sort_field)?);
                    docs_with_fields.push(reader.get_docs_with_field(sort_field.field())?);
                }
                let missing_value = if let Some(missing) = sort_field.missing_value() {
                    if field_type == SortFieldType::Long {
                        missing.get_long().unwrap()
                    } else if field_type == SortFieldType::Int {
                        missing.get_int().unwrap() as i64
                    } else {
                        unreachable!()
                    }
                } else {
                    0
                };
                Ok(CrossReaderComparatorEnum::Long(
                    LongCrossReaderComparator::new(
                        docs_with_fields,
                        values,
                        missing_value,
                        reverse,
                    ),
                ))
            }
            SortFieldType::Double | SortFieldType::Float => {
                let mut values = Vec::with_capacity(readers.len());
                let mut docs_with_fields = Vec::with_capacity(readers.len());
                for reader in readers {
                    values.push(Sorter::get_or_wrap_numeric(reader, sort_field)?);
                    docs_with_fields.push(reader.get_docs_with_field(sort_field.field())?);
                }
                let missing_value = if let Some(missing) = sort_field.missing_value() {
                    if field_type == SortFieldType::Double {
                        missing.get_double().unwrap()
                    } else if field_type == SortFieldType::Float {
                        missing.get_float().unwrap() as f64
                    } else {
                        unreachable!()
                    }
                } else {
                    0.0
                };
                Ok(CrossReaderComparatorEnum::Double(
                    DoubleCrossReaderComparator::new(
                        docs_with_fields,
                        values,
                        missing_value,
                        reverse,
                    ),
                ))
            }
            _ => bail!(IllegalArgument(format!(
                "unhandled SortField.getType()={:?}",
                field_type
            ))),
        }
    }
}

struct LeafAndDocId {
    reader_index: usize,
    live_docs: BitsRef,
    max_doc: i32,
    doc_id: DocId,
    comparators: *mut [CrossReaderComparatorEnum],
}

impl LeafAndDocId {
    fn new(
        reader_index: usize,
        live_docs: BitsRef,
        max_doc: i32,
        comparators: &mut [CrossReaderComparatorEnum],
    ) -> Self {
        LeafAndDocId {
            reader_index,
            live_docs,
            max_doc,
            comparators,
            doc_id: 0,
        }
    }

    #[allow(clippy::mut_from_ref)]
    fn comparators(&self) -> &mut [CrossReaderComparatorEnum] {
        unsafe { &mut *self.comparators }
    }
}

impl Eq for LeafAndDocId {}

impl PartialEq for LeafAndDocId {
    fn eq(&self, other: &LeafAndDocId) -> bool {
        self.reader_index == other.reader_index && self.doc_id == other.doc_id
    }
}

impl Ord for LeafAndDocId {
    // reverse ord for BinaryHeap
    fn cmp(&self, other: &Self) -> Ordering {
        for comparator in self.comparators() {
            let cmp = comparator
                .compare(
                    other.reader_index,
                    other.doc_id,
                    self.reader_index,
                    self.doc_id,
                )
                .unwrap();
            if cmp != Ordering::Equal {
                return cmp.reverse();
            }
        }
        // tie-break by doc_id natural order:
        if self.reader_index != other.reader_index {
            self.reader_index.cmp(&other.reader_index)
        } else {
            self.doc_id.cmp(&other.doc_id)
        }
    }
}

impl PartialOrd for LeafAndDocId {
    fn partial_cmp(&self, other: &LeafAndDocId) -> Option<Ordering> {
        Some(other.cmp(self))
    }
}

enum CrossReaderComparatorEnum {
    Long(LongCrossReaderComparator),
    Double(DoubleCrossReaderComparator),
}

impl CrossReaderComparator for CrossReaderComparatorEnum {
    fn compare(
        &mut self,
        reader_index1: usize,
        doc_id1: DocId,
        reader_index2: usize,
        doc_id2: DocId,
    ) -> Result<Ordering> {
        match self {
            CrossReaderComparatorEnum::Long(l) => {
                l.compare(reader_index1, doc_id1, reader_index2, doc_id2)
            }
            CrossReaderComparatorEnum::Double(d) => {
                d.compare(reader_index1, doc_id1, reader_index2, doc_id2)
            }
        }
    }
}

trait CrossReaderComparator {
    fn compare(
        &mut self,
        reader_index1: usize,
        doc_id1: DocId,
        reader_index2: usize,
        doc_id2: DocId,
    ) -> Result<Ordering>;
}

struct LongCrossReaderComparator {
    docs_with_fields: Vec<Box<dyn BitsMut>>,
    values: Vec<Box<dyn NumericDocValues>>,
    missing_value: i64,
    reverse: bool,
}

impl LongCrossReaderComparator {
    fn new(
        docs_with_fields: Vec<Box<dyn BitsMut>>,
        values: Vec<Box<dyn NumericDocValues>>,
        missing_value: i64,
        reverse: bool,
    ) -> Self {
        LongCrossReaderComparator {
            docs_with_fields,
            values,
            missing_value,
            reverse,
        }
    }
}

impl CrossReaderComparator for LongCrossReaderComparator {
    fn compare(
        &mut self,
        idx1: usize,
        doc_id1: DocId,
        idx2: usize,
        doc_id2: DocId,
    ) -> Result<Ordering> {
        let value1 = if self.docs_with_fields[idx1].get(doc_id1 as usize)? {
            self.values[idx1].get_mut(doc_id1)?
        } else {
            self.missing_value
        };
        let value2 = if self.docs_with_fields[idx2].get(doc_id2 as usize)? {
            self.values[idx2].get_mut(doc_id2)?
        } else {
            self.missing_value
        };
        let res = value1.cmp(&value2);
        if self.reverse {
            Ok(res.reverse())
        } else {
            Ok(res)
        }
    }
}

struct DoubleCrossReaderComparator {
    docs_with_fields: Vec<Box<dyn BitsMut>>,
    values: Vec<Box<dyn NumericDocValues>>,
    missing_value: f64,
    reverse: bool,
}

impl DoubleCrossReaderComparator {
    fn new(
        docs_with_fields: Vec<Box<dyn BitsMut>>,
        values: Vec<Box<dyn NumericDocValues>>,
        missing_value: f64,
        reverse: bool,
    ) -> Self {
        DoubleCrossReaderComparator {
            docs_with_fields,
            values,
            missing_value,
            reverse,
        }
    }
}

impl CrossReaderComparator for DoubleCrossReaderComparator {
    fn compare(
        &mut self,
        idx1: usize,
        doc_id1: DocId,
        idx2: usize,
        doc_id2: DocId,
    ) -> Result<Ordering> {
        let value1 = if self.docs_with_fields[idx1].get(doc_id1 as usize)? {
            f64::from_bits(self.values[idx1].get_mut(doc_id1)? as u64)
        } else {
            self.missing_value
        };
        let value2 = if self.docs_with_fields[idx2].get(doc_id2 as usize)? {
            f64::from_bits(self.values[idx2].get_mut(doc_id2)? as u64)
        } else {
            self.missing_value
        };

        // NaN
        if let Some(res) = value1.partial_cmp(&value2) {
            if self.reverse {
                Ok(res.reverse())
            } else {
                Ok(res)
            }
        } else {
            Ok(Ordering::Equal)
        }
    }
}

pub struct DVSortDocComparator<T> {
    data: Vec<T>,
    cmp_fn: fn(d1: &T, d2: &T) -> Ordering,
}

impl<T> DVSortDocComparator<T> {
    pub fn new(data: Vec<T>, cmp_fn: fn(d1: &T, d2: &T) -> Ordering) -> Self {
        Self { data, cmp_fn }
    }
}

impl<T> SorterDocComparator for DVSortDocComparator<T> {
    fn compare(&mut self, doc1: i32, doc2: i32) -> Result<Ordering> {
        Ok((self.cmp_fn)(
            &self.data[doc1 as usize],
            &self.data[doc2 as usize],
        ))
    }
}
