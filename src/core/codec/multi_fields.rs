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

use core::codec::postings::FieldsProducer;
use core::codec::PostingIterator;
use core::codec::{Codec, CodecFieldsProducer, Fields, SeekStatus, TermIterator, Terms};
use core::codec::{MultiPostingIterEnum, MultiTermIteratorEnum, MultiTerms};
use core::index::merge::MergeState;
use core::index::merge::{doc_id_merger_of_count, DocIdMergerSubBase};
use core::index::merge::{DocIdMerger, DocIdMergerEnum, DocIdMergerSub};
use core::index::merge::{LiveDocsDocMap, MergeFieldsProducer};
use core::index::reader::{IndexReader, ReaderSlice};
use core::index::writer::INDEX_MAX_POSITION;
use core::search::{DocIterator, Payload, NO_MORE_DOCS};
use core::store::directory::Directory;
use core::util::DocId;

use error::ErrorKind::{CorruptIndex, UnsupportedOperation};
use error::Result;

use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::mem;
use std::ptr;
use std::sync::Arc;

pub struct MultiFields<T: FieldsProducer> {
    subs: Vec<T>,
    #[allow(dead_code)]
    sub_slices: Vec<ReaderSlice>,
    terms: RefCell<HashMap<String, Arc<MultiTerms<T::Terms>>>>,
}

impl<T> MultiFields<T>
where
    T: FieldsProducer,
{
    pub fn new(subs: Vec<T>, sub_slices: Vec<ReaderSlice>) -> MultiFields<T> {
        MultiFields {
            subs,
            sub_slices,
            terms: RefCell::new(HashMap::new()),
        }
    }
}

impl<T> Fields for MultiFields<T>
where
    T: FieldsProducer,
{
    type Terms = Arc<MultiTerms<T::Terms>>;
    fn fields(&self) -> Vec<String> {
        let mut res = vec![];
        for sub in &self.subs {
            res.extend(sub.fields());
        }
        res.sort();
        res.dedup();
        res
    }

    fn terms(&self, field: &str) -> Result<Option<Self::Terms>> {
        if let Some(res) = self.terms.borrow().get(field) {
            return Ok(Some(res.clone()));
        }

        // TODO cache terms by field
        let mut subs2 = Vec::new();
        let mut slices2 = Vec::new();

        for i in 0..self.subs.len() {
            if let Some(terms) = self.subs[i].terms(field)? {
                subs2.push(terms);
                slices2.push(self.sub_slices[i]);
            }
        }

        let terms = Arc::new(MultiTerms::new(subs2, slices2)?);
        self.terms
            .borrow_mut()
            .insert(field.to_string(), Arc::clone(&terms));
        Ok(Some(terms))
    }

    fn size(&self) -> usize {
        1 as usize
    }
}

impl<T> FieldsProducer for MultiFields<T>
where
    T: FieldsProducer,
{
    fn check_integrity(&self) -> Result<()> {
        unimplemented!();
    }
}

fn fields<C: Codec, IR: IndexReader<Codec = C> + ?Sized>(
    reader: &IR,
) -> Result<FieldsEnum<CodecFieldsProducer<C>>> {
    let leaves = reader.leaves();

    if leaves.len() == 1 {
        Ok(FieldsEnum::Raw(leaves[0].reader.fields()?))
    } else {
        let mut fields = Vec::with_capacity(leaves.len());
        let mut slices: Vec<ReaderSlice> = Vec::with_capacity(leaves.len());
        for leaf in leaves {
            fields.push(leaf.reader.fields()?);
            slices.push(ReaderSlice::new(
                leaf.doc_base(),
                reader.max_doc(),
                fields.len() - 1,
            ));
        }
        Ok(FieldsEnum::Multi(MultiFields::new(fields, slices)))
    }
}

pub fn get_terms<C: Codec, IR: IndexReader<Codec = C> + ?Sized>(
    reader: &IR,
    field: &str,
) -> Result<Option<TermsEnum<<CodecFieldsProducer<C> as Fields>::Terms>>> {
    fields(reader)?.terms(field)
}

enum FieldsEnum<T: FieldsProducer> {
    Raw(T),
    Multi(MultiFields<T>),
}

impl<T: FieldsProducer> Fields for FieldsEnum<T> {
    type Terms = TermsEnum<T::Terms>;
    fn fields(&self) -> Vec<String> {
        match self {
            FieldsEnum::Raw(f) => f.fields(),
            FieldsEnum::Multi(f) => f.fields(),
        }
    }

    fn terms(&self, field: &str) -> Result<Option<Self::Terms>> {
        match self {
            FieldsEnum::Raw(f) => {
                if let Some(terms) = f.terms(field)? {
                    Ok(Some(TermsEnum::Raw(terms)))
                } else {
                    Ok(None)
                }
            }
            FieldsEnum::Multi(f) => {
                if let Some(terms) = f.terms(field)? {
                    Ok(Some(TermsEnum::Multi(terms)))
                } else {
                    Ok(None)
                }
            }
        }
    }

    fn size(&self) -> usize {
        match self {
            FieldsEnum::Raw(f) => f.size(),
            FieldsEnum::Multi(f) => f.size(),
        }
    }

    fn terms_freq(&self, field: &str) -> usize {
        match self {
            FieldsEnum::Raw(f) => f.terms_freq(field),
            FieldsEnum::Multi(f) => f.terms_freq(field),
        }
    }
}

pub enum TermsEnum<T: Terms> {
    Raw(T),
    Multi(Arc<MultiTerms<T>>),
}

impl<T: Terms> Terms for TermsEnum<T> {
    type Iterator = MultiTermIteratorEnum<T::Iterator>;
    fn iterator(&self) -> Result<Self::Iterator> {
        match self {
            TermsEnum::Raw(t) => Ok(MultiTermIteratorEnum::Raw(t.iterator()?)),
            TermsEnum::Multi(t) => t.iterator(),
        }
    }

    fn size(&self) -> Result<i64> {
        match self {
            TermsEnum::Raw(t) => t.size(),
            TermsEnum::Multi(t) => t.size(),
        }
    }

    fn sum_total_term_freq(&self) -> Result<i64> {
        match self {
            TermsEnum::Raw(t) => t.sum_total_term_freq(),
            TermsEnum::Multi(t) => t.sum_total_term_freq(),
        }
    }

    fn sum_doc_freq(&self) -> Result<i64> {
        match self {
            TermsEnum::Raw(t) => t.sum_doc_freq(),
            TermsEnum::Multi(t) => t.sum_doc_freq(),
        }
    }

    fn doc_count(&self) -> Result<i32> {
        match self {
            TermsEnum::Raw(t) => t.doc_count(),
            TermsEnum::Multi(t) => t.doc_count(),
        }
    }

    fn has_freqs(&self) -> Result<bool> {
        match self {
            TermsEnum::Raw(t) => t.has_freqs(),
            TermsEnum::Multi(t) => t.has_freqs(),
        }
    }

    fn has_offsets(&self) -> Result<bool> {
        match self {
            TermsEnum::Raw(t) => t.has_offsets(),
            TermsEnum::Multi(t) => t.has_offsets(),
        }
    }

    fn has_positions(&self) -> Result<bool> {
        match self {
            TermsEnum::Raw(t) => t.has_positions(),
            TermsEnum::Multi(t) => t.has_positions(),
        }
    }

    fn has_payloads(&self) -> Result<bool> {
        match self {
            TermsEnum::Raw(t) => t.has_payloads(),
            TermsEnum::Multi(t) => t.has_payloads(),
        }
    }

    fn min(&self) -> Result<Option<Vec<u8>>> {
        match self {
            TermsEnum::Raw(t) => t.min(),
            TermsEnum::Multi(t) => t.min(),
        }
    }

    fn max(&self) -> Result<Option<Vec<u8>>> {
        match self {
            TermsEnum::Raw(t) => t.max(),
            TermsEnum::Multi(t) => t.max(),
        }
    }

    fn stats(&self) -> Result<String> {
        match self {
            TermsEnum::Raw(t) => t.stats(),
            TermsEnum::Multi(t) => t.stats(),
        }
    }
}

/// Exposes `PostingIterator`, merged from `PostingIterator` API of sub-segments.
pub struct MultiPostingsIterator<T: PostingIterator> {
    subs: Vec<IterWithSlice<T>>,
    num_subs: usize,
    upto: usize,
    current_index: usize,
    current_base: DocId,
    doc: DocId,
}

impl<T: PostingIterator> MultiPostingsIterator<T> {
    pub fn new(sub_reader_count: usize) -> Self {
        MultiPostingsIterator {
            // sub_postings: Vec::with_capacity(sub_reader_count),
            subs: Vec::with_capacity(sub_reader_count),
            num_subs: 0,
            upto: 0,
            current_index: 0,
            current_base: 0,
            doc: -1,
        }
    }

    pub fn reset(&mut self, subs: Vec<IterWithSlice<T>>, num_subs: usize) {
        self.subs = subs;
        self.num_subs = num_subs;
    }
}

impl<T: PostingIterator> PostingIterator for MultiPostingsIterator<T> {
    fn freq(&self) -> Result<i32> {
        debug_assert!(self.current_index < self.num_subs);
        self.subs[self.current_index].postings_iter.freq()
    }

    fn next_position(&mut self) -> Result<i32> {
        debug_assert!(self.current_index < self.num_subs);
        self.subs[self.current_index].postings_iter.next_position()
    }

    fn start_offset(&self) -> Result<i32> {
        debug_assert!(self.current_index < self.num_subs);
        self.subs[self.current_index].postings_iter.start_offset()
    }

    fn end_offset(&self) -> Result<i32> {
        debug_assert!(self.current_index < self.num_subs);
        self.subs[self.current_index].postings_iter.end_offset()
    }

    fn payload(&self) -> Result<Payload> {
        debug_assert!(self.current_index < self.num_subs);
        self.subs[self.current_index].postings_iter.payload()
    }
}

impl<T: PostingIterator> DocIterator for MultiPostingsIterator<T> {
    fn doc_id(&self) -> DocId {
        self.doc
    }

    fn next(&mut self) -> Result<DocId> {
        loop {
            if self.current_index == self.num_subs {
                if self.upto == self.num_subs - 1 {
                    self.doc = NO_MORE_DOCS;
                    return Ok(self.doc);
                } else {
                    self.upto += 1;
                    self.current_index = self.upto;
                    self.current_base = self.subs[self.current_index].slice.start;
                }
            }
            let doc = self.subs[self.current_index].postings_iter.next()?;
            if doc != NO_MORE_DOCS {
                self.doc = self.current_base + doc;
                return Ok(self.doc);
            } else {
                self.current_index = self.num_subs;
            }
        }
    }

    fn advance(&mut self, target: DocId) -> Result<DocId> {
        debug_assert!(target > self.doc);
        loop {
            if self.current_index < self.num_subs {
                let doc = if target < self.current_base {
                    // target was in the previous slice but there was no matching doc after it
                    self.subs[self.current_index].postings_iter.next()?
                } else {
                    self.subs[self.current_index]
                        .postings_iter
                        .advance(target - self.current_base)?
                };
                if doc == NO_MORE_DOCS {
                    self.current_index = self.num_subs;
                } else {
                    self.doc = doc + self.current_base;
                    return Ok(self.doc);
                }
            } else if self.upto == self.num_subs - 1 {
                self.doc = NO_MORE_DOCS;
                return Ok(self.doc);
            } else {
                self.upto += 1;
                self.current_index = self.upto;
                self.current_base = self.subs[self.current_index].slice.start;
            }
        }
    }

    fn cost(&self) -> usize {
        self.subs
            .iter()
            .take(self.num_subs)
            .map(|s| s.postings_iter.cost())
            .sum()
    }
}

pub struct IterWithSlice<T: PostingIterator> {
    pub postings_iter: T,
    pub slice: ReaderSlice,
}

impl<T: PostingIterator> IterWithSlice<T> {
    pub fn new(postings_iter: T, slice: ReaderSlice) -> Self {
        IterWithSlice {
            postings_iter,
            slice,
        }
    }
}

struct FieldsMergeState<C: Codec> {
    doc_maps: Vec<Arc<LiveDocsDocMap>>,
    fields_producers: Vec<MergeFieldsProducer<CodecFieldsProducer<C>>>,
    needs_index_sort: bool,
}

impl<C: Codec> FieldsMergeState<C> {
    fn new<D: Directory>(state: &MergeState<D, C>) -> Self {
        FieldsMergeState {
            doc_maps: state.doc_maps.clone(),
            fields_producers: state.fields_producers.clone(),
            needs_index_sort: state.needs_index_sort,
        }
    }
}

/// A `Fields` implementation that merges multiple Fields into one,
/// and maps around deleted documents. This is used for merging.
pub struct MappedMultiFields<C: Codec, T: FieldsProducer> {
    fields: MultiFields<T>,
    fields_state: Arc<FieldsMergeState<C>>,
}

impl<C: Codec, T: FieldsProducer> MappedMultiFields<C, T> {
    pub fn new<D: Directory>(merge_state: &MergeState<D, C>, fields: MultiFields<T>) -> Self {
        MappedMultiFields {
            fields,
            fields_state: Arc::new(FieldsMergeState::new(merge_state)),
        }
    }
}

impl<C: Codec, T: FieldsProducer> Fields for MappedMultiFields<C, T> {
    type Terms = MappedMultiTerms<C, T::Terms>;
    fn fields(&self) -> Vec<String> {
        self.fields.fields()
    }

    fn terms(&self, field: &str) -> Result<Option<Self::Terms>> {
        match self.fields.terms(field)? {
            Some(t) => Ok(Some(MappedMultiTerms::new(
                field.to_string(),
                Arc::clone(&self.fields_state),
                t,
            ))),
            None => Ok(None),
        }
    }

    fn size(&self) -> usize {
        self.fields.size()
    }
}

pub struct MappedMultiTerms<C: Codec, T: Terms> {
    terms: Arc<MultiTerms<T>>,
    field: String,
    fields_state: Arc<FieldsMergeState<C>>,
}

impl<C: Codec, T: Terms> MappedMultiTerms<C, T> {
    fn new(
        field: String,
        fields_state: Arc<FieldsMergeState<C>>,
        terms: Arc<MultiTerms<T>>,
    ) -> Self {
        MappedMultiTerms {
            terms,
            field,
            fields_state,
        }
    }
}

impl<C: Codec, T: Terms> Terms for MappedMultiTerms<C, T> {
    type Iterator = MappedMultiTermsIterEnum<C, T::Iterator>;
    fn iterator(&self) -> Result<Self::Iterator> {
        let iterator = self.terms.iterator()?;
        if iterator.is_empty() {
            Ok(MappedMultiTermsIterEnum::Multi(iterator))
        } else {
            Ok(MappedMultiTermsIterEnum::Mapped(
                MappedMultiTermsIterator::new(
                    self.field.clone(),
                    Arc::clone(&self.fields_state),
                    iterator,
                ),
            ))
        }
    }

    fn size(&self) -> Result<i64> {
        bail!(UnsupportedOperation(Cow::Borrowed("")))
    }

    fn sum_total_term_freq(&self) -> Result<i64> {
        bail!(UnsupportedOperation(Cow::Borrowed("")))
    }

    fn sum_doc_freq(&self) -> Result<i64> {
        bail!(UnsupportedOperation(Cow::Borrowed("")))
    }

    fn doc_count(&self) -> Result<i32> {
        bail!(UnsupportedOperation(Cow::Borrowed("")))
    }

    fn has_freqs(&self) -> Result<bool> {
        self.terms.has_freqs()
    }

    fn has_offsets(&self) -> Result<bool> {
        self.terms.has_offsets()
    }

    fn has_positions(&self) -> Result<bool> {
        self.terms.has_positions()
    }

    fn has_payloads(&self) -> Result<bool> {
        self.terms.has_payloads()
    }

    fn min(&self) -> Result<Option<Vec<u8>>> {
        self.terms.min()
    }

    fn max(&self) -> Result<Option<Vec<u8>>> {
        self.terms.max()
    }

    fn stats(&self) -> Result<String> {
        self.terms.stats()
    }
}

pub struct MappedMultiTermsIterator<C: Codec, T: TermIterator> {
    fields_state: Arc<FieldsMergeState<C>>,
    field: String,
    terms: MultiTermIteratorEnum<T>,
}

impl<C: Codec, T: TermIterator> MappedMultiTermsIterator<C, T> {
    fn new(
        field: String,
        fields_state: Arc<FieldsMergeState<C>>,
        terms: MultiTermIteratorEnum<T>,
    ) -> Self {
        MappedMultiTermsIterator {
            field,
            fields_state,
            terms,
        }
    }
}

impl<C: Codec, T: TermIterator> TermIterator for MappedMultiTermsIterator<C, T> {
    type Postings = MappingMultiPostingsIter<T::Postings>;
    type TermState = T::TermState;
    fn next(&mut self) -> Result<Option<Vec<u8>>> {
        self.terms.next()
    }

    fn seek_ceil(&mut self, text: &[u8]) -> Result<SeekStatus> {
        self.terms.seek_ceil(text)
    }

    fn seek_exact_ord(&mut self, ord: i64) -> Result<()> {
        self.terms.seek_exact_ord(ord)
    }

    fn term(&self) -> Result<&[u8]> {
        self.terms.term()
    }

    fn ord(&self) -> Result<i64> {
        self.terms.ord()
    }

    fn doc_freq(&mut self) -> Result<i32> {
        bail!(UnsupportedOperation(Cow::Borrowed("")))
    }

    fn total_term_freq(&mut self) -> Result<i64> {
        bail!(UnsupportedOperation(Cow::Borrowed("")))
    }

    fn postings_with_flags(&mut self, flags: u16) -> Result<Self::Postings> {
        let mut mapping_docs_and_positions_iter =
            MappingMultiPostingsIter::new(self.field.clone(), self.fields_state.as_ref())?;
        let docs_and_positins_iter = self.terms.postings_with_flags(flags)?;
        mapping_docs_and_positions_iter.reset(docs_and_positins_iter)?;
        Ok(mapping_docs_and_positions_iter)
    }
}

pub enum MappedMultiTermsIterEnum<C: Codec, T: TermIterator> {
    Mapped(MappedMultiTermsIterator<C, T>),
    Multi(MultiTermIteratorEnum<T>),
}

impl<C: Codec, T: TermIterator> TermIterator for MappedMultiTermsIterEnum<C, T> {
    type Postings = MappedMultiPostingIterEnum<T::Postings>;
    type TermState = T::TermState;

    fn next(&mut self) -> Result<Option<Vec<u8>>> {
        match self {
            MappedMultiTermsIterEnum::Mapped(t) => t.next(),
            MappedMultiTermsIterEnum::Multi(t) => t.next(),
        }
    }

    fn seek_exact(&mut self, text: &[u8]) -> Result<bool> {
        match self {
            MappedMultiTermsIterEnum::Mapped(t) => t.seek_exact(text),
            MappedMultiTermsIterEnum::Multi(t) => t.seek_exact(text),
        }
    }

    fn seek_ceil(&mut self, text: &[u8]) -> Result<SeekStatus> {
        match self {
            MappedMultiTermsIterEnum::Mapped(t) => t.seek_ceil(text),
            MappedMultiTermsIterEnum::Multi(t) => t.seek_ceil(text),
        }
    }

    fn seek_exact_ord(&mut self, ord: i64) -> Result<()> {
        match self {
            MappedMultiTermsIterEnum::Mapped(t) => t.seek_exact_ord(ord),
            MappedMultiTermsIterEnum::Multi(t) => t.seek_exact_ord(ord),
        }
    }

    fn seek_exact_state(&mut self, text: &[u8], state: &Self::TermState) -> Result<()> {
        match self {
            MappedMultiTermsIterEnum::Mapped(t) => t.seek_exact_state(text, state),
            MappedMultiTermsIterEnum::Multi(t) => t.seek_exact_state(text, state),
        }
    }

    fn term(&self) -> Result<&[u8]> {
        match self {
            MappedMultiTermsIterEnum::Mapped(t) => t.term(),
            MappedMultiTermsIterEnum::Multi(t) => t.term(),
        }
    }

    fn ord(&self) -> Result<i64> {
        match self {
            MappedMultiTermsIterEnum::Mapped(t) => t.ord(),
            MappedMultiTermsIterEnum::Multi(t) => t.ord(),
        }
    }

    fn doc_freq(&mut self) -> Result<i32> {
        match self {
            MappedMultiTermsIterEnum::Mapped(t) => t.doc_freq(),
            MappedMultiTermsIterEnum::Multi(t) => t.doc_freq(),
        }
    }

    fn total_term_freq(&mut self) -> Result<i64> {
        match self {
            MappedMultiTermsIterEnum::Mapped(t) => t.total_term_freq(),
            MappedMultiTermsIterEnum::Multi(t) => t.total_term_freq(),
        }
    }

    fn postings(&mut self) -> Result<Self::Postings> {
        match self {
            MappedMultiTermsIterEnum::Mapped(t) => {
                Ok(MappedMultiPostingIterEnum::Mapped(t.postings()?))
            }
            MappedMultiTermsIterEnum::Multi(t) => {
                Ok(MappedMultiPostingIterEnum::Multi(t.postings()?))
            }
        }
    }

    fn postings_with_flags(&mut self, flags: u16) -> Result<Self::Postings> {
        match self {
            MappedMultiTermsIterEnum::Mapped(t) => Ok(MappedMultiPostingIterEnum::Mapped(
                t.postings_with_flags(flags)?,
            )),
            MappedMultiTermsIterEnum::Multi(t) => Ok(MappedMultiPostingIterEnum::Multi(
                t.postings_with_flags(flags)?,
            )),
        }
    }

    fn term_state(&mut self) -> Result<Self::TermState> {
        match self {
            MappedMultiTermsIterEnum::Mapped(t) => t.term_state(),
            MappedMultiTermsIterEnum::Multi(t) => t.term_state(),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            MappedMultiTermsIterEnum::Mapped(t) => t.is_empty(),
            MappedMultiTermsIterEnum::Multi(t) => t.is_empty(),
        }
    }
}

pub struct MappingMultiPostingsIter<T: PostingIterator> {
    _field: String,
    doc_id_merger: DocIdMergerEnum<MappingPostingsSub<T>>,
    current: *mut MappingPostingsSub<T>,
    // current is point to doc_id_merge
    // multi_docs_and_positions_iter: Option<MultiPostingsIterator>,
    all_subs: Vec<MappingPostingsSub<T>>,
    // subs: Vec<MappingPostingsSub>,
}

impl<T: PostingIterator> MappingMultiPostingsIter<T> {
    fn new<C: Codec>(field: String, merge_state: &FieldsMergeState<C>) -> Result<Self> {
        let mut all_subs = Vec::with_capacity(merge_state.fields_producers.len());
        for i in 0..merge_state.fields_producers.len() {
            all_subs.push(MappingPostingsSub::new(Arc::clone(
                &merge_state.doc_maps[i],
            )));
        }
        let doc_id_merger =
            doc_id_merger_of_count(vec![], all_subs.len(), merge_state.needs_index_sort)?;
        Ok(MappingMultiPostingsIter {
            _field: field,
            doc_id_merger,
            current: ptr::null_mut(),
            // multi_docs_and_positions_iter: None,
            all_subs,
        })
    }

    fn reset(&mut self, mut postings_iter: MultiPostingIterEnum<T>) -> Result<()> {
        debug_assert!(postings_iter.is_multi());
        let (num_subs, mut subs) = match &mut postings_iter {
            MultiPostingIterEnum::Multi(iter) => (
                iter.num_subs,
                mem::replace(&mut iter.subs, Vec::with_capacity(0)),
            ),
            _ => unreachable!(),
        };

        self.doc_id_merger.subs_mut().clear();
        for s in subs.drain(..num_subs) {
            let mut sub = self.all_subs[s.slice.reader_index].copy();
            sub.postings = Some(s.postings_iter);
            self.doc_id_merger.subs_mut().push(sub);
        }
        self.doc_id_merger.reset()
    }

    #[allow(clippy::mut_from_ref)]
    fn current(&self) -> &mut MappingPostingsSub<T> {
        unsafe { &mut *self.current }
    }
}

unsafe impl<T: PostingIterator> Send for MappingMultiPostingsIter<T> {}

impl<T: PostingIterator> PostingIterator for MappingMultiPostingsIter<T> {
    fn freq(&self) -> Result<i32> {
        debug_assert!(!self.current.is_null());
        self.current().postings.as_ref().unwrap().freq()
    }

    fn next_position(&mut self) -> Result<i32> {
        debug_assert!(!self.current.is_null());
        let pos = self.current().postings.as_mut().unwrap().next_position()?;
        if pos < 0 {
            bail!(CorruptIndex("position is negative".into()));
        } else if pos > INDEX_MAX_POSITION {
            bail!(CorruptIndex(format!("{} is too large", pos)));
        }
        Ok(pos)
    }

    fn start_offset(&self) -> Result<i32> {
        debug_assert!(!self.current.is_null());
        self.current().postings.as_ref().unwrap().start_offset()
    }

    fn end_offset(&self) -> Result<i32> {
        debug_assert!(!self.current.is_null());
        self.current().postings.as_ref().unwrap().end_offset()
    }

    fn payload(&self) -> Result<Payload> {
        debug_assert!(!self.current.is_null());
        self.current().postings.as_ref().unwrap().payload()
    }
}

impl<T: PostingIterator> DocIterator for MappingMultiPostingsIter<T> {
    fn doc_id(&self) -> DocId {
        if self.current.is_null() {
            -1
        } else {
            self.current().base.mapped_doc_id
        }
    }

    fn next(&mut self) -> Result<DocId> {
        if let Some(sub) = self.doc_id_merger.next()? {
            self.current = sub;
            Ok(sub.base().mapped_doc_id)
        } else {
            self.current = ptr::null_mut();
            Ok(NO_MORE_DOCS)
        }
    }

    fn advance(&mut self, _target: DocId) -> Result<DocId> {
        bail!(UnsupportedOperation(Cow::Borrowed("")))
    }

    fn cost(&self) -> usize {
        self.doc_id_merger
            .subs()
            .iter()
            .map(|s| s.postings.as_ref().unwrap().cost())
            .sum()
    }
}

pub struct MappingPostingsSub<T: PostingIterator> {
    // postings: Option<MultiPostingIterEnum<T>>,
    postings: Option<T>,
    base: DocIdMergerSubBase,
}

impl<T: PostingIterator> MappingPostingsSub<T> {
    fn new(doc_map: Arc<LiveDocsDocMap>) -> Self {
        let base = DocIdMergerSubBase::new(doc_map);
        MappingPostingsSub {
            postings: None,
            base,
        }
    }

    fn copy(&self) -> MappingPostingsSub<T> {
        MappingPostingsSub {
            postings: None,
            base: self.base.clone(),
        }
    }
}

impl<T: PostingIterator> DocIdMergerSub for MappingPostingsSub<T> {
    fn next_doc(&mut self) -> Result<DocId> {
        debug_assert!(self.postings.is_some());
        self.postings.as_mut().unwrap().next()
    }

    fn base(&self) -> &DocIdMergerSubBase {
        &self.base
    }

    fn base_mut(&mut self) -> &mut DocIdMergerSubBase {
        &mut self.base
    }

    fn reset(&mut self) {
        self.base.reset();
    }
}

pub enum MappedMultiPostingIterEnum<T: PostingIterator> {
    Mapped(MappingMultiPostingsIter<T>),
    Multi(MultiPostingIterEnum<T>),
}

impl<T: PostingIterator> PostingIterator for MappedMultiPostingIterEnum<T> {
    fn freq(&self) -> Result<i32> {
        match self {
            MappedMultiPostingIterEnum::Mapped(i) => i.freq(),
            MappedMultiPostingIterEnum::Multi(i) => i.freq(),
        }
    }

    fn next_position(&mut self) -> Result<i32> {
        match self {
            MappedMultiPostingIterEnum::Mapped(i) => i.next_position(),
            MappedMultiPostingIterEnum::Multi(i) => i.next_position(),
        }
    }

    fn start_offset(&self) -> Result<i32> {
        match self {
            MappedMultiPostingIterEnum::Mapped(i) => i.start_offset(),
            MappedMultiPostingIterEnum::Multi(i) => i.start_offset(),
        }
    }

    fn end_offset(&self) -> Result<i32> {
        match self {
            MappedMultiPostingIterEnum::Mapped(i) => i.end_offset(),
            MappedMultiPostingIterEnum::Multi(i) => i.end_offset(),
        }
    }

    fn payload(&self) -> Result<Payload> {
        match self {
            MappedMultiPostingIterEnum::Mapped(i) => i.payload(),
            MappedMultiPostingIterEnum::Multi(i) => i.payload(),
        }
    }
}

impl<T: PostingIterator> DocIterator for MappedMultiPostingIterEnum<T> {
    fn doc_id(&self) -> DocId {
        match self {
            MappedMultiPostingIterEnum::Mapped(i) => i.doc_id(),
            MappedMultiPostingIterEnum::Multi(i) => i.doc_id(),
        }
    }

    fn next(&mut self) -> Result<DocId> {
        match self {
            MappedMultiPostingIterEnum::Mapped(i) => i.next(),
            MappedMultiPostingIterEnum::Multi(i) => i.next(),
        }
    }

    fn advance(&mut self, target: i32) -> Result<DocId> {
        match self {
            MappedMultiPostingIterEnum::Mapped(i) => i.advance(target),
            MappedMultiPostingIterEnum::Multi(i) => i.advance(target),
        }
    }

    fn slow_advance(&mut self, target: i32) -> Result<DocId> {
        match self {
            MappedMultiPostingIterEnum::Mapped(i) => i.slow_advance(target),
            MappedMultiPostingIterEnum::Multi(i) => i.slow_advance(target),
        }
    }

    fn cost(&self) -> usize {
        match self {
            MappedMultiPostingIterEnum::Mapped(i) => i.cost(),
            MappedMultiPostingIterEnum::Multi(i) => i.cost(),
        }
    }

    fn matches(&mut self) -> Result<bool> {
        match self {
            MappedMultiPostingIterEnum::Mapped(i) => i.matches(),
            MappedMultiPostingIterEnum::Multi(i) => i.matches(),
        }
    }

    fn match_cost(&self) -> f32 {
        match self {
            MappedMultiPostingIterEnum::Mapped(i) => i.match_cost(),
            MappedMultiPostingIterEnum::Multi(i) => i.match_cost(),
        }
    }

    fn approximate_next(&mut self) -> Result<DocId> {
        match self {
            MappedMultiPostingIterEnum::Mapped(i) => i.approximate_next(),
            MappedMultiPostingIterEnum::Multi(i) => i.approximate_next(),
        }
    }

    fn approximate_advance(&mut self, target: i32) -> Result<DocId> {
        match self {
            MappedMultiPostingIterEnum::Mapped(i) => i.approximate_advance(target),
            MappedMultiPostingIterEnum::Multi(i) => i.approximate_advance(target),
        }
    }
}
