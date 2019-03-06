use std::collections::HashMap;

use core::codec::FieldsProducer;
use core::codec::FieldsProducerRef;
use core::index::doc_id_merger::{doc_id_merger_of_count, DocIdMergerSubBase};
use core::index::doc_id_merger::{DocIdMerger, DocIdMergerEnum, DocIdMergerSub};
use core::index::field_info::Fields;
use core::index::index_writer::INDEX_MAX_POSITION;
use core::index::merge_state::LiveDocsDocMap;
use core::index::multi_terms::MultiTerms;
use core::index::term::*;
use core::index::IndexReader;
use core::index::MergeState;
use core::index::ReaderSlice;
use core::search::posting_iterator::PostingIterator;
use core::search::{DocIterator, Payload, NO_MORE_DOCS};
use core::util::DocId;

use error::ErrorKind::UnsupportedOperation;
use error::Result;

use std::any::Any;
use std::borrow::Cow;
use std::mem;
use std::ptr;
use std::sync::Arc;

pub struct MultiFields {
    subs: Vec<FieldsProducerRef>,
    #[allow(dead_code)]
    sub_slices: Vec<ReaderSlice>,
    // TODO use ConcurrentHashMap instead
    // because Rc<T> is not Thread safety
    // could we change Rc<Terms> to Arc<Terms>
    #[allow(dead_code)]
    terms: HashMap<String, TermsRef>,
}

fn fields(reader: &IndexReader) -> Result<FieldsProducerRef> {
    let leaves = reader.leaves();

    if leaves.len() == 1 {
        Ok(leaves[0].fields()?)
    } else {
        let mut fields: Vec<FieldsProducerRef> = Vec::with_capacity(leaves.len());
        let mut slices: Vec<ReaderSlice> = Vec::with_capacity(leaves.len());
        for leaf in leaves {
            fields.push(leaf.fields()?);
            slices.push(ReaderSlice::new(
                leaf.doc_base(),
                reader.max_doc(),
                fields.len() - 1,
            ));
        }
        if fields.len() == 1 {
            Ok(fields[0].clone())
        } else {
            Ok(Arc::new(MultiFields::new(fields, slices)))
        }
    }
}

impl MultiFields {
    pub fn new(subs: Vec<FieldsProducerRef>, sub_slices: Vec<ReaderSlice>) -> MultiFields {
        MultiFields {
            subs,
            sub_slices,
            terms: HashMap::<String, TermsRef>::new(),
        }
    }

    pub fn get_terms(reader: &IndexReader, field: &str) -> Result<Option<TermsRef>> {
        fields(reader)?.terms(field)
    }
}

impl Fields for MultiFields {
    fn fields(&self) -> Vec<String> {
        let mut res = vec![];
        for sub in &self.subs {
            res.extend(sub.fields());
        }
        res.sort();
        res.dedup();
        res
    }

    fn terms(&self, field: &str) -> Result<Option<TermsRef>> {
        if let Some(res) = self.terms.get(field) {
            return Ok(Some(Arc::clone(res)));
        }

        // TODO cache terms by field
        let mut subs2 = Vec::new();
        let mut slices2 = Vec::new();

        for i in 0..self.subs.len() {
            if let Some(terms) = self.subs[i].terms(field)? {
                subs2.push(terms.to_owned());
                slices2.push(self.sub_slices[i]);
            }
        }
        Ok(Some(Arc::new(MultiTerms::new(subs2, slices2)?)))
    }

    fn size(&self) -> usize {
        1 as usize
    }
}

impl FieldsProducer for MultiFields {
    fn check_integrity(&self) -> Result<()> {
        unimplemented!();
    }
}

/// Exposes `PostingIterator`, merged from `PostingIterator` API of sub-segments.
pub struct MultiPostingsIterator {
    // pub sub_postings: Vec<Box<PostingIterator>>,
    subs: Vec<IterWithSlice>,
    num_subs: usize,
    upto: usize,
    current_index: usize,
    current_base: DocId,
    doc: DocId,
}

impl MultiPostingsIterator {
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

    pub fn reset(&mut self, subs: Vec<IterWithSlice>, num_subs: usize) {
        self.subs = subs;
        self.num_subs = num_subs;
    }
}

impl PostingIterator for MultiPostingsIterator {
    fn clone_as_doc_iterator(&self) -> Result<Box<DocIterator>> {
        unimplemented!()
    }

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

    fn as_any_mut(&mut self) -> &mut Any {
        self
    }
}

impl DocIterator for MultiPostingsIterator {
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

pub struct IterWithSlice {
    pub postings_iter: Box<PostingIterator>,
    pub slice: ReaderSlice,
}

impl IterWithSlice {
    pub fn new(postings_iter: Box<PostingIterator>, slice: ReaderSlice) -> Self {
        IterWithSlice {
            postings_iter,
            slice,
        }
    }
}

struct FieldsMergeState {
    doc_maps: Vec<Arc<LiveDocsDocMap>>,
    fields_producers: Vec<FieldsProducerRef>,
    needs_index_sort: bool,
}

impl FieldsMergeState {
    fn new(state: &MergeState) -> Self {
        FieldsMergeState {
            doc_maps: state.doc_maps.clone(),
            fields_producers: state.fields_producers.clone(),
            needs_index_sort: state.needs_index_sort,
        }
    }
}

/// A `Fields` implementation that merges multiple Fields into one,
/// and maps around deleted documents. This is used for merging.
pub struct MappedMultiFields<T: Fields> {
    fields: T,
    fields_state: Arc<FieldsMergeState>,
}

impl<T: Fields> MappedMultiFields<T> {
    pub fn new(merge_state: &MergeState, fields: T) -> Self {
        MappedMultiFields {
            fields,
            fields_state: Arc::new(FieldsMergeState::new(merge_state)),
        }
    }
}

impl<T: Fields> Fields for MappedMultiFields<T> {
    fn fields(&self) -> Vec<String> {
        self.fields.fields()
    }

    fn terms(&self, field: &str) -> Result<Option<TermsRef>> {
        match self.fields.terms(field)? {
            Some(t) => {
                let terms =
                    MappedMultiTerms::new(field.to_string(), Arc::clone(&self.fields_state), t);
                Ok(Some(Arc::new(terms)))
            }
            None => Ok(None),
        }
    }

    fn size(&self) -> usize {
        self.fields.size()
    }
}

pub struct MappedMultiTerms {
    terms: TermsRef,
    field: String,
    fields_state: Arc<FieldsMergeState>,
}

impl MappedMultiTerms {
    fn new(field: String, fields_state: Arc<FieldsMergeState>, terms: TermsRef) -> Self {
        MappedMultiTerms {
            terms,
            field,
            fields_state,
        }
    }
}

impl Terms for MappedMultiTerms {
    fn iterator(&self) -> Result<Box<TermIterator>> {
        let iterator = self.terms.iterator()?;
        if iterator.as_any().is::<EmptyTermIterator>() {
            Ok(iterator)
        } else {
            Ok(Box::new(MappedMultiTermsIterator::new(
                self.field.clone(),
                Arc::clone(&self.fields_state),
                iterator,
            )))
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

pub struct MappedMultiTermsIterator {
    fields_state: Arc<FieldsMergeState>,
    field: String,
    terms: Box<TermIterator>,
}

impl MappedMultiTermsIterator {
    fn new(field: String, fields_state: Arc<FieldsMergeState>, terms: Box<TermIterator>) -> Self {
        MappedMultiTermsIterator {
            field,
            fields_state,
            terms,
        }
    }
}

impl TermIterator for MappedMultiTermsIterator {
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

    fn postings_with_flags(&mut self, flags: i16) -> Result<Box<PostingIterator>> {
        let mut mapping_docs_and_positions_iter =
            MappingMultiPostingsIter::new(self.field.clone(), self.fields_state.as_ref())?;
        let docs_and_positins_iter = self.terms.postings_with_flags(flags)?;
        mapping_docs_and_positions_iter.reset(docs_and_positins_iter)?;
        Ok(Box::new(mapping_docs_and_positions_iter))
    }

    fn as_any(&self) -> &Any {
        self
    }
}

struct MappingMultiPostingsIter {
    field: String,
    doc_id_merger: DocIdMergerEnum<MappingPostingsSub>,
    current: *mut MappingPostingsSub, // current is point to doc_id_merge
    multi_docs_and_positions_iter: Option<MultiPostingsIterator>,
    all_subs: Vec<MappingPostingsSub>,
    // subs: Vec<MappingPostingsSub>,
}

impl MappingMultiPostingsIter {
    fn new(field: String, merge_state: &FieldsMergeState) -> Result<Self> {
        let mut all_subs = Vec::with_capacity(merge_state.fields_producers.len());
        for i in 0..merge_state.fields_producers.len() {
            all_subs.push(MappingPostingsSub::new(Arc::clone(
                &merge_state.doc_maps[i],
            )));
        }
        let subs: Vec<MappingPostingsSub> = vec![];
        let doc_id_merger =
            doc_id_merger_of_count(subs, all_subs.len(), merge_state.needs_index_sort)?;
        Ok(MappingMultiPostingsIter {
            field,
            doc_id_merger,
            current: ptr::null_mut(),
            multi_docs_and_positions_iter: None,
            all_subs,
        })
    }

    fn reset(&mut self, mut postings_iter: Box<PostingIterator>) -> Result<()> {
        debug_assert!(postings_iter.as_any_mut().is::<MultiPostingsIterator>());
        let (num_subs, mut subs) = if let Some(iter) = postings_iter
            .as_any_mut()
            .downcast_mut::<MultiPostingsIterator>()
        {
            (
                iter.num_subs,
                mem::replace(&mut iter.subs, Vec::with_capacity(0)),
            )
        } else {
            unreachable!()
        };

        self.doc_id_merger.subs_mut().clear();
        for s in subs.drain(..num_subs) {
            let mut sub = self.all_subs[s.slice.reader_index].copy();
            sub.postings = Some(s.postings_iter);
            self.doc_id_merger.subs_mut().push(sub);
        }
        self.doc_id_merger.reset()
    }

    fn current(&self) -> &mut MappingPostingsSub {
        unsafe { &mut *self.current }
    }
}

unsafe impl Send for MappingMultiPostingsIter {}
unsafe impl Sync for MappingMultiPostingsIter {}

impl PostingIterator for MappingMultiPostingsIter {
    fn clone_as_doc_iterator(&self) -> Result<Box<DocIterator>> {
        unimplemented!()
    }

    fn freq(&self) -> Result<i32> {
        debug_assert!(!self.current.is_null());
        self.current().postings.as_ref().unwrap().freq()
    }

    fn next_position(&mut self) -> Result<i32> {
        debug_assert!(!self.current.is_null());
        let pos = self.current().postings.as_mut().unwrap().next_position()?;
        if pos < 0 {
            bail!("CorruptIndex: position is negative");
        } else if pos > INDEX_MAX_POSITION {
            bail!("CorruptIndex: {} is too large", pos);
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

    fn as_any_mut(&mut self) -> &mut Any {
        self
    }
}

impl DocIterator for MappingMultiPostingsIter {
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

struct MappingPostingsSub {
    postings: Option<Box<PostingIterator>>,
    base: DocIdMergerSubBase,
}

impl MappingPostingsSub {
    fn new(doc_map: Arc<LiveDocsDocMap>) -> Self {
        let base = DocIdMergerSubBase::new(doc_map);
        MappingPostingsSub {
            postings: None,
            base,
        }
    }

    fn copy(&self) -> MappingPostingsSub {
        MappingPostingsSub {
            postings: None,
            base: self.base.clone(),
        }
    }
}

impl DocIdMergerSub for MappingPostingsSub {
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
}
