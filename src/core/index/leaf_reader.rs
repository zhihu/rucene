use core::codec::{DocValuesProducerRef, FieldsProducerRef};
use core::codec::{NormsProducer, StoredFieldsReader, TermVectorsReader};
use core::index::point_values::PointValuesRef;
use core::index::term::{TermState, TermsRef};
use core::index::BinaryDocValuesRef;
use core::index::SortedDocValuesRef;
use core::index::SortedNumericDocValuesRef;
use core::index::SortedSetDocValuesRef;
use core::index::StoredFieldVisitor;
use core::index::Term;
use core::index::{FieldInfo, FieldInfos, Fields};
use core::index::{NumericDocValues, NumericDocValuesRef};
use core::search::posting_iterator::{EmptyPostingIterator, PostingIterator};
use core::search::sort::Sort;
use core::search::DocIterator;
use core::util::external::deferred::Deferred;
use core::util::{BitsRef, DocId};

use error::Result;

use std::sync::Arc;

pub trait LeafReader: Send + Sync {
    fn fields(&self) -> Result<FieldsProducerRef>;

    fn name(&self) -> &str;

    fn terms(&self, field: &str) -> Result<Option<TermsRef>> {
        self.fields()?.terms(field)
    }

    fn doc_freq(&self, term: &Term) -> Result<i32> {
        if let Some(terms) = self.terms(&term.field)? {
            let mut terms_iter = terms.iterator()?;
            if terms_iter.seek_exact(&term.bytes)? {
                return terms_iter.doc_freq();
            }
        }

        Ok(0)
    }

    fn doc_base(&self) -> DocId;

    fn docs(&self, term: &Term, flags: i32) -> Result<Box<DocIterator>> {
        self.postings(term, flags)?.clone_as_doc_iterator()
    }

    fn postings(&self, term: &Term, flags: i32) -> Result<Box<PostingIterator>> {
        if let Some(terms) = self.terms(term.field())? {
            let mut terms_iter = terms.iterator()?;
            if terms_iter.seek_exact(term.bytes.as_ref())? {
                return Ok(terms_iter.postings_with_flags(flags as i16)?);
            }
        }
        Ok(Box::new(EmptyPostingIterator::default()))
    }

    fn docs_from_state(
        &self,
        term: &Term,
        state: &TermState,
        flags: i32,
    ) -> Result<Box<DocIterator>> {
        self.postings_from_state(term, state, flags)?
            .clone_as_doc_iterator()
    }

    fn postings_from_state(
        &self,
        term: &Term,
        state: &TermState,
        flags: i32,
    ) -> Result<Box<PostingIterator>> {
        if let Some(terms) = self.terms(term.field())? {
            let mut terms_iter = terms.iterator()?;
            terms_iter.seek_exact_state(term.bytes.as_ref(), state)?;
            return terms_iter.postings_with_flags(flags as i16);
        }
        Ok(Box::new(EmptyPostingIterator::default()))
    }

    fn term_vector(&self, doc_id: DocId) -> Result<Option<Box<Fields>>>;

    fn document(&self, doc_id: DocId, visitor: &mut StoredFieldVisitor) -> Result<()>;

    fn live_docs(&self) -> BitsRef;

    fn field_info(&self, field: &str) -> Option<&FieldInfo>;

    fn field_infos(&self) -> &FieldInfos;

    fn clone_field_infos(&self) -> Arc<FieldInfos>;

    fn max_doc(&self) -> DocId;

    fn num_docs(&self) -> i32;

    fn get_numeric_doc_values(&self, field: &str) -> Result<NumericDocValuesRef>;

    fn get_binary_doc_values(&self, field: &str) -> Result<BinaryDocValuesRef>;

    fn get_sorted_doc_values(&self, field: &str) -> Result<SortedDocValuesRef>;

    fn get_sorted_numeric_doc_values(&self, field: &str) -> Result<SortedNumericDocValuesRef>;

    fn get_sorted_set_doc_values(&self, field: &str) -> Result<SortedSetDocValuesRef>;

    fn norm_values(&self, field: &str) -> Result<Option<Box<NumericDocValues>>>;

    fn get_docs_with_field(&self, field: &str) -> Result<BitsRef>;

    /// Returns the `PointValuesRef` used for numeric or
    /// spatial searches, or None if there are no point fields.
    fn point_values(&self) -> Option<PointValuesRef>;

    /// Expert: Returns a key for this IndexReader, so CachingWrapperFilter can find
    // it again.
    // This key must not have equals()/hashCode() methods, so &quot;equals&quot; means
    // &quot;identical&quot;.
    fn core_cache_key(&self) -> &str;

    /// Returns null if this leaf is unsorted, or the `Sort` that it was sorted by
    fn index_sort(&self) -> Option<&Sort>;

    /// Expert: adds a CoreClosedListener to this reader's shared core
    fn add_core_drop_listener(&self, listener: Deferred);

    // TODO, currently we don't provide remove listener method

    // following methods are from `CodecReader`
    // if this return false, then the following methods must not be called
    fn is_codec_reader(&self) -> bool;

    fn store_fields_reader(&self) -> Result<Arc<StoredFieldsReader>>;

    fn term_vectors_reader(&self) -> Result<Option<Arc<TermVectorsReader>>>;

    fn norms_reader(&self) -> Result<Option<Arc<NormsProducer>>>;

    fn doc_values_reader(&self) -> Result<Option<DocValuesProducerRef>>;

    fn postings_reader(&self) -> Result<FieldsProducerRef>;
}
