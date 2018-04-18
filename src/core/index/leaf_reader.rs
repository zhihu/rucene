use core::codec::FieldsProducerRef;
use core::index::point_values::PointValuesRef;
use core::index::term::TermsRef;
use core::index::BinaryDocValuesRef;
use core::index::SortedDocValuesRef;
use core::index::SortedNumericDocValuesRef;
use core::index::SortedSetDocValuesRef;
use core::index::StoredFieldVisitor;
use core::index::Term;
use core::index::{FieldInfo, FieldInfos, Fields};
use core::index::{NumericDocValues, NumericDocValuesRef};
use core::search::posting_iterator::{EmptyPostingIterator, PostingIterator};
use core::search::DocIterator;
use core::util::{BitsRef, DocId};

use error::Result;

pub trait LeafReader {
    fn fields(&self) -> Result<FieldsProducerRef>;

    fn terms(&self, field: &str) -> Result<Option<TermsRef>> {
        self.fields()?.terms(field)
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

    fn term_vector(&self, doc_id: DocId) -> Result<Box<Fields>>;

    fn document(&self, doc_id: DocId, visitor: &mut StoredFieldVisitor) -> Result<()>;

    fn live_docs(&self) -> BitsRef;

    fn field_info(&self, field: &str) -> Option<&FieldInfo>;

    fn field_infos(&self) -> &FieldInfos;

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
}
