use core::codec::{
    Codec, CodecFieldsProducer, CodecNormsProducer, CodecPointsReader, CodecStoredFieldsReader,
    CodecTVFields, CodecTVReader, DocValuesProducerRef, FieldsProducer, NormsProducer,
    StoredFieldsReader, TermVectorsReader,
};
use core::index::{
    BinaryDocValuesRef, FieldInfo, FieldInfos, Fields, IndexReader, NumericDocValues,
    NumericDocValuesRef, SortedDocValuesRef, SortedNumericDocValuesRef, SortedSetDocValuesRef,
    StoredFieldVisitor, Term, TermIterator, Terms,
};
use core::search::sort::Sort;
use core::util::external::deferred::Deferred;
use core::util::{BitsRef, DocId};

use error::Result;

use core::index::point_values::PointValues;
use std::sync::Arc;

pub type ReaderPostings<FP> =
    <<<FP as Fields>::Terms as Terms>::Iterator as TermIterator>::Postings;

pub trait LeafReader {
    type Codec: Codec;
    type FieldsProducer: FieldsProducer + Clone;
    type TVFields: Fields;
    type TVReader: TermVectorsReader<Fields = Self::TVFields> + Clone;
    type StoredReader: StoredFieldsReader + Clone;
    type NormsReader: NormsProducer + Clone;
    type PointsReader: PointValues + Clone;

    fn codec(&self) -> &Self::Codec;

    fn fields(&self) -> Result<Self::FieldsProducer>;

    fn name(&self) -> &str;

    fn terms(&self, field: &str) -> Result<Option<<Self::FieldsProducer as Fields>::Terms>> {
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

    fn postings(
        &self,
        term: &Term,
        flags: i32,
    ) -> Result<Option<ReaderPostings<Self::FieldsProducer>>> {
        if let Some(terms) = self.terms(term.field())? {
            let mut terms_iter = terms.iterator()?;
            if terms_iter.seek_exact(term.bytes.as_ref())? {
                return Ok(Some(terms_iter.postings_with_flags(flags as u32 as u16)?));
            }
        }
        Ok(None)
    }

    fn postings_from_state(
        &self,
        term: &Term,
        state: &<<<Self::FieldsProducer as Fields>::Terms as Terms>::Iterator as TermIterator>::TermState,
        flags: i32,
    ) -> Result<Option<ReaderPostings<Self::FieldsProducer>>> {
        if let Some(terms) = self.terms(term.field())? {
            let mut terms_iter = terms.iterator()?;
            terms_iter.seek_exact_state(&term.bytes, state)?;
            return Ok(Some(terms_iter.postings_with_flags(flags as u32 as u16)?));
        }
        Ok(None)
    }

    fn term_vector(&self, leaf_doc_id: DocId) -> Result<Option<Self::TVFields>>;

    fn document(&self, doc_id: DocId, visitor: &mut dyn StoredFieldVisitor) -> Result<()>;

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

    fn norm_values(&self, field: &str) -> Result<Option<Box<dyn NumericDocValues>>>;

    fn get_docs_with_field(&self, field: &str) -> Result<BitsRef>;

    /// Returns the `PointValues` used for numeric or
    /// spatial searches, or None if there are no point fields.
    fn point_values(&self) -> Option<Self::PointsReader>;

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

    fn store_fields_reader(&self) -> Result<Self::StoredReader>;

    fn term_vectors_reader(&self) -> Result<Option<Self::TVReader>>;

    fn norms_reader(&self) -> Result<Option<Self::NormsReader>>;

    fn doc_values_reader(&self) -> Result<Option<DocValuesProducerRef>>;

    fn postings_reader(&self) -> Result<Self::FieldsProducer>;
}

pub type SearchLeafReader<C> = LeafReader<
        Codec = C,
        FieldsProducer = CodecFieldsProducer<C>,
        TVReader = Arc<CodecTVReader<C>>,
        TVFields = CodecTVFields<C>,
        StoredReader = Arc<CodecStoredFieldsReader<C>>,
        NormsReader = Arc<CodecNormsProducer<C>>,
        PointsReader = Arc<CodecPointsReader<C>>,
    > + 'static;

// TODO currently we don't support multi-level index reader
pub struct LeafReaderContext<'a, C: Codec> {
    /// ord in parent
    pub ord: usize,
    /// doc base in parent
    pub doc_base: DocId,
    pub reader: &'a SearchLeafReader<C>,
    pub parent: &'a IndexReader<Codec = C>,
}

impl<'a, C: Codec> LeafReaderContext<'a, C> {
    pub fn new(
        parent: &'a IndexReader<Codec = C>,
        reader: &'a SearchLeafReader<C>,
        ord: usize,
        doc_base: DocId,
    ) -> Self {
        Self {
            parent,
            reader,
            ord,
            doc_base,
        }
    }

    #[inline]
    pub fn doc_base(&self) -> DocId {
        self.doc_base
    }
}

impl<'a, C: Codec> Clone for LeafReaderContext<'a, C> {
    fn clone(&self) -> Self {
        Self {
            ord: self.ord,
            doc_base: self.doc_base,
            reader: self.reader,
            parent: self.parent,
        }
    }
}
