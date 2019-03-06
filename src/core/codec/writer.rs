use core::analysis::TokenStream;
use core::codec::reader::{PointsReader, StoredFieldsReader, TermVectorsReader};
use core::codec::BlockTermState;
use core::codec::MutablePointsReader;
use core::doc::{FieldType, STORE_FIELD_TYPE};
use core::index::doc_id_merger::doc_id_merger_of;
use core::index::doc_id_merger::{DocIdMerger, DocIdMergerSub, DocIdMergerSubBase};
use core::index::TermIterator;
use core::index::{DocMap, LiveDocsDocMap, MergeState};
use core::index::{FieldInfo, FieldInfos, Fieldable, Fields, SegmentWriteState};
use core::index::{IntersectVisitor, PointValues, Relation};
use core::index::{Status, StoredFieldVisitor};
use core::search::posting_iterator::{POSTING_ITERATOR_FLAG_OFFSETS, POSTING_ITERATOR_FLAG_PAYLOADS};
use core::search::NO_MORE_DOCS;
use core::store::{DataInput, DataOutput, IndexOutput};
use core::util::bit_set::FixedBitSet;
use core::util::bit_util::UnsignedShift;
use core::util::byte_ref::BytesRef;
use core::util::{DocId, Numeric, VariantValue};

use error::ErrorKind::{IllegalArgument, IllegalState, UnsupportedOperation};
use error::Result;

use std::any::Any;
use std::borrow::Cow;
use std::mem;
use std::ptr;
use std::sync::Arc;

pub trait PointsWriter {
    /// Write all values contained in the provided reader
    fn write_field(
        &mut self,
        field_info: &FieldInfo,
        values: Box<MutablePointsReader>,
    ) -> Result<()>;

    /// Default naive merge implementation for one field: it just re-indexes all the values
    /// from the incoming segment.  The default codec overrides this for 1D fields and uses
    /// a faster but more complex implementation.
    fn merge_one_field(&mut self, merge_state: &MergeState, field_info: &FieldInfo) -> Result<()> {
        let mut max_point_count = 0;
        let mut doc_count = 0;
        let mut i = 0;
        for reader_opt in &merge_state.points_readers {
            if let Some(reader) = reader_opt {
                if let Some(reader_field_info) =
                    merge_state.fields_infos[i].field_info_by_name(&field_info.name)
                {
                    if reader_field_info.point_dimension_count > 0 {
                        max_point_count += reader.size(&field_info.name)?;
                        doc_count += reader.doc_count(&field_info.name)?;
                    }
                }
            }
            i += 1;
        }

        self.write_field(
            field_info,
            Box::new(MergePointsReader::new(
                field_info.clone(),
                merge_state,
                max_point_count,
                doc_count,
            )),
        )
    }

    /// Default merge implementation to merge incoming points readers by visiting all their points
    /// and adding to this writer
    fn merge(&mut self, merge_state: &MergeState) -> Result<()>;

    /// Called once at the end before close
    fn finish(&mut self) -> Result<()>;
}

pub fn merge_point_values(writer: &mut PointsWriter, merge_state: &MergeState) -> Result<()> {
    // merge field at a time
    for field_info in merge_state
        .merge_field_infos
        .as_ref()
        .unwrap()
        .by_number
        .values()
    {
        if field_info.point_dimension_count > 0 {
            writer.merge_one_field(merge_state, field_info.as_ref())?;
        }
    }
    writer.finish()
}

struct MergePointsReader {
    field_info: FieldInfo,
    points_readers: Vec<Option<Arc<PointValues>>>,
    fields_infos: Vec<Arc<FieldInfos>>,
    doc_maps: Vec<Arc<LiveDocsDocMap>>,
    max_point_count: i64,
    doc_count: i32,
}

impl MergePointsReader {
    fn new(
        field_info: FieldInfo,
        merge_state: &MergeState,
        max_point_count: i64,
        doc_count: i32,
    ) -> Self {
        MergePointsReader {
            field_info,
            points_readers: merge_state.points_readers.clone(),
            fields_infos: merge_state.fields_infos.clone(),
            doc_maps: merge_state.doc_maps.clone(),
            max_point_count,
            doc_count,
        }
    }
}

// TODO just to make the compiler happy
impl MutablePointsReader for MergePointsReader {
    fn value(&self, _i: i32, _packed_value: &mut Vec<u8>) {
        unreachable!()
    }

    fn byte_at(&self, _i: i32, _k: i32) -> u8 {
        unreachable!()
    }

    fn doc_id(&self, _i: i32) -> i32 {
        unreachable!()
    }

    fn swap(&mut self, _i: i32, _j: i32) {
        unreachable!()
    }

    fn clone(&self) -> Box<MutablePointsReader> {
        unreachable!()
    }
}

impl PointsReader for MergePointsReader {
    fn check_integrity(&self) -> Result<()> {
        bail!(UnsupportedOperation(Cow::Borrowed("")))
    }

    fn as_any(&self) -> &Any {
        self
    }
}

impl PointValues for MergePointsReader {
    fn intersect(&self, field_name: &str, visitor: &mut IntersectVisitor) -> Result<()> {
        if field_name != &self.field_info.name {
            bail!(IllegalArgument(
                "field name must match the field being merged".into()
            ));
        }

        for i in 0..self.points_readers.len() {
            if let Some(reader) = &self.points_readers[i] {
                if let Some(reader_field_info) = self.fields_infos[i].field_info_by_name(field_name)
                {
                    if reader_field_info.point_dimension_count == 0 {
                        continue;
                    }

                    reader.intersect(
                        &self.field_info.name,
                        &mut MergeIntersectVisitorWrapper {
                            visitor,
                            doc_map: self.doc_maps[i].as_ref(),
                        },
                    )?;
                }
            }
        }
        Ok(())
    }

    fn min_packed_value(&self, _field_name: &str) -> Result<Vec<u8>> {
        bail!(UnsupportedOperation(Cow::Borrowed("")))
    }

    fn max_packed_value(&self, _field_name: &str) -> Result<Vec<u8>> {
        bail!(UnsupportedOperation(Cow::Borrowed("")))
    }

    fn num_dimensions(&self, _field_name: &str) -> Result<usize> {
        bail!(UnsupportedOperation(Cow::Borrowed("")))
    }

    fn bytes_per_dimension(&self, _field_name: &str) -> Result<usize> {
        bail!(UnsupportedOperation(Cow::Borrowed("")))
    }

    fn size(&self, _field_name: &str) -> Result<i64> {
        Ok(self.max_point_count)
    }

    fn doc_count(&self, _field_name: &str) -> Result<i32> {
        Ok(self.doc_count)
    }

    fn as_any(&self) -> &Any {
        self
    }
}

struct MergeIntersectVisitorWrapper<'a> {
    visitor: &'a mut IntersectVisitor,
    doc_map: &'a LiveDocsDocMap,
}

impl<'a> IntersectVisitor for MergeIntersectVisitorWrapper<'a> {
    fn visit(&mut self, _doc_id: DocId) -> Result<()> {
        // Should never be called because our compare method never returns
        // Relation.CELL_INSIDE_QUERY
        bail!(IllegalState("".into()))
    }

    fn visit_by_packed_value(&mut self, doc_id: DocId, packed_value: &[u8]) -> Result<()> {
        let new_doc_id = self.doc_map.get(doc_id)?;
        if new_doc_id != -1 {
            // not deleted
            self.visitor.visit_by_packed_value(new_doc_id, packed_value)
        } else {
            Ok(())
        }
    }

    fn compare(&self, _min_packed_value: &[u8], _max_packed_value: &[u8]) -> Relation {
        // Forces this segment's PointsReader to always visit all docs + values:
        Relation::CellCrossesQuery
    }
}

pub trait PostingsWriterBase {
    /// Called once after startup, before any terms have been
    /// added.  Implementations typically write a header to
    /// the provided {@code termsOut}.
    fn init(&mut self, terms_out: &mut IndexOutput, state: &SegmentWriteState) -> Result<()>;

    fn close(&mut self) -> Result<()>;

    /// Write all postings for one term; use the provided
    /// {@link TermsEnum} to pull a {@link org.apache.lucene.index.PostingsEnum}.
    /// This method should not
    /// re-position the {@code TermsEnum}!  It is already
    /// positioned on the term that should be written.  This
    /// method must set the bit in the provided {@link
    /// FixedBitSet} for every docID written.  If no docs
    /// were written, this method should return null, and the
    /// terms dict will skip the term.
    fn write_term(
        &mut self,
        term: &[u8],
        terms: &mut TermIterator,
        docs_seen: &mut FixedBitSet,
    ) -> Result<Option<BlockTermState>>;

    /// Encode metadata as long[] and byte[]. {@code absolute} controls whether
    /// current term is delta encoded according to latest term.
    /// Usually elements in {@code longs} are file pointers, so each one always
    /// increases when a new term is consumed. {@code out} is used to write generic
    /// bytes, which are not monotonic.
    ///
    /// NOTE: sometimes long[] might contain "don't care" values that are unused, e.g.
    /// the pointer to postings list may not be defined for some terms but is defined
    /// for others, if it is designed to inline  some postings data in term dictionary.
    /// In this case, the postings writer should always use the last value, so that each
    /// element in metadata long[] remains monotonic.
    ///
    fn encode_term(
        &mut self,
        longs: &mut [i64],
        out: &mut DataOutput,
        field_info: &FieldInfo,
        state: &BlockTermState,
        absolute: bool,
    ) -> Result<()>;

    /// Sets the current field for writing, and returns the
    /// fixed length of long[] metadata (which is fixed per
    /// field), called when the writing switches to another field.
    // TODO: better name?
    fn set_field(&mut self, field_info: &FieldInfo) -> i32;
}

/// Codec API for writing term vectors:
/// For every document, {@link #startDocument(int)} is called,
/// informing the Codec how many fields will be written.
/// {@link #startField(FieldInfo, int, boolean, boolean, boolean)} is called for
/// each field in the document, informing the codec how many terms
/// will be written for that field, and whether or not positions,
/// offsets, or payloads are enabled.
/// Within each field, {@link #startTerm(BytesRef, int)} is called
/// for each term.
/// If offsets and/or positions are enabled, then
/// {@link #addPosition(int, int, int, BytesRef)} will be called for each term
/// occurrence.
/// After all documents have been written, {@link #finish(FieldInfos, int)}
/// is called for verification/sanity-checks.
/// Finally the writer is closed ({@link #close()})
///
///
pub trait TermVectorsWriter {
    /// Called before writing the term vectors of the document.
    ///  {@link #startField(FieldInfo, int, boolean, boolean, boolean)} will
    ///  be called <code>numVectorFields</code> times. Note that if term
    ///  vectors are enabled, this is called even if the document
    ///  has no vector fields, in this case <code>numVectorFields</code>
    ///  will be zero.
    fn start_document(&mut self, num_vector_fields: usize) -> Result<()>;

    /// Called after a doc and all its fields have been added.
    fn finish_document(&mut self) -> Result<()>;

    /// Called before writing the terms of the field.
    ///  {@link #startTerm(BytesRef, int)} will be called <code>numTerms</code> times.
    fn start_field(
        &mut self,
        info: &FieldInfo,
        num_terms: usize,
        has_positions: bool,
        has_offsets: bool,
        has_payloads: bool,
    ) -> Result<()>;

    /// Called after a field and all its terms have been added.
    fn finish_field(&mut self) -> Result<()>;

    /// Adds a term and its term frequency <code>freq</code>.
    /// If this field has positions and/or offsets enabled, then
    /// {@link #addPosition(int, int, int, BytesRef)} will be called
    /// <code>freq</code> times respectively.
    ///
    fn start_term(&mut self, term: &BytesRef, freq: i32) -> Result<()>;

    /// Called after a term and all its positions have been added.
    fn finish_term(&mut self) -> Result<()> {
        Ok(())
    }

    /// Adds a term position and offsets
    fn add_position(
        &mut self,
        position: i32,
        start_offset: i32,
        end_offset: i32,
        payload: &[u8],
    ) -> Result<()>;

    /// Called before {@link #close()}, passing in the number
    ///  of documents that were written. Note that this is
    ///  intentionally redundant (equivalent to the number of
    ///  calls to {@link #startDocument(int)}, but a Codec should
    ///  check that this is the case to detect the JRE bug described
    ///  in LUCENE-1282.
    ///
    fn finish(&mut self, fis: &FieldInfos, num_docs: usize) -> Result<()>;

    /// Called by IndexWriter when writing new segments.
    /// <p>
    /// This is an expert API that allows the codec to consume
    /// positions and offsets directly from the indexer.
    /// <p>
    /// The default implementation calls {@link #addPosition(int, int, int, BytesRef)},
    /// but subclasses can override this if they want to efficiently write
    /// all the positions, then all the offsets, for example.
    /// <p>
    /// NOTE: This API is extremely expert and subject to change or removal!!!
    /// @lucene.internal
    ///
    /// TODO: we should probably nuke this and make a more efficient 4.x format
    /// PreFlex-RW could then be slow and buffer (it's only used in tests...)
    ///
    fn add_prox(
        &mut self,
        num_prox: usize,
        positions: Option<&mut DataInput>,
        offsets: Option<&mut DataInput>,
    ) -> Result<()> {
        let mut last_offset = 0;

        let mut positions = positions;
        let mut offsets = offsets;
        for _ in 0..num_prox {
            let mut payload = vec![];
            let mut position = -1;
            let mut start_offset = -1;
            let mut end_offset = -1;

            if let Some(ref mut pos) = positions {
                let code: i32 = pos.read_vint()?;
                position += code.unsigned_shift(1);

                if (code & 1) != 0 {
                    // This position has a payload
                    let payload_length = pos.read_vint()? as usize;

                    payload.resize(payload_length, 0u8);
                    pos.read_bytes(&mut payload, 0, payload_length)?;
                }
            }

            if let Some(ref mut offs) = offsets {
                start_offset = last_offset + offs.read_vint()?;
                end_offset = start_offset + offs.read_vint()?;
                last_offset = end_offset;
            }

            self.add_position(position, start_offset, end_offset, &payload)?;
        }

        Ok(())
    }

    /// Merges in the term vectors from the readers in
    ///  <code>mergeState</code>. The default implementation skips
    ///  over deleted documents, and uses {@link #startDocument(int)},
    ///  {@link #startField(FieldInfo, int, boolean, boolean, boolean)},
    ///  {@link #startTerm(BytesRef, int)}, {@link #addPosition(int, int, int, BytesRef)},
    ///  and {@link #finish(FieldInfos, int)},
    ///  returning the number of documents that were written.
    ///  Implementations can override this method for more sophisticated
    ///  merging (bulk-byte copying, etc).
    fn merge(&mut self, merge_state: &mut MergeState) -> Result<i32>;

    /// Safe (but, slowish) default method to write every
    ///  vector field in the document.
    fn add_all_doc_vectors(
        &mut self,
        vectors: Option<&Fields>,
        merge_state: &MergeState,
    ) -> Result<()> {
        use core::index::TermsRef;
        if let Some(vectors) = vectors {
            let num_fields = vectors.size();
            self.start_document(num_fields)?;

            let mut last_field_name = String::with_capacity(0);

            let mut field_count = 0;
            for field_name in vectors.fields() {
                field_count += 1;
                let field_info = merge_state
                    .merge_field_infos
                    .as_ref()
                    .unwrap()
                    .field_info_by_name(&field_name)
                    .unwrap();
                debug_assert!(last_field_name.is_empty() || field_name > last_field_name);
                last_field_name = field_name;
                let terms_opt = vectors.terms(&last_field_name)?;
                if terms_opt.is_none() {
                    // FieldsEnum shouldn't lie...
                    continue;
                }
                let terms: TermsRef = terms_opt.unwrap();
                let has_positions = terms.has_positions()?;
                let has_offsets = terms.has_offsets()?;
                let has_payloads = terms.has_payloads()?;
                debug_assert!(!has_payloads || has_positions);
                let mut num_terms = terms.size()?;
                if num_terms == -1 {
                    // count manually. It is stupid, but needed, as Terms.size() is not
                    // a mandatory statistics function
                    num_terms = 0;
                    let mut terms_iter = terms.iterator()?;
                    while terms_iter.next()?.is_some() {
                        num_terms += 1;
                    }
                }

                self.start_field(
                    field_info,
                    num_terms as usize,
                    has_positions,
                    has_offsets,
                    has_payloads,
                )?;
                let mut terms_iter = terms.iterator()?;
                let mut term_count = 0;
                loop {
                    if let Some(term) = terms_iter.next()? {
                        term_count += 1;
                        let freq = terms_iter.total_term_freq()?;
                        self.start_term(&BytesRef::new(&term), freq as i32)?;

                        if has_positions || has_offsets {
                            let mut docs_and_positions_iter = terms_iter.postings_with_flags(
                                POSTING_ITERATOR_FLAG_OFFSETS | POSTING_ITERATOR_FLAG_PAYLOADS,
                            )?;
                            let doc_id = docs_and_positions_iter.next()?;
                            debug_assert_ne!(doc_id, NO_MORE_DOCS);
                            debug_assert_eq!(docs_and_positions_iter.freq()? as i64, freq);

                            for _ in 0..freq {
                                let pos = docs_and_positions_iter.next_position()?;
                                let start_offset = docs_and_positions_iter.start_offset()?;
                                let end_offset = docs_and_positions_iter.end_offset()?;
                                let payload = docs_and_positions_iter.payload()?;

                                debug_assert!(!has_positions || pos >= 0);
                                self.add_position(pos, start_offset, end_offset, &payload)?;
                            }
                        }
                        self.finish_term()?;
                    } else {
                        break;
                    }
                }
                debug_assert_eq!(term_count, num_terms as i32);
                self.finish_field()?;
            }
            debug_assert_eq!(field_count, num_fields);
        } else {
            self.start_document(0)?;
        }
        self.finish_document()
    }
}

pub fn merge_term_vectors(
    writer: &mut TermVectorsWriter,
    merge_state: &mut MergeState,
) -> Result<i32> {
    let readers = mem::replace(&mut merge_state.term_vectors_readers, Vec::with_capacity(0));
    let mut subs = Vec::with_capacity(readers.len());
    let mut i = 0;
    for reader in readers {
        let sub = TermVectorsMergeSub::new(
            Arc::clone(&merge_state.doc_maps[i]),
            reader,
            merge_state.max_docs[i],
        );
        subs.push(sub);
        i += 1;
    }
    let mut doc_id_merger = doc_id_merger_of(subs, merge_state.needs_index_sort)?;
    let mut doc_count = 0i32;
    loop {
        if let Some(sub) = doc_id_merger.next()? {
            // NOTE: it's very important to first assign to vectors then pass it to
            // termVectorsWriter.addAllDocVectors; see LUCENE-1282
            let vectors = if let Some(ref reader) = sub.reader {
                reader.get(sub.doc_id)?
            } else {
                None
            };
            writer.add_all_doc_vectors(vectors.as_ref().map(|v| v.as_ref()), merge_state)?;
            doc_count += 1;
        } else {
            break;
        }
    }
    writer.finish(
        merge_state.merge_field_infos.as_ref().unwrap().as_ref(),
        doc_count as usize,
    )?;
    Ok(doc_count)
}

struct TermVectorsMergeSub {
    reader: Option<Arc<TermVectorsReader>>,
    max_doc: i32,
    doc_id: DocId,
    base: DocIdMergerSubBase,
}

impl TermVectorsMergeSub {
    fn new(
        doc_map: Arc<LiveDocsDocMap>,
        reader: Option<Arc<TermVectorsReader>>,
        max_doc: i32,
    ) -> Self {
        let base = DocIdMergerSubBase::new(doc_map);
        TermVectorsMergeSub {
            reader,
            max_doc,
            base,
            doc_id: -1,
        }
    }
}

impl DocIdMergerSub for TermVectorsMergeSub {
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

/// Codec API for writing stored fields:
/// <ol>
/// <li>For every document, {@link #startDocument()} is called,
/// informing the Codec that a new document has started.
/// <li>{@link #writeField(FieldInfo, IndexableField)} is called for
/// each field in the document.
/// <li>After all documents have been written, {@link #finish(FieldInfos, int)}
/// is called for verification/sanity-checks.
/// <li>Finally the writer is closed ({@link #close()})
/// </ol>
///
/// @lucene.experimental
///
pub trait StoredFieldsWriter {
    /// Called before writing the stored fields of the document.
    /// {@link #writeField(FieldInfo, IndexableField)} will be called
    /// for each stored field. Note that this is
    /// called even if the document has no stored fields.
    fn start_document(&mut self) -> Result<()>;

    /// Called when a document and all its fields have been added.
    fn finish_document(&mut self) -> Result<()>;

    /// Writes a single stored field.
    fn write_field(&mut self, field_info: &FieldInfo, field: &Fieldable) -> Result<()>;

    /// Called before {@link #close()}, passing in the number
    /// of documents that were written. Note that this is
    /// intentionally redundant (equivalent to the number of
    /// calls to {@link #startDocument()}, but a Codec should
    /// check that this is the case to detect the JRE bug described
    /// in LUCENE-1282.
    fn finish(&mut self, field_infos: &FieldInfos, num_docs: usize) -> Result<()>;

    /// Merges in the stored fields from the readers in
    /// <code>mergeState</code>. The default implementation skips
    /// over deleted documents, and uses {@link #startDocument()},
    /// {@link #writeField(FieldInfo, IndexableField)}, and {@link #finish(FieldInfos, int)},
    /// returning the number of documents that were written.
    /// Implementations can override this method for more sophisticated
    /// merging (bulk-byte copying, etc).
    fn merge(&mut self, merge_state: &mut MergeState) -> Result<i32>;
}

pub fn merge_store_fields(
    writer: &mut (StoredFieldsWriter + 'static),
    state: &mut MergeState,
) -> Result<i32> {
    let fields_readers = mem::replace(&mut state.stored_fields_readers, Vec::with_capacity(0));
    let mut subs = Vec::with_capacity(fields_readers.len());
    let mut i = 0;
    for reader in fields_readers {
        let sub = StoredFieldsMergeSub::new(
            MergeVisitor::new(state, i, writer),
            Arc::clone(&state.doc_maps[i]),
            reader,
            state.max_docs[i],
        );
        subs.push(sub);
        i += 1;
    }
    let mut doc_id_merger = doc_id_merger_of(subs, state.needs_index_sort)?;

    let mut doc_count = 0;
    loop {
        if let Some(sub) = doc_id_merger.next()? {
            debug_assert_eq!(sub.base().mapped_doc_id, doc_count);
            writer.start_document()?;
            sub.reader.visit_document_mut(sub.doc_id, &mut sub.visitor)?;
            writer.finish_document()?;
            doc_count += 1;
        } else {
            break;
        }
    }
    writer.finish(
        state.merge_field_infos.as_ref().unwrap().as_ref(),
        doc_count as usize,
    )?;
    Ok(doc_count)
}

struct StoredFieldsMergeSub {
    reader: Box<StoredFieldsReader>,
    max_doc: i32,
    visitor: MergeVisitor,
    base: DocIdMergerSubBase,
    doc_id: DocId,
}

impl StoredFieldsMergeSub {
    fn new(
        visitor: MergeVisitor,
        doc_map: Arc<LiveDocsDocMap>,
        reader: Box<StoredFieldsReader>,
        max_doc: i32,
    ) -> Self {
        let base = DocIdMergerSubBase::new(doc_map);
        StoredFieldsMergeSub {
            reader,
            max_doc,
            visitor,
            base,
            doc_id: -1,
        }
    }
}

impl DocIdMergerSub for StoredFieldsMergeSub {
    fn next_doc(&mut self) -> Result<DocId> {
        self.doc_id += 1;
        Ok(if self.doc_id == self.max_doc {
            NO_MORE_DOCS
        } else {
            self.doc_id
        })
    }

    fn base(&self) -> &DocIdMergerSubBase {
        &self.base
    }

    fn base_mut(&mut self) -> &mut DocIdMergerSubBase {
        &mut self.base
    }
}

pub struct MergeVisitor {
    value: Option<VariantValue>,
    current_field: *const FieldInfo,
    fields_writer: *mut StoredFieldsWriter,
    remapper: Option<Arc<FieldInfos>>,
}

impl MergeVisitor {
    pub fn new(
        merge_state: &MergeState,
        reader_index: usize,
        fields_writer: &mut (StoredFieldsWriter + 'static),
    ) -> Self {
        // if field numbers are aligned, we can save hash lookups
        // on every field access. Otherwise, we need to lookup
        // fieldname each time, and remap to a new number.
        let mut remapper = None;
        for fi in merge_state.fields_infos[reader_index].by_number.values() {
            if let Some(info) = merge_state
                .merge_field_infos
                .as_ref()
                .unwrap()
                .field_info_by_number(fi.number)
            {
                if info.name != fi.name {
                    remapper = merge_state.merge_field_infos.as_ref().map(Arc::clone);
                    break;
                }
            } else {
                remapper = merge_state.merge_field_infos.as_ref().map(Arc::clone);
                break;
            }
        }
        MergeVisitor {
            value: None,
            current_field: ptr::null(),
            remapper,
            fields_writer,
        }
    }

    fn reset(&mut self, field: &FieldInfo) {
        if let Some(ref remapper) = self.remapper {
            self.current_field = remapper.field_info_by_name(&field.name).unwrap();
        } else {
            self.current_field = field;
        }
        self.value = None;
    }

    fn write(&mut self) -> Result<()> {
        unsafe { (*self.fields_writer).write_field(&*self.current_field, self) }
    }
}

impl StoredFieldVisitor for MergeVisitor {
    fn binary_field(&mut self, field_info: &FieldInfo, value: Vec<u8>) -> Result<()> {
        self.reset(field_info);
        self.value = Some(VariantValue::Binary(value));
        self.write()
    }

    fn string_field(&mut self, field_info: &FieldInfo, value: Vec<u8>) -> Result<()> {
        self.reset(field_info);
        self.value = Some(VariantValue::VString(String::from_utf8(value)?));
        self.write()
    }

    fn int_field(&mut self, field_info: &FieldInfo, value: i32) -> Result<()> {
        self.reset(field_info);
        self.value = Some(VariantValue::Int(value));
        self.write()
    }

    fn long_field(&mut self, field_info: &FieldInfo, value: i64) -> Result<()> {
        self.reset(field_info);
        self.value = Some(VariantValue::Long(value));
        self.write()
    }

    fn float_field(&mut self, field_info: &FieldInfo, value: f32) -> Result<()> {
        self.reset(field_info);
        self.value = Some(VariantValue::Float(value));
        self.write()
    }

    fn double_field(&mut self, field_info: &FieldInfo, value: f64) -> Result<()> {
        self.reset(field_info);
        self.value = Some(VariantValue::Double(value));
        self.write()
    }

    fn needs_field(&self, _field_info: &FieldInfo) -> Status {
        Status::Yes
    }
}

impl Fieldable for MergeVisitor {
    fn name(&self) -> &str {
        unsafe { &(*self.current_field).name }
    }
    fn field_type(&self) -> &FieldType {
        &STORE_FIELD_TYPE
    }
    fn boost(&self) -> f32 {
        1.0
    }
    fn fields_data(&self) -> Option<&VariantValue> {
        debug_assert!(self.value.is_some());
        self.value.as_ref()
    }
    fn token_stream(&mut self) -> Result<Box<TokenStream>> {
        unreachable!()
    }

    fn binary_value(&self) -> Option<&[u8]> {
        if let Some(VariantValue::Binary(ref b)) = self.value {
            Some(b.as_ref())
        } else {
            None
        }
    }

    // fn binary_value(&self) -> Option<&[u8]>;
    fn string_value(&self) -> Option<&str> {
        if let Some(VariantValue::VString(s)) = &self.value {
            Some(&s)
        } else {
            None
        }
    }

    fn numeric_value(&self) -> Option<Numeric> {
        unimplemented!()
    }
}
