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

mod term_vectors_reader;

pub use self::term_vectors_reader::*;

mod term_vectors_writer;

pub use self::term_vectors_writer::*;

mod term_vector_consumer;

pub use self::term_vector_consumer::*;

use error::Result;
use std::any::Any;
use std::mem;
use std::sync::Arc;

use core::codec::field_infos::{FieldInfo, FieldInfos};
use core::codec::posting_iterator::PostingIteratorFlags;
use core::codec::segment_infos::SegmentInfo;
use core::codec::Codec;
use core::codec::{Fields, PostingIterator, TermIterator, Terms};
use core::index::merge::DocIdMerger;
use core::index::merge::{
    doc_id_merger_of, DocIdMergerSub, DocIdMergerSubBase, LiveDocsDocMap, MergeState,
};
use core::search::DocIterator;
use core::search::NO_MORE_DOCS;
use core::store::directory::Directory;
use core::store::io::{DataInput, IndexOutput};
use core::store::IOContext;
use core::util::UnsignedShift;
use core::util::{BytesRef, DocId};

/// Controls the format of term vectors
pub trait TermVectorsFormat {
    type TVReader: TermVectorsReader;
    /// Returns a {@link TermVectorsReader} to read term
    /// vectors.
    fn tv_reader<D: Directory, DW: Directory, C: Codec>(
        &self,
        directory: &DW,
        si: &SegmentInfo<D, C>,
        field_info: Arc<FieldInfos>,
        ioctx: &IOContext,
    ) -> Result<Self::TVReader>;

    /// Returns a {@link TermVectorsWriter} to write term
    /// vectors.
    fn tv_writer<D: Directory, DW: Directory, C: Codec>(
        &self,
        directory: &DW,
        segment_info: &SegmentInfo<D, C>,
        context: &IOContext,
    ) -> Result<TermVectorsWriterEnum<DW::IndexOutput>>;
}

/// `Lucene50TermVectorsFormat` is just `CompressingTermVectorsFormat`
pub fn term_vectors_format() -> CompressingTermVectorsFormat {
    CompressingTermVectorsFormat::default()
}

pub trait TermVectorsReader {
    type Fields: Fields;
    fn get(&self, doc: DocId) -> Result<Option<Self::Fields>>;
    fn as_any(&self) -> &dyn Any;
}

impl<T: TermVectorsReader + 'static> TermVectorsReader for Arc<T> {
    type Fields = T::Fields;

    fn get(&self, doc: DocId) -> Result<Option<Self::Fields>> {
        (**self).get(doc)
    }

    fn as_any(&self) -> &dyn Any {
        (**self).as_any()
    }
}

/// Codec API for writing term vectors:
///
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
    fn start_term(&mut self, term: BytesRef, freq: i32) -> Result<()>;

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
    fn add_prox<I: DataInput + ?Sized>(
        &mut self,
        num_prox: usize,
        positions: Option<&mut I>,
        offsets: Option<&mut I>,
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
                    pos.read_exact(&mut payload)?;
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
    fn merge<D: Directory, C: Codec>(&mut self, merge_state: &mut MergeState<D, C>) -> Result<i32>;

    /// Safe (but, slowish) default method to write every
    ///  vector field in the document.
    fn add_all_doc_vectors<D: Directory, C: Codec>(
        &mut self,
        vectors: Option<&impl Fields>,
        merge_state: &MergeState<D, C>,
    ) -> Result<()> {
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
                let terms = terms_opt.unwrap();
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
                while let Some(term) = terms_iter.next()? {
                    term_count += 1;
                    let freq = terms_iter.total_term_freq()?;
                    self.start_term(BytesRef::new(&term), freq as i32)?;

                    if has_positions || has_offsets {
                        let mut docs_and_positions_iter =
                            terms_iter.postings_with_flags(PostingIteratorFlags::ALL)?;
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

pub fn merge_term_vectors<D: Directory, C: Codec, T: TermVectorsWriter>(
    writer: &mut T,
    merge_state: &mut MergeState<D, C>,
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
    while let Some(sub) = doc_id_merger.next()? {
        // NOTE: it's very important to first assign to vectors then pass it to
        // termVectorsWriter.addAllDocVectors; see LUCENE-1282
        let vectors = if let Some(ref reader) = sub.reader {
            reader.get(sub.doc_id)?
        } else {
            None
        };
        writer.add_all_doc_vectors(vectors.as_ref(), merge_state)?;
        doc_count += 1;
    }
    writer.finish(
        merge_state.merge_field_infos.as_ref().unwrap().as_ref(),
        doc_count as usize,
    )?;
    Ok(doc_count)
}

struct TermVectorsMergeSub<T: TermVectorsReader> {
    reader: Option<T>,
    max_doc: i32,
    doc_id: DocId,
    base: DocIdMergerSubBase,
}

impl<T: TermVectorsReader> TermVectorsMergeSub<T> {
    fn new(doc_map: Arc<LiveDocsDocMap>, reader: Option<T>, max_doc: i32) -> Self {
        let base = DocIdMergerSubBase::new(doc_map);
        TermVectorsMergeSub {
            reader,
            max_doc,
            base,
            doc_id: -1,
        }
    }
}

impl<T: TermVectorsReader> DocIdMergerSub for TermVectorsMergeSub<T> {
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

pub enum TermVectorsWriterEnum<O: IndexOutput> {
    Compressing(CompressingTermVectorsWriter<O>),
}

impl<O: IndexOutput> TermVectorsWriter for TermVectorsWriterEnum<O> {
    fn start_document(&mut self, num_vector_fields: usize) -> Result<()> {
        match self {
            TermVectorsWriterEnum::Compressing(w) => w.start_document(num_vector_fields),
        }
    }

    fn finish_document(&mut self) -> Result<()> {
        match self {
            TermVectorsWriterEnum::Compressing(w) => w.finish_document(),
        }
    }

    fn start_field(
        &mut self,
        info: &FieldInfo,
        num_terms: usize,
        has_positions: bool,
        has_offsets: bool,
        has_payloads: bool,
    ) -> Result<()> {
        match self {
            TermVectorsWriterEnum::Compressing(w) => {
                w.start_field(info, num_terms, has_positions, has_offsets, has_payloads)
            }
        }
    }

    fn finish_field(&mut self) -> Result<()> {
        match self {
            TermVectorsWriterEnum::Compressing(w) => w.finish_field(),
        }
    }

    fn start_term(&mut self, term: BytesRef, freq: i32) -> Result<()> {
        match self {
            TermVectorsWriterEnum::Compressing(w) => w.start_term(term, freq),
        }
    }

    fn finish_term(&mut self) -> Result<()> {
        match self {
            TermVectorsWriterEnum::Compressing(w) => w.finish_term(),
        }
    }

    fn add_position(
        &mut self,
        position: i32,
        start_offset: i32,
        end_offset: i32,
        payload: &[u8],
    ) -> Result<()> {
        match self {
            TermVectorsWriterEnum::Compressing(w) => {
                w.add_position(position, start_offset, end_offset, payload)
            }
        }
    }

    fn finish(&mut self, fis: &FieldInfos, num_docs: usize) -> Result<()> {
        match self {
            TermVectorsWriterEnum::Compressing(w) => w.finish(fis, num_docs),
        }
    }

    fn add_prox<I: DataInput + ?Sized>(
        &mut self,
        num_prox: usize,
        positions: Option<&mut I>,
        offsets: Option<&mut I>,
    ) -> Result<()> {
        match self {
            TermVectorsWriterEnum::Compressing(w) => w.add_prox(num_prox, positions, offsets),
        }
    }

    fn merge<D: Directory, C: Codec>(&mut self, merge_state: &mut MergeState<D, C>) -> Result<i32> {
        match self {
            TermVectorsWriterEnum::Compressing(w) => w.merge(merge_state),
        }
    }

    fn add_all_doc_vectors<D: Directory, C: Codec>(
        &mut self,
        vectors: Option<&impl Fields>,
        merge_state: &MergeState<D, C>,
    ) -> Result<()> {
        match self {
            TermVectorsWriterEnum::Compressing(w) => w.add_all_doc_vectors(vectors, merge_state),
        }
    }
}
