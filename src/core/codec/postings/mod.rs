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

pub mod blocktree;

mod for_util;

pub use self::for_util::*;

mod posting_format;

pub use self::posting_format::*;

mod per_field_postings_format;

pub use self::per_field_postings_format::*;

mod posting_reader;

pub use self::posting_reader::*;

mod posting_writer;

pub use self::posting_writer::*;

mod skip_reader;

pub use self::skip_reader::*;

mod skip_writer;

pub use self::skip_writer::*;

mod terms_hash;

pub use self::terms_hash::*;

mod terms_hash_per_field;

pub use self::terms_hash_per_field::*;

mod partial_block_decoder;

pub use self::partial_block_decoder::*;

mod simd_block_decoder;

pub use self::simd_block_decoder::*;

use core::codec::field_infos::FieldInfo;
use core::codec::multi_fields::{MappedMultiFields, MultiFields};
use core::codec::postings::blocktree::{
    BlockTermState, BlockTreeTermsReader, BlockTreeTermsWriter, FieldReaderRef,
};
use core::codec::segment_infos::{SegmentReadState, SegmentWriteState};
use core::codec::{Codec, Fields, TermIterator};
use core::index::merge::MergeState;
use core::index::reader::ReaderSlice;
use core::store::directory::Directory;
use core::store::io::{DataOutput, IndexOutput};
use core::util::over_size;
use core::util::FixedBitSet;

use error::ErrorKind::IllegalArgument;
use error::Result;
use std::sync::Arc;

// sometimes will cause miss increasing with phrase/highlight.
// pub const DEFAULT_SEGMENT_DOC_FREQ: i32 = 500_000;
// for dmp
pub const DEFAULT_SEGMENT_DOC_FREQ: i32 = 1_000_000_000;
pub const DEFAULT_DOC_TERM_FREQ: i32 = 10;

/// Encodes/decodes terms, postings, and proximity data.
/// <p>
/// Note, when extending this class, the name ({@link #getName}) may
/// written into the index in certain configurations. In order for the segment
/// to be read, the name must resolve to your implementation via {@link #forName(String)}.
/// This method uses Java's
/// {@link ServiceLoader Service Provider Interface} (SPI) to resolve format names.
/// <p>
/// If you implement your own format, make sure that it has a no-arg constructor
/// so SPI can load it.
/// @see ServiceLoader
/// @lucene.experimental
pub trait PostingsFormat {
    type FieldsProducer: FieldsProducer;
    /// Reads a segment.  NOTE: by the time this call
    /// returns, it must hold open any files it will need to
    /// use; else, those files may be deleted.
    /// Additionally, required files may be deleted during the execution of
    /// this call before there is a chance to open them. Under these
    /// circumstances an IOException should be thrown by the implementation.
    /// IOExceptions are expected and will automatically cause a retry of the
    /// segment opening logic with the newly revised segments.
    fn fields_producer<D: Directory, DW: Directory, C: Codec>(
        &self,
        state: &SegmentReadState<'_, D, DW, C>,
    ) -> Result<Self::FieldsProducer>;

    /// Writes a new segment
    fn fields_consumer<D: Directory, DW: Directory, C: Codec>(
        &self,
        state: &SegmentWriteState<D, DW, C>,
    ) -> Result<FieldsConsumerEnum<D, DW, C>>;

    /// Returns this posting format's name
    fn name(&self) -> &str;
}

/// composite `PostingsFormat` use for `CodecEnum`
pub enum PostingsFormatEnum {
    Lucene50(Lucene50PostingsFormat),
}

impl PostingsFormat for PostingsFormatEnum {
    type FieldsProducer = FieldsProducerEnum;

    fn fields_producer<D: Directory, DW: Directory, C: Codec>(
        &self,
        state: &SegmentReadState<'_, D, DW, C>,
    ) -> Result<Self::FieldsProducer> {
        match self {
            PostingsFormatEnum::Lucene50(f) => {
                Ok(FieldsProducerEnum::Lucene50(f.fields_producer(state)?))
            }
        }
    }

    fn fields_consumer<D: Directory, DW: Directory, C: Codec>(
        &self,
        state: &SegmentWriteState<D, DW, C>,
    ) -> Result<FieldsConsumerEnum<D, DW, C>> {
        match self {
            PostingsFormatEnum::Lucene50(f) => f.fields_consumer(state),
        }
    }

    fn name(&self) -> &str {
        match self {
            PostingsFormatEnum::Lucene50(f) => f.name(),
        }
    }
}

pub fn postings_format_for_name(name: &str) -> Result<PostingsFormatEnum> {
    match name {
        "Lucene50" => Ok(PostingsFormatEnum::Lucene50(
            Lucene50PostingsFormat::default(),
        )),
        _ => bail!(IllegalArgument(format!(
            "Invalid postings format: {}",
            name
        ))),
    }
}

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

///  Abstract API that produces terms, doc, freq, prox, offset and payloads postings
pub trait FieldsProducer: Fields {
    /// Checks consistency of this reader.
    /// Note that this may be costly in terms of I/O, e.g.
    /// may involve computing a checksum value against large data files.
    fn check_integrity(&self) -> Result<()>;

    // Returns an instance optimized for merging.
    // fn get_merge_instance(&self) -> Result<FieldsProducerRef>;
}

// this type should be Arc<Codec::PostingsFormat::FieldsProducer>
pub type FieldsProducerRef = Arc<PerFieldFieldsReader>;

impl<T: FieldsProducer> FieldsProducer for Arc<T> {
    fn check_integrity(&self) -> Result<()> {
        (**self).check_integrity()
    }
}

impl<T: FieldsProducer> Fields for Arc<T> {
    type Terms = T::Terms;
    fn fields(&self) -> Vec<String> {
        (**self).fields()
    }

    fn terms(&self, field: &str) -> Result<Option<Self::Terms>> {
        (**self).terms(field)
    }

    fn size(&self) -> usize {
        (**self).size()
    }

    fn terms_freq(&self, field: &str) -> usize {
        (**self).terms_freq(field)
    }
}

/// `FieldsProducer` impl for `PostingsFormatEnum`
pub enum FieldsProducerEnum {
    Lucene50(BlockTreeTermsReader),
}

impl FieldsProducer for FieldsProducerEnum {
    fn check_integrity(&self) -> Result<()> {
        match self {
            FieldsProducerEnum::Lucene50(f) => f.check_integrity(),
        }
    }
}

impl Fields for FieldsProducerEnum {
    type Terms = FieldReaderRef;
    fn fields(&self) -> Vec<String> {
        match self {
            FieldsProducerEnum::Lucene50(f) => f.fields(),
        }
    }

    fn terms(&self, field: &str) -> Result<Option<Self::Terms>> {
        match self {
            FieldsProducerEnum::Lucene50(f) => f.terms(field),
        }
    }

    fn size(&self) -> usize {
        match self {
            FieldsProducerEnum::Lucene50(f) => f.size(),
        }
    }

    fn terms_freq(&self, field: &str) -> usize {
        match self {
            FieldsProducerEnum::Lucene50(f) => f.terms_freq(field),
        }
    }
}

pub trait PostingsWriterBase {
    /// Called once after startup, before any terms have been
    /// added.  Implementations typically write a header to
    /// the provided {@code termsOut}.
    fn init<D: Directory, DW: Directory, C: Codec>(
        &mut self,
        terms_out: &mut impl IndexOutput,
        state: &SegmentWriteState<D, DW, C>,
    ) -> Result<()>;

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
        terms: &mut impl TermIterator,
        docs_seen: &mut FixedBitSet,
        doc_freq_limit: i32,
        term_freq_limit: i32,
    ) -> Result<Option<BlockTermState>>;

    /// Encode metadata as [i64] and [u8]. {@param absolute} controls whether
    /// current term is delta encoded according to latest term.
    /// Usually elements in {@code longs} are file pointers, so each one always
    /// increases when a new term is consumed. {@param out} is used to write generic
    /// bytes, which are not monotonic.
    ///
    /// NOTE: sometimes [i64] might contain "don't care" values that are unused, e.g.
    /// the pointer to postings list may not be defined for some terms but is defined
    /// for others, if it is designed to inline  some postings data in term dictionary.
    /// In this case, the postings writer should always use the last value, so that each
    /// element in metadata [i64] remains monotonic.
    fn encode_term(
        &mut self,
        longs: &mut [i64],
        out: &mut impl DataOutput,
        field_info: &FieldInfo,
        state: &BlockTermState,
        absolute: bool,
    ) -> Result<()>;

    /// Sets the current field for writing, and returns the
    /// fixed length of [i64] metadata (which is fixed per
    /// field), called when the writing switches to another field.
    // TODO: better name?
    fn set_field(&mut self, field_info: &FieldInfo) -> i32;
}

const BYTES_PER_POSTING: usize = 3 * 4; // 3 * sizeof(i32)

pub trait PostingsArray: Default {
    fn parallel_array(&self) -> &ParallelPostingsArray;

    fn parallel_array_mut(&mut self) -> &mut ParallelPostingsArray;

    fn bytes_per_posting(&self) -> usize;

    fn grow(&mut self);

    fn clear(&mut self);
}

pub struct ParallelPostingsArray {
    pub size: usize,
    pub text_starts: Vec<u32>,
    pub int_starts: Vec<u32>,
    pub byte_starts: Vec<u32>,
}

impl Default for ParallelPostingsArray {
    fn default() -> Self {
        ParallelPostingsArray::new(2)
    }
}

impl ParallelPostingsArray {
    pub fn new(size: usize) -> Self {
        ParallelPostingsArray {
            size,
            text_starts: vec![0u32; size],
            int_starts: vec![0u32; size],
            byte_starts: vec![0u32; size],
        }
    }
}

impl PostingsArray for ParallelPostingsArray {
    fn parallel_array(&self) -> &ParallelPostingsArray {
        self
    }

    fn parallel_array_mut(&mut self) -> &mut ParallelPostingsArray {
        self
    }

    fn bytes_per_posting(&self) -> usize {
        BYTES_PER_POSTING
    }

    fn grow(&mut self) {
        self.size = over_size(self.size + 1);
        let new_size = self.size;
        self.text_starts.resize(new_size, 0u32);
        self.int_starts.resize(new_size, 0u32);
        self.byte_starts.resize(new_size, 0u32);
    }

    fn clear(&mut self) {
        self.size = 0;
        self.text_starts = Vec::with_capacity(0);
        self.int_starts = Vec::with_capacity(0);
        self.byte_starts = Vec::with_capacity(0);
    }
}
