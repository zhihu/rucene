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

mod per_field;
pub use self::per_field::*;

mod blocktree;
pub use self::blocktree::*;

pub mod codec_util;

mod compressing;
pub use self::compressing::*;

mod format;
pub use self::format::*;

mod lucene50;
pub use self::lucene50::*;

mod lucene53;
pub use self::lucene53::*;

mod lucene54;
pub use self::lucene54::*;

mod lucene60;
pub use self::lucene60::*;

mod lucene62;
pub use self::lucene62::*;

mod reader;
pub use self::reader::*;

mod writer;
pub use self::writer::*;

mod consumer;
pub use self::consumer::*;

mod producer;
pub use self::producer::*;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use core::index::{Fields, TermIterator, TermState, Terms};
use error::ErrorKind::*;
use error::{Error, Result};
use std::convert::TryFrom;
use std::sync::Arc;

const BLOCK_TERM_STATE_SERIALIZED_SIZE: usize = 76;

#[allow(dead_code)]
pub(crate) const CHAR_BYTES: i32 = 2;
pub(crate) const INT_BYTES: i32 = 4;
pub(crate) const LONG_BYTES: i32 = 8;

/// Holds all state required for `PostingsReaderBase` to produce a
/// `PostingIterator` without re-seeking the term dict.
#[derive(Clone, Debug)]
pub struct BlockTermState {
    /// Term ordinal, i.e. its position in the full list of
    /// sorted terms.
    ord: i64,
    /// how many docs have this term
    doc_freq: i32,

    /// total number of occurrences of this term
    total_term_freq: i64,

    /// the term's ord in the current block
    term_block_ord: i32,

    /// fp into the terms dict primary file (_X.tim) that holds this term
    // TODO: update BTR to nuke this
    block_file_pointer: i64,

    /// fields from IntBlockTermState
    doc_start_fp: i64,
    pos_start_fp: i64,
    pay_start_fp: i64,
    skip_offset: i64,
    last_pos_block_offset: i64,
    // docid when there is a single pulsed posting, otherwise -1
    // freq is always implicitly totalTermFreq in this case.
    singleton_doc_id: i32,
}

impl BlockTermState {
    fn new() -> BlockTermState {
        BlockTermState {
            ord: 0,
            doc_freq: 0,
            total_term_freq: 0,
            term_block_ord: 0,
            block_file_pointer: 0,

            doc_start_fp: 0,
            pos_start_fp: 0,
            pay_start_fp: 0,
            skip_offset: -1,
            last_pos_block_offset: -1,
            singleton_doc_id: -1,
        }
    }

    pub fn copy_from(&mut self, other: &BlockTermState) {
        self.ord = other.ord;
        self.doc_freq = other.doc_freq;
        self.total_term_freq = other.total_term_freq;
        self.term_block_ord = other.term_block_ord;
        self.block_file_pointer = other.block_file_pointer;
        self.doc_start_fp = other.doc_start_fp;
        self.pos_start_fp = other.pos_start_fp;
        self.pay_start_fp = other.pay_start_fp;
        self.last_pos_block_offset = other.last_pos_block_offset;
        self.skip_offset = other.skip_offset;
        self.singleton_doc_id = other.singleton_doc_id;
    }

    pub fn deserialize(from: &[u8]) -> Result<BlockTermState> {
        // 76 bytes in total
        let mut from = from;
        if from.len() != BLOCK_TERM_STATE_SERIALIZED_SIZE {
            bail!(IllegalArgument(
                "Serialized bytes is not for BlockTermState".into()
            ))
        }
        let ord = from.read_i64::<LittleEndian>()?;
        let doc_freq = from.read_i32::<LittleEndian>()?;

        let total_term_freq = from.read_i64::<LittleEndian>()?;
        let term_block_ord = from.read_i32::<LittleEndian>()?;

        let block_file_pointer = from.read_i64::<LittleEndian>()?;

        let doc_start_fp = from.read_i64::<LittleEndian>()?;
        let pos_start_fp = from.read_i64::<LittleEndian>()?;
        let pay_start_fp = from.read_i64::<LittleEndian>()?;
        let skip_offset = from.read_i64::<LittleEndian>()?;
        let last_pos_block_offset = from.read_i64::<LittleEndian>()?;
        let singleton_doc_id = from.read_i32::<LittleEndian>()?;
        Ok(BlockTermState {
            ord,
            doc_freq,
            total_term_freq,
            term_block_ord,
            block_file_pointer,

            doc_start_fp,
            pos_start_fp,
            pay_start_fp,
            skip_offset,
            last_pos_block_offset,
            singleton_doc_id,
        })
    }

    pub fn ord(&self) -> i64 {
        self.ord
    }

    pub fn doc_freq(&self) -> i32 {
        self.doc_freq
    }

    pub fn total_term_freq(&self) -> i64 {
        self.total_term_freq
    }

    pub fn term_block_ord(&self) -> i32 {
        self.term_block_ord
    }

    pub fn block_file_pointer(&self) -> i64 {
        self.block_file_pointer
    }

    pub fn doc_start_fp(&self) -> i64 {
        self.doc_start_fp
    }
    pub fn pos_start_fp(&self) -> i64 {
        self.pos_start_fp
    }
    pub fn pay_start_fp(&self) -> i64 {
        self.pay_start_fp
    }
    pub fn skip_offset(&self) -> i64 {
        self.skip_offset
    }
    pub fn last_pos_block_offset(&self) -> i64 {
        self.last_pos_block_offset
    }
    pub fn singleton_doc_id(&self) -> i32 {
        self.singleton_doc_id
    }
}

impl TermState for BlockTermState {
    fn ord(&self) -> i64 {
        self.ord
    }

    fn serialize(&self) -> Vec<u8> {
        let mut buffer = Vec::with_capacity(BLOCK_TERM_STATE_SERIALIZED_SIZE);
        buffer.write_i64::<LittleEndian>(self.ord).unwrap();
        buffer.write_i32::<LittleEndian>(self.doc_freq).unwrap();

        buffer
            .write_i64::<LittleEndian>(self.total_term_freq)
            .unwrap();
        buffer
            .write_i32::<LittleEndian>(self.term_block_ord)
            .unwrap();

        buffer
            .write_i64::<LittleEndian>(self.block_file_pointer)
            .unwrap();

        buffer.write_i64::<LittleEndian>(self.doc_start_fp).unwrap();
        buffer.write_i64::<LittleEndian>(self.pos_start_fp).unwrap();
        buffer.write_i64::<LittleEndian>(self.pay_start_fp).unwrap();
        buffer.write_i64::<LittleEndian>(self.skip_offset).unwrap();
        buffer
            .write_i64::<LittleEndian>(self.last_pos_block_offset)
            .unwrap();
        buffer
            .write_i32::<LittleEndian>(self.singleton_doc_id)
            .unwrap();
        debug_assert!(buffer.len() == BLOCK_TERM_STATE_SERIALIZED_SIZE);
        buffer
    }
}

/// Encodes/decodes an inverted index segment.
///
/// Note, when extending this class, the name `get_name` is
/// written into the index. In order for the segment to be read, the
/// name must resolve to your implementation via {@link TryFrom::try_from(String)}.
pub trait Codec: TryFrom<String, Error = Error> + 'static {
    type FieldsProducer: FieldsProducer + Clone;
    type PostingFmt: PostingsFormat<FieldsProducer = Self::FieldsProducer>;
    type DVFmt: DocValuesFormat;
    type StoredFmt: StoredFieldsFormat;
    type TVFmt: TermVectorsFormat;
    type FieldFmt: FieldInfosFormat;
    type SegmentFmt: SegmentInfoFormat;
    type NormFmt: NormsFormat;
    type LiveDocFmt: LiveDocsFormat;
    type CompoundFmt: CompoundFormat;
    type PointFmt: PointsFormat;

    fn name(&self) -> &str;
    fn postings_format(&self) -> Self::PostingFmt;
    fn doc_values_format(&self) -> Self::DVFmt;
    fn stored_fields_format(&self) -> Self::StoredFmt;
    fn term_vectors_format(&self) -> Self::TVFmt;
    fn field_infos_format(&self) -> Self::FieldFmt;
    fn segment_info_format(&self) -> Self::SegmentFmt;
    fn norms_format(&self) -> Self::NormFmt;
    fn live_docs_format(&self) -> Self::LiveDocFmt;
    fn compound_format(&self) -> Self::CompoundFmt;
    fn points_format(&self) -> Self::PointFmt;
}

pub type CodecFieldsProducer<C> = <<C as Codec>::PostingFmt as PostingsFormat>::FieldsProducer;
pub type CodecStoredFieldsReader<C> = <<C as Codec>::StoredFmt as StoredFieldsFormat>::Reader;
pub type CodecTerms<C> =
    <<<C as Codec>::PostingFmt as PostingsFormat>::FieldsProducer as Fields>::Terms;
pub type CodecTermIterator<C> = <<<<C as Codec>::PostingFmt as PostingsFormat>::FieldsProducer as Fields>::Terms as Terms>::Iterator;
pub type CodecPostingIterator<C> = <<<<<C as Codec>::PostingFmt as PostingsFormat>::FieldsProducer as Fields>::Terms as Terms>::Iterator as TermIterator>::Postings;
pub type CodecTermState<C> = <<<<<C as Codec>::PostingFmt as PostingsFormat>::FieldsProducer as Fields>::Terms as Terms>::Iterator as TermIterator>::TermState;
pub type CodecTVReader<C> = <<C as Codec>::TVFmt as TermVectorsFormat>::TVReader;
pub type CodecTVFields<C> =
    <<<C as Codec>::TVFmt as TermVectorsFormat>::TVReader as TermVectorsReader>::Fields;
pub type CodecNormsProducer<C> = <<C as Codec>::NormFmt as NormsFormat>::NormsProducer;
pub type CodecPointsReader<C> = <<C as Codec>::PointFmt as PointsFormat>::Reader;

pub enum CodecEnum {
    Lucene62(Lucene62Codec),
}

impl Codec for CodecEnum {
    type FieldsProducer = Arc<PerFieldFieldsReader>;
    type PostingFmt = PerFieldPostingsFormat;
    type DVFmt = DocValuesFormatEnum;
    type StoredFmt = Lucene50StoredFieldsFormat;
    type TVFmt = CompressingTermVectorsFormat;
    type FieldFmt = Lucene60FieldInfosFormat;
    type SegmentFmt = Lucene62SegmentInfoFormat;
    type NormFmt = Lucene53NormsFormat;
    type LiveDocFmt = Lucene50LiveDocsFormat;
    type CompoundFmt = Lucene50CompoundFormat;
    type PointFmt = Lucene60PointsFormat;

    fn name(&self) -> &str {
        match self {
            CodecEnum::Lucene62(c) => c.name(),
        }
    }
    fn postings_format(&self) -> Self::PostingFmt {
        match self {
            CodecEnum::Lucene62(c) => c.postings_format(),
        }
    }
    fn doc_values_format(&self) -> Self::DVFmt {
        match self {
            CodecEnum::Lucene62(c) => DocValuesFormatEnum::PerField(c.doc_values_format()),
        }
    }
    fn stored_fields_format(&self) -> Self::StoredFmt {
        match self {
            CodecEnum::Lucene62(c) => c.stored_fields_format(),
        }
    }
    fn term_vectors_format(&self) -> Self::TVFmt {
        match self {
            CodecEnum::Lucene62(c) => c.term_vectors_format(),
        }
    }
    fn field_infos_format(&self) -> Self::FieldFmt {
        match self {
            CodecEnum::Lucene62(c) => c.field_infos_format(),
        }
    }
    fn segment_info_format(&self) -> Self::SegmentFmt {
        match self {
            CodecEnum::Lucene62(c) => c.segment_info_format(),
        }
    }
    fn norms_format(&self) -> Self::NormFmt {
        match self {
            CodecEnum::Lucene62(c) => c.norms_format(),
        }
    }
    fn live_docs_format(&self) -> Self::LiveDocFmt {
        match self {
            CodecEnum::Lucene62(c) => c.live_docs_format(),
        }
    }
    fn compound_format(&self) -> Self::CompoundFmt {
        match self {
            CodecEnum::Lucene62(c) => c.compound_format(),
        }
    }

    /// Encodes/decodes points index
    fn points_format(&self) -> Self::PointFmt {
        match self {
            CodecEnum::Lucene62(c) => c.points_format(),
        }
    }
}

impl TryFrom<String> for CodecEnum {
    type Error = Error;

    fn try_from(value: String) -> Result<Self> {
        match value.as_str() {
            "Lucene62" => Ok(CodecEnum::Lucene62(lucene62::Lucene62Codec::try_from(
                value,
            )?)),
            _ => bail!(IllegalArgument(format!("Invalid codec name: {}", value))),
        }
    }
}

/// looks up a codec by name
pub fn codec_for_name(name: &str) -> Result<CodecEnum> {
    match name {
        "Lucene62" => Ok(CodecEnum::Lucene62(lucene62::Lucene62Codec::try_from(
            name.to_string(),
        )?)),
        _ => bail!(IllegalArgument(format!("Invalid codec name: {}", name))),
    }
}

#[cfg(test)]
pub mod tests {
    use core::codec::{CodecEnum, Lucene62Codec};

    pub type TestCodec = CodecEnum;

    impl Default for CodecEnum {
        fn default() -> Self {
            CodecEnum::Lucene62(Lucene62Codec::default())
        }
    }
}
