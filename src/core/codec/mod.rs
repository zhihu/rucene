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

pub mod doc_values;
pub mod field_infos;
pub mod norms;
pub mod points;
pub mod postings;
pub mod segment_infos;
pub mod stored_fields;
pub mod term_vectors;

mod codec_util;

pub use self::codec_util::*;

mod matching_reader;

pub use self::matching_reader::*;

mod live_docs;

pub use self::live_docs::*;

mod compound;

pub use self::compound::*;

mod multi_fields;

pub use self::multi_fields::*;

mod multi_terms;

pub use self::multi_terms::*;

pub(crate) mod terms;

pub use self::terms::*;

mod fields;

pub use self::fields::*;

mod sorter;

pub use self::sorter::*;

mod posting_iterator;

pub use self::posting_iterator::*;

use core::codec::doc_values::{DocValuesFormat, DocValuesFormatEnum, PerFieldDocValuesFormat};
use core::codec::field_infos::{FieldInfosFormat, Lucene60FieldInfosFormat};
use core::codec::norms::{Lucene53NormsFormat, NormsFormat};
use core::codec::points::{Lucene60PointsFormat, PointsFormat};
use core::codec::postings::{
    FieldsProducer, PerFieldFieldsReader, PerFieldPostingsFormat, PostingsFormat,
};
use core::codec::stored_fields::{
    Lucene50StoredFieldsFormat, StoredFieldCompressMode, StoredFieldsFormat,
};
use core::codec::term_vectors::{
    term_vectors_format, CompressingTermVectorsFormat, TermVectorsFormat, TermVectorsReader,
};

use core::codec::segment_infos::{Lucene62SegmentInfoFormat, SegmentInfoFormat};
use error::ErrorKind::{CorruptIndex, IllegalArgument};
use error::{Error, Result};
use std::convert::TryFrom;
use std::sync::Arc;

#[allow(dead_code)]
pub const CHAR_BYTES: i32 = 2;
pub const INT_BYTES: i32 = 4;
pub const LONG_BYTES: i32 = 8;

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
            "Lucene62" => Ok(CodecEnum::Lucene62(Lucene62Codec::try_from(value)?)),
            _ => bail!(IllegalArgument(format!("Invalid codec name: {}", value))),
        }
    }
}

/// looks up a codec by name
pub fn codec_for_name(name: &str) -> Result<CodecEnum> {
    match name {
        "Lucene62" => Ok(CodecEnum::Lucene62(Lucene62Codec::try_from(
            name.to_string(),
        )?)),
        _ => bail!(IllegalArgument(format!("Invalid codec name: {}", name))),
    }
}

/// Implements the Lucene 6.2 index format, with configurable per-field postings
/// and docvalues formats.
pub struct Lucene62Codec {
    postings_format: PerFieldPostingsFormat,
    field_infos_format: Lucene60FieldInfosFormat,
    segment_info_format: Lucene62SegmentInfoFormat,
    compound_format: Lucene50CompoundFormat,
    term_vector_format: CompressingTermVectorsFormat,
    doc_values_format: PerFieldDocValuesFormat,
    live_docs_format: Lucene50LiveDocsFormat,
    stored_fields_format: Lucene50StoredFieldsFormat,
    norms_format: Lucene53NormsFormat,
    points_format: Lucene60PointsFormat,
}

impl Default for Lucene62Codec {
    fn default() -> Lucene62Codec {
        Lucene62Codec {
            field_infos_format: Lucene60FieldInfosFormat::default(),
            segment_info_format: Lucene62SegmentInfoFormat::default(),
            postings_format: PerFieldPostingsFormat::default(),
            compound_format: Lucene50CompoundFormat {},
            term_vector_format: term_vectors_format(),
            live_docs_format: Lucene50LiveDocsFormat {},
            stored_fields_format: Lucene50StoredFieldsFormat::new(Some(
                StoredFieldCompressMode::BestSpeed,
            )),
            doc_values_format: PerFieldDocValuesFormat::default(),
            norms_format: Lucene53NormsFormat::default(),
            points_format: Lucene60PointsFormat {},
        }
    }
}

impl Codec for Lucene62Codec {
    type FieldsProducer = Arc<PerFieldFieldsReader>;
    type PostingFmt = PerFieldPostingsFormat;
    type DVFmt = PerFieldDocValuesFormat;
    type StoredFmt = Lucene50StoredFieldsFormat;
    type TVFmt = CompressingTermVectorsFormat;
    type FieldFmt = Lucene60FieldInfosFormat;
    type SegmentFmt = Lucene62SegmentInfoFormat;
    type NormFmt = Lucene53NormsFormat;
    type LiveDocFmt = Lucene50LiveDocsFormat;
    type CompoundFmt = Lucene50CompoundFormat;
    type PointFmt = Lucene60PointsFormat;

    fn name(&self) -> &str {
        "Lucene62"
    }

    fn postings_format(&self) -> Self::PostingFmt {
        self.postings_format
    }

    fn doc_values_format(&self) -> Self::DVFmt {
        self.doc_values_format
    }

    fn stored_fields_format(&self) -> Self::StoredFmt {
        self.stored_fields_format
    }

    fn term_vectors_format(&self) -> Self::TVFmt {
        self.term_vector_format.clone()
    }

    fn field_infos_format(&self) -> Self::FieldFmt {
        self.field_infos_format
    }

    fn segment_info_format(&self) -> Self::SegmentFmt {
        self.segment_info_format
    }

    fn norms_format(&self) -> Self::NormFmt {
        self.norms_format
    }

    fn live_docs_format(&self) -> Self::LiveDocFmt {
        self.live_docs_format
    }

    fn compound_format(&self) -> Self::CompoundFmt {
        self.compound_format
    }

    fn points_format(&self) -> Self::PointFmt {
        self.points_format
    }
}

impl TryFrom<String> for Lucene62Codec {
    type Error = Error;

    fn try_from(value: String) -> Result<Self> {
        if value.as_str() == "Lucene62" {
            Ok(Self::default())
        } else {
            bail!(CorruptIndex(format!(
                "unknown codec name, expected 'Lucene62' got {:?}",
                value
            )))
        }
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
