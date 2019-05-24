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

use core::codec::compressing::CompressingTermVectorsFormat;
use core::codec::lucene50::term_vectors_format;
use core::codec::lucene50::Lucene50CompoundFormat;
use core::codec::lucene50::Lucene50LiveDocsFormat;
use core::codec::lucene50::Lucene50StoredFieldsFormat;
use core::codec::lucene50::StoredFieldCompressMode;
use core::codec::lucene53::Lucene53NormsFormat;
use core::codec::lucene60::Lucene60FieldInfosFormat;
use core::codec::lucene60::Lucene60PointsFormat;
use core::codec::lucene62::Lucene62SegmentInfoFormat;
use core::codec::Codec;
use core::codec::{PerFieldDocValuesFormat, PerFieldPostingsFormat};

use error::{Error, ErrorKind};

use core::codec::per_field::PerFieldFieldsReader;
use std::convert::TryFrom;
use std::sync::Arc;

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

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.as_str() == "Lucene62" {
            Ok(Self::default())
        } else {
            bail!(ErrorKind::CorruptIndex(format!(
                "unknown codec name, expected 'Lucene62' got {:?}",
                value
            )))
        }
    }
}
