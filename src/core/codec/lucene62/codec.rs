use core::codec::compressing::CompressingTermVectorsFormat;
use core::codec::format::DocValuesFormat;
use core::codec::format::*;
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
            points_format: Lucene60PointsFormat::default(),
        }
    }
}

impl Codec for Lucene62Codec {
    fn name(&self) -> &str {
        "Lucene62"
    }

    fn postings_format(&self) -> &PostingsFormat {
        &self.postings_format
    }

    fn doc_values_format(&self) -> &DocValuesFormat {
        &self.doc_values_format
    }

    fn stored_fields_format(&self) -> &StoredFieldsFormat {
        &self.stored_fields_format
    }

    fn term_vectors_format(&self) -> &TermVectorsFormat {
        &self.term_vector_format
    }

    fn field_infos_format(&self) -> &FieldInfosFormat {
        &self.field_infos_format
    }

    fn segment_info_format(&self) -> &SegmentInfoFormat {
        &self.segment_info_format
    }

    fn norms_format(&self) -> &NormsFormat {
        &self.norms_format
    }

    fn live_docs_format(&self) -> &LiveDocsFormat {
        &self.live_docs_format
    }

    fn compound_format(&self) -> &CompoundFormat {
        &self.compound_format
    }

    fn points_format(&self) -> &PointsFormat {
        &self.points_format
    }
}
