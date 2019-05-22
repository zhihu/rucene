use std::str::FromStr;
use std::sync::Arc;

use core::codec::compressing::{
    CompressingStoredFieldsFormat, CompressingStoredFieldsReader, CompressionMode,
};
use core::codec::format::StoredFieldsFormat;
use core::codec::writer::StoredFieldsWriterEnum;
use core::codec::Codec;
use core::index::field_info::FieldInfos;
use core::index::SegmentInfo;
use core::store::{Directory, IOContext};
use error::{Error as CoreError, ErrorKind::IllegalState, Result};

const MODE_KEY: &str = "Lucene50StoredFieldsFormat.mode";

#[derive(Debug, Copy, Clone)]
pub enum StoredFieldCompressMode {
    BestSpeed,
    BestCompression,
}

impl FromStr for StoredFieldCompressMode {
    type Err = CoreError;
    fn from_str(v: &str) -> Result<Self> {
        let r = if v == "BEST_SPEED" {
            StoredFieldCompressMode::BestSpeed
        } else {
            StoredFieldCompressMode::BestCompression
        };
        Ok(r)
    }
}

#[derive(Copy, Clone)]
pub struct Lucene50StoredFieldsFormat {
    #[allow(dead_code)]
    mode: StoredFieldCompressMode,
}

impl Lucene50StoredFieldsFormat {
    pub fn new(mode: Option<StoredFieldCompressMode>) -> Lucene50StoredFieldsFormat {
        if let Some(m) = mode {
            Lucene50StoredFieldsFormat { mode: m }
        } else {
            Lucene50StoredFieldsFormat {
                mode: StoredFieldCompressMode::BestSpeed,
            }
        }
    }

    pub fn format(&self, mode: &StoredFieldCompressMode) -> CompressingStoredFieldsFormat {
        match mode {
            StoredFieldCompressMode::BestSpeed => CompressingStoredFieldsFormat::new(
                "Lucene50StoredFieldsFast",
                "",
                CompressionMode::FAST,
                1 << 14,
                128,
                1024,
            ),
            StoredFieldCompressMode::BestCompression => CompressingStoredFieldsFormat::new(
                "Lucene50StoredFieldsHigh",
                "",
                CompressionMode::HighCompression,
                61440,
                512,
                1024,
            ),
        }
    }
}

impl StoredFieldsFormat for Lucene50StoredFieldsFormat {
    type Reader = CompressingStoredFieldsReader;
    fn fields_reader<D: Directory, DW: Directory, C: Codec>(
        &self,
        directory: &DW,
        si: &SegmentInfo<D, C>,
        field_info: Arc<FieldInfos>,
        ioctx: &IOContext,
    ) -> Result<Self::Reader> {
        if let Some(value) = si.attributes.get(MODE_KEY) {
            let mode = StoredFieldCompressMode::from_str(value)?;

            self.format(&mode)
                .fields_reader(directory, si, field_info, ioctx)
        } else {
            bail!(IllegalState(format!(
                "missing value for {} for segment: {}",
                MODE_KEY, si.name
            )))
        }
    }

    fn fields_writer<D, DW, C>(
        &self,
        directory: Arc<DW>,
        si: &mut SegmentInfo<D, C>,
        ioctx: &IOContext,
    ) -> Result<StoredFieldsWriterEnum<DW::IndexOutput>>
    where
        D: Directory,
        DW: Directory,
        DW::IndexOutput: 'static,
        C: Codec,
    {
        if si.attributes.contains_key(MODE_KEY) {
            bail!(IllegalState(format!(
                "found existing value for {} for segment: {}",
                MODE_KEY, si.name
            )))
        }

        si.attributes
            .insert(MODE_KEY.to_string(), "BEST_SPEED".to_string());
        self.format(&self.mode).fields_writer(directory, si, ioctx)
    }
}
