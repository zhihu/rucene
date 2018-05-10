use std::str::FromStr;
use std::sync::Arc;

use core::codec::compressing::{CompressingStoredFieldsFormat, CompressionMode};
use core::codec::format::StoredFieldsFormat;
use core::codec::reader::StoredFieldsReader;
use core::codec::writer::StoredFieldsWriter;
use core::index::field_info::FieldInfos;
use core::index::SegmentInfo;
use core::store::{DirectoryRc, IOContext};
use error::{Error as CoreError, Result};

const MODE_KEY: &str = "Lucene50StoredFieldsFormat.mode";

#[derive(Debug)]
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

    pub fn format(&self, mode: &StoredFieldCompressMode) -> Result<Box<StoredFieldsFormat>> {
        match mode {
            StoredFieldCompressMode::BestSpeed => Ok(Box::new(CompressingStoredFieldsFormat::new(
                "Lucene50StoredFieldsFast",
                "",
                CompressionMode::FAST,
                1 << 14,
                128,
                1024,
            ))),
            StoredFieldCompressMode::BestCompression => {
                Ok(Box::new(CompressingStoredFieldsFormat::new(
                    "Lucene50StoredFieldsHigh",
                    "",
                    CompressionMode::HighCompression,
                    61440,
                    512,
                    1024,
                )))
            }
        }
    }
}

impl StoredFieldsFormat for Lucene50StoredFieldsFormat {
    fn fields_reader(
        &self,
        directory: DirectoryRc,
        si: &SegmentInfo,
        field_info: Arc<FieldInfos>,
        ioctx: &IOContext,
    ) -> Result<Box<StoredFieldsReader>> {
        if let Some(value) = si.attributes.get(MODE_KEY) {
            let mode = StoredFieldCompressMode::from_str(value)?;

            self.format(&mode)?
                .fields_reader(directory, si, field_info, ioctx)
        } else {
            bail!("missing value for {} for segment: {}", MODE_KEY, si.name)
        }
    }

    fn fields_writer(
        &self,
        _directory: DirectoryRc,
        _si: &SegmentInfo,
        _ioctx: &IOContext,
    ) -> Result<Box<StoredFieldsWriter>> {
        unimplemented!()
    }
}
