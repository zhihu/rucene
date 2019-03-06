use core::codec::format::NormsFormat;
use core::codec::lucene53::norms_consumer::Lucene53NormsConsumer;
use core::codec::lucene53::norms_producer::Lucene53NormsProducer;
use core::codec::NormsConsumer;
use core::codec::NormsProducer;
use core::index::{SegmentReadState, SegmentWriteState};

use error::Result;

pub const DATA_CODEC: &str = "Lucene53NormsData";
pub const DATA_EXTENSION: &str = "nvd";
pub const METADATA_CODEC: &str = "Lucene53NormsMetadata";
pub const METADATA_EXTENSION: &str = "nvm";
pub const VERSION_START: i32 = 0;
pub const VERSION_CURRENT: i32 = VERSION_START;

pub struct Lucene53NormsFormat;

impl Default for Lucene53NormsFormat {
    fn default() -> Lucene53NormsFormat {
        Lucene53NormsFormat {}
    }
}

impl NormsFormat for Lucene53NormsFormat {
    fn norms_producer(&self, state: &SegmentReadState) -> Result<Box<NormsProducer>> {
        Ok(Box::new(Lucene53NormsProducer::new(
            state,
            DATA_CODEC,
            DATA_EXTENSION,
            METADATA_CODEC,
            METADATA_EXTENSION,
        )?))
    }

    fn norms_consumer(&self, state: &SegmentWriteState) -> Result<Box<NormsConsumer>> {
        Ok(Box::new(Lucene53NormsConsumer::new(
            state,
            DATA_CODEC,
            DATA_EXTENSION,
            METADATA_CODEC,
            METADATA_EXTENSION,
        )?))
    }
}
