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

use core::codec::norms::{
    Lucene53NormsConsumer, Lucene53NormsProducer, NormsConsumerEnum, NormsFormat,
};
use core::codec::segment_infos::{SegmentReadState, SegmentWriteState};
use core::codec::Codec;
use core::store::directory::Directory;

use error::Result;

pub const DATA_CODEC: &str = "Lucene53NormsData";
pub const DATA_EXTENSION: &str = "nvd";
pub const METADATA_CODEC: &str = "Lucene53NormsMetadata";
pub const METADATA_EXTENSION: &str = "nvm";
pub const VERSION_START: i32 = 0;
pub const VERSION_CURRENT: i32 = VERSION_START;

#[derive(Copy, Clone, Default)]
pub struct Lucene53NormsFormat;

impl NormsFormat for Lucene53NormsFormat {
    type NormsProducer = Lucene53NormsProducer;
    fn norms_producer<'a, D: Directory, DW: Directory, C: Codec>(
        &self,
        state: &SegmentReadState<'a, D, DW, C>,
    ) -> Result<Self::NormsProducer> {
        Lucene53NormsProducer::new(
            state,
            DATA_CODEC,
            DATA_EXTENSION,
            METADATA_CODEC,
            METADATA_EXTENSION,
        )
    }

    fn norms_consumer<D: Directory, DW: Directory, C: Codec>(
        &self,
        state: &SegmentWriteState<D, DW, C>,
    ) -> Result<NormsConsumerEnum<DW::IndexOutput>> {
        Ok(NormsConsumerEnum::Lucene53(Lucene53NormsConsumer::new(
            state,
            DATA_CODEC,
            DATA_EXTENSION,
            METADATA_CODEC,
            METADATA_EXTENSION,
        )?))
    }
}
