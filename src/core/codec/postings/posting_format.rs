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

use core::codec::postings::blocktree::{BlockTreeTermsReader, BlockTreeTermsWriter};
use core::codec::postings::{
    FieldsConsumerEnum, Lucene50PostingsReader, Lucene50PostingsWriter, PostingsFormat,
};
use core::codec::segment_infos::{SegmentReadState, SegmentWriteState};
use core::codec::Codec;
use core::store::directory::Directory;

use error::Result;

use std::fmt;

#[derive(Hash, Eq, Ord, PartialEq, PartialOrd)]
pub struct Lucene50PostingsFormat {
    name: &'static str,
    min_term_block_size: usize,
    max_term_block_size: usize,
}

/// Fixed packed block size, number of integers encoded in
/// a single packed block.
// NOTE: must be multiple of 64 because of PackedInts long-aligned encoding/decoding
pub const BLOCK_SIZE: i32 = 128;

const DEFAULT_MIN_BLOCK_SIZE: usize = 25;
const DEFAULT_MAX_BLOCK_SIZE: usize = 48;

impl fmt::Display for Lucene50PostingsFormat {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}(blocksize={})", self.name, BLOCK_SIZE)
    }
}

impl Default for Lucene50PostingsFormat {
    fn default() -> Lucene50PostingsFormat {
        Self::with_block_size(DEFAULT_MIN_BLOCK_SIZE, DEFAULT_MAX_BLOCK_SIZE)
    }
}

impl Lucene50PostingsFormat {
    pub fn with_block_size(
        min_term_block_size: usize,
        max_term_block_size: usize,
    ) -> Lucene50PostingsFormat {
        Lucene50PostingsFormat {
            name: "Lucene50",
            min_term_block_size,
            max_term_block_size,
        }
    }
}

impl PostingsFormat for Lucene50PostingsFormat {
    type FieldsProducer = BlockTreeTermsReader;
    fn fields_producer<'a, D: Directory, DW: Directory, C: Codec>(
        &self,
        state: &SegmentReadState<'a, D, DW, C>,
    ) -> Result<Self::FieldsProducer> {
        let reader = Lucene50PostingsReader::open(&state)?;
        BlockTreeTermsReader::new(reader, state)
    }

    fn fields_consumer<D: Directory, DW: Directory, C: Codec>(
        &self,
        state: &SegmentWriteState<D, DW, C>,
    ) -> Result<FieldsConsumerEnum<D, DW, C>> {
        let postings_writer = Lucene50PostingsWriter::new(state)?;
        Ok(FieldsConsumerEnum::Lucene50(BlockTreeTermsWriter::new(
            state,
            postings_writer,
            self.min_term_block_size,
            self.max_term_block_size,
        )?))
    }

    fn name(&self) -> &str {
        "Lucene50"
    }
}
