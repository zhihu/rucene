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

use core::codec::doc_values::lucene54::{Lucene54DocValuesConsumer, Lucene54DocValuesProducer};
use core::codec::doc_values::{DocValuesConsumerEnum, DocValuesFormat, DocValuesProducer};
use core::codec::segment_infos::{SegmentReadState, SegmentWriteState};
use core::codec::Codec;
use core::store::directory::Directory;

use error::Result;

#[derive(Copy, Clone, Default)]
pub struct Lucene54DocValuesFormat;

impl Lucene54DocValuesFormat {
    const DATA_CODEC: &'static str = "Lucene54DocValuesData";
    const DATA_EXTENSION: &'static str = "dvd";
    const META_CODEC: &'static str = "Lucene54DocValuesMetadata";
    const META_EXTENSION: &'static str = "dvm";
    pub const VERSION_START: i32 = 0;
    pub const VERSION_CURRENT: i32 = 0;

    // indicates docvalues type
    pub const NUMERIC: u8 = 0;
    pub const BINARY: u8 = 1;
    pub const SORTED: u8 = 2;
    pub const SORTED_SET: u8 = 3;
    pub const SORTED_NUMERIC: u8 = 4;

    // address terms in blocks of 16 terms
    pub const INTERVAL_SHIFT: i32 = 4;
    pub const INTERVAL_COUNT: i32 = 1 << Self::INTERVAL_SHIFT;
    pub const INTERVAL_MASK: i32 = Self::INTERVAL_COUNT - 1;

    // build reverse index from every 1024th term
    pub const REVERSE_INTERVAL_SHIFT: i32 = 10;
    pub const REVERSE_INTERVAL_COUNT: i32 = 1 << Self::REVERSE_INTERVAL_SHIFT;
    pub const REVERSE_INTERVAL_MASK: i32 = Self::REVERSE_INTERVAL_COUNT - 1;

    // for conversion from reverse index to block
    pub const BLOCK_INTERVAL_SHIFT: i32 = Self::REVERSE_INTERVAL_SHIFT - Self::INTERVAL_SHIFT;
    pub const BLOCK_INTERVAL_COUNT: i32 = 1 << Self::BLOCK_INTERVAL_SHIFT;
    pub const BLOCK_INTERVAL_MASK: i32 = Self::BLOCK_INTERVAL_COUNT - 1;

    // Compressed using packed blocks of ints
    pub const DELTA_COMPRESSED: i32 = 0;
    // Compressed by computing the GCD
    pub const GCD_COMPRESSED: i32 = 1;
    // Compressed by giving IDs to unique values
    pub const TABLE_COMPRESSED: i32 = 2;
    // Compressed with monotonically increasing values
    pub const MONOTONIC_COMPRESSED: i32 = 3;
    // Compressed with pub constant value (uses only missing bitset)
    pub const CONST_COMPRESSED: i32 = 4;
    // Compressed with sparse arrays
    pub const SPARSE_COMPRESSED: i32 = 5;

    // Uncompressed binary, written directly (fixed length)
    pub const BINARY_FIXED_UNCOMPRESSED: i32 = 0;
    // Uncompressed binary, written directly (variable length)
    pub const BINARY_VARIABLE_UNCOMPRESSED: i32 = 1;
    // Compressed binary with shared prefixes
    pub const BINARY_PREFIX_COMPRESSED: i32 = 2;

    // Standard storage for sorted set values with 1 level of indirection:
    // docId -> address -> ord
    pub const SORTED_WITH_ADDRESSES: i32 = 0;
    // Single-valued sorted set values, encoded as sorted values, so no level
    //  of indirection: docId -> ord
    pub const SORTED_SINGLE_VALUED: i32 = 1;
    // Compressed giving IDs to unique sets of values:
    //  docId -> setId -> ords
    pub const SORTED_SET_TABLE: i32 = 2;

    // placeholder for missing offset that means there are no missing values
    pub const ALL_LIVE: i32 = -1;
    // placeholder for missing offset that means all values are missing
    pub const ALL_MISSING: i32 = -2;

    // addressing uses 16k blocks
    pub const MONOTONIC_BLOCK_SIZE: i32 = 16384;
    pub const DIRECT_MONOTONIC_BLOCK_SHIFT: i32 = 16;
}

impl DocValuesFormat for Lucene54DocValuesFormat {
    fn name(&self) -> &str {
        "Lucene54"
    }
    fn fields_producer<'a, D: Directory, DW: Directory, C: Codec>(
        &self,
        state: &SegmentReadState<'a, D, DW, C>,
    ) -> Result<Box<dyn DocValuesProducer>> {
        let boxed = Lucene54DocValuesProducer::new(
            state,
            Self::DATA_CODEC,
            Self::DATA_EXTENSION,
            Self::META_CODEC,
            Self::META_EXTENSION,
        )?;
        Ok(Box::new(boxed))
    }

    fn fields_consumer<D: Directory, DW: Directory, C: Codec>(
        &self,
        state: &SegmentWriteState<D, DW, C>,
    ) -> Result<DocValuesConsumerEnum<D, DW, C>> {
        Ok(DocValuesConsumerEnum::Lucene54(
            Lucene54DocValuesConsumer::new(
                state,
                Self::DATA_CODEC,
                Self::DATA_EXTENSION,
                Self::META_CODEC,
                Self::META_EXTENSION,
            )?,
        ))
    }
}
