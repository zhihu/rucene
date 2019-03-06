use core::codec::format::DocValuesFormat;
use core::codec::lucene54::Lucene54DocValuesConsumer;
use core::codec::lucene54::Lucene54DocValuesProducer;
use core::codec::{DocValuesConsumer, DocValuesProducer};
use core::index::{SegmentReadState, SegmentWriteState};
use error::Result;

pub const DATA_CODEC: &str = "Lucene54DocValuesData";
pub const DATA_EXTENSION: &str = "dvd";
pub const META_CODEC: &str = "Lucene54DocValuesMetadata";
pub const META_EXTENSION: &str = "dvm";
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
pub const INTERVAL_COUNT: i32 = 1 << INTERVAL_SHIFT;
pub const INTERVAL_MASK: i32 = INTERVAL_COUNT - 1;

// build reverse index from every 1024th term
pub const REVERSE_INTERVAL_SHIFT: i32 = 10;
pub const REVERSE_INTERVAL_COUNT: i32 = 1 << REVERSE_INTERVAL_SHIFT;
pub const REVERSE_INTERVAL_MASK: i32 = REVERSE_INTERVAL_COUNT - 1;

// for conversion from reverse index to block
pub const BLOCK_INTERVAL_SHIFT: i32 = REVERSE_INTERVAL_SHIFT - INTERVAL_SHIFT;
pub const BLOCK_INTERVAL_COUNT: i32 = 1 << BLOCK_INTERVAL_SHIFT;
pub const BLOCK_INTERVAL_MASK: i32 = BLOCK_INTERVAL_COUNT - 1;

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

#[derive(Debug, Copy, Clone)]
pub enum NumberType {
    // Dense ordinals
    ORDINAL,
    // Random long values
    VALUE,
}

pub struct Lucene54DocValuesFormat {
    name: String,
}

impl Default for Lucene54DocValuesFormat {
    fn default() -> Self {
        Lucene54DocValuesFormat {
            name: String::from("Lucene54"),
        }
    }
}

impl DocValuesFormat for Lucene54DocValuesFormat {
    fn name(&self) -> &str {
        &self.name
    }
    fn fields_producer(&self, state: &SegmentReadState) -> Result<Box<DocValuesProducer>> {
        let boxed = Lucene54DocValuesProducer::new(
            state,
            DATA_CODEC,
            DATA_EXTENSION,
            META_CODEC,
            META_EXTENSION,
        )?;
        Ok(Box::new(boxed))
    }

    fn fields_consumer(&self, state: &SegmentWriteState) -> Result<Box<DocValuesConsumer>> {
        Ok(Box::new(Lucene54DocValuesConsumer::new(
            state,
            DATA_CODEC,
            DATA_EXTENSION,
            META_CODEC,
            META_EXTENSION,
        )?))
    }
}
