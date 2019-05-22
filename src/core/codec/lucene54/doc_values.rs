use core::codec::format::{DocValuesConsumerEnum, DocValuesFormat};
use core::codec::lucene54::Lucene54DocValuesConsumer;
use core::codec::lucene54::Lucene54DocValuesProducer;
use core::codec::{Codec, DocValuesProducer};
use core::index::{SegmentReadState, SegmentWriteState};
use core::store::Directory;
use error::Result;

#[derive(Debug, Copy, Clone)]
pub enum NumberType {
    // Dense ordinals
    ORDINAL,
    // Random long values
    VALUE,
}

#[derive(Copy, Clone, Default)]
pub struct Lucene54DocValuesFormat;

impl Lucene54DocValuesFormat {
    const DATA_CODEC: &'static str = "Lucene54DocValuesData";
    const DATA_EXTENSION: &'static str = "dvd";
    const META_CODEC: &'static str = "Lucene54DocValuesMetadata";
    const META_EXTENSION: &'static str = "dvm";
    pub(crate) const VERSION_START: i32 = 0;
    pub(crate) const VERSION_CURRENT: i32 = 0;

    // indicates docvalues type
    pub(crate) const NUMERIC: u8 = 0;
    pub(crate) const BINARY: u8 = 1;
    pub(crate) const SORTED: u8 = 2;
    pub(crate) const SORTED_SET: u8 = 3;
    pub(crate) const SORTED_NUMERIC: u8 = 4;

    // address terms in blocks of 16 terms
    pub(crate) const INTERVAL_SHIFT: i32 = 4;
    pub(crate) const INTERVAL_COUNT: i32 = 1 << Self::INTERVAL_SHIFT;
    pub(crate) const INTERVAL_MASK: i32 = Self::INTERVAL_COUNT - 1;

    // build reverse index from every 1024th term
    pub(crate) const REVERSE_INTERVAL_SHIFT: i32 = 10;
    pub(crate) const REVERSE_INTERVAL_COUNT: i32 = 1 << Self::REVERSE_INTERVAL_SHIFT;
    pub(crate) const REVERSE_INTERVAL_MASK: i32 = Self::REVERSE_INTERVAL_COUNT - 1;

    // for conversion from reverse index to block
    pub(crate) const BLOCK_INTERVAL_SHIFT: i32 =
        Self::REVERSE_INTERVAL_SHIFT - Self::INTERVAL_SHIFT;
    pub(crate) const BLOCK_INTERVAL_COUNT: i32 = 1 << Self::BLOCK_INTERVAL_SHIFT;
    pub(crate) const BLOCK_INTERVAL_MASK: i32 = Self::BLOCK_INTERVAL_COUNT - 1;

    // Compressed using packed blocks of ints
    pub(crate) const DELTA_COMPRESSED: i32 = 0;
    // Compressed by computing the GCD
    pub(crate) const GCD_COMPRESSED: i32 = 1;
    // Compressed by giving IDs to unique values
    pub(crate) const TABLE_COMPRESSED: i32 = 2;
    // Compressed with monotonically increasing values
    pub(crate) const MONOTONIC_COMPRESSED: i32 = 3;
    // Compressed with pub constant value (uses only missing bitset)
    pub(crate) const CONST_COMPRESSED: i32 = 4;
    // Compressed with sparse arrays
    pub(crate) const SPARSE_COMPRESSED: i32 = 5;

    // Uncompressed binary, written directly (fixed length)
    pub(crate) const BINARY_FIXED_UNCOMPRESSED: i32 = 0;
    // Uncompressed binary, written directly (variable length)
    pub(crate) const BINARY_VARIABLE_UNCOMPRESSED: i32 = 1;
    // Compressed binary with shared prefixes
    pub(crate) const BINARY_PREFIX_COMPRESSED: i32 = 2;

    // Standard storage for sorted set values with 1 level of indirection:
    // docId -> address -> ord
    pub(crate) const SORTED_WITH_ADDRESSES: i32 = 0;
    // Single-valued sorted set values, encoded as sorted values, so no level
    //  of indirection: docId -> ord
    pub(crate) const SORTED_SINGLE_VALUED: i32 = 1;
    // Compressed giving IDs to unique sets of values:
    //  docId -> setId -> ords
    pub(crate) const SORTED_SET_TABLE: i32 = 2;

    // placeholder for missing offset that means there are no missing values
    pub(crate) const ALL_LIVE: i32 = -1;
    // placeholder for missing offset that means all values are missing
    pub(crate) const ALL_MISSING: i32 = -2;

    // addressing uses 16k blocks
    pub(crate) const MONOTONIC_BLOCK_SIZE: i32 = 16384;
    pub(crate) const DIRECT_MONOTONIC_BLOCK_SHIFT: i32 = 16;
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
