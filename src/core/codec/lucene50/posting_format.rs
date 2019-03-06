use core::codec::blocktree::{BlockTreeTermsReader, BlockTreeTermsWriter};
use core::codec::format::PostingsFormat;
use core::codec::lucene50::{Lucene50PostingsReader, Lucene50PostingsWriter};
use core::codec::{FieldsConsumer, FieldsProducer};
use core::index::{SegmentReadState, SegmentWriteState};
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
///
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
    fn fields_producer(&self, state: &SegmentReadState) -> Result<Box<FieldsProducer>> {
        let reader = Lucene50PostingsReader::open(&state)?;
        Ok(Box::new(BlockTreeTermsReader::new(reader, state)?))
    }

    fn fields_consumer(&self, state: &SegmentWriteState) -> Result<Box<FieldsConsumer>> {
        let postings_writer = Lucene50PostingsWriter::new(state)?;
        Ok(Box::new(BlockTreeTermsWriter::new(
            state,
            Box::new(postings_writer),
            self.min_term_block_size,
            self.max_term_block_size,
        )?))
    }

    fn name(&self) -> &str {
        "Lucene50"
    }
}
