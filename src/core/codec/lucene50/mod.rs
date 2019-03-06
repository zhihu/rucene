mod compound;

pub use self::compound::*;

mod posting_format;
mod posting_reader;
mod posting_writer;

pub use self::posting_format::Lucene50PostingsFormat;
pub use self::posting_reader::*;
pub use self::posting_writer::*;

mod stored_fields;

pub use self::stored_fields::*;

mod live_docs;

pub use self::live_docs::*;

pub mod skip_reader;
pub mod skip_writer;
pub mod util;

use core::codec::compressing::CompressingTermVectorsFormat;

/// `Lucene50TermVectorsFormat` is just `CompressingTermVectorsFormat`
pub fn term_vectors_format() -> CompressingTermVectorsFormat {
    CompressingTermVectorsFormat::default()
}
