mod compound;

pub use self::compound::*;

mod posting;

pub use self::posting::*;

mod stored_fields;

pub use self::stored_fields::*;

mod live_docs;

pub use self::live_docs::*;

pub mod skip;
pub mod util;

use core::codec::compressing::CompressingTermVectorsFormat;

/// `Lucene50TermVectorsFormat` is just `CompressingTermVectorsFormat`
pub fn term_vectors_format() -> CompressingTermVectorsFormat {
    CompressingTermVectorsFormat::default()
}
