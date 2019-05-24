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
