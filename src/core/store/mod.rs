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

mod data_input;
pub use self::data_input::*;

mod index_input;
pub use self::index_input::*;

mod random_access_input;

pub use self::random_access_input::*;

mod checksum_index_input;
pub use self::checksum_index_input::*;

mod buffered_checksum_index_input;
pub use self::buffered_checksum_index_input::*;

mod mmap_index_input;
pub use self::mmap_index_input::*;

mod data_output;
pub use self::data_output::*;

mod index_output;
pub use self::index_output::*;

mod fs_index_output;
pub use self::fs_index_output::*;

mod byte_array_data_input;
pub use self::byte_array_data_input::*;

mod directory;
pub use self::directory::*;

mod fs_directory;
pub use self::fs_directory::*;

mod lock;
pub use self::lock::*;

mod mmap_directory;
pub use self::mmap_directory::*;

mod growable_byte_array_output;
pub use self::growable_byte_array_output::*;

mod tracking_directory_wrapper;
pub use self::tracking_directory_wrapper::*;

mod ram_output;
pub use self::ram_output::*;

mod rate_limiter;
pub use self::rate_limiter::*;

/// A FlushInfo provides information required for a FLUSH context.
///
/// It is used as part of an `IOContext` in case of FLUSH context.
#[derive(PartialEq, Eq, Hash, Clone, Copy)]
pub struct FlushInfo {
    num_docs: u32,
    estimated_segment_size: u64,
}

impl FlushInfo {
    pub fn new(num_docs: u32, estimated_segment_size: u64) -> Self {
        FlushInfo {
            num_docs,
            estimated_segment_size,
        }
    }
}

/// A MergeInfo provides information required for a MERGE context.
///
/// It is used as part of an `IOContext` in case of MERGE context.
#[derive(PartialEq, Eq, Hash, Clone, Copy)]
pub struct MergeInfo {
    total_max_doc: u32,
    estimated_merge_bytes: u64,
    is_external: bool,
    merge_max_num_segments: Option<u32>,
}

impl MergeInfo {
    pub fn new(
        total_max_doc: u32,
        estimated_merge_bytes: u64,
        is_external: bool,
        merge_max_num_segments: Option<u32>,
    ) -> Self {
        MergeInfo {
            total_max_doc,
            estimated_merge_bytes,
            is_external,
            merge_max_num_segments,
        }
    }
}
