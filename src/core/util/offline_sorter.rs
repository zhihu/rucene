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

use error::{ErrorKind::IllegalArgument, Result};

pub const MAX_TEMP_FILES: i32 = 10;
pub const MB: i32 = 1024 * 1024;
pub const GB: i32 = MB * 1024;
pub const ABSOLUTE_MIN_SORT_BUFFER_SIZE: i32 = MB / 2;
pub const MIN_BUFFER_SIZE_MSG: &str = "At least 0.5MB RAM buffer is needed";

pub struct BufferSize {}

impl BufferSize {
    pub fn new(bytes: i32) -> Result<BufferSize> {
        if bytes > i32::max_value() {
            bail!(IllegalArgument("Buffer too large".into()));
        }

        if bytes < ABSOLUTE_MIN_SORT_BUFFER_SIZE {
            bail!(IllegalArgument(format!(
                "{}:{}",
                MIN_BUFFER_SIZE_MSG, bytes
            )));
        }

        Ok(BufferSize {})
    }

    pub fn megabytes(mb: i32) -> Result<BufferSize> {
        BufferSize::new(mb * MB)
    }

    pub fn automatic() -> Result<BufferSize> {
        BufferSize::megabytes(1024)
    }
}

//#[derive(Debug)]
// pub struct SortInfo {}
//
// pub trait BytesRefComparator {}
//
// pub trait SortableBytesRefArray {}
//
// pub struct OfflineSorter {}
//
// impl OfflineSorter {
//    /// All-details constructor.  If {@code valueLength} is -1 (the default), the length of each
//    /// value differs; otherwise, all values have the specified length.
//    pub fn new(
//        _dir: &Directory,
//        _temp_file_name_prefix: &str,
//        _comparator: Box<BytesRefComparator>,
//        _ram_buffer_size: BufferSize,
//        _max_temp_files: i32,
//        _value_length: i32,
//    ) -> Result<OfflineSorter> {
//        unimplemented!()
//    }
//}
