use error::*;

use core::store::DirectoryRc;

pub const MAX_TEMP_FILES: i32 = 10;
pub const MB: i32 = 1024 * 1024;
pub const GB: i32 = MB * 1024;
pub const ABSOLUTE_MIN_SORT_BUFFER_SIZE: i32 = MB / 2;
pub const MIN_BUFFER_SIZE_MSG: &str = "At least 0.5MB RAM buffer is needed";

pub struct BufferSize {}

impl BufferSize {
    pub fn new(bytes: i32) -> Result<BufferSize> {
        if bytes > i32::max_value() {
            bail!("Buffer too large");
        }

        if bytes < ABSOLUTE_MIN_SORT_BUFFER_SIZE {
            bail!("{}:{}", MIN_BUFFER_SIZE_MSG, bytes);
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

#[derive(Debug)]
pub struct SortInfo {}

pub trait BytesRefComparator {}

pub trait SortableBytesRefArray {}

pub struct OfflineSorter {}

impl OfflineSorter {
    /// All-details constructor.  If {@code valueLength} is -1 (the default), the length of each
    /// value differs; otherwise, all values have the specified length.
    ///
    pub fn new(
        _dir: &DirectoryRc,
        _temp_file_name_prefix: &str,
        _comparator: Box<BytesRefComparator>,
        _ram_buffer_size: BufferSize,
        _max_temp_files: i32,
        _value_length: i32,
    ) -> Result<OfflineSorter> {
        unimplemented!()
    }
}
