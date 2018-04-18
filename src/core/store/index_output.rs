use core::store::DataOutput;
use error::Result;

pub trait IndexOutput: DataOutput + Drop {
    fn name(&self) -> &str;
    fn file_pointer(&self) -> i64;
    fn checksum(&self) -> Result<i64>;
}
