use core::store::DataInput;
use core::store::RandomAccessInput;
use error::Result;

pub trait IndexInput: DataInput + Send + Sync {
    fn clone(&self) -> Result<Box<dyn IndexInput>>;

    fn file_pointer(&self) -> i64;
    fn seek(&mut self, pos: i64) -> Result<()>;
    fn len(&self) -> u64;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn name(&self) -> &str;

    fn random_access_slice(&self, _offset: i64, _length: i64)
        -> Result<Box<dyn RandomAccessInput>>;

    fn slice(&self, _description: &str, _offset: i64, _length: i64) -> Result<Box<dyn IndexInput>> {
        unimplemented!();
    }

    fn is_buffered(&self) -> bool {
        false
    }
}
