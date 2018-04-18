use error::Result;

pub trait RandomAccessInput: Send + Sync {
    fn read_byte(&self, pos: i64) -> Result<u8>;
    fn read_short(&self, pos: i64) -> Result<i16>;
    fn read_int(&self, pos: i64) -> Result<i32>;
    fn read_long(&self, pos: i64) -> Result<i64>;
}
