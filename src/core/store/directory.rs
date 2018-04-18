use std::fmt::Display;
use std::sync::Arc;

use core::store::{BufferedChecksumIndexInput, ChecksumIndexInput, IndexInput};
use error::*;

#[derive(PartialEq, Eq, Clone, Copy)]
pub enum IOContext {
    Read(bool),
    Default,
}

pub const IO_CONTEXT_READONCE: IOContext = IOContext::Read(true);

pub trait Directory: Drop + Display + Send + Sync {
    /// Returns an array of strings, one for each entry in the directory, in sorted (UTF16,
    /// java's String.compare) order.
    fn list_all(&self) -> Result<Vec<String>>;

    /// Returns the length of a file in the directory. This method follows the
    /// following contract:
    fn file_length(&self, name: &str) -> Result<i64>;

    fn open_input(&self, name: &str, ctx: &IOContext) -> Result<Box<IndexInput>>;

    fn open_checksum_input(&self, name: &str, ctx: &IOContext) -> Result<Box<ChecksumIndexInput>> {
        let input = self.open_input(name, ctx)?;
        Ok(Box::new(BufferedChecksumIndexInput::new(input)))
    }
}

pub type DirectoryRc = Arc<Directory>;
