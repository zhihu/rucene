use std::collections::HashSet;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use core::store::{BufferedChecksumIndexInput, ChecksumIndexInput, IndexInput, IndexOutput};
use core::store::{FlushInfo, Lock, MergeInfo};
use error::*;

#[derive(PartialEq, Eq, Clone, Copy)]
pub enum IOContext {
    Read(bool),
    Default,
    Flush(FlushInfo),
    Merge(MergeInfo),
}

impl IOContext {
    pub fn is_merge(&self) -> bool {
        match self {
            IOContext::Merge(_) => true,
            _ => false,
        }
    }
}

pub const IO_CONTEXT_READONCE: IOContext = IOContext::Read(true);
pub const IO_CONTEXT_READ: IOContext = IOContext::Read(false);

pub trait Directory: Drop + fmt::Display + Send + Sync {
    /// Returns an array of strings, one for each entry in the directory, in sorted (UTF16,
    /// java's String.compare) order.
    fn list_all(&self) -> Result<Vec<String>>;

    /// Returns the length of a file in the directory. This method follows the
    /// following contract:
    fn file_length(&self, name: &str) -> Result<i64>;

    /// Creates a new, empty file in the directory with the given name.
    /// Returns a stream writing this file.
    fn create_output(&self, name: &str, context: &IOContext) -> Result<Box<IndexOutput>>;

    fn open_input(&self, name: &str, ctx: &IOContext) -> Result<Box<IndexInput>>;

    fn open_checksum_input(&self, name: &str, ctx: &IOContext) -> Result<Box<ChecksumIndexInput>> {
        let input = self.open_input(name, ctx)?;
        Ok(Box::new(BufferedChecksumIndexInput::new(input)))
    }

    /// Returns an obtained {@link Lock}.
    /// @param name the name of the lock file
    /// @throws LockObtainFailedException (optional specific exception) if the lock could
    ///         not be obtained because it is currently held elsewhere.
    /// @throws IOException if any i/o error occurs attempting to gain the lock
    fn obtain_lock(&self, name: &str) -> Result<Box<Lock>>;

    fn create_temp_output(
        &self,
        prefix: &str,
        suffix: &str,
        ctx: &IOContext,
    ) -> Result<Box<IndexOutput>>;

    fn delete_file(&self, name: &str) -> Result<()>;

    /// Ensure that any writes to these files are moved to
    /// stable storage.  Lucene uses this to properly commit
    /// changes to the index, to prevent a machine/OS crash
    /// from corrupting the index.
    ///
    /// NOTE: Clients may call this method for same files over
    /// and over again, so some impls might optimize for that.
    /// For other impls the operation can be a noop, for various
    /// reasons.
    fn sync(&self, name: &HashSet<String>) -> Result<()>;

    /// Ensure that directory metadata, such as recent file renames, are made durable.
    fn sync_meta_data(&self) -> Result<()>;

    fn rename(&self, source: &str, dest: &str) -> Result<()>;

    fn copy_from(&self, from: DirectoryRc, src: &str, dest: &str, ctx: &IOContext) -> Result<()> {
        let mut _success = false;
        let mut is = from.open_input(src, ctx)?;
        let mut os = self.create_output(dest, ctx)?;

        let length = is.len();
        os.copy_bytes(is.as_data_input(), length as usize)?;

        _success = true;

        //        if (!_success) {
        //            IOUtils.deleteFilesIgnoringExceptions(this, dest);
        //        }

        Ok(())
    }

    fn create_files(&self) -> HashSet<String> {
        unreachable!()
    }

    fn resolve(&self, _name: &str) -> PathBuf {
        unimplemented!()
    }
}

pub type DirectoryRc = Arc<Directory>;

/// This struct makes a best-effort check that a provided
/// `Lock` is valid before any destructive filesystem operation.
pub struct LockValidatingDirectoryWrapper {
    dir: DirectoryRc,
    write_lock: Arc<Lock>,
}

impl LockValidatingDirectoryWrapper {
    pub fn new(dir: DirectoryRc, write_lock: Arc<Lock>) -> Self {
        LockValidatingDirectoryWrapper { dir, write_lock }
    }
}

impl Directory for LockValidatingDirectoryWrapper {
    fn list_all(&self) -> Result<Vec<String>> {
        self.dir.list_all()
    }

    fn file_length(&self, name: &str) -> Result<i64> {
        self.dir.file_length(name)
    }

    fn create_output(&self, name: &str, context: &IOContext) -> Result<Box<IndexOutput>> {
        self.write_lock.ensure_valid()?;
        self.dir.create_output(name, context)
    }

    fn open_input(&self, name: &str, ctx: &IOContext) -> Result<Box<IndexInput>> {
        self.dir.open_input(name, ctx)
    }

    fn obtain_lock(&self, name: &str) -> Result<Box<Lock>> {
        self.dir.obtain_lock(name)
    }

    fn create_temp_output(
        &self,
        prefix: &str,
        suffix: &str,
        ctx: &IOContext,
    ) -> Result<Box<IndexOutput>> {
        self.dir.create_temp_output(prefix, suffix, ctx)
    }

    fn delete_file(&self, name: &str) -> Result<()> {
        self.write_lock.ensure_valid()?;
        self.dir.delete_file(name)
    }

    fn sync(&self, name: &HashSet<String>) -> Result<()> {
        self.write_lock.ensure_valid()?;
        self.dir.sync(name)
    }

    fn sync_meta_data(&self) -> Result<()> {
        self.write_lock.ensure_valid()?;
        self.dir.sync_meta_data()
    }

    fn rename(&self, source: &str, dest: &str) -> Result<()> {
        self.write_lock.ensure_valid()?;
        self.dir.rename(source, dest)
    }

    fn copy_from(&self, from: DirectoryRc, src: &str, dest: &str, ctx: &IOContext) -> Result<()> {
        self.write_lock.ensure_valid()?;
        self.dir.copy_from(from, src, dest, ctx)
    }
}

impl fmt::Display for LockValidatingDirectoryWrapper {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "LockValidatingDirectoryWrapper({})", self.dir.as_ref())
    }
}

impl Drop for LockValidatingDirectoryWrapper {
    fn drop(&mut self) {}
}
