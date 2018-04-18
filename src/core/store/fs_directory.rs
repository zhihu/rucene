use std::collections::BTreeSet;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;

use core::store::IndexInput;
use core::store::{Directory, IOContext};
use error::ErrorKind::IllegalState;
use error::Result;

pub struct FSDirectory {
    directory: PathBuf,
    pending_deletes: BTreeSet<String>,
    pub ops_since_last_delete: AtomicUsize,
    pub next_temp_file_counter: AtomicUsize,
}

impl FSDirectory {
    pub fn new<T: AsRef<Path>>(directory: &T) -> Result<FSDirectory> {
        let directory = directory.as_ref();
        if !Path::exists(directory) {
            fs::create_dir_all(directory)?;
        } else if !Path::is_dir(directory) {
            bail!(IllegalState(format!(
                "Path {:?} exists but is not directory",
                directory
            )))
        }

        Ok(FSDirectory {
            directory: From::from(directory),
            pending_deletes: BTreeSet::new(),
            ops_since_last_delete: AtomicUsize::new(0),
            next_temp_file_counter: AtomicUsize::new(0),
        })
    }

    pub fn resolve(&self, name: &str) -> PathBuf {
        self.directory.join(name)
    }
}

fn list_all<T: AsRef<Path>>(path: &T) -> Result<Vec<String>> {
    let mut result = Vec::new();
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        if let Ok(filename) = entry.file_name().into_string() {
            result.push(filename);
        }
    }
    Ok(result)
}

impl Directory for FSDirectory {
    fn list_all(&self) -> Result<Vec<String>> {
        list_all(&self.directory)
    }

    fn file_length(&self, name: &str) -> Result<i64> {
        if self.pending_deletes.contains(name) {
            bail!(IllegalState(format!("pending delete file {}", name)))
        };

        let path = self.resolve(name);
        let meta = fs::metadata(&path)?;
        if meta.is_dir() {
            bail!(IllegalState(format!(
                "file_length should be called for directory: {}",
                path.display()
            )))
        } else {
            Ok(meta.len() as i64)
        }
    }

    fn open_input(&self, _name: &str, _ctx: &IOContext) -> Result<Box<IndexInput>> {
        unimplemented!()
    }
}

impl Drop for FSDirectory {
    fn drop(&mut self) {}
}

impl fmt::Display for FSDirectory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "FSDirectory({})", self.directory.display())
    }
}
