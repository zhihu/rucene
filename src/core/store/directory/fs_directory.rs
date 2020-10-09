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

use std::collections::{BTreeSet, HashSet};
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::RwLock;

use core::codec::segment_infos::segment_file_name;
use core::store::directory::Directory;
use core::store::io::{FSIndexOutput, IndexInput, MmapIndexInput};
use core::store::IOContext;
use core::util::to_base36;
use error::ErrorKind::IllegalState;
use error::Result;

/// a straightforward `Directory` implementations use std::fs::File.
///
/// However, it has poor concurrent performance (multiple threads will bottleneck)
/// as it synchronizes when multiple threads read from the same file.
pub struct FSDirectory {
    pub directory: PathBuf,
    pending_deletes: RwLock<BTreeSet<String>>,
    pub ops_since_last_delete: AtomicUsize,
    pub next_temp_file_counter: AtomicUsize,
}

impl FSDirectory {
    pub fn with_path<T: AsRef<Path> + ?Sized>(directory: &T) -> Result<Self> {
        Self::new(directory)
    }
}

impl FSDirectory {
    pub fn new<T: AsRef<Path> + ?Sized>(directory: &T) -> Result<FSDirectory> {
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
            pending_deletes: RwLock::new(BTreeSet::new()),
            ops_since_last_delete: AtomicUsize::new(0),
            next_temp_file_counter: AtomicUsize::new(0),
        })
    }

    fn delete_pending_files(pending_deletes: &mut BTreeSet<String>, dir: &PathBuf) -> Result<()> {
        let mut deleted_set = BTreeSet::new();
        for name in pending_deletes.iter() {
            let path = dir.join(name);
            if let Err(e) = fs::remove_file(path.clone()) {
                info!("delete_pending_files {:?} failed. {}", path, e.to_string());
            }
            deleted_set.insert(name.clone());
        }
        for name in deleted_set {
            pending_deletes.remove(&name);
        }
        Ok(())
    }

    fn maybe_delete_pending_files(&self) -> Result<()> {
        let mut delete_set = self.pending_deletes.write()?;
        if !delete_set.is_empty() {
            let count = self.ops_since_last_delete.fetch_add(1, Ordering::AcqRel);
            if count > delete_set.len() {
                self.ops_since_last_delete
                    .fetch_sub(count, Ordering::Release);
                Self::delete_pending_files(&mut delete_set, &self.directory)?;
            }
        }
        Ok(())
    }

    fn ensure_can_read(&self, name: &str) -> Result<()> {
        if self.pending_deletes.read()?.contains(name) {
            bail!(
                "file {} is pending delete and cannot be opened for read",
                name
            );
        }
        Ok(())
    }

    fn fsync(&self, path: &Path, is_dir: bool) -> Result<()> {
        // If the file is a directory we have to open read-only, for regular files we must
        // open r/w for the fsync to have an effect.
        // See http://blog.httrack.com/blog/2013/11/15/everything-you-always-wanted-to-know-about-fsync/
        let file = if is_dir {
            fs::File::open(path)?
        } else {
            fs::OpenOptions::new().append(true).open(path)?
        };
        file.sync_all()?;
        Ok(())
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
    type IndexOutput = FSIndexOutput;
    type TempOutput = FSIndexOutput;

    fn list_all(&self) -> Result<Vec<String>> {
        list_all(&self.directory)
    }

    fn file_length(&self, name: &str) -> Result<i64> {
        if self.pending_deletes.read()?.contains(name) {
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

    fn create_output(&self, name: &str, _context: &IOContext) -> Result<Self::IndexOutput> {
        // If this file was pending delete, we are now bringing it back to life:
        self.pending_deletes.write()?.remove(name);
        self.maybe_delete_pending_files()?;
        let path = self.resolve(name);
        FSIndexOutput::new(name.to_string(), &path)
    }

    fn open_input(&self, name: &str, _ctx: &IOContext) -> Result<Box<dyn IndexInput>> {
        self.ensure_can_read(name)?;
        let path = self.directory.as_path().join(name);
        // hack logic, we don'e implement FsIndexInput yes, so just us MmapIndexInput instead
        Ok(Box::new(MmapIndexInput::new(path)?))
    }

    fn create_temp_output(
        &self,
        prefix: &str,
        suffix: &str,
        _ctx: &IOContext,
    ) -> Result<Self::TempOutput> {
        self.maybe_delete_pending_files()?;

        loop {
            let name = segment_file_name(
                prefix,
                &format!(
                    "{}_{}",
                    suffix,
                    to_base36(self.next_temp_file_counter.fetch_add(1, Ordering::AcqRel) as u64)
                ),
                "tmp",
            );

            if self.pending_deletes.read()?.contains(&name) {
                continue;
            }

            let path = self.resolve(&name);
            return FSIndexOutput::new(name, &path);
        }
    }

    fn delete_file(&self, name: &str) -> Result<()> {
        if self.pending_deletes.read()?.contains(name) {
            bail!(IllegalState(format!(
                "file {} is already pending delete",
                name
            )))
        };

        let mut deletes = BTreeSet::new();
        deletes.insert(name.to_string());
        Self::delete_pending_files(&mut deletes, &self.directory)?;
        self.pending_deletes.write()?.remove(name);

        self.maybe_delete_pending_files()
    }

    fn sync(&self, names: &HashSet<String>) -> Result<()> {
        for name in names {
            let path = self.resolve(name);
            self.fsync(&path, false)?;
        }
        self.maybe_delete_pending_files()
    }

    fn sync_meta_data(&self) -> Result<()> {
        self.fsync(&self.directory, true)
    }

    fn rename(&self, source: &str, dest: &str) -> Result<()> {
        if self.pending_deletes.read()?.contains(source) {
            bail!(IllegalState(
                "file '{}' is pending delete and cannot be moved".into()
            ));
        }
        self.pending_deletes.write()?.remove(dest);
        let source_path = self.resolve(source);
        let dest_path = self.resolve(dest);
        fs::rename(&source_path, &dest_path)?;
        self.maybe_delete_pending_files()
    }

    fn resolve(&self, name: &str) -> PathBuf {
        self.directory.join(name)
    }
}

impl fmt::Display for FSDirectory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "FSDirectory({})", self.directory.display())
    }
}
