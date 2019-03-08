use core::store::Directory;
use core::store::{IOContext, IndexInput, IndexOutput, Lock};
use std::collections::HashSet;

use error::Result;
use std::fmt;
use std::sync::Mutex;

pub struct TrackingDirectoryWrapper<T: AsRef<Directory>> {
    create_file_names: Mutex<HashSet<String>>,
    pub directory: T,
}

unsafe impl<T: AsRef<Directory>> Send for TrackingDirectoryWrapper<T> {}

unsafe impl<T: AsRef<Directory>> Sync for TrackingDirectoryWrapper<T> {}

impl<T: AsRef<Directory>> TrackingDirectoryWrapper<T> {
    pub fn new(directory: T) -> TrackingDirectoryWrapper<T> {
        TrackingDirectoryWrapper {
            create_file_names: Mutex::new(HashSet::new()),
            directory,
        }
    }

    pub fn get_create_files(&self) -> HashSet<String> {
        self.create_file_names.lock().unwrap().clone()
    }
}

impl<T: AsRef<Directory>> Directory for TrackingDirectoryWrapper<T> {
    fn list_all(&self) -> Result<Vec<String>> {
        self.directory.as_ref().list_all()
    }

    fn file_length(&self, name: &str) -> Result<i64> {
        self.directory.as_ref().file_length(name)
    }

    fn open_input(&self, name: &str, ctx: &IOContext) -> Result<Box<IndexInput>> {
        self.directory.as_ref().open_input(name, ctx)
    }

    fn create_temp_output(
        &self,
        prefix: &str,
        suffix: &str,
        ctx: &IOContext,
    ) -> Result<Box<IndexOutput>> {
        let temp_output = self
            .directory
            .as_ref()
            .create_temp_output(prefix, suffix, ctx)?;
        self.create_file_names
            .lock()?
            .insert(temp_output.name().to_string());
        Ok(temp_output)
    }

    fn delete_file(&self, name: &str) -> Result<()> {
        self.directory.as_ref().delete_file(name)?;
        self.create_file_names.lock()?.remove(name);
        Ok(())
    }

    fn create_output(&self, name: &str, ctx: &IOContext) -> Result<Box<IndexOutput>> {
        let output = self.directory.as_ref().create_output(name, ctx)?;
        self.create_file_names.lock()?.insert(name.to_string());
        Ok(output)
    }

    fn rename(&self, source: &str, dest: &str) -> Result<()> {
        self.directory.as_ref().rename(source, dest)?;
        let mut guard = self.create_file_names.lock()?;
        guard.insert(dest.to_string());
        guard.remove(source);
        Ok(())
    }

    fn obtain_lock(&self, name: &str) -> Result<Box<Lock>> {
        self.directory.as_ref().obtain_lock(name)
    }

    fn sync(&self, name: &HashSet<String>) -> Result<()> {
        self.directory.as_ref().sync(name)
    }

    fn sync_meta_data(&self) -> Result<()> {
        self.directory.as_ref().sync_meta_data()
    }

    fn create_files(&self) -> HashSet<String> {
        self.create_file_names.lock().unwrap().clone()
    }
}

impl<T: AsRef<Directory>> fmt::Display for TrackingDirectoryWrapper<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TrackingDirectoryWrapper({})", self.directory.as_ref())
    }
}

impl<T: AsRef<Directory>> Drop for TrackingDirectoryWrapper<T> {
    fn drop(&mut self) {}
}
