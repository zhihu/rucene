use core::store::{Directory, IOContext, IndexInput, IndexOutput};

use error::Result;

use std::collections::HashSet;
use std::fmt;
use std::ops::Deref;
use std::sync::Mutex;

pub struct TrackingDirectoryWrapper<D: Directory, T: Deref<Target = D>> {
    create_file_names: Mutex<HashSet<String>>,
    pub directory: T,
}

impl<D: Directory, T: Deref<Target = D>> TrackingDirectoryWrapper<D, T> {
    pub fn new(directory: T) -> TrackingDirectoryWrapper<D, T> {
        TrackingDirectoryWrapper {
            create_file_names: Mutex::new(HashSet::new()),
            directory,
        }
    }

    pub fn get_create_files(&self) -> HashSet<String> {
        self.create_file_names.lock().unwrap().clone()
    }
}

impl<D, T> Directory for TrackingDirectoryWrapper<D, T>
where
    D: Directory,
    T: Deref<Target = D>,
{
    type LK = D::LK;
    type IndexOutput = D::IndexOutput;
    type TempOutput = D::TempOutput;

    fn list_all(&self) -> Result<Vec<String>> {
        self.directory.list_all()
    }

    fn file_length(&self, name: &str) -> Result<i64> {
        self.directory.file_length(name)
    }

    fn create_output(&self, name: &str, ctx: &IOContext) -> Result<Self::IndexOutput> {
        let output = self.directory.create_output(name, ctx)?;
        self.create_file_names.lock()?.insert(name.to_string());
        Ok(output)
    }

    fn open_input(&self, name: &str, ctx: &IOContext) -> Result<Box<dyn IndexInput>> {
        self.directory.open_input(name, ctx)
    }

    fn obtain_lock(&self, name: &str) -> Result<Self::LK> {
        self.directory.obtain_lock(name)
    }

    fn create_temp_output(
        &self,
        prefix: &str,
        suffix: &str,
        ctx: &IOContext,
    ) -> Result<Self::TempOutput> {
        let temp_output = self.directory.create_temp_output(prefix, suffix, ctx)?;
        self.create_file_names
            .lock()?
            .insert(temp_output.name().to_string());
        Ok(temp_output)
    }

    fn delete_file(&self, name: &str) -> Result<()> {
        self.directory.delete_file(name)?;
        self.create_file_names.lock()?.remove(name);
        Ok(())
    }

    fn sync(&self, name: &HashSet<String>) -> Result<()> {
        self.directory.sync(name)
    }

    fn sync_meta_data(&self) -> Result<()> {
        self.directory.sync_meta_data()
    }

    fn rename(&self, source: &str, dest: &str) -> Result<()> {
        self.directory.rename(source, dest)?;
        let mut guard = self.create_file_names.lock()?;
        guard.insert(dest.to_string());
        guard.remove(source);
        Ok(())
    }

    fn create_files(&self) -> HashSet<String> {
        self.create_file_names.lock().unwrap().clone()
    }
}

impl<D, T> fmt::Display for TrackingDirectoryWrapper<D, T>
where
    D: Directory,
    T: Deref<Target = D>,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TrackingDirectoryWrapper({})", &*self.directory)
    }
}
