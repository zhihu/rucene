use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use core::store::{Directory, FilterDirectory, IOContext, IndexInput, IndexOutput};
use core::util::io::delete_file_ignoring_error;

use error::{ErrorKind::IllegalState, Result};

pub struct TrackingTmpOutputDirectoryWrapper<D: Directory> {
    directory: Arc<D>,
    pub file_names: Mutex<HashMap<String, String>>,
}

impl<D: Directory> TrackingTmpOutputDirectoryWrapper<D> {
    pub fn new(directory: Arc<D>) -> Self {
        Self {
            directory,
            file_names: Mutex::new(HashMap::new()),
        }
    }

    pub fn delete_temp_files(&self) {
        for name in self.file_names.lock().unwrap().values() {
            delete_file_ignoring_error(self, name.as_str());
        }
    }
}

impl<D: Directory> FilterDirectory for TrackingTmpOutputDirectoryWrapper<D> {
    type Dir = D;

    fn dir(&self) -> &Self::Dir {
        &*self.directory
    }
}

impl<D: Directory> Directory for TrackingTmpOutputDirectoryWrapper<D> {
    type LK = D::LK;
    type IndexOutput = D::TempOutput;
    type TempOutput = D::TempOutput;
    fn create_output(&self, name: &str, ctx: &IOContext) -> Result<Self::IndexOutput> {
        let mut guard = self.file_names.lock().unwrap();
        if guard.contains_key(name) {
            bail!(IllegalState(format!(
                "output file '{}' already exist!",
                name
            )));
        }

        self.dir().create_temp_output(name, "", ctx).map(|out| {
            guard.insert(name.to_string(), out.name().to_string());
            out
        })
    }

    fn open_input(&self, name: &str, ctx: &IOContext) -> Result<Box<dyn IndexInput>> {
        if let Some(n) = self.file_names.lock().unwrap().get(name) {
            self.dir().open_input(n, ctx)
        } else {
            bail!(IllegalState(format!("input file '{}' not found!", name)));
        }
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
        self.directory.create_temp_output(prefix, suffix, ctx)
    }
}

impl<D: Directory> fmt::Display for TrackingTmpOutputDirectoryWrapper<D> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "TrackingTmpOutputDirectoryWrapper({})",
            self.directory.as_ref()
        )
    }
}
