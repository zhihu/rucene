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

use core::store::directory::{Directory, FilterDirectory};
use core::store::io::{IndexInput, IndexOutput};
use core::store::IOContext;

use error::Result;

use std::collections::HashSet;
use std::fmt;
use std::ops::Deref;
use std::sync::Mutex;

/// A delegating Directory that records which files were written to and deleted.
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

impl<D, T> FilterDirectory for TrackingDirectoryWrapper<D, T>
where
    D: Directory,
    T: Deref<Target = D>,
{
    type Dir = D;

    #[inline]
    fn dir(&self) -> &Self::Dir {
        &*self.directory
    }
}

impl<D, T> Directory for TrackingDirectoryWrapper<D, T>
where
    D: Directory,
    T: Deref<Target = D>,
{
    type IndexOutput = D::IndexOutput;
    type TempOutput = D::TempOutput;

    fn create_output(&self, name: &str, ctx: &IOContext) -> Result<Self::IndexOutput> {
        let output = self.directory.create_output(name, ctx)?;
        self.create_file_names.lock()?.insert(name.to_string());
        Ok(output)
    }

    fn open_input(&self, name: &str, ctx: &IOContext) -> Result<Box<dyn IndexInput>> {
        self.directory.open_input(name, ctx)
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
