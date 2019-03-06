use std::fs::OpenOptions;
use std::path::Path;

use core::store::Directory;

use error::Result;

pub fn fsync(path: &Path, is_dir: bool) -> Result<()> {
    let mut option = OpenOptions::new();
    option.read(true);
    let file = if is_dir {
        option.open(path)
    } else {
        option.write(true).open(path)
    }?;
    file.sync_all()?;
    Ok(())
}

/// Deletes all given files, suppressing all Errors.
pub fn delete_files_ignoring_errors(dir: &Directory, files: &[String]) {
    for name in files {
        if let Err(e) = dir.delete_file(name.as_ref()) {
            warn!("delete file '{}' failed by '{:?}'", name, e);
        }
    }
}

pub fn delete_file_ignoring_error(dir: &Directory, file: &str) {
    if let Err(e) = dir.delete_file(file) {
        warn!("delete file '{}' failed by '{:?}'", file, e);
    }
}
