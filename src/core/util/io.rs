use std::fs::OpenOptions;
use std::path::Path;

use error::*;

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
