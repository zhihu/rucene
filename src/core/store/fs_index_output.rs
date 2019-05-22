use core::store::DataOutput;
use core::store::IndexOutput;
use error::Result;

use std::fs::{File, OpenOptions};
use std::io::BufWriter;
use std::io::Write;
use std::path::{Path, PathBuf};

use flate2::CrcWriter;

const CHUNK_SIZE: usize = 8192;

pub struct FSIndexOutput {
    name: PathBuf,
    writer: CrcWriter<BufWriter<File>>,
    bytes_written: usize,
}

impl FSIndexOutput {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<FSIndexOutput> {
        let name = PathBuf::from(path.as_ref());
        let file = OpenOptions::new().write(true).create(true).open(path)?;

        Ok(FSIndexOutput {
            name,
            writer: CrcWriter::new(BufWriter::with_capacity(CHUNK_SIZE, file)),
            bytes_written: 0,
        })
    }
}

impl Drop for FSIndexOutput {
    fn drop(&mut self) {
        if let Err(ref desc) = self.writer.flush() {
            error!("Oops, failed to flush {:?}, errmsg: {}", self.name, desc);
        }
        self.bytes_written = 0;
    }
}

impl DataOutput for FSIndexOutput {}

impl Write for FSIndexOutput {
    fn write(&mut self, buf: &[u8]) -> ::std::io::Result<usize> {
        let count = self.writer.write(buf)?;
        self.bytes_written += count;
        Ok(count)
    }

    fn flush(&mut self) -> ::std::io::Result<()> {
        self.writer.flush()
    }
}

impl IndexOutput for FSIndexOutput {
    fn name(&self) -> &str {
        self.name.to_str().unwrap()
    }

    fn file_pointer(&self) -> i64 {
        self.bytes_written as i64
    }

    fn checksum(&self) -> Result<i64> {
        // self.writer.flush()?;
        Ok((self.writer.crc().sum() as i64) & 0xffff_ffffi64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn test_write_byte() {
        let path: PathBuf = Path::new("hello.txt").into();
        let mut fsout = FSIndexOutput::new(&path).unwrap();
        fsout.write_byte(b'a').unwrap();
        assert_eq!(fsout.file_pointer(), 1);
        ::std::fs::remove_file("hello.txt").unwrap();
    }
}
