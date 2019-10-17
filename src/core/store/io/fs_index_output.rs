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

use core::store::io::{DataOutput, IndexOutput};

use error::Result;

use std::fs::{File, OpenOptions};
use std::io::BufWriter;
use std::io::Write;
use std::path::Path;

use flate2::CrcWriter;

const CHUNK_SIZE: usize = 8192;

/// `IndexOutput` implement for `FsDirectory`
pub struct FSIndexOutput {
    name: String,
    writer: CrcWriter<BufWriter<File>>,
    bytes_written: usize,
}

impl FSIndexOutput {
    pub fn new<P: AsRef<Path>>(name: String, path: P) -> Result<FSIndexOutput> {
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
            error!("Oops, failed to flush {}, errmsg: {}", self.name, desc);
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
        &self.name
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
    use std::path::{Path, PathBuf};

    #[test]
    fn test_write_byte() {
        let name = "hello.txt";
        let path: PathBuf = Path::new(name).into();
        let mut fsout = FSIndexOutput::new(name.to_string(), &path).unwrap();
        fsout.write_byte(b'a').unwrap();
        assert_eq!(fsout.file_pointer(), 1);
        ::std::fs::remove_file("hello.txt").unwrap();
    }
}
