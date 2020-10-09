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

use core::store::io::{DataInput, IndexInput, RandomAccessInput};

use error::ErrorKind::{IllegalArgument, IllegalState};
use error::Result;
use memmap::{Mmap, MmapOptions};
use std::fmt::Debug;
use std::fs::File;
use std::io::{self, Read};
use std::path::Path;
use std::slice;
use std::sync::Arc;

pub struct ReadOnlySource {
    map: Arc<Mmap>,
    offset: u64,
    len: u64,
}

impl ReadOnlySource {
    pub fn range(&self, offset: u64, len: u64) -> Result<ReadOnlySource> {
        if self.len < offset + len {
            bail!(IllegalArgument("Slice too big".to_owned()));
        }

        let source = ReadOnlySource {
            map: Arc::clone(&self.map),
            offset: self.offset + offset,
            len,
        };

        Ok(source)
    }

    fn len(&self) -> u64 {
        self.len
    }

    /// Returns the data underlying the ReadOnlySource object.
    pub fn as_slice(&self) -> &[u8] {
        let offset = self.offset as usize;
        let stop = (self.offset + self.len) as usize;
        unsafe { &slice::from_raw_parts(self.map.as_ptr(), self.map.len())[offset..stop] }
    }

    /// Splits into 2 `ReadOnlySource`, at the offset given
    /// as an argument.
    pub fn split(self, addr: u64) -> Result<(ReadOnlySource, ReadOnlySource)> {
        let left = self.slice(0, addr)?;
        let right = self.slice_from(addr)?;
        Ok((left, right))
    }

    /// Creates a ReadOnlySource that is just a
    /// view over a slice of the data.
    ///
    /// Keep in mind that any living slice extends
    /// the lifetime of the original ReadOnlySource,
    ///
    /// For instance, if `ReadOnlySource` wraps 500MB
    /// worth of data in anonymous memory, and only a
    /// 1KB slice is remaining, the whole `500MBs`
    /// are retained in memory.
    pub fn slice(&self, from_offset: u64, to_offset: u64) -> Result<ReadOnlySource> {
        if from_offset > to_offset {
            bail!(IllegalArgument(format!(
                "from_offset must be <= to_offset, got: from_offset: {} > to_offset: {}",
                from_offset, to_offset
            )));
        }
        self.range(from_offset, to_offset - from_offset)
    }

    /// Like `.slice(...)` but enforcing only the `from`
    /// boundary.
    ///
    /// Equivalent to `.slice(from_offset, self.len())`
    pub fn slice_from(&self, from_offset: u64) -> Result<ReadOnlySource> {
        let len = self.len();
        self.slice(from_offset, len)
    }

    /// Like `.slice(...)` but enforcing only the `to`
    /// boundary.
    ///
    /// Equivalent to `.slice(0, to_offset)`
    pub fn slice_to(&self, to_offset: u64) -> Result<ReadOnlySource> {
        self.slice(0, to_offset)
    }
}

impl Clone for ReadOnlySource {
    fn clone(&self) -> Self {
        ReadOnlySource {
            map: Arc::clone(&self.map),
            offset: self.offset,
            len: self.len,
        }
    }
}

impl From<Arc<Mmap>> for ReadOnlySource {
    fn from(mmap: Arc<Mmap>) -> ReadOnlySource {
        let len = mmap.len() as u64;
        ReadOnlySource {
            map: mmap,
            offset: 0,
            len,
        }
    }
}

#[derive(Clone)]
pub struct MmapIndexInput {
    source: ReadOnlySource,
    position: usize,
    slice: &'static [u8],
    description: String,
}

unsafe impl Send for MmapIndexInput {}

unsafe impl Sync for MmapIndexInput {}

impl From<ReadOnlySource> for MmapIndexInput {
    fn from(source: ReadOnlySource) -> Self {
        let len = source.len();
        let slice_ptr: *const u8 = source.as_slice().as_ptr();
        let slice = unsafe { slice::from_raw_parts(slice_ptr, len as usize) };
        MmapIndexInput {
            source,
            slice,
            position: 0,
            description: String::from(""),
        }
    }
}

impl MmapIndexInput {
    pub fn new<P: AsRef<Path> + Debug>(name: P) -> Result<MmapIndexInput> {
        let mmap = MmapIndexInput::mmap(name.as_ref(), 0, 0)?;
        Ok(mmap
            .map(ReadOnlySource::from)
            .map(MmapIndexInput::from)
            .ok_or_else(|| IllegalState(format!("Memmap empty file: {:?}", name)))?)
    }

    pub fn mmap(path: &Path, offset: usize, length: usize) -> Result<Option<Arc<Mmap>>> {
        let file = File::open(path)?;
        let meta_data = file.metadata()?;
        let file_len = meta_data.len() as usize;
        if file_len == 0 {
            Ok(None)
        } else if file_len < offset + length {
            bail!(IllegalArgument(format!(
                "Mapping end offset `{}` is beyond the size `{}` of file `{:?}`",
                offset + length,
                file_len,
                path
            )))
        } else {
            let adapted_len = if length == 0 {
                file_len - offset
            } else {
                length
            };
            let mmap = unsafe {
                MmapOptions::new()
                    .offset(offset)
                    .len(adapted_len)
                    .map(&file)?
            };
            Ok(Some(Arc::new(mmap)))
        }
    }

    fn slice_impl(&self, description: &str, offset: i64, length: i64) -> Result<Self> {
        let total_len = self.len() as i64;
        if offset < 0 || length < 0 || offset + length > total_len {
            bail!(IllegalArgument(format!(
                "Illegal (offset, length) slice: ({}, {}) for file of length: {}",
                offset, length, total_len
            )));
        };

        let slice = &self.slice[offset as usize..(offset + length) as usize];
        Ok(MmapIndexInput {
            slice,
            source: self.source.clone(),
            position: 0,
            description: description.to_string(),
        })
    }

    #[inline]
    fn check_random_access(&self, from: u64, len: u64) -> Result<()> {
        if from + len > self.len() {
            let msg = format!(
                "invalid position, expecting 0 < pos < {}, got: {}",
                self.len(),
                from
            );
            bail!(IllegalArgument(msg));
        }
        Ok(())
    }
}

impl IndexInput for MmapIndexInput {
    fn clone(&self) -> Result<Box<dyn IndexInput>> {
        Ok(Box::new(Clone::clone(self)))
    }

    fn file_pointer(&self) -> i64 {
        self.position as i64
    }

    fn seek(&mut self, pos: i64) -> Result<()> {
        self.position = pos as usize;
        Ok(())
    }

    #[inline]
    fn len(&self) -> u64 {
        self.slice.len() as u64
    }

    fn random_access_slice(&self, offset: i64, length: i64) -> Result<Box<dyn RandomAccessInput>> {
        let boxed = self.slice_impl("RandomAccessSlice", offset, length)?;
        Ok(Box::new(boxed))
    }

    #[inline(always)]
    unsafe fn get_and_advance(&mut self, length: usize) -> *const u8 {
        debug_assert!(self.position + length <= self.slice.len());
        let ptr = self.slice.as_ptr().add(self.position);
        self.position += length;
        ptr
    }

    fn slice(&self, description: &str, offset: i64, length: i64) -> Result<Box<dyn IndexInput>> {
        let boxed = self.slice_impl(description, offset, length)?;
        Ok(Box::new(boxed))
    }

    fn name(&self) -> &str {
        "MmapIndexInput" // hard-coded
    }
}

impl DataInput for MmapIndexInput {
    fn read_byte(&mut self) -> Result<u8> {
        let b = self.slice[self.position];
        self.position += 1;
        Ok(b)
    }

    //    fn read_vint(&mut self) -> Result<i32> {
    //        let mut slice = &self.slice[self.position..];
    //        let start = slice.as_ptr() as usize;
    //        let v = slice.read_vint()?;
    //        self.position += slice.as_ptr() as usize - start;
    //        Ok(v)
    //    }
    //
    //    fn read_vlong(&mut self) -> Result<i64> {
    //        let mut slice = &self.slice[self.position..];
    //        let start = slice.as_ptr() as usize;
    //        let v = slice.read_vlong()?;
    //        self.position += slice.as_ptr() as usize - start;
    //        Ok(v)
    //    }

    fn skip_bytes(&mut self, count: usize) -> Result<()> {
        if self.position + count > self.slice.len() {
            bail!(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "failed to fill whole buffer"
            ));
        }
        self.position += count;
        Ok(())
    }
}

impl Read for MmapIndexInput {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let count = buf.len().min(self.slice.len() - self.position);
        buf[..count].copy_from_slice(&self.slice[self.position..self.position + count]);

        self.position += count;
        Ok(count)
    }
}

impl RandomAccessInput for MmapIndexInput {
    fn read_byte(&self, pos: u64) -> Result<u8> {
        self.check_random_access(pos as u64, 1)?;
        Ok(self.slice[pos as usize])
    }

    fn read_short(&self, pos: u64) -> Result<i16> {
        self.check_random_access(pos, 2)?;
        (&self.slice[pos as usize..]).read_short()
    }

    fn read_int(&self, pos: u64) -> Result<i32> {
        self.check_random_access(pos, 4)?;
        (&self.slice[pos as usize..]).read_int()
    }

    fn read_long(&self, pos: u64) -> Result<i64> {
        self.check_random_access(pos, 8)?;
        (&self.slice[pos as usize..]).read_long()
    }
}

#[cfg(test)]
mod tests {
    extern crate tempfile;

    use super::*;
    use core::store::io::DataOutput;
    use core::store::io::FSIndexOutput;
    use std::io::Write;
    use std::path::{Path, PathBuf};

    #[test]
    fn test_mmap_index_input() {
        let name = "test.txt";
        let temp_dir = tempfile::tempdir().unwrap();
        let path: PathBuf = temp_dir.path().join(name);

        let mut fsout = FSIndexOutput::new(name.to_string(), &path).unwrap();
        fsout.write_byte(b'a').unwrap();
        fsout.write_short(0x7F_i16).unwrap();
        fsout.write_long(567_890).unwrap();
        fsout.write_int(1_234_567).unwrap();
        fsout.write_byte(b'b').unwrap();
        fsout.flush().unwrap();

        let mmap_input = MmapIndexInput::new(&path).unwrap();
        let mut slice = mmap_input.slice("from3", 3, 13).unwrap();
        assert_eq!(slice.read_long().unwrap(), 567_890_i64);
        assert_eq!(slice.read_int().unwrap(), 1_234_567_i32);
        assert!(slice.read_int().is_err());
    }

    #[test]
    fn test_mmap_random_access_input() {
        let path: PathBuf = Path::new("test.txt").into();
        let name = "test.txt";

        let mut fsout = FSIndexOutput::new(name.to_string(), &path).unwrap();
        fsout.write_byte(b'a').unwrap();
        fsout.write_short(0x7F_i16).unwrap();
        fsout.write_long(567_890).unwrap();
        fsout.write_int(1_234_567).unwrap();
        fsout.write_byte(b'b').unwrap();
        fsout.flush().unwrap();

        let mmap_input = MmapIndexInput::new(name).unwrap();
        let random_input = mmap_input.random_access_slice(1, 15).unwrap();
        assert_eq!(0x7f_i16, random_input.read_short(0).unwrap());
        assert_eq!(567_890, random_input.read_long(2).unwrap());
        assert_eq!(1_234_567, random_input.read_int(10).unwrap());
        assert_eq!(b'b', random_input.read_byte(14).unwrap());
        assert_eq!(1_234_567, random_input.read_int(10).unwrap());

        assert!(random_input.read_int(15).is_err());

        ::std::fs::remove_file(name).unwrap();
    }
}
