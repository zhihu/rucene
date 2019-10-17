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

use core::store::io::{DataInput, DataOutput};

use error::Result;
use std::cmp::min;
use std::io::{self, Read, Write};
use std::sync::Arc;

pub struct ByteArrayRef(Arc<Vec<u8>>);

impl ByteArrayRef {
    pub fn new(v: Arc<Vec<u8>>) -> ByteArrayRef {
        ByteArrayRef(v)
    }
}

impl AsRef<[u8]> for ByteArrayRef {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// DataInput backed by a byte array.
///
/// *WARNING:* This class omits all low-level checks.
pub struct ByteArrayDataInput<T: AsRef<[u8]>> {
    bytes: T,
    pos: usize,
}

impl<T: AsRef<[u8]>> ByteArrayDataInput<T> {
    pub fn new(bytes: T) -> ByteArrayDataInput<T> {
        ByteArrayDataInput { bytes, pos: 0usize }
    }

    pub fn rewind(&mut self) {
        self.pos = 0;
    }

    pub fn position(&self) -> usize {
        self.pos
    }

    pub fn set_position(&mut self, pos: usize) {
        self.pos = pos;
    }

    pub fn length(&self) -> usize {
        self.bytes.as_ref().len()
    }

    pub fn eof(&self) -> bool {
        self.pos == self.length()
    }

    pub fn reset(&mut self, bytes: T) {
        self.bytes = bytes;
        self.pos = 0;
    }

    pub fn get_slice(&self, pos: usize, len: usize) -> Result<&[u8]> {
        let limit = self.bytes.as_ref().len();
        if pos < self.pos || pos > limit || pos + len > limit {
            bail!(
                "Invalid Argument: slice ({}, {}) is beyond valid range of ({}, {})",
                pos,
                pos + len,
                self.pos,
                limit
            )
        }
        Ok(&self.bytes.as_ref()[pos..pos + len])
    }
}

impl<T: AsRef<[u8]>> DataInput for ByteArrayDataInput<T> {
    fn read_byte(&mut self) -> Result<u8> {
        let b = self.bytes.as_ref()[self.pos];
        self.pos += 1;
        Ok(b)
    }

    fn read_bytes(&mut self, b: &mut [u8], offset: usize, len: usize) -> Result<()> {
        b[offset..offset + len].copy_from_slice(&self.bytes.as_ref()[self.pos..self.pos + len]);
        self.pos += len;
        Ok(())
    }

    fn skip_bytes(&mut self, count: usize) -> Result<()> {
        self.pos += count;
        Ok(())
    }
}

impl<T: AsRef<[u8]>> Read for ByteArrayDataInput<T> {
    fn read(&mut self, buf: &mut [u8]) -> ::std::io::Result<usize> {
        let size = ::std::cmp::min(buf.len(), self.length() - self.pos);
        buf[0..size].copy_from_slice(&self.bytes.as_ref()[self.pos..self.pos + size]);
        self.pos += size;
        Ok(size)
    }
}

/// DataOutput backed by a byte array.
pub struct ByteArrayDataOutput<T> {
    bytes: T,
    pub pos: usize,
    limit: usize,
}

impl<T> ByteArrayDataOutput<T>
where
    T: AsMut<[u8]>,
{
    pub fn new(bytes: T, offset: usize, len: usize) -> ByteArrayDataOutput<T> {
        ByteArrayDataOutput {
            bytes,
            pos: offset,
            limit: offset + len,
        }
    }

    #[inline]
    fn bytes_slice(&mut self) -> &mut [u8] {
        self.bytes.as_mut()
    }
}

impl<T> Write for ByteArrayDataOutput<T>
where
    T: AsMut<[u8]>,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let length = min(self.limit - self.pos, buf.len());
        let pos = self.pos;
        self.bytes_slice()[pos..pos + length].copy_from_slice(&buf[..length]);
        self.pos += length;
        Ok(length)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<T> DataOutput for ByteArrayDataOutput<T> where T: AsMut<[u8]> {}
