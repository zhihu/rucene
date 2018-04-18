use core::store::DataInput;
use error::Result;
use std::io::Read;
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
}

impl<T: AsRef<[u8]>> DataInput for ByteArrayDataInput<T> {
    fn skip_bytes(&mut self, count: usize) -> Result<()> {
        self.pos += count;
        Ok(())
    }

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
}

impl<T: AsRef<[u8]>> Read for ByteArrayDataInput<T> {
    fn read(&mut self, buf: &mut [u8]) -> ::std::io::Result<usize> {
        let size = ::std::cmp::min(buf.len(), self.length() - self.pos);
        buf[0..size].copy_from_slice(&self.bytes.as_ref()[self.pos..self.pos + size]);
        self.pos += size;
        Ok(size)
    }
}

pub struct ByteSlicesDataInput<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> ByteSlicesDataInput<'a> {
    pub fn new(bytes: &'a [u8]) -> ByteSlicesDataInput {
        ByteSlicesDataInput { bytes, pos: 0usize }
    }

    pub fn position(&self) -> usize {
        self.pos
    }

    pub fn set_position(&mut self, pos: usize) {
        self.pos = pos;
    }
}

impl<'a> Read for ByteSlicesDataInput<'a> {
    fn read(&mut self, buf: &mut [u8]) -> ::std::io::Result<usize> {
        let size = ::std::cmp::min(buf.len(), self.bytes.len() - self.pos);
        buf[0..size].copy_from_slice(&self.bytes[self.pos..self.pos + size]);
        self.pos += size;
        Ok(size)
    }
}

impl<'a> DataInput for ByteSlicesDataInput<'a> {}
