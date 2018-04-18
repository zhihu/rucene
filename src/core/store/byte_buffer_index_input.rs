use core::store::{DataInput, IndexInput, RandomAccessInput};
use error::Result;
use std::convert::{From, Into};
use std::io::Read;

impl<'a> DataInput for &'a [u8] {}

pub struct ByteBufferIndexInput {
    buffer: Vec<u8>,
    pos: usize,
    limit: usize,
}

impl ByteBufferIndexInput {
    pub fn backing(&self) -> &[u8] {
        &self.buffer
    }
}

impl ByteBufferIndexInput {
    pub fn with_capacity(capacity: usize) -> ByteBufferIndexInput {
        ByteBufferIndexInput {
            buffer: vec![0u8; capacity],
            pos: 0,
            limit: 0,
        }
    }

    pub fn reload_with_len<T: DataInput + ?Sized>(
        &mut self,
        input: &mut T,
        len: usize,
    ) -> Result<()> {
        if self.buffer.len() < len {
            self.buffer.resize(len, 0);
        }
        let buffer = &mut self.buffer[0..len];
        input.read_exact(buffer)?;
        self.pos = 0;
        self.limit = len;
        Ok(())
    }

    pub fn reload<T: DataInput + ?Sized>(&mut self, input: &mut T) -> Result<()> {
        let len = input.read_vint()? as usize;
        self.reload_with_len(input, len)
    }

    pub fn reload_slice(&mut self, slice: &[u8]) -> Result<()> {
        self.buffer.resize(0, 0);
        self.buffer.extend(slice.iter());
        self.pos = 0;
        self.limit = slice.len();
        Ok(())
    }

    pub fn reset(&mut self, v: Vec<u8>) {
        self.limit = v.len();
        self.pos = 0;
        self.buffer = v;
    }

    pub fn position(&self) -> usize {
        self.pos
    }

    pub fn get_slice(&self, pos: i64, len: usize) -> Result<&[u8]> {
        let pos = pos as usize;
        if pos < self.pos || pos > self.limit || pos + len > self.limit {
            bail!(
                "Invalid Argument: slice ({}, {}) is beyond valid range of ({}, {})",
                pos,
                pos + len,
                self.pos,
                self.limit
            )
        }
        Ok(&self.buffer[pos..pos + len])
    }

    pub fn get_slice_clone(&self, pos: i64, len: usize) -> Result<Vec<u8>> {
        let slice = self.get_slice(pos, len)?;
        let mut vec = Vec::with_capacity(len);
        vec.extend(slice.iter());
        Ok(vec)
    }

    fn clone_impl(&self) -> ByteBufferIndexInput {
        ByteBufferIndexInput {
            buffer: self.buffer.clone(),
            pos: self.pos,
            limit: self.limit,
        }
    }
}

impl From<Vec<u8>> for ByteBufferIndexInput {
    fn from(f: Vec<u8>) -> ByteBufferIndexInput {
        ByteBufferIndexInput {
            limit: f.len(),
            buffer: f,
            pos: 0,
        }
    }
}

impl DataInput for ByteBufferIndexInput {
    // TODO: No check?
    fn skip_bytes(&mut self, count: usize) -> Result<()> {
        self.pos += count;
        Ok(())
    }
}

impl IndexInput for ByteBufferIndexInput {
    fn file_pointer(&self) -> i64 {
        self.pos as i64
    }

    fn seek(&mut self, pos: i64) -> Result<()> {
        if pos < 0 || pos > self.limit as i64 {
            bail!(
                "Invalid Argument: position {} is beyond valid range of [0, {})",
                pos,
                self.limit
            )
        }
        self.pos = pos as usize;
        Ok(())
    }

    fn len(&self) -> u64 {
        self.buffer.len() as u64
    }

    fn clone(&self) -> Result<Box<IndexInput>> {
        Ok(Box::new(self.clone_impl()))
    }

    fn name(&self) -> &str {
        "ByteBufferIndexInput"
    }

    fn random_access_slice(&self, _offset: i64, _length: i64) -> Result<Box<RandomAccessInput>> {
        Ok(Box::new(self.clone_impl()))
    }
}

impl RandomAccessInput for ByteBufferIndexInput {
    fn read_byte(&self, pos: i64) -> Result<u8> {
        self.get_slice(pos, 1)?.read_byte()
    }
    fn read_short(&self, pos: i64) -> Result<i16> {
        self.get_slice(pos, 2)?.read_short()
    }
    fn read_int(&self, pos: i64) -> Result<i32> {
        self.get_slice(pos, 4)?.read_int()
    }
    fn read_long(&self, pos: i64) -> Result<i64> {
        self.get_slice(pos, 8)?.read_long()
    }
}

impl Read for ByteBufferIndexInput {
    fn read(&mut self, buf: &mut [u8]) -> ::std::io::Result<usize> {
        let read = (&self.buffer[self.pos..self.limit]).read(buf)?;
        self.pos += read;
        Ok(read)
    }
}

impl Drop for ByteBufferIndexInput {
    fn drop(&mut self) {}
}
