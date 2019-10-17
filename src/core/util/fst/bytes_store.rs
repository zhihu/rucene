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

use core::store::io::{DataInput, DataOutput, IndexInput, RandomAccessInput};
use core::util::fst::BytesReader;

use error::{ErrorKind, Result};

use std::cmp::min;
use std::io;
use std::io::{Read, Write};
use std::mem;
use std::vec::Vec;

#[derive(Debug)]
pub struct BytesStore {
    block_size: usize,
    pub block_bits: usize,
    block_mask: usize,
    blocks: Vec<Vec<u8>>,
    current_index: isize,
    // the index of current block in blocks, -1 for invalid
}

impl BytesStore {
    pub fn with_block_bits(block_bits: usize) -> BytesStore {
        let block_size = 1 << block_bits;
        let block_mask = block_size - 1;
        BytesStore {
            block_size,
            block_bits,
            block_mask,
            blocks: vec![],
            current_index: -1,
        }
    }

    pub fn new<T: DataInput + ?Sized>(
        input: &mut T,
        num_bytes: usize,
        max_block_size: usize,
    ) -> Result<BytesStore> {
        let mut block_size: usize = 2;
        let mut block_bits: usize = 1;

        while block_size < num_bytes && block_size < max_block_size {
            block_size *= 2;
            block_bits += 1;
        }

        let mut blocks: Vec<Vec<u8>> = vec![];

        let mut left = num_bytes;
        while left > 0 {
            let chunk = min(block_size, left);
            let mut block = vec![0; chunk];
            input.read_exact(block.as_mut_slice())?;
            blocks.push(block);

            if left >= chunk {
                left -= chunk;
            } else {
                break;
            }
        }

        Ok(BytesStore {
            block_size,
            block_bits,
            block_mask: block_size - 1,
            blocks,
            current_index: -1,
        })
    }

    pub fn len(&self) -> usize {
        if !self.blocks.is_empty() {
            self.block_size * (self.blocks.len() - 1) + self.blocks[self.blocks.len() - 1].len()
        } else {
            0
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline(always)]
    fn write_bytes_unchecked(&mut self, idx: usize, bytes: &[u8]) {
        let cur_block = &mut self.blocks[idx];
        debug_assert!(cur_block.capacity() - cur_block.len() >= bytes.len());
        let start = cur_block.len();
        let end = start + bytes.len();
        unsafe {
            cur_block.set_len(end);
        }
        cur_block[start..end].copy_from_slice(bytes);
    }
}

impl DataOutput for BytesStore {}

impl Write for BytesStore {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut len = buf.len();
        let mut offset: usize = 0;
        if self.current_index < 0 {
            assert!(self.blocks.is_empty());
            let new_block = Vec::with_capacity(self.block_size);
            self.blocks.push(new_block);
            self.current_index += 1;
        }
        assert_eq!(self.current_index, self.blocks.len() as isize - 1);
        while len > 0 {
            let idx = self.current_index as usize;
            let chunk = self.block_size - self.blocks[idx].len();
            if len <= chunk {
                self.write_bytes_unchecked(idx, &buf[offset..offset + len]);
                offset += len;
                break;
            } else {
                if chunk > 0 {
                    self.write_bytes_unchecked(idx, &buf[offset..offset + chunk]);
                    offset += chunk;
                    len -= chunk;
                }
                let current = Vec::with_capacity(self.block_size);
                // we are sure it won't be used in multiple threads during writing
                self.blocks.push(current);
                self.current_index += 1;
            }
        }

        Ok(offset)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.finish();
        Ok(())
    }
}

impl BytesStore {
    pub fn get_position(&self) -> usize {
        if self.blocks.is_empty() {
            assert_eq!(self.current_index, -1);
            0
        } else {
            let length = self.blocks.len() - 1;
            length * self.block_size + self.blocks[length].len()
        }
    }

    // unsafe implement, used for when bytes is a slice of self.blocks
    unsafe fn write_bytes_local_unsafe(
        &mut self,
        dest: usize,
        bytes: *const [u8],
        offset: usize,
        len: usize,
    ) {
        self.write_bytes_local(dest, &*bytes, offset, len)
    }

    /// Absolute writeBytes without changing the current
    /// position.  Note: this cannot "grow" the bytes, so you
    /// must only call it on already written parts.
    #[allow(dead_code)]
    pub fn write_bytes_local(&mut self, dest: usize, bytes: &[u8], offset: usize, len: usize) {
        assert!(dest + len <= self.get_position());

        let end = dest + len;
        let mut block_index = end >> self.block_bits;
        let mut down_to = end & self.block_mask;
        if down_to == 0 {
            block_index -= 1;
            down_to = self.block_size;
        }

        let mut len = len;
        while len > 0 {
            if len <= down_to {
                self.blocks[block_index][down_to - len..down_to]
                    .copy_from_slice(&bytes[offset..offset + len]);
                break;
            } else {
                len -= down_to;
                self.blocks[block_index][..down_to]
                    .copy_from_slice(&bytes[offset + len..offset + len + down_to]);
                block_index -= 1;
                down_to = self.block_size;
            }
        }
    }

    fn grow(&mut self, size: usize) {
        debug_assert!(size > 0);
        let mut size = size;
        while size > 0 {
            let current_index = self.current_index as usize;
            if size <= self.block_size - self.blocks[current_index].len() {
                let new_len = self.blocks[current_index].len() + size;
                self.blocks[current_index].resize(new_len, 0u8);
                break;
            } else {
                size -= self.block_size - self.blocks[current_index].len();
                let new_len = self.block_size;
                self.blocks[current_index].resize(new_len, 0u8);
                let new_block = Vec::with_capacity(self.block_size);
                self.blocks.push(new_block);
                self.current_index += 1;
            }
        }
    }

    /// Absolute copy bytes self to self, without changing the
    /// position. Note: this cannot "grow" the bytes, so must
    /// only call it on already written parts.
    #[allow(dead_code)]
    pub fn copy_bytes_local(&mut self, src: usize, dest: usize, len: usize) {
        assert!(src < dest);

        if dest + len > self.get_position() {
            let grow_size = dest + len - self.get_position();
            self.grow(grow_size);
        }

        // no overlap, copy directly
        if src + len <= dest {
            let end = src + len;
            let mut block_index = end >> self.block_bits;
            let mut down_to = end & self.block_mask;
            if down_to == 0 {
                block_index -= 1;
                down_to = self.block_size;
            }
            let mut len = len;
            while len > 0 {
                unsafe {
                    let bytes = self.blocks[block_index].as_ref() as *const [u8];
                    if len <= down_to {
                        self.write_bytes_local_unsafe(dest, bytes, down_to - len, len);
                        break;
                    } else {
                        len -= down_to;
                        self.write_bytes_local_unsafe(dest + len, bytes, 0, down_to);
                        block_index -= 1;
                        down_to = self.block_size;
                    }
                }
            }
        } else {
            // has over lap, copy by byte from end
            let block_bits = self.block_bits;
            let block_mask = self.block_mask;
            for i in 0..len {
                let cur_dest = dest + len - 1 - i;
                let cur_src = src + len - 1 - i;
                let value = self.blocks[cur_src >> block_bits][cur_src & block_mask];
                self.blocks[cur_dest >> block_bits][cur_dest & block_mask] = value;
            }
        }
    }

    /// Writes an int at the absolute position without
    /// changing the current pointer.
    #[allow(dead_code)]
    pub fn write_int_local(&mut self, pos: usize, value: i32) {
        let mut block_index = pos >> self.block_bits;
        let mut upto = pos & self.block_mask;
        let mut shift = 24;
        for _ in 0..4 {
            self.blocks[block_index][upto] = (value >> shift) as u8;
            upto += 1;
            shift -= 8;
            if upto == self.block_size {
                upto = 0;
                block_index += 1;
            }
        }
    }

    /// Reverse from src_pos, inclusive, to dest_pos, inclusive.
    #[allow(dead_code)]
    pub fn reverse(&mut self, src_pos: usize, dest_pos: usize) {
        assert!(src_pos < dest_pos);
        assert!(dest_pos < self.get_position());

        let mut src_block_index = src_pos >> self.block_bits;
        let mut src = src_pos & self.block_mask;
        let mut dest_block_index = dest_pos >> self.block_bits;
        let mut dest = dest_pos & self.block_mask;
        let limit = (dest_pos - src_pos + 1) / 2;

        for _ in 0..limit {
            let tmp = self.blocks[src_block_index][src];
            self.blocks[src_block_index][src] = self.blocks[dest_block_index][dest];
            self.blocks[dest_block_index][dest] = tmp;

            src += 1;
            if src == self.block_size {
                src_block_index += 1;
                src = 0;
            }

            if dest == 0 {
                dest_block_index -= 1;
                dest = self.block_size - 1;
            } else {
                dest -= 1;
            }
        }
    }

    #[allow(dead_code)]
    pub fn skip_bytes(&mut self, len: usize) {
        let mut len = len;
        while len > 0 {
            let current_block_size = (&self.blocks)[self.current_index as usize].len();
            let chunk = self.block_size - current_block_size;
            if len <= chunk {
                self.blocks[self.current_index as usize].resize(current_block_size + len, 0u8);
                break;
            } else {
                len -= chunk;
                self.blocks[self.current_index as usize].resize(self.block_size, 0u8);
                let current = Vec::with_capacity(self.block_size);
                self.blocks.push(current);
                self.current_index += 1;
            }
        }
    }

    /// Pos must be less than the max position written so far!
    /// Ie, you cannot "grow" the file with this! */]
    #[allow(dead_code)]
    pub fn truncate(&mut self, new_len: usize) {
        assert!(new_len <= self.get_position());
        let mut block_index = new_len >> self.block_bits;
        {
            let length = new_len & self.block_bits;
            if length == 0 {
                if block_index > 0 {
                    block_index -= 1;
                }
            } else {
                self.blocks[block_index].truncate(length);
            }
            self.blocks.truncate(block_index);
        }

        if new_len == 0 {
            self.current_index = -1;
        } else {
            self.current_index = block_index as isize;
        }
    }

    pub fn finish(&mut self) {
        if self.current_index >= 0 {
            let idx = self.current_index as usize;
            debug_assert_eq!(idx, self.blocks.len() - 1);
            let mut buffer = vec![0u8; self.blocks[idx].len()];
            buffer.copy_from_slice(&self.blocks[idx]);
            self.blocks[idx] = buffer;
            self.current_index = -1;
        }
    }

    /// Writes all of our bytes to the target {@link DataOutput}.
    #[allow(dead_code)]
    pub fn write_to<T: DataOutput + ?Sized>(&self, output: &mut T) -> Result<()> {
        let length = self.blocks.len();
        for i in 0..length {
            output.write_bytes(self.blocks[i].as_slice(), 0, self.blocks[i].len())?;
        }
        Ok(())
    }
}

enum VecStores {
    Owned(Vec<Vec<u8>>),
    Borrowed(*const Vec<Vec<u8>>),
}

impl AsRef<[Vec<u8>]> for VecStores {
    fn as_ref(&self) -> &[Vec<u8>] {
        match self {
            VecStores::Owned(o) => &o,
            VecStores::Borrowed(v) => unsafe { &**v },
        }
    }
}

pub struct StoreBytesReader {
    blocks: VecStores,
    pub length: usize,
    pub block_size: usize,
    pub block_bits: usize,
    pub block_mask: usize,
    pub block_index: usize,
    pub next_read: usize,
    pub reversed: bool,
}

unsafe impl Send for StoreBytesReader {}

unsafe impl Sync for StoreBytesReader {}

impl StoreBytesReader {
    fn new(bytes_store: &BytesStore, reversed: bool) -> StoreBytesReader {
        let length = bytes_store.len();
        StoreBytesReader {
            blocks: VecStores::Borrowed(&bytes_store.blocks),
            length,
            block_size: bytes_store.block_size,
            block_bits: bytes_store.block_bits,
            block_mask: bytes_store.block_mask,
            block_index: 0,
            next_read: 0,
            reversed,
        }
    }

    pub fn from_bytes_store(mut bytes_store: BytesStore, reversed: bool) -> Self {
        let length = bytes_store.len();
        let blocks = mem::replace(&mut bytes_store.blocks, Vec::with_capacity(0));
        StoreBytesReader {
            blocks: VecStores::Owned(blocks),
            length,
            block_size: bytes_store.block_size,
            block_bits: bytes_store.block_bits,
            block_mask: bytes_store.block_mask,
            block_index: 0,
            next_read: 0,
            reversed,
        }
    }

    #[inline]
    fn remain(&mut self) -> usize {
        if self.reversed {
            self.position() + 1
        } else {
            self.length - self.position()
        }
    }
}

impl BytesReader for StoreBytesReader {
    #[inline]
    fn position(&self) -> usize {
        self.block_index * self.block_size + self.next_read
    }

    fn set_position(&mut self, pos: usize) {
        self.block_index = pos >> self.block_bits;
        self.next_read = pos & self.block_mask;
        debug_assert_eq!(pos, self.block_index * self.block_size + self.next_read);
    }

    fn reversed(&self) -> bool {
        self.reversed
    }
}

impl DataInput for StoreBytesReader {
    fn read_byte(&mut self) -> Result<u8> {
        let b = unsafe {
            *(*((*self.blocks.as_ref()).as_ptr().add(self.block_index)))
                .as_ptr()
                .add(self.next_read)
        };

        if self.reversed {
            if self.next_read == 0 {
                if self.block_index > 0 {
                    self.block_index -= 1;
                    self.next_read = self.block_size - 1;
                }
            } else {
                self.next_read -= 1;
            }
        } else {
            self.next_read += 1;

            if self.next_read == self.block_size {
                self.block_index += 1;
                self.next_read = 0;
            }
        }

        Ok(b)
    }

    fn read_bytes(&mut self, b: &mut [u8], offset: usize, len: usize) -> Result<()> {
        unsafe {
            let ptr = b.as_mut_ptr().add(offset);
            for i in 0..len {
                *ptr.add(i) = self.read_byte()?;
            }
        }

        Ok(())
    }

    fn skip_bytes(&mut self, count: usize) -> Result<()> {
        let pos = self.position();

        if self.reversed {
            debug_assert!(pos > count);
            self.set_position(pos - count);
        } else {
            self.set_position(pos + count);
        }

        Ok(())
    }
}

impl IndexInput for StoreBytesReader {
    fn clone(&self) -> Result<Box<dyn IndexInput>> {
        unreachable!()
    }

    fn file_pointer(&self) -> i64 {
        self.position() as i64
    }

    fn seek(&mut self, pos: i64) -> Result<()> {
        if pos < 0 || pos > self.length as i64 {
            bail!(ErrorKind::IllegalArgument("pos out of range!".into()));
        }
        self.set_position(pos as usize);
        Ok(())
    }

    fn len(&self) -> u64 {
        self.length as u64
    }

    fn name(&self) -> &str {
        "IndexInput(BytesStore)"
    }

    fn random_access_slice(
        &self,
        _offset: i64,
        _length: i64,
    ) -> Result<Box<dyn RandomAccessInput>> {
        unreachable!()
    }
}

impl Read for StoreBytesReader {
    fn read(&mut self, mut b: &mut [u8]) -> io::Result<usize> {
        let total = b.len().min(self.remain());
        let mut left = total;
        while left > 0 {
            if self.reversed {
                let cur_buf = &self.blocks.as_ref()[self.block_index];
                let len = (self.next_read + 1).min(left);
                let start = self.next_read + 1 - len;
                cur_buf[start..start + len]
                    .iter()
                    .rev()
                    .zip(&mut b[..len])
                    .for_each(|(s, d)| *d = *s);
                b = &mut b[len..];
                left -= len;
                if self.next_read < len {
                    // XXX: compatible with current code, don't coredump when block_index=0
                    if self.block_index == 0 {
                        self.block_index = usize::max_value();
                    } else {
                        self.block_index -= 1;
                    }
                    self.next_read = self.block_size - 1;
                } else {
                    self.next_read -= len;
                }
            } else {
                let cur_buf = &self.blocks.as_ref()[self.block_index];
                let len = left.min(cur_buf.len() - self.next_read);
                b[..len].copy_from_slice(&cur_buf[self.next_read..self.next_read + len]);
                b = &mut b[len..];
                self.next_read += len;
                if self.next_read == self.block_size {
                    self.block_index += 1;
                    self.next_read = 0;
                }
                left -= len;
            }
        }

        Ok(total)
    }
}

impl BytesStore {
    pub fn get_forward_reader(&self) -> StoreBytesReader {
        StoreBytesReader::new(&self, false)
    }
    pub fn get_reverse_reader(&self) -> StoreBytesReader {
        StoreBytesReader::new(&self, true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::util::fst::tests::*;

    fn create_test_bytes_store() -> Result<BytesStore> {
        let mut outputs = TestBufferedDataIO::default();

        let bytes_in = [1, 2, 3, 4, 5, 6, 7, 8, 9];
        outputs.write_bytes(&bytes_in, 0, 9).unwrap();

        let store = BytesStore::new(&mut outputs, 9, 4)?;

        Ok(store)
    }

    #[test]
    fn test_forward_reader() {
        let store = create_test_bytes_store().unwrap();
        let mut forward_reader = store.get_forward_reader();

        assert_eq!(false, forward_reader.reversed());

        forward_reader.set_position(1);
        assert_eq!(forward_reader.position(), 1);

        assert_eq!(forward_reader.read_byte().unwrap(), 2);
        forward_reader.skip_bytes(1).unwrap();
        assert_eq!(forward_reader.read_byte().unwrap(), 4);

        let mut b = vec![0; 5];
        forward_reader.read_bytes(b.as_mut_slice(), 0, 5).unwrap();
        assert_eq!(b.as_slice(), [5, 6, 7, 8, 9]);
    }

    #[test]
    fn test_reverse_reader() {
        let bstore = create_test_bytes_store().unwrap();
        let mut reverse_reader = bstore.get_reverse_reader();

        assert_eq!(true, reverse_reader.reversed());
        reverse_reader.set_position(7);
        assert_eq!(reverse_reader.position(), 7);

        assert_eq!(reverse_reader.read_byte().unwrap(), 8);
        reverse_reader.skip_bytes(1).unwrap();
        assert_eq!(reverse_reader.read_byte().unwrap(), 6);

        let mut b = vec![0; 5];
        reverse_reader.read_bytes(b.as_mut_slice(), 0, 5).unwrap();
        assert_eq!(b.as_slice(), [5, 4, 3, 2, 1]);
    }
}
