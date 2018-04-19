use core::store::{DataInput, DataOutput};
use core::util::fst::BytesReader;
use error::*;
use std::cmp::min;
use std::io;
use std::io::{Read, Write};
use std::mem;
use std::sync::Arc;
use std::vec::Vec;

type BlockRef = Arc<Vec<Vec<u8>>>;

#[derive(Debug)]
pub struct BytesStore {
    block_size: usize,
    block_bits: usize,
    block_mask: usize,
    blocks: BlockRef,
    current: Vec<u8>,
}

impl BytesStore {
    pub fn with_block_bits(block_bits: usize) -> BytesStore {
        let block_size = 1 << block_bits;
        let block_mask = block_size - 1;
        BytesStore {
            block_size,
            block_bits,
            block_mask,
            blocks: BlockRef::new(vec![]),
            current: vec![0u8; block_size],
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
            input.read_bytes(block.as_mut_slice(), 0, chunk)?;
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
            blocks: Arc::new(blocks),
            current: vec![0 as u8; block_size],
        })
    }
}

impl DataOutput for BytesStore {}

impl Write for BytesStore {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut len = buf.len();
        let mut offset: usize = 0;
        while len > 0 {
            let chunk = self.block_size - self.current.len();
            if len <= chunk {
                self.current.extend(buf[offset..offset + len].iter());
                offset += len;
                break;
            } else {
                if chunk > 0 {
                    self.current.extend(buf[offset..offset + chunk].iter());
                    offset += chunk;
                    len -= chunk;
                }
                let mut current = vec![];
                mem::swap(&mut current, &mut self.current);
                // we are sure it won't be used in multiple threads during writing
                Arc::make_mut(&mut self.blocks).push(current);
            }
        }

        Ok(offset)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl BytesStore {
    #[allow(dead_code)]
    fn get_position(&self) -> Result<usize> {
        Ok(self.blocks.len() * self.block_size + self.current.len() as usize)
    }

    /// Absolute writeBytes without changing the current
    /// position.  Note: this cannot "grow" the bytes, so you
    /// must only call it on already written parts.
    #[allow(dead_code)]
    fn write_bytes_local(&mut self, _dest: usize, _bytes: &[u8], _offset: usize, _len: usize) {
        unimplemented!()
    }

    /// Absolute copy bytes self to self, without changing the
    /// position. Note: this cannot "grow" the bytes, so must
    /// only call it on already written parts.
    #[allow(dead_code)]
    fn copy_bytes_local(&mut self, _src: usize, _dest: usize, _len: usize) {
        unimplemented!()
    }

    /// Writes an int at the absolute position without
    /// changing the current pointer.
    ///
    #[allow(dead_code)]
    fn write_int_local(&mut self, _pos: usize, _value: i32) {
        unimplemented!()
    }

    /// Reverse from src_pos, inclusive, to dest_pos, inclusive.
    #[allow(dead_code)]
    fn reverse(&mut self, _src_pos: usize, _dest_pos: usize) {
        unimplemented!()
    }

    #[allow(dead_code)]
    fn skip_bytes(&mut self, _len: usize) {
        unimplemented!()
    }

    /// Pos must be less than the max position written so far!
    /// Ie, you cannot "grow" the file with this! */]
    #[allow(dead_code)]
    fn truncate(&mut self, _new_len: i64) {
        unimplemented!()
    }

    #[allow(dead_code)]
    fn finish() {
        // nothing to do
    }

    /// Writes all of our bytes to the target {@link DataOutput}.
    #[allow(dead_code)]
    fn write_to(&self, output: &mut DataOutput) -> Result<()> {
        let length = self.blocks.len();
        for i in 0..length {
            output.write_bytes(self.blocks[i].as_slice(), 0, self.blocks[i].len())?;
        }
        Ok(())
    }
}

pub struct StoreBytesReader {
    blocks: BlockRef,
    block_size: usize,
    block_bits: usize,
    block_mask: usize,
    block_index: usize,
    next_read: usize,
    pub reversed: bool,
}

impl StoreBytesReader {
    fn new(bytes_store: &BytesStore, reversed: bool) -> StoreBytesReader {
        StoreBytesReader {
            blocks: bytes_store.blocks.clone(),
            block_size: bytes_store.block_size,
            block_bits: bytes_store.block_bits,
            block_mask: bytes_store.block_mask,
            block_index: 0,
            next_read: 0,
            reversed,
        }
    }
}

impl BytesReader for StoreBytesReader {
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

    fn read_byte(&mut self) -> Result<u8> {
        let b = unsafe {
            *(*self.blocks.as_ptr().offset(self.block_index as isize))
                .as_ptr()
                .offset(self.next_read as isize)
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
        for i in 0..len {
            b[offset + i] = self.read_byte()?;
        }

        Ok(())
    }
}

impl Read for StoreBytesReader {
    fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
        let left = b.len();

        for i in b.iter_mut().take(left) {
            if let Ok(byte) = self.read_byte() {
                *i = byte;
            }
        }

        Ok(b.len() - left)
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
