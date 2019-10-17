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

use core::store::io::{DataInput, DataOutput, IndexInput};
use core::util::BytesRef;

use error::{
    ErrorKind::{IllegalArgument, IllegalState},
    Result,
};

use std::io::{self, Read, Write};

pub struct PagedBytes {
    block_size: usize,
    block_bits: usize,
    block_mask: usize,
    up_to: usize,
    num_blocks: usize,
    blocks: Vec<Vec<u8>>,
    current_block_created: bool,
    current_block: Vec<u8>,
    pub frozen: bool,
}

impl PagedBytes {
    pub fn new(block_bits: usize) -> PagedBytes {
        debug_assert!(block_bits > 0 && block_bits <= 31);
        let block_size = 1 << block_bits;

        PagedBytes {
            block_size,
            block_bits,
            block_mask: block_size - 1,
            up_to: block_size,
            num_blocks: 0,
            blocks: vec![],
            current_block_created: false,
            current_block: vec![],
            frozen: false,
        }
    }

    fn add_block(&mut self) {
        if self.current_block_created {
            self.blocks.push(self.current_block.clone());
            self.num_blocks += 1;
        }

        self.current_block = vec![0u8; self.block_size];
        self.current_block_created = true;
        self.up_to = 0;
    }

    pub fn copy(&mut self, input: &mut dyn IndexInput, byte_cnt: i64) -> Result<()> {
        let mut byte_cnt = byte_cnt as usize;
        while byte_cnt > 0 {
            let mut left = self.block_size - self.up_to;
            if left == 0 {
                self.add_block();
                left = self.block_size;
            }

            if left < byte_cnt {
                input.read_bytes(&mut self.current_block, self.up_to, left)?;
                self.up_to = self.block_size;
                byte_cnt -= left;
            } else {
                input.read_bytes(&mut self.current_block, self.up_to, byte_cnt)?;
                self.up_to += byte_cnt;
                break;
            }
        }
        Ok(())
    }

    pub fn freeze(&mut self, trim: bool) -> Result<()> {
        if self.frozen {
            bail!(IllegalState("already frozen".into()));
        }

        if trim && self.up_to < self.block_size {
            self.current_block.truncate(self.up_to);
        }

        if !self.current_block_created {
            self.current_block = vec![];
            self.current_block_created = true;
        }

        self.blocks.push(self.current_block.clone());
        self.num_blocks += 1;
        self.frozen = true;
        self.current_block = vec![];
        Ok(())
    }

    /// Copy bytes in, writing the length as a 1 or 2 byte
    /// vInt prefix. */
    /// TODO: this really needs to be refactored into fieldcacheimpl!
    pub fn copy_using_length_prefix(&mut self, bytes: &BytesRef) -> Result<i64> {
        if bytes.len() >= 32768 {
            bail!(IllegalArgument(format!(
                "max length is 32767 (got {})",
                bytes.len()
            )));
        }

        if self.up_to + bytes.len() + 2 > self.block_size {
            if bytes.len() + 2 > self.block_size {
                bail!(IllegalArgument(format!(
                    "block size {} is too small to store length {} bytes",
                    self.block_size,
                    bytes.len()
                )));
            }

            self.add_block();
        }

        let pointer = self.get_pointer();

        if bytes.len() < 128 {
            self.current_block[self.up_to] = bytes.len() as u8;
            self.up_to += 1;
        } else {
            self.current_block[self.up_to] = (0x80 | (bytes.len() >> 8)) as u8;
            self.up_to += 1;
            self.current_block[self.up_to] = (bytes.len() & 0xFF) as u8;
            self.up_to += 1;
        }

        self.current_block[self.up_to..self.up_to + bytes.len()].copy_from_slice(bytes.bytes());
        self.up_to += bytes.len();

        Ok(pointer)
    }

    pub fn get_pointer(&self) -> i64 {
        if !self.current_block_created {
            0
        } else {
            (self.num_blocks * self.block_size + self.up_to) as i64
        }
    }

    pub fn get_input(&self) -> Result<PagedBytesDataInput> {
        if !self.frozen {
            bail!(IllegalState(
                "must call freeze() before getDataInput".into()
            ));
        }

        Ok(PagedBytesDataInput::new(self))
    }

    pub fn get_output(&mut self) -> Result<PagedBytesDataOutput> {
        if self.frozen {
            bail!(IllegalState(
                "must call freeze() before getDataInput".into()
            ));
        }

        Ok(PagedBytesDataOutput::new(self))
    }
}

pub struct PagedBytesDataInput {
    current_block_index: usize,
    current_block_up_to: usize,
    paged_bytes: *const PagedBytes,
}

impl PagedBytesDataInput {
    pub fn new(paged_bytes: &PagedBytes) -> PagedBytesDataInput {
        PagedBytesDataInput {
            current_block_index: 0,
            current_block_up_to: 0,
            paged_bytes: paged_bytes as *const PagedBytes,
        }
    }

    fn next_block(&mut self) {
        self.current_block_index += 1;
        self.current_block_up_to = 0;
    }
}

impl DataInput for PagedBytesDataInput {
    fn read_bytes(&mut self, b: &mut [u8], offset: usize, length: usize) -> Result<()> {
        debug_assert!(b.len() >= offset + length);
        let offset_end = offset + length;
        let paged_bytes = unsafe { &(*self.paged_bytes) };
        let mut offset = offset;

        loop {
            let block_left = paged_bytes.block_size - self.current_block_up_to;
            let left = offset_end - offset;
            if block_left < left {
                b[offset..offset + block_left].copy_from_slice(
                    &paged_bytes.blocks[self.current_block_index]
                        [self.current_block_up_to..self.current_block_up_to + block_left],
                );
                self.next_block();
                offset += block_left;
            } else {
                // Last block
                b[offset..offset + left].copy_from_slice(
                    &paged_bytes.blocks[self.current_block_index]
                        [self.current_block_up_to..self.current_block_up_to + left],
                );
                self.current_block_up_to += left;
                break;
            }
        }

        Ok(())
    }
}

impl Read for PagedBytesDataInput {
    fn read(&mut self, buf: &mut [u8]) -> ::std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        let paged_bytes = unsafe { &(*self.paged_bytes) };
        if self.current_block_up_to == paged_bytes.block_size {
            self.next_block();
        }

        buf[0] = paged_bytes.blocks[self.current_block_index][self.current_block_up_to];
        self.current_block_up_to += 1;

        Ok(1)
    }
}

pub struct PagedBytesDataOutput {
    paged_bytes: *mut PagedBytes,
}

impl PagedBytesDataOutput {
    pub fn new(paged_bytes: &mut PagedBytes) -> PagedBytesDataOutput {
        PagedBytesDataOutput { paged_bytes }
    }

    pub fn get_position(&self) -> i64 {
        let paged_bytes = unsafe { &(*self.paged_bytes) };
        paged_bytes.get_pointer()
    }
}

impl DataOutput for PagedBytesDataOutput {
    fn write_byte(&mut self, b: u8) -> Result<()> {
        let paged_bytes = unsafe { &mut (*self.paged_bytes) };
        if paged_bytes.up_to == paged_bytes.block_size {
            paged_bytes.add_block();
        }

        paged_bytes.current_block[paged_bytes.up_to] = b;
        paged_bytes.up_to += 1;

        Ok(())
    }

    fn write_bytes(&mut self, b: &[u8], offset: usize, length: usize) -> Result<()> {
        debug_assert!(b.len() >= offset + length);
        if length == 0 {
            return Ok(());
        }

        let paged_bytes = unsafe { &mut (*self.paged_bytes) };
        if paged_bytes.up_to == paged_bytes.block_size {
            paged_bytes.add_block();
        }

        let mut offset = offset;
        let offset_end = offset + length;
        loop {
            let left = offset_end - offset;
            let block_left = paged_bytes.block_size - paged_bytes.up_to;
            if block_left < left {
                paged_bytes.current_block[paged_bytes.up_to..paged_bytes.up_to + block_left]
                    .copy_from_slice(&b[offset..offset + block_left]);
                paged_bytes.add_block();
                offset += block_left;
            } else {
                // last block
                paged_bytes.current_block[paged_bytes.up_to..paged_bytes.up_to + left]
                    .copy_from_slice(&b[offset..offset + left]);
                paged_bytes.up_to += left;
                break;
            }
        }

        Ok(())
    }
}

impl Write for PagedBytesDataOutput {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        let _ = self.write_byte(buf[0]);

        Ok(1)
    }

    fn flush(&mut self) -> io::Result<()> {
        unimplemented!()
    }
}

// Original Reader
pub struct PagedBytesReader {
    #[allow(dead_code)]
    block_size: usize,
    block_bits: usize,
    block_mask: usize,
    blocks: Vec<Vec<u8>>,
}

impl PagedBytesReader {
    pub fn new(paged_bytes: PagedBytes) -> Self {
        let blocks = paged_bytes.blocks;
        let block_bits = paged_bytes.block_bits;
        let block_size = paged_bytes.block_size;
        let block_mask = paged_bytes.block_mask;

        PagedBytesReader {
            block_size,
            block_mask,
            block_bits,
            blocks,
        }
    }

    pub fn fill(&self, start: i64) -> Vec<u8> {
        let index = (start >> self.block_bits) as usize;
        let offset = (start & self.block_mask as i64) as usize;
        let block = &self.blocks[index];
        if block[offset] & 128 == 0 {
            let length = block[offset] as usize;
            block[offset + 1..offset + 1 + length].to_vec()
        } else {
            let length = ((block[offset] as usize & 0x7f) << 8) | (block[offset + 1] as usize);
            let end = offset + 2 + length;
            block[offset + 2..end].to_vec()
        }
    }
}
