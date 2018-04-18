use core::store::IndexInput;
use error::Result;

pub struct PagedBytes {
    block_size: u32,
    block_bits: u32,
    block_mask: u32,
    upto: u32,
    blocks: Vec<Vec<u8>>,
    current_block: Vec<u8>,
}

impl PagedBytes {
    pub fn new(block_bits: usize) -> Self {
        assert!(block_bits != 0 && block_bits <= 31);
        let block_bits = block_bits as u32;
        let block_size = 1 << block_bits;
        let block_mask = block_size - 1;

        PagedBytes {
            block_size,
            block_bits,
            block_mask,
            upto: block_size,
            blocks: Vec::new(),
            current_block: Vec::new(),
        }
    }
}

impl PagedBytes {
    pub fn copy(&mut self, input: &mut IndexInput, mut byte_cnt: i64) -> Result<()> {
        let block_size = i64::from(self.block_size);
        while byte_cnt > 0 {
            let mut left = block_size - i64::from(self.upto);
            if left == 0 {
                let mut new_block = vec![0u8; block_size as usize];
                ::std::mem::swap(&mut self.current_block, &mut new_block);
                if !new_block.is_empty() {
                    self.blocks.push(new_block);
                }
                self.upto = 0;
                left = block_size;
            }

            if left < byte_cnt {
                input.read_bytes(&mut self.current_block, self.upto as usize, left as usize)?;
                self.upto = self.block_size;
                byte_cnt -= left;
            } else {
                input.read_bytes(
                    &mut self.current_block,
                    self.upto as usize,
                    byte_cnt as usize,
                )?;
                self.upto += byte_cnt as u32;
                break;
            }
        }
        Ok(())
    }

    pub fn freeze(&mut self, trim: bool) {
        if trim && self.upto < self.block_size {
            self.current_block.truncate(self.upto as usize);
        }
        if !self.current_block.is_empty() {
            let mut new_block = vec![];
            ::std::mem::swap(&mut new_block, &mut self.current_block);
            self.blocks.push(new_block);
        }
    }
}

// Original Reader
pub struct PagedBytesReader {
    #[allow(dead_code)]
    block_size: u32,
    block_bits: u32,
    block_mask: u32,
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
        let offset = (start & i64::from(self.block_mask)) as usize;
        let block = &self.blocks[index][..];
        if block[offset] & 128 == 0 {
            let length = block[offset] as usize;
            block[offset + 1..offset + 1 + length].as_ref().to_vec()
        } else {
            let length =
                ((block[offset] as usize & 0x7f) << 8) | (block[offset + 1] as usize & 0xff);
            let end = offset + 2 + length;
            block[offset + 2..end].as_ref().to_vec()
        }
    }
}
