use error::*;

use core::store::IndexOutput;
use core::util::bkd::PointType;
use core::util::bkd::{PointReader, PointWriter};
use core::util::DocId;

pub struct HeapPointReader {
    point_writer: *const HeapPointWriter,
    curr_read: usize,
    end: usize,
}

impl HeapPointReader {
    pub fn new(point_writer: &HeapPointWriter, start: usize, end: usize) -> HeapPointReader {
        HeapPointReader {
            point_writer: point_writer as *const HeapPointWriter,
            curr_read: start - 1,
            end,
        }
    }

    fn point_writer(&self) -> &HeapPointWriter {
        unsafe { &(*self.point_writer) }
    }
}

impl PointReader for HeapPointReader {
    fn next(&mut self) -> Result<bool> {
        self.curr_read += 1;
        Ok(self.curr_read < self.end)
    }

    fn packed_value(&self) -> Vec<u8> {
        let block = self.curr_read / self.point_writer().values_per_block;
        let block_index = self.curr_read % self.point_writer().values_per_block;

        let bytes_len = self.point_writer().packed_bytes_length;
        let mut scratch = vec![0u8; bytes_len];
        let src_pos = block_index * bytes_len;
        scratch.copy_from_slice(&self.point_writer().blocks[block][src_pos..src_pos + bytes_len]);

        scratch
    }

    fn ord(&self) -> i64 {
        if self.point_writer().single_value_per_doc {
            self.point_writer().doc_ids[self.curr_read] as i64
        } else if self.point_writer().ords_long.capacity() > 0 {
            self.point_writer().ords_long[self.curr_read]
        } else {
            debug_assert!(self.point_writer().ords.capacity() > 0);
            self.point_writer().ords[self.curr_read] as i64
        }
    }

    fn doc_id(&self) -> DocId {
        self.point_writer().doc_ids[self.curr_read]
    }
}

pub struct HeapPointWriter {
    pub doc_ids: Vec<DocId>,
    pub ords_long: Vec<i64>,
    pub ords: Vec<i32>,
    pub next_write: usize,
    pub max_size: usize,
    pub values_per_block: usize,
    pub packed_bytes_length: usize,
    pub single_value_per_doc: bool,
    pub blocks: Vec<Vec<u8>>,
    pub closed: bool,
    pub shared_reader: Option<Box<PointReader>>,
}

impl HeapPointWriter {
    pub fn new(
        init_size: usize,
        max_size: usize,
        packed_bytes_length: usize,
        long_ords: bool,
        single_value_per_doc: bool,
    ) -> HeapPointWriter {
        let mut ords_long = vec![];
        let mut ords = vec![];
        if !single_value_per_doc {
            if long_ords {
                ords_long = vec![0i64; init_size];
            } else {
                ords = vec![0i32; init_size];
            }
        }

        HeapPointWriter {
            doc_ids: vec![0i32; init_size],
            ords_long,
            ords,
            next_write: 0,
            max_size,
            values_per_block: 1.max(4096 / packed_bytes_length),
            packed_bytes_length,
            single_value_per_doc,
            blocks: vec![],
            closed: false,
            shared_reader: None,
        }
    }

    pub fn copy_from(&mut self, other: &HeapPointWriter) -> Result<()> {
        if self.doc_ids.len() < other.next_write {
            bail!(
                "doc_ids.len={}, other.next_write={}",
                self.doc_ids.capacity(),
                other.next_write
            );
        }

        self.doc_ids[0..other.next_write].copy_from_slice(&other.doc_ids[0..other.next_write]);
        if !self.single_value_per_doc {
            if other.ords.len() > 0 {
                debug_assert!(self.ords.len() > 0);
                self.ords[0..other.next_write].copy_from_slice(&other.ords[0..other.next_write]);
            } else {
                debug_assert!(self.ords_long.len() > 0);
                self.ords_long[0..other.next_write]
                    .copy_from_slice(&other.ords_long[0..other.next_write]);
            }
        }

        for block in &other.blocks {
            self.blocks.push(block.clone());
        }

        self.next_write = other.next_write;

        Ok(())
    }

    pub fn read_packed_value(&self, index: usize, bytes: &mut [u8]) {
        debug_assert!(bytes.len() == self.packed_bytes_length);
        let block = index / self.values_per_block;
        let block_index = index % self.values_per_block;

        let src_pos = block_index * self.packed_bytes_length;
        bytes[0..self.packed_bytes_length]
            .copy_from_slice(&self.blocks[block][src_pos..src_pos + self.packed_bytes_length]);
    }

    pub fn packed_value_slice(&self, index: usize, bytes: &mut [u8]) {
        debug_assert_eq!(bytes.len(), self.packed_bytes_length);
        let block = index / self.values_per_block;
        let block_index = index % self.values_per_block;

        let offset = block_index * self.packed_bytes_length;
        bytes.copy_from_slice(&self.blocks[block][offset..offset + self.packed_bytes_length]);
    }

    pub fn write_packed_value(&mut self, index: usize, bytes: &[u8]) {
        debug_assert!(bytes.len() == self.packed_bytes_length);
        let block = index / self.values_per_block;
        let block_index = index % self.values_per_block;

        let mut block_len = self.blocks.len();
        while block_len <= block {
            // If this is the last block, only allocate as large as necessary for maxSize:
            let values_in_block = self.values_per_block
                .min(self.max_size - block_len * self.values_per_block);
            self.blocks
                .push(vec![0u8; values_in_block * self.packed_bytes_length]);
            block_len += 1;
        }

        let src_pos = block_index * self.packed_bytes_length;
        self.blocks[block][src_pos..src_pos + self.packed_bytes_length]
            .copy_from_slice(&bytes[0..self.packed_bytes_length]);
    }
}

impl PointWriter for HeapPointWriter {
    fn append(&mut self, packed_value: &[u8], ord: i64, doc_id: DocId) -> Result<()> {
        debug_assert!(!self.closed);
        debug_assert_eq!(packed_value.len(), self.packed_bytes_length);
        let next_write = self.next_write;
        self.write_packed_value(next_write, packed_value);

        if !self.single_value_per_doc {
            if self.ords_long.capacity() > 0 {
                self.ords_long[self.next_write] = ord;
            } else {
                self.ords[self.next_write] = ord as i32;
            }
        }

        self.doc_ids[self.next_write] = doc_id;
        self.next_write += 1;

        Ok(())
    }

    fn point_type(&self) -> PointType {
        PointType::Heap
    }

    fn destory(&mut self) -> Result<()> {
        Ok(())
    }

    fn index_output(&mut self) -> &mut IndexOutput {
        unimplemented!()
    }
    fn set_count(&mut self, _count: i64) {
        unimplemented!()
    }
    fn close(&mut self) -> Result<()> {
        self.closed = true;
        Ok(())
    }

    fn point_reader(&self, start: usize, length: usize) -> Result<Box<PointReader>> {
        debug_assert!(start + length <= self.doc_ids.len());
        debug_assert!(start + length <= self.next_write);

        Ok(Box::new(HeapPointReader::new(self, start, length)))
    }

    fn shared_point_reader(
        &mut self,
        start: usize,
        _length: usize,
        _to_close_heroically: &mut Vec<Box<PointReader>>,
    ) -> Result<&mut PointReader> {
        self.shared_reader = Some(Box::new(HeapPointReader::new(self, start, self.next_write)));

        Ok(self.shared_reader.as_mut().unwrap().as_mut())
    }

    fn try_as_heap_writer(&mut self) -> &mut HeapPointWriter {
        debug_assert!(self.point_type() == PointType::Heap);
        self
    }

    fn clone(&self) -> Box<PointWriter> {
        Box::new(HeapPointWriter {
            doc_ids: self.doc_ids.clone(),
            ords_long: self.ords_long.clone(),
            ords: self.ords.clone(),
            next_write: self.next_write,
            max_size: self.max_size,
            values_per_block: self.values_per_block,
            packed_bytes_length: self.packed_bytes_length,
            single_value_per_doc: self.single_value_per_doc,
            blocks: self.blocks.clone(),
            closed: self.closed,
            shared_reader: None,
        })
    }
}
