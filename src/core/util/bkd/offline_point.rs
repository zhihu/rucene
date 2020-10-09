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

use core::codec::{footer_length, write_footer, INT_BYTES, LONG_BYTES};
use core::store::directory::Directory;
use core::store::io::{DataOutput, IndexInput, IndexOutput, IndexOutputRef};
use core::store::IOContext;
use core::util::bkd::{LongBitSet, PointReader, PointReaderEnum, PointType, PointWriter};
use core::util::DocId;

use error::{Error, ErrorKind::UnexpectedEOF, Result};
use std::io::Read;
use std::sync::Arc;

pub struct OfflinePointReader {
    count_left: i64,
    input: Box<dyn IndexInput>,
    packed_value: Vec<u8>,
    single_value_per_doc: bool,
    bytes_per_doc: i32,
    ord: i64,
    doc_id: DocId,
    // true if ords are written as long (8 bytes), else 4 bytes
    long_ords: bool,
    _checked: bool,
    pub is_checksum: bool,
    _temp_file_name: String,
}

impl OfflinePointReader {
    pub fn new<D: Directory>(
        temp_dir: &D,
        temp_file_name: &str,
        packed_bytes_length: i32,
        start: usize,
        length: usize,
        long_ords: bool,
        single_value_per_doc: bool,
    ) -> Result<OfflinePointReader> {
        let mut bytes_per_doc = packed_bytes_length + INT_BYTES;
        if !single_value_per_doc {
            if long_ords {
                bytes_per_doc += LONG_BYTES;
            } else {
                bytes_per_doc += INT_BYTES;
            }
        }

        let footer_length = footer_length();
        let file_length = temp_dir.file_length(temp_file_name)?;

        if (start + length) * (bytes_per_doc as usize) + footer_length > file_length as usize {
            bail!(
                "requested slice is beyond the length of this file: start={} length={} \
                 bytes_per_doc={} fileLength={} tempFileName={}",
                start,
                length,
                bytes_per_doc,
                file_length,
                temp_file_name
            );
        }

        let mut input = temp_dir.open_input(temp_file_name, &IOContext::READ_ONCE)?;

        //        let mut is_checksum = false;
        //        // Best-effort checksumming:
        //        let mut input = if start == 0
        //            && length * bytes_per_doc as usize == file_length as usize - footer_length
        //        {
        //            // If we are going to read the entire file, e.g. because BKDWriter is now
        //            // partitioning it, we open with checksums:
        //
        //            // is_checksum = true;
        //            temp_dir.open_checksum_input(temp_file_name, IOContext::READ_ONCE)?
        //
        //        } else {
        //            // Since we are going to seek somewhere in the middle of a possibly huge
        //            // file, and not read all bytes from there, don't use ChecksumIndexInput here.
        //            // This is typically fine, because this same file will later be read fully,
        //            // at another level of the BKDWriter recursion
        //            temp_dir.open_input(temp_file_name, &IOContext::READ_ONCE)?
        //        };

        let seek_fp = start as i64 * bytes_per_doc as i64;
        input.as_mut().seek(seek_fp)?;

        Ok(OfflinePointReader {
            count_left: length as i64,
            input,
            packed_value: vec![0u8; packed_bytes_length as usize],
            single_value_per_doc,
            bytes_per_doc,
            ord: 0,
            doc_id: 0,
            long_ords,
            _checked: false,
            is_checksum: false,
            _temp_file_name: temp_file_name.to_string(),
        })
    }

    fn read_long(bytes: &[u8], pos: usize) -> i64 {
        let mut pos = pos;
        let i1p1 = bytes[pos] as u32 as i32;
        pos += 1;
        let i1p2 = bytes[pos] as u32 as i32;
        pos += 1;
        let i1p3 = bytes[pos] as u32 as i32;
        pos += 1;
        let i1p4 = bytes[pos] as u32 as i32;
        pos += 1;
        let i1 = i1p1 << 24 | i1p2 << 16 | i1p3 << 8 | i1p4;

        let i2p1 = bytes[pos] as u32 as i32;
        pos += 1;
        let i2p2 = bytes[pos] as u32 as i32;
        pos += 1;
        let i2p3 = bytes[pos] as u32 as i32;
        pos += 1;
        let i2p4 = bytes[pos] as u32 as i32;
        let i2 = i2p1 << 24 | i2p2 << 16 | i2p3 << 8 | i2p4;

        ((i1 as i64) << 32) | ((i2 as i64) & 0xFFFF_FFFFi64)
    }

    fn read_int(bytes: &[u8], pos: usize) -> i32 {
        let mut pos = pos;
        let i1p1 = bytes[pos] as u32 as i32;
        pos += 1;
        let i1p2 = bytes[pos] as u32 as i32;
        pos += 1;
        let i1p3 = bytes[pos] as u32 as i32;
        pos += 1;
        let i1p4 = bytes[pos] as u32 as i32;

        i1p1 << 24 | i1p2 << 16 | i1p3 << 8 | i1p4
    }
}

impl PointReader for OfflinePointReader {
    fn next(&mut self) -> Result<bool> {
        if self.count_left >= 0 {
            if self.count_left == 0 {
                return Ok(false);
            }
            self.count_left -= 1;
        }

        let len = self.packed_value.len();
        match self.input.read_bytes(&mut self.packed_value, 0, len) {
            Err(Error(UnexpectedEOF(_), _)) => {
                debug_assert_eq!(self.count_left, -1);
                return Ok(false);
            }
            Err(e) => {
                return Err(e);
            }
            _ => {}
        }

        self.doc_id = self.input.read_int()?;
        if self.single_value_per_doc {
            if self.long_ords {
                self.ord = self.input.read_long()?;
            } else {
                self.ord = self.input.read_int()? as i64;
            }
        } else {
            self.ord = self.doc_id as i64;
        }

        Ok(true)
    }

    fn packed_value(&self) -> &[u8] {
        &self.packed_value
    }

    fn ord(&self) -> i64 {
        self.ord
    }

    fn doc_id(&self) -> DocId {
        self.doc_id
    }

    fn mark_ords(&mut self, count: i64, ord_bit_set: &mut LongBitSet) -> Result<()> {
        if self.count_left < count {
            bail!(
                "only {} points remain, but {} were requested.",
                self.count_left,
                count
            );
        }

        let mut fp = self.input.file_pointer() + self.packed_value.capacity() as i64;
        if !self.single_value_per_doc {
            fp += INT_BYTES as i64;
        }

        for _ in 0..count {
            self.input.seek(fp)?;
            let ord = if self.long_ords {
                self.input.read_long()?
            } else {
                self.input.read_int()? as i64
            };

            debug_assert!(!ord_bit_set.get(ord));
            ord_bit_set.set(ord);
            fp += self.bytes_per_doc as i64;
        }

        Ok(())
    }

    fn split(
        &mut self,
        count: i64,
        right_tree: &mut LongBitSet,
        left: &mut impl PointWriter,
        right: &mut impl PointWriter,
        do_clear_bits: bool,
    ) -> Result<i64> {
        if left.point_type() != PointType::Offline || right.point_type() != PointType::Offline {
            return PointReader::split(self, count, right_tree, left, right, do_clear_bits);
        }

        // We specialize the offline -> offline split since the default impl
        // is somewhat wasteful otherwise (e.g. decoding docID when we don't
        // need to)
        let packed_bytes_length = self.packed_value.capacity();
        let mut bytes_per_doc = packed_bytes_length + INT_BYTES as usize;
        if self.single_value_per_doc {
            if self.long_ords {
                bytes_per_doc += LONG_BYTES as usize;
            } else {
                bytes_per_doc += INT_BYTES as usize;
            }
        }

        let mut count_start = count;
        let mut right_count = 0;

        {
            let mut right_out = right.index_output();
            let mut left_out = left.index_output();

            debug_assert!(count <= self.count_left);
            self.count_left -= count;
            let mut buffer = vec![0u8; bytes_per_doc];
            while count_start > 0 {
                self.input.read_exact(&mut buffer)?;
                let ord = if self.long_ords {
                    OfflinePointReader::read_long(&buffer, packed_bytes_length + INT_BYTES as usize)
                } else if self.single_value_per_doc {
                    OfflinePointReader::read_int(&buffer, packed_bytes_length) as i64
                } else {
                    OfflinePointReader::read_int(&buffer, packed_bytes_length + INT_BYTES as usize)
                        as i64
                };

                if right_tree.get(ord) {
                    right_out.write_bytes(&buffer, 0, bytes_per_doc)?;
                    if do_clear_bits {
                        right_tree.clear(ord);
                    }

                    right_count += 1;
                } else {
                    left_out.write_bytes(&buffer, 0, bytes_per_doc)?;
                }

                count_start -= 1;
            }
        }

        right.set_count(right_count as i64);
        left.set_count((count_start - right_count) as i64);

        Ok(right_count)
    }
}

pub struct OfflinePointWriter<D: Directory> {
    temp_dir: Arc<D>,
    output: Option<D::TempOutput>,
    name: String,
    packed_bytes_length: usize,
    single_value_per_doc: bool,
    count: i64,
    long_ords: bool,
    shared_reader: Option<PointReaderEnum>,
    next_shared_read: i64,
    expected_count: i64,
    closed: bool,
    temp_file_name_prefix: String,
    desc: String,
}

impl<D: Directory> OfflinePointWriter<D> {
    #[allow(dead_code)]
    pub fn new(
        temp_dir: Arc<D>,
        name: String,
        packed_bytes_length: usize,
        count: i64,
        long_ords: bool,
        single_value_per_doc: bool,
    ) -> OfflinePointWriter<D> {
        OfflinePointWriter {
            temp_dir,
            output: None,
            name,
            packed_bytes_length,
            single_value_per_doc,
            count,
            long_ords,
            shared_reader: None,
            next_shared_read: 0,
            expected_count: 0,
            closed: false,
            temp_file_name_prefix: "".to_string(),
            desc: "".to_string(),
        }
    }

    // only used for BKDWriter
    pub fn output(&self) -> &D::TempOutput {
        debug_assert!(self.output.is_some());
        self.output.as_ref().unwrap()
    }

    pub fn prefix_new(
        temp_dir: Arc<D>,
        temp_file_name_prefix: &str,
        packed_bytes_length: usize,
        long_ords: bool,
        desc: &str,
        expected_count: i64,
        single_value_per_doc: bool,
    ) -> OfflinePointWriter<D> {
        let output = temp_dir
            .create_temp_output(
                temp_file_name_prefix,
                &format!("bkd_{}", desc),
                &IOContext::Default,
            )
            .unwrap();

        let name = output.name().to_string();

        OfflinePointWriter {
            temp_dir,
            output: Some(output),
            name,
            packed_bytes_length,
            single_value_per_doc,
            count: 0,
            long_ords,
            shared_reader: None,
            next_shared_read: 0,
            expected_count,
            closed: true,
            temp_file_name_prefix: temp_file_name_prefix.to_string(),
            desc: desc.to_string(),
        }
    }
}

impl<D: Directory> PointWriter for OfflinePointWriter<D> {
    type IndexOutput = IndexOutputRef<D::TempOutput>;
    type PointReader = PointReaderEnum;

    fn append(&mut self, packed_value: &[u8], ord: i64, doc_id: DocId) -> Result<()> {
        debug_assert!(packed_value.len() == self.packed_bytes_length);
        let output = self.output.as_mut().unwrap();

        output.write_bytes(packed_value, 0, packed_value.len())?;
        output.write_int(doc_id)?;
        if !self.single_value_per_doc {
            if self.long_ords {
                output.write_long(ord)?;
            } else {
                debug_assert!(ord <= <i32>::max_value() as i64);
                output.write_int(ord as i32)?;
            }
        }

        self.count += 1;
        debug_assert!(self.expected_count == 0 || self.count <= self.expected_count);
        Ok(())
    }

    fn destory(&mut self) -> Result<()> {
        if self.shared_reader.is_some() {
            debug_assert!(self.next_shared_read == self.count);
            self.shared_reader = None;
        }

        self.temp_dir.delete_file(&self.name)
    }

    fn point_reader(&self, start: usize, length: usize) -> Result<Self::PointReader> {
        debug_assert!(self.closed);
        debug_assert!((start + length) as i64 <= self.count);
        debug_assert!(self.expected_count == 0 || self.count == self.expected_count);

        Ok(PointReaderEnum::Offline(OfflinePointReader::new(
            self.temp_dir.as_ref(),
            &self.name,
            self.packed_bytes_length as i32,
            start,
            length,
            self.long_ords,
            self.single_value_per_doc,
        )?))
    }

    fn shared_point_reader(
        &mut self,
        start: usize,
        length: usize,
    ) -> Result<&mut Self::PointReader> {
        if self.shared_reader.is_none() {
            debug_assert!(start == 0 && length as i64 <= self.count);
            let shared_reader = PointReaderEnum::Offline(OfflinePointReader::new(
                self.temp_dir.as_ref(),
                &self.name,
                self.packed_bytes_length as i32,
                0,
                self.count as usize,
                self.long_ords,
                self.single_value_per_doc,
            )?);

            //_to_close.push(shared_reader);
            self.shared_reader = Some(shared_reader);

        // debug_assert!(self.shared_reader.as_ref().unwrap().as_ref().is_checksum);
        } else {
            debug_assert!(start as i64 == self.next_shared_read);
        }

        self.next_shared_read += length as i64;

        Ok(self.shared_reader.as_mut().unwrap())
    }

    fn point_type(&self) -> PointType {
        PointType::Offline
    }

    fn index_output(&mut self) -> Self::IndexOutput {
        debug_assert!(self.output.is_some());
        IndexOutputRef::new(self.output.as_mut().unwrap())
    }

    fn set_count(&mut self, count: i64) {
        self.count = count;
    }

    fn close(&mut self) -> Result<()> {
        if !self.closed {
            debug_assert!(self.shared_reader.is_none());
            let output = self.output.as_mut().unwrap();
            write_footer(output)?;
            self.closed = true;
        }

        Ok(())
    }

    fn clone(&self) -> Self {
        let temp_dir = Arc::clone(&self.temp_dir);
        let output = temp_dir
            .create_temp_output(
                &self.temp_file_name_prefix,
                &format!("bkd_{}", self.desc),
                &IOContext::Default,
            )
            .unwrap();

        let name = output.name().to_string();

        OfflinePointWriter {
            temp_dir,
            output: Some(output),
            name,
            packed_bytes_length: self.packed_bytes_length,
            single_value_per_doc: self.single_value_per_doc,
            count: self.count,
            long_ords: self.long_ords,
            shared_reader: None,
            next_shared_read: self.next_shared_read,
            expected_count: self.expected_count,
            closed: self.closed,
            temp_file_name_prefix: self.temp_file_name_prefix.clone(),
            desc: self.desc.clone(),
        }
    }
}
