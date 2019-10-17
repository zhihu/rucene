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

use std::io;
use std::io::Read;

use core::codec::postings::posting_format::BLOCK_SIZE;
use core::store::io::{DataInput, IndexInput, RandomAccessInput};
use core::util::{log, DocId};

use error::Result;

/// used to buffer the top skip levels
struct SkipBuffer {
    data: Vec<u8>,
    pointer: i64,
    pos: i32,
}

impl SkipBuffer {
    fn new(input: &mut dyn IndexInput, length: i32) -> Result<SkipBuffer> {
        let mut data = vec![0; length as usize];
        let pointer = input.file_pointer();
        input.read_exact(&mut data)?;
        Ok(SkipBuffer {
            data,
            pointer,
            pos: 0,
        })
    }
}

impl DataInput for SkipBuffer {}

impl IndexInput for SkipBuffer {
    fn clone(&self) -> Result<Box<dyn IndexInput>> {
        Ok(Box::new(SkipBuffer {
            data: self.data.clone(),
            pointer: self.pointer,
            pos: self.pos,
        }))
    }

    fn file_pointer(&self) -> i64 {
        self.pointer + i64::from(self.pos)
    }

    fn len(&self) -> u64 {
        self.data.len() as u64
    }

    fn seek(&mut self, pos: i64) -> Result<()> {
        self.pos = (pos - self.pointer) as i32;
        Ok(())
    }

    fn name(&self) -> &str {
        "SkipBuffer"
    }

    fn random_access_slice(
        &self,
        _offset: i64,
        _length: i64,
    ) -> Result<Box<dyn RandomAccessInput>> {
        unimplemented!()
    }
}

impl Read for SkipBuffer {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let pos = self.pos as usize;
        let mut len = buffer.len();
        let available = self.data.len() - pos;
        if len > available {
            len = available;
        }
        buffer[0..len].clone_from_slice(&self.data[pos..pos + len]);
        self.pos += len as i32;
        Ok(len)
    }
}

/// Implements the skip list reader for block postings format
/// that stores positions and payloads.
///
/// Although this skipper uses MultiLevelSkipListReader as an interface,
/// its definition of skip position will be a little different.
///
/// For example, when skipInterval = blockSize = 3, df = 2*skipInterval = 6,
///
/// 0 1 2 3 4 5
/// d d d d d d    (posting list)
/// ^     ^    (skip point in MultiLeveSkipWriter)
/// ^        (skip point in Lucene50SkipWriter)
///
/// In this case, MultiLevelSkipListReader will use the last document as a skip point,
/// while Lucene50SkipReader should assume no skip point will comes.
///
/// If we use the interface directly in Lucene50SkipReader, it may silly try to read
/// another skip data after the only skip point is loaded.
///
/// To illustrate this, we can call skip_to(d\[5\]), since skip point d\[3\] has smaller docId,
/// and numSkipped+blockSize== df, the MultiLevelSkipListReader will assume the skip list
/// isn't exhausted yet, and try to load a non-existed skip point
///
/// Therefore, we'll trim df before passing it to the interface. see trim(int)
pub struct Lucene50SkipReader {
    // fields from MultiLevelSkipListReader in lucene java implementation
    /// the maximum number of skip levels possible for this index
    max_number_of_skip_levels: i32,

    /// number of levels in this skip list
    number_of_skip_levels: i32,

    /// Expert: defines the number of top skip levels to buffer in memory.
    /// Reducing this number results in less memory usage, but possibly
    /// slower performance due to more random I/Os.
    /// Please notice that the space each level occupies is limited by
    /// the skipInterval. The top level can not contain more than
    /// skipLevel entries, the second top level can not contain more
    /// than skipLevel^2 entries and so forth.
    number_of_levels_to_buffer: i32,

    doc_count: i32,

    /// skip_stream for each level.
    skip_stream: Vec<Option<Box<dyn IndexInput>>>,

    /// The start pointer of each skip level.
    skip_pointer: Vec<i64>,

    /// skipInterval of each level.
    skip_interval: Vec<i64>,

    /// Number of docs skipped per level.
    /// It's possible for some values to overflow a signed int, but this has been accounted for.
    num_skipped: Vec<i64>,

    /// Doc id of current skip entry per level.
    skip_doc: Vec<i32>,

    /// Doc id of last read skip entry with docId &lt,= target.
    last_doc: DocId,

    /// Child pointer of current skip entry per level.
    child_pointer: Vec<i64>,

    /// childPointer of last read skip entry with docId &lt,=
    /// target. */
    last_child_pointer: i64,

    input_is_buffered: bool,
    skip_multiplier: i32,

    // fields from Lucene50SkipListReader in lucene java implementation
    doc_pointer: Vec<i64>,
    pos_pointer: Option<Vec<i64>>,
    pay_pointer: Option<Vec<i64>>,
    pos_buffer_upto: Option<Vec<i32>>,
    payload_byte_upto: Option<Vec<i32>>,

    last_pos_pointer: i64,
    last_pay_pointer: i64,
    last_payload_byte_upto: i32,
    last_doc_pointer: i64,
    last_pos_buffer_upto: i32,
}

impl Lucene50SkipReader {
    pub fn clone_reader(&self) -> Result<Self> {
        let mut skip_stream = Vec::with_capacity(self.skip_stream.len());
        for s in &self.skip_stream {
            skip_stream.push(match s {
                Some(ref stream) => Some(stream.as_ref().clone()?),
                None => None,
            });
        }

        Ok(Lucene50SkipReader {
            // MultiLevelSkipListReader
            max_number_of_skip_levels: self.max_number_of_skip_levels,
            number_of_skip_levels: self.number_of_skip_levels,
            number_of_levels_to_buffer: self.number_of_levels_to_buffer,
            doc_count: self.doc_count,
            skip_stream,
            skip_pointer: self.skip_pointer.clone(),
            skip_interval: self.skip_interval.clone(),
            num_skipped: self.num_skipped.clone(),
            skip_doc: self.skip_doc.clone(),
            last_doc: self.last_doc,
            child_pointer: self.child_pointer.clone(),
            last_child_pointer: self.last_child_pointer,
            input_is_buffered: self.input_is_buffered,
            skip_multiplier: self.skip_multiplier,

            // Lucene50SkipReader
            doc_pointer: self.doc_pointer.clone(),
            pos_pointer: self.pos_pointer.clone(),
            pay_pointer: self.pay_pointer.clone(),
            pos_buffer_upto: self.pos_buffer_upto.clone(),
            payload_byte_upto: self.payload_byte_upto.clone(),

            last_pos_pointer: self.last_pos_pointer,
            last_pay_pointer: self.last_pay_pointer,
            last_payload_byte_upto: self.last_payload_byte_upto,
            last_doc_pointer: self.last_doc_pointer,
            last_pos_buffer_upto: self.last_pos_buffer_upto,
        })
    }

    pub fn new(
        skip_stream: Box<dyn IndexInput>,
        max_skip_levels: usize,
        has_pos: bool,
        has_offsets: bool,
        has_payloads: bool,
    ) -> Lucene50SkipReader {
        // fields for MultiLevelSkipReader part
        let max_number_of_skip_levels = max_skip_levels;
        let skip_pointer = vec![0 as i64; max_number_of_skip_levels];
        let child_pointer = vec![0 as i64; max_number_of_skip_levels];
        let num_skipped = vec![0 as i64; max_number_of_skip_levels];
        let skip_interval = BLOCK_SIZE;
        let skip_multiplier = 8;
        let mut skip_intervals = vec![];
        let mut skip_streams = Vec::with_capacity(max_number_of_skip_levels);
        let input_is_buffered = skip_stream.is_buffered();
        skip_streams.push(Some(skip_stream));
        skip_intervals.push(i64::from(skip_interval));
        for i in 1..max_number_of_skip_levels {
            // cache skip intervals
            let last_interval = skip_intervals[i - 1];
            skip_intervals.push(last_interval * i64::from(skip_multiplier));
            skip_streams.push(None);
        }
        let skip_doc = vec![0 as i32; max_number_of_skip_levels];
        let max_number_of_skip_levels = max_number_of_skip_levels as i32;
        let skip_stream = skip_streams;
        let skip_interval = skip_intervals;

        // fields for Lucene50SkipReader part
        let max_skip_levels = max_skip_levels;
        let doc_pointer = vec![0_i64; max_skip_levels];
        let mut pos_pointer = None;
        let mut pos_buffer_upto = None;
        let mut payload_byte_upto = None;
        let mut pay_pointer = None;
        if has_pos {
            pos_pointer = Some(vec![0_i64; max_skip_levels]);
            pos_buffer_upto = Some(vec![0_i32; max_skip_levels]);
            if has_payloads {
                payload_byte_upto = Some(vec![0_i32; max_skip_levels]);
            }
            if has_offsets || has_payloads {
                pay_pointer = Some(vec![0_i64; max_skip_levels]);
            }
        };
        Lucene50SkipReader {
            // MultiLevelSkipListReader
            max_number_of_skip_levels,
            skip_pointer,
            child_pointer,
            num_skipped,
            skip_interval,
            skip_stream,
            input_is_buffered,
            skip_doc,
            skip_multiplier,
            doc_count: 0,
            last_child_pointer: 0,
            last_doc: 0,
            number_of_levels_to_buffer: 1,
            number_of_skip_levels: 0,

            // Lucene50SkipReader
            doc_pointer,
            pos_pointer,
            pay_pointer,
            pos_buffer_upto,
            payload_byte_upto,

            last_pos_pointer: 0,
            last_pay_pointer: 0,
            last_payload_byte_upto: 0,
            last_doc_pointer: 0,
            last_pos_buffer_upto: 0,
        }
    }

    /// Trim original docFreq to tell skipReader read proper number of skip points.
    ///
    /// Since our definition in Lucene50Skip* is a little different from MultiLevelSkip*
    /// This trimmed docFreq will prevent skipReader from:
    /// 1. silly reading a non-existed skip point after the last block boundary
    /// 2. moving into the vInt block
    fn trim(df: i32) -> i32 {
        if df % BLOCK_SIZE == 0 {
            df - 1
        } else {
            df
        }
    }

    pub fn init(
        &mut self,
        skip_pointer: i64,
        doc_base_pointer: i64,
        pos_base_pointer: i64,
        pay_base_pointer: i64,
        df: i32,
    ) -> Result<()> {
        let df = Self::trim(df);
        self.skip_pointer[0] = skip_pointer;
        self.doc_count = df;
        self.skip_doc.iter_mut().map(|x| *x = 0).count();
        self.num_skipped.iter_mut().map(|x| *x = 0).count();
        self.child_pointer.iter_mut().map(|x| *x = 0).count();

        for i in 1..self.number_of_skip_levels as usize {
            self.skip_stream[i] = None;
        }
        self.load_skip_levels()?;

        self.last_doc_pointer = doc_base_pointer;
        self.last_pos_pointer = pos_base_pointer;
        self.last_pay_pointer = pay_base_pointer;

        self.doc_pointer
            .iter_mut()
            .map(|x| *x = doc_base_pointer)
            .count();
        if let Some(ref mut pos_pointer) = self.pos_pointer {
            pos_pointer
                .iter_mut()
                .map(|x| *x = pos_base_pointer)
                .count();
        }
        if let Some(ref mut pay_pointer) = self.pay_pointer {
            pay_pointer
                .iter_mut()
                .map(|x| *x = pay_base_pointer)
                .count();
        }
        Ok(())
    }

    /// Returns the doc pointer of the doc to which the last call of
    /// {@link MultiLevelSkipListReader#skipTo(int)} has skipped.  */
    pub fn doc_pointer(&self) -> i64 {
        self.last_doc_pointer
    }

    pub fn pos_pointer(&self) -> i64 {
        self.last_pos_pointer
    }

    pub fn pos_buffer_upto(&self) -> i32 {
        self.last_pos_buffer_upto
    }

    pub fn pay_pointer(&self) -> i64 {
        self.last_pay_pointer
    }

    pub fn payload_byte_upto(&self) -> i32 {
        self.last_payload_byte_upto
    }

    pub fn next_skip_doc(&self) -> i32 {
        self.skip_doc[0]
    }

    /// Seeks the skip entry on the given level
    pub fn seek_child(&mut self, level: i32) -> Result<()> {
        let ulevel = level as usize;
        let skip_stream = self.skip_stream[ulevel].as_mut().unwrap();
        skip_stream.seek(self.last_child_pointer)?;
        self.num_skipped[ulevel] = self.num_skipped[ulevel + 1] - self.skip_interval[ulevel + 1];
        self.skip_doc[ulevel] = self.last_doc;
        if ulevel > 0 {
            self.child_pointer[ulevel] = skip_stream.read_vlong()? + self.skip_pointer[ulevel - 1];
        }

        let level = level as usize;
        self.doc_pointer[level] = self.last_doc_pointer;
        if let Some(ref mut pos_pointer) = self.pos_pointer {
            pos_pointer[level] = self.last_pos_pointer;
            self.pos_buffer_upto.as_mut().unwrap()[level] = self.last_pos_buffer_upto;
            if let Some(ref mut payload_byte_upto) = self.payload_byte_upto {
                payload_byte_upto[level] = self.last_payload_byte_upto;
            }
            if let Some(ref mut pay_pointer) = self.pay_pointer {
                pay_pointer[level] = self.last_pay_pointer;
            }
        }
        Ok(())
    }

    pub fn set_last_skip_data(&mut self, level: i32) -> Result<()> {
        let level = level as usize;
        self.last_doc = self.skip_doc[level];
        self.last_child_pointer = self.child_pointer[level];

        let level = level as usize;
        self.last_doc_pointer = self.doc_pointer[level];

        if let Some(ref mut pos_pointer) = self.pos_pointer {
            self.last_pos_pointer = pos_pointer[level];
            self.last_pos_buffer_upto = self.pos_buffer_upto.as_mut().unwrap()[level];
            if let Some(ref mut pay_pointer) = self.pay_pointer {
                self.last_pay_pointer = pay_pointer[level];
            }
            if let Some(ref mut payload_byte_upto) = self.payload_byte_upto {
                self.last_payload_byte_upto = payload_byte_upto[level];
            }
        }
        Ok(())
    }

    pub fn read_skip_data(&mut self, level: usize) -> Result<i32> {
        let delta = self.stream(level)?.read_vint()?;
        let pointer = self.stream(level)?.read_vlong()?;
        self.doc_pointer[level] += pointer;

        if self.pos_pointer.is_some() {
            let pointer = self.stream(level)?.read_vlong()?;
            self.pos_pointer.as_mut().unwrap()[level] += pointer;
            let upto = self.stream(level)?.read_vint()?;
            self.pos_buffer_upto.as_mut().unwrap()[level] = upto;

            if self.payload_byte_upto.is_some() {
                let upto = self.stream(level)?.read_vint()?;
                self.payload_byte_upto.as_mut().unwrap()[level] = upto;
            }

            if self.pay_pointer.is_some() {
                let pointer = self.stream(level)?.read_vlong()?;
                self.pay_pointer.as_mut().unwrap()[level] += pointer;
            }
        }
        Ok(delta)
    }

    pub fn is_input_buffered(&self) -> bool {
        self.input_is_buffered
    }

    /// Loads the skip levels
    fn load_skip_levels(&mut self) -> Result<()> {
        if i64::from(self.doc_count) <= self.skip_interval[0] {
            self.number_of_skip_levels = 1;
        } else {
            self.number_of_skip_levels = 1 + log(
                i64::from(self.doc_count) / self.skip_interval[0],
                self.skip_multiplier,
            );
        }

        if self.number_of_skip_levels > self.max_number_of_skip_levels {
            self.number_of_skip_levels = self.max_number_of_skip_levels;
        }

        self.skip_stream[0]
            .as_mut()
            .unwrap()
            .seek(self.skip_pointer[0])?;

        let mut to_buffer = self.number_of_levels_to_buffer;

        for i in (1..self.number_of_skip_levels as usize).rev() {
            // the length of the current level
            let length = self.skip_stream[0].as_mut().unwrap().read_vlong()?;

            // the start pointer of the current level
            self.skip_pointer[i] = self.skip_stream[0].as_mut().unwrap().file_pointer();
            if to_buffer > 0 {
                // buffer this level
                let stream = { SkipBuffer::new(self.stream(0)?, length as i32)? };
                self.skip_stream[i] = Some(Box::new(stream));
                to_buffer -= 1;
            } else {
                // clone this stream, it is already at the start of the current level
                let stream = { IndexInput::clone(self.stream(0)?)? };
                self.skip_stream[i] = Some(stream);
                // if self.input_is_buffered && length < BufferedIndexInput.BUFFER_SIZE {
                // ((BufferedIndexInput)
                // skip_stream[i]).setBufferSize(Math.max(BufferedIndexInput.MIN_BUFFER_SIZE, (int)
                // length)); }
                //

                // move base stream beyond the current level
                let fp = self.skip_stream[0].as_mut().unwrap().file_pointer();
                self.skip_stream[0].as_mut().unwrap().seek(fp + length)?;
            }
        }

        // use base stream for the lowest level
        self.skip_pointer[0] = self.skip_stream[0].as_mut().unwrap().file_pointer();
        Ok(())
    }

    fn load_next_skip(&mut self, level: i32) -> Result<bool> {
        let ulevel = level as usize;
        self.set_last_skip_data(level)?;

        self.num_skipped[ulevel] += self.skip_interval[ulevel];

        // numSkipped may overflow a signed int, so compare as unsigned.
        if self.num_skipped[ulevel] > i64::from(self.doc_count) {
            // this skip list is exhausted
            self.skip_doc[ulevel] = i32::max_value();
            if self.number_of_skip_levels > level {
                self.number_of_skip_levels = level;
            }
            return Ok(false);
        }

        // read next skip entry
        self.skip_doc[ulevel] += self.read_skip_data(ulevel)?;

        if level != 0 {
            // read the child pointer if we are not on the leaf level
            self.child_pointer[ulevel] = self.skip_stream[ulevel].as_mut().unwrap().read_vlong()?
                + self.skip_pointer[ulevel - 1];
        }

        Ok(true)
    }

    fn stream(&mut self, id: usize) -> Result<&mut dyn IndexInput> {
        debug_assert!(id < self.skip_stream.len());
        Ok(self.skip_stream[id].as_mut().unwrap().as_mut())
    }

    /// Returns the id of the doc to which the last call of {@link #skipTo(int)}
    /// has skipped.
    pub fn doc(&self) -> DocId {
        self.last_doc
    }

    /// Skips entries to the first beyond the current whose document number is
    /// greater than or equal to <i>target</i>. Returns the current doc count.
    pub fn skip_to(&mut self, target: DocId) -> Result<DocId> {
        // walk up the levels until highest level is found that has a skip
        // for this target
        let mut level = 0i32;
        while level < self.number_of_skip_levels - 1 && target > self.skip_doc[(level + 1) as usize]
        {
            level += 1;
        }

        while level >= 0 {
            if target > self.skip_doc[level as usize] {
                if !self.load_next_skip(level)? {
                    continue;
                }
            } else {
                // no more skips on this level, go down one level
                if level > 0
                    && self.last_child_pointer
                        > self.skip_stream[(level - 1) as usize]
                            .as_mut()
                            .unwrap()
                            .file_pointer()
                {
                    self.seek_child(level - 1)?;
                }
                level -= 1;
            }
        }

        Ok((self.num_skipped[0] - self.skip_interval[0] - 1) as i32)
    }
}
