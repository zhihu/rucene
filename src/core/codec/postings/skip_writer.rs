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

use std::cmp::min;

use core::store::io::{DataOutput, IndexOutput, RAMOutputStream};
use core::util::{fill_slice, log, DocId};

use error::Result;

const SKIP_MULTIPLIER: u32 = 8;

/// Write skip lists with multiple levels, and support skip within block ints.
///
/// Assume that docFreq = 28, skipInterval = blockSize = 12
///
///  |       block#0       | |      block#1        | |vInts|
///  d d d d d d d d d d d d d d d d d d d d d d d d d d d d (posting list)
///                          ^                       ^       (level 0 skip point)
///
/// Note that skipWriter will ignore first document in block#0, since
/// it is useless as a skip point.  Also, we'll never skip into the vInts
/// block, only record skip data at the start its start point(if it exist).
///
/// For each skip point, we will record:
/// 1. doc_id in former position, i.e. for position 12, record doc_id\[11\], etc.
/// 2. its related file points(position, payload),
/// 3. related numbers or uptos(position, payload).
/// 4. start offset.
pub struct Lucene50SkipWriter {
    last_skip_doc: Vec<i32>,
    last_skip_doc_pointer: Vec<i64>,
    last_skip_pos_pointer: Vec<i64>,
    last_skip_pay_pointer: Vec<i64>,
    // these three output must be parameter when called
    //    doc_out: Box<IndexOutput>,
    //    pos_out: Option<Box<IndexOutput>>,
    //    pay_out: Option<Box<IndexOutput>>,
    cur_doc: DocId,
    cur_doc_pointer: i64,
    cur_pos_pointer: i64,
    cur_pay_pointer: i64,
    cur_pos_buffer_upto: usize,
    cur_payload_byte_upto: usize,
    field_has_positions: bool,
    field_has_offsets: bool,
    field_has_payloads: bool,

    // fields from MultiLevelSkipListWriter
    /// number of levels in this skip list
    number_of_skip_levels: usize,
    /// the skip interval in the list with level = 0
    skip_interval: u32,
    /// skipInterval used for level > 0
    skip_multiplier: u32,
    /// for every skip level a different buffer is used
    skip_buffer: Vec<RAMOutputStream>,

    // tricky: we only skip data for blocks (terms with more than 128 docs), but re-init'ing the
    // skipper is pretty slow for rare terms in large segments as we have to fill O(log #docs
    // in segment) of junk. this is the vast majority of terms (worst case: ID field or
    // similar).  so in resetSkip() we save away the previous pointers, and lazy-init only if
    // we need to buffer skip data for the term.
    initialized: bool,
    last_doc_fp: i64,
    last_pos_fp: i64,
    last_pay_fp: i64,
}

impl Lucene50SkipWriter {
    pub fn new(
        max_skip_levels: usize,
        block_size: u32,
        doc_count: u32,
        write_pos: bool,
        write_pay: bool,
    ) -> Self {
        debug_assert!(block_size > 1);
        let number_of_skip_levels = if doc_count <= block_size {
            1
        } else {
            1 + log(doc_count as i64 / block_size as i64, SKIP_MULTIPLIER as i32) as usize
        };

        let mut last_skip_pos_pointer = Vec::with_capacity(0);
        let mut last_skip_pay_pointer = Vec::with_capacity(0);
        if write_pos {
            last_skip_pos_pointer = vec![0i64; max_skip_levels];
            if write_pay {
                last_skip_pay_pointer = vec![0i64; max_skip_levels];
            }
        }

        Lucene50SkipWriter {
            last_skip_doc: vec![0i32; max_skip_levels],
            last_skip_doc_pointer: vec![0i64; max_skip_levels],
            last_skip_pos_pointer,
            last_skip_pay_pointer,
            cur_doc: 0,
            cur_doc_pointer: 0,
            cur_pos_pointer: 0,
            cur_pay_pointer: 0,
            cur_pos_buffer_upto: 0,
            cur_payload_byte_upto: 0,
            field_has_positions: false,
            field_has_offsets: false,
            field_has_payloads: false,
            skip_interval: block_size,
            skip_multiplier: SKIP_MULTIPLIER,
            number_of_skip_levels: min(max_skip_levels, number_of_skip_levels),
            skip_buffer: Vec::new(),
            initialized: false,
            last_doc_fp: 0,
            last_pos_fp: 0,
            last_pay_fp: 0,
        }
    }

    pub fn set_field(
        &mut self,
        field_has_positions: bool,
        field_has_offsets: bool,
        field_has_payloads: bool,
    ) {
        self.field_has_positions = field_has_positions;
        self.field_has_offsets = field_has_offsets;
        self.field_has_payloads = field_has_payloads;
    }

    pub fn reset_skip(&mut self, doc_fp: i64, pos_fp: i64, pay_fp: i64) {
        self.last_doc_fp = doc_fp;
        if self.field_has_positions {
            self.last_pos_fp = pos_fp;
            if self.field_has_offsets || self.field_has_payloads {
                self.last_pay_fp = pay_fp;
            }
        }
        self.initialized = false;
    }

    fn init_skip(&mut self) {
        if !self.initialized {
            self.reset_skip_base();
            fill_slice(&mut self.last_skip_doc, 0);
            fill_slice(&mut self.last_skip_doc_pointer, self.last_doc_fp);
            if self.field_has_positions {
                fill_slice(&mut self.last_skip_pos_pointer, self.last_pos_fp);
                if self.field_has_offsets || self.field_has_payloads {
                    fill_slice(&mut self.last_skip_pay_pointer, self.last_pay_fp);
                }
            }
            self.initialized = true;
        }
    }

    // Allocates internal skip buffers.
    fn init_multi_level(&mut self) {
        self.skip_buffer.truncate(0);
        for _i in 0..self.number_of_skip_levels {
            self.skip_buffer.push(RAMOutputStream::new(false));
        }
    }

    // Creates new buffers or empties the existing ones
    fn reset_skip_base(&mut self) {
        if self.skip_buffer.is_empty() {
            self.init_multi_level();
        } else {
            for buf in &mut self.skip_buffer {
                buf.reset();
            }
        }
    }

    /// Sets the values for the current skip data.
    #[allow(clippy::too_many_arguments)]
    pub fn buffer_skip(
        &mut self,
        doc: DocId,
        num_docs: u32,
        pos_fp: i64,
        pay_fp: i64,
        pos_buffer_upto: usize,
        payload_byte_upto: usize,
        doc_out_pointer: i64,
    ) -> Result<()> {
        self.init_skip();
        self.cur_doc = doc;
        self.cur_doc_pointer = doc_out_pointer;
        self.cur_pos_pointer = pos_fp;
        self.cur_pay_pointer = pay_fp;
        self.cur_pos_buffer_upto = pos_buffer_upto;
        self.cur_payload_byte_upto = payload_byte_upto;
        self.buffer_skip_levels(num_docs)
    }

    /// Writes the current skip data to the buffers. The current document frequency determines
    /// the max level is skip data is to be written to.
    fn buffer_skip_levels(&mut self, df: u32) -> Result<()> {
        debug_assert_eq!(df % self.skip_interval, 0);
        let mut num_levels = 1;
        let mut df = df / self.skip_interval;

        // determine max level
        loop {
            if df % self.skip_multiplier != 0 || num_levels >= self.number_of_skip_levels {
                break;
            }
            num_levels += 1;
            df /= self.skip_multiplier;
        }

        let mut child_pointer = 0;
        for i in 0..num_levels {
            self.write_skip_data_local(i)?;

            let new_child_pointer = self.skip_buffer[i].file_pointer();
            if i != 0 {
                // store child pointers for all levels except the lowest
                self.skip_buffer[i].write_vlong(child_pointer)?;
            }

            // remember the childPointer for the next level
            child_pointer = new_child_pointer;
        }

        Ok(())
    }

    /// Writes the buffered skip lists to the given output.
    pub fn write_skip(&self, output: &mut impl IndexOutput) -> Result<i64> {
        let skip_pointer = output.file_pointer();

        if self.skip_buffer.is_empty() {
            return Ok(skip_pointer);
        }

        for i in 1..self.number_of_skip_levels {
            let level = self.number_of_skip_levels - i;
            let length = self.skip_buffer[level].file_pointer();
            if length > 0 {
                output.write_vlong(length)?;
                self.skip_buffer[level].write_to(output)?;
            }
        }
        self.skip_buffer[0].write_to(output)?;
        Ok(skip_pointer)
    }

    /// Subclasses must implement the actual skip data encoding in this method.
    fn write_skip_data_local(&mut self, level: usize) -> Result<()> {
        let delta = self.cur_doc - self.last_skip_doc[level];

        self.skip_buffer[level].write_vint(delta)?;
        self.last_skip_doc[level] = self.cur_doc;

        self.skip_buffer[level]
            .write_vlong(self.cur_doc_pointer - self.last_skip_doc_pointer[level])?;
        self.last_skip_doc_pointer[level] = self.cur_doc_pointer;

        if self.field_has_positions {
            self.skip_buffer[level]
                .write_vlong(self.cur_pos_pointer - self.last_skip_pos_pointer[level])?;
            self.last_skip_pos_pointer[level] = self.cur_pos_pointer;
            self.skip_buffer[level].write_vint(self.cur_pos_buffer_upto as i32)?;

            if self.field_has_payloads {
                self.skip_buffer[level].write_vint(self.cur_payload_byte_upto as i32)?;
            }

            if self.field_has_offsets || self.field_has_payloads {
                self.skip_buffer[level]
                    .write_vlong(self.cur_pay_pointer - self.last_skip_pay_pointer[level])?;
                self.last_skip_pay_pointer[level] = self.cur_pay_pointer;
            }
        }

        Ok(())
    }
}
