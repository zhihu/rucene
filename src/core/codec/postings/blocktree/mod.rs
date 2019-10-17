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

mod blocktree_reader;

pub use self::blocktree_reader::*;

mod blocktree_writer;

pub use self::blocktree_writer::*;

mod term_iter_frame;

pub use self::term_iter_frame::*;

const MAX_LONGS_SIZE: usize = 3;

use core::codec::TermState;

/// Holds all state required for `PostingsReaderBase` to produce a
/// `PostingIterator` without re-seeking the term dict.
#[derive(Clone, Debug)]
pub struct BlockTermState {
    /// Term ordinal, i.e. its position in the full list of
    /// sorted terms.
    pub ord: i64,
    /// how many docs have this term
    pub doc_freq: i32,

    /// total number of occurrences of this term
    pub total_term_freq: i64,

    /// the term's ord in the current block
    pub term_block_ord: i32,

    /// fp into the terms dict primary file (_X.tim) that holds this term
    // TODO: update BTR to nuke this
    pub block_file_pointer: i64,

    /// fields from IntBlockTermState
    pub doc_start_fp: i64,
    pub pos_start_fp: i64,
    pub pay_start_fp: i64,
    pub skip_offset: i64,
    pub last_pos_block_offset: i64,
    // docid when there is a single pulsed posting, otherwise -1
    // freq is always implicitly totalTermFreq in this case.
    pub singleton_doc_id: i32,
}

impl BlockTermState {
    pub fn new() -> BlockTermState {
        BlockTermState {
            ord: 0,
            doc_freq: 0,
            total_term_freq: 0,
            term_block_ord: 0,
            block_file_pointer: 0,

            doc_start_fp: 0,
            pos_start_fp: 0,
            pay_start_fp: 0,
            skip_offset: -1,
            last_pos_block_offset: -1,
            singleton_doc_id: -1,
        }
    }

    pub fn copy_from(&mut self, other: &BlockTermState) {
        self.ord = other.ord;
        self.doc_freq = other.doc_freq;
        self.total_term_freq = other.total_term_freq;
        self.term_block_ord = other.term_block_ord;
        self.block_file_pointer = other.block_file_pointer;
        self.doc_start_fp = other.doc_start_fp;
        self.pos_start_fp = other.pos_start_fp;
        self.pay_start_fp = other.pay_start_fp;
        self.last_pos_block_offset = other.last_pos_block_offset;
        self.skip_offset = other.skip_offset;
        self.singleton_doc_id = other.singleton_doc_id;
    }

    pub fn ord(&self) -> i64 {
        self.ord
    }

    pub fn doc_freq(&self) -> i32 {
        self.doc_freq
    }

    pub fn total_term_freq(&self) -> i64 {
        self.total_term_freq
    }

    pub fn term_block_ord(&self) -> i32 {
        self.term_block_ord
    }

    pub fn block_file_pointer(&self) -> i64 {
        self.block_file_pointer
    }

    pub fn doc_start_fp(&self) -> i64 {
        self.doc_start_fp
    }
    pub fn pos_start_fp(&self) -> i64 {
        self.pos_start_fp
    }
    pub fn pay_start_fp(&self) -> i64 {
        self.pay_start_fp
    }
    pub fn skip_offset(&self) -> i64 {
        self.skip_offset
    }
    pub fn last_pos_block_offset(&self) -> i64 {
        self.last_pos_block_offset
    }
    pub fn singleton_doc_id(&self) -> i32 {
        self.singleton_doc_id
    }
}

impl TermState for BlockTermState {}
