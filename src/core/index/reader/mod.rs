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

mod directory_reader;

pub use self::directory_reader::*;

mod leaf_reader;

pub use self::leaf_reader::*;

mod leaf_reader_wrapper;

pub use self::leaf_reader_wrapper::*;

mod segment_reader;

pub use self::segment_reader::*;

mod index_lookup;

pub use self::index_lookup::*;

use core::codec::Codec;
use core::codec::CodecTVFields;
use core::doc::Document;
use core::util::DocId;

use error::Result;

/// `IndexReader` providing an interface for accessing a point-in-time view of an index.
///
/// Any changes made to the index via `IndexWriter` will not be visible until a new
/// `IndexReader` is opened.  It's best to use {@link
/// StandardDirectoryReader#open(IndexWriter)} to obtain an `IndexReader`, if your
/// `IndexWriter` is in-process.  When you need to re-open to see changes to the
/// index, it's best to use {@link DirectoryReader#openIfChanged(DirectoryReader)}
/// since the new reader will share resources with the previous
/// one when possible.  Search of an index is done entirely
/// through this abstract interface, so that any subclass which
/// implements it is searchable.
///
/// IndexReader instances for indexes on disk are usually constructed
/// with a call to one of the static StandardDirectoryReader::open() methods.
///
/// For efficiency, in this API documents are often referred to via
/// *document numbers*, non-negative integers which each name a unique
/// document in the index.  These document numbers are ephemeral -- they may change
/// as documents are added to and deleted from an index.  Clients should thus not
/// rely on a given document having the same number between sessions.
///
/// NOTE: `IndexReader` instances are completely thread
/// safe, meaning multiple threads can call any of its methods,
/// concurrently.  If your application requires external
/// synchronization, you should *not* synchronize on the
/// `IndexReader` instance; use your own (non-Lucene) objects instead.
pub trait IndexReader {
    type Codec: Codec;
    fn leaves(&self) -> Vec<LeafReaderContext<'_, Self::Codec>>;
    fn term_vector(&self, doc_id: DocId) -> Result<Option<CodecTVFields<Self::Codec>>>;
    fn document(&self, doc_id: DocId, fields: &[String]) -> Result<Document>;
    fn max_doc(&self) -> i32;
    fn num_docs(&self) -> i32;
    fn num_deleted_docs(&self) -> i32 {
        self.max_doc() - self.num_docs()
    }
    fn has_deletions(&self) -> bool {
        self.num_deleted_docs() > 0
    }
    fn leaf_reader_for_doc(&self, doc: DocId) -> LeafReaderContext<'_, Self::Codec> {
        let leaves = self.leaves();
        let size = leaves.len();
        let mut lo = 0usize;
        let mut hi = size - 1;
        while hi >= lo {
            let mut mid = (lo + hi) >> 1;
            let mid_value = leaves[mid].doc_base;
            if doc < mid_value {
                hi = mid - 1;
            } else if doc > mid_value {
                lo = mid + 1;
            } else {
                while mid + 1 < size && leaves[mid + 1].doc_base == mid_value {
                    mid += 1;
                }
                return leaves[mid].clone();
            }
        }
        leaves[hi].clone()
    }

    // used for refresh
    fn refresh(&self) -> Result<Option<Box<dyn IndexReader<Codec = Self::Codec>>>> {
        Ok(None)
    }
}

#[derive(Copy, Clone)]
pub struct ReaderSlice {
    pub start: i32,
    pub length: i32,
    pub reader_index: usize,
}

impl ReaderSlice {
    pub fn new(start: i32, length: i32, reader_index: usize) -> ReaderSlice {
        ReaderSlice {
            start,
            length,
            reader_index,
        }
    }
}
