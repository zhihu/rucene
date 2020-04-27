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

use core::codec::{Codec, CodecEnum, Lucene62Codec};
use core::index::merge::MergeScheduler;
use core::index::merge::SerialMergeScheduler;
use core::index::merge::{MergePolicy, TieredMergePolicy};
use core::index::writer::KeepOnlyLastCommitDeletionPolicy;
use core::search::sort_field::Sort;

use std::sync::Arc;

/// Denotes a flush trigger is disabled.
pub const DISABLE_AUTO_FLUSH: i32 = -1;

/// Disabled by default (because IndexWriter flushes by RAM usage by default).
pub const DEFAULT_MAX_BUFFERED_DELETE_TERMS: i32 = DISABLE_AUTO_FLUSH;

/// Disabled by default (because IndexWriter flushes by RAM usage by default).
pub const DEFAULT_MAX_BUFFERED_DOCS: i32 = DISABLE_AUTO_FLUSH;

/// Default setting for `seg_reader_pooling`
pub const DEFAULT_READER_POOLING: bool = false;

/// Default value for compound file system for newly written segments
/// (set to <code>true</code>). For batch indexing with very large
/// ram buffers use <code>false</code>
pub const DEFAULT_USE_COMPOUND_FILE_SYSTEM: bool = true;

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum OpenMode {
    Create,
    Append,
    CreateOrAppend,
}

/// Holds all the configuration that is used to create an {@link IndexWriter}.
/// Once {@link IndexWriter} has been created with this object, changes to this
/// object will not affect the {@link IndexWriter} instance. For that, use
/// {@link LiveIndexWriterConfig} that is returned from {@link IndexWriter#getConfig()}.
///
/// All setter methods return {@link IndexWriterConfig} to allow chaining
/// settings conveniently, for example:
///
/// <pre class="prettyprint">
/// IndexWriterConfig conf = new IndexWriterConfig(analyzer);
/// conf.setter1().setter2();
/// </pre>
///
/// @see IndexWriter#getConfig()
pub struct IndexWriterConfig<C: Codec, MS: MergeScheduler, MP: MergePolicy> {
    pub use_compound_file: bool,
    pub max_buffered_delete_terms: Option<u32>,
    pub max_buffered_docs: Option<u32>,
    pub merge_policy: MP,
    pub merge_scheduler: MS,
    pub index_sort: Option<Sort>,
    /// True if readers should be pooled.
    pub reader_pooling: bool,
    pub open_mode: OpenMode,
    pub codec: Arc<C>,
    pub commit_on_close: bool,
}

impl Default for IndexWriterConfig<CodecEnum, SerialMergeScheduler, TieredMergePolicy> {
    fn default() -> Self {
        Self::new(
            Arc::new(CodecEnum::Lucene62(Lucene62Codec::default())),
            SerialMergeScheduler {},
            TieredMergePolicy::default(),
        )
    }
}

impl<C: Codec, MS: MergeScheduler, MP: MergePolicy> IndexWriterConfig<C, MS, MP> {
    pub fn new(codec: Arc<C>, merge_scheduler: MS, merge_policy: MP) -> Self {
        IndexWriterConfig {
            use_compound_file: false,
            max_buffered_delete_terms: None,
            max_buffered_docs: None,
            merge_policy,
            merge_scheduler,
            index_sort: None,
            reader_pooling: true,
            open_mode: OpenMode::CreateOrAppend,
            codec,
            commit_on_close: true,
        }
    }

    pub fn max_buffered_delete_terms(&self) -> u32 {
        self.max_buffered_delete_terms.unwrap_or(0)
    }

    pub fn max_buffered_docs(&self) -> u32 {
        self.max_buffered_docs.unwrap_or(0)
    }

    pub fn flush_on_delete_terms(&self) -> bool {
        self.max_buffered_delete_terms.is_some()
    }

    pub fn flush_on_doc_count(&self) -> bool {
        self.max_buffered_docs.is_some()
    }

    pub fn merge_policy(&self) -> &MP {
        &self.merge_policy
    }

    pub fn index_sort(&self) -> Option<&Sort> {
        self.index_sort.as_ref()
    }

    pub fn index_deletion_policy(&self) -> KeepOnlyLastCommitDeletionPolicy {
        KeepOnlyLastCommitDeletionPolicy::default()
    }

    pub fn merge_scheduler(&self) -> MS {
        self.merge_scheduler.clone()
    }

    pub fn codec(&self) -> &C {
        self.codec.as_ref()
    }
}
