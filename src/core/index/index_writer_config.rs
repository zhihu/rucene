use core::codec::{Codec, Lucene62Codec};
use core::index::delete_policy::IndexDeletionPolicy;
use core::index::delete_policy::KeepOnlyLastCommitDeletionPolicy;
use core::index::merge_policy::{MergePolicy, TieredMergePolicy};
use core::index::merge_scheduler::MergeScheduler;
use core::index::merge_scheduler::SerialMergeScheduler;
use core::search::bm25_similarity::BM25Similarity;
use core::search::sort::Sort;
use core::search::Similarity;

use std::sync::Arc;

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
pub struct IndexWriterConfig {
    pub ram_buffer_size_mb: Option<f64>,
    pub use_compound_file: bool,
    pub max_buffered_delete_terms: Option<u32>,
    pub max_buffered_docs: Option<u32>,
    pub merge_policy: Box<MergePolicy>,
    pub index_sort: Option<Sort>,
    /// True if readers should be pooled.
    pub reader_pooling: bool,
    pub open_mode: OpenMode,
    pub per_thread_hard_limit_mb: u32,
    pub codec: Arc<Codec>,
    pub commit_on_close: bool,
    // pub similarity: Box<Similarity>,
}

impl Default for IndexWriterConfig {
    fn default() -> Self {
        IndexWriterConfig {
            ram_buffer_size_mb: None,
            use_compound_file: true,
            max_buffered_delete_terms: None,
            max_buffered_docs: None,
            merge_policy: Box::new(TieredMergePolicy::default()),
            index_sort: None,
            reader_pooling: true,
            open_mode: OpenMode::CreateOrAppend,
            per_thread_hard_limit_mb: DEFAULT_RAM_PER_THREAD_HARD_LIMIT_MB,
            codec: Arc::new(Lucene62Codec::default()),
            commit_on_close: true,
            // similarity: Box::new(BM25Similarity::default()),
        }
    }
}

impl IndexWriterConfig {
    pub fn ram_buffer_size_mb(&self) -> f64 {
        let res = self.ram_buffer_size_mb.unwrap_or(0.0);
        debug_assert!(res >= 0.0);
        res
    }

    pub fn ram_buffer_size(&self) -> usize {
        debug_assert!(self.ram_buffer_size_mb.is_some());
        (self.ram_buffer_size_mb() * 1024.0 * 1024.0) as usize
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

    pub fn flush_on_ram(&self) -> bool {
        self.ram_buffer_size_mb.is_some()
    }

    pub fn flush_on_doc_count(&self) -> bool {
        self.max_buffered_docs.is_some()
    }

    pub fn merge_policy(&self) -> &MergePolicy {
        self.merge_policy.as_ref()
    }

    pub fn index_sort(&self) -> Option<&Sort> {
        self.index_sort.as_ref()
    }

    pub fn per_thread_hard_limit(&self) -> u64 {
        self.per_thread_hard_limit_mb as u64 * 1024 * 1024
    }

    pub fn index_deletion_policy(&self) -> Box<IndexDeletionPolicy> {
        Box::new(KeepOnlyLastCommitDeletionPolicy::default())
    }

    pub fn merge_scheduler(&self) -> Box<MergeScheduler> {
        Box::new(SerialMergeScheduler {})
    }

    pub fn codec(&self) -> &Codec {
        self.codec.as_ref()
    }

    // pub fn similarity(&self) -> &Similarity {
    //     self.similarity.as_ref()
    // }
}

/// Denotes a flush trigger is disabled.
pub const DISABLE_AUTO_FLUSH: i32 = -1;

/// Disabled by default (because IndexWriter flushes by RAM usage by default).
pub const DEFAULT_MAX_BUFFERED_DELETE_TERMS: i32 = DISABLE_AUTO_FLUSH;

/// Disabled by default (because IndexWriter flushes by RAM usage by default).
pub const DEFAULT_MAX_BUFFERED_DOCS: i32 = DISABLE_AUTO_FLUSH;

/// Default value is 16 MB (which means flush when buffered docs consume
/// approximately 16 MB RAM.
pub const DEFAULT_RAM_BUFFER_SIZE_MB: f64 = 16.0;

/// Default setting for `seg_reader_pooling`
pub const DEFAULT_READER_POOLING: bool = false;

pub const DEFAULT_RAM_PER_THREAD_HARD_LIMIT_MB: u32 = 1945;

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
