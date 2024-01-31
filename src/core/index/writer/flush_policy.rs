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

use core::codec::Codec;
use core::index::merge::{MergePolicy, MergeScheduler};
use core::index::writer::{
    DocumentsWriterFlushControl, FlushControlLock, IndexWriterConfig, ThreadState,
};
use core::store::directory::Directory;

use std::sync::{Arc, MutexGuard};

/// `FlushPolicy` controls when segments are flushed from a RAM resident
/// internal data-structure to the `IndexWriter`s `Directory`.
///
/// Segments are traditionally flushed by:
/// - RAM consumption - configured via
/// - `IndexWriterConfig#setRAMBufferSizeMB(double)`
/// - Number of RAM resident documents - configured via
/// - `IndexWriterConfig#setMaxBufferedDocs(int)`
///
/// The policy also applies pending delete operations (by term and/or query),
/// given the threshold set in
/// `IndexWriterConfig#setMaxBufferedDeleteTerms(int)`.
///
/// `IndexWriter` consults the provided `FlushPolicy` to control the
/// flushing process. The policy is informed for each added or updated document
/// as well as for each delete term. Based on the `FlushPolicy`, the
/// information provided via `ThreadState` and
/// `DocumentsWriterFlushControl`, the `FlushPolicy` decides if a
/// `DocumentsWriterPerThread` needs flushing and mark it as flush-pending
/// via `DocumentsWriterFlushControl#setFlushPending`, or if deletes need
/// to be applied.
///
/// @see ThreadState
/// @see DocumentsWriterFlushControl
/// @see DocumentsWriterPerThread
/// @see IndexWriterConfig#setFlushPolicy(FlushPolicy)
pub trait FlushPolicy {
    /// Called for each delete term. If this is a delete triggered due to an update
    /// the given `ThreadState` is non-null.
    ///
    /// Note: This method is called synchronized on the given
    /// `DocumentsWriterFlushControl` and it is guaranteed that the calling
    /// thread holds the lock on the given `ThreadState`
    fn on_delete<D, C, MS, MP>(
        &self,
        control: &mut DocumentsWriterFlushControl<D, C, MS, MP>,
        state: Option<&ThreadState<D, C, MS, MP>>,
    ) where
        D: Directory + Send + Sync + 'static,
        C: Codec,
        MS: MergeScheduler,
        MP: MergePolicy;

    /// Called for each document update on the given `ThreadState`'s
    /// `DocumentsWriterPerThread`.
    ///
    /// Note: This method is called  synchronized on the given
    /// `DocumentsWriterFlushControl` and it is guaranteed that the calling
    /// thread holds the lock on the given `ThreadState`
    fn on_update<D, C, MS, MP>(
        &self,
        control: &mut DocumentsWriterFlushControl<D, C, MS, MP>,
        lg: &MutexGuard<FlushControlLock>,
        state: &ThreadState<D, C, MS, MP>,
    ) where
        D: Directory + Send + Sync + 'static,
        C: Codec,
        MS: MergeScheduler,
        MP: MergePolicy,
    {
        self.on_insert(control, lg, state);
        self.on_delete(control, Some(state));
    }

    /// Called for each document addition on the given `ThreadState`s
    /// `DocumentsWriterPerThread`.
    ///
    /// Note: This method is synchronized by the given
    /// `DocumentsWriterFlushControl` and it is guaranteed that the calling
    /// thread holds the lock on the given `ThreadState`
    fn on_insert<D, C, MS, MP>(
        &self,
        control: &mut DocumentsWriterFlushControl<D, C, MS, MP>,
        lg: &MutexGuard<FlushControlLock>,
        state: &ThreadState<D, C, MS, MP>,
    ) where
        D: Directory + Send + Sync + 'static,
        C: Codec,
        MS: MergeScheduler,
        MP: MergePolicy;
}

/// Default `FlushPolicy` implementation that flushes new segments based on
/// RAM used and document count depending on the IndexWriter's
/// `IndexWriterConfig`. It also applies pending deletes based on the
/// number of buffered delete terms.
///
/// <ul>
/// <li>
/// `#onDelete(DocumentsWriterFlushControl, DocumentsWriterPerThreadPool.ThreadState)`
/// - applies pending delete operations based on the global number of buffered
/// delete terms iff `IndexWriterConfig#getMaxBufferedDeleteTerms()` is
/// enabled</li>
/// <li>
/// `#onInsert(DocumentsWriterFlushControl, DocumentsWriterPerThreadPool.ThreadState)`
/// - flushes either on the number of documents per
/// `DocumentsWriterPerThread` (
/// `DocumentsWriterPerThread#getNumDocsInRAM()`) or on the global active
/// memory consumption in the current indexing session iff
/// `IndexWriterConfig#getMaxBufferedDocs()` or
/// `IndexWriterConfig#getRAMBufferSizeMB()` is enabled respectively</li>
/// <li>
/// `#onUpdate(DocumentsWriterFlushControl, DocumentsWriterPerThreadPool.ThreadState)`
/// - calls
/// `#onInsert(DocumentsWriterFlushControl, DocumentsWriterPerThreadPool.ThreadState)`
/// and
/// `#onDelete(DocumentsWriterFlushControl, DocumentsWriterPerThreadPool.ThreadState)`
/// in order</li>
/// </ul>
/// All `IndexWriterConfig` settings are used to mark
/// `DocumentsWriterPerThread` as flush pending during indexing with
/// respect to their live updates.
///
/// If `IndexWriterConfig#setRAMBufferSizeMB(double)` is enabled, the
/// largest ram consuming `DocumentsWriterPerThread` will be marked as
/// pending iff the global active RAM consumption is {@code >=} the configured max RAM
/// buffer.
pub struct FlushByCountsPolicy<C: Codec, MS: MergeScheduler, MP: MergePolicy> {
    index_write_config: Arc<IndexWriterConfig<C, MS, MP>>,
}

impl<C: Codec, MS: MergeScheduler, MP: MergePolicy> FlushByCountsPolicy<C, MS, MP> {
    pub fn new(index_write_config: Arc<IndexWriterConfig<C, MS, MP>>) -> Self {
        FlushByCountsPolicy { index_write_config }
    }
}

impl<C1: Codec, MS1: MergeScheduler, MP1: MergePolicy> FlushPolicy
    for FlushByCountsPolicy<C1, MS1, MP1>
{
    fn on_delete<D, C, MS, MP>(
        &self,
        control: &mut DocumentsWriterFlushControl<D, C, MS, MP>,
        _state: Option<&ThreadState<D, C, MS, MP>>,
    ) where
        D: Directory + Send + Sync + 'static,
        C: Codec,
        MS: MergeScheduler,
        MP: MergePolicy,
    {
        if self.index_write_config.flush_on_delete_terms() {
            // flush this state by num del terms
            if control.documents_writer().num_global_term_deletes()
                >= self.index_write_config.max_buffered_delete_terms() as usize
            {
                control.set_apply_all_deletes();
            }
        }
    }

    fn on_insert<D, C, MS, MP>(
        &self,
        control: &mut DocumentsWriterFlushControl<D, C, MS, MP>,
        lg: &MutexGuard<FlushControlLock>,
        state: &ThreadState<D, C, MS, MP>,
    ) where
        D: Directory + Send + Sync + 'static,
        C: Codec,
        MS: MergeScheduler,
        MP: MergePolicy,
    {
        if (self.index_write_config.flush_on_doc_count()
            && state.dwpt().num_docs_in_ram >= self.index_write_config.max_buffered_docs())
            || unsafe { state.dwpt().consumer.assume_init_ref().need_flush() }
        {
            // Flush this state by num docs
            control.set_flush_pending(state, lg);
        }
    }
}
