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
use core::index::flush_control::{DocumentsWriterFlushControl, FlushControlLock};
use core::index::index_writer_config::IndexWriterConfig;
use core::index::merge_policy::MergePolicy;
use core::index::merge_scheduler::MergeScheduler;
use core::index::thread_doc_writer::ThreadState;
use core::store::Directory;

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
pub(crate) trait FlushPolicy {
    /// Called for each delete term. If this is a delete triggered due to an update
    /// the given `ThreadState` is non-null.
    ///
    /// Note: This method is called synchronized on the given
    /// `DocumentsWriterFlushControl` and it is guaranteed that the calling
    /// thread holds the lock on the given `ThreadState`
    fn on_delete<D: Directory, C: Codec, MS: MergeScheduler, MP: MergePolicy>(
        &self,
        control: &mut DocumentsWriterFlushControl<D, C, MS, MP>,
        state: Option<&ThreadState<D, C, MS, MP>>,
    );

    /// Called for each document update on the given `ThreadState`'s
    /// `DocumentsWriterPerThread`.
    ///
    /// Note: This method is called  synchronized on the given
    /// `DocumentsWriterFlushControl` and it is guaranteed that the calling
    /// thread holds the lock on the given `ThreadState`
    fn on_update<D: Directory, C: Codec, MS: MergeScheduler, MP: MergePolicy>(
        &self,
        control: &mut DocumentsWriterFlushControl<D, C, MS, MP>,
        lg: &MutexGuard<FlushControlLock>,
        state: &ThreadState<D, C, MS, MP>,
    ) {
        self.on_insert(control, lg, state);
        self.on_delete(control, Some(state));
    }

    /// Called for each document addition on the given `ThreadState`s
    /// `DocumentsWriterPerThread`.
    ///
    /// Note: This method is synchronized by the given
    /// `DocumentsWriterFlushControl` and it is guaranteed that the calling
    /// thread holds the lock on the given `ThreadState`
    fn on_insert<D: Directory, C: Codec, MS: MergeScheduler, MP: MergePolicy>(
        &self,
        control: &mut DocumentsWriterFlushControl<D, C, MS, MP>,
        lg: &MutexGuard<FlushControlLock>,
        state: &ThreadState<D, C, MS, MP>,
    );

    /// Returns the current most RAM consuming non-pending `ThreadState` with
    /// at least one indexed document.
    ///
    /// @Return: Arc<ThreadState>, the largest pending writer
    ///          None: if the current is the largest
    fn find_largest_non_pending_writer<
        D: Directory,
        C: Codec,
        MS: MergeScheduler,
        MP: MergePolicy,
    >(
        &self,
        control: &DocumentsWriterFlushControl<D, C, MS, MP>,
        per_thread_state: &ThreadState<D, C, MS, MP>,
    ) -> Option<Arc<ThreadState<D, C, MS, MP>>> {
        debug_assert!(per_thread_state.dwpt().num_docs_in_ram > 0);
        let mut max_ram_so_far = per_thread_state.bytes_used;
        // the dwpt which needs to be flushed eventually
        debug_assert!(!per_thread_state.flush_pending());
        let mut count = 0;
        let mut max_thread_state_idx = usize::max_value();
        let pool = control.per_thread_pool();
        {
            for idx in 0..control.per_thread_pool().active_thread_state_count() {
                let state = pool.get_thread_state(idx);
                if !state.flush_pending() {
                    let next_ram = state.bytes_used();
                    if next_ram > 0 && state.dwpt().num_docs_in_ram > 0 {
                        debug!(
                            "FP - thread state has {} bytes; doc_in_ram: {}",
                            next_ram,
                            state.dwpt().num_docs_in_ram
                        );
                    }
                    count += 1;
                    if next_ram > max_ram_so_far {
                        max_ram_so_far = next_ram;
                        max_thread_state_idx = idx;
                    }
                }
            }
        }
        debug!("FP - {} in-use non-flushing threads states.", count);
        debug!("FP - set largest ram consuming thread pending on lower watermark");
        if max_thread_state_idx != usize::max_value() {
            Some(control.per_thread_pool().locked_state(max_thread_state_idx))
        } else {
            None
        }
    }
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
pub(crate) struct FlushByRamOrCountsPolicy<C: Codec, MS: MergeScheduler, MP: MergePolicy> {
    index_write_config: Arc<IndexWriterConfig<C, MS, MP>>,
}

impl<C: Codec, MS: MergeScheduler, MP: MergePolicy> FlushByRamOrCountsPolicy<C, MS, MP> {
    pub fn new(index_write_config: Arc<IndexWriterConfig<C, MS, MP>>) -> Self {
        FlushByRamOrCountsPolicy { index_write_config }
    }

    fn mark_largest_writer_pending<
        D: Directory,
        C1: Codec,
        MS1: MergeScheduler,
        MP1: MergePolicy,
    >(
        &self,
        control: &mut DocumentsWriterFlushControl<D, C1, MS1, MP1>,
        lg: &MutexGuard<FlushControlLock>,
        per_thread_state: &ThreadState<D, C1, MS1, MP1>,
    ) {
        if let Some(locked_state) = self.find_largest_non_pending_writer(control, per_thread_state)
        {
            control.set_flush_pending(&*locked_state, lg);
        } else {
            control.set_flush_pending(per_thread_state, lg);
        }
    }
}

impl<C1: Codec, MS1: MergeScheduler, MP1: MergePolicy> FlushPolicy
    for FlushByRamOrCountsPolicy<C1, MS1, MP1>
{
    fn on_delete<D: Directory, C: Codec, MS: MergeScheduler, MP: MergePolicy>(
        &self,
        control: &mut DocumentsWriterFlushControl<D, C, MS, MP>,
        _state: Option<&ThreadState<D, C, MS, MP>>,
    ) {
        if self.index_write_config.flush_on_delete_terms() {
            // flush this state by num del terms
            if control.num_global_term_deletes()
                >= self.index_write_config.max_buffered_delete_terms() as usize
            {
                control.set_apply_all_deletes();
            }
        }

        if self.index_write_config.flush_on_ram()
            && control.delete_bytes_used() > self.index_write_config.ram_buffer_size()
        {
            control.set_apply_all_deletes();
            debug!(
                "FP - force apply deletes bytes_used: {} vs ram_bufffer_mb={}",
                control.delete_bytes_used(),
                self.index_write_config.ram_buffer_size_mb()
            );
        }
    }

    fn on_insert<D: Directory, C: Codec, MS: MergeScheduler, MP: MergePolicy>(
        &self,
        control: &mut DocumentsWriterFlushControl<D, C, MS, MP>,
        lg: &MutexGuard<FlushControlLock>,
        state: &ThreadState<D, C, MS, MP>,
    ) {
        if self.index_write_config.flush_on_doc_count()
            && state.dwpt().num_docs_in_ram >= self.index_write_config.max_buffered_docs()
        {
            // Flush this state by num docs
            control.set_flush_pending(state, lg);
        } else if self.index_write_config.flush_on_ram() {
            let limit = self.index_write_config.ram_buffer_size();
            let total_ram = control.active_bytes as usize + control.delete_bytes_used();
            if total_ram >= limit {
                debug!(
                    "FP - trigger flush: active_bytes={}, delete_bytes={} vs limit={}",
                    control.active_bytes,
                    control.delete_bytes_used(),
                    limit
                );
                self.mark_largest_writer_pending(control, lg, state);
            }
        }
    }
}
