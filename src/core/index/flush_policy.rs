use core::index::flush_control::DocumentsWriterFlushControl;
use core::index::index_writer_config::IndexWriterConfig;
use core::index::thread_doc_writer::{LockedThreadState, ThreadState};

use std::sync::Arc;

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
    fn on_delete(&self, control: &mut DocumentsWriterFlushControl, state: Option<&ThreadState>);

    /// Called for each document update on the given `ThreadState`'s
    /// `DocumentsWriterPerThread`.
    ///
    /// Note: This method is called  synchronized on the given
    /// `DocumentsWriterFlushControl` and it is guaranteed that the calling
    /// thread holds the lock on the given `ThreadState`
    fn on_update(&self, control: &mut DocumentsWriterFlushControl, state: &ThreadState) {
        self.on_insert(control, state);
        self.on_delete(control, Some(state));
    }

    /// Called for each document addition on the given `ThreadState`s
    /// `DocumentsWriterPerThread`.
    ///
    /// Note: This method is synchronized by the given
    /// `DocumentsWriterFlushControl` and it is guaranteed that the calling
    /// thread holds the lock on the given `ThreadState`
    fn on_insert(&self, control: &mut DocumentsWriterFlushControl, state: &ThreadState);

    /// Returns the current most RAM consuming non-pending `ThreadState` with
    /// at least one indexed document.
    ///
    /// @Return: LockedThreadState, the largest pending writer
    ///          None: if the current is the largest
    fn find_largest_non_pending_writer(
        &self,
        control: &DocumentsWriterFlushControl,
        per_thread_state: &ThreadState,
    ) -> Option<LockedThreadState> {
        debug_assert!(per_thread_state.dwpt().num_docs_in_ram > 0);
        let mut max_ram_so_far = per_thread_state.bytes_used;
        // the dwpt which needs to be flushed eventually
        debug_assert!(!per_thread_state.flush_pending());
        let mut count = 0;
        let mut max_thread_state_idx = usize::max_value();
        for idx in 0..control.per_thread_pool().thread_states.len() {
            match control.per_thread_pool().thread_states[idx].try_lock() {
                Ok(guard) => {
                    if !guard.flush_pending() {
                        let next_ram = guard.bytes_used;
                        if next_ram > 0 && guard.dwpt().num_docs_in_ram > 0 {
                            debug!(
                                "FP - thread state has {} bytes; doc_in_ram: {}",
                                next_ram,
                                guard.dwpt().num_docs_in_ram
                            );
                        }
                        count += 1;
                        if next_ram > max_ram_so_far {
                            max_ram_so_far = next_ram;
                            max_thread_state_idx = idx;
                        }
                    }
                }
                Err(_e) => {}
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
pub struct FlushByRamOrCountsPolicy {
    index_write_config: Arc<IndexWriterConfig>,
}

impl FlushByRamOrCountsPolicy {
    pub fn new(index_write_config: Arc<IndexWriterConfig>) -> Self {
        FlushByRamOrCountsPolicy { index_write_config }
    }

    fn mark_largest_writer_pending(
        &self,
        control: &mut DocumentsWriterFlushControl,
        per_thread_state: &ThreadState,
    ) {
        if let Some(locked_state) = self.find_largest_non_pending_writer(control, per_thread_state)
        {
            let guard = locked_state.state.lock().unwrap();
            control.set_flush_pending(&guard);
        } else {
            control.set_flush_pending(per_thread_state);
        }
    }
}

impl FlushPolicy for FlushByRamOrCountsPolicy {
    fn on_delete(&self, control: &mut DocumentsWriterFlushControl, _state: Option<&ThreadState>) {
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

    fn on_insert(&self, control: &mut DocumentsWriterFlushControl, state: &ThreadState) {
        if self.index_write_config.flush_on_doc_count()
            && state.dwpt().num_docs_in_ram >= self.index_write_config.max_buffered_docs()
        {
            // Flush this state by num docs
            control.set_flush_pending(state);
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
                self.mark_largest_writer_pending(control, state);
            }
        }
    }
}
