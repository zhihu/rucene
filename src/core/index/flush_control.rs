use core::index::bufferd_updates::BufferedUpdatesStream;
use core::index::doc_writer::DocumentsWriter;
use core::index::doc_writer_delete_queue::DocumentsWriterDeleteQueue;
use core::index::flush_policy::FlushPolicy;
use core::index::index_writer_config::IndexWriterConfig;
use core::index::thread_doc_writer::DocumentsWriterPerThreadPool;
use core::index::thread_doc_writer::{DocumentsWriterPerThread, LockedThreadState, ThreadState};
use error::Result;

use std::cmp::max;
use std::collections::{HashMap, VecDeque};
use std::mem;
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, ThreadId};
use std::time::Duration;

/// This class controls `DocumentsWriterPerThread` flushing during
/// indexing. It tracks the memory consumption per
/// `DocumentsWriterPerThread} and uses a configured {@link FlushPolicy` to
/// decide if a `DocumentsWriterPerThread` must flush.
///
/// In addition to the `FlushPolicy` the flush control might set certain
/// `DocumentsWriterPerThread` as flush pending iff a
/// `DocumentsWriterPerThread` exceeds the
/// `IndexWriterConfig#getRAMPerThreadHardLimitMB()` to prevent address
/// space exhaustion.
pub struct DocumentsWriterFlushControl {
    lock: Arc<Mutex<()>>,
    cond: Condvar,
    hard_max_bytes_per_dwpt: u64,
    pub active_bytes: u64,
    flush_bytes: u64,
    num_pending: usize,
    num_docs_since_stalled: u32,
    // only with assert
    flush_deletes: AtomicBool,
    full_flush: AtomicBool,
    pub flush_queue: VecDeque<DocumentsWriterPerThread>,
    // only for safety reasons if a DWPT is close to the RAM limit
    blocked_flushes: Vec<BlockedFlush>,
    flushing_writers: HashMap<String, u64>,
    // key is segment_name of the DocumentsWriterPerThread
    max_configured_ram_buffer: f64,
    peak_active_bytes: u64,
    // only with assert
    peak_flush_bytes: u64,
    // only with assert
    peak_net_bytes: u64,
    // only with assert
    peak_delta: u64,
    // only with assert
    flush_by_ram_was_disabled: bool,
    // only with assert
    stall_control: DocumentsWriterStallControl,
    per_thread_pool: *mut DocumentsWriterPerThreadPool,
    flush_policy: Arc<FlushPolicy>,
    closed: bool,
    documents_writer: *mut DocumentsWriter,
    config: Arc<IndexWriterConfig>,
    buffered_update_stream: *const BufferedUpdatesStream,
    full_flush_buffer: Vec<DocumentsWriterPerThread>,
    inited: bool,
}

impl DocumentsWriterFlushControl {
    pub fn new(config: Arc<IndexWriterConfig>, flush_policy: Arc<FlushPolicy>) -> Self {
        let hard_max_bytes_per_dwpt = config.per_thread_hard_limit();
        DocumentsWriterFlushControl {
            lock: Arc::new(Mutex::new(())),
            cond: Condvar::new(),
            hard_max_bytes_per_dwpt,
            active_bytes: 0,
            flush_bytes: 0,
            num_pending: 0,
            num_docs_since_stalled: 0,
            flush_deletes: AtomicBool::new(false),
            full_flush: AtomicBool::new(false),
            flush_queue: VecDeque::new(),
            blocked_flushes: vec![],
            flushing_writers: HashMap::new(),
            max_configured_ram_buffer: 0.0,
            peak_active_bytes: 0,
            peak_flush_bytes: 0,
            peak_net_bytes: 0,
            peak_delta: 0,
            flush_by_ram_was_disabled: false,
            stall_control: DocumentsWriterStallControl::new(),
            per_thread_pool: ptr::null_mut(),
            flush_policy,
            closed: false,
            documents_writer: ptr::null_mut(),
            config,
            buffered_update_stream: ptr::null(),
            full_flush_buffer: vec![],
            inited: false,
        }
    }

    pub fn init(
        &mut self,
        document_writer: &DocumentsWriter,
        buffered_update_stream: &BufferedUpdatesStream,
    ) {
        debug_assert!(!self.inited);
        self.per_thread_pool = &document_writer.per_thread_pool
            as *const DocumentsWriterPerThreadPool
            as *mut DocumentsWriterPerThreadPool;
        self.documents_writer = document_writer as *const DocumentsWriter as *mut DocumentsWriter;
        self.buffered_update_stream = buffered_update_stream;
        self.inited = true;
    }

    fn documents_writer(&self) -> &mut DocumentsWriter {
        debug_assert!(self.inited);
        unsafe { &mut *self.documents_writer }
    }

    fn buffered_update_stream(&self) -> &BufferedUpdatesStream {
        unsafe { &*self.buffered_update_stream }
    }

    pub fn per_thread_pool(&self) -> &mut DocumentsWriterPerThreadPool {
        debug_assert!(self.inited);
        unsafe { &mut *self.per_thread_pool }
    }

    pub fn do_after_document(
        &mut self,
        per_thread: &mut ThreadState,
        is_update: bool,
    ) -> Result<Option<DocumentsWriterPerThread>> {
        let lock = Arc::clone(&self.lock);
        let _l = lock.lock()?;
        let res = self.process_after_document(per_thread, is_update);
        let stalled = self.update_stall_state();
        debug_assert!(self.assert_num_docs_since_stalled(stalled) && self.assert_memory());
        res
    }

    fn process_after_document(
        &mut self,
        per_thread: &mut ThreadState,
        is_update: bool,
    ) -> Result<Option<DocumentsWriterPerThread>> {
        self.commit_per_thread_bytes(per_thread);
        if !per_thread.flush_pending() {
            unsafe {
                let writer = self as *mut DocumentsWriterFlushControl;
                if is_update {
                    self.flush_policy.on_update(&mut *writer, per_thread);
                } else {
                    self.flush_policy.on_insert(&mut *writer, per_thread);
                }
            }

            if !per_thread.flush_pending() && per_thread.bytes_used > self.hard_max_bytes_per_dwpt {
                // Safety check to prevent a single DWPT exceeding its RAM limit. This
                // is super important since we can not address more than 2048 MB per DWPT
                self.set_flush_pending(per_thread);
            }
        }
        let flushing_dwpt = if self.is_full_flush() {
            if per_thread.flush_pending() {
                self.checkout_and_block(per_thread);
                self.next_pending_flush()
            } else {
                None
            }
        } else {
            self.try_checkout_for_flush(per_thread)
        };
        Ok(flushing_dwpt)
    }

    fn assert_num_docs_since_stalled(&mut self, stalled: bool) -> bool {
        // updates the number of documents "finished" while we are in a stalled state.
        // this is important for asserting memory upper bounds since it corresponds
        // to the number of threads that are in-flight and crossed the stall control
        // check before we actually stalled.
        // see #assertMemory()
        if stalled {
            self.num_docs_since_stalled += 1;
        } else {
            self.num_docs_since_stalled = 0;
        }
        true
    }

    pub fn do_on_delete(&mut self) {
        // pass None this is a global delete no update
        unsafe {
            let writer = self as *mut DocumentsWriterFlushControl;
            self.flush_policy.on_delete(&mut *writer, None);
        }
    }

    pub fn num_global_term_deletes(&self) -> usize {
        self.documents_writer()
            .delete_queue
            .num_global_term_deletes()
            + self.buffered_update_stream().num_terms()
    }

    pub fn delete_bytes_used(&self) -> usize {
        self.documents_writer().delete_queue.ram_bytes_used()
            + self.buffered_update_stream().ram_bytes_used()
    }

    pub fn do_on_abort(&mut self, state: &mut ThreadState) -> Option<DocumentsWriterPerThread> {
        let lock = Arc::clone(&self.lock);
        let _l = lock.lock().unwrap();
        if state.flush_pending.load(Ordering::Acquire) {
            self.flush_bytes -= state.bytes_used;
        } else {
            self.active_bytes -= state.bytes_used;
        }
        let r = self.assert_memory();
        debug_assert!(r);

        // Take it out of the loop this DWPT is stale
        let dwpt = self.per_thread_pool().reset(state);

        self.update_stall_state();

        dwpt
    }

    fn stall_limit_bytes(&self) -> u64 {
        if self.config.flush_on_ram() {
            2 * self.config.ram_buffer_size() as u64
        } else {
            i64::max_value() as u64
        }
    }

    fn commit_per_thread_bytes(&mut self, per_thread: &mut ThreadState) {
        let delta = per_thread.dwpt().bytes_used() as u64 - per_thread.bytes_used;
        per_thread.bytes_used += delta;

        // We need to differentiate here if we are pending since setFlushPending
        // moves the per_thread memory to the flush_bytes and we could be set to
        // pending during a delete
        if per_thread.flush_pending() {
            self.flush_bytes += delta;
        } else {
            self.active_bytes += delta;
        }
        debug_assert!(self.update_peaks(delta));
    }

    // only for asserts
    fn update_peaks(&mut self, delta: u64) -> bool {
        self.peak_active_bytes = max(self.peak_active_bytes, self.active_bytes);
        self.peak_flush_bytes = max(self.peak_flush_bytes, self.flush_bytes);
        self.peak_net_bytes = max(self.peak_net_bytes, self.flush_bytes + self.active_bytes);
        self.peak_delta = max(self.peak_delta, delta);
        true
    }

    pub fn get_and_reset_apply_all_deletes(&self) -> bool {
        self.flush_deletes.swap(false, Ordering::AcqRel)
    }

    pub fn set_apply_all_deletes(&self) {
        self.flush_deletes.store(true, Ordering::Release)
    }

    pub fn abort_pending_flushes(&mut self) {
        let lock = Arc::clone(&self.lock);
        let _l = lock.lock().unwrap();

        let flush_queue = mem::replace(&mut self.flush_queue, VecDeque::with_capacity(0));
        for mut dwpt in flush_queue {
            self.documents_writer()
                .subtract_flushed_num_docs(dwpt.num_docs_in_ram);
            dwpt.abort();
            self.do_after_flush(dwpt);
        }

        let blocked_flushes = mem::replace(&mut self.blocked_flushes, vec![]);
        for mut blocked_flush in blocked_flushes {
            self.flushing_writers.insert(
                blocked_flush.dwpt.segment_info.name.clone(),
                blocked_flush.bytes,
            );
            self.documents_writer()
                .subtract_flushed_num_docs(blocked_flush.dwpt.num_docs_in_ram);
            blocked_flush.dwpt.abort();
            self.do_after_flush(blocked_flush.dwpt);
        }

        self.flush_queue.clear();
        self.update_stall_state();
    }

    pub fn is_full_flush(&self) -> bool {
        self.full_flush.load(Ordering::Acquire)
    }

    fn assert_memory(&self) -> bool {
        let _max_ram_mb = self.config.ram_buffer_size_mb();

        // We can only assert if we have always been flushing by RAM usage; otherwise
        // the assert will false trip if e.g. the flush-by-doc-count * doc size was
        // large enough to use far more RAM than the sudden change to IWC's maxRAMBufferSizeMB:
        // TODO
        true
    }

    pub fn do_after_flush(&mut self, dwpt: DocumentsWriterPerThread) {
        let lock = Arc::clone(&self.lock);
        let _l = lock.lock().unwrap();
        debug_assert!(self.flushing_writers.contains_key(&dwpt.segment_info.name));
        let bytes = self.flushing_writers.remove(&dwpt.segment_info.name);
        self.flush_bytes -= bytes.unwrap();
        self.per_thread_pool().recycle(dwpt);

        self.update_stall_state();
        self.cond.notify_all();
    }

    fn update_stall_state(&mut self) -> bool {
        let limit = self.stall_limit_bytes();
        // we block indexing threads if net byte grows due to slow flushes
        // yet, for small ram buffers and large documents we can easily
        // reach the limit without any ongoing flushes. we need to ensure
        // that we don't stall/block if an ongoing or pending flush can
        // not free up enough memory to release the stall lock.
        let stall = (self.active_bytes + self.flush_bytes) > limit
            && self.active_bytes < limit
            && !self.closed;
        self.stall_control.update_stalled(stall);
        stall
    }

    pub fn wait_for_flush(&mut self) -> Result<()> {
        let mut l = self.lock.lock().unwrap();
        while !self.flushing_writers.is_empty() {
            l = self.cond.wait(l)?;
        }
        Ok(())
    }

    pub fn num_queued_flushes(&self) -> usize {
        let _l = self.lock.lock().unwrap();
        self.flush_queue.len()
    }

    pub fn any_stalled_threads(&self) -> bool {
        self.stall_control.stalled
    }

    pub fn next_pending_flush(&mut self) -> Option<DocumentsWriterPerThread> {
        let num_pending: usize;
        let full_flush: bool;
        {
            let lock = Arc::clone(&self.lock);
            let _l = lock.lock().unwrap();
            if let Some(dwpt) = self.flush_queue.pop_front() {
                self.update_stall_state();
                return Some(dwpt);
            }
            full_flush = self.full_flush.load(Ordering::Acquire);
            num_pending = self.num_pending;
        }

        if num_pending > 0 && !full_flush {
            // don't check if we are doing a full flush
            let limit = self.per_thread_pool().active_thread_state_count();

            for i in 0..limit {
                let next = Arc::clone(&self.per_thread_pool().thread_states[i]);
                let mut guard = next.lock().unwrap();
                if guard.flush_pending() {
                    if let Some(dwpt) = self.try_checkout_for_flush(&mut guard) {
                        return Some(dwpt);
                    }
                }
            }
        }
        None
    }

    // TODO, actually we didn't lock the state, this should be done by the caller
    pub fn obtain_and_lock(&mut self) -> Result<LockedThreadState> {
        let per_thread: LockedThreadState = self.per_thread_pool().get_and_lock()?;
        {
            let mut guard = match per_thread.state.try_lock() {
                Ok(g) => g,
                Err(e) => {
                    bail!("obtain_and_lock try lock per_thread.state failed: {:?}", e);
                }
            };

            if guard.inited()
                && guard.dwpt().delete_queue.generation.load(Ordering::Acquire)
                    != self
                        .documents_writer()
                        .delete_queue
                        .generation
                        .load(Ordering::Acquire)
            {
                // There is a flush-all in process and this DWPT is
                // now stale -- enroll it for flush and try for
                // another DWPT:
                self.add_flushable_state(&mut guard);
            }
        }
        // simply return the ThreadState even in a flush all case sine we already hold the lock
        Ok(per_thread)
    }

    pub fn mark_for_full_flush(&mut self) -> (u64, Arc<DocumentsWriterDeleteQueue>) {
        let flushing_queue: Arc<DocumentsWriterDeleteQueue>;
        let seq_no: u64;
        {
            let _l = self.lock.lock().unwrap();
            debug_assert!(!self.is_full_flush());
            debug_assert!(self.full_flush_buffer.is_empty());
            self.full_flush.store(true, Ordering::Release);
            // Set a new delete queue - all subsequent DWPT will use this queue until
            // we do another full flush

            // Insert a gap in seqNo of current active thread count, in the worst
            // case each of those threads now have one operation in flight.  It's fine
            // if we have some sequence numbers that were never assigned:
            seq_no = self.documents_writer().delete_queue.last_sequence_number()
                + self.per_thread_pool().active_thread_state_count() as u64
                + 2;
            let new_queue = Arc::new(DocumentsWriterDeleteQueue::with_generation(
                self.documents_writer()
                    .delete_queue
                    .generation
                    .load(Ordering::Acquire)
                    + 1,
                seq_no + 1,
            ));

            flushing_queue = Arc::clone(&self.documents_writer().delete_queue);
            self.documents_writer().delete_queue = new_queue;
        }

        let limit = self.per_thread_pool().active_thread_state_count();
        for i in 0..limit {
            let next = Arc::clone(&self.per_thread_pool().thread_states[i]);
            let mut guard = next.lock().unwrap();

            if !guard.inited() {
                continue;
            }

            if guard.dwpt().delete_queue.generation.load(Ordering::Acquire)
                != flushing_queue.generation.load(Ordering::Acquire)
            {
                // this one is already a new DWPT
                continue;
            }
            self.add_flushable_state(&mut guard);
        }

        {
            // make sure we move all DWPT that are where concurrently marked as
            // pending and moved to blocked are moved over to the flushQueue. There is
            // a chance that this happens since we marking DWPT for full flush without
            // blocking indexing.
            let lock = Arc::clone(&self.lock);
            let _l = lock.lock().unwrap();
            self.prune_blocked_queue(flushing_queue.generation.load(Ordering::Acquire));
            debug_assert!(self.assert_blocked_flushes());
            let full_flush_buffer =
                mem::replace(&mut self.full_flush_buffer, Vec::with_capacity(0));
            for full_flush in full_flush_buffer {
                self.flush_queue.push_back(full_flush);
            }
            self.update_stall_state();
        }
        debug_assert!(self.assert_active_delete_queue());
        (seq_no, flushing_queue)
    }

    fn assert_active_delete_queue(&self) -> bool {
        let limit = self.per_thread_pool().active_thread_state_count();
        for state in self.per_thread_pool().thread_states.iter().take(limit) {
            let guard = state.lock().unwrap();
            debug_assert!(
                !guard.inited()
                    || guard.dwpt().delete_queue.generation.load(Ordering::Acquire)
                        == self
                            .documents_writer()
                            .delete_queue
                            .generation
                            .load(Ordering::Acquire)
            );
        }
        true
    }

    fn assert_blocked_flushes(&self) -> bool {
        for blocked_flush in &self.blocked_flushes {
            debug_assert_eq!(
                blocked_flush
                    .dwpt
                    .delete_queue
                    .generation
                    .load(Ordering::Acquire),
                self.documents_writer()
                    .delete_queue
                    .generation
                    .load(Ordering::Acquire)
            );
        }
        true
    }

    pub fn finish_full_flush(&mut self) {
        debug_assert!(self.is_full_flush());
        debug_assert!(self.flush_queue.is_empty());
        debug_assert!(self.flushing_writers.is_empty());

        if !self.blocked_flushes.is_empty() {
            debug_assert!(self.assert_blocked_flushes());
            let gen = self
                .documents_writer()
                .delete_queue
                .generation
                .load(Ordering::Acquire);
            self.prune_blocked_queue(gen);
            debug_assert!(self.blocked_flushes.is_empty());
        }
        self.full_flush.store(false, Ordering::Release);
        self.update_stall_state();
    }

    pub fn abort_full_flushes(&mut self) {
        self.abort_pending_flushes();
        self.full_flush.store(false, Ordering::Release);
    }

    fn add_flushable_state(&mut self, per_therad: &mut ThreadState) {
        debug!(
            "DWFC: add_flushable_state for {}",
            &per_therad.dwpt().segment_info.name
        );
        debug_assert!(per_therad.inited());
        debug_assert!(self.is_full_flush());
        if per_therad.dwpt().num_docs_in_ram > 0 {
            let lock = Arc::clone(&self.lock);
            let _l = lock.lock().unwrap();
            if !per_therad.flush_pending() {
                self.set_flush_pending(per_therad);
            }
            let flushing_dwpt = self.internal_try_checkout_for_flush(per_therad);
            debug_assert!(flushing_dwpt.is_some());
            self.full_flush_buffer.push(flushing_dwpt.unwrap());
        } else {
            // make this state inactive
            self.per_thread_pool().reset(per_therad);
        }
    }

    /// Prunes the blockedQueue by removing all DWPT that are
    /// associated with the given flush queue.
    fn prune_blocked_queue(&mut self, flushing_generation: u64) {
        let pruned = self.blocked_flushes.drain_filter(|bf| {
            bf.dwpt.delete_queue.generation.load(Ordering::Acquire) == flushing_generation
        });
        for blocked_flush in pruned {
            debug_assert!(!self
                .flushing_writers
                .contains_key(&blocked_flush.dwpt.segment_info.name));
            // Record the flushing DWPT to reduce flushBytes in doAfterFlush
            self.flushing_writers.insert(
                blocked_flush.dwpt.segment_info.name.clone(),
                blocked_flush.bytes,
            );
            // don't decr pending here - it's already done when DWPT is blocked
            self.flush_queue.push_back(blocked_flush.dwpt);
        }
    }

    pub fn set_flush_pending(&mut self, per_thread: &ThreadState) {
        if per_thread.dwpt().num_docs_in_ram > 0 {
            // write access synced
            per_thread.flush_pending.store(true, Ordering::Release);
            let bytes = per_thread.bytes_used;
            self.flush_bytes += bytes;
            self.active_bytes -= bytes;
            self.num_pending += 1;
            debug_assert!(self.assert_memory());
        }
        // don't assert on numDocs since we could hit an abort excp.
        // while selecting that dwpt for flushing
    }

    fn try_checkout_for_flush(
        &mut self,
        per_thread: &mut ThreadState,
    ) -> Option<DocumentsWriterPerThread> {
        if per_thread.flush_pending() {
            self.internal_try_checkout_for_flush(per_thread)
        } else {
            None
        }
    }

    fn checkout_and_block(&mut self, per_thread: &mut ThreadState) {
        debug_assert!(per_thread.flush_pending());
        debug_assert!(self.is_full_flush());
        let bytes = per_thread.bytes_used;
        let dwpt = self.per_thread_pool().reset(per_thread);
        debug_assert!(dwpt.is_some());
        self.num_pending -= 1;
        self.blocked_flushes
            .push(BlockedFlush::new(dwpt.unwrap(), bytes));
    }

    fn internal_try_checkout_for_flush(
        &mut self,
        per_thread: &mut ThreadState,
    ) -> Option<DocumentsWriterPerThread> {
        debug_assert!(per_thread.flush_pending());
        // We are pending so all memory is already moved to flushBytes
        let res = if per_thread.inited() {
            let bytes = per_thread.bytes_used;

            let dwpt = self.per_thread_pool().reset(per_thread).unwrap();
            debug_assert!(!self.flushing_writers.contains_key(&dwpt.segment_info.name));

            self.flushing_writers
                .insert(dwpt.segment_info.name.clone(), bytes);
            self.num_pending -= 1;

            Some(dwpt)
        } else {
            None
        };
        self.update_stall_state();

        res
    }

    /// This method will block if too many DWPT are currently flushing and no
    /// checked out DWPT are available
    pub fn wait_if_stalled(&mut self) -> Result<()> {
        self.stall_control.wait_if_stalled()
    }

    pub fn set_closed(&mut self) {
        // set by DW to signal that we should not release new DWPT after close
        let _l = self.lock.lock().unwrap();
        self.closed = true;
    }
}

struct BlockedFlush {
    dwpt: DocumentsWriterPerThread,
    bytes: u64,
}

impl BlockedFlush {
    fn new(dwpt: DocumentsWriterPerThread, bytes: u64) -> Self {
        BlockedFlush { dwpt, bytes }
    }
}

/// Controls the health status of a `DocumentsWriter` sessions. This class
/// used to block incoming indexing threads if flushing significantly slower than
/// indexing to ensure the `DocumentsWriter`s healthiness. If flushing is
/// significantly slower than indexing the net memory used within an
/// `IndexWriter` session can increase very quickly and easily exceed the
/// JVM's available memory.
///
/// To prevent OOM Errors and ensure IndexWriter's stability this class blocks
/// incoming threads from indexing once 2 x number of available
/// `ThreadState`s in `DocumentsWriterPerThreadPool` is exceeded.
/// Once flushing catches up and the number of flushing DWPT is equal or lower
/// than the number of active `ThreadState`s threads are released and can
/// continue indexing.
struct DocumentsWriterStallControl {
    lock: Arc<Mutex<()>>,
    cond: Condvar,
    stalled: bool,
    num_waiting: u32,
    // only with assert
    was_stalled: bool,
    // only with assert
    waiting: HashMap<ThreadId, bool>,
    // only with assert
}

impl DocumentsWriterStallControl {
    fn new() -> Self {
        DocumentsWriterStallControl {
            lock: Arc::new(Mutex::new(())),
            cond: Condvar::new(),
            stalled: false,
            num_waiting: 0,
            was_stalled: false,
            waiting: HashMap::new(),
        }
    }
    ///
    // Update the stalled flag status. This method will set the stalled flag to
    // <code>true</code> iff the number of flushing
    // {@link DocumentsWriterPerThread} is greater than the number of active
    // {@link DocumentsWriterPerThread}. Otherwise it will reset the
    // {@link DocumentsWriterStallControl} to healthy and release all threads
    // waiting on {@link #waitIfStalled()}
    //
    fn update_stalled(&mut self, stalled: bool) {
        let _l = self.lock.lock().unwrap();
        if self.stalled != stalled {
            self.stalled = stalled;
            if stalled {
                self.was_stalled = true;
            }
            self.cond.notify_all();
        }
    }

    /// Blocks if documents writing is currently in a stalled state.
    fn wait_if_stalled(&mut self) -> Result<()> {
        if self.stalled {
            let lock = Arc::clone(&self.lock);
            let l = lock.lock().unwrap();
            if self.stalled {
                // don't loop here, higher level logic will re-stall!
                self.inc_waiters();
                // Defensive, in case we have a concurrency bug that fails to
                // .notify/All our thread: just wait for up to 1 second here,
                // and let caller re-stall if it's still needed:
                self.cond.wait_timeout(l, Duration::new(1, 0))?;
                self.decr_waiters();
            }
        }
        Ok(())
    }

    fn inc_waiters(&mut self) {
        self.num_waiting += 1;
        let v = self.waiting.insert(thread::current().id(), true);
        debug_assert!(v.is_none());
    }

    fn decr_waiters(&mut self) {
        debug_assert!(self.num_waiting > 0);
        let v = self.waiting.remove(&thread::current().id());
        debug_assert!(v.is_some());
        self.num_waiting -= 1;
    }

    fn has_blocked(&self) -> bool {
        let _l = self.lock.lock().unwrap();
        self.num_waiting > 0
    }

    // for tests
    fn is_healthy(&self) -> bool {
        // volatile read!
        !self.stalled
    }

    fn is_thread_queued(&self, t: &ThreadId) -> bool {
        let _l = self.lock.lock().unwrap();
        self.waiting.contains_key(t)
    }

    fn was_stalled(&self) -> bool {
        let _l = self.lock.lock().unwrap();
        self.was_stalled
    }
}
