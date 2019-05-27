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
use core::index::bufferd_updates::BufferedUpdatesStream;
use core::index::doc_writer::DocumentsWriter;
use core::index::doc_writer_delete_queue::DocumentsWriterDeleteQueue;
use core::index::flush_policy::{FlushByRamOrCountsPolicy, FlushPolicy};
use core::index::index_writer_config::{IndexWriterConfig, DISABLE_AUTO_FLUSH};
use core::index::merge_policy::MergePolicy;
use core::index::merge_scheduler::MergeScheduler;
use core::index::thread_doc_writer::DocumentsWriterPerThreadPool;
use core::index::thread_doc_writer::{DocumentsWriterPerThread, ThreadState};
use core::util::Volatile;
use error::{ErrorKind::IllegalState, Result};

use core::store::Directory;
use std::cell::Cell;
use std::cmp::max;
use std::collections::{HashMap, VecDeque};
use std::mem;
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, ThreadId};
use std::time::Duration;

/// This class controls `DocumentsWriterPerThread` flushing during
/// indexing. It tracks the memory consumption per
/// `DocumentsWriterPerThread` and uses a configured `FlushPolicy` to
/// decide if a `DocumentsWriterPerThread` must flush.
///
/// In addition to the `FlushPolicy` the flush control might set certain
/// `DocumentsWriterPerThread` as flush pending iff a
/// `DocumentsWriterPerThread` exceeds the
/// `IndexWriterConfig#getRAMPerThreadHardLimitMB()` to prevent address
/// space exhaustion.
pub(crate) struct DocumentsWriterFlushControl<
    D: Directory + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
> {
    lock: Arc<Mutex<FlushControlLock>>,
    cond: Condvar,
    hard_max_bytes_per_dwpt: u64,
    pub active_bytes: u64,
    flush_bytes: u64,
    num_pending: Volatile<usize>,
    num_docs_since_stalled: u32,
    // only with assert
    flush_deletes: AtomicBool,
    full_flush: AtomicBool,
    pub flush_queue: VecDeque<DocumentsWriterPerThread<D, C, MS, MP>>,
    // only for safety reasons if a DWPT is close to the RAM limit
    blocked_flushes: Vec<BlockedFlush<D, C, MS, MP>>,
    // key is segment_name of the DocumentsWriterPerThread
    flushing_writers: HashMap<String, u64>,
    max_configured_ram_buffer: Cell<f64>,
    // only with assert
    peak_active_bytes: u64,
    // only with assert
    peak_flush_bytes: u64,
    // only with assert
    peak_net_bytes: u64,
    // only with assert
    peak_delta: u64,
    // only with assert
    flush_by_ram_was_disabled: Cell<bool>,
    stall_control: DocumentsWriterStallControl,
    per_thread_pool: *mut DocumentsWriterPerThreadPool<D, C, MS, MP>,
    flush_policy: Arc<FlushByRamOrCountsPolicy<C, MS, MP>>,
    closed: bool,
    documents_writer: *const DocumentsWriter<D, C, MS, MP>,
    config: Arc<IndexWriterConfig<C, MS, MP>>,
    buffered_update_stream: *const BufferedUpdatesStream<C>,
    full_flush_buffer: Vec<DocumentsWriterPerThread<D, C, MS, MP>>,
    inited: bool,
}

pub(crate) struct FlushControlLock;

impl<D: Directory + 'static, C: Codec, MS: MergeScheduler, MP: MergePolicy>
    DocumentsWriterFlushControl<D, C, MS, MP>
{
    pub fn new(
        config: Arc<IndexWriterConfig<C, MS, MP>>,
        flush_policy: Arc<FlushByRamOrCountsPolicy<C, MS, MP>>,
    ) -> Self {
        let hard_max_bytes_per_dwpt = config.per_thread_hard_limit();
        DocumentsWriterFlushControl {
            lock: Arc::new(Mutex::new(FlushControlLock)),
            cond: Condvar::new(),
            hard_max_bytes_per_dwpt,
            active_bytes: 0,
            flush_bytes: 0,
            num_pending: Volatile::new(0),
            num_docs_since_stalled: 0,
            flush_deletes: AtomicBool::new(false),
            full_flush: AtomicBool::new(false),
            flush_queue: VecDeque::new(),
            blocked_flushes: vec![],
            flushing_writers: HashMap::new(),
            max_configured_ram_buffer: Cell::new(0.0),
            peak_active_bytes: 0,
            peak_flush_bytes: 0,
            peak_net_bytes: 0,
            peak_delta: 0,
            flush_by_ram_was_disabled: Cell::new(false),
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
        document_writer: &DocumentsWriter<D, C, MS, MP>,
        buffered_update_stream: &BufferedUpdatesStream<C>,
    ) {
        debug_assert!(!self.inited);
        self.per_thread_pool = &document_writer.per_thread_pool
            as *const DocumentsWriterPerThreadPool<D, C, MS, MP>
            as *mut DocumentsWriterPerThreadPool<D, C, MS, MP>;
        self.documents_writer = document_writer;
        self.buffered_update_stream = buffered_update_stream;
        self.inited = true;
    }

    // TODO: maybe this action is not safe
    fn documents_writer(&self) -> &DocumentsWriter<D, C, MS, MP> {
        debug_assert!(self.inited);
        unsafe { &*self.documents_writer }
    }

    fn buffered_update_stream(&self) -> &BufferedUpdatesStream<C> {
        unsafe { &*self.buffered_update_stream }
    }

    pub fn per_thread_pool(&self) -> &DocumentsWriterPerThreadPool<D, C, MS, MP> {
        debug_assert!(self.inited);
        unsafe { &*self.per_thread_pool }
    }

    unsafe fn flush_control_mut(
        &self,
        _l: &MutexGuard<FlushControlLock>,
    ) -> &mut DocumentsWriterFlushControl<D, C, MS, MP> {
        let control = self as *const DocumentsWriterFlushControl<D, C, MS, MP>
            as *mut DocumentsWriterFlushControl<D, C, MS, MP>;
        &mut *control
    }

    pub fn do_after_document(
        &self,
        per_thread: &mut ThreadState<D, C, MS, MP>,
        is_update: bool,
    ) -> Result<Option<DocumentsWriterPerThread<D, C, MS, MP>>> {
        let l = self.lock.lock()?;
        let flush_control_mut = unsafe { self.flush_control_mut(&l) };
        let res = flush_control_mut.process_after_document(per_thread, is_update, &l);
        let stalled = self.update_stall_state();
        debug_assert!(
            flush_control_mut.assert_num_docs_since_stalled(stalled) && self.assert_memory(&l)
        );
        res
    }

    fn process_after_document(
        &mut self,
        per_thread: &mut ThreadState<D, C, MS, MP>,
        is_update: bool,
        lg: &MutexGuard<FlushControlLock>,
    ) -> Result<Option<DocumentsWriterPerThread<D, C, MS, MP>>> {
        self.commit_per_thread_bytes(per_thread);
        if !per_thread.flush_pending() {
            unsafe {
                let writer = self as *mut DocumentsWriterFlushControl<D, C, MS, MP>;
                if is_update {
                    self.flush_policy.on_update(&mut *writer, lg, per_thread);
                } else {
                    self.flush_policy.on_insert(&mut *writer, lg, per_thread);
                }
            }

            if !per_thread.flush_pending() && per_thread.bytes_used > self.hard_max_bytes_per_dwpt {
                // Safety check to prevent a single DWPT exceeding its RAM limit. This
                // is super important since we can not address more than 2048 MB per DWPT
                self.set_flush_pending(per_thread, lg);
            }
        }
        let flushing_dwpt = if self.is_full_flush() {
            if per_thread.flush_pending() {
                self.checkout_and_block(per_thread);
                self.do_next_pending_flush(lg)
            } else {
                None
            }
        } else {
            self.try_checkout_for_flush(per_thread, lg)
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
            let writer = self as *mut DocumentsWriterFlushControl<D, C, MS, MP>;
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

    pub fn do_on_abort(
        &self,
        state: &mut ThreadState<D, C, MS, MP>,
    ) -> Option<DocumentsWriterPerThread<D, C, MS, MP>> {
        let l = self.lock.lock().unwrap();
        let flush_control_mut = unsafe { self.flush_control_mut(&l) };
        if state.flush_pending.load(Ordering::Acquire) {
            flush_control_mut.flush_bytes -= state.bytes_used;
        } else {
            flush_control_mut.active_bytes -= state.bytes_used;
        }
        let r = self.assert_memory(&l);
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

    fn commit_per_thread_bytes(&mut self, per_thread: &mut ThreadState<D, C, MS, MP>) {
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

    pub fn abort_pending_flushes(&self) {
        let l = self.lock.lock().unwrap();
        let control_mut = unsafe { self.flush_control_mut(&l) };

        let flush_queue = mem::replace(&mut control_mut.flush_queue, VecDeque::with_capacity(0));
        for mut dwpt in flush_queue {
            self.documents_writer()
                .subtract_flushed_num_docs(dwpt.num_docs_in_ram);
            dwpt.abort();
            self.do_after_flush(dwpt, &l);
        }

        let blocked_flushes = mem::replace(&mut control_mut.blocked_flushes, vec![]);
        for mut blocked_flush in blocked_flushes {
            control_mut.flushing_writers.insert(
                blocked_flush.dwpt.segment_info.name.clone(),
                blocked_flush.bytes,
            );
            self.documents_writer()
                .subtract_flushed_num_docs(blocked_flush.dwpt.num_docs_in_ram);
            blocked_flush.dwpt.abort();
            self.do_after_flush(blocked_flush.dwpt, &l);
        }

        control_mut.flush_queue.clear();
        self.update_stall_state();
    }

    pub fn is_full_flush(&self) -> bool {
        self.full_flush.load(Ordering::Acquire)
    }

    fn assert_memory(&self, guard: &MutexGuard<FlushControlLock>) -> bool {
        let max_ram_mb = self.config.ram_buffer_size_mb();

        // We can only assert if we have always been flushing by RAM usage; otherwise
        // the assert will false trip if e.g. the flush-by-doc-count * doc size was
        // large enough to use far more RAM than the sudden change to IWC's maxRAMBufferSizeMB:
        if max_ram_mb != DISABLE_AUTO_FLUSH as f64 && !self.flush_by_ram_was_disabled.get() {
            self.max_configured_ram_buffer
                .set(max_ram_mb.max(self.max_configured_ram_buffer.get()));
            let ram = self.flush_bytes + self.active_bytes;
            let ram_buffer_bytes = (self.max_configured_ram_buffer.get() * 1024.0 * 1024.0) as u64;
            // take peakDelta into account - worst case is that all flushing, pending and blocked
            // DWPT had maxMem and the last doc had the peakDelta

            // 2 * ramBufferBytes -> before we stall we need to cross the 2xRAM Buffer border this
            // is still a valid limit (num_pending + num_flushing_dwpt() + num_blocked_flushes()) *
            // peak_delta) -> those are the total number of DWPT that are not active but not yet
            // fully flushed all of them could theoretically be taken out of the loop once they
            // crossed the RAM buffer and the last document was the peak delta (numDocsSinceStalled
            // * peakDelta) -> at any given time there could be n threads in flight that crossed the
            // stall control before we reached the limit and each of them could hold a peak document
            let expected = (2 * ram_buffer_bytes)
                + (self.num_pending.read() as u64
                    + self.num_flushing_dwpt(guard) as u64
                    + self.num_blocked_flushes(guard) as u64)
                    * self.peak_delta
                + self.num_docs_since_stalled as u64 * self.peak_delta;
            // the expected ram consumption is an upper bound at this point and not really
            // the expected consumption
            if self.peak_delta < (ram_buffer_bytes >> 1) {
                // if we are indexing with very low maxRamBuffer like 0.1MB memory can
                // easily overflow if we check out some DWPT based on docCount and have
                // several DWPT in flight indexing large documents (compared to the ram
                // buffer). This means that those DWPT and their threads will not hit
                // the stall control before asserting the memory which would in turn
                // fail. To prevent this we only assert if the the largest document seen
                // is smaller than the 1/2 of the maxRamBufferMB
                return ram <= expected;
            }
        } else {
            self.flush_by_ram_was_disabled.set(true);
        }
        true
    }

    pub fn after_flush(&self, dwpt: DocumentsWriterPerThread<D, C, MS, MP>) {
        let guard = self.lock.lock().unwrap();
        self.do_after_flush(dwpt, &guard);
    }

    fn do_after_flush(
        &self,
        dwpt: DocumentsWriterPerThread<D, C, MS, MP>,
        lg: &MutexGuard<FlushControlLock>,
    ) {
        let flush_control_mut = unsafe { self.flush_control_mut(lg) };

        debug_assert!(flush_control_mut
            .flushing_writers
            .contains_key(&dwpt.segment_info.name));
        let bytes = flush_control_mut
            .flushing_writers
            .remove(&dwpt.segment_info.name);
        flush_control_mut.flush_bytes -= bytes.unwrap();
        self.per_thread_pool().recycle(dwpt);

        self.update_stall_state();
        self.cond.notify_all();
    }

    fn update_stall_state(&self) -> bool {
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

    pub fn wait_for_flush(&self) -> Result<()> {
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

    fn num_flushing_dwpt(&self, _guard: &MutexGuard<FlushControlLock>) -> usize {
        self.flushing_writers.len()
    }

    fn num_blocked_flushes(&self, _guard: &MutexGuard<FlushControlLock>) -> usize {
        self.blocked_flushes.len()
    }

    pub fn any_stalled_threads(&self) -> bool {
        self.stall_control.stalled.read()
    }

    pub fn next_pending_flush(&self) -> Option<DocumentsWriterPerThread<D, C, MS, MP>> {
        let guard = self.lock.lock().unwrap();
        self.do_next_pending_flush(&guard)
    }

    fn do_next_pending_flush(
        &self,
        lg: &MutexGuard<FlushControlLock>,
    ) -> Option<DocumentsWriterPerThread<D, C, MS, MP>> {
        let num_pending: usize;
        let full_flush: bool;
        {
            let flush_control_mut = unsafe { self.flush_control_mut(lg) };

            if let Some(dwpt) = flush_control_mut.flush_queue.pop_front() {
                self.update_stall_state();
                return Some(dwpt);
            }
            full_flush = self.full_flush.load(Ordering::Acquire);
            num_pending = self.num_pending.read();
        }

        if num_pending > 0 && !full_flush {
            // don't check if we are doing a full flush
            let limit = self.per_thread_pool().active_thread_state_count();

            for i in 0..limit {
                let next = self.per_thread_pool().get_thread_state(i);
                if next.flush_pending() {
                    if let Some(dwpt) = self.try_checkout_for_flush(next.as_ref(), lg) {
                        return Some(dwpt);
                    }
                }
            }
        }
        None
    }

    // TODO, actually we didn't lock the state, this should be done by the caller
    pub fn obtain_and_lock(&self) -> Result<Arc<ThreadState<D, C, MS, MP>>> {
        let per_thread = self.per_thread_pool().get_and_lock()?;
        {
            let guard = match per_thread.lock.try_lock() {
                Ok(g) => g,
                Err(e) => {
                    bail!(IllegalState(format!(
                        "obtain_and_lock try lock per_thread.state failed: {:?}",
                        e
                    )));
                }
            };

            let per_thread_mut = per_thread.thread_state_mut(&guard);

            if per_thread_mut.inited()
                && per_thread_mut.dwpt().delete_queue.generation
                    != self.documents_writer().delete_queue.generation
            {
                // There is a flush-all in process and this DWPT is
                // now stale -- enroll it for flush and try for
                // another DWPT:
                self.add_flushable_state(per_thread_mut);
            }
        }
        // simply return the ThreadState even in a flush all case sine we already hold the lock
        Ok(per_thread)
    }

    pub fn mark_for_full_flush(&self) -> (u64, Arc<DocumentsWriterDeleteQueue<C>>) {
        let flushing_queue: Arc<DocumentsWriterDeleteQueue<C>>;
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
                self.documents_writer().delete_queue.generation + 1,
                seq_no + 1,
            ));

            flushing_queue = Arc::clone(&self.documents_writer().delete_queue);
            flushing_queue.max_seq_no.set(seq_no + 1);
            self.documents_writer().set_delete_queue(new_queue);
        }

        let limit = self.per_thread_pool().active_thread_state_count();
        for i in 0..limit {
            let next = self.per_thread_pool().get_thread_state(i);
            let guard = next.lock.lock().unwrap();
            let per_thread_mut = next.thread_state_mut(&guard);

            if !per_thread_mut.inited() {
                continue;
            }

            if per_thread_mut.dwpt().delete_queue.generation != flushing_queue.generation {
                // this one is already a new DWPT
                continue;
            }
            self.add_flushable_state(per_thread_mut);
        }

        {
            // make sure we move all DWPT that are where concurrently marked as
            // pending and moved to blocked are moved over to the flushQueue. There is
            // a chance that this happens since we marking DWPT for full flush without
            // blocking indexing.
            let l = self.lock.lock().unwrap();
            let control_mut = unsafe { self.flush_control_mut(&l) };
            control_mut.prune_blocked_queue(flushing_queue.generation);
            debug_assert!(self.assert_blocked_flushes());
            let full_flush_buffer = mem::replace(&mut control_mut.full_flush_buffer, vec![]);
            control_mut.flush_queue.extend(full_flush_buffer);
            self.update_stall_state();
        }
        debug_assert!(self.assert_active_delete_queue());
        (seq_no, flushing_queue)
    }

    fn assert_active_delete_queue(&self) -> bool {
        let thread_pool = self.per_thread_pool();
        let limit = thread_pool.active_thread_state_count();
        for i in 0..limit {
            let state = thread_pool.get_thread_state(i);
            let guard = state.lock.lock().unwrap();
            let per_thread_mut = state.thread_state_mut(&guard);
            assert!(
                !per_thread_mut.inited()
                    || per_thread_mut.dwpt().delete_queue.generation
                        == self.documents_writer().delete_queue.generation
            );
            assert!(
                !per_thread_mut.inited()
                    || per_thread_mut.dwpt().delete_queue.as_ref()
                        as *const DocumentsWriterDeleteQueue<C>
                        == self.documents_writer().delete_queue.as_ref()
                            as *const DocumentsWriterDeleteQueue<C>
            );
        }
        true
    }

    fn assert_blocked_flushes(&self) -> bool {
        for blocked_flush in &self.blocked_flushes {
            debug_assert_eq!(
                blocked_flush.dwpt.delete_queue.generation,
                self.documents_writer().delete_queue.generation
            );
        }
        true
    }

    pub fn finish_full_flush(&self) {
        debug_assert!(self.is_full_flush());
        debug_assert!(self.flush_queue.is_empty());
        debug_assert!(self.flushing_writers.is_empty());

        let l = self.lock.lock().unwrap();
        let control_mut = unsafe { self.flush_control_mut(&l) };
        if !self.blocked_flushes.is_empty() {
            debug_assert!(self.assert_blocked_flushes());
            let gen = self.documents_writer().delete_queue.generation;
            control_mut.prune_blocked_queue(gen);
            debug_assert!(self.blocked_flushes.is_empty());
        }
        self.full_flush.store(false, Ordering::Release);
        self.update_stall_state();
    }

    pub fn abort_full_flushes(&self) {
        self.abort_pending_flushes();
        self.full_flush.store(false, Ordering::Release);
    }

    fn add_flushable_state(&self, per_therad: &mut ThreadState<D, C, MS, MP>) {
        debug!(
            "DWFC: add_flushable_state for {}",
            &per_therad.dwpt().segment_info.name
        );
        debug_assert!(per_therad.inited());
        debug_assert!(self.is_full_flush());
        if per_therad.dwpt().num_docs_in_ram > 0 {
            let l = self.lock.lock().unwrap();
            let flush_control_mut = unsafe { self.flush_control_mut(&l) };
            if !per_therad.flush_pending() {
                flush_control_mut.set_flush_pending(per_therad, &l);
            }
            let flushing_dwpt =
                flush_control_mut.internal_try_checkout_for_flush_no_lock(per_therad);
            debug_assert!(flushing_dwpt.is_some());
            flush_control_mut
                .full_flush_buffer
                .push(flushing_dwpt.unwrap());
        } else {
            // make this state inactive
            self.per_thread_pool().reset(per_therad);
        }
    }

    /// Prunes the blockedQueue by removing all DWPT that are
    /// associated with the given flush queue.
    fn prune_blocked_queue(&mut self, flushing_generation: u64) {
        let pruned = self
            .blocked_flushes
            .drain_filter(|bf| bf.dwpt.delete_queue.generation == flushing_generation);
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

    pub fn set_flush_pending(
        &mut self,
        per_thread: &ThreadState<D, C, MS, MP>,
        guard: &MutexGuard<FlushControlLock>,
    ) {
        if per_thread.dwpt().num_docs_in_ram > 0 {
            // write access synced
            per_thread.flush_pending.store(true, Ordering::Release);
            let bytes = per_thread.bytes_used;
            self.flush_bytes += bytes;
            self.active_bytes -= bytes;
            self.num_pending.update(|v| *v += 1);
            debug_assert!(self.assert_memory(guard));
        }
        // don't assert on numDocs since we could hit an abort excp.
        // while selecting that dwpt for flushing
    }

    fn try_checkout_for_flush(
        &self,
        per_thread: &ThreadState<D, C, MS, MP>,
        lock: &MutexGuard<FlushControlLock>,
    ) -> Option<DocumentsWriterPerThread<D, C, MS, MP>> {
        if per_thread.flush_pending() {
            let control_mut = unsafe { self.flush_control_mut(&lock) };
            control_mut.internal_try_checkout_for_flush(per_thread)
        } else {
            None
        }
    }

    fn checkout_and_block(&mut self, per_thread: &mut ThreadState<D, C, MS, MP>) {
        debug_assert!(per_thread.flush_pending());
        debug_assert!(self.is_full_flush());
        let bytes = per_thread.bytes_used;
        let dwpt = self.per_thread_pool().reset(per_thread);
        debug_assert!(dwpt.is_some());
        self.num_pending.update(|v| *v -= 1);
        self.blocked_flushes
            .push(BlockedFlush::new(dwpt.unwrap(), bytes));
    }

    fn internal_try_checkout_for_flush(
        &mut self,
        per_thread: &ThreadState<D, C, MS, MP>,
    ) -> Option<DocumentsWriterPerThread<D, C, MS, MP>> {
        if let Ok(lg) = per_thread.lock.try_lock() {
            let thread_state_mut = per_thread.thread_state_mut(&lg);
            self.internal_try_checkout_for_flush_no_lock(thread_state_mut)
        } else {
            self.update_stall_state();
            None
        }
    }

    fn internal_try_checkout_for_flush_no_lock(
        &mut self,
        per_thread: &mut ThreadState<D, C, MS, MP>,
    ) -> Option<DocumentsWriterPerThread<D, C, MS, MP>> {
        debug_assert!(per_thread.flush_pending());
        // We are pending so all memory is already moved to flushBytes
        let res = if per_thread.inited() {
            let bytes = per_thread.bytes_used;

            let dwpt = self.per_thread_pool().reset(per_thread).unwrap();
            debug_assert!(!self.flushing_writers.contains_key(&dwpt.segment_info.name));

            self.flushing_writers
                .insert(dwpt.segment_info.name.clone(), bytes);
            self.num_pending.update(|v| *v -= 1);
            Some(dwpt)
        } else {
            None
        };
        self.update_stall_state();

        res
    }

    /// This method will block if too many DWPT are currently flushing and no
    /// checked out DWPT are available
    pub fn wait_if_stalled(&self) -> Result<()> {
        self.stall_control.wait_if_stalled()
    }

    pub fn set_closed(&mut self) {
        // set by DW to signal that we should not release new DWPT after close
        let _l = self.lock.lock().unwrap();
        self.closed = true;
    }
}

struct BlockedFlush<D: Directory + 'static, C: Codec, MS: MergeScheduler, MP: MergePolicy> {
    dwpt: DocumentsWriterPerThread<D, C, MS, MP>,
    bytes: u64,
}

impl<D, C, MS, MP> BlockedFlush<D, C, MS, MP>
where
    D: Directory + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    fn new(dwpt: DocumentsWriterPerThread<D, C, MS, MP>, bytes: u64) -> Self {
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
    lock: Mutex<()>,
    cond: Condvar,
    stalled: Volatile<bool>,
    num_waiting: u32,
    // only with assert
    waiting: HashMap<ThreadId, bool>,
    // only with assert
}

impl DocumentsWriterStallControl {
    fn new() -> Self {
        DocumentsWriterStallControl {
            lock: Mutex::new(()),
            cond: Condvar::new(),
            stalled: Volatile::new(false),
            num_waiting: 0,
            waiting: HashMap::new(),
        }
    }

    // hold mutex to make sure this operation is safe as mush as possible
    unsafe fn stall_control_mut(&self, _l: &MutexGuard<()>) -> &mut DocumentsWriterStallControl {
        let sc = self as *const DocumentsWriterStallControl as *mut DocumentsWriterStallControl;
        &mut *sc
    }
    ///
    // Update the stalled flag status. This method will set the stalled flag to
    // <code>true</code> iff the number of flushing
    // {@link DocumentsWriterPerThread} is greater than the number of active
    // {@link DocumentsWriterPerThread}. Otherwise it will reset the
    // {@link DocumentsWriterStallControl} to healthy and release all threads
    // waiting on {@link #waitIfStalled()}
    //
    fn update_stalled(&self, stalled: bool) {
        let _l = self.lock.lock().unwrap();
        if self.stalled.read() != stalled {
            self.stalled.write(stalled);
            self.cond.notify_all();
        }
    }

    /// Blocks if documents writing is currently in a stalled state.
    fn wait_if_stalled(&self) -> Result<()> {
        if self.stalled.read() {
            let l = self.lock.lock().unwrap();
            let stall_control_mut = unsafe { self.stall_control_mut(&l) };
            if self.stalled.read() {
                // don't loop here, higher level logic will re-stall!
                stall_control_mut.inc_waiters();
                // Defensive, in case we have a concurrency bug that fails to
                // .notify/All our thread: just wait for up to 1 second here,
                // and let caller re-stall if it's still needed:
                self.cond.wait_timeout(l, Duration::new(1, 0))?;
                stall_control_mut.decr_waiters();
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

    #[allow(dead_code)]
    fn has_blocked(&self) -> bool {
        let _l = self.lock.lock().unwrap();
        self.num_waiting > 0
    }

    // for tests
    #[allow(dead_code)]
    fn is_healthy(&self) -> bool {
        // volatile read!
        !self.stalled.read()
    }

    #[allow(dead_code)]
    fn is_thread_queued(&self, t: &ThreadId) -> bool {
        let _l = self.lock.lock().unwrap();
        self.waiting.contains_key(t)
    }

    #[allow(dead_code)]
    fn was_stalled(&self) -> bool {
        let _l = self.lock.lock().unwrap();
        self.stalled.read()
    }
}
