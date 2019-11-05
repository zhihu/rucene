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
    DocumentsWriter, DocumentsWriterDeleteQueue, DocumentsWriterPerThread,
    DocumentsWriterPerThreadPool, FlushByCountsPolicy, FlushPolicy, ThreadState,
};
use core::util::external::Volatile;
use error::Result;

use core::store::directory::Directory;
use std::collections::{HashMap, VecDeque};
use std::mem;
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

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
pub struct DocumentsWriterFlushControl<
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
> {
    lock: Arc<Mutex<FlushControlLock>>,
    cond: Condvar,
    num_pending: Volatile<usize>,
    // only with assert
    flush_deletes: AtomicBool,
    full_flush: AtomicBool,
    pub flush_queue: VecDeque<DocumentsWriterPerThread<D, C, MS, MP>>,
    // key is segment_name of the DocumentsWriterPerThread
    flushing_writers: HashMap<String, u64>,
    per_thread_pool: *mut DocumentsWriterPerThreadPool<D, C, MS, MP>,
    flush_policy: Arc<FlushByCountsPolicy<C, MS, MP>>,
    closed: bool,
    documents_writer: *const DocumentsWriter<D, C, MS, MP>,
    inited: bool,
}

pub struct FlushControlLock;

impl<D: Directory + Send + Sync + 'static, C: Codec, MS: MergeScheduler, MP: MergePolicy>
    DocumentsWriterFlushControl<D, C, MS, MP>
{
    pub fn new(flush_policy: Arc<FlushByCountsPolicy<C, MS, MP>>) -> Self {
        DocumentsWriterFlushControl {
            lock: Arc::new(Mutex::new(FlushControlLock)),
            cond: Condvar::new(),
            num_pending: Volatile::new(0),
            flush_deletes: AtomicBool::new(false),
            full_flush: AtomicBool::new(false),
            flush_queue: VecDeque::new(),
            flushing_writers: HashMap::new(),
            per_thread_pool: ptr::null_mut(),
            flush_policy,
            closed: false,
            documents_writer: ptr::null_mut(),
            inited: false,
        }
    }

    pub fn init(&mut self, document_writer: &DocumentsWriter<D, C, MS, MP>) {
        debug_assert!(!self.inited);
        self.per_thread_pool = &document_writer.per_thread_pool
            as *const DocumentsWriterPerThreadPool<D, C, MS, MP>
            as *mut DocumentsWriterPerThreadPool<D, C, MS, MP>;
        self.documents_writer = document_writer;
        self.inited = true;
    }

    // TODO: maybe this action is not safe
    pub fn documents_writer(&self) -> &DocumentsWriter<D, C, MS, MP> {
        debug_assert!(self.inited);
        unsafe { &*self.documents_writer }
    }

    pub fn per_thread_pool(&self) -> &DocumentsWriterPerThreadPool<D, C, MS, MP> {
        debug_assert!(self.inited);
        unsafe { &*self.per_thread_pool }
    }

    #[allow(clippy::mut_from_ref)]
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
    ) -> Result<()> {
        let l = self.lock.lock()?;
        let flush_control_mut = unsafe { self.flush_control_mut(&l) };
        flush_control_mut.process_after_document(per_thread, is_update, &l);

        Ok(())
    }

    fn process_after_document(
        &mut self,
        per_thread: &mut ThreadState<D, C, MS, MP>,
        is_update: bool,
        lg: &MutexGuard<FlushControlLock>,
    ) {
        if !per_thread.flush_pending() {
            unsafe {
                let writer = self as *mut DocumentsWriterFlushControl<D, C, MS, MP>;
                if is_update {
                    self.flush_policy.on_update(&mut *writer, lg, per_thread);
                } else {
                    self.flush_policy.on_insert(&mut *writer, lg, per_thread);
                }
            }
        }
    }

    pub fn do_on_delete(&mut self) {
        // pass None this is a global delete no update
        unsafe {
            let writer = self as *mut DocumentsWriterFlushControl<D, C, MS, MP>;
            self.flush_policy.on_delete(&mut *writer, None);
        }
    }

    pub fn do_on_abort(
        &self,
        state: &mut ThreadState<D, C, MS, MP>,
    ) -> Option<DocumentsWriterPerThread<D, C, MS, MP>> {
        let _l = self.lock.lock().unwrap();
        // Take it out of the loop this DWPT is stale
        let dwpt = self.per_thread_pool().reset(state);

        dwpt
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

        control_mut.flush_queue.clear();
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
        flush_control_mut
            .flushing_writers
            .remove(&dwpt.segment_info.name);
        self.per_thread_pool().recycle(dwpt);

        self.cond.notify_all();
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

    pub fn set_flush_pending(
        &mut self,
        per_thread: &ThreadState<D, C, MS, MP>,
        _guard: &MutexGuard<FlushControlLock>,
    ) {
        assert!(!per_thread.flush_pending());
        if per_thread.dwpt().num_docs_in_ram > 0 {
            // write access synced
            per_thread.flush_pending.store(true, Ordering::Release);
            self.num_pending.update(|v| *v += 1);
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
            if let Ok(lg) = per_thread.lock.try_lock() {
                let thread_state_mut = per_thread.thread_state_mut(&lg);
                control_mut.internal_try_checkout_for_flush_no_lock(thread_state_mut)
            } else {
                None
            }
        } else {
            None
        }
    }

    fn internal_try_checkout_for_flush_no_lock(
        &mut self,
        per_thread: &mut ThreadState<D, C, MS, MP>,
    ) -> Option<DocumentsWriterPerThread<D, C, MS, MP>> {
        debug_assert!(per_thread.flush_pending());
        // We are pending so all memory is already moved to flushBytes
        if per_thread.inited() {
            let dwpt = self.per_thread_pool().reset(per_thread).unwrap();
            debug_assert!(!self.flushing_writers.contains_key(&dwpt.segment_info.name));

            self.flushing_writers
                .insert(dwpt.segment_info.name.clone(), 0);
            self.num_pending.update(|v| *v -= 1);
            Some(dwpt)
        } else {
            None
        }
    }

    pub fn set_closed(&mut self) {
        // set by DW to signal that we should not release new DWPT after close
        let _l = self.lock.lock().unwrap();
        self.closed = true;
    }

    pub fn is_full_flush(&self) -> bool {
        self.full_flush.load(Ordering::Acquire)
    }

    pub fn mark_for_full_flush(&self) -> (u64, Arc<DocumentsWriterDeleteQueue<C>>) {
        let delete_queue: Arc<DocumentsWriterDeleteQueue<C>>;
        let seq_no: u64;
        {
            let _l = self.lock.lock().unwrap();
            debug_assert!(!self.is_full_flush());
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

            delete_queue = Arc::clone(&self.documents_writer().delete_queue);
            delete_queue.max_seq_no.set(seq_no + 1);
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

            if per_thread_mut.dwpt().delete_queue.generation != delete_queue.generation {
                // this one is already a new DWPT
                continue;
            }
            self.add_flushable_state(per_thread_mut);
        }

        debug_assert!(self.assert_active_delete_queue());

        (seq_no, delete_queue)
    }

    pub fn add_flushable_state(&self, per_therad: &mut ThreadState<D, C, MS, MP>) {
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
                .flush_queue
                .push_back(flushing_dwpt.unwrap());
        } else {
            // make this state inactive
            self.per_thread_pool().reset(per_therad);
        }
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

    pub fn finish_full_flush(&self) {
        debug_assert!(self.is_full_flush());
        self.full_flush.store(false, Ordering::Release);
    }

    pub fn abort_full_flushes(&self) {
        self.abort_pending_flushes();
        self.full_flush.store(false, Ordering::Release);
    }
}
