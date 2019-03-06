use core::index::doc_writer_delete_queue::DocumentsWriterDeleteQueue;
use core::index::doc_writer_flush_queue::{DocumentsWriterFlushQueue, SegmentFlushTicket};
use core::index::flush_control::DocumentsWriterFlushControl;
use core::index::flush_policy::{FlushByRamOrCountsPolicy, FlushPolicy};
use core::index::index_writer::IndexWriter;
use core::index::index_writer_config::IndexWriterConfig;
use core::index::thread_doc_writer::{DocumentsWriterPerThread, LockedThreadState};
use core::index::thread_doc_writer::{DocumentsWriterPerThreadPool, ThreadState};
use core::index::Fieldable;
use core::index::SegmentInfo;
use core::index::Term;
use core::search::Query;
use core::store::DirectoryRc;
use error::Result;

use crossbeam::queue::MsQueue;

use std::cmp::max;
use std::collections::HashSet;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;

///
// This class accepts multiple added documents and directly
// writes segment files.
//
// Each added document is passed to the indexing chain,
// which in turn processes the document into the different
// codec formats.  Some formats write bytes to files
// immediately, e.g. stored fields and term vectors, while
// others are buffered by the indexing chain and written
// only on flush.
//
// Once we have used our allowed RAM buffer, or the number
// of added docs is large enough (in the case we are
// flushing by doc count instead of RAM usage), we create a
// real segment and flush it to the Directory.
//
// Threads:
//
// Multiple threads are allowed into addDocument at once.
// There is an initial synchronized call to getThreadState
// which allocates a ThreadState for this thread.  The same
// thread will get the same ThreadState over time (thread
// affinity) so that if there are consistent patterns (for
// example each thread is indexing a different content
// source) then we make better use of RAM.  Then
// processDocument is called on that ThreadState without
// synchronization (most of the "heavy lifting" is in this
// call).  Finally the synchronized "finishDocument" is
// called to flush changes to the directory.
//
// When flush is called by IndexWriter we forcefully idle
// all threads and flush only once they are all idle.  This
// means you can call flush with a given thread even while
// other threads are actively adding/deleting documents.
//
//
// Exceptions:
//
// Because this class directly updates in-memory posting
// lists, and flushes stored fields and term vectors
// directly to files in the directory, there are certain
// limited times when an exception can corrupt this state.
// For example, a disk full while flushing stored fields
// leaves this file in a corrupt state.  Or, an OOM
// exception while appending to the in-memory posting lists
// can corrupt that posting list.  We call such exceptions
// "aborting exceptions".  In these cases we must call
// abort() to discard all docs added since the last flush.
//
// All other exceptions ("non-aborting exceptions") can
// still partially update the index structures.  These
// updates are consistent, but, they represent only a part
// of the document seen up until the exception was hit.
// When this happens, we immediately mark the document as
// deleted so that the document is always atomically ("all
// or none") added to the index.
//
pub struct DocumentsWriter {
    lock: Arc<Mutex<()>>,
    directory_orig: DirectoryRc,
    directory: DirectoryRc,
    closed: bool,
    num_docs_in_ram: AtomicU32,
    // TODO: cut over to BytesRefHash in BufferedDeletes
    pub delete_queue: Arc<DocumentsWriterDeleteQueue>,
    ticket_queue: DocumentsWriterFlushQueue,

    // we preserve changes during a full flush since IW might not checkout
    // before we release all changes. NTR Readers otherwise suddenly return
    // true from is_current while there are actually changes currently
    // committed. See also self.any_change() & self.flush_all_threads.
    pending_changes_in_current_full_flush: AtomicBool,
    pub per_thread_pool: DocumentsWriterPerThreadPool,
    pub flush_policy: Arc<FlushPolicy>,
    flush_control: DocumentsWriterFlushControl,
    config: Arc<IndexWriterConfig>,
    writer: *mut IndexWriter,
    pub events: MsQueue<WriterEvent>,
    pub last_seq_no: u64,
    inited: bool,
    // must init flush_control after new
}

impl DocumentsWriter {
    pub fn new(
        config: Arc<IndexWriterConfig>,
        directory_orig: DirectoryRc,
        directory: DirectoryRc,
    ) -> Self {
        let flush_policy: Arc<FlushPolicy> =
            Arc::new(FlushByRamOrCountsPolicy::new(Arc::clone(&config)));
        let flush_control =
            DocumentsWriterFlushControl::new(Arc::clone(&config), Arc::clone(&flush_policy));
        DocumentsWriter {
            lock: Arc::new(Mutex::new(())),
            directory_orig,
            directory,
            closed: false,
            num_docs_in_ram: AtomicU32::new(0),
            delete_queue: Arc::new(DocumentsWriterDeleteQueue::default()),
            ticket_queue: DocumentsWriterFlushQueue::new(),
            pending_changes_in_current_full_flush: AtomicBool::new(false),
            per_thread_pool: DocumentsWriterPerThreadPool::new(),
            flush_policy,
            flush_control,
            config,
            writer: ptr::null_mut(),
            events: MsQueue::new(),
            last_seq_no: 0,
            inited: false,
        }
    }

    pub fn init(&mut self, writer: &IndexWriter) {
        self.writer = writer as *const IndexWriter as *mut IndexWriter;
        unsafe {
            let flush_control = &mut self.flush_control as *mut DocumentsWriterFlushControl;
            (*flush_control).init(self, &writer.buffered_updates_stream);
        }
        self.inited = true;
    }

    fn writer(&self) -> &mut IndexWriter {
        unsafe { &mut *self.writer }
    }

    pub fn update_documents(
        &mut self,
        docs: Vec<Vec<Box<Fieldable>>>,
        // analyzer: Analyzer,
        del_term: Option<Term>,
    ) -> Result<(u64, bool)> {
        debug_assert!(self.inited);
        let has_events = self.pre_update()?;

        let mut per_thread = self.flush_control.obtain_and_lock()?;
        let (seq_no, flush_dwpt) = self.do_update_documents(&mut per_thread, docs, del_term)?;
        self.per_thread_pool.release(per_thread);

        let has_event = self.post_update(flush_dwpt, has_events)?;
        Ok((seq_no, has_event))
    }

    fn do_update_documents(
        &mut self,
        per_thread: &mut LockedThreadState,
        docs: Vec<Vec<Box<Fieldable>>>,
        // analyzer: Analyzer,
        del_term: Option<Term>,
    ) -> Result<(u64, Option<DocumentsWriterPerThread>)> {
        let is_update = del_term.is_some();
        let mut guard = match per_thread.state.try_lock() {
            Ok(g) => g,
            Err(e) => {
                bail!(
                    "update document try obtain per_thread.state failed by: {:?}",
                    e
                );
            }
        };

        // This must happen after we've pulled the ThreadState because IW.close
        // waits for all ThreadStates to be released:
        self.ensure_open()?;
        self.ensure_inited(&mut guard)?;
        debug_assert!(guard.inited());
        let dwpt_num_docs = guard.dwpt().num_docs_in_ram;

        let res = guard.dwpt_mut().update_documents(docs, del_term);
        let num_docs_in_ram = if res.is_err() {
            // TODO, we should only deal with AbortException here instead of
            // all errors
            let mut dwpt = self.flush_control.do_on_abort(&mut guard);
            dwpt.as_mut().unwrap().abort();
            dwpt.as_ref().unwrap().num_docs_in_ram
        } else {
            guard.dwpt().num_docs_in_ram
        };

        // We don't know whether the document actually
        // counted as being indexed, so we must subtract here to
        // accumulate our separate counter:
        self.num_docs_in_ram
            .fetch_add(num_docs_in_ram - dwpt_num_docs, Ordering::AcqRel);

        let seq_no = match res {
            Ok(n) => n,
            Err(e) => {
                return Err(e);
            }
        };

        let flush_dwpt = self.flush_control.do_after_document(&mut guard, is_update)?;

        debug_assert!(seq_no > guard.last_seq_no());
        guard.set_last_seq_no(seq_no);
        Ok((seq_no, flush_dwpt))
    }

    pub fn update_document(
        &mut self,
        doc: Vec<Box<Fieldable>>,
        // analyzer: Analyzer,
        del_term: Option<Term>,
    ) -> Result<(u64, bool)> {
        debug_assert!(self.inited);
        let mut has_event = self.pre_update()?;

        let mut per_thread = self.flush_control.obtain_and_lock()?;
        let (seq_no, flush_dwpt) = self.do_update_document(&mut per_thread, doc, del_term)?;
        self.per_thread_pool.release(per_thread);

        has_event = self.post_update(flush_dwpt, has_event)?;

        Ok((seq_no, has_event))
    }

    fn do_update_document(
        &mut self,
        per_thread: &mut LockedThreadState,
        doc: Vec<Box<Fieldable>>,
        // analyzer: Analyzer,
        del_term: Option<Term>,
    ) -> Result<(u64, Option<DocumentsWriterPerThread>)> {
        let is_update = del_term.is_some();
        let mut guard = match per_thread.state.try_lock() {
            Ok(g) => g,
            Err(e) => {
                bail!(
                    "update document try obtain per_thread.state failed by: {:?}",
                    e
                );
            }
        };

        // This must happen after we've pulled the ThreadState because IW.close
        // waits for all ThreadStates to be released:
        self.ensure_open()?;
        self.ensure_inited(&mut guard)?;
        debug_assert!(guard.inited());

        let dwpt_num_docs = guard.dwpt().num_docs_in_ram;
        let res = guard.dwpt_mut().update_document(doc, del_term);
        let num_docs_in_ram = if res.is_err() {
            // TODO, we should only deal with AbortException here instead of
            // all errors
            let mut dwpt = self.flush_control.do_on_abort(&mut guard);
            dwpt.as_mut().unwrap().abort();
            dwpt.as_ref().unwrap().num_docs_in_ram
        } else {
            guard.dwpt().num_docs_in_ram
        };

        // We don't know whether the document actually
        // counted as being indexed, so we must subtract here to
        // accumulate our separate counter:
        self.num_docs_in_ram
            .fetch_add(num_docs_in_ram - dwpt_num_docs, Ordering::AcqRel);

        let seq_no = match res {
            Ok(n) => n,
            Err(e) => {
                return Err(e);
            }
        };

        let flush_dwpt = self.flush_control.do_after_document(&mut guard, is_update)?;

        debug_assert!(seq_no > guard.last_seq_no());
        guard.set_last_seq_no(seq_no);
        Ok((seq_no, flush_dwpt))
    }

    fn pre_update(&mut self) -> Result<bool> {
        self.ensure_open()?;
        let mut has_events = false;
        if self.flush_control.any_stalled_threads() || self.flush_control.num_queued_flushes() > 0 {
            // Help out flushing any queued DWPTs so we can un-stall:
            loop {
                // Try pick up pending threads here if possible
                loop {
                    if let Some(dwpt) = self.flush_control.next_pending_flush() {
                        // Don't push the delete here since the update could fail!
                        has_events |= self.do_flush(dwpt)?;
                    } else {
                        break;
                    }
                }
                self.flush_control.wait_if_stalled()?; // block is stalled
                if self.flush_control.num_queued_flushes() == 0 {
                    break;
                }
            }
        }
        Ok(has_events)
    }

    fn post_update(
        &mut self,
        flushing_dwpt: Option<DocumentsWriterPerThread>,
        mut has_events: bool,
    ) -> Result<bool> {
        has_events |= self.apply_all_deletes_local()?;
        if let Some(dwpt) = flushing_dwpt {
            has_events |= self.do_flush(dwpt)?;
        } else {
            if let Some(mut next_pending_flush) = self.flush_control.next_pending_flush() {
                has_events |= self.do_flush(next_pending_flush)?;
            }
        }

        Ok(has_events)
    }

    fn ensure_inited(&self, state: &mut ThreadState) -> Result<()> {
        if state.dwpt.is_none() {
            let writer = self.writer();
            let segment_name = writer.new_segment_name();
            let pending_num_docs = Arc::clone(&writer.pending_num_docs);
            let dwpt: DocumentsWriterPerThread = DocumentsWriterPerThread::new(
                writer,
                segment_name,
                Arc::clone(&self.directory_orig),
                Arc::clone(&self.directory),
                Arc::clone(&self.config),
                Arc::clone(&self.delete_queue),
                pending_num_docs,
            )?;

            state.dwpt = Some(dwpt);
            state.dwpt.as_mut().unwrap().init(
                &mut writer.global_field_numbers,
                Arc::clone(&writer.config.codec),
            );
        }
        Ok(())
    }

    pub fn delete_queries(&mut self, queries: Vec<Arc<Query>>) -> Result<(u64, bool)> {
        debug_assert!(self.inited);
        // TODO why is this synchronized?
        let lock = Arc::clone(&self.lock);
        let _l = lock.lock()?;
        let seq_no = self.delete_queue.add_delete_queries(queries)?;
        self.flush_control.do_on_delete();

        let applyed = self.apply_all_deletes_local()?;
        self.last_seq_no = max(self.last_seq_no, seq_no);
        Ok((seq_no, applyed))
    }

    pub fn delete_terms(&mut self, terms: Vec<Term>) -> Result<(u64, bool)> {
        debug_assert!(self.inited);
        // TODO why is this synchronized?
        let lock = Arc::clone(&self.lock);
        let _l = lock.lock()?;
        let seq_no = self.delete_queue.add_delete_terms(terms)?;
        self.flush_control.do_on_delete();

        let applyed = self.apply_all_deletes_local()?;
        self.last_seq_no = max(self.last_seq_no, seq_no);
        Ok((seq_no, applyed))
    }

    pub fn num_docs(&self) -> u32 {
        self.num_docs_in_ram.load(Ordering::Acquire)
    }

    fn apply_all_deletes_local(&mut self) -> Result<bool> {
        if self.flush_control.get_and_reset_apply_all_deletes() {
            if !self.flush_control.is_full_flush() {
                self.ticket_queue.add_deletes(&self.delete_queue)?;
            }
            self.put_event(WriterEvent::ApplyDeletes);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn put_event(&mut self, event: WriterEvent) {
        self.events.push(event);
    }

    pub fn purge_buffer(&mut self, writer: &mut IndexWriter, forced: bool) -> Result<u32> {
        if forced {
            self.ticket_queue.force_purge(writer)
        } else {
            self.ticket_queue.try_purge(writer)
        }
    }

    fn ensure_open(&self) -> Result<()> {
        if self.closed {
            bail!("this IndexWriter is closed");
        }
        Ok(())
    }

    /// Called if we hit an exception at a bad time (when
    /// updating the index files) and must discard all
    /// currently buffered docs.  This resets our state,
    /// discarding any docs added since last flush.
    pub fn abort(&mut self) -> Result<()> {
        let lock = Arc::clone(&self.lock);
        let _l = lock.lock()?;
        self.delete_queue.clear()?;
        debug!("DW: start to abort");

        for i in 0..self.per_thread_pool.active_thread_state_count() {
            let per_thread = Arc::clone(&self.per_thread_pool.thread_states[i]);
            let mut guard = per_thread.lock()?;
            self.abort_thread_state(&mut guard);
        }
        self.flush_control.abort_pending_flushes();
        self.flush_control.wait_for_flush()?;
        debug!("DW: done abort successed!");
        Ok(())
    }

    /// Return how many documents were aborted
    /// _l is IndexWriter.full_flush_lock guard
    pub fn lock_and_abort_all(&mut self, _l: &MutexGuard<()>) -> Result<u32> {
        debug!("DW - lock_and_abort_all");

        let mut aborted_doc_count = 0;
        self.delete_queue.clear()?;
        self.per_thread_pool.set_abort();
        for i in 0..self.per_thread_pool.thread_states.len() {
            let per_thread = Arc::clone(&self.per_thread_pool.thread_states[i]);
            let mut gurad = per_thread.lock().unwrap();
            aborted_doc_count += self.abort_thread_state(&mut gurad);
        }
        self.delete_queue.clear()?;

        // jump over any possible in flight ops:
        let jump = self.per_thread_pool.active_thread_state_count() + 1;
        self.delete_queue.skip_sequence_number(jump as u64);

        self.flush_control.abort_pending_flushes();
        self.flush_control.wait_for_flush()?;
        Ok(aborted_doc_count)
    }

    /// Returns how many documents were aborted.
    fn abort_thread_state(&mut self, per_thread: &mut ThreadState) -> u32 {
        if per_thread.inited() {
            let aborted_doc_count = per_thread.dwpt().num_docs_in_ram;
            self.subtract_flushed_num_docs(aborted_doc_count);
            per_thread.dwpt_mut().abort();
            self.flush_control.do_on_abort(per_thread);
            aborted_doc_count
        } else {
            self.flush_control.do_on_abort(per_thread);
            0
        }
    }

    fn do_flush(&mut self, flushing_dwpt: DocumentsWriterPerThread) -> Result<bool> {
        let mut dwpt = flushing_dwpt;
        loop {
            let res = self.flush_dwpt(&mut dwpt);
            self.flush_control.do_after_flush(dwpt);
            res?;

            match self.flush_control.next_pending_flush() {
                Some(writer) => {
                    dwpt = writer;
                }
                None => {
                    break;
                }
            }
        }
        self.put_event(WriterEvent::MergePending);

        // If deletes alone are consuming > 1/2 our RAM
        // buffer, force them all to apply now. This is to
        // prevent too-frequent flushing of a long tail of
        // tiny segments:
        if self.config.flush_on_ram()
            && self.flush_control.delete_bytes_used() > self.config.ram_buffer_size() / 2
        {
            if !self.apply_all_deletes_local()? {
                debug!("DW: force apply deletes");
                self.put_event(WriterEvent::ApplyDeletes);
            }
        }

        Ok(true)
    }

    fn flush_dwpt(&mut self, dwpt: &mut DocumentsWriterPerThread) -> Result<()> {
        // Since with DWPT the flush process is concurrent and several DWPT
        // could flush at the same time we must maintain the order of the
        // flushes before we can apply the flushed segment and the frozen global
        // deletes it is buffering. The reason for this is that the global
        // deletes mark a certain point in time where we took a DWPT out of
        // rotation and freeze the global deletes.
        //
        // Example: A flush 'A' starts and freezes the global deletes, then
        // flush 'B' starts and freezes all deletes occurred since 'A' has
        // started. if 'B' finishes before 'A' we need to wait until 'A' is done
        // otherwise the deletes frozen by 'B' are not applied to 'A' and we
        // might miss to deletes documents in 'A'.

        // Each flush is assigned a ticket in the order they acquire the
        // ticket_queue lock

        match dwpt.prepare_flush() {
            Ok(global_deletes) => {
                let mut ticket = SegmentFlushTicket::new(global_deletes);
                let flushing_docs_in_ram = dwpt.num_docs_in_ram;
                let res = dwpt.flush();
                let success = res.is_ok();
                match res {
                    Ok(seg) => {
                        ticket.set_segment(seg);
                    }
                    Err(e) => {
                        ticket.set_failed();
                    }
                };

                self.subtract_flushed_num_docs(flushing_docs_in_ram);
                if !dwpt.files_to_delete.is_empty() {
                    self.put_event(WriterEvent::DeleteNewFiles(dwpt.files_to_delete.clone()));
                }
                if !success {
                    self.put_event(WriterEvent::FlushFailed(dwpt.segment_info.clone()));
                }

                self.ticket_queue.add_flush_ticket(ticket);
            }
            Err(e) => {}
        }

        Ok(())
    }

    pub fn any_changes(&self) -> bool {
        // changes are either in a DWPT or in the deleteQueue.
        // yet if we currently flush deletes and / or dwpt there
        // could be a window where all changes are in the ticket queue
        // before they are published to the IW. ie we need to check if the
        // ticket queue has any tickets.
        self.num_docs_in_ram.load(Ordering::Acquire) > 0 || self.delete_queue.any_changes()
            || self.ticket_queue.has_tickets()
            || self.pending_changes_in_current_full_flush
                .load(Ordering::Acquire)
    }

    pub fn subtract_flushed_num_docs(&self, num_flushed: u32) {
        debug_assert!(self.num_docs_in_ram.load(Ordering::Acquire) >= num_flushed);
        self.num_docs_in_ram
            .fetch_sub(num_flushed, Ordering::AcqRel);
    }

    /// FlushAllThreads is synced by IW fullFlushLock. Flushing all threads is a
    /// two stage operation; the caller must ensure (in try/finally) that finishFlush
    /// is called after this method, to release the flush lock in DWFlushControl
    pub fn flush_all_threads(&mut self) -> Result<(bool, u64)> {
        debug!("DW: start full flush");

        let (seq_no, flushing_queue) = {
            let lock = Arc::clone(&self.lock);
            let _l = lock.lock()?;
            self.pending_changes_in_current_full_flush
                .store(self.any_changes(), Ordering::Release);
            self.flush_control.mark_for_full_flush()
        };

        let mut anything_flushed = false;
        loop {
            // Help out with flushing:
            if let Some(flushing_dwpt) = self.flush_control.next_pending_flush() {
                anything_flushed |= self.do_flush(flushing_dwpt)?;
            } else {
                break;
            }
        }
        // If a concurrent flush is still in flight wait for it
        self.flush_control.wait_for_flush()?;
        if !anything_flushed && flushing_queue.any_changes() {
            // apply deletes if we did not flush any document
            debug!(
                "DW - {:?}: flush naked frozen global deletes",
                thread::current().name()
            );

            self.ticket_queue.add_deletes(flushing_queue.as_ref())?;
        }
        self.ticket_queue.force_purge(unsafe { &mut *self.writer })?;
        debug_assert!(!flushing_queue.any_changes() && !self.ticket_queue.has_tickets());

        Ok((anything_flushed, seq_no))
    }

    pub fn finish_full_flush(&mut self, success: bool) {
        debug!(
            "DW - {:?} finish full flush, success={}",
            thread::current().name(),
            success
        );
        if success {
            self.flush_control.finish_full_flush();
        } else {
            self.flush_control.abort_full_flushes();
        }
        self.pending_changes_in_current_full_flush
            .store(false, Ordering::Release);
    }

    pub fn close(&mut self) {
        self.closed = true;
        self.flush_control.set_closed();
    }
}

/// Interface for internal atomic events. See {@link DocumentsWriter} for details.
/// Events are executed concurrently and no order is guaranteed. Each event should
/// only rely on the serializeability within its process method. All actions that
/// must happen before or after a certain action must be encoded inside the
/// {@link #process(IndexWriter, boolean, boolean)} method.
pub trait Event {
    /// Processes the event. This method is called by the `IndexWriter`
    /// passed as the first argument.
    fn process(
        &self,
        writer: &mut IndexWriter,
        trigger_merge: bool,
        clear_buffer: bool,
    ) -> Result<()>;
}

pub enum WriterEvent {
    ApplyDeletes,
    MergePending,
    ForcedPurge,
    FlushFailed(SegmentInfo),
    DeleteNewFiles(HashSet<String>),
}

impl Event for WriterEvent {
    fn process(
        &self,
        writer: &mut IndexWriter,
        trigger_merge: bool,
        clear_buffer: bool,
    ) -> Result<()> {
        match self {
            WriterEvent::ApplyDeletes => writer.apply_deletes_and_purge(true),
            WriterEvent::MergePending => {
                writer.do_after_segment_flushed(trigger_merge, clear_buffer)
            }
            WriterEvent::ForcedPurge => {
                writer.purge(true)?;
                Ok(())
            }
            WriterEvent::FlushFailed(s) => writer.flush_failed(s),
            WriterEvent::DeleteNewFiles(files) => writer.delete_new_files(files),
        }
    }
}
