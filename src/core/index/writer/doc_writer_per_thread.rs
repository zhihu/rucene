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

use core::{
    codec::field_infos::{FieldInfos, FieldInfosBuilder, FieldNumbers, FieldNumbersRef},
    codec::segment_infos::{SegmentCommitInfo, SegmentInfo, SegmentInfoFormat, SegmentWriteState},
    codec::{Codec, LiveDocsFormat},
    doc::{Fieldable, Term},
    index::writer::{
        BufferedUpdates, DeleteSlice, DocConsumer, DocumentsWriterDeleteQueue,
        FrozenBufferedUpdates, IndexWriterConfig, IndexWriterInner, INDEX_MAX_DOCS,
    },
    index::{merge::MergePolicy, merge::MergeScheduler},
    store::directory::{Directory, LockValidatingDirectoryWrapper, TrackingDirectoryWrapper},
    store::{FlushInfo, IOContext},
    util::{
        random_id, BitSet, BitsRef, DirectTrackingAllocator, DocId, VERSION_LATEST,
        {IntAllocator, INT_BLOCK_SIZE},
    },
};

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockWriteGuard, Weak};
use std::time::SystemTime;

use core::codec::{PackedLongDocMap, SorterDocMap};
use core::util::external::Volatile;
use core::util::Bits;
use core::util::FixedBitSet;
use error::ErrorKind::IllegalArgument;
use error::Result;
use std::mem::MaybeUninit;
use std::ptr;

#[derive(Default)]
pub struct DocState {
    // analyzer: Analyzer,  // TODO, current Analyzer is not implemented
    // pub similarity: Option<Box<Similarity>>,
    pub doc_id: DocId,
    // pub doc: Vec<Box<dyn Fieldable>>,
}

impl DocState {
    pub fn new() -> Self {
        Default::default()
    }
    pub fn clear(&mut self) {
        // self.doc = Vec::with_capacity(0);
    }
}

pub type TrackingValidDirectory<D> = TrackingDirectoryWrapper<
    LockValidatingDirectoryWrapper<D>,
    Arc<LockValidatingDirectoryWrapper<D>>,
>;

pub struct DocumentsWriterPerThread<
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
> {
    // we should use TrackingDirectoryWrapper instead
    pub directory: Arc<TrackingValidDirectory<D>>,
    pub directory_orig: Arc<D>,
    pub doc_state: DocState,
    pub consumer: MaybeUninit<DocConsumer<D, C, MS, MP>>,
    pending_updates: BufferedUpdates<C>,
    pub segment_info: SegmentInfo<D, C>,
    // current segment we are working on
    aborted: bool,
    // true if we aborted
    pub num_docs_in_ram: u32,
    pub delete_queue: Arc<DocumentsWriterDeleteQueue<C>>,
    // pointer to DocumentsWriter.delete_queue
    delete_slice: DeleteSlice<C>,
    pub byte_block_allocator: DirectTrackingAllocator,
    pub int_block_allocator: Box<dyn IntAllocator>,
    pending_num_docs: Arc<AtomicI64>,
    pub index_writer_config: Arc<IndexWriterConfig<C, MS, MP>>,
    // enable_test_points: bool,
    index_writer: Weak<IndexWriterInner<D, C, MS, MP>>,
    pub files_to_delete: HashSet<String>,
    inited: bool,
}

impl<D: Directory + Send + Sync + 'static, C: Codec, MS: MergeScheduler, MP: MergePolicy> Drop
    for DocumentsWriterPerThread<D, C, MS, MP>
{
    fn drop(&mut self) {
        unsafe {
            ptr::drop_in_place(self.consumer.as_mut_ptr());
        }
    }
}

impl<D, C, MS, MP> DocumentsWriterPerThread<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    pub fn new(
        index_writer: Weak<IndexWriterInner<D, C, MS, MP>>,
        segment_name: String,
        directory_orig: Arc<D>,
        dir: Arc<LockValidatingDirectoryWrapper<D>>,
        index_writer_config: Arc<IndexWriterConfig<C, MS, MP>>,
        delete_queue: Arc<DocumentsWriterDeleteQueue<C>>,
        pending_num_docs: Arc<AtomicI64>,
    ) -> Result<Self> {
        let directory = Arc::new(TrackingDirectoryWrapper::new(dir));
        let writer = index_writer.upgrade().unwrap();
        let segment_info = SegmentInfo::new(
            VERSION_LATEST,
            &segment_name,
            -1,
            Arc::clone(&directory_orig),
            false,
            Some(Arc::clone(&writer.config.codec)),
            HashMap::new(),
            random_id(),
            HashMap::new(),
            writer.config.index_sort().map(|s| s.clone()),
        )?;
        let delete_slice = delete_queue.new_slice();
        let doc_state = DocState::new();
        // doc_state.similarity = Some(index_writer_config.similarity());
        Ok(DocumentsWriterPerThread {
            directory,
            directory_orig,
            doc_state,
            consumer: MaybeUninit::<DocConsumer<D, C, MS, MP>>::uninit(),
            pending_updates: BufferedUpdates::new(segment_name),
            segment_info,
            aborted: false,
            num_docs_in_ram: 0,
            delete_queue,
            delete_slice,
            byte_block_allocator: DirectTrackingAllocator::new(),
            int_block_allocator: Box::new(IntBlockAllocator::new()),
            pending_num_docs,
            index_writer_config,
            index_writer,
            files_to_delete: HashSet::new(),
            inited: false,
        })
    }

    pub fn init(&mut self, field_numbers: Arc<FieldNumbers>) {
        let field_infos = FieldInfosBuilder::new(FieldNumbersRef::new(field_numbers));
        self.byte_block_allocator = DirectTrackingAllocator::new();
        self.int_block_allocator = Box::new(IntBlockAllocator::new());

        let consumer = DocConsumer::new(self, field_infos);
        self.consumer.write(consumer);
        unsafe {
            self.consumer.assume_init_mut().init();
        }

        self.inited = true;
    }

    fn index_writer(&self) -> Arc<IndexWriterInner<D, C, MS, MP>> {
        self.index_writer.upgrade().unwrap()
    }

    pub fn codec(&self) -> &C {
        self.index_writer_config.codec()
    }

    // Anything that will add N docs to the index should reserve first to make sure it's allowed
    fn reserve_one_doc(&mut self) -> Result<()> {
        self.pending_num_docs.fetch_add(1, Ordering::AcqRel);
        if self.pending_num_docs.load(Ordering::Acquire) > INDEX_MAX_DOCS as i64 {
            // Reserve failed: put the one doc back and throw exc:
            self.pending_num_docs.fetch_sub(1, Ordering::AcqRel);
            bail!(IllegalArgument(
                "number of documents in the index cannot exceed".into()
            ));
        }
        Ok(())
    }

    pub fn update_document<F: Fieldable>(
        &mut self,
        mut doc: Vec<F>,
        del_term: Option<Term>,
    ) -> Result<u64> {
        // debug_assert!(self.inited);
        self.reserve_one_doc()?;
        // self.doc_state.doc = doc;
        self.doc_state.doc_id = self.num_docs_in_ram as i32;
        // self.doc_state.analyzer = analyzer;

        // Even on exception, the document is still added (but marked
        // deleted), so we don't need to un-reserve at that point.
        // Aborting exceptions will actually "lose" more than one
        // document, so the counter will be "wrong" in that case, but
        // it's very hard to fix (we can't easily distinguish aborting
        // vs non-aborting exceptions):
        let res = unsafe {
            self.consumer
                .assume_init_mut()
                .process_document(&mut self.doc_state, &mut doc)
        };
        self.doc_state.clear();
        if res.is_err() {
            // mark document as deleted
            error!(" process document failed, res: {:?}", res);
            let doc = self.doc_state.doc_id;
            self.delete_doc_id(doc);
            self.num_docs_in_ram += 1;
            res?;
        }
        Ok(self.finish_document(del_term))
    }

    pub fn update_documents<F: Fieldable>(
        &mut self,
        docs: Vec<Vec<F>>,
        del_term: Option<Term>,
    ) -> Result<u64> {
        // debug_assert!(self.inited);
        let mut doc_count = 0;
        let mut all_docs_indexed = false;

        let res = self.do_update_documents(docs, del_term, &mut doc_count, &mut all_docs_indexed);
        if !all_docs_indexed && !self.aborted {
            // the iterator threw an exception that is not aborting
            // go and mark all docs from this block as deleted
            let mut doc_id = self.num_docs_in_ram as i32 - 1;
            let end_doc_id = doc_id - doc_count;
            while doc_id > end_doc_id {
                self.delete_doc_id(doc_id);
                doc_id -= 1;
            }
        }
        self.doc_state.clear();
        res
    }

    fn do_update_documents<F: Fieldable>(
        &mut self,
        docs: Vec<Vec<F>>,
        del_term: Option<Term>,
        doc_count: &mut i32,
        all_docs_indexed: &mut bool,
    ) -> Result<u64> {
        for mut doc in docs {
            // Even on exception, the document is still added (but marked
            // deleted), so we don't need to un-reserve at that point.
            // Aborting exceptions will actually "lose" more than one
            // document, so the counter will be "wrong" in that case, but
            // it's very hard to fix (we can't easily distinguish aborting
            // vs non-aborting exceptions):
            self.reserve_one_doc()?;
            // self.doc_state.doc = doc;
            self.doc_state.doc_id = self.num_docs_in_ram as i32;
            *doc_count += 1;

            let res = unsafe {
                self.consumer
                    .assume_init_mut()
                    .process_document(&mut self.doc_state, &mut doc)
            };
            if res.is_err() {
                // Incr here because finishDocument will not
                // be called (because an exc is being thrown):
                self.num_docs_in_ram += 1;
                res?;
            }

            self.num_docs_in_ram += 1;
        }

        *all_docs_indexed = true;

        // Apply delTerm only after all indexing has
        // succeeded, but apply it only to docs prior to when
        // this batch started:
        let seq_no = if let Some(del_term) = del_term {
            let seq = self
                .delete_queue
                .add_term_to_slice(del_term, &mut self.delete_slice);
            self.delete_slice.apply(
                &mut self.pending_updates,
                self.num_docs_in_ram as i32 - *doc_count,
            );
            seq
        } else {
            let (seq, changed) = self.delete_queue.update_slice(&mut self.delete_slice);
            if changed {
                self.delete_slice.apply(
                    &mut self.pending_updates,
                    self.num_docs_in_ram as i32 - *doc_count,
                );
            } else {
                self.delete_slice.reset();
            }
            seq
        };
        Ok(seq_no)
    }

    // Buffer a specific docID for deletion. Currently only
    // used when we hit an exception when adding a document
    fn delete_doc_id(&mut self, doc_id_upto: DocId) {
        self.pending_updates.add_doc_id(doc_id_upto);
        // NOTE: we do not trigger flush here.  This is
        // potentially a RAM leak, if you have an app that tries
        // to add docs but every single doc always hits a
        // non-aborting exception.  Allowing a flush here gets
        // very messy because we are only invoked when handling
        // exceptions so to do this properly, while handling an
        // exception we'd have to go off and flush new deletes
        // which is risky (likely would hit some other
        // confounding exception).
    }

    fn finish_document(&mut self, del_term: Option<Term>) -> u64 {
        // here we actually finish the document in two steps:
        // 1. push the delete into the queue and update our slice
        // 2. increment the DWPT private document id.
        //
        // the updated slice we get from 1. holds all the deletes that have
        // occurred since we updated the slice the last time.
        let mut apply_slice = self.num_docs_in_ram > 0;
        let seq_no: u64;
        if let Some(del_term) = del_term {
            seq_no = self
                .delete_queue
                .add_term_to_slice(del_term, &mut self.delete_slice);
        } else {
            let (seq, apply) = self.delete_queue.update_slice(&mut self.delete_slice);
            seq_no = seq;
            apply_slice = apply;
        }
        if apply_slice {
            self.delete_slice
                .apply(&mut self.pending_updates, self.num_docs_in_ram as i32);
        } else {
            self.delete_slice.reset();
        }
        self.num_docs_in_ram += 1;
        seq_no
    }

    // Prepares this DWPT for flushing. This method will freeze and return the
    // `DocumentsWriterDeleteQueue`s global buffer and apply all pending deletes
    // to this DWPT
    pub fn prepare_flush(&mut self) -> Result<FrozenBufferedUpdates<C>> {
        debug_assert!(self.inited);
        debug_assert!(self.num_docs_in_ram > 0);

        let frozen_updates = self
            .delete_queue
            .freeze_global_buffer(Some(&mut self.delete_slice));
        // apply all deletes before we flush and release the delete slice
        self.delete_slice
            .apply(&mut self.pending_updates, self.num_docs_in_ram as i32);
        debug_assert!(self.delete_slice.is_empty());
        self.delete_slice.reset();
        Ok(frozen_updates)
    }

    /// Flush all pending docs to a new segment
    pub fn flush(&mut self) -> Result<Option<FlushedSegment<D, C>>> {
        debug_assert!(self.inited);
        debug_assert!(self.num_docs_in_ram > 0);
        debug_assert!(self.delete_slice.is_empty());

        self.segment_info.max_doc = self.num_docs_in_ram as i32;
        let ctx = IOContext::Flush(FlushInfo::new(self.num_docs_in_ram));

        let mut flush_state = SegmentWriteState::new(
            Arc::clone(&self.directory),
            self.segment_info.clone(),
            unsafe { self.consumer.assume_init_ref().field_infos.finish()? },
            Some(&self.pending_updates),
            ctx,
            "".into(),
        );

        // Apply delete-by-docID now (delete-byDocID only
        // happens when an exception is hit processing that
        // doc, eg if analyzer has some problem w/ the text):
        if !self.pending_updates.deleted_doc_ids.is_empty() {
            flush_state.live_docs = self
                .codec()
                .live_docs_format()
                .new_live_docs(self.num_docs_in_ram as usize)?;
            let docs_len = self.pending_updates.deleted_doc_ids.len();
            for del_doc_id in self.pending_updates.deleted_doc_ids.drain(..) {
                flush_state.live_docs.clear(del_doc_id as usize);
            }
            flush_state.del_count_on_flush = docs_len as u32;
        }

        if self.aborted {
            debug!("DWPT: flush: skip because aborting is set.");
            return Ok(None);
        }

        debug!(
            "DWPT: flush postings as segment '{}' num_docs={}",
            &flush_state.segment_info.name, self.num_docs_in_ram
        );
        let res = self.do_flush(flush_state);
        if res.is_err() {
            self.abort();
        }
        res
    }

    fn do_flush<DW>(
        &mut self,
        mut flush_state: SegmentWriteState<D, DW, C>,
    ) -> Result<Option<FlushedSegment<D, C>>>
    where
        DW: Directory + 'static,
        <DW as Directory>::IndexOutput: 'static,
    {
        let t0 = SystemTime::now();
        let doc_writer = self as *mut DocumentsWriterPerThread<D, C, MS, MP>;

        // re-init
        unsafe {
            self.consumer.assume_init_mut().reset_doc_writer(doc_writer);
            self.consumer.assume_init_mut().init();
        }

        let sort_map = unsafe { self.consumer.assume_init_mut().flush(&mut flush_state)? };
        self.pending_updates.deleted_terms.clear();
        self.segment_info
            .set_files(&self.directory.create_files())?;
        let segment_info_per_commit = SegmentCommitInfo::new(
            self.segment_info.clone(),
            0,
            -1,
            -1,
            -1,
            HashMap::new(),
            HashSet::new(),
        );

        let mut fs = {
            let segment_deletes = if self.pending_updates.deleted_queries.is_empty()
                && self.pending_updates.doc_values_updates.is_empty()
            {
                self.pending_updates.clear();
                None
            } else {
                Some(&mut self.pending_updates)
            };
            FlushedSegment::new(
                Arc::new(segment_info_per_commit),
                flush_state.field_infos,
                segment_deletes,
                Arc::new(flush_state.live_docs),
                flush_state.del_count_on_flush,
            )
        };
        self.seal_flushed_segment(&mut fs, sort_map)?;

        debug!(
            "DWPT: flush time {:?}",
            SystemTime::now().duration_since(t0).unwrap()
        );
        Ok(Some(fs))
    }

    fn sort_live_docs(
        &self,
        live_docs: &dyn Bits,
        sort_map: &PackedLongDocMap,
    ) -> Result<FixedBitSet> {
        let mut sorted_live_docs = FixedBitSet::new(live_docs.len());
        sorted_live_docs.batch_set(0, live_docs.len());
        for i in 0..live_docs.len() {
            if !live_docs.get(i)? {
                sorted_live_docs.clear(sort_map.old_to_new(i as i32) as usize);
            }
        }
        Ok(sorted_live_docs)
    }

    fn seal_flushed_segment(
        &mut self,
        flushed_segment: &mut FlushedSegment<D, C>,
        sort_map: Option<Arc<PackedLongDocMap>>,
    ) -> Result<()> {
        // set_diagnostics(&mut flushed_segment.segment_info.info, index_writer::SOURCE_FLUSH);

        let flush_info = FlushInfo::new(flushed_segment.segment_info.info.max_doc() as u32);
        let ctx = &IOContext::Flush(flush_info);

        if self.index_writer_config.use_compound_file {
            let original_files = flushed_segment.segment_info.info.files().clone();
            // TODO: like addIndexes, we are relying on createCompoundFile to successfully
            // cleanup...
            let dir = TrackingDirectoryWrapper::new(self.directory.as_ref());
            // flushed_segment has no other reference, so Arc::get_mut is safe
            let segment_info = Arc::get_mut(&mut flushed_segment.segment_info).unwrap();
            self.index_writer()
                .create_compound_file(&dir, &mut segment_info.info, ctx)?;
            segment_info.info.set_use_compound_file();
            self.files_to_delete.extend(original_files);
        }

        // Have codec write SegmentInfo.  Must do this after
        // creating CFS so that 1) .si isn't slurped into CFS,
        // and 2) .si reflects useCompoundFile=true change
        // above:
        {
            let segment_info = Arc::get_mut(&mut flushed_segment.segment_info).unwrap();
            self.codec().segment_info_format().write(
                &self.directory,
                &mut segment_info.info,
                ctx,
            )?;
        }

        // TODO: ideally we would freeze newSegment here!!
        // because any changes after writing the .si will be
        // lost...

        // Must write deleted docs after the CFS so we don't
        // slurp the del file into CFS:
        if !flushed_segment.live_docs.is_empty() {
            debug_assert!(flushed_segment.del_count > 0);

            // TODO: we should prune the segment if it's 100%
            // deleted... but merge will also catch it.

            // TODO: in the NRT case it'd be better to hand
            // this del vector over to the
            // shortly-to-be-opened SegmentReader and let it
            // carry the changes; there's no reason to use
            // filesystem as intermediary here.
            let codec = flushed_segment.segment_info.info.codec();
            if let Some(sort_map) = &sort_map {
                let sorted_bits =
                    self.sort_live_docs(flushed_segment.live_docs.as_ref(), sort_map.as_ref())?;
                codec.live_docs_format().write_live_docs(
                    &sorted_bits,
                    self.directory.as_ref(),
                    flushed_segment.segment_info.as_ref(),
                    flushed_segment.del_count as i32,
                    ctx,
                )?;
            } else {
                codec.live_docs_format().write_live_docs(
                    flushed_segment.live_docs.as_ref(),
                    self.directory.as_ref(),
                    flushed_segment.segment_info.as_ref(),
                    flushed_segment.del_count as i32,
                    ctx,
                )?;
            }

            flushed_segment
                .segment_info
                .set_del_count(flushed_segment.del_count as i32)?;
            flushed_segment.segment_info.advance_del_gen();
        }

        // set sort_map for updates & query delete
        if flushed_segment.segment_updates.is_some()
            && flushed_segment.segment_updates.as_ref().unwrap().any()
        {
            flushed_segment.set_sort_map(sort_map);
        }

        Ok(())
    }

    /// Called if we hit an exception at a bad time (when
    /// updating the index files) and must discard all
    /// currently buffered docs.  This resets our state,
    /// discarding any docs added since last flush.
    pub fn abort(&mut self) {
        self.aborted = true;
        debug!("DWPT: now abort");

        unsafe {
            if let Err(e) = self.consumer.assume_init_mut().abort() {
                error!("DefaultIndexChain abort failed by error: '{:?}'", e);
            }
        }

        self.pending_updates.clear();
        debug!("DWPT: done abort");
    }
}

pub struct FlushedSegment<D: Directory, C: Codec> {
    pub segment_info: Arc<SegmentCommitInfo<D, C>>,
    pub field_infos: FieldInfos,
    pub segment_updates: Option<FrozenBufferedUpdates<C>>,
    pub live_docs: BitsRef,
    pub del_count: u32,
    pub sort_map: Option<Arc<PackedLongDocMap>>,
}

impl<D: Directory, C: Codec> FlushedSegment<D, C> {
    pub fn new(
        segment_info: Arc<SegmentCommitInfo<D, C>>,
        field_infos: FieldInfos,
        buffered_updates: Option<&mut BufferedUpdates<C>>,
        live_docs: BitsRef,
        del_count: u32,
    ) -> Self {
        let mut segment_updates = None;
        if let Some(b) = buffered_updates {
            if b.any() {
                segment_updates = Some(FrozenBufferedUpdates::new(b, true));
            }
        }
        FlushedSegment {
            segment_info,
            field_infos,
            segment_updates,
            live_docs,
            del_count,
            sort_map: None,
        }
    }
    pub fn set_sort_map(&mut self, sort_map: Option<Arc<PackedLongDocMap>>) {
        self.sort_map = sort_map;
    }
}

/// `DocumentsWriterPerThreadPool` controls `ThreadState` instances
/// and their thread assignments during indexing. Each `ThreadState` holds
/// a reference to a `DocumentsWriterPerThread` that is once a
/// `ThreadState` is obtained from the pool exclusively used for indexing a
/// single document by the obtaining thread. Each indexing thread must obtain
/// such a `ThreadState` to make progress. Depending on the
/// `DocumentsWriterPerThreadPool` implementation `ThreadState`
/// assignments might differ from document to document.
///
/// Once a `DocumentsWriterPerThread` is selected for flush the thread pool
/// is reusing the flushing `DocumentsWriterPerThread`s ThreadState with a
/// new `DocumentsWriterPerThread` instance.
pub struct DocumentsWriterPerThreadPool<
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
> {
    inner: RwLock<DWPTPoolInner<D, C, MS, MP>>,
    aborted: Volatile<bool>,
}

struct DWPTPoolInner<
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
> {
    thread_states: Vec<Arc<ThreadState<D, C, MS, MP>>>,
    // valid thread_state index in `self.thread_states`
    free_list: Vec<usize>,
}

impl<D, C, MS, MP> DocumentsWriterPerThreadPool<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    pub fn new() -> Self {
        let inner = DWPTPoolInner {
            thread_states: vec![],
            free_list: Vec::with_capacity(64),
        };
        DocumentsWriterPerThreadPool {
            inner: RwLock::new(inner),
            aborted: Volatile::new(false),
        }
    }

    /// Returns the active number of `ThreadState` instances.
    pub fn active_thread_state_count(&self) -> usize {
        self.inner.read().unwrap().thread_states.len()
    }

    pub fn get_thread_state(&self, i: usize) -> Arc<ThreadState<D, C, MS, MP>> {
        let guard = self.inner.read().unwrap();
        debug_assert!(i < guard.thread_states.len());
        Arc::clone(&guard.thread_states[i])
    }

    pub fn set_abort(&self) {
        self.aborted.write(true);
    }

    /// Returns a new `ThreadState` iff any new state is available other `None`
    /// NOTE: the returned `ThreadState` is already locked iff non-None
    fn new_thread_state(
        &self,
        mut guard: RwLockWriteGuard<'_, DWPTPoolInner<D, C, MS, MP>>,
    ) -> Result<Arc<ThreadState<D, C, MS, MP>>> {
        let thread_state = Arc::new(ThreadState::new(None, guard.thread_states.len()));
        guard.thread_states.push(thread_state);
        let idx = guard.thread_states.len() - 1;
        // recap the free list queue
        //        if guard.thread_states.len() > guard.free_list.capacity() {
        //            let mut new_free_list = Vec::with_capacity(guard.free_list.capacity() * 2);
        //            while let Some(idx) = guard.free_list.pop() {
        //                new_free_list.push(idx);
        //            }
        //            guard.free_list = new_free_list;
        //        }
        Ok(Arc::clone(&guard.thread_states[idx]))
    }

    pub fn reset(
        &self,
        thread_state: &mut ThreadState<D, C, MS, MP>,
    ) -> Option<DocumentsWriterPerThread<D, C, MS, MP>> {
        thread_state.reset()
    }

    pub fn recycle(&self, _dwpt: DocumentsWriterPerThread<D, C, MS, MP>) {
        // do nothing
    }

    pub fn get_and_lock(&self) -> Result<Arc<ThreadState<D, C, MS, MP>>> {
        {
            let mut guard = self.inner.write().unwrap();
            if let Some(idx) = guard.free_list.pop() {
                return Ok(Arc::clone(&guard.thread_states[idx]));
            }
        }
        let guard = self.inner.write().unwrap();
        self.new_thread_state(guard)
    }

    pub fn release(&self, state: Arc<ThreadState<D, C, MS, MP>>) {
        let mut guard = self.inner.write().unwrap();
        // this shouldn't fail
        guard.free_list.push(state.index);
        // In case any thread is waiting, wake one of them up since we just
        // released a thread state; notify() should be sufficient but we do
        // notifyAll defensively:
    }

    pub fn locked_state(&self, idx: usize) -> Arc<ThreadState<D, C, MS, MP>> {
        let guard = self.inner.read().unwrap();
        debug_assert!(idx < guard.thread_states.len());
        Arc::clone(&guard.thread_states[idx])
    }
}

pub struct ThreadStateLock;

/// `ThreadState` references and guards a `DocumentsWriterPerThread`
/// instance that is used during indexing to build a in-memory index
/// segment. `ThreadState` also holds all flush related per-thread
/// data controlled by `DocumentsWriterFlushControl`.
///
/// A `ThreadState`, its methods and members should only accessed by one
/// thread a time. Users must acquire the lock via `ThreadState#lock()`
/// and release the lock in a finally block via `ThreadState#unlock()`
/// before accessing the state.
pub struct ThreadState<
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
> {
    pub lock: Mutex<ThreadStateLock>,
    pub dwpt: Option<DocumentsWriterPerThread<D, C, MS, MP>>,
    // TODO this should really be part of DocumentsWriterFlushControl
    // write access guarded by DocumentsWriterFlushControl
    pub flush_pending: AtomicBool,
    // TODO this should really be part of DocumentsWriterFlushControl
    // write access guarded by DocumentsWriterFlushControl
    // set by DocumentsWriter after each indexing op finishes
    last_seq_no: AtomicU64,
    // index in DocumentsWriterPerThreadPool
    index: usize,
}

impl<D, C, MS, MP> ThreadState<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    fn new(dwpt: Option<DocumentsWriterPerThread<D, C, MS, MP>>, index: usize) -> Self {
        ThreadState {
            lock: Mutex::new(ThreadStateLock),
            dwpt,
            flush_pending: AtomicBool::new(false),
            last_seq_no: AtomicU64::new(0),
            index,
        }
    }

    #[allow(clippy::mut_from_ref)]
    pub fn thread_state_mut(
        &self,
        _lock: &MutexGuard<ThreadStateLock>,
    ) -> &mut ThreadState<D, C, MS, MP> {
        let state = self as *const ThreadState<D, C, MS, MP> as *mut ThreadState<D, C, MS, MP>;
        unsafe { &mut *state }
    }

    pub fn dwpt(&self) -> &DocumentsWriterPerThread<D, C, MS, MP> {
        debug_assert!(self.dwpt.is_some());
        self.dwpt.as_ref().unwrap()
    }

    pub fn dwpt_mut(&mut self) -> &mut DocumentsWriterPerThread<D, C, MS, MP> {
        debug_assert!(self.dwpt.is_some());
        self.dwpt.as_mut().unwrap()
    }

    pub fn flush_pending(&self) -> bool {
        self.flush_pending.load(Ordering::Acquire)
    }

    fn reset(&mut self) -> Option<DocumentsWriterPerThread<D, C, MS, MP>> {
        let dwpt = self.dwpt.take();
        self.flush_pending.store(false, Ordering::Release);
        dwpt
    }

    pub fn inited(&self) -> bool {
        self.dwpt.is_some()
    }

    pub fn set_last_seq_no(&self, seq_no: u64) {
        self.last_seq_no.store(seq_no, Ordering::Release);
    }

    pub fn last_seq_no(&self) -> u64 {
        self.last_seq_no.load(Ordering::Acquire)
    }
}

struct IntBlockAllocator {
    block_size: usize,
}

impl IntBlockAllocator {
    fn new() -> Self {
        IntBlockAllocator {
            block_size: INT_BLOCK_SIZE,
        }
    }
}

impl IntAllocator for IntBlockAllocator {
    fn block_size(&self) -> usize {
        self.block_size
    }

    fn recycle_int_blocks(&mut self, _blocks: &mut [Vec<i32>], _start: usize, _end: usize) {}

    fn int_block(&mut self) -> Vec<i32> {
        let b = vec![0; self.block_size];
        b
    }

    fn shallow_copy(&mut self) -> Box<dyn IntAllocator> {
        Box::new(IntBlockAllocator::new())
    }
}
