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

use core::codec::doc_values::doc_values_format::DocValuesFormat;
use core::codec::field_infos::{
    FieldInfo, FieldInfos, FieldInfosBuilder, FieldInfosFormat, FieldNumbers, FieldNumbersRef,
};
use core::codec::segment_infos::{
    file_name_from_generation, get_last_commit_segments_filename, SegmentCommitInfo, SegmentInfo,
    SegmentInfoFormat, SegmentInfos, SegmentWriteState, INDEX_FILE_PENDING_SEGMENTS,
};
use core::codec::{Codec, CompoundFormat, LiveDocsFormat, PackedLongDocMap};
use core::doc::Term;
use core::doc::{DocValuesType, Fieldable};
use core::index::merge::MergeRateLimiter;
use core::index::merge::MergeScheduler;
use core::index::merge::SegmentMerger;
use core::index::merge::{DocMap, MergeState};
use core::index::merge::{MergePolicy, MergeSpecification, MergerTrigger};
use core::index::merge::{OneMerge, OneMergeRunningInfo};
use core::index::reader::index_exist;
use core::index::reader::{LeafReader, SegmentReader, StandardDirectoryReader};
use core::index::writer::{
    BufferedUpdatesStream, DocumentsWriter, Event, FlushedSegment, FrozenBufferedUpdates,
    IndexFileDeleter, IndexWriterConfig, MergedDocValuesUpdatesIterator, NewDocValuesIterator,
    NumericDocValuesUpdate, OpenMode,
};
use core::search::query::{MatchAllDocsQuery, Query};
use core::store::directory::{Directory, LockValidatingDirectoryWrapper, TrackingDirectoryWrapper};
use core::store::{FlushInfo, IOContext};
use core::util::random_id;
use core::util::to_base36;
use core::util::{BitsRef, DerefWrapper, DocId, VERSION_LATEST};

use core::index::ErrorKind::MergeAborted;
use error::ErrorKind::{AlreadyClosed, IllegalArgument, IllegalState, Index, RuntimeError};
use error::{Error, Result};

use std::collections::{HashMap, HashSet, VecDeque};
use std::mem;
use std::ops::Deref;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, Weak};
use std::time::{Duration, SystemTime};

use core::codec::doc_values::{
    DocValuesWriter, NumericDocValuesWriter, SortedNumericDocValuesWriter,
};
use core::index::writer::dir_wrapper::RateLimitFilterDirectory;
use core::search::NO_MORE_DOCS;
use thread_local::ThreadLocal;

/// Hard limit on maximum number of documents that may be added to the index
/// If you try to add more than this you'll hit `IllegalArgument` Error
pub const INDEX_MAX_DOCS: i32 = i32::max_value() - 128;

/// Maximum value of the token position in an indexed field.
pub const INDEX_MAX_POSITION: i32 = i32::max_value() - 128;

/// An `IndexWriter` creates and maintains an index.
///
/// The `OpenMode` option on  {@link IndexWriterConfig#setOpenMode(OpenMode)} determines
/// whether a new index is created, or whether an existing index is
/// opened. Note that you can open an index with OpenMode::Create
/// even while readers are using the index. The old readers will
/// continue to search the "point in time" snapshot they had opened,
/// and won't see the newly created index until they re-open. If
/// {@link OpenMode::CrateOrAppend} is used IndexWriter will create a
/// new index if there is not already an index at the provided path
/// and otherwise open the existing index.</p>
///
/// In either case, documents are added with add_document()
///  and removed with delete_documents_by_term() or delete_documents_by_query()
/// . A document can be updated with update_document() (which just deletes
/// and then adds the entire document). When finished adding, deleting
/// and updating documents, close() should be called.
///
/// sequence_numbers
/// Each method that changes the index returns a `i64` sequence number, which
/// expresses the effective order in which each change was applied.
/// {@link #commit} also returns a sequence number, describing which
/// changes are in the commit point and which are not.  Sequence numbers
/// are transient (not saved into the index in any way) and only valid
/// within a single {@code IndexWriter} instance.
///
/// flush
/// These changes are buffered in memory and periodically
/// flushed to the `Directory` (during the above method
/// calls). A flush is triggered when there are enough added documents
/// since the last flush. Flushing is triggered either by RAM usage of the
/// documents (see {@link IndexWriterConfig#setRAMBufferSizeMB}) or the
/// number of added documents (see {@link IndexWriterConfig#setMaxBufferedDocs(int)}).
/// The default is to flush when RAM usage hits
/// {@link IndexWriterConfig#DEFAULT_RAM_BUFFER_SIZE_MB} MB. For
/// best indexing speed you should flush by RAM usage with a
/// large RAM buffer. Additionally, if IndexWriter reaches the configured number of
/// buffered deletes (see {@link IndexWriterConfig#setMaxBufferedDeleteTerms})
/// the deleted terms and queries are flushed and applied to existing segments.
/// In contrast to the other flush options {@link IndexWriterConfig#setRAMBufferSizeMB} and
/// {@link IndexWriterConfig#setMaxBufferedDocs(int)}, deleted terms
/// won't trigger a segment flush. Note that flushing just moves the
/// internal buffered state in IndexWriter into the index, but
/// these changes are not visible to IndexReader until either
/// {@link #commit()} or {@link #close} is called.  A flush may
/// also trigger one or more segment merges which by default
/// run with a background thread so as not to block the
/// addDocument calls (see <a href="#mergePolicy">below</a>
/// for changing the {@link MergeScheduler}).</p>
///
/// <p>Opening an <code>IndexWriter</code> creates a lock file for the directory in use. Trying to
/// open another <code>IndexWriter</code> on the same directory will lead to a
/// {@link LockObtainFailedException}.</p>
///
/// <a name="deletionPolicy"></a>
/// <p>Expert: <code>IndexWriter</code> allows an optional
/// {@link IndexDeletionPolicy} implementation to be specified.  You
/// can use this to control when prior commits are deleted from
/// the index.  The default policy is {@link KeepOnlyLastCommitDeletionPolicy}
/// which removes all prior commits as soon as a new commit is
/// done.  Creating your own policy can allow you to explicitly
/// keep previous "point in time" commits alive in the index for
/// some time, either because this is useful for your application,
/// or to give readers enough time to refresh to the new commit
/// without having the old commit deleted out from under them.
/// The latter is necessary when multiple computers take turns opening
/// their own {@code IndexWriter} and {@code IndexReader}s
/// against a single shared index mounted via remote filesystems
/// like NFS which do not support "delete on last close" semantics.
/// A single computer accessing an index via NFS is fine with the
/// default deletion policy since NFS clients emulate "delete on
/// last close" locally.  That said, accessing an index via NFS
/// will likely result in poor performance compared to a local IO
/// device. </p>
///
/// <a name="mergePolicy"></a> <p>Expert:
/// <code>IndexWriter</code> allows you to separately change
/// the {@link MergePolicy} and the {@link MergeScheduler}.
/// The {@link MergePolicy} is invoked whenever there are
/// changes to the segments in the index.  Its role is to
/// select which merges to do, if any, and return a {@link
/// MergePolicy.MergeSpecification} describing the merges.
/// The default is {@link LogByteSizeMergePolicy}.  Then, the {@link
/// MergeScheduler} is invoked with the requested merges and
/// it decides when and how to run the merges.  The default is
/// {@link ConcurrentMergeScheduler}.
///
/// NOTE: if you hit a
/// VirtualMachineError, or disaster strikes during a checkpoint
/// then IndexWriter will close itself.  This is a
/// defensive measure in case any internal state (buffered
/// documents, deletions, reference counts) were corrupted.
/// Any subsequent calls will throw an AlreadyClosedException.</p>
///
/// NOTE: `IndexWriter` instances are completely thread
/// safe, meaning multiple threads can call any of its
/// methods, concurrently.  If your application requires
/// external synchronization, you should *not*
/// synchronize on the `IndexWriter` instance as
/// this may cause deadlock; use your own (non-Lucene) objects
/// instead.
///
/// Clarification: Check Points (and commits)
/// IndexWriter writes new index files to the directory without writing a new segments_N
/// file which references these new files. It also means that the state of
/// the in memory SegmentInfos object is different than the most recent
/// segments_N file written to the directory.
///
/// Each time the SegmentInfos is changed, and matches the (possibly
/// modified) directory files, we have a new "check point".
/// If the modified/new SegmentInfos is written to disk - as a new
/// (generation of) segments_N file - this check point is also an
/// IndexCommit.
///
/// A new checkpoint always replaces the previous checkpoint and
/// becomes the new "front" of the index. This allows the IndexFileDeleter
/// to delete files that are referenced only by stale checkpoints.
/// (files that were created since the last commit, but are no longer
/// referenced by the "front" of the index). For this, IndexFileDeleter
/// keeps track of the last non commit checkpoint.
pub struct IndexWriter<
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
> {
    writer: Arc<IndexWriterInner<D, C, MS, MP>>,
}

impl<D, C, MS, MP> Clone for IndexWriter<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    fn clone(&self) -> Self {
        Self {
            writer: Arc::clone(&self.writer),
        }
    }
}

impl<D, C, MS, MP> IndexWriter<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    /// Constructs a new IndexWriter per the settings given in <code>conf</code>.
    pub fn new(
        d: Arc<D>,
        conf: Arc<IndexWriterConfig<C, MS, MP>>,
    ) -> Result<IndexWriter<D, C, MS, MP>> {
        let mut index_writer = IndexWriter {
            writer: Arc::new(IndexWriterInner::new(d, conf)?),
        };
        index_writer.init();
        Ok(index_writer)
    }

    /// Expert: returns a readonly reader, covering all
    /// committed as well as un-committed changes to the index.
    /// This provides "near real-time" searching, in that
    /// changes made during an IndexWriter session can be
    /// quickly made available for searching without closing
    /// the writer nor calling {@link #commit}.
    ///
    /// Note that this is functionally equivalent to calling
    /// {#flush} and then opening a new reader.  But the turnaround time of this
    /// method should be faster since it avoids the potentially
    /// costly {@link #commit}.
    ///
    /// You must close the {@link IndexReader} returned by
    /// this method once you are done using it.
    ///
    /// It's <i>near</i> real-time because there is no hard
    /// guarantee on how quickly you can get a new reader after
    /// making changes with IndexWriter.  You'll have to
    /// experiment in your situation to determine if it's
    /// fast enough.  As this is a new and experimental
    /// feature, please report back on your findings so we can
    /// learn, improve and iterate.
    ///
    /// The resulting reader supports {@link
    /// DirectoryReader#openIfChanged}, but that call will simply forward
    /// back to this method (though this may change in the
    /// future).
    ///
    /// The very first time this method is called, this
    /// writer instance will make every effort to pool the
    /// readers that it opens for doing merges, applying
    /// deletes, etc.  This means additional resources (RAM,
    /// file descriptors, CPU time) will be consumed.
    ///
    /// For lower latency on reopening a reader, you should
    /// call {@link IndexWriterConfig#setMergedSegmentWarmer} to
    /// pre-warm a newly merged segment before it's committed
    /// to the index.  This is important for minimizing
    /// index-to-search delay after a large merge.
    ///
    /// If an addIndexes* call is running in another thread,
    /// then this reader will only search those segments from
    /// the foreign index that have been successfully copied
    /// over, so far.
    ///
    /// *NOTE*: Once the writer is closed, any
    /// outstanding readers may continue to be used.  However,
    /// if you attempt to reopen any of those readers, you'll
    /// hit an {@link AlreadyClosedException}.
    ///
    /// @return:
    /// - Ok(IndexReader) that covers entire index plus all changes made so far by this IndexWriter
    ///   instance
    /// - Err: If there is a low-level I/O error
    pub fn get_reader(
        &self,
        apply_all_deletes: bool,
        write_all_deletes: bool,
    ) -> Result<StandardDirectoryReader<D, C, MS, MP>> {
        IndexWriterInner::get_reader(self, apply_all_deletes, write_all_deletes)
    }

    #[inline]
    pub fn config(&self) -> &Arc<IndexWriterConfig<C, MS, MP>> {
        &self.writer.config
    }

    #[inline]
    pub fn max_doc(&self) -> u32 {
        // self.ensure_open(true);
        let _l = self.writer.lock.lock().unwrap();
        self.writer.doc_writer.num_docs() + self.writer.segment_infos.total_max_doc() as u32
    }

    pub fn num_docs(&self) -> u32 {
        // let _l = self.writer.lock.lock().unwrap();
        let mut count = self.writer.doc_writer.num_docs();
        for info in &self.writer.segment_infos.segments {
            count += info.info.max_doc() as u32 - self.writer.num_deleted_docs(&info);
        }
        count
    }

    #[inline]
    /// Returns the Directory used by this index.
    pub fn directory(&self) -> &Arc<D> {
        // return the original directory the use supplied, unwrapped.
        &self.writer.directory_orig
    }

    pub fn close(&self) -> Result<()> {
        IndexWriterInner::close(self)
    }

    /// Close the <code>IndexWriter</code> without committing
    /// any changes that have occurred since the last commit
    /// (or since it was opened, if commit hasn't been called).
    /// This removes any temporary files that had been created,
    /// after which the state of the index will be the same as
    /// it was when commit() was last called or when this
    /// writer was first opened.  This also clears a previous
    /// call to `#prepareCommit`.
    /// @throws IOException if there is a low-level IO error
    pub fn rollback(&self) -> Result<()> {
        self.writer.rollback()
    }

    /// Adds a document to this index.
    ///
    /// Note that if an Exception is hit (for example disk full)
    /// then the index will be consistent, but this document
    /// may not have been added.  Furthermore, it's possible
    /// the index will have one segment in non-compound format
    /// even when using compound files (when a merge has
    /// partially succeeded).
    ///
    /// This method periodically flushes pending documents
    /// to the Directory (see <a href="#flush">above</a>), and
    /// also periodically triggers segment merges in the index
    /// according to the `MergePolicy` in use.
    ///
    /// Merges temporarily consume space in the
    /// directory. The amount of space required is up to 1X the
    /// size of all segments being merged, when no
    /// readers/searchers are open against the index, and up to
    /// 2X the size of all segments being merged when
    /// readers/searchers are open against the index (see
    /// `#forceMerge(int)` for details). The sequence of
    /// primitive merge operations performed is governed by the
    /// merge policy.
    ///
    /// Note that each term in the document can be no longer
    /// than `#MAX_TERM_LENGTH` in bytes, otherwise an
    /// IllegalArgumentException will be thrown.
    ///
    /// Note that it's possible to create an invalid Unicode
    /// string in java if a UTF16 surrogate pair is malformed.
    /// In this case, the invalid characters are silently
    /// replaced with the Unicode replacement character
    /// U+FFFD.
    ///
    /// @return The <a href="#sequence_number">sequence number</a>
    /// for this operation
    pub fn add_document<F: Fieldable>(&self, doc: Vec<F>) -> Result<u64> {
        IndexWriterInner::update_document(self, doc, None)
    }

    /// Updates a document by first deleting the document(s)
    /// containing <code>term</code> and then adding the new
    /// document.  The delete and then add are atomic as seen
    /// by a reader on the same index (flush may happen only after
    /// the add).
    ///
    /// @return The <a href="#sequence_number">sequence number</a>
    /// for this operation
    ///
    /// @param term the term to identify the document(s) to be
    /// deleted
    /// @param doc the document to be added
    /// @throws CorruptIndexException if the index is corrupt
    /// @throws IOException if there is a low-level IO error
    pub fn update_document<F: Fieldable>(&self, doc: Vec<F>, term: Option<Term>) -> Result<u64> {
        IndexWriterInner::update_document(self, doc, term)
    }

    /// Atomically adds a block of documents with sequentially
    /// assigned document IDs, such that an external reader
    /// will see all or none of the documents.
    ///
    /// <b>WARNING</b>: the index does not currently record
    /// which documents were added as a block.  Today this is
    /// fine, because merging will preserve a block. The order of
    /// documents within a segment will be preserved, even when child
    /// documents within a block are deleted. Most search features
    /// (like result grouping and block joining) require you to
    /// mark documents; when these documents are deleted these
    /// search features will not work as expected. Obviously adding
    /// documents to an existing block will require you the reindex
    /// the entire block.
    ///
    /// However it's possible that in the future Lucene may
    /// merge more aggressively re-order documents (for example,
    /// perhaps to obtain better index compression), in which case
    /// you may need to fully re-index your documents at that time.
    ///
    /// See `#addDocument(Iterable)` for details on
    /// index and IndexWriter state after an Exception, and
    /// flushing/merging temporary free space requirements.
    ///
    /// <b>NOTE</b>: tools that do offline splitting of an index
    /// (for example, IndexSplitter in contrib) or
    /// re-sorting of documents (for example, IndexSorter in
    /// contrib) are not aware of these atomically added documents
    /// and will likely break them up.  Use such tools at your
    /// own risk!
    ///
    /// @return The <a href="#sequence_number">sequence number</a>
    /// for this operation
    ///
    /// @throws CorruptIndexException if the index is corrupt
    /// @throws IOException if there is a low-level IO error
    ///
    /// @lucene.experimental
    pub fn add_documents<F: Fieldable>(&self, docs: Vec<Vec<F>>) -> Result<u64> {
        IndexWriterInner::update_documents(self, docs, None)
    }

    /// Atomically deletes documents matching the provided
    /// delTerm and adds a block of documents with sequentially
    /// assigned document IDs, such that an external reader
    /// will see all or none of the documents.
    ///
    /// See `#addDocuments()`.
    ///
    /// @return The <a href="#sequence_number">sequence number</a>
    /// for this operation
    ///
    /// @throws CorruptIndexException if the index is corrupt
    /// @throws IOException if there is a low-level IO error
    pub fn update_documents<F: Fieldable>(
        &self,
        docs: Vec<Vec<F>>,
        term: Option<Term>,
    ) -> Result<u64> {
        IndexWriterInner::update_documents(self, docs, term)
    }

    /// Deletes the document(s) containing any of the
    /// terms. All given deletes are applied and flushed atomically
    /// at the same time.
    ///
    /// @return The <a href="#sequence_number">sequence number</a>
    /// for this operation
    ///
    /// @param terms array of terms to identify the documents
    /// to be deleted
    /// @throws CorruptIndexException if the index is corrupt
    /// @throws IOException if there is a low-level IO error
    pub fn delete_documents_by_terms(&self, terms: Vec<Term>) -> Result<u64> {
        IndexWriterInner::delete_documents_by_terms(self, terms)
    }

    /// Deletes the document(s) matching any of the provided queries.
    /// All given deletes are applied and flushed atomically at the same time.
    ///
    /// @return The <a href="#sequence_number">sequence number</a>
    /// for this operation
    ///
    /// @param queries array of queries to identify the documents
    /// to be deleted
    /// @throws CorruptIndexException if the index is corrupt
    /// @throws IOException if there is a low-level IO error
    pub fn delete_documents_by_queries(&self, queries: Vec<Arc<dyn Query<C>>>) -> Result<u64> {
        IndexWriterInner::delete_documents_by_queries(self, queries)
    }

    /// Delete all documents in the index.
    ///
    /// This method will drop all buffered documents and will remove all segments
    /// from the index. This change will not be visible until a `#commit()`
    /// has been called. This method can be rolled back using `#rollback()`.
    ///
    /// NOTE: this method is much faster than using deleteDocuments( new
    /// MatchAllDocsQuery() ). Yet, this method also has different semantics
    /// compared to `#deleteDocuments(Query...)` since internal
    /// data-structures are cleared as well as all segment information is
    /// forcefully dropped anti-viral semantics like omitting norms are reset or
    /// doc value types are cleared. Essentially a call to `#deleteAll()` is
    /// equivalent to creating a new `IndexWriter` with
    /// `OpenMode#CREATE` which a delete query only marks documents as
    /// deleted.
    ///
    /// NOTE: this method will forcefully abort all merges in progress. If other
    /// threads are running `#forceMerge`, `#addIndexes(CodecReader[])`
    /// or `#forceMergeDeletes` methods, they may receive
    /// `MergePolicy.MergeAbortedException`s.
    ///
    /// @return The <a href="#sequence_number">sequence number</a>
    /// for this operation
    pub fn delete_all(&self) -> Result<u64> {
        IndexWriterInner::delete_all(self)
    }

    pub fn nrt_is_current(&self, infos: &SegmentInfos<D, C>) -> bool {
        self.writer.nrt_is_current(infos)
    }

    pub fn update_numeric_doc_value(&self, term: Term, field: &str, value: i64) -> Result<u64> {
        IndexWriterInner::update_numeric_doc_value(self, term, field, value)
    }

    /// Forces merge policy to merge segments until there are
    /// max_num_segments. The actual merges to be
    /// executed are determined by the `MergePolicy`.
    ///
    /// This is a horribly costly operation, especially when
    /// you pass a small {@code maxNumSegments}; usually you
    /// should only call this if the index is static (will no
    /// longer be changed).
    ///
    /// Note that this requires free space that is proportional
    /// to the size of the index in your Directory: 2X if you are
    /// not using compound file format, and 3X if you are.
    /// For example, if your index size is 10 MB then you need
    /// an additional 20 MB free for this to complete (30 MB if
    /// you're using compound file format). This is also affected
    /// by the `Codec` that is used to execute the merge,
    /// and may result in even a bigger index. Also, it's best
    /// to call `#commit()` afterwards, to allow IndexWriter
    /// to free up disk space.
    ///
    /// If some but not all readers re-open while merging
    /// is underway, this will cause {@code > 2X} temporary
    /// space to be consumed as those new readers will then
    /// hold open the temporary segments at that time.  It is
    /// best not to re-open readers while merging is running.
    ///
    /// The actual temporary usage could be much less than
    /// these figures (it depends on many factors).
    ///
    /// In general, once this completes, the total size of the
    /// index will be less than the size of the starting index.
    /// It could be quite a bit smaller (if there were many
    /// pending deletes) or just slightly smaller.
    ///
    /// If an Exception is hit, for example
    /// due to disk full, the index will not be corrupted and no
    /// documents will be lost.  However, it may have
    /// been partially merged (some segments were merged but
    /// not all), and it's possible that one of the segments in
    /// the index will be in non-compound format even when
    /// using compound file format.  This will occur when the
    /// Exception is hit during conversion of the segment into
    /// compound format.
    ///
    /// This call will merge those segments present in
    /// the index when the call started.  If other threads are
    /// still adding documents and flushing segments, those
    /// newly created segments will not be merged unless you
    /// call forceMerge again.
    ///
    /// @param maxNumSegments maximum number of segments left
    /// in the index after merging finishes
    ///
    /// @throws CorruptIndexException if the index is corrupt
    /// @throws IOException if there is a low-level IO error
    /// @see MergePolicy#findMerges
    pub fn force_merge(&self, max_num_segments: u32, do_wait: bool) -> Result<()> {
        IndexWriterInner::force_merge(self, max_num_segments, do_wait)
    }

    /// Returns true if there may be changes that have not been
    /// committed.  There are cases where this may return true
    /// when there are no actual "real" changes to the index,
    /// for example if you've deleted by Term or Query but
    /// that Term or Query does not match any documents.
    /// Also, if a merge kicked off as a result of flushing a
    /// new segment during {@link #commit}, or a concurrent
    /// merged finished, this method may return true right
    /// after you had just called {@link #commit}.
    pub fn has_uncommitted_changes(&self) -> bool {
        self.writer.has_uncommitted_changes()
    }

    /// Commits all pending changes.
    ///
    /// (added and deleted documents, segment merges, added
    /// indexes, etc.) to the index, and syncs all referenced
    /// index files, such that a reader will see the changes
    /// and the index updates will survive an OS or machine
    /// crash or power loss.  Note that this does not wait for
    /// any running background merges to finish.  This may be a
    /// costly operation, so you should test the cost in your
    /// application and do it only when really necessary.</p>
    ///
    /// Note that this operation calls Directory.sync on
    /// the index files.  That call should not return until the
    /// file contents and metadata are on stable storage.  For
    /// FSDirectory, this calls the OS's fsync.  But, beware:
    /// some hardware devices may in fact cache writes even
    /// during fsync, and return before the bits are actually
    /// on stable storage, to give the appearance of faster
    /// performance.  If you have such a device, and it does
    /// not have a battery backup (for example) then on power
    /// loss it may still lose data.  Lucene cannot guarantee
    /// consistency on such devices.
    ///
    /// If nothing was committed, because there were no
    /// pending changes, this returns -1.  Otherwise, it returns
    /// the sequence number such that all indexing operations
    /// prior to this sequence will be included in the commit
    /// point, and all other operations will not.
    ///
    ///
    /// @return The <a href="#sequence_number">sequence number</a>
    /// of the last operation in the commit.  All sequence numbers <= this value
    /// will be reflected in the commit, and all others will not.
    pub fn commit(&self) -> Result<i64> {
        IndexWriterInner::commit(self)
    }

    /// Moves all in-memory segments to the `Directory`, but does not commit
    /// (fsync) them (call {@link #commit} for that).
    pub fn flush(&self) -> Result<()> {
        IndexWriterInner::flush(self, true, true)
    }

    pub fn is_open(&self) -> bool {
        self.writer.is_open()
    }

    pub fn tragedy(&self) -> Option<&Error> {
        self.writer.tragedy.as_ref()
    }

    pub fn explicit_merge(&self) -> Result<()> {
        IndexWriterInner::maybe_merge(self, MergerTrigger::Explicit, None)
    }
}

impl<D, C, MS, MP> IndexWriter<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    fn init(&mut self) {
        unsafe {
            let w1 = Arc::downgrade(&self.writer);
            let w2 = Arc::downgrade(&self.writer);

            let writer = self.writer.as_ref() as *const IndexWriterInner<D, C, MS, MP>
                as *mut IndexWriterInner<D, C, MS, MP>;
            let doc_writer = &mut (*writer).doc_writer as *mut DocumentsWriter<D, C, MS, MP>;
            let reader_pool = &mut (*writer).reader_pool as *mut ReaderPool<D, C, MS, MP>;

            (*doc_writer).init(w1);
            (*reader_pool).init(w2);
        }
    }

    /// Atomically adds the segment private delete packet and publishes the flushed
    /// segments SegmentInfo to the index writer.
    pub fn publish_flushed_segment(
        &self,
        new_segment: FlushedSegment<D, C>,
        global_packet: Option<FrozenBufferedUpdates<C>>,
    ) -> Result<()> {
        self.writer
            .publish_flushed_segment(new_segment, global_packet)
    }

    pub fn publish_frozen_updates(&self, packet: FrozenBufferedUpdates<C>) -> Result<()> {
        self.writer.publish_frozen_updates(packet)
    }

    pub fn with_inner(writer: Arc<IndexWriterInner<D, C, MS, MP>>) -> Self {
        Self { writer }
    }

    pub fn apply_deletes_and_purge(&self, force_purge: bool) -> Result<()> {
        IndexWriterInner::apply_deletes_and_purge(self, force_purge)
    }

    pub fn do_after_segment_flushed(&self, trigger_merge: bool, force_purge: bool) -> Result<()> {
        IndexWriterInner::do_after_segment_flushed(self, trigger_merge, force_purge)
    }

    pub fn purge(&self, forced: bool) -> Result<u32> {
        IndexWriterInner::purge(self, forced)
    }

    /// Cleans up residuals from a segment that could not be entirely flushed due to a error
    pub fn flush_failed(&self, info: &SegmentInfo<D, C>) -> Result<()> {
        self.writer.flush_failed(info)
    }

    pub fn is_closed(&self) -> bool {
        self.writer.is_closed()
    }

    // Tries to delete the given files if unreferenced
    pub fn delete_new_files(&self, files: &HashSet<String>) -> Result<()> {
        let _l = self.writer.lock.lock()?;
        self.writer.delete_new_files(files)
    }

    pub fn num_deleted_docs(&self, info: &SegmentCommitInfo<D, C>) -> u32 {
        self.writer.num_deleted_docs(info)
    }

    /// Record that the files referenced by this `SegmentInfos` are still in use.
    pub fn inc_ref_deleter(&self, segment_infos: &SegmentInfos<D, C>) -> Result<()> {
        self.writer.inc_ref_deleter(segment_infos)
    }

    pub fn dec_ref_deleter(&self, segment_infos: &SegmentInfos<D, C>) -> Result<()> {
        self.writer.dec_ref_deleter(segment_infos)
    }

    #[inline]
    pub fn reader_pool(&self) -> &ReaderPool<D, C, MS, MP> {
        &self.writer.reader_pool
    }

    /// Merges the indicated segments, replacing them in the stack with a single segment.
    pub fn merge(&self, merge: &mut OneMerge<D, C>) -> Result<()> {
        IndexWriterInner::merge(self, merge)
    }

    #[inline]
    pub fn merging_segments(&self) -> &HashSet<String> {
        &self.writer.merging_segments
    }

    #[inline]
    pub fn next_merge_id(&self) -> u32 {
        self.writer.next_merge_id()
    }

    pub fn next_merge(&self) -> Option<OneMerge<D, C>> {
        self.writer.next_merge()
    }

    pub fn has_pending_merges(&self) -> bool {
        let _l = self.writer.lock.lock().unwrap();
        !self.writer.pending_merges.is_empty()
    }
}

// TODO: maybe we should impl this for `IndexWriterInner`,
// but currently, some of the methods called depends on the IndexWriter strust
// instead of IndexWriterInner
impl<D, C, MS, MP> Drop for IndexWriter<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    fn drop(&mut self) {
        // this is the last reference, actual drop
        if Arc::strong_count(&self.writer) == 1 {
            if self.writer.config.commit_on_close {
                if let Err(e) = IndexWriterInner::shutdown(self) {
                    error!("IndexWriter: shutdown on close failed by: {:?}", e);
                }
            } else if let Err(e) = self.rollback() {
                error!("IndexWriter: rollback on close failed by: {:?}", e);
            }
        }
    }
}

pub struct IndexWriterInner<
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
> {
    pub config: Arc<IndexWriterConfig<C, MS, MP>>,
    // original use directory
    directory_orig: Arc<D>,
    // wrapped with additional checks
    directory: Arc<LockValidatingDirectoryWrapper<D>>,

    lock: Arc<Mutex<()>>,
    closed: AtomicBool,
    closing: AtomicBool,
    cond: Condvar,

    global_field_numbers: Arc<FieldNumbers>,

    /// How many documents are in the index, or are in the process of being added (reserved).
    pub pending_num_docs: Arc<AtomicI64>,
    pub doc_writer: DocumentsWriter<D, C, MS, MP>,
    pub deleter: IndexFileDeleter<D>,

    // when unrecoverable disaster strikes, we populate this
    // with the reason that we had to close IndexWriter
    tragedy: Option<Error>,

    segment_infos: SegmentInfos<D, C>,
    segment_infos_lock: Mutex<()>,
    change_count: AtomicU64,
    last_commit_change_count: AtomicU64,
    rollback_segments: Vec<Arc<SegmentCommitInfo<D, C>>>,

    // Used only by commit and prepareCommit, below; lock order is commit_lock -> IW
    commit_lock: Mutex<()>,
    // set when a commit is pending (after prepareCommit() & before commit())
    pending_commit: Option<SegmentInfos<D, C>>,
    pending_seq_no: AtomicI64,
    pending_commit_change_count: AtomicU64,
    files_to_commit: HashSet<String>,

    rate_limiters: Arc<ThreadLocal<Arc<MergeRateLimiter>>>,
    merge_directory: RateLimitFilterDirectory<LockValidatingDirectoryWrapper<D>, MergeRateLimiter>,

    segments_to_merge: HashMap<Arc<SegmentCommitInfo<D, C>>, bool>,
    merge_max_num_segments: u32,

    merging_segments: HashSet<String>,
    merge_scheduler: MS,
    merge_id_gen: AtomicU32,
    pending_merges: VecDeque<OneMerge<D, C>>,
    running_merges: HashMap<u32, OneMergeRunningInfo<D, C>>,

    merge_exceptions: Vec<OneMerge<D, C>>,
    merge_gen: u64,
    stop_merges: bool,

    // Ensures only one flush() in actually flushing segments at a time.
    full_flush_lock: Arc<Mutex<()>>,
    flush_count: AtomicU32,
    flush_deletes_count: AtomicU32,

    reader_pool: ReaderPool<D, C, MS, MP>,
    updates_stream_lock: Mutex<()>,
    buffered_updates_stream: BufferedUpdatesStream<C>,

    // This is a "write once" variable (like the organic dye
    // on a DVD-R that may or may not be heated by a laser and
    // then cooled to permanently record the event): it's
    // false, until getReader() is called for the first time,
    // at which point it's switched to true and never changes
    // back to false.  Once this is true, we hold open and
    // reuse SegmentReader instances internally for applying
    // deletes, doing merges, and reopening near real-time
    // readers.
    pool_readers: AtomicBool,
}

unsafe impl<D, C, MS, MP> Send for IndexWriterInner<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
}

unsafe impl<D, C, MS, MP> Sync for IndexWriterInner<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
}

impl<D, C, MS, MP> IndexWriterInner<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    /// Constructs a new IndexWriter per the settings given in <code>conf</code>.
    ///
    /// NOTE: after ths writer is created, the given configuration instance
    /// cannot be passed to another writer.
    ///
    /// @param d
    ///          the index directory. The index is either created or appended
    ///          according <code>conf.getOpenMode()</code>.
    /// @param conf
    ///          the configuration settings according to which IndexWriter should
    ///          be initialized.
    /// @throws IOException
    ///           if the directory cannot be read/written to, or if it does not
    ///           exist and <code>conf.getOpenMode()</code> is
    ///           <code>OpenMode.APPEND</code> or if there is any other low-level
    ///           IO error
    fn new(d: Arc<D>, conf: Arc<IndexWriterConfig<C, MS, MP>>) -> Result<Self> {
        let directory = Arc::new(LockValidatingDirectoryWrapper::new(Arc::clone(&d)));

        let rate_limiters = Arc::new(ThreadLocal::default());

        // Directory we use for merging, so we can abort running merges, and so
        // merge schedulers can optionally rate-limit per-merge IO:
        let merge_directory =
            RateLimitFilterDirectory::new(Arc::clone(&directory), Arc::clone(&rate_limiters));

        let buffered_updates_stream = BufferedUpdatesStream::default();
        let pool_readers = conf.reader_pooling;

        let create = match conf.open_mode {
            OpenMode::Create => true,
            OpenMode::Append => false,
            // CREATE_OR_APPEND - create only if an index does not exist
            OpenMode::CreateOrAppend => !index_exist(directory.as_ref())?,
        };

        // If index is too old, reading the segments will throw
        // IndexFormatTooOldException
        let mut initial_index_exists = true;
        let files = directory.list_all()?;

        // Set up our initial SegmentInfos:
        let mut segment_infos: SegmentInfos<D, C>;
        let rollback_segments: Vec<Arc<SegmentCommitInfo<D, C>>>;
        let change_count = AtomicU64::new(0);
        if create {
            // Try to read first.  This is to allow create
            // against an index that's currently open for
            // searching.  In this case we write the next
            // segments_N file with no segments:
            segment_infos = {
                match SegmentInfos::read_latest_commit(&d) {
                    Ok(mut sis) => {
                        sis.clear();
                        sis
                    }
                    Err(_) => {
                        // Likely this means it's a fresh directory
                        initial_index_exists = false;
                        SegmentInfos::default()
                    }
                }
            };
            rollback_segments = segment_infos.create_backup_segment_infos();
            // Record that we have a change (zero out all segments) pending:
            // NOTE: this is copy from self.changed()
            change_count.fetch_add(1, Ordering::AcqRel);
            segment_infos.changed();
        } else {
            // Init from either the latest commit point, or an explicit prior commit point:
            let last_segments_file = get_last_commit_segments_filename(&files)?;
            if last_segments_file.is_none() {
                bail!(
                    "IndexNotFound: no segments* file found in '{}', files: {:?}",
                    &directory,
                    &files
                );
            }
            let last_segments_file = last_segments_file.unwrap();

            // Do not use SegmentInfos.read(Directory) since the spooky
            // retrying it does is not necessary here (we hold the write lock):
            segment_infos = SegmentInfos::read_commit(&d, &last_segments_file)?;
            rollback_segments = segment_infos.create_backup_segment_infos();
        }

        let pending_num_docs = AtomicI64::new(segment_infos.total_max_doc() as i64);
        // start with previous field numbers, but new FieldInfos
        // NOTE: this is correct even for an NRT reader because we'll pull
        // FieldInfos even for the un-committed segments:
        let global_field_numbers = Arc::new(FieldNumbers::new());
        for info in &segment_infos.segments {
            let fis = read_field_infos(info.as_ref())?;
            for fi in fis.by_number.values() {
                global_field_numbers.add_or_get(
                    &fi.name,
                    fi.number,
                    fi.doc_values_type,
                    fi.point_dimension_count,
                    fi.point_num_bytes,
                )?;
            }
        }

        Self::validate_index_sort(conf.as_ref(), &segment_infos)?;

        let doc_writer =
            DocumentsWriter::new(Arc::clone(&conf), Arc::clone(&d), Arc::clone(&directory));

        // Default deleter (for backwards compatibility) is
        // KeepOnlyLastCommitDeleter:

        let mut deleter = IndexFileDeleter::new(directory.clone());
        let starting_commit_deleted =
            deleter.init(d.clone(), &files, &mut segment_infos, initial_index_exists)?;

        if starting_commit_deleted {
            // Deletion policy deleted the "head" commit point.
            // We have to mark ourself as changed so that if we
            // are closed w/o any further changes we write a new
            // segments_N file.

            // NOTE: this is copy from self.changed()
            change_count.fetch_add(1, Ordering::AcqRel);
            segment_infos.changed();
        }

        Ok(IndexWriterInner {
            lock: Arc::new(Mutex::new(())),
            cond: Condvar::new(),
            directory_orig: d,
            directory,
            merge_directory,
            change_count,
            last_commit_change_count: AtomicU64::new(0),
            rollback_segments,
            pending_commit: None,
            pending_seq_no: AtomicI64::new(0),
            pending_commit_change_count: AtomicU64::new(0),
            files_to_commit: HashSet::new(),
            segment_infos,
            segment_infos_lock: Mutex::new(()),
            global_field_numbers,
            doc_writer,
            deleter,
            segments_to_merge: HashMap::new(),
            merge_max_num_segments: 0,
            closed: AtomicBool::new(false),
            closing: AtomicBool::new(false),
            merging_segments: HashSet::new(),
            merge_scheduler: conf.merge_scheduler(),
            merge_id_gen: AtomicU32::new(0),
            pending_merges: VecDeque::new(),
            running_merges: HashMap::new(),
            merge_exceptions: vec![],
            merge_gen: 0,
            stop_merges: false,
            flush_count: AtomicU32::new(0),
            flush_deletes_count: AtomicU32::new(0),
            reader_pool: ReaderPool::new(),
            updates_stream_lock: Mutex::new(()),
            buffered_updates_stream,
            pool_readers: AtomicBool::new(pool_readers),
            config: conf,
            pending_num_docs: Arc::new(pending_num_docs),
            full_flush_lock: Arc::new(Mutex::new(())),
            commit_lock: Mutex::new(()),
            rate_limiters,
            tragedy: None,
        })
    }

    #[allow(clippy::mut_from_ref)]
    unsafe fn writer_mut(&self, _l: &MutexGuard<()>) -> &mut IndexWriterInner<D, C, MS, MP> {
        let writer =
            self as *const IndexWriterInner<D, C, MS, MP> as *mut IndexWriterInner<D, C, MS, MP>;
        &mut *writer
    }

    fn get_reader(
        index_writer: &IndexWriter<D, C, MS, MP>,
        apply_all_deletes: bool,
        write_all_deletes: bool,
    ) -> Result<StandardDirectoryReader<D, C, MS, MP>> {
        index_writer.writer.ensure_open(true)?;

        if write_all_deletes && !apply_all_deletes {
            bail!(IllegalArgument(
                "apply_all_deletes must be true when write_all_deletes=true".into()
            ));
        }

        debug!("IW - flush at get_reader");
        let start = SystemTime::now();

        // Do this up front before flushing so that the readers
        // obtained during this flush are pooled, the first time
        // this method is called:
        index_writer
            .writer
            .pool_readers
            .store(true, Ordering::Release);
        index_writer.writer.do_before_flush();
        let mut any_changes = false;

        // for releasing a NRT reader we must ensure that DW doesn't add any
        // segments or deletes until we are done with creating the NRT
        // DirectoryReader. We release the two stage full flush after we are
        // done opening the directory reader!
        let reader = Self::do_get_reader(
            index_writer,
            apply_all_deletes,
            write_all_deletes,
            &mut any_changes,
        )?;

        if any_changes {
            let _ = Self::maybe_merge(index_writer, MergerTrigger::Explicit, None);
        }

        debug!(
            "IW - get_reader took {} ms",
            SystemTime::now()
                .duration_since(start)
                .unwrap()
                .subsec_millis()
        );
        Ok(reader)
    }

    fn do_get_reader(
        index_writer: &IndexWriter<D, C, MS, MP>,
        apply_all_deletes: bool,
        write_all_deletes: bool,
        any_changes: &mut bool,
    ) -> Result<StandardDirectoryReader<D, C, MS, MP>> {
        let _l = index_writer.writer.full_flush_lock.lock()?;

        let res = Self::flush_and_open(
            index_writer,
            apply_all_deletes,
            write_all_deletes,
            any_changes,
        );
        // Done: finish the full flush!
        index_writer.writer.doc_writer.finish_full_flush(true);
        match res {
            Ok(reader) => {
                Self::process_events(index_writer, false, true)?;
                index_writer.writer.do_after_flush();
                Ok(reader)
            }
            Err(e) => {
                error!("IW - hit error during NRT reader: {:?}", e);
                Err(e)
            }
        }
    }

    fn flush_and_open(
        index_writer: &IndexWriter<D, C, MS, MP>,
        apply_all_deletes: bool,
        write_all_deletes: bool,
        any_changes: &mut bool,
    ) -> Result<StandardDirectoryReader<D, C, MS, MP>> {
        let (changes, _) = index_writer.writer.doc_writer.flush_all_threads()?;
        *any_changes = changes;
        if !changes {
            // prevent double increment since docWriter#doFlush increments
            // the flush count if we flushed anything.
            index_writer
                .writer
                .flush_count
                .fetch_add(1, Ordering::AcqRel);
        }

        let reader = {
            // Prevent segmentInfos from changing while opening the
            // reader; in theory we could instead do similar retry logic,
            // just like we do when loading segments_N
            let l = index_writer.writer.lock.lock()?;
            *any_changes |= index_writer
                .writer
                .maybe_apply_deletes(apply_all_deletes, &l)?;
            if write_all_deletes {
                // Must move the deletes to disk:
                index_writer
                    .writer
                    .reader_pool
                    .commit(&index_writer.writer.segment_infos)?;
            }

            let r = StandardDirectoryReader::open_by_writer(
                index_writer.clone(),
                &index_writer.writer.segment_infos,
                apply_all_deletes,
                write_all_deletes,
            )?;
            debug!(
                "IW - return reader version: {}, reader: {:?} ",
                r.version(),
                &r
            );
            r
        };
        Ok(reader)
    }

    /// Confirms that the incoming index sort (if any) matches the existing index
    /// sort (if any). This is unfortunately just best effort, because it could
    /// be the old index only has flushed segments.
    fn validate_index_sort<MS1: MergeScheduler, MP1: MergePolicy>(
        config: &IndexWriterConfig<C, MS1, MP1>,
        segment_infos: &SegmentInfos<D, C>,
    ) -> Result<()> {
        if let Some(index_sort) = config.index_sort() {
            for info in &segment_infos.segments {
                if let Some(segment_sort) = info.info.index_sort() {
                    if segment_sort != index_sort {
                        bail!(IllegalArgument(format!(
                            "config and segment index sort mismatch. segment: {:?}, config: {:?}",
                            segment_sort, index_sort
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    #[inline]
    fn next_merge_id(&self) -> u32 {
        self.merge_id_gen.fetch_add(1, Ordering::AcqRel)
    }

    #[inline]
    pub fn global_field_numbers(&self) -> &Arc<FieldNumbers> {
        &self.global_field_numbers
    }

    #[inline]
    pub fn buffered_updates_stream(&self) -> &BufferedUpdatesStream<C> {
        &self.buffered_updates_stream
    }

    fn close(index_writer: &IndexWriter<D, C, MS, MP>) -> Result<()> {
        debug!(
            "IW - start close. commit on close: {}",
            index_writer.writer.config.commit_on_close
        );
        if index_writer.writer.config.commit_on_close {
            Self::shutdown(index_writer)
        } else {
            index_writer.writer.rollback()
        }
    }

    /// Gracefully closes (commits, waits for merges), but calls rollback
    /// if there's an error so the IndexWriter is always closed. This is
    /// called from `#close` when `IndexWriterConfig#commit_on_close`
    /// is *true*.
    fn shutdown(index_writer: &IndexWriter<D, C, MS, MP>) -> Result<()> {
        if index_writer.writer.pending_commit.is_some() {
            bail!(IllegalState(
                "cannot close: prepareCommit was already called with no corresponding call to \
                 commit"
                    .into()
            ));
        }

        // Ensure that only one thread actually gets to do the closing
        if index_writer.writer.should_close(true) {
            debug!("IW - now flush at close");

            if let Err(e) = Self::do_shutdown(index_writer) {
                // Be certain to close the index on any error
                let commit_lock = index_writer.writer.commit_lock.lock()?;
                if let Err(err) = index_writer.writer.rollback_internal(&commit_lock) {
                    warn!("rollback internal failed when shutdown by '{:?}'", err);
                }
                return Err(e);
            }
        }
        Ok(())
    }

    fn do_shutdown(index_writer: &IndexWriter<D, C, MS, MP>) -> Result<()> {
        IndexWriterInner::flush(index_writer, true, true)?;
        Self::wait_for_merges(index_writer)?;
        Self::commit(index_writer)?;
        let commit_lock = index_writer.writer.commit_lock.lock()?;
        index_writer.writer.rollback_internal(&commit_lock) // ie close, since we just committed
    }

    // Returns true if this thread should attempt to close, or
    // false if IndexWriter is now closed; else,
    // waits until another thread finishes closing
    fn should_close(&self, wait_for_close: bool) -> bool {
        let mut l = self.lock.lock().unwrap();
        loop {
            if !self.closed.load(Ordering::Acquire) {
                if !self.closing.load(Ordering::Acquire) {
                    // we get to close
                    self.closing.store(true, Ordering::Release);
                    return true;
                } else if !wait_for_close {
                    return false;
                } else {
                    // Another thread is presently trying to close;
                    // wait until it finishes one way (closes
                    // successfully) or another (fails to close)
                    let (loc, _) = self
                        .cond
                        .wait_timeout(l, Duration::from_millis(1000))
                        .unwrap();
                    l = loc;
                }
            } else {
                return false;
            }
        }
    }

    /// Atomically adds the segment private delete packet and publishes the flushed
    /// segments SegmentInfo to the index writer.
    fn publish_flushed_segment(
        &self,
        new_segment: FlushedSegment<D, C>,
        global_packet: Option<FrozenBufferedUpdates<C>>,
    ) -> Result<()> {
        let res = self.do_publish_flushed_segment(new_segment, global_packet);
        self.flush_count.fetch_add(1, Ordering::AcqRel);
        self.do_after_flush();
        res
    }

    fn do_publish_flushed_segment(
        &self,
        mut new_segment: FlushedSegment<D, C>,
        global_packet: Option<FrozenBufferedUpdates<C>>,
    ) -> Result<()> {
        let l = self.lock.lock()?;
        // Lock order IW -> BDS
        self.ensure_open(false)?;
        let _bl = self.updates_stream_lock.lock()?;

        debug!("publish_flushed_segment");

        if let Some(global_packet) = global_packet {
            if global_packet.any() {
                self.buffered_updates_stream.push(global_packet)?;
            }
        }

        // Publishing the segment must be synched on IW -> BDS to make the sure
        // that no merge prunes away the seg. private delete packet
        let segment_updates = new_segment.segment_updates.take();
        let next_gen = if let Some(p) = segment_updates {
            if p.any() {
                let rld = self.reader_pool.get_or_create(&new_segment.segment_info)?;
                rld.inner.lock()?.sort_map = new_segment.sort_map;
                self.buffered_updates_stream.push(p)?
            } else {
                self.buffered_updates_stream.get_next_gen()
            }
        } else {
            self.buffered_updates_stream.get_next_gen()
        };
        debug!(
            "publish sets new_segment del_gen={}, seg={}",
            next_gen, &new_segment.segment_info
        );

        new_segment
            .segment_info
            .set_buffered_deletes_gen(next_gen as i64);
        let writer = unsafe { self.writer_mut(&l) };
        writer
            .segment_infos
            .add(Arc::clone(&new_segment.segment_info));
        writer.check_point(&l)
    }

    fn publish_frozen_updates(&self, packet: FrozenBufferedUpdates<C>) -> Result<()> {
        debug_assert!(packet.any());
        let _gl = self.lock.lock()?;
        let _l = self.updates_stream_lock.lock()?;
        self.buffered_updates_stream.push(packet)?;
        Ok(())
    }

    fn rollback(&self) -> Result<()> {
        // don't call ensureOpen here: this acts like "close()" in closeable.

        // Ensure that only one thread actually gets to do the
        // closing, and make sure no commit is also in progress:
        if self.should_close(true) {
            let commit_lock = self.commit_lock.lock()?;
            self.rollback_internal(&commit_lock)
        } else {
            Ok(())
        }
    }

    fn rollback_internal(&self, commit_lock: &MutexGuard<()>) -> Result<()> {
        // Make sure no commit is running, else e.g. we can close while
        // another thread is still fsync'ing:
        let writer_mut = unsafe { self.writer_mut(commit_lock) };
        writer_mut.rollback_internal_no_commit()
    }

    fn rollback_internal_no_commit(&mut self) -> Result<()> {
        debug!("IW - rollback");

        let res = self.do_rollback_internal_no_commit();
        if res.is_err() {
            // Must not hold IW's lock while closing
            // mergeScheduler: this can lead to deadlock,
            // e.g. TestIW.testThreadInterruptDeadlock
            if let Err(e) = self.merge_scheduler.close() {
                warn!(
                    "merge_scheduler close when rollback commit faile by '{:?}'",
                    e
                );
            }
        }

        {
            let _l = self.lock.lock()?;
            if res.is_err() {
                // we tried to be nice about it: do the minimum

                // don't leak a segments_N file if there is a pending commit
                if self.pending_commit.is_some() {
                    self.pending_commit
                        .as_mut()
                        .unwrap()
                        .rollback_commit(self.directory.as_ref());
                    if let Err(e) = self
                        .deleter
                        .dec_ref_files(&self.pending_commit.as_ref().unwrap().files(false))
                    {
                        warn!(
                            "index write deleter dec ref by segment failed when rollback with '{}'",
                            e
                        );
                    }
                    self.pending_commit = None;
                }

                // close all the closeables we can (but important is reader_pool and
                // write_lock to prevent leaks)
                let _res = self.reader_pool.drop_all(false);
                let _res = self.deleter.close();
            }
            self.closed.store(true, Ordering::Release);
            self.closing.store(false, Ordering::Release);

            // so any "concurrently closing" threads wake up and see that the close has now
            // completed:
            self.cond.notify_all();
        }
        Ok(())
    }

    fn do_rollback_internal_no_commit(&mut self) -> Result<()> {
        {
            let lock = Arc::clone(&self.lock);
            let l = lock.lock()?;
            let _ = self.abort_merges(l)?;
        }
        self.rate_limiters = Arc::new(ThreadLocal::new());
        debug!("IW - rollback: done finish merges");

        // Must pre-close in case it increments changeCount so that we can then
        // set it to false before calling rollbackInternal
        self.merge_scheduler.close()?;

        self.buffered_updates_stream.clear()?;
        // mark it as closed first to prevent subsequent indexing actions/flushes
        self.doc_writer.close();
        // don't sync on IW here
        self.doc_writer.abort()?;

        {
            let _l = self.lock.lock()?;
            if self.pending_commit.is_some() {
                self.pending_commit
                    .as_mut()
                    .unwrap()
                    .rollback_commit(self.directory.as_ref());
                let res = self
                    .deleter
                    .dec_ref_files(&self.pending_commit.as_ref().unwrap().files(false));
                self.pending_commit = None;
                self.cond.notify_all();
                res?;
            }

            // Don't bother saving any changes in our segment_infos
            self.reader_pool.drop_all(false)?;

            // Keep the same segmentInfos instance but replace all
            // of its SegmentInfo instances so IFD below will remove
            // any segments we flushed since the last commit:

            self.segment_infos
                .rollback_segment_infos(self.rollback_segments.clone());
            debug!("IW - rollback segment commit infos");

            // Ask deleter to locate unreferenced files & remove
            // them ... only when we are not experiencing a tragedy, else
            // these methods throw ACE:
            if self.tragedy.is_none() {
                self.deleter.checkpoint(&self.segment_infos, false)?;
                self.deleter.refresh()?;
                self.deleter.close()?;
            }

            self.last_commit_change_count
                .store(self.change_count(), Ordering::Release);

            // Must set closed while inside same sync block where we call deleter.refresh, else
            // concurrent threads may try to sneak a flush in, after we leave this sync block
            // and before we enter the sync block in the finally clause below that sets closed:
            self.closed.store(true, Ordering::Release);
        }
        Ok(())
    }

    fn delete_all(index_writer: &IndexWriter<D, C, MS, MP>) -> Result<u64> {
        index_writer.writer.ensure_open(true)?;
        let seq_no: u64;
        {
            let l = index_writer.writer.full_flush_lock.lock()?;
            // TODO this did not locks the rlds, may cause error?
            let aborted_doc_count = index_writer.writer.doc_writer.lock_and_abort_all(&l)?;
            index_writer
                .writer
                .pending_num_docs
                .fetch_sub(aborted_doc_count as i64, Ordering::AcqRel);

            Self::process_events(index_writer, false, true)?;

            {
                let mut _gl = index_writer.writer.lock.lock()?;
                let writer_mut = unsafe { index_writer.writer.writer_mut(&_gl) };
                // Abort any running merges
                let _gl = writer_mut.abort_merges(_gl)?;
                // Let merges run again
                writer_mut.stop_merges = false;
                // Remove all segments
                index_writer.writer.pending_num_docs.fetch_sub(
                    index_writer.writer.segment_infos.total_max_doc() as i64,
                    Ordering::AcqRel,
                );
                writer_mut.segment_infos.clear();
                // Ask deleter to locate unreferenced files & remove them:
                writer_mut
                    .deleter
                    .checkpoint(&index_writer.writer.segment_infos, false)?;
                // don't refresh the deleter here since there might
                // be concurrent indexing requests coming in opening
                // files on the directory after we called DW#abort()
                // if we do so these indexing requests might hit FNF exceptions.
                // We will remove the files incrementally as we go...

                // don't bother saving any changes in our segment_infos
                index_writer.writer.reader_pool.drop_all(false)?;
                // Mask that the index has changes
                index_writer
                    .writer
                    .change_count
                    .fetch_add(1, Ordering::AcqRel);
                writer_mut.segment_infos.changed();
                index_writer.writer.global_field_numbers.clear();

                seq_no = index_writer
                    .writer
                    .doc_writer
                    .delete_queue
                    .next_sequence_number();
                writer_mut.doc_writer.last_seq_no = seq_no;
            }
        }
        Ok(seq_no)
    }

    /// Called whenever the SegmentInfos has been updated and the index files
    /// referenced exist (correctly) in the index directory.
    fn check_point(&mut self, lock: &MutexGuard<()>) -> Result<()> {
        self.changed(lock);
        self.deleter.checkpoint(&self.segment_infos, false)
    }

    fn changed(&mut self, _lock: &MutexGuard<()>) {
        self.change_count.fetch_add(1, Ordering::AcqRel);
        self.segment_infos.changed();
    }

    fn num_deleted_docs(&self, info: &SegmentCommitInfo<D, C>) -> u32 {
        // self.ensure_open(false);
        let mut del_count = info.del_count() as u32;

        if let Some(rld) = self.reader_pool.get(info) {
            del_count += rld.pending_delete_count();
        }
        del_count
    }

    fn change_count(&self) -> u64 {
        self.change_count.load(Ordering::Acquire)
    }

    fn do_before_flush(&self) {}

    ///
    // A hook for extending classes to execute operations after pending added and
    // deleted documents have been flushed to the Directory but before the change
    // is committed (new segments_N file written).
    //
    fn do_after_flush(&self) {}

    /// Used internally to throw an `AlreadyClosedException` if this
    /// IndexWriter has been closed or is in the process of closing.
    fn ensure_open(&self, fail_if_closing: bool) -> Result<()> {
        if self.closed.load(Ordering::Acquire)
            || (fail_if_closing && self.closing.load(Ordering::Acquire))
        {
            bail!(AlreadyClosed("this IndexWriter is closed".into()));
        }
        Ok(())
    }

    fn apply_deletes_and_purge(
        index_writer: &IndexWriter<D, C, MS, MP>,
        force_purge: bool,
    ) -> Result<()> {
        let res = Self::purge(index_writer, force_purge);
        let _any_changes = {
            let l = index_writer.writer.lock.lock()?;
            index_writer.writer.apply_all_deletes_and_update(&l)?
        };
        index_writer
            .writer
            .flush_count
            .fetch_add(1, Ordering::AcqRel);
        match res {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    fn do_after_segment_flushed(
        index_writer: &IndexWriter<D, C, MS, MP>,
        _trigger_merge: bool,
        force_purge: bool,
    ) -> Result<()> {
        let res = Self::purge(index_writer, force_purge);
        match res {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Record that the files referenced by this `SegmentInfos` are still in use.
    fn inc_ref_deleter(&self, segment_infos: &SegmentInfos<D, C>) -> Result<()> {
        self.ensure_open(true)?;
        self.deleter.inc_ref_files(&segment_infos.files(false));
        Ok(())
    }

    fn dec_ref_deleter(&self, segment_infos: &SegmentInfos<D, C>) -> Result<()> {
        self.ensure_open(true)?;
        self.deleter.dec_ref_files(&segment_infos.files(false))
    }

    fn process_events(
        index_writer: &IndexWriter<D, C, MS, MP>,
        trigger_merge: bool,
        force_purge: bool,
    ) -> Result<bool> {
        let mut processed = false;
        if index_writer.writer.tragedy.is_none() {
            while let Ok(event) = index_writer.writer.doc_writer.events.pop() {
                processed = true;
                event.process(index_writer, trigger_merge, force_purge)?;
            }
        }

        Ok(processed)
    }

    fn purge(index_writer: &IndexWriter<D, C, MS, MP>, forced: bool) -> Result<u32> {
        index_writer
            .writer
            .doc_writer
            .purge_buffer(index_writer, forced)
    }

    fn has_uncommitted_changes(&self) -> bool {
        self.change_count() != self.last_commit_change_count.load(Ordering::Acquire)
            || self.doc_writer.any_changes()
            || self.buffered_updates_stream.any()
    }

    fn commit(index_writer: &IndexWriter<D, C, MS, MP>) -> Result<i64> {
        debug!("IW - commit: start");

        let mut do_maybe_merge = false;
        let seq_no: i64;
        {
            let l = index_writer.writer.commit_lock.lock()?;
            let writer = unsafe { index_writer.writer.writer_mut(&l) };

            index_writer.writer.ensure_open(false)?;

            debug!("IW - commit: enter lock");

            seq_no = if index_writer.writer.pending_commit.is_none() {
                debug!("IW - commit: now prepare");
                writer.prepare_commit_internal(&mut do_maybe_merge, index_writer, &l)?
            } else {
                debug!("IW - commit: already prepared");
                index_writer.writer.pending_seq_no.load(Ordering::Acquire)
            };

            writer.finish_commit(&l)?;
        }

        Ok(seq_no)
    }

    fn prepare_commit_internal(
        &mut self,
        do_maybe_merge: &mut bool,
        index_writer: &IndexWriter<D, C, MS, MP>,
        commit_lock: &MutexGuard<()>,
    ) -> Result<i64> {
        // self.start_commit_time = SystemTime::now();
        self.ensure_open(false)?;
        debug!("IW - prepare commit: flush");

        if let Some(ref tragedy) = self.tragedy {
            bail!(IllegalState(format!(
                "this writer hit an unrecoverable error; cannot commit: {:?}",
                tragedy
            )));
        }

        if self.pending_commit.is_some() {
            bail!(IllegalState(
                "prepareCommit was already called with no corresponding call to commit".into()
            ));
        }

        self.do_before_flush();
        let mut seq_no = 0u64;
        let to_commit: SegmentInfos<D, C>;
        let mut any_segments_flushed = false;

        // This is copied from doFlush, except it's modified to
        // clone & incRef the flushed SegmentInfos inside the
        // sync block:
        {
            let full_flush_lock = Arc::clone(&self.full_flush_lock);
            let _fl = full_flush_lock.lock()?;
            let mut flush_success = true;

            let res = self.prepare_commit_internal_inner(
                index_writer,
                &mut seq_no,
                &mut flush_success,
                &mut any_segments_flushed,
            );
            if res.is_err() {
                debug!("IW - hit error during perpare commit");
            }
            // Done: finish the full flush
            self.doc_writer.finish_full_flush(flush_success);
            self.do_after_flush();
            match res {
                Ok(infos) => {
                    to_commit = infos;
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }

        if any_segments_flushed {
            *do_maybe_merge = true;
        }
        let res = self.start_commit(to_commit, commit_lock);
        if res.is_err() {
            let lock = Arc::clone(&self.lock);
            let _l = lock.lock().unwrap();
            if !self.files_to_commit.is_empty() {
                self.deleter.dec_ref_files_no_error(&self.files_to_commit);
                self.files_to_commit.clear();
            }
        }
        match res {
            Ok(()) => {
                if self.pending_commit.is_none() {
                    Ok(-1)
                } else {
                    Ok(seq_no as i64)
                }
            }
            Err(e) => Err(e),
        }
    }

    fn prepare_commit_internal_inner(
        &mut self,
        index_writer: &IndexWriter<D, C, MS, MP>,
        seq_no: &mut u64,
        flush_success: &mut bool,
        any_segment_flushed: &mut bool,
    ) -> Result<SegmentInfos<D, C>> {
        let (any_flushed, no) = self.doc_writer.flush_all_threads()?;
        *seq_no = no;
        *any_segment_flushed = any_flushed;
        if !any_flushed {
            // prevent double increment since docWriter#doFlush increments
            // the flush count if we flushed anything.
            self.flush_count.fetch_add(1, Ordering::AcqRel);
        }
        Self::process_events(index_writer, false, true)?;
        *flush_success = true;

        let lock = Arc::clone(&self.lock);
        let gl = lock.lock()?;
        self.maybe_apply_deletes(true, &gl)?;
        self.reader_pool.commit(&self.segment_infos)?;
        if self.change_count() != self.last_commit_change_count.load(Ordering::Acquire) {
            // There are changes to commit, so we will write a new segments_N in startCommit.
            // The act of committing is itself an NRT-visible change (an NRT reader that was
            // just opened before this should see it on reopen) so we increment changeCount
            // and segments version so a future NRT reopen will see the change:
            self.change_count.fetch_add(1, Ordering::AcqRel);
            self.segment_infos.changed();
        }

        // TODO currently not support SegmentInfos#user_data
        // if self.commit_user_data.is_some() {}

        // Must clone the segmentInfos while we still
        // hold fullFlushLock and while sync'd so that
        // no partial changes (eg a delete w/o
        // corresponding add from an updateDocument) can
        // sneak into the commit point:
        let to_commit = self.segment_infos.clone();

        self.pending_commit_change_count
            .store(self.change_count(), Ordering::Release);

        // This protects the segmentInfos we are now going
        // to commit.  This is important in case, eg, while
        // we are trying to sync all referenced files, a
        // merge completes which would otherwise have
        // removed the files we are now syncing.
        self.files_to_commit = to_commit.files(false);
        self.deleter.inc_ref_files(&self.files_to_commit);
        Ok(to_commit)
    }

    /// Walk through all files referenced by the current
    /// segmentInfos and ask the Directory to sync each file,
    /// if it wasn't already.  If that succeeds, then we
    /// prepare a new segments_N file but do not fully commit
    /// it.
    fn start_commit(
        &mut self,
        to_sync: SegmentInfos<D, C>,
        commit_lock: &MutexGuard<()>,
    ) -> Result<()> {
        debug_assert!(self.pending_commit.is_none());

        if let Some(ref tragedy) = self.tragedy {
            bail!(IllegalState(format!(
                "this writer hit an unrecoverable error; cannot commit: {:?}",
                tragedy
            )));
        }

        debug!("IW - start_commit(): start");
        {
            let lock = Arc::clone(&self.lock);
            let _l = lock.lock()?;
            if self.last_commit_change_count.load(Ordering::Acquire) > self.change_count() {
                bail!(IllegalState(
                    "change_count is smaller than last_commit_change_count".into()
                ));
            }

            if self.pending_commit_change_count.load(Ordering::Acquire) > self.change_count() {
                debug!("IW - skip start_commit(): no changes pending");
                let res = self.deleter.dec_ref_files(&self.files_to_commit);
                self.files_to_commit.clear();
                return res;
            }
            // debug!("IW - start_commit");

            debug_assert!(self.files_exist(&to_sync));
        }

        let mut pending_commit_set = false;
        let last_gen = to_sync.last_generation;
        let gen = to_sync.generation;
        let err = self.start_commit_inner(to_sync, &mut pending_commit_set);
        {
            let lock = Arc::clone(&self.lock);
            let _l = lock.lock()?;
            // Have our master segmentInfos record the
            // generations we just prepared.  We do this
            // on error or success so we don't
            // double-write a segments_N file.
            self.segment_infos.update_generation(last_gen, gen);

            if !pending_commit_set {
                debug!("IW - hit error committing segments file");

                // hit error
                self.deleter.dec_ref_files_no_error(&self.files_to_commit);
                self.files_to_commit.clear();
            }
        }
        if let Err(e) = err {
            self.tragic_event(e, "start_commit", Some(commit_lock))?;
        }
        Ok(())
    }

    fn start_commit_inner(
        &mut self,
        mut to_sync: SegmentInfos<D, C>,
        pending_commit_set: &mut bool,
    ) -> Result<()> {
        {
            let lock = Arc::clone(&self.lock);
            let _l = lock.lock()?;
            debug_assert!(self.pending_commit.is_none());
            // Exception here means nothing is prepared
            // (this method unwinds everything it did on
            // an exception)
            to_sync.prepare_commit(self.directory.as_ref())?;

            debug!(
                "IW - start_commit: wrote pending segment file '{}' ",
                file_name_from_generation(
                    INDEX_FILE_PENDING_SEGMENTS,
                    "",
                    to_sync.generation as u64,
                )
            );

            *pending_commit_set = true;
            self.pending_commit = Some(to_sync);
        }

        let files_to_sync: HashSet<String> = self.pending_commit.as_ref().unwrap().files(false);
        if let Err(e) = self.directory.sync(&files_to_sync) {
            *pending_commit_set = false;
            self.pending_commit
                .as_mut()
                .unwrap()
                .rollback_commit(self.directory.as_ref());
            self.pending_commit = None;
            return Err(e);
        }

        debug!("IW - done all syncs: {:?}", &files_to_sync);
        Ok(())
    }

    fn finish_commit(&mut self, commit_lock: &MutexGuard<()>) -> Result<()> {
        let mut commit_completed = false;

        let res = self.try_finish_commit(&mut commit_completed);

        if let Err(e) = res {
            if commit_completed {
                {
                    let l = self.lock.lock()?;

                    // It's possible you could have a really bad day
                    if self.tragedy.is_some() {
                        bail!(e);
                    }

                    let writer = unsafe { self.writer_mut(&l) };
                    writer.tragedy = Some(e);
                }

                // if we are already closed (e.g. called by rollback), this will be a no-op.
                if self.should_close(false) {
                    self.rollback_internal(commit_lock)?;
                }

                bail!(IllegalState(format!(
                    "this writer hit an unrecoverable error; {:?}",
                    &self.tragedy
                )))
            } else {
                return Err(e);
            }
        }

        debug!("IW - commit: done");
        Ok(())
    }

    fn try_finish_commit(&mut self, commit_completed: &mut bool) -> Result<()> {
        let lock = Arc::clone(&self.lock);
        let _l = lock.lock()?;
        self.ensure_open(false)?;

        if let Some(ref tragedy) = self.tragedy {
            bail!(IllegalState(format!(
                "this writer hit an unrecoverable error; cannot complete commit: {:?}",
                tragedy
            )));
        }

        if self.pending_commit.is_some() {
            let mut res = self.do_finish_commit(commit_completed);
            self.cond.notify_all();
            if res.is_ok() {
                // all is good
                res = self.deleter.dec_ref_files(&self.files_to_commit);
            } else {
                // exc happened in finishCommit: not a tragedy
                self.deleter.dec_ref_files_no_error(&self.files_to_commit);
            }
            self.pending_commit = None;
            self.files_to_commit.clear();
            if res.is_err() {
                return res;
            }
        } else {
            debug!("IW - commit: pendingCommit is None; skip");
        }
        Ok(())
    }

    fn do_finish_commit(&mut self, commit_completed: &mut bool) -> Result<()> {
        debug!("IW - commit: pending_commit is not none");

        let committed_segments_file = self
            .pending_commit
            .as_mut()
            .unwrap()
            .finish_commit(self.directory.as_ref())?;

        // we committed, if anything goes wrong after this, we are
        // screwed and it's a tragedy:
        *commit_completed = true;

        debug!(
            "IW - commit: done writing segments file {}",
            &committed_segments_file
        );

        // NOTE: don't use this.checkpoint() here, because
        // we do not want to increment changeCount:
        self.deleter
            .checkpoint(self.pending_commit.as_ref().unwrap(), true)?;

        // Carry over generation to our master SegmentInfos:
        let last_gen = self.pending_commit.as_ref().unwrap().last_generation;
        let gen = self.pending_commit.as_ref().unwrap().generation;
        self.segment_infos.update_generation(last_gen, gen);

        self.last_commit_change_count.store(
            self.pending_commit_change_count.load(Ordering::Acquire),
            Ordering::Release,
        );
        self.rollback_segments = self
            .pending_commit
            .as_ref()
            .unwrap()
            .create_backup_segment_infos();
        Ok(())
    }

    // called only from assert
    fn files_exist(&self, to_syc: &SegmentInfos<D, C>) -> bool {
        let files = to_syc.files(false);

        for file_name in &files {
            // If this trips it means we are missing a call to
            // .checkpoint somewhere, because by the time we
            // are called, deleter should know about every
            // file referenced by the current head
            // segmentInfos:
            assert!(self.deleter.exists(file_name));
        }
        true
    }

    fn flush(
        index_writer: &IndexWriter<D, C, MS, MP>,
        trigger_merge: bool,
        apply_all_deletes: bool,
    ) -> Result<()> {
        // NOTE: this method cannot be sync'd because maybe_merge() in turn calls
        // mergeScheduler.merge which in turn can take a long time to run and we
        // don't want to hold the lock for that.  In the case of ConcurrentMergeScheduler
        // this can lead to deadlock when it stalls due to too many running merges.

        // We can be called during close, when closing==true, so we must pass false to ensureOpen:
        index_writer.writer.ensure_open(false)?;
        if Self::do_flush(index_writer, apply_all_deletes)? && trigger_merge {
            Self::maybe_merge(index_writer, MergerTrigger::Explicit, None)?;
        }
        Ok(())
    }

    /// Returns true a segment was flushed or deletes were applied.
    fn do_flush(index_writer: &IndexWriter<D, C, MS, MP>, apply_deletes: bool) -> Result<bool> {
        if let Some(ref tragedy) = index_writer.writer.tragedy {
            bail!(IllegalState(format!(
                "this writer hit an unrecoverable error; cannot flush: {:?}",
                tragedy
            )));
        }
        index_writer.writer.do_before_flush();

        debug!("IW - start flush: apply_all_deletes={}", apply_deletes);
        // debug!("IW - index before flush");

        let mut any_changes = false;
        {
            let _l = index_writer.writer.full_flush_lock.lock()?;

            let res = index_writer.writer.doc_writer.flush_all_threads();
            if let Ok((any_flush, _)) = &res {
                any_changes = *any_flush;
                if !any_changes {
                    // flush_count is incremented in flush_all_threads
                    index_writer
                        .writer
                        .flush_count
                        .fetch_add(1, Ordering::AcqRel);
                }
            }
            index_writer.writer.doc_writer.finish_full_flush(true);
            if let Err(e) = Self::process_events(index_writer, false, true) {
                if res.is_ok() {
                    return Err(e);
                } else {
                    error!("process events failed after do_flush by: '{:?}'", e);
                }
            }

            if let Err(e) = res {
                return Err(e);
            }
        }

        {
            let l = index_writer.writer.lock.lock()?;
            any_changes |= index_writer.writer.maybe_apply_deletes(apply_deletes, &l)?;
            index_writer.writer.do_after_flush();
        }
        Ok(any_changes)
    }

    // the lock guard is refer to `self.lock`
    fn maybe_apply_deletes(&self, apply_all_deletes: bool, l: &MutexGuard<()>) -> Result<bool> {
        if apply_all_deletes {
            debug!("IW - apply all deletes during flush");
            return self.apply_all_deletes_and_update(l);
        }
        debug!(
            "IW - don't apply deletes now del_term_count={}",
            self.buffered_updates_stream.num_terms(),
        );
        Ok(false)
    }

    fn apply_all_deletes_and_update(&self, l: &MutexGuard<()>) -> Result<bool> {
        self.flush_deletes_count.fetch_add(1, Ordering::AcqRel);

        debug!(
            "IW: now apply all deletes for all segments, max_doc={}",
            self.doc_writer.num_docs() + self.segment_infos.total_max_doc() as u32
        );

        let writer_mut = unsafe { self.writer_mut(l) };

        let result = self
            .buffered_updates_stream
            .apply_deletes_and_updates(&self.reader_pool, &self.segment_infos.segments)?;
        if result.any_deletes {
            writer_mut.check_point(l)?;
        }

        if !result.all_deleted.is_empty() {
            debug!("IW: drop 100% deleted segments.");

            for info in result.all_deleted {
                // If a merge has already registered for this
                // segment, we leave it in the readerPool; the
                // merge will skip merging it and will then drop
                // it once it's done:

                if !self.merging_segments.contains(&info.info.name) {
                    writer_mut.segment_infos.remove(&info);
                    self.pending_num_docs
                        .fetch_sub(info.info.max_doc() as i64, Ordering::AcqRel);
                    self.reader_pool.drop(&info)?;
                }
            }
            writer_mut.check_point(l)?;
        }

        self.buffered_updates_stream.prune(&self.segment_infos);
        Ok(result.any_deletes)
    }

    /// Cleans up residuals from a segment that could not be entirely flushed due to a error
    fn flush_failed(&self, info: &SegmentInfo<D, C>) -> Result<()> {
        let mut files = HashSet::new();
        for f in info.files() {
            files.insert(f.clone());
        }
        self.deleter.delete_new_files(&files)
    }

    fn update_documents<F: Fieldable>(
        index_writer: &IndexWriter<D, C, MS, MP>,
        docs: Vec<Vec<F>>,
        term: Option<Term>,
    ) -> Result<u64> {
        index_writer.writer.ensure_open(true)?;

        let (seq_no, changed) = index_writer
            .writer
            .doc_writer
            .update_documents(docs, term)?;
        if changed {
            Self::process_events(index_writer, true, false)?;
        }

        Ok(seq_no)
    }

    fn delete_documents_by_terms(
        index_writer: &IndexWriter<D, C, MS, MP>,
        terms: Vec<Term>,
    ) -> Result<u64> {
        index_writer.writer.ensure_open(true)?;

        let (seq_no, changed) = index_writer.writer.doc_writer.delete_terms(terms);
        if changed {
            Self::process_events(index_writer, true, false)?;
        }
        Ok(seq_no)
    }

    /// Deletes the document(s) matching any of the provided queries.
    /// All given deletes are applied and flushed atomically at the same time.
    ///
    /// @return The <a href="#sequence_number">sequence number</a>
    /// for this operation
    ///
    /// @param queries array of queries to identify the documents
    /// to be deleted
    /// @throws CorruptIndexException if the index is corrupt
    /// @throws IOException if there is a low-level IO error
    fn delete_documents_by_queries(
        index_writer: &IndexWriter<D, C, MS, MP>,
        queries: Vec<Arc<dyn Query<C>>>,
    ) -> Result<u64> {
        index_writer.writer.ensure_open(true)?;

        // LUCENE-6379: Specialize MatchAllDocsQuery
        for q in &queries {
            if q.as_any().is::<MatchAllDocsQuery>() {
                return Self::delete_all(index_writer);
            }
        }

        let (seq_no, changed) = index_writer.writer.doc_writer.delete_queries(queries);
        if changed {
            Self::process_events(index_writer, true, false)?;
        }
        Ok(seq_no)
    }

    fn update_document<F: Fieldable>(
        index_writer: &IndexWriter<D, C, MS, MP>,
        doc: Vec<F>,
        term: Option<Term>,
    ) -> Result<u64> {
        index_writer.writer.ensure_open(true)?;
        let (seq_no, changed) = index_writer.writer.doc_writer.update_document(doc, term)?;
        if changed {
            Self::process_events(index_writer, true, false)?;
        }

        Ok(seq_no)
    }

    /// Updates a document's `NumericDocValues` for <code>field</code> to the
    /// given <code>value</code>. You can only update fields that already exist in
    /// the index, not add new fields through this method.
    ///
    /// @param term
    ///          the term to identify the document(s) to be updated
    /// @param field
    ///          field name of the `NumericDocValues` field
    /// @param value
    ///          new value for the field
    ///
    /// @return The <a href="#sequence_number">sequence number</a>
    /// for this operation
    ///
    /// @throws CorruptIndexException
    ///           if the index is corrupt
    /// @throws IOException
    ///           if there is a low-level IO error
    #[allow(dead_code)]
    fn update_numeric_doc_value(
        index_writer: &IndexWriter<D, C, MS, MP>,
        term: Term,
        field: &str,
        value: i64,
    ) -> Result<u64> {
        index_writer.writer.ensure_open(true)?;

        let dv_type = index_writer
            .writer
            .global_field_numbers
            .get_doc_values_type(field)?;
        if dv_type.is_none()
            || (dv_type != Some(DocValuesType::Numeric)
                && dv_type != Some(DocValuesType::SortedNumeric))
        {
            bail!(IllegalArgument(format!("invalid field [{}]", field)));
        }

        if let Some(sort_field) = index_writer.writer.config.index_sort() {
            let sort_field = sort_field.get_sort();
            for f in sort_field {
                if f.field() == field {
                    bail!(IllegalArgument(format!(
                        "can't update sort field [{}]",
                        field
                    )));
                }
            }
        }

        let (seq, changed) = index_writer.writer.doc_writer.update_doc_values(Arc::new(
            NumericDocValuesUpdate::new(term, field.to_string(), dv_type.unwrap(), value, None),
        ))?;
        if changed {
            Self::process_events(index_writer, true, false)?;
        }
        Ok(seq)
    }

    pub fn new_segment_name(&self) -> String {
        // Cannot synchronize on IndexWriter because that causes deadlock
        let _l = self.segment_infos_lock.lock().unwrap();
        // Important to increment changeCount so that the
        // segmentInfos is written on close.  Otherwise we
        // could close, re-open and re-return the same segment
        // name that was previously returned which can cause
        // problems at least with ConcurrentMergeScheduler.
        let writer =
            self as *const IndexWriterInner<D, C, MS, MP> as *mut IndexWriterInner<D, C, MS, MP>;
        let count = unsafe {
            (*writer).segment_infos.changed();
            let counter = self.segment_infos.counter;
            (*writer).segment_infos.counter += 1;
            counter
        };
        format!("_{}", to_base36(count as u64))
    }

    /// NOTE: this method creates a compound file for all files returned by
    /// info.files(). While, generally, this may include separate norms and
    /// deletion files, this SegmentInfo must not reference such files when this
    /// method is called, because they are not allowed within a compound file.
    pub fn create_compound_file<DW: Directory, T: Deref<Target = DW>>(
        &self,
        directory: &TrackingDirectoryWrapper<DW, T>,
        info: &mut SegmentInfo<D, C>,
        context: &IOContext,
    ) -> Result<()> {
        // maybe this check is not needed, but why take the risk?
        if !directory.create_files().is_empty() {
            bail!(IllegalState(
                "pass a clean tracking dir for CFS creation".into()
            ));
        }

        debug!("IW: create compound file for segment: {}", &info.name);

        let res = info
            .codec()
            .compound_format()
            .write(directory, info, context);
        if let Err(err) = res {
            // Safe: these files must exist
            if let Err(e) = self.delete_new_files(&directory.create_files()) {
                error!(
                    "clean up files when create compound file failed, error occur: {:?}",
                    e
                );
            }
            return Err(err);
        }

        // Replace all previous files with the CFS/CFE files:
        info.set_files(&directory.create_files())?;
        Ok(())
    }

    fn nrt_is_current(&self, infos: &SegmentInfos<D, C>) -> bool {
        let _l = self.lock.lock().unwrap();
        infos.version == self.segment_infos.version
            && !self.doc_writer.any_changes()
            && !self.buffered_updates_stream.any()
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    fn is_open(&self) -> bool {
        !self.closing.load(Ordering::Acquire) || !self.is_closed()
    }

    // Tries to delete the given files if unreferenced
    fn delete_new_files(&self, files: &HashSet<String>) -> Result<()> {
        self.deleter.delete_new_files(files)
    }

    /// Forces merge policy to merge segments until there are
    /// max_num_segments. The actual merges to be
    /// executed are determined by the `MergePolicy`.
    fn force_merge(
        index_writer: &IndexWriter<D, C, MS, MP>,
        max_num_segments: u32,
        do_wait: bool,
    ) -> Result<()> {
        index_writer.writer.ensure_open(true)?;

        if max_num_segments < 1 {
            bail!(IllegalArgument(format!(
                "max_num_segments must be >= 1, got {}",
                max_num_segments
            )));
        }

        trace!("IW - force_merge: flush at force merge");

        Self::flush(index_writer, true, true)?;
        {
            let l = index_writer.writer.lock.lock()?;
            let writer_mut = unsafe { index_writer.writer.writer_mut(&l) };
            writer_mut.reset_merge_exceptions(&l);
            writer_mut.segments_to_merge.clear();
            for info in &index_writer.writer.segment_infos.segments {
                writer_mut.segments_to_merge.insert(Arc::clone(info), true);
            }
            writer_mut.merge_max_num_segments = max_num_segments;

            // Now mark all pending & running merges for forced merge:
            for merge in &mut writer_mut.pending_merges {
                merge.max_num_segments.set(Some(max_num_segments));
                writer_mut
                    .segments_to_merge
                    .insert(Arc::clone(merge.info.as_ref().unwrap()), true);
            }
            let new_running_merges = HashMap::with_capacity(writer_mut.running_merges.len());
            let mut running_merges =
                mem::replace(&mut writer_mut.running_merges, new_running_merges);
            for (_, merge) in running_merges.drain() {
                merge.max_num_segments.set(Some(max_num_segments));
                writer_mut
                    .segments_to_merge
                    .insert(Arc::clone(merge.info.as_ref().unwrap()), true);
                writer_mut.running_merges.insert(merge.id, merge);
            }
        }
        Self::maybe_merge(
            index_writer,
            MergerTrigger::Explicit,
            Some(max_num_segments),
        )?;

        if do_wait {
            let mut l = index_writer.writer.lock.lock()?;
            loop {
                if let Some(ref tragedy) = index_writer.writer.tragedy {
                    bail!(IllegalState(format!(
                        "this writer hit an unrecoverable error; cannot complete forceMerge: {:?}",
                        tragedy
                    )));
                }
                if !index_writer.writer.merge_exceptions.is_empty() {
                    // Forward any exceptions in background merge
                    // threads to the current thread:
                    for merge in &index_writer.writer.merge_exceptions {
                        if merge.max_num_segments.get().is_some() {
                            bail!(RuntimeError("background merge hit exception".into()));
                        }
                    }
                }

                if index_writer.writer.max_num_segments_merges_pending(&l) {
                    let (guard, _) = index_writer
                        .writer
                        .cond
                        .wait_timeout(l, Duration::from_millis(1000))?;
                    l = guard;
                } else {
                    break;
                }
            }

            // If close is called while we are still
            // running, throw an exception so the calling
            // thread will know merging did not complete
            index_writer.writer.ensure_open(true)?;
        }
        // NOTE: in the ConcurrentMergeScheduler case, when
        // doWait is false, we can return immediately while
        // background threads accomplish the merging
        Ok(())
    }

    /// Returns true if any merges in pendingMerges or
    /// runningMerges are maxNumSegments merges.
    fn max_num_segments_merges_pending(&self, _lock: &MutexGuard<()>) -> bool {
        self.pending_merges
            .iter()
            .any(|m| m.max_num_segments.get().is_some())
            || self
                .running_merges
                .values()
                .any(|m| m.max_num_segments.get().is_some())
    }

    // lock is self.lock's guard
    fn reset_merge_exceptions(&mut self, _lock: &MutexGuard<()>) {
        self.merge_exceptions = vec![];
        self.merge_gen += 1;
    }

    /// Checks whether this merge involves any segments
    /// already participating in a merge.  If not, this merge
    /// is "registered", meaning we record that its segments
    /// are now participating in a merge, and true is
    /// returned.  Else (the merge conflicts) false is
    /// returned.
    fn register_merge(
        &mut self,
        mut merge: OneMerge<D, C>,
        _lock: &MutexGuard<()>,
    ) -> Result<bool> {
        if merge.register_done {
            return Ok(true);
        }

        debug_assert!(!merge.segments.is_empty());
        if self.stop_merges {
            merge.rate_limiter.set_abort();
            bail!(Index(MergeAborted("merge is abort!".into())));
        }

        let mut is_external = false;
        for info in &merge.segments {
            if self.merging_segments.contains(&info.info.name) {
                return Ok(false);
            }
            if !self.segment_infos.segments.contains(info) {
                return Ok(false);
            }
            if info.info.directory.as_ref() as *const D != self.directory_orig.as_ref() as *const D
            {
                is_external = true;
            }
            if self.segments_to_merge.contains_key(info) {
                merge
                    .max_num_segments
                    .set(Some(self.merge_max_num_segments));
            }
        }

        for info in &merge.segments {
            if !self.segment_infos.segments.contains(info) {
                bail!(
                    "MergeError: MergePolicy selected a segment '{}' that is not in the current \
                     index",
                    &info.info.name
                );
            }
        }

        merge.merge_gen = self.merge_gen;
        merge.is_external = is_external;

        // OK it does not conflict; now record that this merge is running
        // (while synchronized) to avoid race condition where two conflicting
        // merges from different threads, start
        for info in &merge.segments {
            self.merging_segments.insert(info.info.name.clone());
        }

        debug_assert_eq!(merge.estimated_merge_bytes.read(), 0);
        debug_assert_eq!(merge.total_merge_bytes, 0);

        for info in &merge.segments {
            if info.info.max_doc > 0 {
                let del_count = self.num_deleted_docs(info.as_ref());
                debug_assert!((del_count as i32) <= info.info.max_doc);
                let total_size = info.size_in_bytes();
                let del_ratio = del_count as f64 / info.info.max_doc as f64;
                merge
                    .estimated_merge_bytes
                    .update(|bytes| *bytes += (total_size as f64 * (1.0 - del_ratio)) as u64);
                merge.total_merge_bytes = total_size as u64;
            }
        }

        // Merge is now registered
        merge.register_done = true;

        self.pending_merges.push_back(merge);

        Ok(true)
    }

    fn next_merge(&self) -> Option<OneMerge<D, C>> {
        let l = self.lock.lock().unwrap();
        let writer_mut = unsafe { self.writer_mut(&l) };
        if let Some(one_merge) = writer_mut.pending_merges.pop_front() {
            // Advance the merge from pending to running
            writer_mut
                .running_merges
                .insert(one_merge.id, one_merge.running_info());
            Some(one_merge)
        } else {
            None
        }
    }

    pub fn maybe_merge(
        index_writer: &IndexWriter<D, C, MS, MP>,
        trigger: MergerTrigger,
        max_num_segments: Option<u32>,
    ) -> Result<()> {
        index_writer.writer.ensure_open(false)?;

        let new_merges_found = {
            let l = index_writer.writer.lock.lock()?;
            let writer = unsafe { index_writer.writer.writer_mut(&l) };
            writer.update_pending_merges(trigger, max_num_segments, index_writer, &l)?
        };
        index_writer
            .writer
            .merge_scheduler
            .merge(index_writer, trigger, new_merges_found)
    }

    fn update_pending_merges(
        &mut self,
        trigger: MergerTrigger,
        max_num_segments: Option<u32>,
        index_writer: &IndexWriter<D, C, MS, MP>,
        l: &MutexGuard<()>,
    ) -> Result<bool> {
        // In case infoStream was disabled on init, but then enabled at some
        // point, try again to log the config here:
        // self.message_state();

        debug_assert!(max_num_segments.is_none() || *max_num_segments.as_ref().unwrap() > 0);
        if self.stop_merges {
            return Ok(false);
        }

        // Do not start new merges if disaster struck
        if self.tragedy.is_some() {
            return Ok(false);
        }

        let mut spec: Option<MergeSpecification<D, C>>;
        if let Some(max_num_segments) = max_num_segments {
            debug_assert!(
                trigger == MergerTrigger::Explicit || trigger == MergerTrigger::MergeFinished
            );
            spec = self.config.merge_policy().find_forced_merges(
                &self.segment_infos,
                max_num_segments,
                &self.segments_to_merge,
                index_writer,
            )?;
            if let Some(ref mut spec) = spec {
                for merge in &mut spec.merges {
                    merge.max_num_segments.set(Some(max_num_segments));
                }
            }
        } else {
            spec = self.config.merge_policy().find_merges(
                trigger,
                &self.segment_infos,
                index_writer,
            )?;
        }
        let new_merges_found = spec.is_some();
        if let Some(ref mut spec) = spec {
            let merges = mem::replace(&mut spec.merges, vec![]);
            for merge in merges {
                self.register_merge(merge, &l)?;
            }
        }

        Ok(new_merges_found)
    }

    /// Merges the indicated segments, replacing them in the stack with a single segment.
    fn merge(index_writer: &IndexWriter<D, C, MS, MP>, merge: &mut OneMerge<D, C>) -> Result<()> {
        if let Err(e) = Self::do_merge(index_writer, merge) {
            index_writer.writer.tragic_event(e, "merge", None)?;
        }

        Ok(())
    }

    fn do_merge(
        index_writer: &IndexWriter<D, C, MS, MP>,
        merge: &mut OneMerge<D, C>,
    ) -> Result<()> {
        let res = Self::execute_merge(index_writer, merge);
        {
            let l = index_writer.writer.lock.lock().unwrap();
            let writer_mut = unsafe { index_writer.writer.writer_mut(&l) };

            writer_mut.merge_finish(&l, merge);
            if res.is_err() {
                trace!("IW - hit error during merge");
            } else if !merge.rate_limiter.aborted() && merge.max_num_segments.get().is_some()
                || (!index_writer.writer.closed.load(Ordering::Acquire)
                    && !index_writer.writer.closing.load(Ordering::Acquire))
            {
                // This merge (and, generally, any change to the
                // segments) may now enable new merges, so we call
                // merge policy & update pending merges.
                writer_mut.update_pending_merges(
                    MergerTrigger::MergeFinished,
                    merge.max_num_segments.get(),
                    index_writer,
                    &l,
                )?;
            }
        }
        match res {
            Err(Error(Index(MergeAborted(_)), _)) => {
                let segments: Vec<_> = merge.segments.iter().map(|s| &s.info.name).collect();
                warn!("the merge for segments {:?} is aborted!", segments);
                // the merge is aborted, ignore this error
                Ok(())
            }
            Ok(()) => Ok(()),
            Err(e) => Err(e),
        }
    }

    fn execute_merge(
        index_writer: &IndexWriter<D, C, MS, MP>,
        merge: &mut OneMerge<D, C>,
    ) -> Result<()> {
        index_writer
            .writer
            .rate_limiters
            .get_or(|| Box::new(Arc::clone(&merge.rate_limiter)));

        // let t0 = SystemTime::now();

        index_writer.writer.merge_init(merge)?;

        trace!("IW - now merge");

        Self::merge_middle(index_writer, merge)?;
        // self.merge_success();
        Ok(())
    }

    /// Does initial setup for a merge, which is fast but holds
    /// the synchronized lock on IndexWriter instance
    fn merge_init(&self, merge: &mut OneMerge<D, C>) -> Result<()> {
        let lock = Arc::clone(&self.lock);
        let l = lock.lock().unwrap();

        let writer = unsafe { self.writer_mut(&l) };
        let res = writer.do_merge_init(merge, &l);
        if res.is_err() {
            trace!("IW - hit error in merge_init");
            writer.merge_finish(&l, merge);
        }
        res
    }

    fn do_merge_init(&mut self, merge: &mut OneMerge<D, C>, l: &MutexGuard<()>) -> Result<()> {
        debug_assert!(merge.register_done);

        if self.tragedy.is_some() {
            bail!(IllegalState(
                "this writer hit an unrecoverable error; cannot merge".into()
            ));
        }

        if merge.info.is_some() {
            // merge_init already done
            return Ok(());
        }

        if merge.rate_limiter.aborted() {
            return Ok(());
        }

        // TODO: in the non-pool'd case this is somewhat
        // wasteful, because we open these readers, close them,
        // and then open them again for merging.  Maybe  we
        // could pre-pool them somehow in that case...
        trace!(
            "IW - now apply deletes for {} merging segments.",
            merge.segments.len()
        );
        // Lock order: IW - BD
        let result = self
            .buffered_updates_stream
            .apply_deletes_and_updates(&self.reader_pool, &merge.segments)?;

        if result.any_deletes {
            self.check_point(&l)?;
        }

        if !result.all_deleted.is_empty() {
            trace!(
                "IW - drop 100% deleted {} segments",
                result.all_deleted.len()
            );

            for info in &result.all_deleted {
                self.segment_infos.remove(info);
                self.pending_num_docs
                    .fetch_sub(info.info.max_doc as i64, Ordering::AcqRel);
                if merge.segments.contains(info) {
                    self.merging_segments.remove(&info.info.name);
                    merge.segments.remove_item(info);
                }
                self.reader_pool.drop(info.as_ref())?;
            }
            self.check_point(&l)?;
        }

        // Bind a new segment name here so even with ConcurrentMergePolicy
        // we keep deterministic segment names.
        let merge_segment_name = self.new_segment_name();
        let mut si = SegmentInfo::new(
            VERSION_LATEST,
            &merge_segment_name,
            -1,
            Arc::clone(&self.directory_orig),
            false,
            Some(Arc::clone(&self.config.codec)),
            HashMap::new(),
            random_id(),
            HashMap::new(),
            self.config.index_sort().map(Clone::clone),
        )?;
        let mut details = HashMap::new();
        details.insert(
            "merge_max_num_segments".into(),
            merge.max_num_segments.get().unwrap_or(0).to_string(),
        );
        details.insert("merge_factor".into(), merge.segments.len().to_string());
        details.insert("source".into(), "merge".into());
        si.set_diagnostics(details);
        let sci = SegmentCommitInfo::new(si, 0, -1, -1, -1, HashMap::new(), HashSet::new());
        merge.info = Some(Arc::new(sci));

        self.buffered_updates_stream.prune(&self.segment_infos);

        Ok(())
    }

    /// Does the actual (time-consuming) work of the merge, but without holding
    /// synchronized lock on IndexWriter instance.
    fn merge_middle(
        index_writer: &IndexWriter<D, C, MS, MP>,
        merge: &mut OneMerge<D, C>,
    ) -> Result<i32> {
        match Self::do_merge_middle(index_writer, merge) {
            Err(e) => {
                error!("merge_middle err {:?}", e);
                let l = index_writer.writer.lock.lock().unwrap();
                index_writer.writer.close_merge_readers(merge, true, &l)?;
                Err(e)
            }
            Ok(r) => Ok(r),
        }
    }

    fn do_merge_middle(
        index_writer: &IndexWriter<D, C, MS, MP>,
        merge: &mut OneMerge<D, C>,
    ) -> Result<i32> {
        merge.rate_limiter.check_abort()?;

        let context = IOContext::Merge(merge.store_merge_info());

        let dir_wrapper = Arc::new(TrackingDirectoryWrapper::new(DerefWrapper(
            index_writer.writer.merge_directory.clone(),
        )));
        merge.readers = Vec::with_capacity(merge.segments.len());

        // This is try/finally to make sure merger's readers are closed:
        let mut seg_upto = 0;
        while seg_upto < merge.segments.len() {
            // Hold onto the "live" reader; we will use this to
            // commit merged deletes
            let rld = index_writer
                .writer
                .reader_pool
                .get_or_create(&merge.segments[seg_upto])?;

            // Carefully pull the most recent live docs and reader
            let mut reader: Arc<SegmentReader<D, C>>;
            let live_docs: BitsRef;
            let del_count: i32;
            {
                let _l = index_writer.writer.lock.lock()?;

                let res = rld.reader_for_merge(&context);
                match res {
                    Ok(r) => reader = r,
                    Err(e) => {
                        return Err(e);
                    }
                }

                live_docs = rld.readonly_live_docs();
                del_count =
                    rld.pending_delete_count() as i32 + merge.segments[seg_upto].del_count();

                debug_assert!(rld.verify_doc_counts());
            }

            // Deletes might have happened after we pulled the merge reader and
            // before we got a read-only copy of the segment's actual live docs
            // (taking pending deletes into account). In that case we need to
            // make a new reader with updated live docs and del count.
            if reader.num_deleted_docs() != del_count {
                // fix the reader's live docs and del count
                debug_assert!(del_count > reader.num_deleted_docs());

                let new_reader = {
                    let _l = index_writer.writer.lock.lock()?;
                    SegmentReader::build_from(
                        Arc::clone(&merge.segments[seg_upto]),
                        reader.as_ref(),
                        live_docs,
                        merge.segments[seg_upto].info.max_doc - del_count,
                        true,
                    )?
                };

                // rld.release(&reader);
                reader = Arc::new(new_reader);
            }

            merge.readers.push(reader);
            debug_assert!(del_count <= merge.segments[seg_upto].info.max_doc);
            seg_upto += 1;
        }

        // Let the merge wrap readers
        let merge_readers: Vec<Arc<SegmentReader<D, C>>> =
            merge.readers.iter().map(Arc::clone).collect();
        let mut merger = SegmentMerger::new(
            merge_readers,
            &merge.info.as_ref().unwrap().info,
            Arc::clone(&dir_wrapper),
            FieldNumbersRef::new(Arc::clone(&index_writer.writer.global_field_numbers)),
            context,
        )?;
        merge.rate_limiter.check_abort()?;
        merge.merge_start_time.write(Some(SystemTime::now()));

        // This is where all the work happens:
        if merger.should_merge() {
            merger.merge()?;
        }
        merger
            .merge_state
            .segment_info()
            .set_files(&dir_wrapper.create_files())?;

        if !merger.should_merge() {
            // Merge would produce a 0-doc segment, so we do nothing except commit the merge to
            // remove all the 0-doc segments that we "merged":
            debug_assert_eq!(merger.merge_state.segment_info().max_doc, 0);
            index_writer
                .writer
                .commit_merge(merge, &merger.merge_state)?;
            return Ok(0);
        }
        debug_assert!(merger.merge_state.segment_info().max_doc > 0);

        // Very important to do this before opening the reader
        // because codec must know if prox was written for
        // this segment:
        // System.out.println("merger set hasProx=" + merger.hasProx() + " seg=" + merge.info.name);
        let use_compound_file = {
            let _l = index_writer.writer.lock.lock()?;
            index_writer.writer.config.merge_policy().use_compound_file(
                &index_writer.writer.segment_infos,
                merge.info.as_ref().unwrap().as_ref(),
                index_writer,
            )
        };
        if use_compound_file {
            let tracking_cfs_dir =
                TrackingDirectoryWrapper::new(&index_writer.writer.merge_directory);

            let info = merge.info.as_mut().unwrap();
            let segment_info = Arc::get_mut(info).unwrap();
            let files_to_remove = segment_info.files();

            if let Err(e) = index_writer.writer.create_compound_file(
                &tracking_cfs_dir,
                &mut segment_info.info,
                &context,
            ) {
                let _l = index_writer.writer.lock.lock().unwrap();
                if merge.rate_limiter.aborted() {
                    // This can happen if rollback is called while we were building
                    // our CFS -- fall through to logic below to remove the non-CFS
                    // merged files:
                    return Ok(0);
                } else {
                    index_writer
                        .writer
                        .delete_new_files(&segment_info.files())?;
                    return Err(e);
                }
            }

            // So that, if we hit exc in deleteNewFiles (next)
            // or in commitMerge (later), we close the
            // per-segment readers in the finally clause below:
            {
                let _l = index_writer.writer.lock.lock().unwrap();
                // delete new non cfs files directly: they were never
                // registered with IFD
                index_writer.writer.delete_new_files(&files_to_remove)?;

                if merge.rate_limiter.aborted() {
                    index_writer
                        .writer
                        .delete_new_files(&segment_info.files())?;
                    return Ok(0);
                }
            }

            segment_info.info.set_use_compound_file();
        }

        // Have codec write SegmentInfo.  Must do this after
        // creating CFS so that 1) .si isn't slurped into CFS,
        // and 2) .si reflects useCompoundFile=true change
        // above:
        {
            let info = merge.info.as_mut().unwrap();
            let segment_info = Arc::get_mut(info).unwrap();
            if let Err(e) = index_writer
                .writer
                .config
                .codec()
                .segment_info_format()
                .write(
                    &index_writer.writer.directory,
                    &mut segment_info.info,
                    &context,
                )
            {
                index_writer
                    .writer
                    .delete_new_files(&segment_info.files())?;
                return Err(e);
            }
        }

        if !index_writer
            .writer
            .commit_merge(merge, &merger.merge_state)?
        {
            // commitMerge will return false if this merge was aborted
            return Ok(0);
        }

        Ok(merge.info.as_ref().unwrap().info.max_doc)
    }

    /// Does finishing for a merge, which is fast but holds the
    /// synchronized lock on IndexWriter instance.
    fn merge_finish(&mut self, _lock: &MutexGuard<()>, merge: &mut OneMerge<D, C>) {
        // forceMerge, addIndexes or waitForMerges may be waiting
        // on merges to finish.
        self.cond.notify_all();

        // It's possible we are called twice, eg if there was an
        // exception inside mergeInit
        if merge.register_done {
            for info in &merge.segments {
                self.merging_segments.remove(&info.info.name);
            }
            merge.register_done = false;
        }
        self.running_merges.remove(&merge.id);
    }

    /// Wait for any currently outstanding merges to finish.
    ///
    /// It is guaranteed that any merges started prior to calling this method
    /// will have completed once this method completes.
    fn wait_for_merges(index_writer: &IndexWriter<D, C, MS, MP>) -> Result<()> {
        // Give merge scheduler last chance to run, in case
        // any pending merges are waiting. We can't hold IW's lock
        // when going into merge because it can lead to deadlock.
        index_writer
            .writer
            .merge_scheduler
            .merge(index_writer, MergerTrigger::Closing, false)?;

        {
            let mut l = index_writer.writer.lock.lock()?;
            index_writer.writer.ensure_open(false)?;
            debug!("IW - wait for merges");

            while !index_writer.writer.pending_merges.is_empty()
                || !index_writer.writer.running_merges.is_empty()
            {
                let (loc, _) = index_writer
                    .writer
                    .cond
                    .wait_timeout(l, Duration::from_millis(1000))?;
                l = loc;
            }

            // sanity check
            debug_assert!(index_writer.writer.merging_segments.is_empty());
            debug!("IW - wait for merges done");
        }
        Ok(())
    }

    /// Aborts running merges.  Be careful when using this method: when you
    /// abort a long-running merge, you lose a lot of work that must later be redone.
    fn abort_merges<'a>(&mut self, mut lock: MutexGuard<'a, ()>) -> Result<MutexGuard<'a, ()>> {
        self.stop_merges = true;
        // Abort all pending & running merge:
        let pending_merges = mem::replace(&mut self.pending_merges, VecDeque::new());
        for mut merge in pending_merges {
            merge.rate_limiter.set_abort();
            self.merge_finish(&lock, &mut merge);
        }

        for merge in self.running_merges.values() {
            merge.rate_limiter.set_abort();
        }

        // We wait here to make all merges stop.  It should not
        // take very long because they periodically check if
        // they are aborted.
        while !self.running_merges.is_empty() {
            // in case merge panic.
            if let Some(thread_count) = self.merge_scheduler.merging_thread_count() {
                if thread_count == 0 {
                    break;
                }
            }

            let (loc, _) = self.cond.wait_timeout(lock, Duration::from_millis(1000))?;
            warn!(
                "IW - abort merges, waiting for running_merges to be empty, current size: {}",
                self.running_merges.len()
            );
            lock = loc;
        }

        self.cond.notify_all();

        debug!(
            "debug abort_merges {} {} {}",
            self.pending_merges.len(),
            self.running_merges.len(),
            self.merging_segments.len()
        );

        debug_assert!(self.merging_segments.is_empty());
        trace!("IW - all running merges have aborted");
        Ok(lock)
    }

    /// Carefully merges deletes and updates for the segments we just merged. This
    /// is tricky because, although merging will clear all deletes (compacts the
    /// documents) and compact all the updates, new deletes and updates may have
    /// been flushed to the segments since the merge was started. This method
    /// "carries over" such new deletes and updates onto the newly merged segment,
    /// and saves the resulting deletes and updates files (incrementing the delete
    /// and DV generations for merge.info). If no deletes were flushed, no new
    /// deletes file is saved.
    fn commit_merged_deletes_and_updates(
        &mut self,
        merge: &OneMerge<D, C>,
        merge_state: &MergeState<D, C>,
        _lock: &MutexGuard<()>,
    ) -> Result<Option<Arc<ReadersAndUpdates<D, C, MS, MP>>>> {
        // Carefully merge deletes that occurred after we started merging:
        let mut min_gen = i64::max_value();

        // lazy init (only when we find a delete to carry over):
        let mut holder = MergedDeletesAndUpdates::default();
        debug_assert_eq!(merge.segments.len(), merge_state.doc_maps.len());
        let mut dv_updates: HashMap<String, Vec<(i32, i64)>> = HashMap::new();

        for i in 0..merge.segments.len() {
            let info = &merge.segments[i];
            min_gen = min_gen.min(info.buffered_deletes_gen());
            let max_doc = info.info.max_doc;
            let prev_live_docs = merge.readers[i].live_docs();
            let rld = self.reader_pool.get(info.as_ref()).unwrap();
            let inner = rld.inner.lock()?;

            let mut fields: Vec<&String> = Vec::with_capacity(inner.merging_dv_updates.len());
            let mut field_updates: Vec<Vec<(i32, i64)>> =
                Vec::with_capacity(inner.merging_dv_updates.len());
            let mut indexes: Vec<usize> = Vec::with_capacity(inner.merging_dv_updates.len());

            // get all updates for fields
            for (field, updates) in &inner.merging_dv_updates {
                let updates = MergedDocValuesUpdatesIterator::join(updates.clone());
                let mut updates = NewDocValuesIterator::new(
                    merge.readers[i].clone(),
                    updates,
                    None,
                    true,
                    false,
                )?;

                let updates = updates.get_numeric_updates();
                if !updates.is_empty() {
                    fields.push(field);
                    field_updates.push(updates);
                    indexes.push(0);
                }
            }

            if !prev_live_docs.is_empty() {
                debug_assert!(inner.live_docs.is_some());
                debug_assert_eq!(prev_live_docs.len(), max_doc as usize);
                let cur_live_doc = inner.live_docs();
                debug_assert_eq!(cur_live_doc.len(), max_doc as usize);

                // There were deletes on this segment when the merge
                // started.  The merge has collapsed away those
                // deletes, but, if new deletes were flushed since
                // the merge started, we must now carefully keep any
                // newly flushed deletes but mapping them to the new
                // docIDs.

                // Since we copy-on-write, if any new deletes were
                // applied after merging has started, we can just
                // check if the before/after liveDocs have changed.
                // If so, we must carefully merge the liveDocs one
                // doc at a time:
                if !ptr::eq(cur_live_doc.as_ref(), prev_live_docs.as_ref()) {
                    // This means this segment received new deletes
                    // since we started the merge, so we
                    // must merge them:
                    for j in 0..max_doc as usize {
                        if !prev_live_docs.get(j)? {
                            // if the document was deleted before, it better still be deleted.
                            debug_assert!(!cur_live_doc.get(j).unwrap());
                        } else if !cur_live_doc.get(j)? {
                            // the document was deleted while we are merging:
                            if holder.merged_deletes_and_updates.is_none()
                                || !holder.inited_writable_live_docs
                            {
                                holder.init(&self.reader_pool, merge, true)?;
                            }
                            let doc_id = merge_state.doc_maps[i]
                                .get(merge_state.leaf_doc_maps[i].get(j as i32)?)?;
                            holder
                                .merged_deletes_and_updates
                                .as_ref()
                                .unwrap()
                                .delete(doc_id)?;
                        } else if !fields.is_empty() {
                            self.maybe_apply_merged_dv_updates(
                                merge,
                                merge_state,
                                &mut holder,
                                &fields,
                                &field_updates,
                                &mut indexes,
                                &mut dv_updates,
                                i,
                                j as i32,
                            )?;
                        }
                    }
                } else if !fields.is_empty() {
                    for j in 0..max_doc as usize {
                        if prev_live_docs.get(j)? {
                            self.maybe_apply_merged_dv_updates(
                                merge,
                                merge_state,
                                &mut holder,
                                &fields,
                                &field_updates,
                                &mut indexes,
                                &mut dv_updates,
                                i,
                                j as i32,
                            )?;
                        }
                    }
                }
            } else if inner.live_docs.is_some() && !inner.live_docs().is_empty() {
                let current_live_docs = inner.live_docs();
                debug_assert_eq!(current_live_docs.len(), max_doc as usize);
                // This segment had no deletes before but now it does:
                for j in 0..max_doc {
                    if !current_live_docs.get(j as usize)? {
                        if holder.merged_deletes_and_updates.is_none()
                            || !holder.inited_writable_live_docs
                        {
                            holder.init(&self.reader_pool, merge, true)?;
                        }
                        let doc_id =
                            merge_state.doc_maps[i].get(merge_state.leaf_doc_maps[i].get(j)?)?;
                        holder
                            .merged_deletes_and_updates
                            .as_ref()
                            .unwrap()
                            .delete(doc_id)?;
                    } else if !fields.is_empty() {
                        self.maybe_apply_merged_dv_updates(
                            merge,
                            merge_state,
                            &mut holder,
                            &fields,
                            &field_updates,
                            &mut indexes,
                            &mut dv_updates,
                            i,
                            j as i32,
                        )?;
                    }
                }
            } else if !fields.is_empty() {
                for j in 0..max_doc as usize {
                    self.maybe_apply_merged_dv_updates(
                        merge,
                        merge_state,
                        &mut holder,
                        &fields,
                        &field_updates,
                        &mut indexes,
                        &mut dv_updates,
                        i,
                        j as i32,
                    )?;
                }
            }
        }

        if !dv_updates.is_empty() {
            for (field, mut updates) in dv_updates {
                updates.sort_by(|a, b| a.0.cmp(&b.0));
                let up = MergedDocValuesUpdatesIterator::from_updates(field.clone(), updates);
                if holder.merged_deletes_and_updates.is_none() {
                    holder.init(&self.reader_pool, merge, false)?;
                }
                holder
                    .merged_deletes_and_updates
                    .as_mut()
                    .unwrap()
                    .inner
                    .lock()
                    .unwrap()
                    .add_field_updates(field, up);
            }
            if holder.merged_deletes_and_updates.is_none() {
                holder.init(&self.reader_pool, merge, false)?;
            }
            holder
                .merged_deletes_and_updates
                .as_mut()
                .unwrap()
                .create_reader_if_not_exist(&IOContext::READ_ONCE)?;
            holder
                .merged_deletes_and_updates
                .as_ref()
                .unwrap()
                .inner
                .lock()
                .unwrap()
                .write_field_updates()?;
        }

        merge
            .info
            .as_ref()
            .unwrap()
            .set_buffered_deletes_gen(min_gen);
        Ok(holder.merged_deletes_and_updates.take())
    }

    fn maybe_apply_merged_dv_updates(
        &self,
        merge: &OneMerge<D, C>,
        merge_state: &MergeState<D, C>,
        holder: &mut MergedDeletesAndUpdates<D, C, MS, MP>,
        fields: &Vec<&String>,
        updates: &Vec<Vec<(i32, i64)>>,
        indexes: &mut Vec<usize>,
        out_updates: &mut HashMap<String, Vec<(i32, i64)>>,
        segment: usize,
        doc_id: i32,
    ) -> Result<()> {
        let mut new_doc = -1;
        for i in 0..fields.len() {
            let idx = indexes[i];
            if idx < updates[i].len() {
                if doc_id == updates[i][idx].0 {
                    if holder.merged_deletes_and_updates.is_none() {
                        holder.init(&self.reader_pool, merge, false)?;
                    }
                    if new_doc == -1 {
                        new_doc = merge_state
                            .doc_maps
                            .get(segment)
                            .unwrap()
                            .get(doc_id)
                            .unwrap();
                    }
                    if let Some(upds) = out_updates.get_mut(fields[i]) {
                        upds.push((new_doc, updates[i][idx].1));
                    } else {
                        let values = vec![(new_doc, updates[i][idx].1)];
                        out_updates.insert(fields[i].clone(), values);
                    }
                    indexes[i] += 1;
                }
            }
        }
        Ok(())
    }

    fn commit_merge(
        &self,
        merge: &mut OneMerge<D, C>,
        merge_state: &MergeState<D, C>,
    ) -> Result<bool> {
        let l = self.lock.lock()?;
        let writer_mut = unsafe { self.writer_mut(&l) };
        if self.tragedy.is_some() {
            bail!(IllegalState(
                "this writer hit an unrecoverable error".into()
            ));
        }

        trace!("IW - commit_merge");

        debug_assert!(merge.register_done);

        // If merge was explicitly aborted, or, if rollback() or
        // rollbackTransaction() had been called since our merge
        // started (which results in an unqualified
        // deleter.refresh() call that will remove any index
        // file that current segments does not reference), we
        // abort this merge
        if merge.rate_limiter.aborted() {
            trace!("IW - commit_merge: skip, it was aborted");

            // In case we opened and pooled a reader for this
            // segment, drop it now.  This ensures that we close
            // the reader before trying to delete any of its
            // files.  This is not a very big deal, since this
            // reader will never be used by any NRT reader, and
            // another thread is currently running close(false)
            // so it will be dropped shortly anyway, but not
            // doing this  makes  MockDirWrapper angry in
            // TestNRTThreads (LUCENE-5434):
            self.reader_pool
                .drop(merge.info.as_ref().unwrap().as_ref())?;

            // Safe: these files must exist:
            self.delete_new_files(merge_state.segment_info().files())?;

            return Ok(false);
        }

        let merge_updates = if merge_state.segment_info().max_doc == 0 {
            None
        } else {
            writer_mut.commit_merged_deletes_and_updates(merge, merge_state, &l)?
        };

        // If the doc store we are using has been closed and
        // is in now compound format (but wasn't when we
        // started), then we will switch to the compound
        // format as well:
        debug_assert!(!self
            .segment_infos
            .segments
            .contains(merge.info.as_ref().unwrap()));

        let drop_segment = merge.segments.is_empty()
            || merge.info.as_ref().unwrap().info.max_doc == 0
            || (merge_updates.is_some()
                && merge_updates.as_ref().unwrap().pending_delete_count()
                    == merge.info.as_ref().unwrap().info.max_doc as u32);

        // If we merged no segments then we better be dropping the new segment:
        debug_assert!(!merge.segments.is_empty() || drop_segment);
        debug_assert!(merge.info.as_ref().unwrap().info.max_doc > 0 || drop_segment);

        if let Some(merged_updates) = merge_updates {
            if drop_segment {
                merged_updates.drop_changes();
            }
            // Pass false for assertInfoLive because the merged
            // segment is not yet live (only below do we commit it
            // to the segmentInfos):
            if let Err(e) = self.reader_pool.release(&merged_updates) {
                merged_updates.drop_changes();
                if let Err(e) = self.reader_pool.drop(merge.info.as_ref().unwrap().as_ref()) {
                    warn!("IndexWriter: drop segment failed with error: {:?}", e);
                }
                return Err(e);
            }
        }

        // Must do this after readerPool.release, in case an
        // exception is hit e.g. writing the live docs for the
        // merge segment, in which case we need to abort the
        // merge:
        writer_mut
            .segment_infos
            .apply_merge_changes(merge, drop_segment);

        // Now deduct the deleted docs that we just reclaimed from this merge:
        let del_doc_count = merge.total_max_doc as i32 - merge.info.as_ref().unwrap().info.max_doc;
        debug_assert!(del_doc_count >= 0);
        self.pending_num_docs
            .fetch_sub(del_doc_count as i64, Ordering::AcqRel);

        if drop_segment {
            self.reader_pool
                .drop(merge.info.as_ref().unwrap().as_ref())?;
            // Safe: these files must exist
            self.delete_new_files(&merge.info.as_ref().unwrap().files())?;
        }

        // Must close before checkpoint, otherwise IFD won't be
        // able to delete the held-open files from the merge
        // readers:
        match self.close_merge_readers(merge, false, &l) {
            // Must note the change to segmentInfos so any commits
            // in-flight don't lose it (IFD will incRef/protect the
            // new files we created):
            Ok(()) => {
                writer_mut.check_point(&l)?;
            }
            Err(e) => {
                // Ignore so we keep throwing original exception.
                let _ = writer_mut.check_point(&l);
                return Err(e);
            }
        }

        if merge.max_num_segments.get().is_some() && !drop_segment {
            // cascade the force_merge:
            if !self
                .segments_to_merge
                .contains_key(merge.info.as_ref().unwrap())
            {
                writer_mut
                    .segments_to_merge
                    .insert(Arc::clone(merge.info.as_ref().unwrap()), false);
            }
        }

        Ok(true)
    }

    fn close_merge_readers(
        &self,
        merge: &mut OneMerge<D, C>,
        suppress_errors: bool,
        _lock: &MutexGuard<()>,
    ) -> Result<()> {
        let drop = !suppress_errors;

        let mut res = Ok(());
        for reader in &merge.readers {
            if let Some(rld) = self.reader_pool.get(reader.si.as_ref()) {
                if drop {
                    rld.drop_changes();
                } else {
                    rld.drop_merging_updates();
                }

                let mut res_drop = self.reader_pool.release(&rld);
                if res_drop.is_ok() {
                    if drop {
                        res_drop = self.reader_pool.drop(rld.info.as_ref());
                    }
                }

                if let Err(e) = res_drop {
                    if res.is_ok() {
                        res = Err(e);
                    }
                }
            }
        }
        merge.readers.clear();

        if !suppress_errors {
            return res;
        }
        Ok(())
    }

    fn tragic_event(
        &self,
        tragedy: Error,
        location: &str,
        commit_lock: Option<&MutexGuard<()>>,
    ) -> Result<()> {
        trace!("IW - hit tragic '{:?}' inside {}", &tragedy, location);

        {
            let l = self.lock.lock()?;

            // It's possible you could have a really bad day
            if self.tragedy.is_some() {
                bail!(tragedy);
            }

            let writer = unsafe { self.writer_mut(&l) };
            writer.tragedy = Some(tragedy);
        }

        // if we are already closed (e.g. called by rollback), this will be a no-op.
        if self.should_close(false) {
            if let Some(lock) = commit_lock {
                self.rollback_internal(lock)?;
            } else {
                let lock = self.commit_lock.lock()?;
                self.rollback_internal(&lock)?;
            }
        }

        bail!(IllegalState(format!(
            "this writer hit an unrecoverable error; {:?}",
            &self.tragedy
        )))
    }
}

/// Holds shared SegmentReader instances. IndexWriter uses SegmentReaders for:
/// 1) applying deletes, 2) doing merges, 3) handing out a real-time reader.
/// This pool reuses instances of the SegmentReaders in all these places if it
/// is in "near real-time mode" (getReader() has been called on this instance).
pub struct ReaderPool<
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
> {
    lock: Mutex<()>,
    reader_map: Arc<Mutex<HashMap<String, Arc<ReadersAndUpdates<D, C, MS, MP>>>>>,
    // key is SegmentCommitInfo.
    // info.name
    index_writer: Weak<IndexWriterInner<D, C, MS, MP>>,
    inited: bool,
}

impl<D, C, MS, MP> ReaderPool<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    pub fn new() -> Self {
        Default::default()
    }

    fn init(&mut self, index_writer: Weak<IndexWriterInner<D, C, MS, MP>>) {
        self.index_writer = index_writer;
        self.inited = true;
    }

    fn writer(&self) -> Arc<IndexWriterInner<D, C, MS, MP>> {
        debug_assert!(self.inited);
        self.index_writer.upgrade().unwrap()
    }

    // used only by asserts
    pub fn info_is_live(&self, info: &SegmentCommitInfo<D, C>) -> bool {
        let l = self.lock.lock().unwrap();
        self.info_is_live_syn(info, &l)
    }

    fn info_is_live_syn(&self, info: &SegmentCommitInfo<D, C>, _lock: &MutexGuard<()>) -> bool {
        self.writer()
            .segment_infos
            .segments
            .iter()
            .any(|i| i.info.name == info.info.name)
    }

    pub fn drop(&self, info: &SegmentCommitInfo<D, C>) -> Result<()> {
        let _lock = self.lock.lock().unwrap();
        if let Some(rld) = self.reader_map.lock()?.remove(&info.info.name) {
            // debug_assert!(info == rld.info);
            rld.drop_readers()?;
        }
        Ok(())
    }

    /// Remove all our references to readers, and commits any pending changes
    fn drop_all(&self, do_save: bool) -> Result<()> {
        let l = self.lock.lock()?;
        let mut prior_err = Ok(());

        let keys: Vec<String> = self.reader_map.lock()?.keys().cloned().collect();

        let mut reader_map = self.reader_map.lock()?;
        for key in keys {
            {
                let rld = reader_map.get(&key).unwrap();
                if let Err(e) = self.flush_and_check(do_save, rld, &l) {
                    if do_save {
                        return Err(e);
                    } else if prior_err.is_ok() {
                        prior_err = Err(e);
                    }
                }
            }

            // Important to remove as-we-go, not with .clear()
            // in the end, in case we hit an exception;
            // otherwise we could over-decref if close() is
            // called again:
            let rld = reader_map.remove(&key).unwrap();

            // NOTE: it is allowed that these decRefs do not
            // actually close the SRs; this happens when a
            // near real-time reader is kept open after the
            // IndexWriter instance is closed:
            if let Err(e) = rld.drop_readers() {
                if do_save {
                    return Err(e);
                } else if prior_err.is_ok() {
                    prior_err = Err(e);
                }
            }
        }

        debug_assert!(reader_map.is_empty());
        prior_err
    }

    fn flush_and_check(
        &self,
        do_save: bool,
        rld: &ReadersAndUpdates<D, C, MS, MP>,
        guard: &MutexGuard<()>,
    ) -> Result<()> {
        if do_save && rld.write_live_docs(&self.writer().directory)? {
            // Make sure we only write del docs and field updates for a live segment:
            debug_assert!(self.info_is_live(rld.info.as_ref()));
            // Must checkpoint because we just
            // created new _X_N.del and field updates files;
            // don't call IW.checkpoint because that also
            // increments SIS.version, which we do not want to
            // do here: it was done previously (after we
            // invoked BDS.applyDeletes), whereas here all we
            // did was move the state to disk:
            self.check_point_no_sis(guard)?;
        }
        Ok(())
    }

    #[allow(dead_code)]
    fn any_pending_deletes(&self) -> bool {
        let _lock = self.lock.lock().unwrap();
        (&self.reader_map.lock().unwrap())
            .values()
            .any(|rld| rld.pending_delete_count() > 0)
    }

    pub fn release(&self, rld: &Arc<ReadersAndUpdates<D, C, MS, MP>>) -> Result<()> {
        let _lock = self.lock.lock().unwrap();
        // Matches inc_ref in get:
        rld.dec_ref();
        // Pool still holds a ref:
        debug_assert!(rld.ref_count() >= 1);

        Ok(())
    }

    fn check_point_no_sis(&self, _guard: &MutexGuard<()>) -> Result<()> {
        let writer_inner = self.writer();
        unsafe {
            let writer = writer_inner.as_ref() as *const IndexWriterInner<D, C, MS, MP>
                as *mut IndexWriterInner<D, C, MS, MP>;

            (*writer).change_count.fetch_add(1, Ordering::AcqRel);
            (*writer)
                .deleter
                .checkpoint(&(*writer).segment_infos, false)
        }
    }

    /// Commit live docs changes for the segment readers for the previous infos.
    pub fn commit(&self, infos: &SegmentInfos<D, C>) -> Result<()> {
        let l = self.lock.lock()?;
        for info in &infos.segments {
            if let Some(rld) = self.reader_map.lock()?.get(&info.info.name) {
                if rld.write_live_docs(&self.writer().directory)? {
                    // Make sure we only write del docs for a live segment:
                    debug_assert!(self.info_is_live_syn(info.as_ref(), &l));

                    // Must checkpoint because we just
                    // created new _X_N.del and field updates files;
                    // don't call IW.checkpoint because that also
                    // increments SIS.version, which we do not want to
                    // do here: it was done previously (after we
                    // invoked BDS.applyDeletes), whereas here all we
                    // did was move the state to disk:
                    self.check_point_no_sis(&l)?;
                }
            }
        }
        Ok(())
    }

    /// Obtain a ReadersAndLiveDocs instance from the reader_pool.
    pub fn get(
        &self,
        info: &SegmentCommitInfo<D, C>,
    ) -> Option<Arc<ReadersAndUpdates<D, C, MS, MP>>> {
        // Make sure no new readers can be opened if another thread just closed us:
        // TODO, how can we ensure we are open when we don't directly refer to IndexWrier
        // self.writer().ensure_open(true);
        let _lock = self.lock.lock().unwrap();

        debug_assert!(ptr::eq(
            info.info.directory.as_ref(),
            self.writer().directory_orig.as_ref(),
        ));

        self.reader_map
            .lock()
            .unwrap()
            .get(&info.info.name)
            .map(Arc::clone)
    }

    /// Obtain a ReadersAndLiveDocs instance from the reader_pool.
    /// if currently not exist, the will create one.
    /// you must later call `#release`.
    pub fn get_or_create(
        &self,
        info: &Arc<SegmentCommitInfo<D, C>>,
    ) -> Result<Arc<ReadersAndUpdates<D, C, MS, MP>>> {
        // Make sure no new readers can be opened if another thread just closed us:
        // TODO, how can we ensure we are open when we don't directly refer to IndexWrier
        let writer = self.writer();
        writer.ensure_open(false)?;

        let _lock = self.lock.lock().unwrap();

        debug_assert!(ptr::eq(
            info.info.directory.as_ref(),
            writer.directory_orig.as_ref(),
        ));

        if !self.reader_map.lock()?.contains_key(&info.info.name) {
            let rld = Arc::new(ReadersAndUpdates::new(
                Weak::clone(&self.index_writer),
                Arc::clone(info),
            ));
            // Steal initial reference:
            self.reader_map.lock()?.insert(info.info.name.clone(), rld);
        }

        let rld = Arc::clone(self.reader_map.lock()?.get_mut(&info.info.name).unwrap());
        rld.inc_ref();
        Ok(rld)
    }
}

impl<D: Directory + Send + Sync + 'static, C: Codec, MS: MergeScheduler, MP: MergePolicy> Default
    for ReaderPool<D, C, MS, MP>
{
    fn default() -> Self {
        ReaderPool {
            lock: Mutex::new(()),
            reader_map: Arc::new(Mutex::new(HashMap::new())),
            index_writer: Weak::new(),
            inited: false,
        }
    }
}

impl<D: Directory + Send + Sync + 'static, C: Codec, MS: MergeScheduler, MP: MergePolicy> Drop
    for ReaderPool<D, C, MS, MP>
{
    fn drop(&mut self) {
        if let Err(e) = self.drop_all(false) {
            error!("ReaderPool: drop_all on close failed by: {:?}", e);
        }
    }
}

// reads latest field infos for the commit
// this is used on IW init and addIndexes(Dir) to create/update the global field map.
// TODO: fix tests abusing this method!
fn read_field_infos<D: Directory, C: Codec>(si: &SegmentCommitInfo<D, C>) -> Result<FieldInfos> {
    let codec = si.info.codec();
    let reader = codec.field_infos_format();

    if si.has_field_updates() {
        // there are updates, we read latest (always outside of CFS)
        let segment_suffix = to_base36(si.field_infos_gen() as u64);
        reader.read(
            &*si.info.directory,
            &si.info,
            &segment_suffix,
            &IOContext::READ_ONCE,
        )
    } else if si.info.is_compound_file() {
        let cfs = codec.compound_format().get_compound_reader(
            Arc::clone(&si.info.directory),
            &si.info,
            &IOContext::Default,
        )?;
        reader.read(&cfs, &si.info, "", &IOContext::READ_ONCE)
    } else {
        // no cfs
        reader.read(&*si.info.directory, &si.info, "", &IOContext::READ_ONCE)
    }
}

// Used by IndexWriter to hold open SegmentReaders (for searching or merging),
// plus pending deletes and updates, for a given segment
pub struct ReadersAndUpdates<
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
> {
    // Not final because we replace (clone) when we need to
    // change it and it's been shared:
    pub info: Arc<SegmentCommitInfo<D, C>>,
    pub inner: Mutex<ReadersAndUpdatesInner<D, C, MS, MP>>,
    // Tracks how many consumers are using this instance:
    ref_count: AtomicU32,
}

impl<D, C, MS, MP> ReadersAndUpdates<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    fn new(
        writer: Weak<IndexWriterInner<D, C, MS, MP>>,
        info: Arc<SegmentCommitInfo<D, C>>,
    ) -> Self {
        ReadersAndUpdates {
            inner: Mutex::new(ReadersAndUpdatesInner::new(writer)),
            ref_count: AtomicU32::new(1),
            info,
        }
    }

    #[allow(dead_code)]
    fn with_reader(
        writer: Weak<IndexWriterInner<D, C, MS, MP>>,
        reader: SegmentReader<D, C>,
    ) -> Self {
        let info = Arc::clone(&reader.si);
        let inner = ReadersAndUpdatesInner::with_reader(writer, reader);
        ReadersAndUpdates {
            inner: Mutex::new(inner),
            ref_count: AtomicU32::new(1),
            info,
        }
    }

    pub fn pending_delete_count(&self) -> u32 {
        let guard = self.inner.lock().unwrap();
        guard.pending_delete_count
    }

    pub fn ref_count(&self) -> u32 {
        self.ref_count.load(Ordering::Acquire)
    }

    pub fn inc_ref(&self) {
        self.ref_count.fetch_add(1, Ordering::AcqRel);
    }

    pub fn dec_ref(&self) {
        let org = self.ref_count.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(org > 0);
    }

    pub fn create_reader_if_not_exist(&self, context: &IOContext) -> Result<()> {
        let mut guard = self.inner.lock()?;
        guard.create_reader_if_not_exist(&self.info, context)
    }

    pub fn delete(&self, doc_id: DocId) -> Result<bool> {
        let mut guard = self.inner.lock()?;
        guard.delete(doc_id)
    }

    pub fn init_writable_live_docs(&self) -> Result<()> {
        let mut guard = self.inner.lock()?;
        guard.init_writable_live_docs(&self.info)
    }

    // Commit live docs (writes new _X_N.del files) and field updates (writes new
    // _X_N updates files) to the directory; returns true if it wrote any file
    // and false if there were no new deletes or updates to write:
    pub fn write_live_docs<D1: Directory>(&self, dir: &Arc<D1>) -> Result<bool> {
        let mut guard = self.inner.lock()?;
        guard.write_live_docs(&self.info, dir)
    }

    pub fn drop_readers(&self) -> Result<()> {
        let mut guard = self.inner.lock()?;
        guard.reader = None;
        self.dec_ref();
        Ok(())
    }

    pub fn get_readonly_clone(&self, context: &IOContext) -> Result<SegmentReader<D, C>> {
        let mut guard = self.inner.lock()?;
        guard.get_readonly_clone(&self.info, context)
    }

    pub fn test_doc_id(&self, doc_id: usize) -> Result<bool> {
        let guard = self.inner.lock().unwrap();
        debug_assert!(guard.live_docs.is_some());
        guard.live_docs().get(doc_id)
    }

    fn verify_doc_counts(&self) -> bool {
        let guard = self.inner.lock().unwrap();
        guard.verify_doc_counts(self.info.as_ref())
    }

    pub fn readonly_live_docs(&self) -> BitsRef {
        let mut guard = self.inner.lock().unwrap();
        debug_assert!(guard.live_docs.is_some());
        guard.live_docs_shared = true;
        Arc::clone(guard.live_docs.as_ref().unwrap())
    }

    pub fn drop_changes(&self) {
        // Discard (don't save) changes when we are dropping
        // the reader; this is used only on the sub-readers
        // after a successful merge.  If deletes had
        // accumulated on those sub-readers while the merge
        // is running, by now we have carried forward those
        // deletes onto the newly merged segment, so we can
        // discard them on the sub-readers:
        let mut guard = self.inner.lock().unwrap();
        guard.pending_delete_count = 0;
        guard.drop_merging_updates();
    }

    pub fn drop_merging_updates(&self) {
        let mut guard = self.inner.lock().unwrap();
        guard.drop_merging_updates();
    }

    /// Returns a reader for merge. this method applies filed update if there are
    /// any and marks that this segment is currently merging.
    pub fn reader_for_merge(&self, context: &IOContext) -> Result<Arc<SegmentReader<D, C>>> {
        // must execute these two statements as atomic operation, otherwise we
        // could lose updates if e.g. another thread calls writeFieldUpdates in
        // between, or the updates are applied to the obtained reader, but then
        // re-applied in IW.commitMergedDeletes (unnecessary work and potential
        // bugs).
        self.create_reader_if_not_exist(context)?;
        let mut guard = self.inner.lock()?;
        guard.mark_merge();
        Ok(Arc::clone(guard.reader.as_ref().unwrap()))
    }
}

pub struct ReadersAndUpdatesInner<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    // used for process DocValues update which is not currently implemented
    writer: Weak<IndexWriterInner<D, C, MS, MP>>,
    // Set once (null, and then maybe set, and never set again):
    reader: Option<Arc<SegmentReader<D, C>>>,
    // Holds the current shared (readable and writable)
    // liveDocs.  This is null when there are no deleted
    // docs, and it's copy-on-write (cloned whenever we need
    // to change it but it's been shared to an external NRT
    // reader).
    live_docs: Option<BitsRef>,
    // How many further deletions we've done against
    // liveDocs vs when we loaded it or last wrote it:
    pub pending_delete_count: u32,
    // True if the current live_docs is referenced by an external NRT reader:
    live_docs_shared: bool,
    // Indicates whether this segment is currently being merged. While a segment
    // is merging, all field updates are also registered in the
    // mergingNumericUpdates map. Also, calls to writeFieldUpdates merge the
    // updates with mergingNumericUpdates.
    // That way, when the segment is done merging, IndexWriter can apply the
    // updates on the merged segment too.
    is_merging: bool,
    // merging_dv_updates: HashMap<String, DocValuesFieldUpdates>,
    pending_dv_updates: HashMap<String, Vec<MergedDocValuesUpdatesIterator>>,
    merging_dv_updates: HashMap<String, Vec<MergedDocValuesUpdatesIterator>>,

    // for dv updates
    sort_map: Option<Arc<PackedLongDocMap>>,
}

impl<D, C, MS, MP> ReadersAndUpdatesInner<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    fn new(writer: Weak<IndexWriterInner<D, C, MS, MP>>) -> Self {
        ReadersAndUpdatesInner {
            writer,
            reader: None,
            live_docs: None,
            pending_delete_count: 0,
            live_docs_shared: true,
            is_merging: false,
            pending_dv_updates: HashMap::new(),
            merging_dv_updates: HashMap::new(),
            sort_map: None,
        }
    }

    #[allow(dead_code)]
    fn with_reader(
        writer: Weak<IndexWriterInner<D, C, MS, MP>>,
        reader: SegmentReader<D, C>,
    ) -> Self {
        let live_docs = reader.live_docs();
        let pending_delete_count = reader.num_deleted_docs();
        debug_assert!(pending_delete_count >= 0);
        ReadersAndUpdatesInner {
            writer,
            reader: Some(Arc::new(reader)),
            live_docs: Some(live_docs),
            pending_delete_count: pending_delete_count as u32,
            live_docs_shared: true,
            is_merging: false,
            pending_dv_updates: HashMap::new(),
            merging_dv_updates: HashMap::new(),
            sort_map: None,
        }
    }

    // Call only from assert
    fn verify_doc_counts(&self, info: &SegmentCommitInfo<D, C>) -> bool {
        let mut count = 0;
        if let Some(ref live_docs) = self.live_docs {
            for i in 0..info.info.max_doc {
                if live_docs.get(i as usize).unwrap() {
                    count += 1;
                }
            }
        } else {
            count = info.info.max_doc;
        }

        info.info.max_doc - info.del_count() - self.pending_delete_count as i32 == count
    }

    pub fn create_reader_if_not_exist(
        &mut self,
        info: &Arc<SegmentCommitInfo<D, C>>,
        context: &IOContext,
    ) -> Result<()> {
        if self.reader.is_none() {
            self.reader = Some(Arc::new(SegmentReader::open(info, context)?));
            if self.live_docs.is_none() {
                self.live_docs = Some(self.reader.as_ref().unwrap().live_docs());
            }
        }

        // Ref for caller
        // self.reader.as_mut().unwrap().inc_ref();
        Ok(())
    }

    pub fn mark_merge(&mut self) {
        debug_assert!(self.merging_dv_updates.is_empty());
        self.is_merging = true;
        for (field, updates) in &self.pending_dv_updates {
            self.merging_dv_updates
                .insert(field.clone(), updates.clone());
        }
    }

    fn live_docs(&self) -> &BitsRef {
        debug_assert!(self.live_docs.is_some());
        self.live_docs.as_ref().unwrap()
    }

    pub fn reader(&self) -> &Arc<SegmentReader<D, C>> {
        debug_assert!(self.reader.is_some());

        self.reader.as_ref().unwrap()
    }

    pub fn delete(&mut self, doc_id: DocId) -> Result<bool> {
        debug_assert!(self.live_docs.is_some());
        let live_docs = self.live_docs.as_mut().unwrap();
        debug_assert!(
            doc_id >= 0 && doc_id < live_docs.len() as i32,
            "doc: {}, live docs len: {}",
            doc_id,
            live_docs.len()
        );
        debug_assert!(!self.live_docs_shared);

        let did_deleted = live_docs.get(doc_id as usize)?;
        if did_deleted {
            // when code went here, it can make sure there is only one refer to the live_docs Arc,
            // so the Arc::get_mut call will always success, thus the unwrap call is safe
            Arc::get_mut(live_docs)
                .unwrap()
                .as_bit_set_mut()
                .clear(doc_id as usize);
            self.pending_delete_count += 1;
        }
        Ok(did_deleted)
    }

    // Returns a ref to a clone. NOTE: you should decRef()
    // the reader when you're done (ie do not call close())
    pub fn get_readonly_clone(
        &mut self,
        info: &Arc<SegmentCommitInfo<D, C>>,
        context: &IOContext,
    ) -> Result<SegmentReader<D, C>> {
        if self.reader.is_none() {
            self.create_reader_if_not_exist(info, context)?;
            // self.reader.as_mut().unwrap().dec_ref();
            debug_assert!(self.reader.is_some());
        }

        // force new liveDocs in init_writable_live_docs even if it's null
        self.live_docs_shared = true;
        let live_docs = if self.live_docs.is_some() {
            Arc::clone(self.live_docs.as_ref().unwrap())
        } else {
            let live_docs = &self.reader.as_ref().unwrap().live_docs;
            assert!(live_docs.is_empty());
            // self.reader.as_mut().unwrap().inc_ref();
            Arc::clone(live_docs)
        };
        let reader = self.reader.as_ref().unwrap();
        SegmentReader::build(
            Arc::clone(info),
            live_docs,
            info.info.max_doc - info.del_count() - self.pending_delete_count as i32,
            Arc::clone(&reader.core),
        )
    }

    pub fn init_writable_live_docs(&mut self, info: &Arc<SegmentCommitInfo<D, C>>) -> Result<()> {
        debug_assert!(info.info.max_doc > 0);
        if self.live_docs_shared {
            let live_docs_format = info.info.codec().live_docs_format();

            let mut live_docs = None;
            if let Some(ref docs) = self.live_docs {
                if !docs.is_empty() {
                    live_docs = Some(live_docs_format.new_live_docs_from_existing(docs.as_ref())?);
                }
            };

            if live_docs.is_none() {
                let bits = live_docs_format.new_live_docs(info.info.max_doc() as usize)?;
                live_docs = Some(Arc::new(bits));
            }

            self.live_docs = live_docs;
            self.live_docs_shared = false;
        }
        Ok(())
    }

    // Commit live docs (writes new _X_N.del files) and field updates (writes new
    // _X_N updates files) to the directory; returns true if it wrote any file
    // and false if there were no new deletes or updates to write:
    pub fn write_live_docs<D1: Directory>(
        &mut self,
        info: &Arc<SegmentCommitInfo<D, C>>,
        dir: &Arc<D1>,
    ) -> Result<bool> {
        debug_assert!(self.live_docs.is_some());

        if self.pending_delete_count == 0 {
            return Ok(false);
        }

        // we have new deletes
        debug_assert_eq!(self.live_docs().len(), info.info.max_doc as usize);

        // Do this so we can delete any created files on exceptions;
        // this saves all codecs from having to do it:
        let tracking_dir = TrackingDirectoryWrapper::new(dir.as_ref());

        // We can write directly to the actual name (vs to a
        // .tmp & renaming it) because the file is not live
        // until segments file is written:
        let res = info.info.codec().live_docs_format().write_live_docs(
            self.live_docs().as_ref(),
            &tracking_dir,
            info,
            self.pending_delete_count as i32,
            &IOContext::Default,
        );

        if res.is_err() {
            // Advance only the nextWriteDelGen so that a 2nd
            // attempt to write will write to a new file
            info.advance_next_write_del_gen();

            // Delete any partially created file(s):
            for file_name in &tracking_dir.create_files() {
                if let Err(e) = tracking_dir.delete_file(file_name.as_str()) {
                    warn!("delete file '{}' failed by '{:?}'", file_name, e);
                }
            }
        }

        // If we hit an exc in the line above (eg disk full)
        // then info's delGen remains pointing to the previous
        // (successfully written) del docs:
        info.advance_del_gen();
        let del_count = info.del_count() + self.pending_delete_count as i32;
        info.set_del_count(del_count)?;
        self.pending_delete_count = 0;
        match res {
            Err(e) => Err(e),
            _ => Ok(true),
        }
    }

    fn drop_merging_updates(&mut self) {
        self.merging_dv_updates.clear();
        self.is_merging = false;
    }

    // TODO used for doc values update
    #[allow(dead_code)]
    fn write_field_infos_gen<F: FieldInfosFormat>(
        &self,
        info: &Arc<SegmentCommitInfo<D, C>>,
        field_infos: &FieldInfos,
        dir: &Arc<D>,
        infos_format: &F,
    ) -> Result<HashSet<String>> {
        let next_field_infos_gen = info.next_field_infos_gen();
        let segment_suffix = to_base36(next_field_infos_gen as u64);
        // we write approximately that many bytes (based on Lucene46DVF):
        // HEADER + FOOTER: 40
        // 90 bytes per-field (over estimating long name and attributes map)
        let infos_context = IOContext::Flush(FlushInfo::new(info.info.max_doc() as u32));
        // separately also track which files were created for this gen
        let tracking_dir = TrackingDirectoryWrapper::new(dir.as_ref());
        infos_format.write(
            &tracking_dir,
            &info.info,
            &segment_suffix,
            field_infos,
            &infos_context,
        )?;
        info.advance_field_infos_gen();
        Ok(tracking_dir.get_create_files())
    }

    pub fn add_field_updates(&mut self, field: String, updates: MergedDocValuesUpdatesIterator) {
        if self.is_merging {
            if let Some(mg_list) = self.merging_dv_updates.get_mut(&field) {
                mg_list.push(updates.clone());
            } else {
                let v = vec![updates.clone()];
                self.merging_dv_updates.insert(field.clone(), v);
            }
        }

        if let Some(upd_list) = self.pending_dv_updates.get_mut(&field) {
            upd_list.push(updates);
        } else {
            let v = vec![updates];
            self.pending_dv_updates.insert(field, v);
        }
    }
    // Writes field updates (new _X_N updates files) to the directory
    // pub fn write_field_updates<DW: Directory>(&mut self, _dir: &DW) -> Result<()> {
    pub fn write_field_updates(&self) -> Result<bool> {
        if self.pending_dv_updates.is_empty() {
            // no updates
            return Ok(false);
        }

        let me = unsafe {
            &mut *(self as *const ReadersAndUpdatesInner<D, C, MS, MP>
                as *mut ReadersAndUpdatesInner<D, C, MS, MP>)
        };

        assert!(self.reader.is_some());

        let info = &self.reader.as_ref().unwrap().si;

        let mut builder =
            FieldInfosBuilder::new(self.writer.upgrade().unwrap().global_field_numbers.clone());
        let finfos = self.reader.as_ref().unwrap().field_infos.clone();
        builder.add_infos(finfos.as_ref())?;
        for fi in builder.by_name.values_mut() {
            let origin = finfos.by_name.get(&fi.name).unwrap();
            fi.set_doc_values_gen(origin.dv_gen);
            let attributes = origin.attributes.read()?;
            for (key, val) in attributes.iter() {
                fi.put_attribute(key.clone(), val.clone());
            }
        }
        let mut field_infos = builder.finish()?;

        let codec = info.info.codec();
        let doc_values_format = codec.doc_values_format();

        let mut new_dv_files = me.handle_doc_values_updates(&mut field_infos, doc_values_format)?;

        let info_mut_ref = unsafe {
            &mut *(info.as_ref() as *const SegmentCommitInfo<D, C> as *mut SegmentCommitInfo<D, C>)
        };
        // writeFieldInfosGen fnm
        if !new_dv_files.is_empty() {
            let infos_format = codec.field_infos_format();
            let field_infos_files = self.write_field_infos_gen(
                info,
                &field_infos,
                &info.info.directory,
                &infos_format,
            )?;
            let old_dv_files = info_mut_ref.get_doc_values_updates_files();
            for (id, files) in old_dv_files {
                if !new_dv_files.contains_key(id) {
                    new_dv_files.insert(*id, files.clone());
                }
            }
            info_mut_ref.set_doc_values_updates_files(new_dv_files);
            info_mut_ref.set_field_infos_files(field_infos_files);

            {
                let lock = Mutex::new(());
                let useless = lock.lock()?;
                unsafe {
                    me.writer
                        .upgrade()
                        .as_ref()
                        .unwrap()
                        .writer_mut(&useless)
                        .check_point(&useless)?
                };
            }

            // reopen segment reader for updates
            me.reader = Some(Arc::new(me.get_readonly_clone(info, &IOContext::Default)?));
        }

        me.pending_dv_updates.clear();
        Ok(true)
    }

    fn handle_doc_values_updates(
        &mut self,
        infos: &mut FieldInfos,
        dv_format: <C as Codec>::DVFmt,
    ) -> Result<HashMap<i32, HashSet<String>>> {
        let mut new_dv_files = HashMap::new();
        for (field, updates) in &self.pending_dv_updates {
            if self.reader.is_none() || infos.field_info_by_name(field).is_none() {
                continue;
            }

            let info = self.reader.as_ref().unwrap().si.clone();
            let tracker = Arc::new(TrackingDirectoryWrapper::new(info.info.directory.as_ref()));
            let dv_gen = info.next_write_doc_values_gen();
            // step1 construct segment write state
            let ctx = IOContext::Flush(FlushInfo::new(info.info.max_doc() as u32));
            let field_info = infos.field_info_by_name(field).unwrap();
            let field_info = unsafe { &mut *(field_info as *const FieldInfo as *mut FieldInfo) };
            let old_dv_gen = field_info.set_doc_values_gen(dv_gen);
            let state = SegmentWriteState::new(
                tracker.clone(),
                info.info.clone(),
                FieldInfos::new(vec![field_info.clone()])?,
                None,
                ctx,
                to_base36(dv_gen as u64),
            );
            // step2 get doc values consumer
            let mut field_consumer = dv_format.fields_consumer(&state)?;

            let mut new_dv_updates_iter = if updates.len() == 1 && updates[0].is_merged_updates() {
                NewDocValuesIterator::new(
                    self.reader().clone(),
                    updates[0].clone(),
                    None,
                    false,
                    true,
                )?
            } else {
                NewDocValuesIterator::new(
                    self.reader().clone(),
                    MergedDocValuesUpdatesIterator::join(updates.clone()),
                    self.sort_map.take(),
                    true,
                    true,
                )?
            };
            let mut doc_num = 0;
            match field_info.doc_values_type {
                DocValuesType::Numeric => {
                    // step3 construct doc values writer, add data, and then flush to index
                    let mut ndv_writer = NumericDocValuesWriter::new(field_info);

                    loop {
                        let (doc_id, value) = new_dv_updates_iter.next_numeric();
                        if doc_id == NO_MORE_DOCS {
                            break;
                        }
                        ndv_writer.add_value(doc_id, value)?;
                        doc_num += 1;
                    }
                    if doc_num > 0 {
                        ndv_writer.finish(doc_num);
                        ndv_writer.flush(
                            &state,
                            None as Option<&PackedLongDocMap>,
                            &mut field_consumer,
                        )?;
                    }
                }
                DocValuesType::SortedNumeric => {
                    // step3 construct doc values writer, add data, and then flush to index
                    let mut ndv_writer = SortedNumericDocValuesWriter::new(field_info);

                    loop {
                        let (doc_id, value) = new_dv_updates_iter.next_numeric();
                        if doc_id == NO_MORE_DOCS {
                            break;
                        }
                        ndv_writer.add_value(doc_id, value);
                        doc_num += 1;
                    }
                    if doc_num > 0 {
                        ndv_writer.finish(doc_num);
                        ndv_writer.flush(
                            &state,
                            None as Option<&PackedLongDocMap>,
                            &mut field_consumer,
                        )?;
                    }
                }
                _ => unimplemented!(),
            };

            if doc_num > 0 {
                info.advance_doc_values_gen();
                new_dv_files.insert(field_info.number as i32, tracker.get_create_files());
            } else {
                field_info.set_doc_values_gen(old_dv_gen);
            }
        }
        Ok(new_dv_files)
    }
}

struct MergedDeletesAndUpdates<
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
> {
    merged_deletes_and_updates: Option<Arc<ReadersAndUpdates<D, C, MS, MP>>>,
    inited_writable_live_docs: bool,
}

impl<D, C, MS, MP> Default for MergedDeletesAndUpdates<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    fn default() -> Self {
        Self {
            merged_deletes_and_updates: None,
            inited_writable_live_docs: false,
        }
    }
}

impl<D, C, MS, MP> MergedDeletesAndUpdates<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    fn init(
        &mut self,
        reader_pool: &ReaderPool<D, C, MS, MP>,
        merge: &OneMerge<D, C>,
        init_writable_live_docs: bool,
    ) -> Result<()> {
        if self.merged_deletes_and_updates.is_none() {
            self.merged_deletes_and_updates =
                Some(reader_pool.get_or_create(merge.info.as_ref().unwrap())?);
        }
        if init_writable_live_docs && !self.inited_writable_live_docs {
            self.merged_deletes_and_updates
                .as_ref()
                .unwrap()
                .init_writable_live_docs()?;
            self.inited_writable_live_docs = true;
        }
        Ok(())
    }
}
