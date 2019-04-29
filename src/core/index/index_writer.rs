use core::codec::FieldInfosFormat;
use core::index::bufferd_updates::FrozenBufferedUpdates;
use core::index::bufferd_updates::{ApplyDeletesResult, BufferedUpdatesStream};
use core::index::directory_reader::index_exist;
use core::index::doc_writer::{DocumentsWriter, Event};
use core::index::index_file_deleter::IndexFileDeleter;
use core::index::index_writer_config::{IndexWriterConfig, OpenMode};
use core::index::merge_policy::{MergeSpecification, MergerTrigger};
use core::index::merge_policy::{OneMerge, OneMergeRunningInfo};
use core::index::merge_scheduler::MergeScheduler;
use core::index::merge_state::{DocMap, MergeState};
use core::index::segment_merger::SegmentMerger;
use core::index::thread_doc_writer::FlushedSegment;
use core::index::{file_name_from_generation, INDEX_FILE_PENDING_SEGMENTS};
use core::index::{FieldInfos, FieldNumbers, FieldNumbersRef, Fieldable};
use core::index::{LeafReader, StandardDirectoryReader, Term};
use core::index::{SegmentCommitInfo, SegmentInfo, SegmentInfos, SegmentReader};
use core::search::match_all::MATCH_ALL;
use core::search::Query;
use core::store::LockValidatingDirectoryWrapper;
use core::store::{Directory, DirectoryRc, TrackingDirectoryWrapper};
use core::store::{FlushInfo, IOContext, Lock, IO_CONTEXT_READONCE};
use core::store::{IndexInput, IndexOutput, RateLimiter};
use core::util::bits::{Bits, BitsRef};
use core::util::io::delete_file_ignoring_error;
use core::util::string_util::random_id;
use core::util::VERSION_LATEST;
use core::util::{to_base36, DocId};

use error::ErrorKind::{IllegalArgument, IllegalState};
use error::{Error, Result};

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::mem;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, SystemTime};

use thread_local::ThreadLocal;

/// Hard limit on maximum number of documents that may be added to the index
/// If you try to add more than this you'll hit `IllegalArgument` Error
pub const INDEX_MAX_DOCS: i32 = i32::max_value() - 128;

/// Maximum value of the token position in an indexed field.
pub const INDEX_MAX_POSITION: i32 = i32::max_value() - 128;

/// Name of the write lock in the index.
pub const INDEX_WRITE_LOCK_NAME: &str = "write.lock";

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
pub struct IndexWriter {
    pub lock: Arc<Mutex<()>>,
    cond: Condvar,
    // original use directory
    directory_orig: DirectoryRc,
    // wrapped with additional checks
    directory: DirectoryRc,
    merge_directory: RateLimitFilterDirectory,
    change_count: AtomicU64,
    // last change_count that was committed
    last_commit_change_count: AtomicU64,
    // list of segmentInfo we will fallback to if the commit fails
    rollback_segments: Vec<Arc<SegmentCommitInfo>>,

    // set when a commit is pending (after prepareCommit() & before commit())
    pending_commit: Option<SegmentInfos>,
    pending_seq_no: AtomicI64,
    pending_commit_change_count: AtomicU64,
    files_to_commit: HashSet<String>,

    segment_infos: SegmentInfos,
    segment_infos_lock: Arc<Mutex<()>>,
    pub global_field_numbers: FieldNumbers,

    doc_writer: DocumentsWriter,
    // event_queue: MsQueue<WriterEvent>, self.doc_writer.events
    deleter: IndexFileDeleter,
    segments_to_merge: HashMap<Arc<SegmentCommitInfo>, bool>,
    merge_max_num_segments: u32,

    write_lock: Arc<Lock>,

    closed: AtomicBool,
    closing: AtomicBool,

    pub merging_segments: HashSet<String>,
    merge_scheduler: Box<MergeScheduler>,
    merge_id_gen: AtomicU32,
    pending_merges: VecDeque<OneMerge>,
    running_merges: HashMap<u32, OneMergeRunningInfo>,

    merge_exceptions: Vec<OneMerge>,
    merge_gen: u64,
    stop_merges: bool,
    did_message_state: bool,

    flush_count: AtomicU32,
    flush_deletes_count: AtomicU32,
    pub reader_pool: ReaderPool,
    updates_stream_lock: Arc<Mutex<()>>,
    pub buffered_updates_stream: BufferedUpdatesStream,

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

    // The instance that was passed to the constructor. It is saved only in order
    // to allow users to query an IndexWriter settings.
    pub config: Arc<IndexWriterConfig>,

    /// How many documents are in the index, or are in the process of being
    /// added (reserved).  E.g., operations like addIndexes will first reserve
    /// the right to add N docs, before they actually change the index,
    /// much like how hotels place an "authorization hold" on your credit
    /// card to make sure they can later charge you when you check out.
    pub pending_num_docs: Arc<AtomicI64>,
    keep_fully_deleted_segments: bool,

    // Ensures only one flush() in actually flushing segments at a time.
    full_flush_lock: Arc<Mutex<()>>,

    // Used only by commit and prepareCommit, below; lock order is commit_lock -> IW
    commit_lock: Arc<Mutex<()>>,
    rate_limiters: Arc<ThreadLocal<Arc<RateLimiter>>>,
    // when unrecoverable disaster strikes, we populate this
    // with the reason that we had to close IndexWriter
    pub tragedy: Option<Error>,
}

unsafe impl Send for IndexWriter {}

unsafe impl Sync for IndexWriter {}

impl IndexWriter {
    /// Constructs a new IndexWriter per the settings given in <code>conf</code>.
    /// If you want to make "live" changes to this writer instance, use
    /// {@link #getConfig()}.
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
    pub fn new(d: DirectoryRc, conf: Arc<IndexWriterConfig>) -> Result<IndexWriter> {
        let write_lock = Arc::from(d.obtain_lock(INDEX_WRITE_LOCK_NAME)?);

        let directory: DirectoryRc = Arc::new(LockValidatingDirectoryWrapper::new(
            Arc::clone(&d),
            Arc::clone(&write_lock),
        ));

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
        // TODO
        let mut segment_infos: SegmentInfos;
        let rollback_segments: Vec<Arc<SegmentCommitInfo>>;
        let change_count = AtomicU64::new(0);
        if create {
            // Try to read first.  This is to allow create
            // against an index that's currently open for
            // searching.  In this case we write the next
            // segments_N file with no segments:
            segment_infos = {
                match SegmentInfos::read_latest_commit(&directory) {
                    Ok(mut sis) => {
                        sis.clear();
                        sis
                    }
                    Err(e) => {
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
            let last_segments_file = SegmentInfos::get_last_commit_segments_filename(&files)?;
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

        let pending_num_docs = AtomicU64::new(segment_infos.total_max_doc() as u64);
        // start with previous field numbers, but new FieldInfos
        // NOTE: this is correct even for an NRT reader because we'll pull
        // FieldInfos even for the un-committed segments:
        let mut global_field_numbers = FieldNumbers::new();
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

        let doc_writer =
            DocumentsWriter::new(Arc::clone(&conf), Arc::clone(&d), Arc::clone(&directory));

        // Default deleter (for backwards compatibility) is
        // KeepOnlyLastCommitDeleter:

        let mut deleter = IndexFileDeleter::new(
            Arc::clone(&d),
            Arc::clone(&directory),
            conf.index_deletion_policy(),
        );
        deleter.init(&files, &mut segment_infos, initial_index_exists, false)?;

        if deleter.starting_commit_deleted {
            // Deletion policy deleted the "head" commit point.
            // We have to mark ourself as changed so that if we
            // are closed w/o any further changes we write a new
            // segments_N file.

            // NOTE: this is copy from self.changed()
            change_count.fetch_add(1, Ordering::AcqRel);
            segment_infos.changed();
        }

        Ok(IndexWriter {
            lock: Arc::new(Mutex::new(())),
            cond: Condvar::new(),
            directory_orig: d,
            directory,
            merge_directory,
            change_count,
            last_commit_change_count: AtomicU64::new(0),
            rollback_segments: vec![],
            pending_commit: None,
            pending_seq_no: AtomicI64::new(0),
            pending_commit_change_count: AtomicU64::new(0),
            files_to_commit: HashSet::new(),
            segment_infos,
            segment_infos_lock: Arc::new(Mutex::new(())),
            global_field_numbers,
            doc_writer,
            deleter,
            segments_to_merge: HashMap::new(),
            merge_max_num_segments: 0,
            write_lock,
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
            did_message_state: false,
            flush_count: AtomicU32::new(0),
            flush_deletes_count: AtomicU32::new(0),
            reader_pool: ReaderPool::new(),
            updates_stream_lock: Arc::new(Mutex::new(())),
            buffered_updates_stream,
            pool_readers: AtomicBool::new(pool_readers),
            config: conf,
            pending_num_docs: Arc::new(AtomicI64::new(0)),
            keep_fully_deleted_segments: false,
            full_flush_lock: Arc::new(Mutex::new(())),
            commit_lock: Arc::new(Mutex::new(())),
            rate_limiters,
            tragedy: None,
        })
    }

    pub fn init(&mut self) {
        unsafe {
            let doc_writer = &mut self.doc_writer as *mut DocumentsWriter;
            let reader_pool = &mut self.reader_pool as *mut ReaderPool;
            (*doc_writer).init(self);
            (*reader_pool).init(self);
        }
    }

    pub fn max_doc(&self) -> u32 {
        // self.ensure_open(true);
        self.doc_writer.num_docs() + self.segment_infos.total_max_doc() as u32
    }

    pub fn num_docs(&self) -> u32 {
        let mut count = self.doc_writer.num_docs();
        for info in &self.segment_infos.segments {
            count += info.info.max_doc() as u32 - self.num_deleted_docs(&info);
        }
        count
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
    ) -> Result<StandardDirectoryReader> {
        self.ensure_open(true)?;

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
        self.pool_readers.store(true, Ordering::Release);
        self.do_before_flush();
        let mut any_changes = false;

        // for releasing a NRT reader we must ensure that DW doesn't add any
        // segments or deletes until we are done with creating the NRT
        // DirectoryReader. We release the two stage full flush after we are
        // done opening the directory reader!
        let reader = self.do_get_reader(apply_all_deletes, write_all_deletes, &mut any_changes)?;
        {
            if any_changes {
                self.maybe_merge(MergerTrigger::FullFlush, None)?;
                // TODO if this failed, we must close the reader
            }
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
        &self,
        apply_all_deletes: bool,
        write_all_deletes: bool,
        any_changes: &mut bool,
    ) -> Result<StandardDirectoryReader> {
        let full_flush_lock = Arc::clone(&self.full_flush_lock);
        let _l = full_flush_lock.lock()?;
        // TODO tricky logic
        let writer = unsafe {
            let writer_ptr = self as *const IndexWriter as *mut IndexWriter;
            &mut *writer_ptr
        };

        let res = writer.flush_and_open(apply_all_deletes, write_all_deletes, any_changes);
        // Done: finish the full flush!
        writer.doc_writer.finish_full_flush(true);
        match res {
            Ok(reader) => {
                writer.process_events(false, true)?;
                self.do_after_flush();
                Ok(reader)
            }
            Err(e) => {
                error!("IW - hit error during NRT reader: {:?}", e);
                Err(e)
            }
        }
    }

    fn flush_and_open(
        &mut self,
        apply_all_deletes: bool,
        write_all_deletes: bool,
        any_changes: &mut bool,
    ) -> Result<StandardDirectoryReader> {
        let (changes, _) = self.doc_writer.flush_all_threads()?;
        *any_changes = changes;
        if !changes {
            // prevent double increment since docWriter#doFlush increments
            // the flush count if we flushed anything.
            self.flush_count.fetch_add(1, Ordering::AcqRel);
        }

        let reader = {
            // Prevent segmentInfos from changing while opening the
            // reader; in theory we could instead do similar retry logic,
            // just like we do when loading segments_N
            let lock = Arc::clone(&self.lock);
            let l = lock.lock()?;
            *any_changes |= self.maybe_apply_deletes(apply_all_deletes, &l)?;
            if write_all_deletes {
                // Must move the deletes to disk:
                self.reader_pool.commit(&self.segment_infos)?;
            }

            let r = StandardDirectoryReader::open_by_writer(
                self,
                &self.segment_infos,
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

    /// Returns the Directory used by this index.
    pub fn directory(&self) -> &DirectoryRc {
        // return the original directory the use supplied, unwrapped.
        &self.directory_orig
    }

    fn pool_readers(&self) -> bool {
        self.pool_readers.load(Ordering::Acquire)
    }

    pub fn keep_fully_deleted_segments(&self) -> bool {
        self.keep_fully_deleted_segments
    }

    /// Confirms that the incoming index sort (if any) matches the existing index
    /// sort (if any). This is unfortunately just best effort, because it could
    /// be the old index only has flushed segments.
    fn validate_index_sort(&self) -> Result<()> {
        if let Some(index_sort) = self.config.index_sort() {
            for info in &self.segment_infos.segments {
                if let Some(segment_sort) = info.info.index_sort() {
                    if segment_sort != index_sort {
                        bail!(IllegalArgument(
                            "config and segment index sort mismatch".into()
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    pub fn next_merge_id(&self) -> u32 {
        self.merge_id_gen.fetch_add(1, Ordering::AcqRel)
    }

    pub fn close(&mut self) -> Result<()> {
        debug!(
            "IW - start close. commit on close: {}",
            self.config.commit_on_close
        );
        if self.config.commit_on_close {
            self.shutdown()
        } else {
            self.rollback()
        }
    }

    /// Gracefully closes (commits, waits for merges), but calls rollback
    /// if there's an error so the IndexWriter is always closed. This is
    /// called from `#close` when `IndexWriterConfig#commit_on_close`
    /// is *true*.
    fn shutdown(&mut self) -> Result<()> {
        if self.pending_commit.is_some() {
            bail!(IllegalState(
                "cannot close: prepareCommit was already called with no corresponding call to \
                 commit"
                    .into()
            ));
        }

        // Ensure that only one thread actually gets to do the closing
        if self.should_close(true) {
            debug!("IW - now flush at close");

            if let Err(e) = self.do_shutdown() {
                // Be certain to close the index on any error
                if let Err(err) = self.rollback_internal() {
                    warn!("rollback internal failed when shutdown by '{:?}'", err);
                }
                return Err(e);
            }
        }
        Ok(())
    }

    fn do_shutdown(&mut self) -> Result<()> {
        self.flush(true, true)?;
        self.wait_for_merges()?;
        self.commit()?;
        self.rollback_internal() // ie close, since we just committed
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
    pub fn publish_flushed_segment(
        &mut self,
        new_segment: FlushedSegment,
        global_packet: Option<FrozenBufferedUpdates>,
    ) -> Result<()> {
        let res = self.do_publish_flushed_segment(new_segment, global_packet);
        self.flush_count.fetch_add(1, Ordering::AcqRel);
        self.do_after_flush();
        res
    }

    fn do_publish_flushed_segment(
        &mut self,
        mut new_segment: FlushedSegment,
        global_packet: Option<FrozenBufferedUpdates>,
    ) -> Result<()> {
        let lock = Arc::clone(&self.lock);
        let l = lock.lock()?;
        // Lock order IW -> BDS
        self.ensure_open(false)?;
        let updates_stream_lock = Arc::clone(&self.updates_stream_lock);
        let _bl = updates_stream_lock.lock()?;

        debug!("publish_flushed_segment");

        if let Some(global_packet) = global_packet {
            if global_packet.any() {
                self.buffered_updates_stream.push(global_packet)?;
            }
        }

        // Publishing the segment must be synched on IW -> BDS to make the sure
        // that no merge prunes away the seg. private delete packet
        let segment_updates = mem::replace(&mut new_segment.segment_updates, None);
        let next_gen = if let Some(p) = segment_updates {
            if p.any() {
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
        self.segment_infos
            .add(Arc::clone(&new_segment.segment_info));
        self.check_point(&l)
    }

    pub fn publish_frozen_updates(&mut self, packet: FrozenBufferedUpdates) -> Result<()> {
        debug_assert!(packet.any());
        let _gl = self.lock.lock()?;
        let _l = self.updates_stream_lock.lock()?;
        self.buffered_updates_stream.push(packet)?;
        Ok(())
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
        // don't call ensureOpen here: this acts like "close()" in closeable.

        // Ensure that only one thread actually gets to do the
        // closing, and make sure no commit is also in progress:
        if self.should_close(true) {
            unsafe {
                let writer = self as *const IndexWriter as *mut IndexWriter;
                (*writer).rollback_internal()
            }
        } else {
            Ok(())
        }
    }

    fn rollback_internal(&mut self) -> Result<()> {
        // Make sure no commit is running, else e.g. we can close while
        // another thread is still fsync'ing:
        let commit_lock = Arc::clone(&self.commit_lock);
        let _l = commit_lock.lock()?;
        self.rollback_internal_no_commit()
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
                        .dec_ref_by_segment(self.pending_commit.as_ref().unwrap())
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
                    .dec_ref_by_segment(self.pending_commit.as_ref().unwrap());
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

            self.write_lock.close()?;
        }
        Ok(())
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
    pub fn delete_all(&mut self) -> Result<u64> {
        self.ensure_open(true)?;
        let seq_no: u64;
        {
            let full_flush_lock = Arc::clone(&self.full_flush_lock);
            let l = full_flush_lock.lock()?;
            // TODO this did not locks the rlds, may cause error?
            let aborted_doc_count = self.doc_writer.lock_and_abort_all(&l)?;
            self.pending_num_docs
                .fetch_sub(aborted_doc_count as i64, Ordering::AcqRel);

            self.process_events(false, true)?;

            {
                let lock = Arc::clone(&self.lock);
                let mut _gl = lock.lock()?;
                // Abort any running merges
                _gl = self.abort_merges(_gl)?;
                // Let merges run again
                self.stop_merges = false;
                // Remove all segments
                self.pending_num_docs
                    .fetch_sub(self.segment_infos.total_max_doc() as i64, Ordering::AcqRel);
                self.segment_infos.clear();
                // Ask deleter to locate unreferenced files & remove them:
                self.deleter.checkpoint(&self.segment_infos, false)?;
                // don't refresh the deleter here since there might
                // be concurrent indexing requests coming in opening
                // files on the directory after we called DW#abort()
                // if we do so these indexing requests might hit FNF exceptions.
                // We will remove the files incrementally as we go...

                // don't bother saving any changes in our segment_infos
                self.reader_pool.drop_all(false)?;
                // Mask that the index has changes
                self.change_count.fetch_add(1, Ordering::AcqRel);
                self.segment_infos.changed();
                self.global_field_numbers.clear();

                seq_no = self.doc_writer.delete_queue.next_sequence_number();
                self.doc_writer.last_seq_no = seq_no;
            }
        }
        Ok(seq_no)
    }

    /// Called whenever the SegmentInfos has been updated and the index files
    /// referenced exist (correctly) in the index directory.
    fn check_point(&mut self, _lock: &MutexGuard<()>) -> Result<()> {
        self.changed();
        self.deleter.checkpoint(&self.segment_infos, false)
    }

    /// Wait for any currently outstanding merges to finish.
    ///
    /// It is guaranteed that any merges started prior to calling this method
    /// will have completed once this method completes.
    fn wait_for_merges(&mut self) -> Result<()> {
        // Give merge scheduler last chance to run, in case
        // any pending merges are waiting. We can't hold IW's lock
        // when going into merge because it can lead to deadlock.
        let writer = self as *mut IndexWriter;
        unsafe {
            self.merge_scheduler
                .merge(&mut *writer, MergerTrigger::Closing, false)?;
        }

        {
            let lock = Arc::clone(&self.lock);
            let mut l = lock.lock()?;
            self.ensure_open(false)?;
            debug!("IW - wait for merges");

            while !self.pending_merges.is_empty() || !self.running_merges.is_empty() {
                let (loc, _) = self.cond.wait_timeout(l, Duration::from_millis(1000))?;
                l = loc;
            }

            // sanity check
            debug_assert!(self.merging_segments.is_empty());
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
            let (loc, _) = self.cond.wait_timeout(lock, Duration::from_millis(1000))?;
            warn!(
                "IW - abort merges, waiting for running_merges to be empty, current size: {}",
                self.running_merges.len()
            );
            lock = loc;
        }

        self.cond.notify_all();

        println!(
            "debug abort_merges {} {} {}",
            self.pending_merges.len(),
            self.running_merges.len(),
            self.merging_segments.len()
        );

        debug_assert!(self.merging_segments.is_empty());
        trace!("IW - all running merges have aborted");
        Ok(lock)
    }

    /// Does finishing for a merge, which is fast but holds the
    /// synchronized lock on IndexWriter instance.
    fn merge_finish(&mut self, _lock: &MutexGuard<()>, merge: &mut OneMerge) {
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
        debug_assert!(self.running_merges.contains_key(&merge.id));
        self.running_merges.remove(&merge.id);
    }

    fn changed(&mut self) {
        self.change_count.fetch_add(1, Ordering::AcqRel);
        self.segment_infos.changed();
    }

    pub fn num_deleted_docs(&self, info: &SegmentCommitInfo) -> u32 {
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
            bail!("this IndexWriter is closed");
        }
        Ok(())
    }

    pub fn apply_deletes_and_purge(&mut self, force_purge: bool) -> Result<()> {
        let res = self.purge(force_purge);
        let any_changes = {
            let lock = Arc::clone(&self.lock);
            let l = lock.lock()?;
            self.apply_all_deletes_and_update(&l)?
        };
        if any_changes {
            if let Err(e) = self.maybe_merge(MergerTrigger::SegmentFlush, None) {
                error!("IW: try merge failed with: '{:?}'", e);
            }
        }
        self.flush_count.fetch_add(1, Ordering::AcqRel);
        match res {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    pub fn do_after_segment_flushed(
        &mut self,
        trigger_merge: bool,
        force_purge: bool,
    ) -> Result<()> {
        let res = self.purge(force_purge);
        if trigger_merge {
            if let Err(e) = self.maybe_merge(MergerTrigger::SegmentFlush, None) {
                error!("IW: try merge failed with: '{:?}'", e);
            }
        }
        match res {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Record that the files referenced by this `SegmentInfos` are still in use.
    pub fn inc_ref_deleter(&self, segment_infos: &SegmentInfos) -> Result<()> {
        self.ensure_open(true)?;
        self.deleter.inc_ref_by_segment(segment_infos, false);
        Ok(())
    }

    pub fn dec_ref_deleter(&self, segment_infos: &SegmentInfos) -> Result<()> {
        self.ensure_open(true)?;
        self.deleter.dec_ref_by_segment(segment_infos)
    }

    fn process_events(&mut self, trigger_merge: bool, force_purge: bool) -> Result<bool> {
        let mut processed = false;
        if self.tragedy.is_none() {
            loop {
                if let Some(event) = self.doc_writer.events.try_pop() {
                    processed = true;
                    event.process(self, trigger_merge, force_purge)?;
                } else {
                    break;
                }
            }
        }

        Ok(processed)
    }

    pub fn purge(&mut self, forced: bool) -> Result<u32> {
        unsafe {
            let doc_writer = &mut self.doc_writer as *mut DocumentsWriter;
            (*doc_writer).purge_buffer(self, forced)
        }
    }

    fn maybe_merge(&self, trigger: MergerTrigger, max_num_segments: Option<u32>) -> Result<()> {
        self.ensure_open(false)?;
        let writer = self as *const IndexWriter as *mut IndexWriter;
        unsafe {
            let new_merges_found = {
                let lock = Arc::clone(&self.lock);
                let l = lock.lock()?;
                (*writer).update_pending_merges(trigger, max_num_segments, &l)?
            };
            self.merge_scheduler
                .merge(&mut *writer, trigger, new_merges_found)
        }
    }

    fn update_pending_merges(
        &mut self,
        trigger: MergerTrigger,
        max_num_segments: Option<u32>,
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

        let mut spec: Option<MergeSpecification>;
        if let Some(max_num_segments) = max_num_segments {
            debug_assert!(
                trigger == MergerTrigger::Explicit || trigger == MergerTrigger::MergeFinished
            );
            spec = self.config.merge_policy().find_forced_merges(
                &self.segment_infos,
                max_num_segments,
                &self.segments_to_merge,
                self,
            )?;
            if let Some(ref mut spec) = spec {
                for merge in &mut spec.merges {
                    merge.max_num_segments.set(Some(max_num_segments));
                }
            }
        } else {
            spec = self
                .config
                .merge_policy()
                .find_merges(trigger, &self.segment_infos, self)?;
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

    pub fn next_merge(&mut self) -> Option<OneMerge> {
        let lock = Arc::clone(&self.lock);
        let _l = lock.lock().unwrap();
        if let Some(one_merge) = self.pending_merges.pop_front() {
            // Advance the merge from pending to running
            self.running_merges
                .insert(one_merge.id, one_merge.running_info());
            Some(one_merge)
        } else {
            None
        }
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
        self.change_count() != self.last_commit_change_count.load(Ordering::Acquire)
            || self.doc_writer.any_changes()
            || self.buffered_updates_stream.any()
    }

    pub fn commit(&self) -> Result<i64> {
        debug!("IW - commit: start");

        let writer = self as *const IndexWriter as *mut IndexWriter;

        let mut do_maybe_merge = false;
        let seq_no: i64;
        unsafe {
            let commit_lock = Arc::clone(&self.commit_lock);
            let l = commit_lock.lock()?;
            self.ensure_open(false)?;

            debug!("IW - commit: enter lock");

            seq_no = if self.pending_commit.is_none() {
                debug!("IW - commit: now prepare");
                (*writer).prepare_commit_internal(&mut do_maybe_merge, &l)?
            } else {
                debug!("IW - commit: already prepared");
                self.pending_seq_no.load(Ordering::Acquire)
            };

            (*writer).finish_commit()?;
        }

        if do_maybe_merge {
            self.maybe_merge(MergerTrigger::FullFlush, None)?;
        }
        Ok(seq_no)
    }

    // _l is self.commit_lock
    fn prepare_commit_internal(
        &mut self,
        do_maybe_merge: &mut bool,
        _l: &MutexGuard<()>,
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
        let to_commit: SegmentInfos;
        let mut any_segments_flushed = false;

        // This is copied from doFlush, except it's modified to
        // clone & incRef the flushed SegmentInfos inside the
        // sync block:
        {
            let full_flush_lock = Arc::clone(&self.full_flush_lock);
            let _fl = full_flush_lock.lock()?;
            let mut flush_success = true;

            let res = self.prepare_commit_internal_inner(
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
        let res = self.start_commit(to_commit);
        if res.is_err() {
            let lock = Arc::clone(&self.lock);
            let _l = lock.lock().unwrap();
            if !self.files_to_commit.is_empty() {
                self.deleter.dec_ref_without_error(&self.files_to_commit);
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
        seq_no: &mut u64,
        flush_success: &mut bool,
        any_segment_flushed: &mut bool,
    ) -> Result<SegmentInfos> {
        let (any_flushed, no) = self.doc_writer.flush_all_threads()?;
        *seq_no = no;
        *any_segment_flushed = any_flushed;
        if !any_flushed {
            // prevent double increment since docWriter#doFlush increments
            // the flush count if we flushed anything.
            self.flush_count.fetch_add(1, Ordering::AcqRel);
        }
        self.process_events(false, true)?;
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
    fn start_commit(&mut self, to_sync: SegmentInfos) -> Result<()> {
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
                let res = self.deleter.dec_ref_batch((&self.files_to_commit).iter());
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
                self.deleter.dec_ref_without_error(&self.files_to_commit);
                self.files_to_commit.clear();
            }
        }
        if let Err(err) = err {
            self.tragic_event(err, "start_commit")?;
        }
        Ok(())
    }

    fn start_commit_inner(
        &mut self,
        mut to_sync: SegmentInfos,
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

    fn finish_commit(&mut self) -> Result<()> {
        let mut commit_completed = false;

        let res = self.try_finish_commit(&mut commit_completed);

        if let Err(e) = res {
            if commit_completed {
                self.tragic_event(e, "finish_commit")?;
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
                res = self.deleter.dec_ref_batch(self.files_to_commit.iter());
            } else {
                // exc happened in finishCommit: not a tragedy
                self.deleter.dec_ref_without_error(&self.files_to_commit);
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
    fn files_exist(&self, to_syc: &SegmentInfos) -> bool {
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

    fn flush(&mut self, trigger_merge: bool, apply_all_deletes: bool) -> Result<()> {
        // NOTE: this method cannot be sync'd because maybe_merge() in turn calls
        // mergeScheduler.merge which in turn can take a long time to run and we
        // don't want to hold the lock for that.  In the case of ConcurrentMergeScheduler
        // this can lead to deadlock when it stalls due to too many running merges.

        // We can be called during close, when closing==true, so we must pass false to ensureOpen:
        self.ensure_open(false)?;
        if self.do_flush(apply_all_deletes)? && trigger_merge {
            self.maybe_merge(MergerTrigger::FullFlush, None)?;
        }
        Ok(())
    }

    /// Returns true a segment was flushed or deletes were applied.
    fn do_flush(&mut self, apply_deletes: bool) -> Result<bool> {
        if let Some(ref tragedy) = self.tragedy {
            bail!(IllegalState(format!(
                "this writer hit an unrecoverable error; cannot flush: {:?}",
                tragedy
            )));
        }
        self.do_before_flush();

        debug!("IW - start flush: apply_all_deletes={}", apply_deletes);
        // debug!("IW - index before flush");

        let mut any_changes = false;
        {
            let full_flush_lock = Arc::clone(&self.full_flush_lock);
            let _l = full_flush_lock.lock()?;

            let res = self.doc_writer.flush_all_threads();
            if let Ok((any_flush, _)) = &res {
                any_changes = *any_flush;
                if !any_changes {
                    // flush_count is incremented in flush_all_threads
                    self.flush_count.fetch_add(1, Ordering::AcqRel);
                }
            }
            self.doc_writer.finish_full_flush(true);
            if let Err(e) = self.process_events(false, true) {
                if !res.is_err() {
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
            let lock = Arc::clone(&self.lock);
            let l = lock.lock()?;
            any_changes |= self.maybe_apply_deletes(apply_deletes, &l)?;
            self.do_after_flush();
        }
        Ok(any_changes)
    }

    // the lock guard is refer to `self.lock`
    fn maybe_apply_deletes(&mut self, apply_all_deletes: bool, l: &MutexGuard<()>) -> Result<bool> {
        if apply_all_deletes {
            debug!("IW - apply all deletes during flush");
            return self.apply_all_deletes_and_update(l);
        }
        debug!(
            "IW - don't apply deletes now del_term_count={}, bytes_used={}",
            self.buffered_updates_stream.num_terms(),
            self.buffered_updates_stream.ram_bytes_used()
        );
        Ok(false)
    }

    fn apply_all_deletes_and_update(&mut self, l: &MutexGuard<()>) -> Result<bool> {
        self.flush_deletes_count.fetch_add(1, Ordering::AcqRel);

        debug!(
            "IW: now apply all deletes for all segments, max_doc={}",
            self.doc_writer.num_docs() + self.segment_infos.total_max_doc() as u32
        );

        let result: ApplyDeletesResult = self
            .buffered_updates_stream
            .apply_deletes_and_updates(&self.reader_pool, &mut self.segment_infos.segments)?;
        if result.any_deletes {
            self.check_point(l)?;
        }

        if !self.keep_fully_deleted_segments && !result.all_deleted.is_empty() {
            debug!("IW: drop 100% deleted segments.");

            for info in result.all_deleted {
                // If a merge has already registered for this
                // segment, we leave it in the readerPool; the
                // merge will skip merging it and will then drop
                // it once it's done:

                if !self.merging_segments.contains(&info.info.name) {
                    self.segment_infos.remove(&info);
                    self.pending_num_docs
                        .fetch_sub(info.info.max_doc() as i64, Ordering::AcqRel);
                    self.reader_pool.drop(&info)?;
                }
            }
            self.check_point(l)?;
        }

        self.buffered_updates_stream.prune(&self.segment_infos);
        Ok(result.any_deletes)
    }

    /// Cleans up residuals from a segment that could not be entirely flushed due to a error
    pub fn flush_failed(&mut self, info: &SegmentInfo) -> Result<()> {
        let mut files = HashSet::new();
        for f in info.files() {
            files.insert(f);
        }
        self.deleter.delete_new_files(files)
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
    pub fn add_document(&self, doc: Vec<Box<Fieldable>>) -> Result<u64> {
        self.update_document(doc, None)
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
    pub fn add_documents(&self, docs: Vec<Vec<Box<Fieldable>>>) -> Result<u64> {
        self.update_documents(docs, None)
    }

    /// Atomically deletes documents matching the provided
    /// delTerm and adds a block of documents with sequentially
    /// assigned document IDs, such that an external reader
    /// will see all or none of the documents.
    ///
    /// See `#addDocuments(Iterable)`.
    ///
    /// @return The <a href="#sequence_number">sequence number</a>
    /// for this operation
    ///
    /// @throws CorruptIndexException if the index is corrupt
    /// @throws IOException if there is a low-level IO error
    pub fn update_documents(
        &self,
        docs: Vec<Vec<Box<Fieldable>>>,
        term: Option<Term>,
    ) -> Result<u64> {
        self.ensure_open(true)?;

        let writer = self as *const IndexWriter as *mut IndexWriter;
        unsafe {
            let (seq_no, changed) = (*writer).doc_writer.update_documents(docs, term)?;
            if changed {
                (*writer).process_events(false, false)?;
            }

            Ok(seq_no)
        }
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
    pub fn delete_documents_by_terms(&mut self, terms: Vec<Term>) -> Result<u64> {
        self.ensure_open(true)?;

        let (seq_no, changed) = self.doc_writer.delete_terms(terms)?;
        if changed {
            self.process_events(true, false)?;
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
    pub fn delete_documents_by_queries(&mut self, queries: Vec<Arc<Query>>) -> Result<u64> {
        self.ensure_open(true)?;

        // LUCENE-6379: Specialize MatchAllDocsQuery
        for q in &queries {
            if q.query_type() == MATCH_ALL {
                return self.delete_all();
            }
        }

        let (seq_no, changed) = self.doc_writer.delete_queries(queries)?;
        if changed {
            self.process_events(true, false)?;
        }
        Ok(seq_no)
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
    pub fn update_document(&self, doc: Vec<Box<Fieldable>>, term: Option<Term>) -> Result<u64> {
        self.ensure_open(true)?;
        let writer = self as *const IndexWriter as *mut IndexWriter;
        unsafe {
            let (seq_no, changed) = (*writer).doc_writer.update_document(doc, term)?;
            if changed {
                (*writer).process_events(false, false)?;
            }

            Ok(seq_no)
        }
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
    pub fn update_numeric_doc_value(
        &mut self,
        _field: &str,
        _value: i64,
        _term: Term,
    ) -> Result<u64> {
        unimplemented!()
    }

    pub fn new_segment_name(&mut self) -> String {
        let _l = self.segment_infos_lock.lock().unwrap();
        self.segment_infos.changed();
        let counter = self.segment_infos.counter;
        self.segment_infos.counter += 1;
        format!("_{}", to_base36(counter as u64))
    }

    /// NOTE: this method creates a compound file for all files returned by
    /// info.files(). While, generally, this may include separate norms and
    /// deletion files, this SegmentInfo must not reference such files when this
    /// method is called, because they are not allowed within a compound file.
    pub fn create_compound_file<T: AsRef<Directory>>(
        &self,
        directory: &TrackingDirectoryWrapper<T>,
        info: &mut SegmentInfo,
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

    pub fn nrt_is_current(&self, infos: &SegmentInfos) -> bool {
        let _l = self.lock.lock().unwrap();
        infos.version == self.segment_infos.version
            && !self.doc_writer.any_changes()
            && !self.buffered_updates_stream.any()
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    pub fn is_open(&self) -> bool {
        !self.closing.load(Ordering::Acquire) || !self.is_closed()
    }

    // Tries to delete the given files if unreferenced
    pub fn delete_new_files(&self, files: &HashSet<String>) -> Result<()> {
        self.deleter.delete_new_files(files)
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
    pub fn force_merge(&mut self, max_num_segments: u32, do_wait: bool) -> Result<()> {
        self.ensure_open(true)?;

        if max_num_segments < 1 {
            bail!(IllegalArgument(format!(
                "max_num_segments must be >= 1, got {}",
                max_num_segments
            )));
        }

        trace!("IW - force_merge: flush at force merge");

        self.flush(true, true)?;
        {
            let lock = Arc::clone(&self.lock);
            let l = lock.lock()?;
            self.reset_merge_exceptions(&l);
            self.segments_to_merge.clear();
            for info in &self.segment_infos.segments {
                self.segments_to_merge.insert(Arc::clone(info), true);
            }
            self.merge_max_num_segments = max_num_segments;

            // Now mark all pending & running merges for forced merge:
            for merge in &mut self.pending_merges {
                merge.max_num_segments.set(Some(max_num_segments));
                self.segments_to_merge
                    .insert(Arc::clone(merge.info.as_ref().unwrap()), true);
            }
            let new_running_merges = HashMap::with_capacity(self.running_merges.len());
            let mut running_merges = mem::replace(&mut self.running_merges, new_running_merges);
            for (_, merge) in running_merges.drain() {
                merge.max_num_segments.set(Some(max_num_segments));
                self.segments_to_merge
                    .insert(Arc::clone(merge.info.as_ref().unwrap()), true);
                self.running_merges.insert(merge.id, merge);
            }
        }
        self.maybe_merge(MergerTrigger::Explicit, Some(max_num_segments))?;

        if do_wait {
            let lock = Arc::clone(&self.lock);
            let mut l = lock.lock()?;
            loop {
                if let Some(ref tragedy) = self.tragedy {
                    bail!(IllegalState(format!(
                        "this writer hit an unrecoverable error; cannot complete forceMerge: {:?}",
                        tragedy
                    )));
                }
                if !self.merge_exceptions.is_empty() {
                    // Forward any exceptions in background merge
                    // threads to the current thread:
                    for merge in &self.merge_exceptions {
                        if merge.max_num_segments.get().is_some() {
                            bail!("background merge hit exception");
                        }
                    }
                }

                if self.max_num_segments_merges_pending(&l) {
                    let (guard, _) = self.cond.wait_timeout(l, Duration::from_millis(1000))?;
                    l = guard;
                } else {
                    break;
                }
            }

            // If close is called while we are still
            // running, throw an exception so the calling
            // thread will know merging did not complete
            self.ensure_open(true)?;
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
    fn register_merge(&mut self, mut merge: OneMerge, lock: &MutexGuard<()>) -> Result<bool> {
        if merge.register_done {
            return Ok(true);
        }

        debug_assert!(!merge.segments.is_empty());
        if self.stop_merges {
            merge.rate_limiter.set_abort();
            bail!("merge is abort!");
        }

        let mut is_external = false;
        for info in &merge.segments {
            if self.merging_segments.contains(&info.info.name) {
                return Ok(false);
            }
            if !self.segment_infos.segments.contains(info) {
                return Ok(false);
            }
            if info.info.directory.as_ref() as *const Directory
                != self.directory_orig.as_ref() as *const Directory
            {
                is_external = true;
            }
            if self.segments_to_merge.contains_key(info) {
                merge
                    .max_num_segments
                    .set(Some(self.merge_max_num_segments));
            }
        }

        self.ensure_valid_merge(&merge, lock)?;

        merge.merge_gen = self.merge_gen;
        merge.is_external = is_external;

        // OK it does not conflict; now record that this merge is running
        // (while synchronized) to avoid race condition where two conflicting
        // merges from different threads, start
        for info in &merge.segments {
            self.merging_segments.insert(info.info.name.clone());
        }

        debug_assert_eq!(merge.estimated_merge_bytes, 0);
        debug_assert_eq!(merge.total_merge_bytes, 0);

        for info in &merge.segments {
            if info.info.max_doc > 0 {
                let del_count = self.num_deleted_docs(info.as_ref());
                debug_assert!((del_count as i32) <= info.info.max_doc);
                let total_size = info.size_in_bytes();
                let del_ratio = del_count as f64 / info.info.max_doc as f64;
                merge.estimated_merge_bytes += (total_size as f64 * (1.0 - del_ratio)) as u64;
                merge.total_merge_bytes = total_size as u64;
            }
        }

        // Merge is now registered
        merge.register_done = true;

        self.pending_merges.push_back(merge);

        Ok(true)
    }

    /// Merges the indicated segments, replacing them in the stack with a single segment.
    pub fn merge(&mut self, merge: &mut OneMerge) -> Result<()> {
        if let Err(e) = self.do_merge(merge) {
            self.tragic_event(e, "merge")?;
        }
        Ok(())
    }

    fn do_merge(&mut self, merge: &mut OneMerge) -> Result<()> {
        let res = self.execute_merge(merge);
        {
            let lock = Arc::clone(&self.lock);
            let l = lock.lock().unwrap();

            self.merge_finish(&l, merge);
            if res.is_err() {
                trace!("IW - hit error during merge");
            } else if !merge.rate_limiter.aborted() && merge.max_num_segments.get().is_some()
                || (!self.closed.load(Ordering::Acquire) && !self.closing.load(Ordering::Acquire))
            {
                // This merge (and, generally, any change to the
                // segments) may now enable new merges, so we call
                // merge policy & update pending merges.
                self.update_pending_merges(
                    MergerTrigger::MergeFinished,
                    merge.max_num_segments.get(),
                    &l,
                )?;
            }
        }
        res
    }

    fn execute_merge(&mut self, merge: &mut OneMerge) -> Result<()> {
        //        self.rate_limiters.
        //            .lock()?
        //            .insert(thread::current().id(), Arc::clone(&merge.rate_limiter));

        // let t0 = SystemTime::now();

        self.merge_init(merge)?;

        trace!("IW - now merge");

        self.merge_middle(merge)?;
        // self.merge_success();
        Ok(())
    }

    /// Does initial setup for a merge, which is fast but holds
    /// the synchronized lock on IndexWriter instance
    fn merge_init(&mut self, merge: &mut OneMerge) -> Result<()> {
        let lock = Arc::clone(&self.lock);
        let l = lock.lock().unwrap();

        let res = self.do_merge_init(merge, &l);
        if res.is_err() {
            trace!("IW - hit error in merge_init");
            self.merge_finish(&l, merge);
        }
        res
    }

    fn do_merge_init(&mut self, merge: &mut OneMerge, l: &MutexGuard<()>) -> Result<()> {
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
        let result: ApplyDeletesResult = self
            .buffered_updates_stream
            .apply_deletes_and_updates(&self.reader_pool, &merge.segments)?;

        if result.any_deletes {
            self.check_point(&l)?;
        }

        if !self.keep_fully_deleted_segments && !result.all_deleted.is_empty() {
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
            VERSION_LATEST.clone(),
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
    fn merge_middle(&mut self, merge: &mut OneMerge) -> Result<i32> {
        match self.do_merge_middle(merge) {
            Err(e) => {
                error!("merge_middle err {:?}", e);
                let l = self.lock.lock().unwrap();
                self.close_merge_readers(merge, true, &l)?;
                Err(e)
            }
            Ok(r) => Ok(r),
        }
    }

    fn do_merge_middle(&mut self, merge: &mut OneMerge) -> Result<i32> {
        merge.rate_limiter.check_abort()?;

        let context = IOContext::Merge(merge.store_merge_info());

        let dir_wrapper: DirectoryRc =
            Arc::new(TrackingDirectoryWrapper::new(self.merge_directory.clone()));
        merge.readers = Vec::with_capacity(merge.segments.len());

        // This is try/finally to make sure merger's readers are closed:
        let mut seg_upto = 0;
        while seg_upto < merge.segments.len() {
            // Hold onto the "live" reader; we will use this to
            // commit merged deletes
            let rld = self.reader_pool.get_or_create(&merge.segments[seg_upto])?;

            // Carefully pull the most recent live docs and reader
            let mut reader: Arc<SegmentReader>;
            let live_docs: BitsRef;
            let del_count: i32;
            {
                let _l = self.lock.lock()?;

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
            }

            // Deletes might have happened after we pulled the merge reader and
            // before we got a read-only copy of the segment's actual live docs
            // (taking pending deletes into account). In that case we need to
            // make a new reader with updated live docs and del count.
            if reader.num_deleted_docs() != del_count {
                // fix the reader's live docs and del count
                debug_assert!(del_count > reader.num_deleted_docs());

                let new_reader = {
                    let _l = self.lock.lock()?;
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
        let merge_readers: Vec<Arc<SegmentReader>> = merge.readers.iter().map(Arc::clone).collect();
        let mut merger: SegmentMerger = SegmentMerger::new(
            merge_readers,
            &merge.info.as_ref().unwrap().info,
            Arc::clone(&dir_wrapper),
            FieldNumbersRef::new(&mut self.global_field_numbers),
            context,
        )?;
        merge.rate_limiter.check_abort()?;

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
            self.commit_merge(merge, &merger.merge_state)?;
            return Ok(0);
        }
        debug_assert!(merger.merge_state.segment_info().max_doc > 0);

        // Very important to do this before opening the reader
        // because codec must know if prox was written for
        // this segment:
        // System.out.println("merger set hasProx=" + merger.hasProx() + " seg=" + merge.info.name);
        let use_compound_file = {
            let _l = self.lock.lock()?;
            self.config.merge_policy().use_compound_file(
                &self.segment_infos,
                merge.info.as_ref().unwrap().as_ref(),
                self,
            )
        };

        if use_compound_file {
            let tracking_cfs_dir = TrackingDirectoryWrapper::new(&self.merge_directory);

            let info = merge.info.as_mut().unwrap();
            let segment_info = Arc::get_mut(info).unwrap();
            let files_to_remove = segment_info.files();

            if let Err(e) =
                self.create_compound_file(&tracking_cfs_dir, &mut segment_info.info, &context)
            {
                let _l = self.lock.lock().unwrap();
                if merge.rate_limiter.aborted() {
                    // This can happen if rollback is called while we were building
                    // our CFS -- fall through to logic below to remove the non-CFS
                    // merged files:
                    return Ok(0);
                } else {
                    self.delete_new_files(&segment_info.files())?;
                    return Err(e);
                }
            }

            // So that, if we hit exc in deleteNewFiles (next)
            // or in commitMerge (later), we close the
            // per-segment readers in the finally clause below:
            {
                let _l = self.lock.lock().unwrap();
                // delete new non cfs files directly: they were never
                // registered with IFD
                self.delete_new_files(&files_to_remove)?;

                if merge.rate_limiter.aborted() {
                    self.delete_new_files(&segment_info.files())?;
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
            if let Err(e) = self.config.codec().segment_info_format().write(
                self.directory.as_ref(),
                &mut segment_info.info,
                &context,
            ) {
                self.delete_new_files(&segment_info.files())?;
                return Err(e);
            }
        }

        if !self.commit_merge(merge, &merger.merge_state)? {
            // commitMerge will return false if this merge was aborted
            return Ok(0);
        }

        Ok(merge.info.as_ref().unwrap().info.max_doc)
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
        merge: &OneMerge,
        merge_state: &MergeState,
        _lock: &MutexGuard<()>,
    ) -> Result<Option<Arc<ReadersAndUpdates>>> {
        // Carefully merge deletes that occurred after we started merging:
        let mut min_gen = i64::max_value();

        // lazy init (only when we find a delete to carry over):
        let mut holder = MergedDeletesAndUpdates::default();
        debug_assert_eq!(merge.segments.len(), merge_state.doc_maps.len());
        for i in 0..merge.segments.len() {
            let info = &merge.segments[i];
            min_gen = min_gen.min(info.buffered_deletes_gen());
            let max_doc = info.info.max_doc;
            let prev_live_docs = merge.readers[i].live_docs();
            let rld: Arc<ReadersAndUpdates> = self.reader_pool.get(info.as_ref()).unwrap();
            let inner = rld.inner.lock()?;

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
                if cur_live_doc.as_ref() as *const Bits != prev_live_docs.as_ref() as *const Bits {
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
                    }
                }
            }
        }

        merge
            .info
            .as_ref()
            .unwrap()
            .set_buffered_deletes_gen(min_gen);
        Ok(mem::replace(&mut holder.merged_deletes_and_updates, None))
    }

    fn commit_merge(&mut self, merge: &mut OneMerge, merge_state: &MergeState) -> Result<bool> {
        let lock = Arc::clone(&self.lock);
        let l = lock.lock()?;
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
            self.commit_merged_deletes_and_updates(merge, merge_state, &l)?
        };

        // If the doc store we are using has been closed and
        // is in now compound format (but wasn't when we
        // started), then we will switch to the compound
        // format as well:
        debug_assert!(!self
            .segment_infos
            .segments
            .contains(merge.info.as_ref().unwrap()));

        let all_deleted = merge.segments.len() == 0
            || merge.info.as_ref().unwrap().info.max_doc == 0
            || (merge_updates.is_some()
                && merge_updates.as_ref().unwrap().pending_delete_count()
                    == merge.info.as_ref().unwrap().info.max_doc as u32);
        let drop_segment = all_deleted && !self.keep_fully_deleted_segments;

        // If we merged no segments then we better be dropping the new segment:
        debug_assert!(merge.segments.len() > 0 || drop_segment);
        debug_assert!(
            merge.info.as_ref().unwrap().info.max_doc > 0
                || self.keep_fully_deleted_segments
                || drop_segment
        );

        if let Some(merged_updates) = merge_updates {
            if drop_segment {
                merged_updates.drop_changes();
            }
            // Pass false for assertInfoLive because the merged
            // segment is not yet live (only below do we commit it
            // to the segmentInfos):
            if let Err(e) = self.reader_pool.release(&merged_updates, false) {
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
        self.segment_infos.apply_merge_changes(merge, drop_segment);

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
                self.check_point(&l)?;
            }
            Err(e) => {
                // Ignore so we keep throwing original exception.
                let _ = self.check_point(&l);
                return Err(e);
            }
        }

        if merge.max_num_segments.get().is_some() && !drop_segment {
            // cascade the force_merge:
            if !self
                .segments_to_merge
                .contains_key(merge.info.as_ref().unwrap())
            {
                self.segments_to_merge
                    .insert(Arc::clone(merge.info.as_ref().unwrap()), false);
            }
        }

        Ok(true)
    }

    fn close_merge_readers(
        &self,
        merge: &mut OneMerge,
        suppress_errors: bool,
        _lock: &MutexGuard<()>,
    ) -> Result<()> {
        let drop = !suppress_errors;

        let mut res = Ok(());
        for reader in &merge.readers {
            let rld = self.reader_pool.get(reader.si.as_ref());
            // We still hold a ref so it should not have been removed:
            debug_assert!(rld.is_some());
            let rld = rld.unwrap();
            if let Err(e) = self.release_reader_and_updates(rld, drop) {
                if res.is_ok() {
                    res = Err(e);
                }
            }
            // merge.readers[i] = None;
        }
        merge.readers.clear();

        // merge.merge_finished();

        if !suppress_errors {
            return res;
        }
        Ok(())
    }

    fn release_reader_and_updates(&self, rld: Arc<ReadersAndUpdates>, drop: bool) -> Result<()> {
        if drop {
            rld.drop_changes();
        }
        //            else {
        //                rld.drop_merging_updates();
        //            }
        // rld.release(reader);
        self.reader_pool.release(&rld, true)?;
        if drop {
            self.reader_pool.drop(rld.info.as_ref())?;
        }
        Ok(())
    }

    fn ensure_valid_merge(&self, merge: &OneMerge, _lock: &MutexGuard<()>) -> Result<()> {
        for info in &merge.segments {
            if !self.segment_infos.segments.contains(info) {
                bail!(
                    "MergeError: MergePolicy selected a segment '{}' that is not in the current \
                     index",
                    &info.info.name
                );
            }
        }
        Ok(())
    }

    fn tragic_event(&mut self, tragedy: Error, location: &str) -> Result<()> {
        trace!("IW - hit tragic '{:?}' inside {}", &tragedy, location);

        {
            let lock = Arc::clone(&self.lock);
            let _l = lock.lock()?;

            // It's possible you could have a really bad day
            if self.tragedy.is_some() {
                bail!(tragedy);
            }

            self.tragedy = Some(tragedy);
        }

        // if we are already closed (e.g. called by rollback), this will be a no-op.
        if self.should_close(false) {
            self.rollback_internal()?;
        }

        bail!(IllegalState(format!(
            "this writer hit an unrecoverable error; {:?}",
            &self.tragedy
        )))
    }
}

impl Drop for IndexWriter {
    fn drop(&mut self) {
        if self.config.commit_on_close {
            if let Err(e) = self.shutdown() {
                error!("IndexWriter: shutdown on close failed by: {:?}", e);
            }
        } else {
            if let Err(e) = self.rollback() {
                error!("IndexWriter: rollback on close failed by: {:?}", e);
            }
        }
    }
}

/// Holds shared SegmentReader instances. IndexWriter uses SegmentReaders for:
/// 1) applying deletes, 2) doing merges, 3) handing out a real-time reader.
/// This pool reuses instances of the SegmentReaders in all these places if it
/// is in "near real-time mode" (getReader() has been called on this instance).
pub struct ReaderPool {
    lock: Mutex<()>,
    reader_map: Arc<Mutex<HashMap<String, Arc<ReadersAndUpdates>>>>,
    // key is SegmentCommitInfo.
    // info.name
    index_writer: *mut IndexWriter,
    inited: bool,
}

impl ReaderPool {
    pub fn new() -> Self {
        ReaderPool {
            lock: Mutex::new(()),
            reader_map: Arc::new(Mutex::new(HashMap::new())),
            index_writer: ptr::null_mut(),
            inited: false,
        }
    }

    pub fn init(&mut self, index_writer: &IndexWriter) {
        self.index_writer = index_writer as *const IndexWriter as *mut IndexWriter;
        self.inited = true;
    }

    fn writer(&self) -> &mut IndexWriter {
        debug_assert!(self.inited);
        unsafe { &mut *self.index_writer }
    }

    // used only by asserts
    pub fn info_is_live(&self, info: &SegmentCommitInfo, lock: Option<&MutexGuard<()>>) -> bool {
        let _l = if lock.is_none() {
            Some(self.lock.lock().unwrap())
        } else {
            None
        };

        for i in &self.writer().segment_infos.segments {
            if i.info.name == info.info.name {
                return true;
            }
        }
        false
    }

    pub fn drop(&self, info: &SegmentCommitInfo) -> Result<()> {
        let _lock = self.lock.lock().unwrap();
        if let Some(rld) = self.reader_map.lock()?.remove(&info.info.name) {
            // debug_assert!(info == rld.info);
            rld.drop_readers()?;
        }
        Ok(())
    }

    /// Remove all our references to readers, and commits any pending changes
    fn drop_all(&mut self, do_save: bool) -> Result<()> {
        let _l = self.lock.lock()?;
        let mut prior_err = Ok(());

        let keys: Vec<String> = self.reader_map.lock()?.keys().map(|s| s.clone()).collect();

        let mut reader_map = self.reader_map.lock()?;
        for key in keys {
            {
                let rld = reader_map.get(&key).unwrap();
                if let Err(e) = self.flush_and_check(do_save, rld) {
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
            let mut rld = reader_map.remove(&key).unwrap();

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

    fn flush_and_check(&self, do_save: bool, rld: &ReadersAndUpdates) -> Result<()> {
        if do_save && rld.write_live_docs(&self.writer().directory)? {
            // Make sure we only write del docs and field updates for a live segment:
            debug_assert!(self.info_is_live(rld.info.as_ref(), None));
            // Must checkpoint because we just
            // created new _X_N.del and field updates files;
            // don't call IW.checkpoint because that also
            // increments SIS.version, which we do not want to
            // do here: it was done previously (after we
            // invoked BDS.applyDeletes), whereas here all we
            // did was move the state to disk:
            self.check_point_no_sis()?;
        }
        Ok(())
    }

    fn any_pending_deletes(&self) -> bool {
        let _lock = self.lock.lock().unwrap();
        (&self.reader_map.lock().unwrap())
            .values()
            .any(|rld| rld.pending_delete_count() > 0)
    }

    pub fn release(&self, rld: &Arc<ReadersAndUpdates>, _assert_info_live: bool) -> Result<()> {
        let _lock = self.lock.lock().unwrap();
        // Matches inc_ref in get:
        rld.dec_ref();

        // Pool still holds a ref:
        debug_assert!(rld.ref_count() >= 1);

        if !self.writer().pool_readers() && rld.ref_count() == 1 {
            // This is the last ref to this RLD, and we're not
            // pooling, so remove it:
            if rld.write_live_docs(&self.writer().directory)? {
                // Must checkpoint because we just
                // created new _X_N.del and field updates files;
                // don't call IW.checkpoint because that also
                // increments SIS.version, which we do not want to
                // do here: it was done previously (after we
                // invoked BDS.applyDeletes), whereas here all we
                // did was move the state to disk:
                self.check_point_no_sis()?;
            }

            rld.drop_readers()?;
            self.reader_map.lock()?.remove(&rld.info.info.name);
        }
        Ok(())
    }

    fn check_point_no_sis(&self) -> Result<()> {
        let writer = self.writer();
        writer.change_count.fetch_add(1, Ordering::AcqRel);
        writer.deleter.checkpoint(&writer.segment_infos, false)
    }

    /// Commit live docs changes for the segment readers for the previous infos.
    pub fn commit(&self, infos: &SegmentInfos) -> Result<()> {
        let _l = self.lock.lock()?;
        for info in &infos.segments {
            if let Some(rld) = self.reader_map.lock()?.get(&info.info.name) {
                if rld.write_live_docs(&self.writer().directory)? {
                    // Make sure we only write del docs for a live segment:
                    debug_assert!(self.info_is_live(info.as_ref(), Some(&_l)));

                    // Must checkpoint because we just
                    // created new _X_N.del and field updates files;
                    // don't call IW.checkpoint because that also
                    // increments SIS.version, which we do not want to
                    // do here: it was done previously (after we
                    // invoked BDS.applyDeletes), whereas here all we
                    // did was move the state to disk:
                    self.check_point_no_sis()?;
                }
            }
        }
        Ok(())
    }

    /// Obtain a ReadersAndLiveDocs instance from the reader_pool.
    pub fn get(&self, info: &SegmentCommitInfo) -> Option<Arc<ReadersAndUpdates>> {
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
    pub fn get_or_create(&self, info: &Arc<SegmentCommitInfo>) -> Result<Arc<ReadersAndUpdates>> {
        // Make sure no new readers can be opened if another thread just closed us:
        // TODO, how can we ensure we are open when we don't directly refer to IndexWrier
        self.writer().ensure_open(false)?;

        let _lock = self.lock.lock().unwrap();

        debug_assert!(ptr::eq(
            info.info.directory.as_ref(),
            self.writer().directory_orig.as_ref(),
        ));

        if !self.reader_map.lock()?.contains_key(&info.info.name) {
            let rld = Arc::new(ReadersAndUpdates::new(self.writer(), Arc::clone(info)));
            // Steal initial reference:
            self.reader_map.lock()?.insert(info.info.name.clone(), rld);
        }

        let rld = Arc::clone(self.reader_map.lock()?.get_mut(&info.info.name).unwrap());
        rld.inc_ref();
        Ok(rld)
    }
}

impl Drop for ReaderPool {
    fn drop(&mut self) {
        if let Err(e) = self.drop_all(false) {
            error!("ReaderPool: drop_all on close failed by: {:?}", e);
        }
    }
}

// reads latest field infos for the commit
// this is used on IW init and addIndexes(Dir) to create/update the global field map.
// TODO: fix tests abusing this method!
fn read_field_infos(si: &SegmentCommitInfo) -> Result<FieldInfos> {
    let codec = si.info.codec();
    let reader = codec.field_infos_format();

    if si.has_field_updates() {
        // there are updates, we read latest (always outside of CFS)
        let segment_suffix = to_base36(si.field_infos_gen() as u64);
        reader.read(
            si.info.directory.as_ref(),
            &si.info,
            &segment_suffix,
            &IO_CONTEXT_READONCE,
        )
    } else if si.info.is_compound_file() {
        let cfs = codec.compound_format().get_compound_reader(
            Arc::clone(&si.info.directory),
            &si.info,
            &IOContext::Default,
        )?;
        reader.read(cfs.as_ref(), &si.info, "", &IO_CONTEXT_READONCE)
    } else {
        // no cfs
        reader.read(
            si.info.directory.as_ref(),
            &si.info,
            "",
            &IO_CONTEXT_READONCE,
        )
    }
}

// Used by IndexWriter to hold open SegmentReaders (for searching or merging),
// plus pending deletes and updates, for a given segment
pub struct ReadersAndUpdates {
    // Not final because we replace (clone) when we need to
    // change it and it's been shared:
    pub info: Arc<SegmentCommitInfo>,
    pub inner: Mutex<ReadersAndUpdatesInner>,
    // Tracks how many consumers are using this instance:
    ref_count: AtomicU32,
}

impl ReadersAndUpdates {
    pub fn new(writer: &IndexWriter, info: Arc<SegmentCommitInfo>) -> Self {
        ReadersAndUpdates {
            inner: Mutex::new(ReadersAndUpdatesInner::new(writer)),
            ref_count: AtomicU32::new(1),
            info,
        }
    }

    fn with_reader(writer: &IndexWriter, reader: SegmentReader) -> Self {
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
    pub fn write_live_docs(&self, dir: &DirectoryRc) -> Result<bool> {
        let mut guard = self.inner.lock()?;
        guard.write_live_docs(&self.info, dir)
    }

    pub fn drop_readers(&self) -> Result<()> {
        let mut guard = self.inner.lock()?;
        guard.reader = None;
        self.dec_ref();
        Ok(())
    }

    pub fn get_readonly_clone(&self, context: &IOContext) -> Result<SegmentReader> {
        let mut guard = self.inner.lock()?;
        guard.get_readonly_clone(&self.info, context)
    }

    pub fn test_doc_id(&self, doc_id: usize) -> Result<bool> {
        let guard = self.inner.lock().unwrap();
        debug_assert!(guard.live_docs.is_some());
        guard.live_docs().get(doc_id)
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

    /// Returns a reader for merge. this method applies filed update if there are
    /// any and marks that this segment is currently merging.
    pub fn reader_for_merge(&self, context: &IOContext) -> Result<Arc<SegmentReader>> {
        // must execute these two statements as atomic operation, otherwise we
        // could lose updates if e.g. another thread calls writeFieldUpdates in
        // between, or the updates are applied to the obtained reader, but then
        // re-applied in IW.commitMergedDeletes (unnecessary work and potential
        // bugs).
        self.create_reader_if_not_exist(context)?;
        let mut guard = self.inner.lock()?;
        guard.is_merging = true;
        Ok(Arc::clone(guard.reader.as_ref().unwrap()))
    }
}

pub struct ReadersAndUpdatesInner {
    writer: *mut IndexWriter,
    // Set once (null, and then maybe set, and never set again):
    reader: Option<Arc<SegmentReader>>,
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
}

impl ReadersAndUpdatesInner {
    fn new(writer: &IndexWriter) -> Self {
        ReadersAndUpdatesInner {
            writer: writer as *const IndexWriter as *mut IndexWriter,
            reader: None,
            live_docs: None,
            pending_delete_count: 0,
            live_docs_shared: true,
            is_merging: false,
        }
    }

    fn with_reader(writer: &IndexWriter, reader: SegmentReader) -> Self {
        let live_docs = reader.live_docs();
        let pending_delete_count = reader.num_deleted_docs();
        debug_assert!(pending_delete_count >= 0);
        ReadersAndUpdatesInner {
            writer: writer as *const IndexWriter as *mut IndexWriter,
            reader: Some(Arc::new(reader)),
            live_docs: Some(live_docs),
            pending_delete_count: pending_delete_count as u32,
            live_docs_shared: true,
            is_merging: false,
        }
    }

    // Call only from assert
    fn verify_doc_counts(&self, info: &Arc<SegmentCommitInfo>) -> bool {
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

        assert_eq!(
            info.info.max_doc - info.del_count() - self.pending_delete_count as i32,
            count
        );
        true
    }

    pub fn create_reader_if_not_exist(
        &mut self,
        info: &Arc<SegmentCommitInfo>,
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

    fn live_docs(&self) -> &BitsRef {
        debug_assert!(self.live_docs.is_some());
        self.live_docs.as_ref().unwrap()
    }

    pub fn reader(&self) -> &Arc<SegmentReader> {
        debug_assert!(self.reader.is_some());

        self.reader.as_ref().unwrap()
    }

    fn release(&self, info: &Arc<SegmentCommitInfo>) -> Result<()> {
        debug_assert_eq!(&info.info.name, &self.reader.as_ref().unwrap().si.info.name);
        // reader.dec_ref()?;
        Ok(())
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
        info: &Arc<SegmentCommitInfo>,
        context: &IOContext,
    ) -> Result<SegmentReader> {
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

    pub fn init_writable_live_docs(&mut self, info: &Arc<SegmentCommitInfo>) -> Result<()> {
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
                live_docs = Some(Arc::from(bits));
            }

            self.live_docs = live_docs;
            self.live_docs_shared = false;
        }
        Ok(())
    }

    // Commit live docs (writes new _X_N.del files) and field updates (writes new
    // _X_N updates files) to the directory; returns true if it wrote any file
    // and false if there were no new deletes or updates to write:
    pub fn write_live_docs(
        &mut self,
        info: &Arc<SegmentCommitInfo>,
        dir: &DirectoryRc,
    ) -> Result<bool> {
        debug_assert!(self.live_docs.is_some());

        if self.pending_delete_count == 0 {
            return Ok(false);
        }

        // we have new deletes
        debug_assert_eq!(self.live_docs().len(), info.info.max_doc as usize);

        // Do this so we can delete any created files on exceptions;
        // this saves all codecs from having to do it:
        let tracking_dir = TrackingDirectoryWrapper::new(dir);

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
                delete_file_ignoring_error(&tracking_dir, file_name);
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
        // self.merging_dv_updates.clear();
        self.is_merging = false;
    }

    fn write_field_infos_gen(
        &self,
        info: &Arc<SegmentCommitInfo>,
        field_infos: &FieldInfos,
        dir: &DirectoryRc,
        infos_format: &FieldInfosFormat,
    ) -> Result<HashSet<String>> {
        let next_field_infos_gen = info.next_field_infos_gen();
        let segment_suffix = to_base36(next_field_infos_gen as u64);
        // we write approximately that many bytes (based on Lucene46DVF):
        // HEADER + FOOTER: 40
        // 90 bytes per-field (over estimating long name and attributes map)
        let est_infos_size = 40 + 90 * field_infos.len();
        let infos_context = IOContext::Flush(FlushInfo::new(
            info.info.max_doc() as u32,
            est_infos_size as u64,
        ));
        // separately also track which files were created for this gen
        let tracking_dir = TrackingDirectoryWrapper::new(dir);
        infos_format.write(
            &tracking_dir,
            &info.info,
            &segment_suffix,
            field_infos,
            &infos_context,
        )?;
        info.advance_del_gen();
        Ok(tracking_dir.get_create_files())
    }

    // Writes field updates (new _X_N updates files) to the directory
    pub fn write_field_updates(&self, _dir: &Directory) -> Result<()> {
        unreachable!()
    }
}

#[derive(Clone)]
struct RateLimitFilterDirectory {
    dir: DirectoryRc,
    // reference to IndexWriter.rate_limiter
    rate_limiter: Arc<ThreadLocal<Arc<RateLimiter>>>,
}

impl RateLimitFilterDirectory {
    pub fn new(dir: DirectoryRc, rate_limiter: Arc<ThreadLocal<Arc<RateLimiter>>>) -> Self {
        RateLimitFilterDirectory { dir, rate_limiter }
    }
}

impl Directory for RateLimitFilterDirectory {
    fn list_all(&self) -> Result<Vec<String>> {
        self.dir.list_all()
    }

    fn file_length(&self, name: &str) -> Result<i64> {
        self.dir.file_length(name)
    }

    fn create_output(&self, name: &str, context: &IOContext) -> Result<Box<IndexOutput>> {
        debug_assert!(context.is_merge());
        //        let rate_limiter = Arc::clone(&self.rate_limiter.get().unwrap());
        //        let index_output = self.dir.create_output(name, context)?;
        //
        //        Ok(Box::new(RateLimitIndexOutput::new(
        //            rate_limiter,
        //            index_output,
        //        )))
        self.dir.create_output(name, context)
    }

    fn open_input(&self, name: &str, ctx: &IOContext) -> Result<Box<IndexInput>> {
        self.dir.open_input(name, ctx)
    }

    fn obtain_lock(&self, name: &str) -> Result<Box<Lock>> {
        self.dir.obtain_lock(name)
    }

    fn create_temp_output(
        &self,
        prefix: &str,
        suffix: &str,
        ctx: &IOContext,
    ) -> Result<Box<IndexOutput>> {
        self.dir.create_temp_output(prefix, suffix, ctx)
    }

    fn delete_file(&self, name: &str) -> Result<()> {
        self.dir.delete_file(name)
    }

    fn sync(&self, name: &HashSet<String>) -> Result<()> {
        self.dir.sync(name)
    }

    fn sync_meta_data(&self) -> Result<()> {
        self.dir.sync_meta_data()
    }

    fn rename(&self, source: &str, dest: &str) -> Result<()> {
        self.dir.rename(source, dest)
    }
}

impl fmt::Display for RateLimitFilterDirectory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "RateLimitFilterDirectory({})", self.dir.as_ref())
    }
}

impl Drop for RateLimitFilterDirectory {
    fn drop(&mut self) {}
}

impl AsRef<Directory + 'static> for RateLimitFilterDirectory {
    fn as_ref(&self) -> &(Directory + 'static) {
        self
    }
}

#[derive(Default)]
struct MergedDeletesAndUpdates {
    merged_deletes_and_updates: Option<Arc<ReadersAndUpdates>>,
    inited_writable_live_docs: bool,
}

impl MergedDeletesAndUpdates {
    fn init(
        &mut self,
        reader_pool: &ReaderPool,
        merge: &OneMerge,
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
