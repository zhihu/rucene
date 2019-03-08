use core::index::delete_policy::IndexDeletionPolicy;
use core::index::index_commit::IndexCommit;
use core::index::index_writer::INDEX_WRITE_LOCK_NAME;
use core::index::SegmentInfos;
use core::index::{parse_generation, parse_segment_name};
use core::index::{CODEC_FILE_PATTERN, INDEX_FILE_SEGMENTS};
use core::index::{INDEX_FILE_OLD_SEGMENT_GEN, INDEX_FILE_PENDING_SEGMENTS};
use core::store::{Directory, DirectoryRc};

use regex::Regex;
use std::cmp::{max, Ordering};
use std::collections::{HashMap, HashSet};
use std::mem;
use std::ptr;
use std::sync::{Arc, RwLock};

use error::{ErrorKind, Result};

/// This class keeps track of each SegmentInfos instance that
/// is still "live", either because it corresponds to a
/// segments_N file in the Directory (a "commit", i.e. a
/// committed SegmentInfos) or because it's an in-memory
/// SegmentInfos that a writer is actively updating but has
/// not yet committed.  This class uses simple reference
/// counting to map the live SegmentInfos instances to
/// individual files in the Directory.
///
/// The same directory file may be referenced by more than
/// one IndexCommit, i.e. more than one SegmentInfos.
/// Therefore we count how many commits reference each file.
/// When all the commits referencing a certain file have been
/// deleted, the refcount for that file becomes zero, and the
/// file is deleted.
///
/// A separate deletion policy interface
/// (IndexDeletionPolicy) is consulted on creation (onInit)
/// and once per commit (onCommit), to decide when a commit
/// should be removed.
///
/// It is the business of the IndexDeletionPolicy to choose
/// when to delete commit points.  The actual mechanics of
/// file deletion, retrying, etc, derived from the deletion
/// of commit points is the business of the IndexFileDeleter.
///
/// The current default deletion policy is {@link
/// KeepOnlyLastCommitDeletionPolicy}, which removes all
/// prior commits when a new commit has completed.  This
/// matches the behavior before 2.2.
///
/// Note that you must hold the write.lock before
/// instantiating this class.  It opens segments_N file(s)
/// directly with no retry logic.
pub struct IndexFileDeleter {
    /// Reference count for all files in the index. Counts
    /// how many existing commits reference a file.
    ref_counts: Arc<RwLock<HashMap<String, RefCount>>>,
    /// Holds all commits (segments_N) currently in the index.
    /// this will have just 1 commit if you are using the default
    /// delete policy (KeepOnlyLastCommitDeletionPolicy). Other policies
    /// may leave commit points live for longer in which case this list
    /// would be longer than 1.
    commits: Vec<CommitPoint>,
    /// Holds files we had inc_ref'd from the previous non-commit checkpoint:
    last_files: Vec<String>,
    /// Commits that the IndexDeletionPolicy have decided to delete:
    commits_to_delete: Vec<CommitPoint>,
    directory_orig: DirectoryRc,
    directory: DirectoryRc,
    policy: Box<IndexDeletionPolicy>,
    pub starting_commit_deleted: bool,
    last_segment_infos: Option<SegmentInfos>,
    inited: bool,
}

impl IndexFileDeleter {
    pub fn new(
        directory_orig: DirectoryRc,
        directory: DirectoryRc,
        policy: Box<IndexDeletionPolicy>,
    ) -> Self {
        IndexFileDeleter {
            ref_counts: Arc::new(RwLock::new(HashMap::new())),
            commits: vec![],
            last_files: vec![],
            commits_to_delete: vec![],
            directory_orig,
            directory,
            policy,
            starting_commit_deleted: false,
            last_segment_infos: None,
            inited: false,
        }
    }

    pub fn init(
        &mut self,
        files: &[String],
        segment_infos: &mut SegmentInfos,
        initial_index_exists: bool,
        is_reader_init: bool,
    ) -> Result<()> {
        let current_segments_file = segment_infos.segment_file_name();

        let mut current_commit_point_idx: Option<usize> = None;
        if current_segments_file.is_some() {
            let pattern = Regex::new(CODEC_FILE_PATTERN).unwrap();
            for filename in files {
                if !filename.ends_with("write.lock")
                    && (pattern.is_match(filename)
                        || filename.starts_with(INDEX_FILE_SEGMENTS)
                        || filename.starts_with(INDEX_FILE_PENDING_SEGMENTS))
                {
                    // Add this file to ref_counts with initial count 0.
                    {
                        if !self.ref_counts.read()?.contains_key(filename) {
                            self.ref_counts
                                .write()?
                                .insert(filename.to_string(), RefCount::default());
                        }
                    }

                    if filename.starts_with(INDEX_FILE_SEGMENTS)
                        && filename != INDEX_FILE_OLD_SEGMENT_GEN
                    {
                        // This is a commit (segments or segments_N), and
                        // it's valid (<= the max gen).  Load it, then
                        // incref all files it refers to:
                        let sis = SegmentInfos::read_commit(&self.directory_orig, filename)?;
                        let commit_point = CommitPoint::new(
                            &mut self.commits_to_delete,
                            Arc::clone(&self.directory_orig),
                            &sis,
                        );
                        self.commits.push(commit_point);
                        if sis.generation == segment_infos.generation {
                            current_commit_point_idx = Some(self.commits.len() - 1);
                        }
                        self.inc_ref_by_segment(&sis, true);

                        if self.last_segment_infos.is_none()
                            || sis.generation > self.last_segment_infos.as_ref().unwrap().generation
                        {
                            self.last_segment_infos = Some(sis);
                        }
                    }
                }
            }
        }

        if let Some(ref current_segments_file) = current_segments_file {
            if current_commit_point_idx.is_none() && initial_index_exists {
                // We did not in fact see the segments_N file
                // corresponding to the segmentInfos that was passed
                // in.  Yet, it must exist, because our caller holds
                // the write lock.  This can happen when the directory
                // listing was stale (eg when index accessed via NFS
                // client with stale directory listing cache).  So we
                // try now to explicitly open this commit point:
                let sis = SegmentInfos::read_commit(&self.directory_orig, current_segments_file)?;
                let commit_point = CommitPoint::new(
                    &mut self.commits_to_delete,
                    Arc::clone(&self.directory_orig),
                    &sis,
                );
                self.commits.push(commit_point);
                current_commit_point_idx = Some(self.commits.len() - 1);
                self.inc_ref_by_segment(&sis, true);
            }
        }

        if is_reader_init {
            // Incoming SegmentInfos may have NRT changes not yet visible
            // in the latest commit, so we have to protect its files from deletion too:
            self.checkpoint(segment_infos, false)?;
        }

        // We keep commits list in sorted order (oldest to newest):
        self.commits.sort();

        // refCounts only includes "normal" filenames (does not include write.lock)
        {
            let ref_counts = self.ref_counts.read()?;
            let files: Vec<&str> = ref_counts.keys().map(|s| s.as_str()).collect();
            Self::inflate_gens(segment_infos, files)?;
        }

        // Now delete anything with ref count at 0.  These are
        // presumably abandoned files eg due to crash of
        // IndexWriter.
        {
            let mut to_delete = HashSet::new();
            for (filename, rc) in &*self.ref_counts.read()? {
                if rc.count == 0 {
                    // A segments_N file should never have ref count 0 on init
                    if filename.starts_with(INDEX_FILE_SEGMENTS) {
                        bail!(ErrorKind::IllegalState(format!(
                            "file '{}' has ref_count=0, shouldn't happen on init",
                            filename
                        )));
                    }
                    to_delete.insert(filename.clone());
                }
            }
            self.delete_files(&to_delete)?;
        }

        // Finally, give policy a chance to remove things on
        // startup:
        {
            let mut commits: Vec<&mut IndexCommit> = Vec::with_capacity(self.commits.len());
            for i in &mut self.commits {
                commits.push(i);
            }
            self.policy.on_init(commits)?;
        }

        // Always protect the incoming segmentInfos since
        // sometime it may not be the most recent commit
        self.checkpoint(segment_infos, false)?;

        self.starting_commit_deleted =
            current_commit_point_idx.map_or(false, |idx| self.commits[idx].deleted);

        self.delete_commits()?;
        self.inited = true;
        Ok(())
    }

    /// Set all gens beyond what we currently see in the directory, to avoid double-write
    /// in cases where the previous IndexWriter did not gracefully close/rollback (e.g.
    /// os/machine crashed or lost power).
    fn inflate_gens(infos: &mut SegmentInfos, files: Vec<&str>) -> Result<()> {
        let mut max_segment_gen = i64::min_value();
        let mut max_segment_name = i32::min_value();

        // Confusingly, this is the union of live_docs, field infos, doc values
        // (and maybe others, in the future) gens.  This is somewhat messy,
        // since it means DV updates will suddenly write to the next gen after
        // live docs' gen, for example, but we don't have the APIs to ask the
        // codec which file is which:
        let mut max_per_segment_gen = HashMap::new();

        for filename in files {
            if filename == INDEX_FILE_OLD_SEGMENT_GEN || filename == INDEX_WRITE_LOCK_NAME {
                // do nothing
            } else if filename.starts_with(INDEX_FILE_SEGMENTS) {
                // trash file: we have to handle this since we allow anything
                // starting with 'segments' here
                if let Ok(gen) = SegmentInfos::generation_from_segments_file_name(filename) {
                    max_segment_gen = max(gen, max_segment_gen);
                }
            } else if filename.starts_with(INDEX_FILE_PENDING_SEGMENTS) {
                // the first 8 bytes is "pending_", so the slice operation is safe
                if let Ok(gen) = SegmentInfos::generation_from_segments_file_name(&filename[8..]) {
                    max_segment_gen = max(gen, max_segment_gen);
                }
            } else {
                let segment_name = parse_segment_name(filename);
                debug_assert!(segment_name.starts_with("_"));

                if filename.to_lowercase().ends_with(".tmp") {
                    // A temp file: don't try to look at its gen
                    continue;
                }

                max_segment_name = max(
                    max_segment_name,
                    i32::from_str_radix(&segment_name[1..], 36)?,
                );

                let mut cur_gen = max_per_segment_gen.get(segment_name).map_or(0, |x| *x);
                if let Ok(gen) = parse_generation(filename) {
                    cur_gen = max(cur_gen, gen);
                }
                max_per_segment_gen.insert(segment_name.to_string(), cur_gen);
            }
        }

        // Generation is advanced before write:
        let next_write_gen = max(infos.generation, max_segment_gen);
        infos.set_next_write_generation(next_write_gen)?;
        if infos.counter < max_segment_name + 1 {
            infos.counter = max_segment_name
        }

        for info in &mut infos.segments {
            let gen = max_per_segment_gen[&info.info.name];

            if info.next_write_del_gen() < gen + 1 {
                info.set_next_write_del_gen(gen + 1);
            }
            if info.next_write_field_infos_gen() < gen + 1 {
                info.set_next_write_field_infos_gen(gen + 1);
            }
            if info.next_write_doc_values_gen() < gen + 1 {
                info.set_next_write_doc_values_gen(gen + 1);
            }
        }
        Ok(())
    }

    /// For definition of "check point" see IndexWriter comments:
    /// "Clarification: Check Points (and commits)".
    ///
    /// Writer calls this when it has made a "consistent
    /// change" to the index, meaning new files are written to
    /// the index and the in-memory SegmentInfos have been
    /// modified to point to those files.
    ///
    /// This may or may not be a commit (segments_N may or may
    /// not have been written).
    ///
    /// We simply incref the files referenced by the new
    /// SegmentInfos and decref the files we had previously
    /// seen (if any).
    ///
    /// If this is a commit, we also call the policy to give it
    /// a chance to remove other commits.  If any commits are
    /// removed, we decref their files as well.
    pub fn checkpoint(&mut self, segment_infos: &SegmentInfos, is_commit: bool) -> Result<()> {
        // incref the files:
        self.inc_ref_by_segment(segment_infos, is_commit);

        if is_commit {
            // Append to our commits list:
            let p = CommitPoint::new(
                &mut self.commits_to_delete,
                Arc::clone(&self.directory_orig),
                segment_infos,
            );
            self.commits.push(p);

            // Tell policy so it can remove commits:
            {
                let mut commits: Vec<&mut IndexCommit> = Vec::with_capacity(self.commits.len());
                for i in &mut self.commits {
                    i.commits_to_delete = &mut self.commits_to_delete;
                    commits.push(i);
                }
                self.policy.on_commit(commits)?;
            }

            // DecRef file for commits that were deleted by the policy
            self.delete_commits()
        } else {
            let res = self.dec_ref_batch((&self.last_files).iter());
            self.last_files.clear();
            res?;
            self.last_files.extend(segment_infos.files(false));
            Ok(())
        }
    }

    pub fn exists(&self, filename: &str) -> bool {
        if !self.ref_counts.read().unwrap().contains_key(filename) {
            false
        } else {
            self.ensure_ref_count(filename);
            self.ref_counts.read().unwrap()[filename].count > 0
        }
    }

    fn ensure_ref_count(&self, file_name: &str) {
        let mut ref_counts = self.ref_counts.write().unwrap();
        if !ref_counts.contains_key(file_name) {
            ref_counts.insert(file_name.to_string(), RefCount::default());
        }
    }

    pub fn inc_ref_by_segment(&self, segment_infos: &SegmentInfos, is_commit: bool) {
        // If this is a commit point, also incRef the
        // segments_N file:
        for filename in segment_infos.files(is_commit) {
            self.inc_ref(&filename)
        }
    }

    pub fn inc_ref_files(&self, files: &HashSet<String>) {
        for f in files {
            self.inc_ref(f);
        }
    }

    fn inc_ref(&self, filename: &str) {
        self.ensure_ref_count(filename);
        self.ref_counts
            .write()
            .unwrap()
            .get_mut(filename)
            .unwrap()
            .inc_ref();
    }

    pub fn dec_ref_by_segment(&self, segment_infos: &SegmentInfos) -> Result<()> {
        self.dec_ref_batch((&segment_infos.files(false)).into_iter())
    }

    /// Decrefs all provided files, even on exception; throws first exception hit, if any.
    pub fn dec_ref_batch<'a, I, T>(&self, files: T) -> Result<()>
    where
        I: Iterator<Item = &'a String>,
        T: IntoIterator<Item = &'a String, IntoIter = I>,
    {
        let mut to_delete = HashSet::new();
        for f in files {
            if self.dec_ref(f) {
                to_delete.insert(f);
            }
        }
        self.delete_files(to_delete)
    }

    pub fn dec_ref_without_error(&self, files: &HashSet<String>) {
        if let Err(e) = self.dec_ref_batch(files.into_iter()) {
            warn!("dec_ref_without_error failed with '{:?}'", e);
        }
    }

    /// Returns true if the file should now be deleted.
    fn dec_ref(&self, filename: &str) -> bool {
        self.ensure_ref_count(filename);
        if self
            .ref_counts
            .write()
            .unwrap()
            .get_mut(filename)
            .unwrap()
            .dec_ref()
            == 0
        {
            // This file is no longer referenced by any past
            // commit points nor by the in-memory SegmentInfos:
            self.ref_counts.write().unwrap().remove(filename);
            true
        } else {
            false
        }
    }

    /// Remove the CommitPoints in the commitsToDelete List by
    /// DecRef'ing all files from each SegmentInfos.
    fn delete_commits(&mut self) -> Result<()> {
        let size = self.commits_to_delete.len();

        let mut res = Ok(());
        if size > 0 {
            // First decref all files that had been referred to by
            // the now-deleted commits:
            for commit in &self.commits_to_delete {
                let r = self.dec_ref_batch(&commit.files);
                if r.is_err() {
                    res = r;
                }
            }
            self.commits_to_delete.clear();

            // NOTE: does nothing if not err
            if res.is_err() {
                return res;
            }

            // Now compact commits to remove deleted ones (preserving the sort):
            let size = self.commits.len();
            let mut read_from = 0;
            let mut write_to = 0;
            while read_from < size {
                if !self.commits[read_from].deleted {
                    if write_to != read_from {
                        self.commits.swap(read_from, write_to);
                    }
                    write_to += 1;
                }
                read_from += 1;
            }
            self.commits.truncate(write_to);
        }
        Ok(())
    }

    fn delete_files<'a, I, T>(&self, names: T) -> Result<()>
    where
        I: Iterator<Item = &'a String>,
        T: IntoIterator<Item = &'a String, IntoIter = I>,
    {
        // self.ensure_open()?;

        // We make two passes, first deleting any segments_N files, second
        // deleting the rest.  We do this so that if we throw exc or JVM
        // crashes during deletions, even when not on Windows, we don't
        // leave the index in an "apparently corrupt" state:
        let mut copys = vec![];
        for name in names {
            copys.push(name);
            if !name.starts_with(INDEX_FILE_SEGMENTS) {
                continue;
            }
            self.delete_file(name)?;
        }

        for name in copys {
            if name.starts_with(INDEX_FILE_SEGMENTS) {
                continue;
            }
            self.delete_file(name)?;
        }
        Ok(())
    }

    fn delete_file(&self, filename: &str) -> Result<()> {
        // panic!("wrong deleted files");
        self.directory.delete_file(filename)
    }

    /// Deletes the specified files, but only if they are new
    /// (have not yes been incref'd).
    pub fn delete_new_files<'a, I, T>(&self, files: T) -> Result<()>
    where
        I: Iterator<Item = &'a String>,
        T: IntoIterator<Item = &'a String, IntoIter = I>,
    {
        // NOTE: it's very unusual yet possible for the
        // refCount to be present and 0: it can happen if you
        // open IW on a crashed index, and it removes a bunch
        // of unref'd files, and then you add new docs / do
        // merging, and it reuses that segment name.
        // TestCrash.testCrashAfterReopen can hit this:
        let filtered = files.into_iter().filter(|f: &&String| {
            !self.ref_counts.read().unwrap().contains_key(*f)
                || self.ref_counts.read().unwrap()[*f].count == 0
        });

        self.delete_files(filtered)
    }

    fn ensure_open(&self) -> Result<()> {
        // TODO
        Ok(())
    }

    /// Writer calls this when it has hit an error and had to
    /// roll back, to tell us that there may now be
    /// unreferenced files in the filesystem.  So we re-list
    /// the filesystem and delete such files.  If segmentName
    /// is non-null, we will only delete files corresponding to
    /// that segment.
    pub fn refresh(&mut self) -> Result<()> {
        debug_assert!(self.inited);

        let files = self.directory.list_all()?;
        let mut to_delete = HashSet::new();
        let pattern = Regex::new(CODEC_FILE_PATTERN).unwrap();
        for filename in &files {
            if filename.ends_with("write.lock")
                && !self.ref_counts.read()?.contains_key(filename)
                && (pattern.is_match(filename) || filename.starts_with(INDEX_FILE_SEGMENTS) ||
                // we only try to clear out pending_segments_N during rollback(), because we don't ref-count it
                // TODO: this is sneaky, should we do this, or change TestIWExceptions? rollback closes anyway, and
                // any leftover file will be deleted/retried on next IW bootup anyway...
                filename.starts_with(INDEX_FILE_PENDING_SEGMENTS))
            {
                // Unreferenced file, so remove it
                to_delete.insert(filename);
            }
        }

        self.delete_files(to_delete)
    }

    pub fn close(&mut self) -> Result<()> {
        if !self.last_files.is_empty() {
            let files = mem::replace(&mut self.last_files, Vec::with_capacity(0));
            self.dec_ref_batch((&files).iter())?;
        }
        Ok(())
    }
}

struct RefCount {
    inited: bool,
    count: u32,
}

impl Default for RefCount {
    fn default() -> Self {
        RefCount {
            inited: false,
            count: 0,
        }
    }
}

impl RefCount {
    fn inc_ref(&mut self) -> u32 {
        if !self.inited {
            self.inited = true;
        } else {
            debug_assert!(self.count > 0);
        }
        self.count += 1;
        self.count
    }

    fn dec_ref(&mut self) -> u32 {
        debug_assert!(self.count > 0);
        self.count -= 1;
        self.count
    }
}

/// Holds details for each commit point. This class is also passed to
/// the deletion policy. Note: this class has a natural ordering that
/// is inconsistent with equals.
#[derive(Clone)]
struct CommitPoint {
    files: HashSet<String>,
    segments_file_name: String,
    deleted: bool,
    directory_orig: DirectoryRc,
    // refer the commit_to_delete in IndexFileDeleter
    commits_to_delete: *mut Vec<CommitPoint>,
    generation: i64,
    user_data: HashMap<String, String>,
    segment_count: usize,
}

impl CommitPoint {
    fn new(
        commits_to_delete: *mut Vec<CommitPoint>,
        directory_orig: DirectoryRc,
        segment_infos: &SegmentInfos,
    ) -> Self {
        CommitPoint {
            files: segment_infos.files(true),
            segments_file_name: segment_infos.segment_file_name().unwrap(),
            deleted: false,
            directory_orig,
            commits_to_delete,
            generation: segment_infos.generation,
            user_data: HashMap::new(),
            segment_count: segment_infos.len(),
        }
    }
}

impl IndexCommit for CommitPoint {
    fn segments_file_name(&self) -> &str {
        &self.segments_file_name
    }

    fn file_names(&self) -> Result<&HashSet<String>> {
        Ok(&self.files)
    }

    fn directory(&self) -> &Directory {
        self.directory_orig.as_ref()
    }

    fn delete(&mut self) -> Result<()> {
        if !self.deleted {
            self.deleted = true;
            let commit_point = self.clone();
            unsafe {
                (*self.commits_to_delete).push(commit_point);
            }
        }
        Ok(())
    }

    fn is_deleted(&self) -> bool {
        self.deleted
    }

    fn segment_count(&self) -> usize {
        self.segment_count
    }

    fn generation(&self) -> i64 {
        self.generation
    }

    fn user_data(&self) -> &HashMap<String, String> {
        &self.user_data
    }
}

impl Ord for CommitPoint {
    fn cmp(&self, other: &Self) -> Ordering {
        debug_assert!(ptr::eq(
            self.directory_orig.as_ref(),
            other.directory_orig.as_ref()
        ));

        self.generation.cmp(&other.generation)
    }
}

impl PartialOrd for CommitPoint {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for CommitPoint {}

impl PartialEq for CommitPoint {
    fn eq(&self, other: &Self) -> bool {
        ptr::eq(self.directory_orig.as_ref(), other.directory_orig.as_ref())
            && self.generation == other.generation
    }
}
