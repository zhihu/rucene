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

use core::codec::segment_infos::{
    generation_from_segments_file_name, parse_generation, parse_segment_name, SegmentInfos,
    CODEC_FILE_PATTERN, CODEC_UPDATE_DV_PATTERN, CODEC_UPDATE_FNM_PATTERN,
    INDEX_FILE_OLD_SEGMENT_GEN, INDEX_FILE_PENDING_SEGMENTS, INDEX_FILE_SEGMENTS,
};
use core::codec::Codec;
use core::index::writer::KeepOnlyLastCommitDeletionPolicy;
use core::store::directory::{Directory, LockValidatingDirectoryWrapper};

use regex::Regex;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::mem;
use std::sync::{Arc, Mutex, RwLock};

use error::{ErrorKind, Result};
use std::time::{SystemTime, UNIX_EPOCH};

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
pub struct IndexFileDeleter<D: Directory> {
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
    last_files: HashSet<String>,
    policy: KeepOnlyLastCommitDeletionPolicy,

    delayed_dv_update_files: Arc<Mutex<Vec<(u64, Vec<String>)>>>,
    dv_pattern: Regex,
    fnm_pattern: Regex,

    directory: Arc<LockValidatingDirectoryWrapper<D>>,
    inited: bool,
}

impl<D: Directory> IndexFileDeleter<D> {
    pub fn new(directory: Arc<LockValidatingDirectoryWrapper<D>>) -> Self {
        IndexFileDeleter {
            ref_counts: Arc::new(RwLock::new(HashMap::new())),
            commits: vec![],
            last_files: HashSet::new(),
            policy: KeepOnlyLastCommitDeletionPolicy {},
            delayed_dv_update_files: Arc::new(Mutex::new(Vec::new())),
            dv_pattern: Regex::new(CODEC_UPDATE_DV_PATTERN).unwrap(),
            fnm_pattern: Regex::new(CODEC_UPDATE_FNM_PATTERN).unwrap(),
            directory,
            inited: false,
        }
    }

    pub fn init<C: Codec>(
        &mut self,
        directory_orig: Arc<D>,
        files: &[String],
        segment_infos: &mut SegmentInfos<D, C>,
        initial_index_exists: bool,
    ) -> Result<bool> {
        let mut current_commit_point_idx: Option<usize> = None;
        if let Some(ref current_segments_file) = segment_infos.segment_file_name() {
            let pattern = Regex::new(CODEC_FILE_PATTERN).unwrap();
            for filename in files {
                if pattern.is_match(filename)
                    || filename.starts_with(INDEX_FILE_SEGMENTS)
                    || filename.starts_with(INDEX_FILE_PENDING_SEGMENTS)
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
                        let sis: SegmentInfos<D, C> =
                            SegmentInfos::read_commit(&directory_orig, filename)?;
                        let commit_point = CommitPoint::new(
                            sis.generation,
                            sis.segment_file_name().unwrap_or("".to_string()),
                            sis.files(true),
                            sis.has_dv_updates(),
                        );
                        self.commits.push(commit_point);
                        if sis.generation == segment_infos.generation {
                            current_commit_point_idx = Some(self.commits.len() - 1);
                        }
                        self.inc_ref_files(&sis.files(true));
                    }
                }
            }

            if current_commit_point_idx.is_none() && initial_index_exists {
                // We did not in fact see the segments_N file
                // corresponding to the segmentInfos that was passed
                // in.  Yet, it must exist, because our caller holds
                // the write lock.  This can happen when the directory
                // listing was stale (eg when index accessed via NFS
                // client with stale directory listing cache).  So we
                // try now to explicitly open this commit point:
                let sis: SegmentInfos<D, C> =
                    SegmentInfos::read_commit(&directory_orig, current_segments_file)?;
                let commit_point = CommitPoint::new(
                    sis.generation,
                    sis.segment_file_name().unwrap_or("".to_string()),
                    sis.files(true),
                    sis.has_dv_updates(),
                );
                self.commits.push(commit_point);
                current_commit_point_idx = Some(self.commits.len() - 1);
                self.inc_ref_files(&sis.files(true));
            }
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
            self.delete_files(&to_delete, false)?;
        }

        // Finally, give policy a chance to remove things on
        // startup:
        {
            let mut commits: Vec<&mut CommitPoint> = Vec::with_capacity(self.commits.len());
            for i in &mut self.commits {
                commits.push(i);
            }
            self.policy.on_init(commits)?;
        }

        // Always protect the incoming segmentInfos since
        // sometime it may not be the most recent commit
        self.checkpoint(segment_infos, false)?;

        let mut starting_commit_deleted = false;
        if let Some(idx) = current_commit_point_idx {
            if self.commits[idx].deleted {
                starting_commit_deleted = true;
            }
        }

        self.delete_commits()?;
        self.inited = true;

        Ok(starting_commit_deleted)
    }

    /// Set all gens beyond what we currently see in the directory, to avoid double-write
    /// in cases where the previous IndexWriter did not gracefully close/rollback (e.g.
    /// os/machine crashed or lost power).
    fn inflate_gens<C: Codec>(infos: &mut SegmentInfos<D, C>, files: Vec<&str>) -> Result<()> {
        let mut max_segment_gen = i64::min_value();
        let mut max_segment_name = i32::min_value();

        // Confusingly, this is the union of live_docs, field infos, doc values
        // (and maybe others, in the future) gens.  This is somewhat messy,
        // since it means DV updates will suddenly write to the next gen after
        // live docs' gen, for example, but we don't have the APIs to ask the
        // codec which file is which:
        let mut max_per_segment_gen = HashMap::new();

        for filename in files {
            if filename == INDEX_FILE_OLD_SEGMENT_GEN {
                // do nothing
            } else if filename.starts_with(INDEX_FILE_SEGMENTS) {
                // trash file: we have to handle this since we allow anything
                // starting with 'segments' here
                if let Ok(gen) = generation_from_segments_file_name(filename) {
                    max_segment_gen = max_segment_gen.max(gen);
                }
            } else if filename.starts_with(INDEX_FILE_PENDING_SEGMENTS) {
                // the first 8 bytes is "pending_", so the slice operation is safe
                if let Ok(gen) = generation_from_segments_file_name(&filename[8..]) {
                    max_segment_gen = max_segment_gen.max(gen);
                }
            } else {
                let segment_name = parse_segment_name(filename);
                debug_assert!(segment_name.starts_with('_'));

                if filename.to_lowercase().ends_with(".tmp") {
                    // A temp file: don't try to look at its gen
                    continue;
                }

                max_segment_name =
                    max_segment_name.max(i32::from_str_radix(&segment_name[1..], 36)?);

                let mut cur_gen = max_per_segment_gen.get(segment_name).map_or(0, |x| *x);
                if let Ok(gen) = parse_generation(filename) {
                    cur_gen = cur_gen.max(gen);
                }
                max_per_segment_gen.insert(segment_name.to_string(), cur_gen);
            }
        }

        // Generation is advanced before write:
        let next_write_gen = max_segment_gen.max(infos.generation);
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
    pub fn checkpoint<C: Codec>(
        &mut self,
        segment_infos: &SegmentInfos<D, C>,
        is_commit: bool,
    ) -> Result<()> {
        // incref the files:
        self.inc_ref_files(&segment_infos.files(is_commit));

        if is_commit {
            // Append to our commits list:
            let p = CommitPoint::new(
                segment_infos.generation,
                segment_infos.segment_file_name().unwrap_or("".to_string()),
                segment_infos.files(true),
                segment_infos.has_dv_updates(),
            );
            self.commits.push(p);

            // Tell policy so it can remove commits:
            {
                let mut commits: Vec<&mut CommitPoint> = Vec::with_capacity(self.commits.len());
                for i in &mut self.commits {
                    commits.push(i);
                }
                self.policy.on_commit(commits)?;
            }

            // DecRef file for commits that were deleted by the policy
            self.delete_commits()
        } else {
            let res = self.dec_ref_files(&self.last_files);
            self.last_files.clear();
            res?;
            // Save files so we can decr on next checkpoint/commit:
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

    pub fn inc_ref_files(&self, files: &HashSet<String>) {
        for file in files {
            self.ensure_ref_count(file);
            self.ref_counts
                .write()
                .unwrap()
                .get_mut(file)
                .unwrap()
                .inc_ref();
        }
    }

    /// Decrefs all provided files, even on exception; throws first exception hit, if any.
    pub fn dec_ref_files(&self, files: &HashSet<String>) -> Result<()> {
        let mut to_delete = HashSet::new();
        for f in files {
            if self.dec_ref(f) {
                to_delete.insert(f.clone());
            }
        }
        self.delete_files(&to_delete, false)
    }

    fn _dec_ref_files_by_commit(&self, files: &HashSet<String>) -> Result<()> {
        let mut to_delete = HashSet::new();
        for f in files {
            if self.dec_ref(f) {
                to_delete.insert(f.clone());
            }
        }
        self.delete_files(&to_delete, true)
    }

    pub fn dec_ref_files_no_error(&self, files: &HashSet<String>) {
        if let Err(e) = self.dec_ref_files(files) {
            warn!("dec_ref_files_no_error failed with '{:?}'", e);
        }
    }

    /// Returns true if the file should now be deleted.
    fn dec_ref(&self, filename: &str) -> bool {
        self.ensure_ref_count(filename);
        let mut ref_counts = self.ref_counts.write().unwrap();
        if ref_counts.get_mut(filename).unwrap().dec_ref() == 0 {
            // This file is no longer referenced by any past
            // commit points nor by the in-memory SegmentInfos:
            ref_counts.remove(filename);
            true
        } else {
            false
        }
    }

    /// Remove the CommitPoints in the commitsToDelete List by
    /// DecRef'ing all files from each SegmentInfos.
    fn delete_commits(&mut self) -> Result<()> {
        let mut res = Ok(());
        // First decref all files that had been referred to by
        // the now-deleted commits:
        for commit in &self.commits {
            if commit.deleted {
                res = self.dec_ref_files(&commit.files);
            }
        }

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

        Ok(())
    }

    fn delete_files(&self, files: &HashSet<String>, do_commit_filter: bool) -> Result<()> {
        // We make two passes, first deleting any segments_N files, second
        // deleting the rest.  We do this so that if we throw exc or JVM
        // crashes during deletions, even when not on Windows, we don't
        // leave the index in an "apparently corrupt" state:
        let mut copys = vec![];
        for file in files {
            copys.push(file);
            if !file.starts_with(INDEX_FILE_SEGMENTS) {
                continue;
            }
            self.delete_file(file)?;
        }

        if do_commit_filter {
            self.filter_dv_update_files(&mut copys);
        }

        for file in copys {
            if file.starts_with(INDEX_FILE_SEGMENTS) {
                continue;
            }
            self.delete_file(file)?;
        }
        Ok(())
    }

    fn filter_dv_update_files(&self, candidates: &mut Vec<&String>) {
        let dv_update_files: Vec<String> = candidates
            .drain_filter(|f| -> bool {
                self.fnm_pattern.is_match(f) || self.dv_pattern.is_match(f)
            })
            .map(|f| f.clone())
            .collect();
        let to_deletes: Vec<Vec<String>>;
        {
            let mut l = self.delayed_dv_update_files.lock();
            let old_dv_update_files = l.as_mut().unwrap();
            let tm_now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
            to_deletes = old_dv_update_files
                .drain_filter(|(x, _)| -> bool { *x < tm_now })
                .map(|(_, y)| y)
                .collect();
            old_dv_update_files.push((tm_now + 60, dv_update_files));
        }
        for files in to_deletes {
            for file in files {
                self.delete_file(&file).unwrap_or(());
            }
        }
    }

    fn delete_file(&self, filename: &str) -> Result<()> {
        // panic!("wrong deleted files");
        self.directory.delete_file(filename)
    }

    /// Deletes the specified files, but only if they are new
    /// (have not yes been incref'd).
    pub fn delete_new_files(&self, files: &HashSet<String>) -> Result<()> {
        let mut filtered = HashSet::with_capacity(files.len());
        let ref_counts = self.ref_counts.read().unwrap();
        for file in files {
            // NOTE: it's very unusual yet possible for the
            // refCount to be present and 0: it can happen if you
            // open IW on a crashed index, and it removes a bunch
            // of unref'd files, and then you add new docs / do
            // merging, and it reuses that segment name.
            // TestCrash.testCrashAfterReopen can hit this:
            if !ref_counts.contains_key(file) || ref_counts[file].count == 0 {
                filtered.insert(file.clone());
            }
        }

        self.delete_files(&filtered, false)
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
            if !self.ref_counts.read()?.contains_key(filename)
                && (pattern.is_match(filename)
                    || filename.starts_with(INDEX_FILE_SEGMENTS)
                    || filename.starts_with(INDEX_FILE_PENDING_SEGMENTS))
            {
                // Unreferenced file, so remove it
                to_delete.insert(filename.clone());
            }
        }

        self.delete_files(&to_delete, false)
    }

    pub fn close(&mut self) -> Result<()> {
        if !self.last_files.is_empty() {
            let files = mem::replace(&mut self.last_files, HashSet::new());
            self.dec_ref_files(&files)?;
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

impl fmt::Display for RefCount {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.count)
    }
}

impl fmt::Debug for RefCount {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.count)
    }
}

/// Expert: represents a single commit into an index as seen by the
/// {@link IndexDeletionPolicy} or {@link IndexReader}.
///
/// Changes to the content of an index are made visible
/// only after the writer who made that change commits by
/// writing a new segments file
/// (`segments_N</code`). This point in time, when the
/// action of writing of a new segments file to the directory
/// is completed, is an index commit.
///
/// Each index commit point has a unique segments file
/// associated with it. The segments file associated with a
/// later index commit point would have a larger N.
///
/// Holds details for each commit point. This class is also passed to
/// the deletion policy. Note: this class has a natural ordering that
/// is inconsistent with equals.
pub struct CommitPoint {
    generation: i64,
    segment_file_name: String,
    files: HashSet<String>,
    has_dv_updates: bool,
    deleted: bool,
}

impl CommitPoint {
    fn new(
        generation: i64,
        segment_file_name: String,
        files: HashSet<String>,
        has_dv_updates: bool,
    ) -> Self {
        CommitPoint {
            generation,
            segment_file_name,
            files,
            has_dv_updates,
            deleted: false,
        }
    }

    /// Get the segments file (`segments_N`) associated with this commit point
    pub fn segments_file_name(&self) -> &str {
        &self.segment_file_name
    }

    /// Delete this commit point.  This only applies when using
    /// the commit point in the context of IndexWriter's
    /// IndexDeletionPolicy.
    ///
    /// Upon calling this, the writer is notified that this commit
    /// point should be deleted.
    ///
    /// Decision that a commit-point should be deleted is taken by the
    /// `IndexDeletionPolicy` in effect and therefore this should only
    /// be called by its `IndexDeletionPolicy#onInit on_init()` or
    /// `IndexDeletionPolicy#onCommit on_commit()` methods.
    pub fn delete(&mut self) -> Result<()> {
        self.deleted = true;
        Ok(())
    }

    pub fn has_dv_updates(&self) -> bool {
        self.has_dv_updates
    }
}

impl Ord for CommitPoint {
    fn cmp(&self, other: &Self) -> Ordering {
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
        self.segment_file_name == other.segment_file_name && self.generation == other.generation
    }
}
