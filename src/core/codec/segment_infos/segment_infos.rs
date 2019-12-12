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

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use core::codec::segment_infos::{
    file_name_from_generation, SegmentCommitInfo, SegmentInfoFormat, INDEX_FILE_OLD_SEGMENT_GEN,
    INDEX_FILE_PENDING_SEGMENTS, INDEX_FILE_SEGMENTS,
};
use core::codec::Codec;
use core::codec::CODEC_MAGIC;
use core::codec::{
    check_checksum, check_header_no_magic, check_index_header_suffix, validate_footer,
    write_footer, write_index_header,
};
use core::index::merge::OneMerge;
use core::index::writer::CommitPoint;
use core::store::directory::Directory;
use core::store::io::{BufferedChecksumIndexInput, ChecksumIndexInput, IndexInput, IndexOutput};
use core::store::IOContext;
use core::util::{random_id, ID_LENGTH};
use core::util::{to_base36, Version, VERSION_LATEST};
use error::ErrorKind::{IllegalState, NumError};
use error::Result;

/// The file format version for the segments_N codec header, since 5.0+
const SEGMENT_VERSION_50: i32 = 4;
/// The file format version for the segments_N codec header, since 5.1+
#[allow(dead_code)]
const SEGMENT_VERSION_51: i32 = 5;
/// Adds the {@link Version} that committed this segments_N file, as well as the {@link Version} of
/// the oldest segment, since 5.3+
const SEGMENT_VERSION_53: i32 = 6;

const SEGMENT_VERSION_CURRENT: i32 = SEGMENT_VERSION_53;

/// A collection of segmentInfo objects with methods for operating on those
/// segments in relation to the file system.
///
/// The active segments in the index are stored in the segment info file,
/// `segments_N`. There may be one or more `segments_N` files in
/// the index; however, the one with the largest generation is the active one
/// (when older segments_N files are present it's because they temporarily cannot
/// be deleted, or a custom {@link IndexDeletionPolicy} is in
/// use). This file lists each segment by name and has details about the codec
/// and generation of deletes.
#[derive(Debug)]
pub struct SegmentInfos<D: Directory, C: Codec> {
    /// Used to name new segments.
    pub counter: i32,
    /// Counts how often the index has been changed.
    pub version: i64,
    // generation of the "segments_N" for the next commit
    pub generation: i64,
    // generation of the "segments_N" file we last successfully read or wrote; this is normally
    // the same as generation except if there was an IOException that had interrupted a commit
    pub last_generation: i64,
    pub segments: Vec<Arc<SegmentCommitInfo<D, C>>>,
    /// Id for this commit; only written starting with Lucene 5.0
    pub id: [u8; ID_LENGTH],
    /// Which Lucene version wrote this commit, or None if this commit is pre-5.3.
    pub lucene_version: Option<Version>,
    /// Version of the oldest segment in the index, or null if there are no segments.
    pub min_seg_version: Option<Version>,
    // Only true after prepareCommit has been called and
    // before finishCommit is called
    pending_commit: bool,
}

impl<D: Directory, C: Codec> Default for SegmentInfos<D, C> {
    fn default() -> Self {
        SegmentInfos {
            counter: 0,
            version: 0,
            generation: 0,
            last_generation: 0,
            segments: vec![],
            id: [0u8; ID_LENGTH],
            lucene_version: None,
            min_seg_version: None,
            pending_commit: false,
        }
    }
}

#[allow(clippy::len_without_is_empty)]
impl<D: Directory, C: Codec> SegmentInfos<D, C> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        counter: i32,
        version: i64,
        generation: i64,
        last_generation: i64,
        segments: Vec<Arc<SegmentCommitInfo<D, C>>>,
        id: [u8; ID_LENGTH],
        lucene_version: Option<Version>,
        min_seg_version: Option<Version>,
    ) -> SegmentInfos<D, C> {
        SegmentInfos {
            counter,
            version,
            generation,
            last_generation,
            segments,
            id,
            lucene_version,
            min_seg_version,
            pending_commit: false,
        }
    }

    /// return generation of the next pending_segments_N that will be written
    fn next_pending_generation(&self) -> u64 {
        if self.generation == -1 {
            1
        } else {
            self.generation as u64 + 1
        }
    }

    pub fn update_generation(&mut self, last_gen: i64, gen: i64) {
        self.last_generation = last_gen;
        self.generation = gen;
    }

    pub fn has_dv_updates(&self) -> bool {
        let mut has_dv_updates = false;
        for si in &self.segments {
            if si.has_field_updates() {
                has_dv_updates = true;
                break;
            }
        }
        has_dv_updates
    }

    /// Get the segments_N filename in use by this segment infos.
    pub fn segment_file_name(&self) -> Option<String> {
        if self.last_generation < 0 {
            None
        } else {
            Some(file_name_from_generation(
                INDEX_FILE_SEGMENTS,
                "",
                self.last_generation as u64,
            ))
        }
    }

    /// Returns number of segments
    pub fn len(&self) -> usize {
        self.segments.len()
    }

    pub fn rollback_commit<DW: Directory>(&mut self, dir: &DW) {
        if self.pending_commit {
            self.pending_commit = false;

            // we try to clean up our pending_segments_N

            // Must carefully compute fileName from "generation"
            // since lastGeneration isn't incremented:
            let pending =
                file_name_from_generation(INDEX_FILE_PENDING_SEGMENTS, "", self.generation as u64);

            // Suppress so we keep throwing the original error in our caller
            if let Err(e) = dir.delete_file(&pending) {
                warn!(
                    "SegmentInfos: rollback_commit delete file '{}' failed by '{:?}'",
                    &pending, e
                );
            }
        }
    }

    /// Call this to start a commit.  This writes the new
    /// segments file, but writes an invalid checksum at the
    /// end, so that it is not visible to readers.  Once this
    /// is called you must call {@link #finishCommit} to complete
    /// the commit or {@link #rollbackCommit} to abort it.
    ///
    /// Note: {@link #changed()} should be called prior to this
    /// method if changes have been made to this {@link SegmentInfos} instance
    pub fn prepare_commit<DW: Directory>(&mut self, dir: &DW) -> Result<()> {
        if self.pending_commit {
            bail!(IllegalState("prepare_commit was already called".into()));
        }
        self.write_dir(dir)
    }

    fn write_dir<DW: Directory>(&mut self, directory: &DW) -> Result<()> {
        let next_generation = self.next_pending_generation();
        let segment_file_name =
            file_name_from_generation(INDEX_FILE_PENDING_SEGMENTS, "", next_generation);
        // Always advance the generation on write:
        self.generation = next_generation as i64;

        match self.do_write_dir(directory, segment_file_name.clone()) {
            Ok(()) => {
                self.pending_commit = true;
                Ok(())
            }
            Err(e) => {
                if let Err(err) = directory.delete_file(&segment_file_name) {
                    warn!(
                        "delete file '{}' failed by: '{:?}'",
                        &segment_file_name, err
                    );
                }
                Err(e)
            }
        }
    }

    fn do_write_dir<DW: Directory>(
        &mut self,
        directory: &DW,
        segment_file_name: String,
    ) -> Result<()> {
        {
            let mut segn_output =
                directory.create_output(&segment_file_name, &IOContext::Default)?;
            self.write_output(&mut segn_output)?;
        }

        let mut sync_files = HashSet::with_capacity(1);
        sync_files.insert(segment_file_name);
        directory.sync(&sync_files)
    }

    /// Write ourselves to the provided `IndexOuptut`
    pub fn write_output(&self, output: &mut impl IndexOutput) -> Result<()> {
        write_index_header(
            output,
            "segments",
            SEGMENT_VERSION_CURRENT,
            &random_id(),
            &to_base36(self.generation as u64),
        )?;
        output.write_vint(VERSION_LATEST.major)?;
        output.write_vint(VERSION_LATEST.minor)?;
        output.write_vint(VERSION_LATEST.bugfix)?;

        output.write_long(self.version)?;
        output.write_int(self.counter)?;
        output.write_int(self.len() as i32)?;

        if self.len() > 0 {
            let mut min_version = VERSION_LATEST;

            // We do a separate loop up front so we can write the minSegmentVersion before
            // any SegmentInfo; this makes it cleaner to throw IndexFormatTooOldExc at read time:
            for commit in &self.segments {
                if min_version > commit.info.version {
                    min_version = commit.info.version;
                }
            }

            output.write_vint(min_version.major)?;
            output.write_vint(min_version.minor)?;
            output.write_vint(min_version.bugfix)?;
        }

        // write infos
        for commit in &self.segments {
            output.write_string(&commit.info.name)?;
            output.write_byte(1)?;
            output.write_bytes(commit.info.get_id(), 0, ID_LENGTH)?;
            output.write_string(commit.info.codec().name())?;
            output.write_long(commit.del_gen())?;
            let del_count = commit.del_count();
            if del_count < 0 || del_count > commit.info.max_doc() {
                bail!(IllegalState(
                    "cannot write segment: invalid del_count".into()
                ));
            }
            output.write_int(del_count)?;
            output.write_long(commit.field_infos_gen())?;
            output.write_long(commit.doc_values_gen())?;
            output.write_set_of_strings(&commit.field_infos_files)?;
            let dv_files = commit.get_doc_values_updates_files();
            output.write_int(dv_files.len() as i32)?;
            for (gen, files) in dv_files {
                output.write_int(*gen)?;
                output.write_set_of_strings(files)?;
            }
        }
        output.write_map_of_strings(&HashMap::with_capacity(0))?;
        write_footer(output)
    }

    /// Returns all file names referenced by SegmentInfo. The returned
    /// collection is recomputed on each invocation.
    pub fn files(&self, include_segments_file: bool) -> HashSet<String> {
        let mut files = HashSet::new();
        if include_segments_file {
            if let Some(segment_file_name) = self.segment_file_name() {
                files.insert(segment_file_name);
            }
        }
        for info in &self.segments {
            files.extend(info.files());
        }
        files
    }

    /// Returns the committed segments_N filename.
    pub fn finish_commit<DW: Directory>(&mut self, dir: &DW) -> Result<String> {
        if !self.pending_commit {
            bail!(IllegalState("prepare_commit was not called".into()));
        }

        let src =
            file_name_from_generation(INDEX_FILE_PENDING_SEGMENTS, "", self.generation as u64);
        let dest = file_name_from_generation(INDEX_FILE_SEGMENTS, "", self.generation as u64);

        if let Err(e) = self.rename(dir, &src, &dest) {
            self.rollback_commit(dir);
            return Err(e);
        }

        self.pending_commit = false;
        self.last_generation = self.generation;
        Ok(dest)
    }

    fn rename<DW: Directory>(&self, dir: &DW, src: &str, dest: &str) -> Result<()> {
        dir.rename(&src, &dest)?;
        dir.sync_meta_data()
    }

    pub fn total_max_doc(&self) -> i32 {
        (&self.segments).iter().map(|s| s.info.max_doc()).sum()
    }

    /// Set the generation to be used for the next commit
    pub fn set_next_write_generation(&mut self, generation: i64) -> Result<()> {
        if generation < self.generation {
            bail!(IllegalState("cannot decrease generation".into()));
        }
        self.generation = generation;
        Ok(())
    }

    pub fn create_backup_segment_infos(&self) -> Vec<Arc<SegmentCommitInfo<D, C>>> {
        let mut list = Vec::with_capacity(self.segments.len());
        for s in &self.segments {
            list.push(Arc::new(s.as_ref().clone()));
        }
        list
    }

    pub fn rollback_segment_infos(&mut self, infos: Vec<Arc<SegmentCommitInfo<D, C>>>) {
        self.segments = infos;
    }

    pub fn add(&mut self, si: Arc<SegmentCommitInfo<D, C>>) {
        self.segments.push(si);
    }

    pub fn clear(&mut self) {
        self.segments.clear();
    }

    pub fn remove(&mut self, si: &Arc<SegmentCommitInfo<D, C>>) {
        let mut found = self.segments.len();
        for i in 0..self.segments.len() {
            if self.segments[i].info.name.cmp(&si.info.name) == Ordering::Equal {
                found = i;
            }
        }

        if found < self.segments.len() {
            self.segments.remove(found);
        }
    }

    pub fn changed(&mut self) {
        self.version += 1;
    }
}

impl<D: Directory, C: Codec> SegmentInfos<D, C> {
    /// applies all changes caused by committing a merge to this SegmentInfos
    pub fn apply_merge_changes(&mut self, merge: &OneMerge<D, C>, drop_segment: bool) {
        let mut merged_away = HashSet::new();
        for seg in &merge.segments {
            merged_away.insert(seg);
        }

        let mut inserted = false;
        let mut new_seg_idx = 0;
        for i in 0..self.segments.len() {
            debug_assert!(i >= new_seg_idx);
            if merged_away.contains(&self.segments[i]) {
                if !inserted && !drop_segment {
                    self.segments[i] = Arc::clone(&merge.info.as_ref().unwrap());
                    inserted = true;
                    new_seg_idx += 1;
                }
            } else {
                if new_seg_idx != i {
                    self.segments.swap(new_seg_idx, i);
                }
                new_seg_idx += 1;
            }
        }

        // the rest of the segments in list are duplicates,
        // so don't remove from map, only list!
        self.segments.truncate(new_seg_idx);

        // Either we found place to insert segment, or, we did
        // not, but only because all segments we merged becamee
        // deleted while we are merging, in which case it should
        // be the case that the new segment is also all deleted,
        // we insert it at the beginning if it should not be dropped:
        if !inserted && !drop_segment {
            self.segments
                .insert(0, Arc::clone(&merge.info.as_ref().unwrap()));
        }
    }

    /// Read a particular segmentFileName.  Note that this may
    /// throw an IOException if a commit is in process.
    ///
    /// @param directory -- directory containing the segments file
    /// @param segmentFileName -- segment file to load
    /// @throws CorruptIndexException if the index is corrupt
    /// @throws IOException if there is a low-level IO error
    pub fn read_commit(directory: &Arc<D>, segment_file_name: &str) -> Result<Self> {
        let generation = generation_from_segments_file_name(segment_file_name)?;
        let input = directory.open_input(segment_file_name, &IOContext::READ)?;
        let mut checksum = BufferedChecksumIndexInput::new(input);
        let infos = Self::read_commit_generation(directory, &mut checksum, generation)?;
        validate_footer(&mut checksum)?;
        let digest = checksum.checksum();
        check_checksum(&mut checksum, digest)?;
        Ok(infos)
    }

    /// Read the commit from the provided {@link ChecksumIndexInput}.
    fn read_commit_generation(
        directory: &Arc<D>,
        input: &mut dyn IndexInput,
        generation: i64,
    ) -> Result<Self> {
        let magic = input.read_int()?;
        if magic != CODEC_MAGIC {
            return Err("invalid magic number".into());
        }

        let format = check_header_no_magic(
            input,
            "segments",
            SEGMENT_VERSION_50,
            SEGMENT_VERSION_CURRENT,
        )?;

        let mut id = [0; ID_LENGTH];
        input.read_exact(&mut id)?;
        check_index_header_suffix(input, &to_base36(generation as u64))?;
        let lucene_version = if format >= SEGMENT_VERSION_53 {
            Some(Version::new(
                input.read_vint()?,
                input.read_vint()?,
                input.read_vint()?,
            )?)
        } else {
            None
        };
        let version = input.read_long()?;
        let counter = input.read_int()?;
        let num_segs = input.read_int()?;
        if num_segs < 0 {
            return Err(format!("invalid segment count: {}", num_segs).into());
        }
        let min_seg_ver: Option<Version> = if format >= SEGMENT_VERSION_53 && num_segs > 0 {
            Some(Version::new(
                input.read_vint()?,
                input.read_vint()?,
                input.read_vint()?,
            )?)
        } else {
            None
        };

        // let mut total_docs = 0;
        let mut segments = Vec::new();
        for _sge in 0..num_segs {
            let seg_name = input.read_string()?;
            let has_id = input.read_byte()?;
            if has_id != 1u8 {
                return Err(format!("invalid hasID byte, got: {}", has_id).into());
            }
            let mut segment_id = [0; ID_LENGTH];
            input.read_exact(&mut segment_id)?;
            let codec: Arc<C> = Arc::new(read_codec(input, format < SEGMENT_VERSION_53)?);
            let mut info = codec.segment_info_format().read(
                directory,
                seg_name.as_ref(),
                segment_id,
                &IOContext::READ,
            )?;
            info.set_codec(codec);
            // total_docs += info.max_doc();
            let del_gen = input.read_long()?;
            let del_count = input.read_int()?;
            if del_count < 0 || del_count > info.max_doc() {
                return Err(format!(
                    "invalid deletion count: {} vs maxDoc={}",
                    del_count,
                    info.max_doc()
                )
                .into());
            }
            let field_infos_gen = input.read_long()?;
            let dv_gen = input.read_long()?;
            let field_infos_files = input.read_set_of_strings()?;
            let num_dv_fields = input.read_int()?;
            let dv_update_files = if num_dv_fields == 0 {
                HashMap::new()
            } else {
                let mut map = HashMap::with_capacity(num_dv_fields as usize);
                for _i in 0..num_dv_fields {
                    map.insert(input.read_int()?, input.read_set_of_strings()?);
                }
                map
            };
            let si_per_commit = SegmentCommitInfo::new(
                info,
                del_count,
                del_gen,
                field_infos_gen,
                dv_gen,
                dv_update_files,
                field_infos_files,
            );
            segments.push(Arc::new(si_per_commit));

            if format < SEGMENT_VERSION_53 {
                // TODO check version
            }
        }
        let _user_data = input.read_map_of_strings();

        Ok(SegmentInfos::new(
            counter as i32,
            version,
            generation,
            generation,
            segments,
            id,
            lucene_version,
            min_seg_ver,
        ))
    }

    pub fn read_latest_commit(directory: &Arc<D>) -> Result<Self> {
        run_with_find_segment_file(directory, None, |dir: (&Arc<D>, &str)| {
            SegmentInfos::read_commit(dir.0, dir.1)
        })
    }
}

impl<D: Directory, C: Codec> Clone for SegmentInfos<D, C> {
    fn clone(&self) -> Self {
        let segments: Vec<Arc<SegmentCommitInfo<D, C>>> = self
            .segments
            .iter()
            .map(|s| Arc::new(s.as_ref().clone()))
            .collect();
        let mut id = [0u8; ID_LENGTH];
        id.copy_from_slice(&self.id);
        Self {
            counter: self.counter,
            version: self.version,
            generation: self.generation,
            last_generation: self.last_generation,
            segments,
            id,
            lucene_version: self.lucene_version,
            min_seg_version: self.min_seg_version,
            pending_commit: self.pending_commit,
        }
    }
}

pub fn get_last_commit_segments_filename(files: &[String]) -> Result<Option<String>> {
    let generation = get_last_commit_generation(files)?;
    if generation < 0 {
        Ok(None)
    } else {
        Ok(Some(file_name_from_generation(
            INDEX_FILE_SEGMENTS,
            "",
            generation as u64,
        )))
    }
}

/// Get the generation of the most recent commit to the
/// list of index files (N in the segments_N file).
///
/// @param files -- array of file names to check
pub fn get_last_commit_generation(files: &[String]) -> Result<i64> {
    let mut max = -1;
    for file_ref in files {
        if file_ref.starts_with(INDEX_FILE_SEGMENTS) && file_ref != INDEX_FILE_OLD_SEGMENT_GEN {
            let gen = generation_from_segments_file_name(file_ref)?;
            if gen > max {
                max = gen;
            }
        }
    }
    Ok(max)
}

/// Parse the generation off the segments file name and return it.
pub fn generation_from_segments_file_name(file_name: &str) -> Result<i64> {
    if file_name.eq(INDEX_FILE_SEGMENTS) {
        Ok(0)
    } else if file_name.starts_with(INDEX_FILE_SEGMENTS) {
        match i64::from_str_radix(&file_name[INDEX_FILE_SEGMENTS.len() + 1..], 36) {
            Ok(x) => Ok(x),
            Err(e) => bail!(NumError(e)),
        }
    } else {
        Err(format!("fileName \"{}\" is not a segments file", file_name).into())
    }
}

fn read_codec<T: Codec>(input: &mut dyn IndexInput, _unsupported_allowed: bool) -> Result<T> {
    let name = input.read_string()?;
    T::try_from(name)
}

/// Utility function for executing code that needs to do
/// something with the current segments file.  This is
/// necessary with lock-less commits because from the time
/// you locate the current segments file name, until you
/// actually open it, read its contents, or check modified
/// time, etc., it could have been deleted due to a writer
/// commit finishing.
///
/// NOTE: this function is copy from lucene class `FindSegmentsFile` but
/// only return the segment file name ,thus the abstract logic should be done
/// by the app the call this.
///
/// there won't be a trait like `FindSegmentsFile` because of useless for rust
pub fn get_segment_file_name<D: Directory>(directory: &D) -> Result<String> {
    // TODO currently not support IndexCommit

    let mut last_gen;
    let mut gen = -1;
    loop {
        last_gen = gen;
        let files = directory.list_all()?;
        let files2 = directory.list_all()?;
        if !&files.eq(&files2) {
            continue;
        }

        gen = get_last_commit_generation(&files)?;
        if gen == -1 {
            return Err(format!("no segments* file found in directory: files: {:?}", files).into());
        } else if gen > last_gen {
            return Ok(file_name_from_generation(
                &INDEX_FILE_SEGMENTS,
                "",
                gen as u64,
            ));
        }
    }
}

/// Utility function for executing code that needs to do
/// something with the current segments file.  This is
/// necessary with lock-less commits because from the time
/// you locate the current segments file name, until you
/// actually open it, read its contents, or check modified
/// time, etc., it could have been deleted due to a writer
/// commit finishing.
pub fn run_with_find_segment_file<T, Output, D: Directory>(
    directory: &Arc<D>,
    commit: Option<&CommitPoint>,
    do_body: T,
) -> Result<Output>
where
    T: Fn((&Arc<D>, &str)) -> Result<Output>,
{
    if let Some(commit) = commit {
        return do_body((directory, commit.segments_file_name()));
    }

    let mut last_gen;
    let mut gen = -1;

    // just a stub for init
    // Loop until we succeed in calling doBody() without
    // hitting an IOException.  An IOException most likely
    // means an IW deleted our commit while opening
    // the time it took us to load the now-old infos files
    // (and segments files).  It's also possible it's a
    // true error (corrupt index).  To distinguish these,
    // on each retry we must see "forward progress" on
    // which generation we are trying to load.  If we
    // don't, then the original error is real and we throw it.
    let mut err: Result<Output> = Err("".into());
    loop {
        last_gen = gen;
        let mut files = directory.list_all()?;
        let mut files2 = directory.list_all()?;
        files.sort();
        files2.sort();
        if files != files2 {
            // listAll() is weakly consistent, this means we hit "concurrent modification exception"
            continue;
        }

        gen = get_last_commit_generation(&files)?;

        if gen == -1 {
            bail!(
                "IndexNotFound: no segments* file found, files: {:?}",
                &files
            );
        } else if gen > last_gen {
            let segment_file_name = file_name_from_generation(INDEX_FILE_SEGMENTS, "", gen as u64);

            match do_body((directory, &segment_file_name)) {
                Ok(r) => {
                    return Ok(r);
                }
                Err(e) => {
                    debug!(
                        "primary error on {} : err: '{:?}'. will retry: gen={}",
                        &segment_file_name, e, gen
                    );
                    err = Err(e);
                }
            }
        } else {
            return err;
        }
    }
}
