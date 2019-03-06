use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::mem;
use std::sync::{Arc, Mutex};

use core::codec::codec_for_name;
use core::codec::codec_util;
use core::codec::StoredFieldsReader;
use core::codec::TermVectorsReader;
use core::codec::CODEC_MAGIC;
use core::codec::{Codec, FieldsProducerRef, NormsProducer};
use core::index::file_name_from_generation;
use core::index::index_commit::IndexCommit;
use core::index::merge_policy::OneMerge;
use core::index::PointValues;
use core::index::SegmentReadState;
use core::index::{FieldInfos, SegmentCommitInfo, SegmentInfo};
use core::index::{INDEX_FILE_OLD_SEGMENT_GEN, INDEX_FILE_PENDING_SEGMENTS, INDEX_FILE_SEGMENTS};
use core::store::{BufferedChecksumIndexInput, ChecksumIndexInput, IndexInput, IndexOutput};
use core::store::{Directory, DirectoryRc, IOContext};
use core::util::external::deferred::Deferred;
use core::util::string_util::{random_id, ID_LENGTH};
use core::util::to_base36;
use core::util::{Version, VERSION_LATEST};
use error::ErrorKind::{IllegalState, NumError};
use error::Result;

/// The file format version for the segments_N codec header, since 5.0+
pub const SEGMENT_VERSION_50: i32 = 4;
/// The file format version for the segments_N codec header, since 5.1+
pub const SEGMENT_VERSION_51: i32 = 5;
/// Adds the {@link Version} that committed this segments_N file, as well as the {@link Version} of
/// the oldest segment, since 5.3+
pub const SEGMENT_VERSION_53: i32 = 6;

pub const SEGMENT_VERSION_CURRENT: i32 = SEGMENT_VERSION_53;

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
#[derive(Clone, Debug)]
pub struct SegmentInfos {
    /// Used to name new segments.
    pub counter: i32,
    /// Counts how often the index has been changed.
    pub version: i64,
    // generation of the "segments_N" for the next commit
    pub generation: i64,
    // generation of the "segments_N" file we last successfully read or wrote; this is normally
    // the same as generation except if there was an IOException that had interrupted a commit
    pub last_generation: i64,
    pub segments: Vec<Arc<SegmentCommitInfo>>,
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

impl Default for SegmentInfos {
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

impl SegmentInfos {
    #[allow(too_many_arguments)]
    pub fn new(
        counter: i32,
        version: i64,
        generation: i64,
        last_generation: i64,
        segments: Vec<Arc<SegmentCommitInfo>>,
        id: [u8; ID_LENGTH],
        lucene_version: Option<Version>,
        min_seg_version: Option<Version>,
    ) -> SegmentInfos {
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

    /// Read a particular segmentFileName.  Note that this may
    /// throw an IOException if a commit is in process.
    ///
    /// @param directory -- directory containing the segments file
    /// @param segmentFileName -- segment file to load
    /// @throws CorruptIndexException if the index is corrupt
    /// @throws IOException if there is a low-level IO error
    ///
    pub fn read_commit(directory: &DirectoryRc, segment_file_name: &str) -> Result<SegmentInfos> {
        let generation = SegmentInfos::generation_from_segments_file_name(segment_file_name)?;
        let input = directory.open_input(segment_file_name, &IOContext::Read(false))?;
        let mut checksum = BufferedChecksumIndexInput::new(input);
        let infos = SegmentInfos::read_commit_generation(directory, &mut checksum, generation)?;
        codec_util::validate_footer(&mut checksum)?;
        let digest = checksum.checksum();
        codec_util::check_checksum(&mut checksum, digest)?;
        Ok(infos)
    }

    /// Read the commit from the provided {@link ChecksumIndexInput}.
    fn read_commit_generation(
        directory: &DirectoryRc,
        input: &mut IndexInput,
        generation: i64,
    ) -> Result<SegmentInfos> {
        let magic = input.read_int()?;
        if magic != CODEC_MAGIC {
            return Err("invalid magic number".into());
        }

        let format = codec_util::check_header_no_magic(
            input,
            "segments",
            SEGMENT_VERSION_50,
            SEGMENT_VERSION_CURRENT,
        )?;

        let mut id = [0; ID_LENGTH];
        input.read_bytes(&mut id, 0, ID_LENGTH)?;
        codec_util::check_index_header_suffix(input, &to_base36(generation as u64))?;
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
            input.read_bytes(&mut segment_id, 0, ID_LENGTH)?;
            let codec: Arc<Codec> = Arc::from(SegmentInfos::read_codec(
                input,
                format < SEGMENT_VERSION_53,
            )?);
            let mut info: SegmentInfo = codec.segment_info_format().read(
                directory,
                seg_name.as_ref(),
                segment_id,
                &IOContext::Read(false),
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
                ).into());
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

    pub fn read_latest_commit(directory: &DirectoryRc) -> Result<Self> {
        run_with_find_segment_file(directory, None, |dir: (&DirectoryRc, &str)| {
            SegmentInfos::read_commit(dir.0, dir.1)
        })
    }

    fn read_codec(input: &mut IndexInput, _unsupported_allowed: bool) -> Result<Box<Codec>> {
        let name = input.read_string()?;
        codec_for_name(name.as_ref())
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

    /// Get the generation of the most recent commit to the
    /// list of index files (N in the segments_N file).
    ///
    /// @param files -- array of file names to check
    ///
    pub fn get_last_commit_generation(files: &[String]) -> Result<i64> {
        let mut max = -1;
        for file_ref in files {
            if file_ref.starts_with(INDEX_FILE_SEGMENTS) && file_ref != INDEX_FILE_OLD_SEGMENT_GEN {
                let gen = SegmentInfos::generation_from_segments_file_name(file_ref)?;
                if gen > max {
                    max = gen;
                }
            }
        }
        Ok(max)
    }

    pub fn get_last_commit_segments_filename(files: &[String]) -> Result<Option<String>> {
        let generation = Self::get_last_commit_generation(files)?;
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

    /// Parse the generation off the segments file name and return it.
    ///
    pub fn generation_from_segments_file_name(file_name: &str) -> Result<i64> {
        if file_name.eq(INDEX_FILE_SEGMENTS) {
            Ok(0)
        } else if file_name.starts_with(INDEX_FILE_SEGMENTS) {
            // TODO 这里应该有问题， 需要解析 36 进制数字字符串
            match i64::from_str_radix(&file_name[INDEX_FILE_SEGMENTS.len() + 1..], 36) {
                Ok(x) => Ok(x),
                Err(e) => bail!(NumError(e)),
            }
        } else {
            Err(format!("fileName \"{}\" is not a segments file", file_name).into())
        }
    }

    /// Returns a copy of this instance, also copying each SegmentInfo
    pub fn len(&self) -> usize {
        self.segments.len()
    }

    pub fn rollback_commit(&mut self, dir: &Directory) {
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
    pub fn prepare_commit(&mut self, dir: &Directory) -> Result<()> {
        if self.pending_commit {
            bail!(IllegalState("prepare_commit was already called".into()));
        }
        self.write_dir(dir)
    }

    fn write_dir(&mut self, directory: &Directory) -> Result<()> {
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

    fn do_write_dir(&mut self, directory: &Directory, segment_file_name: String) -> Result<()> {
        {
            let mut segn_output = directory.create_output(&segment_file_name, &IOContext::Default)?;
            self.write_output(directory, segn_output.as_mut())?;
        }

        let mut sync_files = HashSet::with_capacity(1);
        sync_files.insert(segment_file_name);
        directory.sync(&sync_files)
    }

    /// Write ourselves to the provided `IndexOuptut`
    pub fn write_output(&self, _directory: &Directory, output: &mut IndexOutput) -> Result<()> {
        codec_util::write_index_header(
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
            let mut min_version = &VERSION_LATEST;

            // We do a separate loop up front so we can write the minSegmentVersion before
            // any SegmentInfo; this makes it cleaner to throw IndexFormatTooOldExc at read time:
            for commit in &self.segments {
                if min_version > &commit.info.version {
                    min_version = &commit.info.version;
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
            output.write_long(commit.doc_values_gen)?;
            output.write_set_of_strings(&commit.field_infos_files)?;
            // TODO add doc_values_updates_files
            output.write_int(0)?;
        }
        output.write_map_of_strings(&HashMap::with_capacity(0))?;
        codec_util::write_footer(output)
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
    pub fn finish_commit(&mut self, dir: &Directory) -> Result<String> {
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

    fn rename(&self, dir: &Directory, src: &str, dest: &str) -> Result<()> {
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

    pub fn create_backup_segment_infos(&self) -> Vec<Arc<SegmentCommitInfo>> {
        let mut list = Vec::with_capacity(self.segments.len());
        for s in &self.segments {
            list.push(Arc::clone(s));
        }
        list
    }

    pub fn rollback_segment_infos(&mut self, infos: Vec<Arc<SegmentCommitInfo>>) {
        self.segments = infos;
    }

    pub fn add(&mut self, si: Arc<SegmentCommitInfo>) {
        self.segments.push(si);
    }

    pub fn clear(&mut self) {
        self.segments.clear();
    }

    pub fn remove(&mut self, si: &Arc<SegmentCommitInfo>) {
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

    /// applies all changes caused by committing a merge to this SegmentInfos
    pub fn apply_merge_changes(&mut self, merge: &OneMerge, drop_segment: bool) {
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
///
pub fn get_segment_file_name(directory: &Directory) -> Result<String> {
    // TODO 暂未实现处理 IndexCommit 的相关逻辑

    let mut last_gen;
    let mut gen = -1;
    loop {
        last_gen = gen;
        let files = directory.list_all()?;
        let files2 = directory.list_all()?;
        if !&files.eq(&files2) {
            continue;
        }

        gen = SegmentInfos::get_last_commit_generation(&files)?;
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
pub fn run_with_find_segment_file<T, Output>(
    directory: &DirectoryRc,
    commit: Option<&IndexCommit>,
    do_body: T,
) -> Result<Output>
where
    T: Fn((&DirectoryRc, &str)) -> Result<Output>,
{
    if let Some(commit) = commit {
        if directory.as_ref() as *const Directory != commit.directory() as *const Directory {
            bail!("the specified commit does not match the specified Directory");
        }
        return do_body((directory, commit.segments_file_name()));
    }

    let mut last_gen;
    let mut gen = -1;
    let mut err: Result<Output> = Err("".into()); // just a stub for init
                                                  // Loop until we succeed in calling doBody() without
                                                  // hitting an IOException.  An IOException most likely
                                                  // means an IW deleted our commit while opening
                                                  // the time it took us to load the now-old infos files
                                                  // (and segments files).  It's also possible it's a
                                                  // true error (corrupt index).  To distinguish these,
                                                  // on each retry we must see "forward progress" on
                                                  // which generation we are trying to load.  If we
                                                  // don't, then the original error is real and we throw it.
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

        gen = SegmentInfos::get_last_commit_generation(&files)?;

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

/// Holds core readers that are shared (unchanged) when
/// SegmentReader is cloned or reopened
///
pub struct SegmentCoreReaders {
    pub fields: FieldsProducerRef,
    pub norms_producer: Option<Arc<NormsProducer>>,
    pub fields_reader: Arc<StoredFieldsReader>,
    pub term_vectors_reader: Option<Arc<TermVectorsReader>>,
    pub segment: String,
    pub cfs_reader: Option<DirectoryRc>,
    /// fieldinfos for this core: means gen=-1.
    /// this is the exact fieldinfos these codec components saw at write.
    /// in the case of DV updates, SR may hold a newer version.
    pub core_field_infos: Arc<FieldInfos>,
    pub points_reader: Option<Arc<PointValues>>,
    core_dropped_listeners: Mutex<Vec<Deferred>>,
}

impl SegmentCoreReaders {
    pub fn new(dir: &DirectoryRc, si: &SegmentInfo, ctx: &IOContext) -> Result<SegmentCoreReaders> {
        let codec = si.codec();

        let cfs_dir = if si.is_compound_file() {
            codec
                .compound_format()
                .get_compound_reader(dir.clone(), si, ctx)?
        } else {
            dir.clone()
        };

        let cfs_reader = if si.is_compound_file() {
            Some(Arc::clone(&cfs_dir))
        } else {
            None
        };

        let segment = si.name.clone();
        let core_field_infos =
            Arc::new(codec
                .field_infos_format()
                .read(cfs_dir.as_ref(), si, "", &ctx)?);
        // TODO cfs_dir is not a Box<Directory>
        let segment_read_state = SegmentReadState::new(
            cfs_dir.clone(),
            si,
            core_field_infos.clone(),
            ctx,
            String::new(),
        );

        let norms_producer = if core_field_infos.has_norms {
            Some(codec.norms_format().norms_producer(&segment_read_state)?)
        } else {
            None
        };

        let format = codec.postings_format();
        let fields = Arc::from(format.fields_producer(&segment_read_state)?);

        let fields_reader = codec.stored_fields_format().fields_reader(
            cfs_dir.clone(),
            si,
            core_field_infos.clone(),
            ctx,
        )?;
        let term_vectors_reader = if core_field_infos.has_vectors {
            Some(Arc::from(codec.term_vectors_format().tv_reader(
                cfs_dir.clone(),
                si,
                core_field_infos.clone(),
                ctx,
            )?))
        } else {
            None
        };
        let points_reader = if core_field_infos.has_point_values {
            Some(Arc::from(codec
                .points_format()
                .fields_reader(&segment_read_state)?))
        } else {
            None
        };
        // TODO process norms_producers/store_fields_reader/term vectors

        Ok(SegmentCoreReaders {
            fields,
            fields_reader: Arc::from(fields_reader),
            norms_producer: norms_producer.map(Arc::from),
            term_vectors_reader,
            segment,
            cfs_reader,
            core_field_infos,
            points_reader,
            core_dropped_listeners: Mutex::new(vec![]),
        })
    }

    pub fn fields(&self) -> FieldsProducerRef {
        self.fields.clone()
    }

    pub fn add_core_drop_listener(&self, listener: Deferred) {
        let mut guard = self.core_dropped_listeners.lock().unwrap();
        guard.push(listener);
    }

    pub fn core_field_infos(&self) -> Arc<FieldInfos> {
        Arc::clone(&self.core_field_infos)
    }

    //    fn field_info(&self, field: &str) -> Option<&FieldInfo> {
    //        self.core_field_infos.field_info_by_name(field)
    //    }
    //
    //    fn sorted_doc_values(&self, _field: &str) -> Box<SortedDocValues> {
    //        unimplemented!()
    //    }
    //
    //    fn sorted_set_doc_values(&self, _field: &str) -> Box<SortedSetDocValues> {
    //        unimplemented!()
    //    }
}

impl Drop for SegmentCoreReaders {
    fn drop(&mut self) {
        let mut listeners_guard = self.core_dropped_listeners.lock().unwrap();
        let listeners = mem::replace(&mut *listeners_guard, Vec::with_capacity(0));
        for mut listener in listeners {
            listener.call();
        }
    }
}
