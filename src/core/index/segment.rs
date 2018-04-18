use std::collections::HashMap;
use std::sync::Arc;

use core::codec::codec_for_name;
use core::codec::codec_util;
use core::codec::codec_util::CODEC_MAGIC;
use core::codec::reader::StoredFieldsReader;
use core::codec::reader::TermVectorsReader;
use core::codec::{Codec, FieldsProducerRef, NormsProducer};
use core::index::file_name_from_generation;
use core::index::point_values::PointValues;
use core::index::SegmentReadState;
use core::index::{FieldInfos, SegmentCommitInfo, SegmentInfo};
use core::index::{INDEX_FILE_OLD_SEGMENT_GEN, INDEX_FILE_SEGMENTS};
use core::store::{BufferedChecksumIndexInput, ChecksumIndexInput, IndexInput};
use core::store::{DirectoryRc, IOContext};
use core::util::string_util::ID_LENGTH;
use core::util::to_base36;
use core::util::Version;
use error::ErrorKind::NumError;
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
        codec_util::check_index_header_suffix(input, &to_base36(generation))?;
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
                Arc::clone(directory),
                seg_name.as_ref(),
                &segment_id,
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

    fn read_codec(input: &mut IndexInput, _unsupported_allowed: bool) -> Result<Box<Codec>> {
        let name = input.read_string()?;
        codec_for_name(name.as_ref())
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
    ///
    pub fn size(&self) -> usize {
        self.segments.len()
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
pub fn get_segment_file_name(directory: &DirectoryRc) -> Result<String> {
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
            return Ok(file_name_from_generation(&INDEX_FILE_SEGMENTS, "", gen));
        }
    }
}

/// Holds core readers that are shared (unchanged) when
/// SegmentReader is cloned or reopened
///
pub struct SegmentCoreReaders {
    pub fields: FieldsProducerRef,
    pub norms_producer: Option<Box<NormsProducer>>,
    pub fields_reader: Box<StoredFieldsReader>,
    pub term_vectors_reader: Option<Box<TermVectorsReader>>,
    pub segment: String,
    pub cfs_reader: Option<DirectoryRc>,
    /// fieldinfos for this core: means gen=-1.
    /// this is the exact fieldinfos these codec components saw at write.
    /// in the case of DV updates, SR may hold a newer version.
    pub core_field_infos: Arc<FieldInfos>,
    pub points_reader: Option<Arc<PointValues>>,
}

impl SegmentCoreReaders {
    pub fn new(dir: &DirectoryRc, si: &SegmentInfo, ctx: IOContext) -> Result<SegmentCoreReaders> {
        let codec = si.codec();

        let cfs_dir = if si.is_compound_file() {
            codec
                .compound_format()
                .get_compound_reader(dir.clone(), si, &ctx)?
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
                .read(cfs_dir.clone(), si, "", &ctx)?);
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
            &ctx,
        )?;
        let term_vectors_reader = if core_field_infos.has_vectors {
            Some(codec.term_vectors_format().vectors_reader(
                cfs_dir.clone(),
                si,
                core_field_infos.clone(),
                &ctx,
            )?)
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
            fields_reader,
            norms_producer,
            term_vectors_reader,
            segment,
            cfs_reader,
            core_field_infos,
            points_reader,
        })
    }

    pub fn fields(&self) -> FieldsProducerRef {
        self.fields.clone()
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
