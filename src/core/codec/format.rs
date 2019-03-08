use core::codec::lucene50::{self, Lucene50PostingsFormat};
use core::codec::lucene54::Lucene54DocValuesFormat;

use core::codec::reader::*;
use core::codec::writer::*;
use core::codec::{DocValuesConsumer, DocValuesProducer};
use core::codec::{FieldsConsumer, FieldsProducer, NormsConsumer, NormsProducer};
use core::index::FieldInfos;
use core::index::PointValues;
use core::index::{SegmentCommitInfo, SegmentInfo, SegmentReadState, SegmentWriteState};
use core::store::{Directory, DirectoryRc, IOContext};
use core::util::string_util::ID_LENGTH;
use core::util::{Bits, BitsRef};
use error::Result;

use std::collections::HashSet;
use std::sync::Arc;

/// Encodes/decodes compound files
/// @lucene.experimental
pub trait CompoundFormat {
    /// Returns a Directory view (read-only) for the compound files in this segment
    fn get_compound_reader(
        &self,
        dir: DirectoryRc,
        si: &SegmentInfo,
        ioctx: &IOContext,
    ) -> Result<DirectoryRc>;

    /// Packs the provided segment's files into a compound format.  All files referenced
    /// by the provided {@link SegmentInfo} must have {@link CodecUtil#writeIndexHeader}
    /// and {@link CodecUtil#writeFooter}.
    fn write(&self, dir: &Directory, si: &SegmentInfo, ioctx: &IOContext) -> Result<()>;
}

/// Encodes/decodes terms, postings, and proximity data.
/// <p>
/// Note, when extending this class, the name ({@link #getName}) may
/// written into the index in certain configurations. In order for the segment
/// to be read, the name must resolve to your implementation via {@link #forName(String)}.
/// This method uses Java's
/// {@link ServiceLoader Service Provider Interface} (SPI) to resolve format names.
/// <p>
/// If you implement your own format, make sure that it has a no-arg constructor
/// so SPI can load it.
/// @see ServiceLoader
/// @lucene.experimental
pub trait PostingsFormat {
    /// Reads a segment.  NOTE: by the time this call
    /// returns, it must hold open any files it will need to
    /// use; else, those files may be deleted.
    /// Additionally, required files may be deleted during the execution of
    /// this call before there is a chance to open them. Under these
    /// circumstances an IOException should be thrown by the implementation.
    /// IOExceptions are expected and will automatically cause a retry of the
    /// segment opening logic with the newly revised segments.
    fn fields_producer(&self, state: &SegmentReadState) -> Result<Box<FieldsProducer>>;

    /// Writes a new segment
    fn fields_consumer(&self, state: &SegmentWriteState) -> Result<Box<FieldsConsumer>>;

    /// Returns this posting format's name
    fn name(&self) -> &str;
}

pub fn postings_format_for_name(name: &str) -> Result<Box<PostingsFormat>> {
    match name {
        "Lucene50" => Ok(Box::new(Lucene50PostingsFormat::default())),
        _ => bail!("Invalid postings format: {}", name),
    }
}

/// Controls the format of stored fields
pub trait StoredFieldsFormat {
    /// Returns a {@link StoredFieldsReader} to load stored
    /// fields. */
    fn fields_reader(
        &self,
        directory: DirectoryRc,
        si: &SegmentInfo,
        field_info: Arc<FieldInfos>,
        ioctx: &IOContext,
    ) -> Result<Box<StoredFieldsReader>>;

    /// Returns a {@link StoredFieldsWriter} to write stored
    /// fields
    fn fields_writer(
        &self,
        directory: DirectoryRc,
        si: &mut SegmentInfo,
        ioctx: &IOContext,
    ) -> Result<Box<StoredFieldsWriter>>;
}

/// Controls the format of term vectors
pub trait TermVectorsFormat {
    /// Returns a {@link TermVectorsReader} to read term
    /// vectors.
    fn tv_reader(
        &self,
        directory: DirectoryRc,
        si: &SegmentInfo,
        field_info: Arc<FieldInfos>,
        ioctx: &IOContext,
    ) -> Result<Box<TermVectorsReader>>;

    /// Returns a {@link TermVectorsWriter} to write term
    /// vectors.
    fn tv_writer(
        &self,
        directory: DirectoryRc,
        segment_info: &SegmentInfo,
        context: &IOContext,
    ) -> Result<Box<TermVectorsWriter>>;
}

pub fn term_vectors_format_for_name(name: &str) -> Result<Box<TermVectorsFormat>> {
    match name {
        "Lucene50" | "Lucene53" | "Lucene54" | "Lucene60" | "Lucene62" => {
            Ok(Box::new(lucene50::term_vectors_format()))
        }
        _ => bail!("Unknown format name"),
    }
}

/// Encodes/decodes {@link FieldInfos}
/// @lucene.experimental
pub trait FieldInfosFormat {
    /// Read the {@link FieldInfos} previously written with {@link #write}. */
    fn read(
        &self,
        directory: &Directory,
        segment_info: &SegmentInfo,
        segment_suffix: &str,
        iocontext: &IOContext,
    ) -> Result<FieldInfos>;

    /// Writes the provided {@link FieldInfos} to the
    /// directory. */
    fn write(
        &self,
        directory: &Directory,
        segment_info: &SegmentInfo,
        segment_suffix: &str,
        infos: &FieldInfos,
        context: &IOContext,
    ) -> Result<()>;
}

/// Expert: Controls the format of the
/// {@link SegmentInfo} (segment metadata file).
/// @see SegmentInfo
/// @lucene.experimental
pub trait SegmentInfoFormat {
    /// Read {@link SegmentInfo} data from a directory.
    /// @param directory directory to read from
    /// @param segmentName name of the segment to read
    /// @param segmentID expected identifier for the segment
    /// @return infos instance to be populated with data
    /// @throws IOException If an I/O error occurs
    fn read(
        &self,
        directory: &DirectoryRc,
        segment_name: &str,
        segment_id: [u8; ID_LENGTH],
        context: &IOContext,
    ) -> Result<SegmentInfo>;

    /// Write {@link SegmentInfo} data.
    /// The codec must add its SegmentInfo filename(s) to {@code info} before doing i/o.
    /// @throws IOException If an I/O error occurs
    fn write(
        &self,
        dir: &Directory,
        info: &mut SegmentInfo,
        created_files: &mut Vec<String>,
        io_context: &IOContext,
    ) -> Result<()>;
}

pub trait DocValuesFormat {
    fn name(&self) -> &str;
    fn fields_producer(&self, _state: &SegmentReadState) -> Result<Box<DocValuesProducer>>;

    fn fields_consumer(&self, _state: &SegmentWriteState) -> Result<Box<DocValuesConsumer>>;
}

pub fn doc_values_format_for_name(format: &str) -> Result<Box<DocValuesFormat>> {
    match format {
        "Lucene54" => Ok(Box::new(Lucene54DocValuesFormat::default())),
        _ => unimplemented!(),
    }
}

/// Encodes/decodes per-document score normalization values.
pub trait NormsFormat {
    /// Returns a {@link NormsProducer} to read norms from the index.
    /// <p>
    /// NOTE: by the time this call returns, it must hold open any files it will
    /// need to use; else, those files may be deleted. Additionally, required files
    /// may be deleted during the execution of this call before there is a chance
    /// to open them. Under these circumstances an IOException should be thrown by
    /// the implementation. IOExceptions are expected and will automatically cause
    /// a retry of the segment opening logic with the newly revised segments.
    fn norms_producer(&self, state: &SegmentReadState) -> Result<Box<NormsProducer>>;

    fn norms_consumer(&self, state: &SegmentWriteState) -> Result<Box<NormsConsumer>>;
}

/// Format for live/deleted documents
/// @lucene.experimental */
pub trait LiveDocsFormat {
    /// Creates a new Bits, with all bits set, for the specified size.
    fn new_live_docs(&self, size: usize) -> Result<Box<Bits>>;
    /// Creates a new mutablebits of the same bits set and size of existing.
    fn new_live_docs_from_existing(&self, existing: &Bits) -> Result<BitsRef>;
    /// Read live docs bits.
    fn read_live_docs(
        &self,
        dir: DirectoryRc,
        info: &SegmentCommitInfo,
        context: &IOContext,
    ) -> Result<BitsRef>;

    /// Persist live docs bits.  Use {@link
    /// SegmentCommitInfo#getNextDelGen} to determine the
    /// generation of the deletes file you should write to. */
    fn write_live_docs(
        &self,
        bits: &Bits,
        dir: &Directory,
        info: &SegmentCommitInfo,
        new_del_count: i32,
        context: &IOContext,
    ) -> Result<()>;

    /// Records all files in use by this {@link SegmentCommitInfo} into the files argument. */
    fn files(&self, info: &SegmentCommitInfo, files: &mut HashSet<String>);
}

/// Encodes/decodes indexed points.
/// @lucene.experimental */
pub trait PointsFormat {
    /// Writes a new segment
    fn fields_writer(&self, state: &SegmentWriteState) -> Result<Box<PointsWriter>>;

    /// Reads a segment.  NOTE: by the time this call
    /// returns, it must hold open any files it will need to
    /// use; else, those files may be deleted.
    /// Additionally, required files may be deleted during the execution of
    /// this call before there is a chance to open them. Under these
    /// circumstances an IOException should be thrown by the implementation.
    /// IOExceptions are expected and will automatically cause a retry of the
    /// segment opening logic with the newly revised segments.
    fn fields_reader(&self, state: &SegmentReadState) -> Result<Box<PointValues>>;
}
