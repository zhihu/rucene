use core::codec::codec_util;
use core::codec::format::FieldInfosFormat;
use core::index::{segment_file_name, DocValuesType, IndexOptions, SegmentInfo};
use core::index::{FieldInfo, FieldInfos};
use core::store::{BufferedChecksumIndexInput, ChecksumIndexInput, IOContext, IndexInput};
use core::store::{Directory, DirectoryRc};
use error::Result;

/// Extension of field infos
const EXTENSION: &str = "fnm";

// Codec header
const CODEC_NAME: &str = "Lucene60FieldInfos";
const FORMAT_START: i32 = 0;
const FORMAT_CURRENT: i32 = FORMAT_START;

// Field flags
const STORE_TERM_VECTOR: u8 = 0x1;
const OMIT_NORMS: u8 = 0x2;
const STORE_PAYLOADS: u8 = 0x4;

fn read_field_infos_from_index<T: IndexInput + ?Sized>(
    input: &mut T,
    segment_info: &SegmentInfo,
    suffix: &str,
) -> Result<Vec<FieldInfo>> {
    let mut infos: Vec<FieldInfo> = Vec::new();
    codec_util::check_index_header(
        input,
        CODEC_NAME,
        FORMAT_START,
        FORMAT_CURRENT,
        &segment_info.id,
        suffix,
    )?;

    let size = input.read_vint()?;
    for _ in 0..size {
        let name = input.read_string()?;
        let field_number = input.read_vint()?;
        if field_number < 0 {
            bail!(
                "Corrupted Index: invalid field number for field: {}, fieldNumber={}",
                name,
                field_number
            );
        }
        let bits = input.read_byte()?;
        let store_term_vector = (bits & STORE_TERM_VECTOR) != 0;
        let omit_norms = (bits & OMIT_NORMS) != 0;
        let store_payloads = (bits & STORE_PAYLOADS) != 0;
        let index_options = read_index_options(input)?;
        let doc_values_type = read_doc_values_type(input)?;
        let dv_gen = input.read_long()?;
        let attributes = input.read_map_of_strings()?;
        let point_dimension_count = input.read_vint()?;
        let point_num_bytes = if point_dimension_count != 0 {
            input.read_vint()?
        } else {
            0
        };

        let info = FieldInfo::new(
            name,
            field_number,
            store_term_vector,
            omit_norms,
            store_payloads,
            index_options,
            doc_values_type,
            dv_gen,
            attributes,
            point_dimension_count,
            point_num_bytes,
        )?;
        infos.push(info);
    }
    Ok(infos)
}

pub fn read_field_infos(
    directory: &mut Directory,
    segment_info: &SegmentInfo,
    suffix: &str,
    context: &IOContext,
) -> Result<FieldInfos> {
    let file_name = segment_file_name(&segment_info.name, suffix, EXTENSION);
    let original_input = directory.open_input(&file_name, context)?;
    let mut checksum = BufferedChecksumIndexInput::new(original_input);
    let infos = read_field_infos_from_index(&mut checksum, segment_info, suffix)?;
    codec_util::validate_footer(&mut checksum)?;
    let digest = checksum.checksum();
    codec_util::check_checksum(&mut checksum, digest)?;
    Ok(FieldInfos::new(infos)?)
}

fn read_index_options<T: IndexInput + ?Sized>(input: &mut T) -> Result<IndexOptions> {
    let byte = input.read_byte()?;
    Ok(match byte {
        0 => IndexOptions::Null,
        1 => IndexOptions::Docs,
        2 => IndexOptions::DocsAndFreqs,
        3 => IndexOptions::DocsAndFreqsAndPositions,
        4 => IndexOptions::DocsAndFreqsAndPositionsAndOffsets,
        _ => bail!("Corrupted Index: invalid IndexOptions byte: {}", byte),
    })
}

fn read_doc_values_type<T: IndexInput + ?Sized>(input: &mut T) -> Result<DocValuesType> {
    let byte = input.read_byte()?;
    Ok(match byte {
        0 => DocValuesType::Null,
        1 => DocValuesType::Numeric,
        2 => DocValuesType::Binary,
        3 => DocValuesType::Sorted,
        4 => DocValuesType::SortedSet,
        5 => DocValuesType::SortedNumeric,
        _ => bail!("Corrupted Index: invalid DocValuesType byte: {}", byte),
    })
}

pub struct Lucene60FieldInfosFormat;

impl Default for Lucene60FieldInfosFormat {
    fn default() -> Lucene60FieldInfosFormat {
        Lucene60FieldInfosFormat {}
    }
}

impl FieldInfosFormat for Lucene60FieldInfosFormat {
    fn read(
        &self,
        directory: DirectoryRc,
        segment_info: &SegmentInfo,
        segment_suffix: &str,
        ctx: &IOContext,
    ) -> Result<FieldInfos> {
        let file_name = segment_file_name(&segment_info.name, segment_suffix, EXTENSION);

        let input = directory.open_input(&file_name, ctx)?;
        let mut checksum = BufferedChecksumIndexInput::new(input);

        let infos = read_field_infos_from_index(&mut checksum, segment_info, segment_suffix)?;

        codec_util::validate_footer(&mut checksum)?;
        let digest = checksum.checksum();

        codec_util::check_checksum(&mut checksum, digest)?;

        Ok(FieldInfos::new(infos)?)
    }

    fn write(
        &self,
        _directory: DirectoryRc,
        _segment_info: &SegmentInfo,
        _segment_suffix: &str,
        _infos: &FieldInfos,
        _context: &IOContext,
    ) -> Result<()> {
        unimplemented!()
    }
}
