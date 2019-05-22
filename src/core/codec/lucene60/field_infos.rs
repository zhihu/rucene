use core::codec::format::FieldInfosFormat;
use core::codec::{codec_util, Codec};
use core::index::{segment_file_name, DocValuesType, IndexOptions, SegmentInfo};
use core::index::{FieldInfo, FieldInfos};
use core::store::Directory;
use core::store::{
    BufferedChecksumIndexInput, ChecksumIndexInput, DataOutput, IOContext, IndexInput,
};
use error::{ErrorKind::CorruptIndex, Result};

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

fn read_field_infos_from_index<T: IndexInput + ?Sized, D: Directory, C: Codec>(
    input: &mut T,
    segment_info: &SegmentInfo<D, C>,
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
            bail!(CorruptIndex(format!(
                "invalid field number for field: {}, field_number={}",
                name, field_number
            )));
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
            field_number as u32,
            store_term_vector,
            omit_norms,
            store_payloads,
            index_options,
            doc_values_type,
            dv_gen,
            attributes,
            point_dimension_count as u32,
            point_num_bytes as u32,
        )?;
        infos.push(info);
    }
    Ok(infos)
}

pub fn read_field_infos<D: Directory, C: Codec>(
    directory: &mut D,
    segment_info: &SegmentInfo<D, C>,
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
        _ => bail!(CorruptIndex(format!("invalid IndexOptions byte: {}", byte))),
    })
}

fn index_options_byte(index_options: IndexOptions) -> u8 {
    use core::index::IndexOptions::*;
    match index_options {
        Null => 0,
        Docs => 1,
        DocsAndFreqs => 2,
        DocsAndFreqsAndPositions => 3,
        DocsAndFreqsAndPositionsAndOffsets => 4,
    }
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
        _ => bail!(CorruptIndex(format!(
            "Corrupted Index: invalid DocValuesType byte: {}",
            byte
        ))),
    })
}

fn doc_values_byte(dv_type: DocValuesType) -> u8 {
    use core::index::DocValuesType::*;
    match dv_type {
        Null => 0,
        Numeric => 1,
        Binary => 2,
        Sorted => 3,
        SortedSet => 4,
        SortedNumeric => 5,
    }
}

#[derive(Copy, Clone, Default)]
pub struct Lucene60FieldInfosFormat;

impl FieldInfosFormat for Lucene60FieldInfosFormat {
    fn read<D: Directory, DW: Directory, C: Codec>(
        &self,
        directory: &DW,
        segment_info: &SegmentInfo<D, C>,
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

    fn write<D: Directory, DW: Directory, C: Codec>(
        &self,
        directory: &DW,
        segment_info: &SegmentInfo<D, C>,
        segment_suffix: &str,
        infos: &FieldInfos,
        context: &IOContext,
    ) -> Result<()> {
        let file_name = segment_file_name(&segment_info.name, segment_suffix, EXTENSION);
        let mut output = directory.create_output(&file_name, context)?;
        codec_util::write_index_header(
            &mut output,
            CODEC_NAME,
            FORMAT_CURRENT,
            segment_info.get_id(),
            segment_suffix,
        )?;
        output.write_vint(infos.len() as i32)?;

        for (_, fi) in &infos.by_number {
            fi.check_consistency()?;

            output.write_string(&fi.name)?;
            output.write_vint(fi.number as i32)?;

            let mut bits = 0u8;
            if fi.has_store_term_vector {
                bits |= STORE_TERM_VECTOR;
            }
            if fi.omit_norms {
                bits |= OMIT_NORMS;
            }
            if fi.has_store_payloads {
                bits |= STORE_PAYLOADS;
            }

            output.write_byte(bits)?;

            output.write_byte(index_options_byte(fi.index_options))?;

            // pack the DV type and has_norms in one byte
            output.write_byte(doc_values_byte(fi.doc_values_type))?;
            output.write_long(fi.dv_gen)?;
            output.write_map_of_strings(&fi.attributes.read().unwrap())?;
            let point_dimension_count = fi.point_dimension_count;
            output.write_vint(point_dimension_count as i32)?;
            if point_dimension_count > 0 {
                output.write_vint(fi.point_num_bytes as i32)?;
            }
        }

        codec_util::write_footer(&mut output)
    }
}
