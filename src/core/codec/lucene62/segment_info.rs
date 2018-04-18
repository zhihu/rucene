use core::codec::codec_util;
use core::codec::format::*;
use core::index::{segment_file_name, SegmentInfo, SEGMENT_INFO_YES};
use core::store::DirectoryRc;
use core::store::{BufferedChecksumIndexInput, ChecksumIndexInput, IOContext, IndexInput};
use core::util::Version;
use error::Result;
use std::sync::Arc;

const SI_EXTENSION: &str = "si";
const CODEC_NAME: &str = "Lucene62SegmentInfo";
const VERSION_START: i32 = 0;
const VERSION_MULTI_VALUED_SORT: i32 = 1;
const VERSION_CURRENT: i32 = VERSION_MULTI_VALUED_SORT;

// TODO should merge to SortField/Sort by zhoufeng
enum SortFieldType {
    String,
    Long,
    Int,
    Double,
    Float,
}

enum SortedSetSelectorType {
    Min,
    Max,
    MiddleMin,
    MiddleMax,
}

enum SortedNumericSelectorType {
    Min,
    Max,
}

enum SortFieldMissingValue {
    StringLast,
    StringFirst,
}

fn read_segment_info_from_index(
    input: &mut IndexInput,
    dir: &DirectoryRc,
    segment: &str,
    id: &[u8],
) -> Result<SegmentInfo> {
    codec_util::check_index_header(input, CODEC_NAME, VERSION_START, VERSION_CURRENT, id, "")?;
    let major = input.read_int()?;
    let minor = input.read_int()?;
    let bugfix = input.read_int()?;
    let version = Version::new(major, minor, bugfix)?;
    let doc_count = input.read_int()?;
    if doc_count < 0 {
        bail!("Corrupt Index: invalid docCount: {}", doc_count);
    }
    let is_compound_file = i32::from(input.read_byte()?) == SEGMENT_INFO_YES;

    let diagnostics = input.read_map_of_strings()?;
    let _files = input.read_set_of_strings()?;
    let attributes = input.read_map_of_strings()?;

    let num_sort_fields = input.read_vint()?;
    if num_sort_fields > 0 {
        // bail!("Sorted fields feature is not supported yet")
        // let mut sort_fields = Vec::with_capacity(num_sort_fields as usize);
        for _ in 0..num_sort_fields {
            let _ = input.read_string()?;
            let sort_type_id = input.read_vint()?;
            let mut _sorted_set_selector = None;
            let mut _sorted_numeric_selector = None;
            let sort_type = match sort_type_id {
                0 => SortFieldType::String,
                1 => SortFieldType::Long,
                2 => SortFieldType::Int,
                3 => SortFieldType::Double,
                4 => SortFieldType::Float,
                5 => {
                    // string
                    let selector = input.read_byte()?;
                    match selector {
                        0 => _sorted_set_selector = Some(SortedSetSelectorType::Min),
                        1 => _sorted_set_selector = Some(SortedSetSelectorType::Max),
                        2 => _sorted_set_selector = Some(SortedSetSelectorType::MiddleMin),
                        3 => _sorted_set_selector = Some(SortedSetSelectorType::MiddleMax),
                        _ => {
                            bail!("invalid index SortedSetSelector ID: {}", selector);
                        }
                    }
                    SortFieldType::String
                }
                6 => {
                    let type_val = input.read_byte()?;
                    let sort_type_tmp = match type_val {
                        0 => SortFieldType::Long,
                        1 => SortFieldType::Int,
                        2 => SortFieldType::Double,
                        3 => SortFieldType::Float,
                        _ => {
                            bail!("invalid index SortedNumericSortField type ID: {}", type_val);
                        }
                    };
                    let numeric_selector = input.read_byte()?;
                    match numeric_selector {
                        0 => {
                            _sorted_numeric_selector = Some(SortedNumericSelectorType::Min);
                        }
                        1 => {
                            _sorted_numeric_selector = Some(SortedNumericSelectorType::Max);
                        }
                        _ => {
                            bail!(
                                "invalid index SortedNumericSelector ID: {}",
                                numeric_selector
                            );
                        }
                    }
                    sort_type_tmp
                }
                _ => {
                    bail!("invalid index sort field type ID: {}", sort_type_id);
                }
            };
            let b = input.read_byte()?;
            if b != 0 && b != 1 {
                bail!("invalid index sort reverse: {}", b);
            }

            // if sorted_set_selector.is_some() {} else if sorted_numeric_selector.is_some() {}
            // else {}

            // missing value
            let bv = input.read_byte()?;
            let mut missing_value = None;
            if bv != 0 {
                match sort_type {
                    SortFieldType::String => {
                        if bv == 1 {
                            missing_value = Some(SortFieldMissingValue::StringLast);
                        } else if bv == 2 {
                            missing_value = Some(SortFieldMissingValue::StringFirst);
                        } else {
                            bail!("invalid missing value flag: {}", bv);
                        }
                    }
                    SortFieldType::Long => {
                        if bv != 1 {
                            bail!("invalid missing value flag: {}", bv);
                        }
                        let _ = input.read_long()?;
                        // missing_value = Some(input.read_long()?);
                    }
                    SortFieldType::Int => {
                        if bv != 1 {
                            bail!("invalid missing value flag: {}", bv);
                        }
                        let _ = input.read_int()?;
                        // missing_value = Some(input.read_int()?);
                    }
                    SortFieldType::Double => {
                        if bv != 1 {
                            bail!("invalid missing value flag: {}", bv);
                        }
                        let _ = input.read_long()?;
                        // missing_value = Some(f64::from_bits(input.read_long()? as u64));
                    }
                    SortFieldType::Float => {
                        if bv != 1 {
                            bail!("invalid missing value flag: {}", bv);
                        }
                        let _ = input.read_int()?;
                        // missing_value = Some(f32::from_bits(input.read_int()? as u32));
                    }
                }
            }
            if missing_value.is_some() {
                // set missing value to sort field here
            }
        }
    // index_sort = Sort::new(...);
    } else if num_sort_fields < 0 {
        bail!(
            "Corrupt Index: invalid index sort field count: {}",
            num_sort_fields
        );
    }

    SegmentInfo::new(
        version,
        segment,
        doc_count,
        Arc::clone(&dir),
        is_compound_file,
        diagnostics,
        id,
        attributes,
    )
}

pub struct Lucene62SegmentInfoFormat;

impl Default for Lucene62SegmentInfoFormat {
    fn default() -> Lucene62SegmentInfoFormat {
        Lucene62SegmentInfoFormat {}
    }
}

impl SegmentInfoFormat for Lucene62SegmentInfoFormat {
    fn read(
        &self,
        directory: DirectoryRc,
        segment_name: &str,
        segment_id: &[u8],
        context: &IOContext,
    ) -> Result<SegmentInfo> {
        let file_name = segment_file_name(segment_name, "", SI_EXTENSION);
        let original_input = directory.open_input(&file_name, context)?;
        let mut checksum = BufferedChecksumIndexInput::new(original_input);
        let segment_info =
            read_segment_info_from_index(&mut checksum, &directory, segment_name, segment_id)?;
        codec_util::validate_footer(&mut checksum)?;
        let digest = checksum.checksum();
        codec_util::check_checksum(&mut checksum, digest)?;
        Ok(segment_info)
    }

    fn write(&self, _dir: DirectoryRc, _info: &SegmentInfo, _io_context: &IOContext) -> Result<()> {
        unimplemented!()
    }
}
