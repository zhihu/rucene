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
    parse_segment_name, segment_file_name, SegmentInfo, SEGMENT_USE_COMPOUND_NO,
    SEGMENT_USE_COMPOUND_YES,
};
use core::codec::{codec_util, Codec};
use core::search::sort_field::{
    SimpleSortField, Sort, SortField, SortFieldType, SortedNumericSelectorType,
    SortedNumericSortField, SortedSetSelectorType,
};
use core::store::directory::Directory;
use core::store::io::{BufferedChecksumIndexInput, ChecksumIndexInput, DataOutput, IndexInput};
use core::store::IOContext;
use core::util::ID_LENGTH;
use core::util::{VariantValue, Version};
use error::ErrorKind::{CorruptIndex, IllegalArgument, IllegalState};
use error::Result;
use std::sync::Arc;

/// Expert: Controls the format of the `SegmentInfo` (segment metadata file).
pub trait SegmentInfoFormat {
    /// Read `SegmentInfo` data from a directory.
    ///
    /// @param directory: directory to read from
    /// @param segment_name: name of the segment to read
    /// @param segment_id: expected identifier for the segment
    /// @return infos: instance to be populated with data
    fn read<D: Directory, C: Codec>(
        &self,
        directory: &Arc<D>,
        segment_name: &str,
        segment_id: [u8; ID_LENGTH],
        context: &IOContext,
    ) -> Result<SegmentInfo<D, C>>;

    /// Write `SegmentInfo` data.
    ///
    /// The codec must add its SegmentInfo filename(s) to {@code info} before doing i/o.
    /// @throws IOException If an I/O error occurs
    fn write<D: Directory, DW: Directory, C: Codec>(
        &self,
        dir: &Arc<DW>,
        info: &mut SegmentInfo<D, C>,
        io_context: &IOContext,
    ) -> Result<()>;
}

const SI_EXTENSION: &str = "si";
const CODEC_NAME: &str = "Lucene62SegmentInfo";
const VERSION_START: i32 = 0;
const VERSION_MULTI_VALUED_SORT: i32 = 1;
const VERSION_CURRENT: i32 = VERSION_MULTI_VALUED_SORT;

fn read_segment_info_from_index<D: Directory, C: Codec>(
    input: &mut dyn IndexInput,
    dir: &Arc<D>,
    segment: &str,
    id: [u8; ID_LENGTH],
) -> Result<SegmentInfo<D, C>> {
    codec_util::check_index_header(input, CODEC_NAME, VERSION_START, VERSION_CURRENT, &id, "")?;
    let major = input.read_int()?;
    let minor = input.read_int()?;
    let bugfix = input.read_int()?;
    let version = Version::new(major, minor, bugfix)?;
    let doc_count = input.read_int()?;
    if doc_count < 0 {
        bail!(CorruptIndex(format!("invalid docCount: {}", doc_count)));
    }
    let is_compound_file = input.read_byte()? == SEGMENT_USE_COMPOUND_YES;

    let diagnostics = input.read_map_of_strings()?;
    let files = input.read_set_of_strings()?;
    let attributes = input.read_map_of_strings()?;

    let num_sort_fields = input.read_vint()?;
    let mut index_sort = None;
    if num_sort_fields > 0 {
        let mut sort_fields = Vec::with_capacity(num_sort_fields as usize);
        for _ in 0..num_sort_fields {
            let field_name = input.read_string()?;
            let sort_type_id = input.read_vint()?;
            let mut sorted_set_selector = None;
            let mut sorted_numeric_selector = None;
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
                        0 => sorted_set_selector = Some(SortedSetSelectorType::Min),
                        1 => sorted_set_selector = Some(SortedSetSelectorType::Max),
                        2 => sorted_set_selector = Some(SortedSetSelectorType::MiddleMin),
                        3 => sorted_set_selector = Some(SortedSetSelectorType::MiddleMax),
                        _ => {
                            bail!(CorruptIndex(format!(
                                "invalid index SortedSetSelector ID: {}",
                                selector
                            )));
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
                            bail!(CorruptIndex(format!(
                                "invalid index SortedNumericSortField type ID: {}",
                                type_val
                            )));
                        }
                    };
                    let numeric_selector = input.read_byte()?;
                    match numeric_selector {
                        0 => {
                            sorted_numeric_selector = Some(SortedNumericSelectorType::Min);
                        }
                        1 => {
                            sorted_numeric_selector = Some(SortedNumericSelectorType::Max);
                        }
                        _ => {
                            bail!(CorruptIndex(format!(
                                "invalid index SortedNumericSelector ID: {}",
                                numeric_selector
                            )));
                        }
                    }
                    sort_type_tmp
                }
                _ => {
                    bail!(CorruptIndex(format!(
                        "invalid index sort field type ID: {}",
                        sort_type_id
                    )));
                }
            };
            let b = input.read_byte()?;
            let reverse = if b == 0 {
                true
            } else if b == 1 {
                false
            } else {
                bail!(CorruptIndex(format!("invalid index sort reverse: {}", b)));
            };

            // TODO: not support sort by SortedSet field yet
            debug_assert!(sorted_set_selector.is_none());
            let mut sort_field = if let Some(sorted_numeric_selector) = sorted_numeric_selector {
                SortField::SortedNumeric(SortedNumericSortField::new(
                    field_name,
                    sort_type,
                    reverse,
                    sorted_numeric_selector,
                ))
            } else {
                SortField::Simple(SimpleSortField::new(field_name, sort_type, reverse))
            };

            // missing value
            let bv = input.read_byte()?;
            let mut missing_value = None;
            if bv != 0 {
                match sort_type {
                    SortFieldType::String => {
                        unreachable!()
                        //                        if bv == 1 {
                        // missing_value =
                        // Some(SortFieldMissingValue::StringLast);
                        // } else if bv == 2 {
                        // missing_value = Some(SortFieldMissingValue::StringFirst);
                        //                        } else {
                        //                            bail!("invalid missing value flag: {}", bv);
                        //                        }
                    }
                    SortFieldType::Long => {
                        if bv != 1 {
                            bail!(CorruptIndex(format!("invalid missing value flag: {}", bv)));
                        }
                        missing_value = Some(VariantValue::Long(input.read_long()?));
                    }
                    SortFieldType::Int => {
                        if bv != 1 {
                            bail!(CorruptIndex(format!("invalid missing value flag: {}", bv)));
                        }
                        missing_value = Some(VariantValue::Int(input.read_int()?));
                    }
                    SortFieldType::Double => {
                        if bv != 1 {
                            bail!(CorruptIndex(format!("invalid missing value flag: {}", bv)));
                        }
                        missing_value = Some(VariantValue::Double(f64::from_bits(
                            input.read_long()? as u64,
                        )));
                    }
                    SortFieldType::Float => {
                        if bv != 1 {
                            bail!(CorruptIndex(format!("invalid missing value flag: {}", bv)));
                        }
                        missing_value = Some(VariantValue::Float(f32::from_bits(
                            input.read_int()? as u32,
                        )));
                    }
                    _ => {
                        unreachable!();
                    }
                }
            }
            if missing_value.is_some() {
                sort_field.set_missing_value(missing_value);
            }
            sort_fields.push(sort_field);
        }
        index_sort = Some(Sort::new(sort_fields));
    } else if num_sort_fields < 0 {
        bail!(CorruptIndex(format!(
            "Corrupt Index: invalid index sort field count: {}",
            num_sort_fields
        )));
    }

    let mut si = SegmentInfo::new(
        version,
        segment,
        doc_count,
        Arc::clone(&dir),
        is_compound_file,
        None,
        diagnostics,
        id,
        attributes,
        index_sort,
    )?;

    si.set_files(&files)?;

    Ok(si)
}

#[derive(Copy, Clone, Default)]
pub struct Lucene62SegmentInfoFormat;

impl SegmentInfoFormat for Lucene62SegmentInfoFormat {
    fn read<D: Directory, C: Codec>(
        &self,
        directory: &Arc<D>,
        segment_name: &str,
        segment_id: [u8; ID_LENGTH],
        context: &IOContext,
    ) -> Result<SegmentInfo<D, C>> {
        let file_name = segment_file_name(segment_name, "", SI_EXTENSION);
        let original_input = directory.open_input(&file_name, context)?;
        let mut checksum = BufferedChecksumIndexInput::new(original_input);
        let segment_info =
            read_segment_info_from_index(&mut checksum, directory, segment_name, segment_id)?;
        codec_util::validate_footer(&mut checksum)?;
        let digest = checksum.checksum();
        codec_util::check_checksum(&mut checksum, digest)?;
        Ok(segment_info)
    }

    fn write<D: Directory, DW: Directory, C: Codec>(
        &self,
        dir: &Arc<DW>,
        info: &mut SegmentInfo<D, C>,
        io_context: &IOContext,
    ) -> Result<()> {
        let file_name = segment_file_name(&info.name, "", SI_EXTENSION);

        let mut output = dir.create_output(&file_name, io_context)?;
        // Only add the file once we've successfully created it,
        // else IFD assert can trip:
        info.add_file(&file_name)?;

        codec_util::write_index_header(
            &mut output,
            CODEC_NAME,
            VERSION_CURRENT,
            info.get_id(),
            "",
        )?;
        if info.version.major < 5 {
            bail!(IllegalArgument(format!(
                "invalid major version: should be >= 5 but got: {}",
                info.version.major
            )));
        }

        // Write the Lucene version that created this segment
        output.write_int(info.version.major)?;
        output.write_int(info.version.minor)?;
        output.write_int(info.version.bugfix)?;
        debug_assert_eq!(info.version.prerelease, 0);
        output.write_int(info.max_doc())?;

        let is_compound = if info.is_compound_file() {
            SEGMENT_USE_COMPOUND_YES
        } else {
            SEGMENT_USE_COMPOUND_NO
        };
        output.write_byte(is_compound)?;
        output.write_map_of_strings(&info.diagnostics)?;
        for file in info.files() {
            if parse_segment_name(file) != info.name {
                bail!(IllegalArgument(format!(
                    "invalid files: expected segment={}, got={}",
                    &info.name, file
                )));
            }
        }
        output.write_set_of_strings(&info.files())?;
        output.write_map_of_strings(&info.attributes)?;
        if let Some(sort) = info.index_sort() {
            output.write_vint(sort.get_sort().len() as i32)?;
            for sort_field in sort.get_sort() {
                output.write_string(sort_field.field())?;
                let type_id = match sort_field.field_type() {
                    SortFieldType::String => 0,
                    SortFieldType::Long => 1,
                    SortFieldType::Int => 2,
                    SortFieldType::Double => 3,
                    SortFieldType::Float => 4,
                    SortFieldType::Custom => {
                        match sort_field {
                            // SortField::SortedSet(_) => 5,
                            SortField::SortedNumeric(_) => 6,
                            _ => {
                                bail!(IllegalState("Unexpected SortedNumericSortField".into()));
                            }
                        }
                    }
                    _ => {
                        bail!(IllegalState(format!(
                            "Unexpected sort type: {:?}",
                            sort_field.field()
                        )));
                    }
                };
                output.write_vint(type_id)?;
                if type_id == 5 {
                    unimplemented!();
                } else if type_id == 6 {
                    if let SortField::SortedNumeric(snsf) = sort_field {
                        let v = match snsf.numeric_type() {
                            SortFieldType::Long => 0,
                            SortFieldType::Int => 1,
                            SortFieldType::Double => 2,
                            SortFieldType::Float => 3,
                            _ => unreachable!(),
                        };
                        output.write_byte(v)?;
                        let select_value = match snsf.selector() {
                            SortedNumericSelectorType::Min => 0,
                            SortedNumericSelectorType::Max => 1,
                        };
                        output.write_byte(select_value)?;
                    }
                }
                let reverse = if sort_field.is_reverse() { 0 } else { 1 };
                output.write_byte(reverse)?;

                // write missing value
                if let Some(missing_value) = sort_field.missing_value() {
                    match missing_value {
                        VariantValue::Long(l) => {
                            debug_assert_eq!(sort_field.field_type(), SortFieldType::Long);
                            output.write_byte(1)?;
                            output.write_long(*l)?;
                        }
                        VariantValue::Int(i) => {
                            debug_assert_eq!(sort_field.field_type(), SortFieldType::Int);
                            output.write_byte(1)?;
                            output.write_int(*i)?;
                        }
                        VariantValue::Double(d) => {
                            debug_assert_eq!(sort_field.field_type(), SortFieldType::Double);
                            output.write_byte(1)?;
                            output.write_long((*d).to_bits() as i64)?;
                        }
                        VariantValue::Float(f) => {
                            debug_assert_eq!(sort_field.field_type(), SortFieldType::Float);
                            output.write_byte(1)?;
                            output.write_int((*f).to_bits() as i32)?;
                        }
                        VariantValue::VString(_) => {
                            unimplemented!();
                        }
                        _ => {
                            unreachable!();
                        }
                    }
                } else {
                    output.write_byte(0)?;
                }
            }
        } else {
            output.write_vint(0)?;
        }

        codec_util::write_footer(&mut output)
    }
}
