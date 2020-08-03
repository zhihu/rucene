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

use core::codec::doc_values::lucene54::*;
use core::codec::doc_values::{
    BinaryDocValuesProvider, DocValuesProducer, NumericDocValuesProvider, SortedDocValuesProvider,
    SortedNumericDocValuesProvider, SortedSetDocValuesProvider,
};
use core::codec::field_infos::{FieldInfo, FieldInfos};
use core::codec::segment_infos::{segment_file_name, SegmentInfo, SegmentReadState};
use core::codec::{codec_util, Codec};
use core::doc::DocValuesType;
use core::store::directory::Directory;
use core::store::io::{BufferedChecksumIndexInput, IndexInput};
use core::util::{
    packed::{
        DirectMonotonicReader, DirectReader, MixinMonotonicLongValues, MonotonicBlockPackedReader,
    },
    BitsMut, LiveBits, MatchAllBits, MatchNoBits, PagedBytes, PagedBytesReader, SparseBits,
};

use error::ErrorKind::{CorruptIndex, IllegalArgument};
use error::Result;
use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub struct Lucene54DocValuesProducer {
    #[allow(dead_code)]
    num_fields: i32,
    max_doc: i32,
    data: Box<dyn IndexInput>,
    merging: bool,
    numerics: HashMap<String, Arc<NumericEntry>>,
    binaries: HashMap<String, BinaryEntry>,
    sorted_sets: HashMap<String, SortedSetEntry>,
    sorted_numerics: HashMap<String, SortedSetEntry>,
    ords: HashMap<String, Arc<NumericEntry>>,
    ord_indexes: HashMap<String, Arc<NumericEntry>>,
    address_instances: RwLock<HashMap<String, Arc<MonotonicBlockPackedReader>>>,
    reverse_index_instances: RwLock<HashMap<String, Arc<ReverseTermsIndex>>>,
}

impl Lucene54DocValuesProducer {
    pub fn new<D: Directory, DW: Directory, C: Codec>(
        state: &SegmentReadState<'_, D, DW, C>,
        data_codec: &str,
        data_ext: &str,
        meta_codec: &str,
        meta_ext: &str,
    ) -> Result<Lucene54DocValuesProducer> {
        let meta_name =
            segment_file_name(&state.segment_info.name, &state.segment_suffix, meta_ext);
        // read in the entries from the metadata file
        let input = state.directory.open_input(&meta_name, state.context)?;
        let mut checksum_input = BufferedChecksumIndexInput::new(input);

        let version = codec_util::check_index_header(
            &mut checksum_input,
            meta_codec,
            Lucene54DocValuesFormat::VERSION_START,
            Lucene54DocValuesFormat::VERSION_CURRENT,
            state.segment_info.get_id(),
            &state.segment_suffix,
        )?;

        let mut numerics = HashMap::new();
        let mut binaries = HashMap::new();
        let mut sorted_sets = HashMap::new();
        let mut sorted_numerics = HashMap::new();
        let mut ords = HashMap::new();
        let mut ord_indexes = HashMap::new();

        let num_fields: i32 = Lucene54DocValuesProducer::read_fields(
            &mut checksum_input,
            &state.field_infos,
            &state.segment_info,
            &mut numerics,
            &mut binaries,
            &mut sorted_sets,
            &mut sorted_numerics,
            &mut ords,
            &mut ord_indexes,
        )?;

        codec_util::check_footer(&mut checksum_input)?;
        let data_name =
            segment_file_name(&state.segment_info.name, &state.segment_suffix, data_ext);
        let mut data = state.directory.open_input(&data_name, state.context)?;
        let version2 = codec_util::check_index_header(
            data.as_mut(),
            data_codec,
            Lucene54DocValuesFormat::VERSION_START,
            Lucene54DocValuesFormat::VERSION_CURRENT,
            state.segment_info.get_id(),
            &state.segment_suffix,
        )?;

        if version != version2 {
            bail!(CorruptIndex(format!(
                "Format versions mismatch: meta={}, data={}",
                version, version2
            )));
        }
        codec_util::retrieve_checksum(data.as_mut())?;
        let address_instances = RwLock::new(HashMap::new());
        let reverse_index_instances = RwLock::new(HashMap::new());

        Ok(Lucene54DocValuesProducer {
            num_fields,
            max_doc: state.segment_info.max_doc(),
            data,
            merging: false,
            numerics,
            binaries,
            sorted_sets,
            sorted_numerics,
            ords,
            ord_indexes,
            address_instances,
            reverse_index_instances,
        })
    }

    pub fn copy_from(producer: &Lucene54DocValuesProducer) -> Result<Lucene54DocValuesProducer> {
        let address_instances = RwLock::new(producer.address_instances.read()?.clone());
        let reverse_index_instances = RwLock::new(producer.reverse_index_instances.read()?.clone());
        Ok(Lucene54DocValuesProducer {
            num_fields: producer.num_fields,
            max_doc: producer.max_doc,
            data: producer.data.clone()?,
            merging: true,
            numerics: producer.numerics.clone(),
            binaries: producer.binaries.clone(),
            sorted_sets: producer.sorted_sets.clone(),
            sorted_numerics: producer.sorted_numerics.clone(),
            ords: producer.ords.clone(),
            ord_indexes: producer.ord_indexes.clone(),
            address_instances,
            reverse_index_instances,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn read_fields<D: Directory, C: Codec>(
        meta: &mut dyn IndexInput,
        infos: &FieldInfos,
        segment_info: &SegmentInfo<D, C>,
        numerics: &mut HashMap<String, Arc<NumericEntry>>,
        binaries: &mut HashMap<String, BinaryEntry>,
        sorted_sets: &mut HashMap<String, SortedSetEntry>,
        sorted_numerics: &mut HashMap<String, SortedSetEntry>,
        ords: &mut HashMap<String, Arc<NumericEntry>>,
        ord_indexes: &mut HashMap<String, Arc<NumericEntry>>,
    ) -> Result<i32> {
        let mut num_fields = 0;
        let mut field_number = meta.read_vint()?;
        while field_number != -1 {
            num_fields += 1;
            let info = infos
                .field_info_by_number(field_number as u32)
                .ok_or_else(|| {
                    IllegalArgument(format!("invalid field number: {}", field_number))
                })?;
            let dv_type = meta.read_byte()?;
            match dv_type {
                Lucene54DocValuesFormat::NUMERIC => {
                    let entry =
                        Lucene54DocValuesProducer::read_numeric_entry(info, segment_info, meta)?;
                    match entry {
                        Some(n) => numerics.insert(info.name.clone(), n),
                        _ => unreachable!(),
                    };
                }

                Lucene54DocValuesFormat::BINARY => {
                    let b = Lucene54DocValuesProducer::read_binary_entry(info, meta)?;
                    binaries.insert(info.name.clone(), b);
                }

                Lucene54DocValuesFormat::SORTED => {
                    Lucene54DocValuesProducer::read_sorted_field(
                        info,
                        segment_info,
                        meta,
                        binaries,
                        ords,
                    )?;
                }

                Lucene54DocValuesFormat::SORTED_SET => {
                    let ss = Lucene54DocValuesProducer::read_sorted_set_entry(meta)?;
                    let ss_fmt = ss.format;
                    sorted_sets.insert(info.name.clone(), ss);
                    match ss_fmt {
                        Lucene54DocValuesFormat::SORTED_WITH_ADDRESSES => {
                            Lucene54DocValuesProducer::read_sorted_set_field_with_addresses(
                                info,
                                segment_info,
                                meta,
                                binaries,
                                ords,
                                ord_indexes,
                            )?;
                        }

                        Lucene54DocValuesFormat::SORTED_SET_TABLE => {
                            Lucene54DocValuesProducer::read_sorted_set_field_with_table(
                                info,
                                segment_info,
                                meta,
                                binaries,
                                ords,
                            )?;
                        }

                        Lucene54DocValuesFormat::SORTED_SINGLE_VALUED => {
                            if meta.read_vint()? != field_number {
                                bail!(CorruptIndex(format!(
                                    "sorted_set entry for field {} is corrupt",
                                    info.name
                                )));
                            }

                            if meta.read_byte()? != Lucene54DocValuesFormat::SORTED {
                                bail!(CorruptIndex(format!(
                                    "sorted_set entry for field {} is corrupt",
                                    info.name
                                )));
                            }

                            Lucene54DocValuesProducer::read_sorted_field(
                                info,
                                segment_info,
                                meta,
                                binaries,
                                ords,
                            )?;
                        }

                        _ => {
                            unreachable!();
                        }
                    }
                }

                Lucene54DocValuesFormat::SORTED_NUMERIC => {
                    let ss = Lucene54DocValuesProducer::read_sorted_set_entry(meta)?;
                    let ss_fmt = ss.format;
                    sorted_numerics.insert(info.name.clone(), ss);
                    match ss_fmt {
                        Lucene54DocValuesFormat::SORTED_WITH_ADDRESSES => {
                            if meta.read_vint()? != field_number {
                                bail!(CorruptIndex(format!(
                                    "sorted_numeric entry for field {} is corrupt",
                                    info.name
                                )));
                            }
                            if meta.read_byte()? != Lucene54DocValuesFormat::NUMERIC {
                                bail!(CorruptIndex(format!(
                                    "sorted_numeric entry for field {} is corrupt",
                                    info.name
                                )));
                            }
                            let entry = Lucene54DocValuesProducer::read_numeric_entry(
                                info,
                                segment_info,
                                meta,
                            )?;

                            match entry {
                                Some(n) => numerics.insert(info.name.clone(), n),
                                _ => unreachable!(),
                            };

                            if meta.read_vint()? != field_number {
                                bail!(CorruptIndex(format!(
                                    "sorted_numeric entry for field {} is corrupt",
                                    info.name
                                )));
                            }

                            if meta.read_byte()? != Lucene54DocValuesFormat::NUMERIC {
                                bail!(CorruptIndex(format!(
                                    "sorted_numeric entry for field {} is corrupt",
                                    info.name
                                )));
                            }

                            let entry = Lucene54DocValuesProducer::read_numeric_entry(
                                info,
                                segment_info,
                                meta,
                            )?;
                            match entry {
                                Some(n) => ord_indexes.insert(info.name.clone(), n),
                                _ => unreachable!(),
                            };
                        }

                        Lucene54DocValuesFormat::SORTED_SET_TABLE => {
                            if meta.read_vint()? != field_number {
                                bail!(CorruptIndex(format!(
                                    "sorted_numeric entry for field {} is corrupt",
                                    info.name
                                )));
                            }
                            if meta.read_byte()? != Lucene54DocValuesFormat::NUMERIC {
                                bail!(CorruptIndex(format!(
                                    "sorted_numeric entry for field {} is corrupt",
                                    info.name
                                )));
                            }

                            let entry = Lucene54DocValuesProducer::read_numeric_entry(
                                info,
                                segment_info,
                                meta,
                            )?;
                            match entry {
                                Some(n) => ords.insert(info.name.clone(), n),
                                _ => unreachable!(),
                            };
                        }

                        Lucene54DocValuesFormat::SORTED_SINGLE_VALUED => {
                            if meta.read_vint()? != field_number {
                                bail!(CorruptIndex(format!(
                                    "sorted_numeric entry for field {} is corrupt",
                                    info.name
                                )));
                            }
                            if meta.read_byte()? != Lucene54DocValuesFormat::NUMERIC {
                                bail!(CorruptIndex(format!(
                                    "sorted_numeric entry for field {} is corrupt",
                                    info.name
                                )));
                            }
                            if let Some(n) = Lucene54DocValuesProducer::read_numeric_entry(
                                info,
                                segment_info,
                                meta,
                            )? {
                                numerics.insert(info.name.clone(), n);
                            } else {
                                unreachable!();
                            }
                        }
                        _ => {
                            bail!(CorruptIndex(format!(
                                "unknown sorted_set format: {}",
                                ss_fmt
                            )));
                        }
                    }
                }

                _ => {
                    bail!(CorruptIndex(format!("invalid doc value type: {}", dv_type)));
                }
            }
            field_number = meta.read_vint()?;
        }
        Ok(num_fields)
    }
}

impl Lucene54DocValuesProducer {
    fn read_numeric_entry<D: Directory, C: Codec>(
        info: &FieldInfo,
        segment_info: &SegmentInfo<D, C>,
        meta: &mut dyn IndexInput,
    ) -> Result<Option<Arc<NumericEntry>>> {
        let mut entry = NumericEntry::new();
        entry.format = meta.read_vint()?;
        entry.missing_offset = meta.read_long()?;
        if entry.format == Lucene54DocValuesFormat::SPARSE_COMPRESSED {
            entry.num_docs_with_value = meta.read_vlong()?;
            let block_shift = meta.read_vint()?;
            let monotonic_meta = Arc::new(DirectMonotonicReader::load_meta(
                meta,
                entry.num_docs_with_value,
                block_shift,
            )?);
            entry.monotonic_meta = Some(Arc::clone(&monotonic_meta));
        }
        entry.offset = meta.read_long()?;
        entry.count = meta.read_vlong()?;

        match entry.format {
            Lucene54DocValuesFormat::CONST_COMPRESSED => {
                entry.min_value = meta.read_long()?;
                if entry.count > i64::from(::std::i32::MAX) {
                    bail!(CorruptIndex(format!(
                        "illegal CONST_COMPRESSED count: {}",
                        entry.count
                    )));
                }
            }
            Lucene54DocValuesFormat::GCD_COMPRESSED => {
                entry.min_value = meta.read_long()?;
                entry.gcd = meta.read_long()?;
                entry.bits_per_value = meta.read_vint()?;
            }
            Lucene54DocValuesFormat::TABLE_COMPRESSED => {
                let uniq_values = meta.read_vint()? as usize;
                if uniq_values > 256 {
                    bail!(CorruptIndex(format!(
                        "TABLE_COMPRESSED can't have more than 256 distinct values, got={}",
                        uniq_values
                    )));
                }
                entry.table.resize(uniq_values, 0);
                for i in 0..uniq_values {
                    entry.table[i] = meta.read_long()?;
                }
                entry.bits_per_value = meta.read_vint()?;
            }
            Lucene54DocValuesFormat::DELTA_COMPRESSED => {
                entry.min_value = meta.read_long()?;
                entry.bits_per_value = meta.read_vint()?;
            }
            Lucene54DocValuesFormat::MONOTONIC_COMPRESSED => {
                let block_shift = meta.read_vint()?;
                let monotonic_meta = Arc::new(DirectMonotonicReader::load_meta(
                    meta,
                    i64::from(segment_info.max_doc + 1),
                    block_shift,
                )?);
                entry.monotonic_meta = Some(monotonic_meta);
            }
            Lucene54DocValuesFormat::SPARSE_COMPRESSED => {
                let number_type = meta.read_byte()?;
                match number_type {
                    0 => {
                        entry.number_type = NumberType::VALUE;
                    }
                    1 => {
                        entry.number_type = NumberType::ORDINAL;
                    }
                    _ => {
                        bail!(CorruptIndex(format!(
                            "Number type can only be 0 or 1, got={}",
                            number_type
                        )));
                    }
                }
                let field_number = meta.read_vint()?;
                if field_number != info.number as i32 {
                    bail!(CorruptIndex(format!(
                        "Field number mismatch: {} != {}",
                        field_number, info.number
                    )));
                }
                let dv_format = meta.read_byte()?;
                if dv_format != Lucene54DocValuesFormat::NUMERIC {
                    bail!(CorruptIndex(format!(
                        "Format mismatch: {} != {}",
                        dv_format,
                        Lucene54DocValuesFormat::NUMERIC
                    )));
                }
                // NOTE: Better way to handle the list?
                entry.non_missing_values =
                    Lucene54DocValuesProducer::read_numeric_entry(info, segment_info, meta)?;
            }
            _ => {
                bail!(CorruptIndex(format!("unknown format: {}", entry.format)));
            }
        }
        entry.end_offset = meta.read_long()?;
        Ok(Some(Arc::new(entry)))
    }

    fn read_binary_entry(_info: &FieldInfo, meta: &mut dyn IndexInput) -> Result<BinaryEntry> {
        let mut entry = BinaryEntry::default();
        entry.format = meta.read_vint()?;
        entry.missing_offset = meta.read_long()?;
        entry.min_length = meta.read_vint()?;
        entry.max_length = meta.read_vint()?;
        entry.count = meta.read_vlong()?;
        entry.offset = meta.read_long()?;
        match entry.format {
            Lucene54DocValuesFormat::BINARY_FIXED_UNCOMPRESSED => {}
            Lucene54DocValuesFormat::BINARY_PREFIX_COMPRESSED => {
                entry.addresses_offset = meta.read_long()?;
                entry.packed_ints_version = meta.read_vint()?;
                entry.block_size = meta.read_vint()?;
                entry.reverse_index_offset = meta.read_long()?;
            }
            Lucene54DocValuesFormat::BINARY_VARIABLE_UNCOMPRESSED => {
                entry.addresses_offset = meta.read_long()?;
                let block_shift = meta.read_vint()?;
                let addresses_meta = Arc::new(DirectMonotonicReader::load_meta(
                    meta,
                    entry.count + 1,
                    block_shift,
                )?);
                entry.addresses_meta = Some(Arc::clone(&addresses_meta));
                entry.addresses_end_offset = meta.read_long()?;
            }
            _ => {
                bail!(CorruptIndex(format!("unknown format: {}", entry.format)));
            }
        }
        Ok(entry)
    }

    fn read_sorted_set_entry(meta: &mut dyn IndexInput) -> Result<SortedSetEntry> {
        let mut entry = SortedSetEntry::new();
        entry.format = meta.read_vint()?;
        match entry.format {
            Lucene54DocValuesFormat::SORTED_SET_TABLE => {
                let total_table_length = meta.read_int()? as usize;
                if total_table_length > 256 {
                    bail!(CorruptIndex(format!(
                        "SORTED_SET_TABLE cannot have more than 256 values in its dictionary, \
                         got={}",
                        total_table_length
                    )));
                }
                entry.table.resize(total_table_length, 0);
                for i in 0..total_table_length {
                    entry.table[i] = meta.read_long()?;
                }
                let table_size = meta.read_int()? as usize;
                if table_size > total_table_length + 1 {
                    // +1 because of the empty set
                    bail!(CorruptIndex(format!(
                        "SORTED_SET_TABLE cannot have more set ids than ords in its dictionary, \
                         got {} ords and {} sets",
                        total_table_length, table_size
                    )));
                }
                let table_offsets_length = table_size + 1;
                entry.table_offsets.resize(table_offsets_length, 0);
                for i in 1..table_offsets_length {
                    entry.table_offsets[i] = entry.table_offsets[i - 1] + meta.read_int()?;
                }
            }

            Lucene54DocValuesFormat::SORTED_SINGLE_VALUED => {
                // do nothing
            }

            Lucene54DocValuesFormat::SORTED_WITH_ADDRESSES => {
                // do nothing
            }
            _ => {
                bail!(CorruptIndex(format!("unknown format: {}", entry.format)));
            }
        }
        Ok(entry)
    }

    fn read_sorted_field<D: Directory, C: Codec>(
        info: &FieldInfo,
        segment_info: &SegmentInfo<D, C>,
        meta: &mut dyn IndexInput,
        binaries: &mut HashMap<String, BinaryEntry>,
        ords: &mut HashMap<String, Arc<NumericEntry>>,
    ) -> Result<()> {
        // sorted = binary + numeric
        if meta.read_vint()? != info.number as i32 {
            bail!(CorruptIndex(format!(
                "sorted entry for field {} is corrupt",
                info.name
            )));
        }

        if meta.read_byte()? != Lucene54DocValuesFormat::BINARY {
            bail!(CorruptIndex(format!(
                "sorted entry for field {} is corrupt",
                info.name
            )));
        }

        let b = Lucene54DocValuesProducer::read_binary_entry(info, meta)?;
        binaries.insert(info.name.clone(), b);
        if meta.read_vint()? != info.number as i32 {
            bail!(CorruptIndex(format!(
                "sorted entry for field {} is corrupt",
                info.name
            )));
        }

        if meta.read_byte()? != Lucene54DocValuesFormat::NUMERIC {
            bail!(CorruptIndex(format!(
                "sorted entry for field {} is corrupt",
                info.name
            )));
        }

        if let Some(n) = Lucene54DocValuesProducer::read_numeric_entry(info, segment_info, meta)? {
            ords.insert(info.name.clone(), n);
        } else {
            unreachable!();
        }
        Ok(())
    }

    fn read_sorted_set_field_with_addresses<D: Directory, C: Codec>(
        info: &FieldInfo,
        segment_info: &SegmentInfo<D, C>,
        meta: &mut dyn IndexInput,
        binaries: &mut HashMap<String, BinaryEntry>,
        ords: &mut HashMap<String, Arc<NumericEntry>>,
        ord_indexes: &mut HashMap<String, Arc<NumericEntry>>,
    ) -> Result<()> {
        // sorted_set = binary + numeric (addresses) + ord_index
        if meta.read_vint()? != info.number as i32 {
            bail!(CorruptIndex(format!(
                "sorted_set entry for field {} is corrupt",
                info.name
            )));
        }

        if meta.read_byte()? != Lucene54DocValuesFormat::BINARY {
            bail!(CorruptIndex(format!(
                "sorted_set entry for field {} is corrupt",
                info.name
            )));
        }

        let b = Lucene54DocValuesProducer::read_binary_entry(info, meta)?;
        binaries.insert(info.name.clone(), b);

        if meta.read_vint()? != info.number as i32 {
            bail!(CorruptIndex(format!(
                "sorted_set entry for field {} is corrupt",
                info.name
            )));
        }

        if meta.read_byte()? != Lucene54DocValuesFormat::NUMERIC {
            bail!(CorruptIndex(format!(
                "sorted_set entry for field {} is corrupt",
                info.name
            )));
        }

        if let Some(n1) = Lucene54DocValuesProducer::read_numeric_entry(info, segment_info, meta)? {
            ords.insert(info.name.clone(), n1);
        } else {
            unreachable!();
        }

        if meta.read_vint()? != info.number as i32 {
            bail!(CorruptIndex(format!(
                "sorted_set entry for field {} is corrupt",
                info.name
            )));
        }

        if meta.read_byte()? != Lucene54DocValuesFormat::NUMERIC {
            bail!(CorruptIndex(format!(
                "sorted_set entry for field {} is corrupt",
                info.name
            )));
        }

        if let Some(n2) = Lucene54DocValuesProducer::read_numeric_entry(info, segment_info, meta)? {
            ord_indexes.insert(info.name.clone(), n2);
        } else {
            unreachable!();
        }
        Ok(())
    }

    fn read_sorted_set_field_with_table<D: Directory, C: Codec>(
        info: &FieldInfo,
        segment_info: &SegmentInfo<D, C>,
        meta: &mut dyn IndexInput,
        binaries: &mut HashMap<String, BinaryEntry>,
        ords: &mut HashMap<String, Arc<NumericEntry>>,
    ) -> Result<()> {
        // sorted_set_table = binary + ord_set table + ordset index
        if meta.read_vint()? != info.number as i32 {
            bail!(CorruptIndex(format!(
                "sorted_set entry for field {} is corrupt",
                info.name
            )));
        }
        if meta.read_byte()? != Lucene54DocValuesFormat::BINARY {
            bail!(CorruptIndex(format!(
                "sorted_set entry for field {} is corrupt",
                info.name
            )));
        }

        let b = Lucene54DocValuesProducer::read_binary_entry(info, meta)?;
        binaries.insert(info.name.clone(), b);

        if meta.read_vint()? != info.number as i32 {
            bail!(CorruptIndex(format!(
                "sorted_set entry for field {} is corrupt",
                info.name
            )));
        }

        if meta.read_byte()? != Lucene54DocValuesFormat::NUMERIC {
            bail!(CorruptIndex(format!(
                "sorted_set entry for field {} is corrupt",
                info.name
            )));
        }

        if let Some(n) = Lucene54DocValuesProducer::read_numeric_entry(info, segment_info, meta)? {
            ords.insert(info.name.clone(), n);
        } else {
            unreachable!();
        }

        Ok(())
    }

    fn get_live_bits(&self, offset: i64, count: usize) -> Result<LiveBitsEnum> {
        Ok(match offset as i32 {
            Lucene54DocValuesFormat::ALL_MISSING => LiveBitsEnum::None(MatchNoBits::new(count)),
            Lucene54DocValuesFormat::ALL_LIVE => LiveBitsEnum::All(MatchAllBits::new(count)),
            _ => {
                let data = self.data.as_ref().clone()?;
                let boxed = LiveBits::new(data.as_ref(), offset, count)?;
                LiveBitsEnum::Bits(boxed)
            }
        })
    }
}

impl Lucene54DocValuesProducer {
    fn get_numeric_const_compressed(&self, entry: &Arc<NumericEntry>) -> Result<LiveLongValues> {
        let constant = entry.min_value;
        let inbox = self.get_live_bits(entry.missing_offset, entry.count as usize)?;
        Ok(LiveLongValues::new(inbox, constant))
    }

    fn get_numeric_delta_compressed(&self, entry: &Arc<NumericEntry>) -> Result<DeltaLongValues> {
        let slice = self
            .data
            .random_access_slice(entry.offset, entry.end_offset - entry.offset)?;
        let slice = Arc::from(slice);
        let delta = entry.min_value;
        let inbox = DirectReader::get_instance(slice, entry.bits_per_value, 0)?;
        Ok(DeltaLongValues::new(inbox, delta))
    }

    fn get_numeric_gcd_compressed(&self, entry: &Arc<NumericEntry>) -> Result<GcdLongValues> {
        let slice = self
            .data
            .random_access_slice(entry.offset, entry.end_offset - entry.offset)?;
        let slice = Arc::from(slice);
        let base = entry.min_value;
        let mult = entry.gcd;
        let inbox = DirectReader::get_instance(slice, entry.bits_per_value, 0)?;
        Ok(GcdLongValues::new(inbox, base, mult))
    }

    fn get_numeric_table_compressed(&self, entry: &Arc<NumericEntry>) -> Result<TableLongValues> {
        let data = self.data.as_ref().clone()?;
        let slice = data.random_access_slice(entry.offset, entry.end_offset - entry.offset)?;
        let slice = Arc::from(slice);
        let table = entry.table.clone();
        let ords = DirectReader::get_instance(slice, entry.bits_per_value, 0)?;
        Ok(TableLongValues::new(ords, table))
    }

    fn get_numeric_sparse_compressed(
        &self,
        entry: &Arc<NumericEntry>,
    ) -> Result<SparseLongValues<MixinMonotonicLongValues>> {
        let docs_with_field = self.get_sparse_live_bits_by_entry(&entry)?;

        debug_assert!(!entry.non_missing_values.is_none());

        let non_missing_values = entry.non_missing_values.as_ref().unwrap();
        let values = self.get_numeric_by_entry(non_missing_values)?;
        let missing_value = match entry.number_type {
            NumberType::ORDINAL => -1_i64,
            NumberType::VALUE => 0_i64,
        };
        Ok(SparseLongValues::new(
            docs_with_field,
            Box::new(values),
            missing_value,
        ))
    }

    fn get_numeric_by_entry(&self, entry: &Arc<NumericEntry>) -> Result<NumericLongValuesEnum> {
        let fmt = entry.format;
        match fmt {
            Lucene54DocValuesFormat::CONST_COMPRESSED => self
                .get_numeric_const_compressed(entry)
                .map(NumericLongValuesEnum::Live),
            Lucene54DocValuesFormat::DELTA_COMPRESSED => self
                .get_numeric_delta_compressed(entry)
                .map(NumericLongValuesEnum::Delta),
            Lucene54DocValuesFormat::GCD_COMPRESSED => self
                .get_numeric_gcd_compressed(entry)
                .map(NumericLongValuesEnum::Gcd),
            Lucene54DocValuesFormat::TABLE_COMPRESSED => self
                .get_numeric_table_compressed(entry)
                .map(NumericLongValuesEnum::Table),
            Lucene54DocValuesFormat::SPARSE_COMPRESSED => self
                .get_numeric_sparse_compressed(entry)
                .map(NumericLongValuesEnum::Sparse),
            _ => bail!(IllegalArgument(format!(
                "Unknown numeric entry format: {}",
                fmt
            ))),
        }
    }

    fn get_sparse_live_bits_by_entry(
        &self,
        entry: &NumericEntry,
    ) -> Result<SparseBits<MixinMonotonicLongValues>> {
        let length = entry.offset - entry.missing_offset;

        let doc_ids_data = self
            .data
            .random_access_slice(entry.missing_offset, length)?;
        let doc_ids_data = Arc::from(doc_ids_data);

        if let Some(ref meta) = entry.monotonic_meta {
            let doc_ids = DirectMonotonicReader::get_instance(meta.as_ref(), &doc_ids_data)?;
            SparseBits::new(i64::from(self.max_doc), entry.num_docs_with_value, doc_ids)
        } else {
            unreachable!();
        }
    }
}

impl Lucene54DocValuesProducer {
    fn get_fixed_binary(
        &self,
        _field: &FieldInfo,
        bytes: &BinaryEntry,
    ) -> Result<FixedBinaryDocValues> {
        let data = self.data.slice(
            "fixed-binary",
            bytes.offset,
            bytes.count * i64::from(bytes.max_length),
        )?;
        let fixed_binary = FixedBinaryDocValues::new(data, bytes.max_length as usize);
        Ok(fixed_binary)
    }

    fn get_variable_binary(
        &self,
        _field: &FieldInfo,
        bytes: &BinaryEntry,
    ) -> Result<VariableBinaryDocValues<MixinMonotonicLongValues>> {
        let addresses_length = bytes.addresses_end_offset - bytes.addresses_offset;
        let meta_ref = bytes
            .addresses_meta
            .as_ref()
            .ok_or_else(|| IllegalArgument("addresses_meta None???".to_owned()))?;
        let meta = Arc::clone(meta_ref);

        let addresses_data = self
            .data
            .random_access_slice(bytes.addresses_offset, addresses_length)?;
        let addresses_data = Arc::from(addresses_data);
        let addresses = DirectMonotonicReader::get_instance(meta.as_ref(), &addresses_data)?;
        let data_length = bytes.addresses_offset - bytes.offset;
        let data = self.data.slice("var-binary", bytes.offset, data_length)?;
        let variable_binary =
            VariableBinaryDocValues::new(addresses, data, bytes.max_length as usize);
        Ok(variable_binary)
    }

    fn get_interval_instance(
        &self,
        field: &FieldInfo,
        bytes: &BinaryEntry,
    ) -> Result<Arc<MonotonicBlockPackedReader>> {
        if let Some(addresses) = self.address_instances.read()?.get(&field.name) {
            return Ok(Arc::clone(addresses));
        }

        let mut data = self.data.as_ref().clone()?;
        data.seek(bytes.addresses_offset)?;
        let size = ((bytes.count + i64::from(Lucene54DocValuesFormat::INTERVAL_MASK))
            >> Lucene54DocValuesFormat::INTERVAL_SHIFT) as usize;
        let addresses = MonotonicBlockPackedReader::new(
            data.as_mut(),
            bytes.packed_ints_version,
            bytes.block_size as usize,
            size,
            false,
        )?;
        let addresses = Arc::new(addresses);
        if !self.merging {
            self.address_instances
                .write()?
                .insert(field.name.clone(), Arc::clone(&addresses));
        }
        Ok(addresses)
    }

    fn get_reverse_index_instance(
        &self,
        field: &FieldInfo,
        bytes: &BinaryEntry,
    ) -> Result<Arc<ReverseTermsIndex>> {
        if let Some(reverse_terms_index) = self.reverse_index_instances.read()?.get(&field.name) {
            return Ok(Arc::clone(reverse_terms_index));
        }

        let mut data = self.data.as_ref().clone()?;
        let data: &mut dyn IndexInput = data.borrow_mut();
        data.seek(bytes.reverse_index_offset)?;
        let size = (bytes.count + i64::from(Lucene54DocValuesFormat::REVERSE_INTERVAL_MASK))
            >> Lucene54DocValuesFormat::REVERSE_INTERVAL_SHIFT;
        let term_addresses = MonotonicBlockPackedReader::new(
            data.borrow_mut(),
            bytes.packed_ints_version,
            bytes.block_size as usize,
            size as usize,
            false,
        )?;
        let data_size = data.read_vlong()?;
        let mut paged_bytes = PagedBytes::new(15);
        paged_bytes.copy(data.borrow_mut(), data_size)?;
        paged_bytes.freeze(true)?;
        let terms = PagedBytesReader::new(paged_bytes);
        let index = ReverseTermsIndex {
            term_addresses,
            terms,
        };
        let index = Arc::new(index);
        if !self.merging {
            self.reverse_index_instances
                .write()?
                .insert(field.name.clone(), Arc::clone(&index));
        }
        Ok(index)
    }

    fn get_compressed_binary(
        &self,
        field: &FieldInfo,
        bytes: &BinaryEntry,
    ) -> Result<CompressedBinaryDocValues> {
        let addresses = self.get_interval_instance(field, &bytes)?;
        let index = self.get_reverse_index_instance(field, &bytes)?;
        debug_assert!(addresses.size() > 0); // we don't have to handle empty case
        let slice =
            self.data
                .slice("terms", bytes.offset, bytes.addresses_offset - bytes.offset)?;
        CompressedBinaryDocValues::new(bytes, addresses, index, slice)
    }
}

impl Lucene54DocValuesProducer {
    fn get_ord_index_instance(
        &self,
        entry: &Arc<NumericEntry>,
    ) -> Result<MixinMonotonicLongValues> {
        let data = self
            .data
            .random_access_slice(entry.offset, entry.end_offset - entry.offset)?;
        if let Some(ref meta) = entry.monotonic_meta {
            DirectMonotonicReader::get_instance(meta.as_ref(), &Arc::from(data))
        } else {
            unreachable!();
        }
    }

    fn get_sorted_set_with_addresses(
        &self,
        field: &FieldInfo,
    ) -> Result<Box<dyn SortedSetDocValuesProvider>> {
        let value_count = self
            .binaries
            .get(&field.name)
            .ok_or_else(|| IllegalArgument(format!("No binary field named {}", field.name)))?
            .count as usize;

        let ordinals = {
            let ord_entry = self
                .ords
                .get(&field.name)
                .ok_or_else(|| IllegalArgument(format!("No ords field named {}", &field.name)))?;

            self.get_numeric_by_entry(ord_entry)?
        };

        let ord_index = {
            let ord_index_entry = self.ord_indexes.get(&field.name).ok_or_else(|| {
                IllegalArgument(format!("No OrdIndex field named {}", field.name))
            })?;
            self.get_ord_index_instance(ord_index_entry)?
        };

        let bytes = self
            .binaries
            .get(&field.name)
            .ok_or_else(|| IllegalArgument(format!("No binary field named {}", field.name)))?
            .clone();

        let my_format = bytes.format;

        match my_format {
            Lucene54DocValuesFormat::BINARY_FIXED_UNCOMPRESSED => {
                let binary =
                    TailoredBoxedBinaryDocValuesEnum::Fixed(self.get_fixed_binary(field, &bytes)?);
                let boxed =
                    AddressedRandomAccessOrds::new(binary, ordinals, ord_index, value_count);
                Ok(Box::new(boxed))
            }
            Lucene54DocValuesFormat::BINARY_VARIABLE_UNCOMPRESSED => {
                let binary = TailoredBoxedBinaryDocValuesEnum::Variable(
                    self.get_variable_binary(field, &bytes)?,
                );
                let boxed =
                    AddressedRandomAccessOrds::new(binary, ordinals, ord_index, value_count);
                Ok(Box::new(boxed))
            }
            Lucene54DocValuesFormat::BINARY_PREFIX_COMPRESSED => {
                let binary = TailoredBoxedBinaryDocValuesEnum::Compressed(
                    self.get_compressed_binary(field, &bytes)?,
                );
                let boxed =
                    AddressedRandomAccessOrds::new(binary, ordinals, ord_index, value_count);
                Ok(Box::new(boxed))
            }
            _ => bail!(IllegalArgument(format!(
                "unknown binary_entry format: {}",
                my_format,
            ))),
        }
    }

    fn get_sorted_set_table(
        &self,
        field: &FieldInfo,
    ) -> Result<Box<dyn SortedSetDocValuesProvider>> {
        let table;
        let table_offsets;
        {
            let ss = self.sorted_sets.get(&field.name).ok_or_else(|| {
                IllegalArgument(format!("No SortedSet field named {}", &field.name))
            })?;

            table = ss.table.clone();
            table_offsets = ss.table_offsets.clone();
        }

        let value_count = self
            .binaries
            .get(&field.name)
            .ok_or_else(|| IllegalArgument(format!("No binary field named {}", field.name)))?
            .count as usize;

        let ordinals = {
            let ord_entry;
            ord_entry = self
                .ords
                .get(&field.name)
                .ok_or_else(|| IllegalArgument(format!("No ords field named {}", &field.name)))?;
            self.get_numeric_by_entry(ord_entry)?
        };

        let bytes = self
            .binaries
            .get(&field.name)
            .ok_or_else(|| IllegalArgument(format!("No binary field named {}", field.name)))?
            .clone();

        match bytes.format {
            Lucene54DocValuesFormat::BINARY_FIXED_UNCOMPRESSED => {
                let binary =
                    TailoredBoxedBinaryDocValuesEnum::Fixed(self.get_fixed_binary(field, &bytes)?);
                let boxed = TabledRandomAccessOrds::new(
                    binary,
                    ordinals,
                    table,
                    table_offsets,
                    value_count,
                );
                Ok(Box::new(boxed))
            }
            Lucene54DocValuesFormat::BINARY_VARIABLE_UNCOMPRESSED => {
                let binary = TailoredBoxedBinaryDocValuesEnum::Variable(
                    self.get_variable_binary(field, &bytes)?,
                );
                let boxed = TabledRandomAccessOrds::new(
                    binary,
                    ordinals,
                    table,
                    table_offsets,
                    value_count,
                );
                Ok(Box::new(boxed))
            }
            Lucene54DocValuesFormat::BINARY_PREFIX_COMPRESSED => {
                let binary = TailoredBoxedBinaryDocValuesEnum::Compressed(
                    self.get_compressed_binary(field, &bytes)?,
                );
                let boxed = TabledRandomAccessOrds::new(
                    binary,
                    ordinals,
                    table,
                    table_offsets,
                    value_count,
                );
                Ok(Box::new(boxed))
            }
            _ => bail!(IllegalArgument(format!(
                "unknown binary_entry format: {}",
                bytes.format
            ))),
        }
    }
}

impl DocValuesProducer for Lucene54DocValuesProducer {
    fn get_numeric(&self, field: &FieldInfo) -> Result<Arc<dyn NumericDocValuesProvider>> {
        let link = self.numerics.get(&field.name).ok_or_else(|| {
            IllegalArgument(format!("No numeric field named {} found", field.name))
        })?;
        let boxed = self.get_numeric_by_entry(link)?;
        Ok(Arc::new(boxed))
    }

    fn get_binary(&self, field: &FieldInfo) -> Result<Arc<dyn BinaryDocValuesProvider>> {
        let bytes = self
            .binaries
            .get(&field.name)
            .ok_or_else(|| IllegalArgument(format!("No binary field named {}", field.name)))?
            .clone();
        let myformat = bytes.format;

        match myformat {
            Lucene54DocValuesFormat::BINARY_FIXED_UNCOMPRESSED => {
                let boxed = self.get_fixed_binary(field, &bytes)?;
                Ok(Arc::new(boxed))
            }
            Lucene54DocValuesFormat::BINARY_VARIABLE_UNCOMPRESSED => {
                let boxed = self.get_variable_binary(field, &bytes)?;
                Ok(Arc::new(boxed))
            }

            Lucene54DocValuesFormat::BINARY_PREFIX_COMPRESSED => {
                let boxed = self.get_compressed_binary(field, &bytes)?;
                Ok(Arc::new(boxed))
            }
            _ => bail!(IllegalArgument(format!(
                "unknown binary_entry format: {}",
                myformat,
            ))),
        }
    }

    fn get_sorted(&self, field: &FieldInfo) -> Result<Arc<dyn SortedDocValuesProvider>> {
        let value_count = self
            .binaries
            .get(&field.name)
            .ok_or_else(|| IllegalArgument(format!("No binary field named {}", &field.name)))?
            .count as usize;
        let ordinals = {
            let entry = self
                .ords
                .get(&field.name)
                .ok_or_else(|| IllegalArgument(format!("No ords field named {}", &field.name)))?;
            self.get_numeric_by_entry(entry)?
        };

        let bytes = self
            .binaries
            .get(&field.name)
            .ok_or_else(|| IllegalArgument(format!("No binary field named {}", field.name)))?
            .clone();

        match bytes.format {
            Lucene54DocValuesFormat::BINARY_FIXED_UNCOMPRESSED => {
                let binary =
                    TailoredBoxedBinaryDocValuesEnum::Fixed(self.get_fixed_binary(field, &bytes)?);
                let boxed = TailoredSortedDocValues::new(ordinals, binary, value_count);
                Ok(Arc::new(boxed))
            }
            Lucene54DocValuesFormat::BINARY_VARIABLE_UNCOMPRESSED => {
                let binary = TailoredBoxedBinaryDocValuesEnum::Variable(
                    self.get_variable_binary(field, &bytes)?,
                );
                let boxed = TailoredSortedDocValues::new(ordinals, binary, value_count);
                Ok(Arc::new(boxed))
            }
            Lucene54DocValuesFormat::BINARY_PREFIX_COMPRESSED => {
                let binary = TailoredBoxedBinaryDocValuesEnum::Compressed(
                    self.get_compressed_binary(field, &bytes)?,
                );
                let boxed = TailoredSortedDocValues::new(ordinals, binary, value_count);
                Ok(Arc::new(boxed))
            }
            _ => bail!(IllegalArgument(format!(
                "unknown binary_entry format: {}",
                bytes.format
            ))),
        }
    }

    fn get_sorted_numeric(
        &self,
        field: &FieldInfo,
    ) -> Result<Arc<dyn SortedNumericDocValuesProvider>> {
        let ss = self.sorted_numerics.get(&field.name).ok_or_else(|| {
            IllegalArgument(format!("No SortedNumeric field named {}", field.name))
        })?;
        match ss.format {
            Lucene54DocValuesFormat::SORTED_SINGLE_VALUED => {
                let numeric_entry = self.numerics.get(&field.name).ok_or_else(|| {
                    IllegalArgument(format!("No Numerics field named {}", field.name))
                })?;
                if numeric_entry.format == Lucene54DocValuesFormat::SPARSE_COMPRESSED {
                    let values = self.get_numeric_sparse_compressed(numeric_entry)?;
                    let docs_with_field = values.docs_with_field_clone();

                    Ok(Arc::new(SingletonSortedNumericDVProvider::new(
                        values,
                        docs_with_field,
                    )))
                } else {
                    let offset = numeric_entry.missing_offset;
                    let count = self.max_doc as usize;

                    let values = self.get_numeric_by_entry(numeric_entry)?;

                    match offset as i32 {
                        Lucene54DocValuesFormat::ALL_MISSING => {
                            let living_room = MatchNoBits::new(count);
                            Ok(Arc::new(SingletonSortedNumericDVProvider::new(
                                values,
                                living_room,
                            )))
                        }
                        Lucene54DocValuesFormat::ALL_LIVE => {
                            let living_room = MatchAllBits::new(count);
                            Ok(Arc::new(SingletonSortedNumericDVProvider::new(
                                values,
                                living_room,
                            )))
                        }
                        _ => {
                            let mut data = self.data.as_ref().clone()?;
                            let living_room = LiveBits::new(data.borrow_mut(), offset, count)?;
                            Ok(Arc::new(SingletonSortedNumericDVProvider::new(
                                values,
                                living_room,
                            )))
                        }
                    }
                }
            }
            Lucene54DocValuesFormat::SORTED_WITH_ADDRESSES => {
                let numeric_entry = self.numerics.get(&field.name).ok_or_else(|| {
                    IllegalArgument(format!("No Numerics field named {}", field.name))
                })?;
                let values = self.get_numeric_by_entry(numeric_entry)?;
                let ord_entry = self.ord_indexes.get(&field.name).ok_or_else(|| {
                    IllegalArgument(format!("no field named {} in ord_indexes", field.name))
                })?;
                let ord_index = self.get_ord_index_instance(ord_entry)?;
                Ok(Arc::new(AddressedSortedNumericDocValues::new(
                    values, ord_index,
                )))
            }
            Lucene54DocValuesFormat::SORTED_SET_TABLE => {
                let numeric_entry = self.ords.get(&field.name).ok_or_else(|| {
                    IllegalArgument(format!("No Ords field named {}", field.name))
                })?;
                let ordinals = self.get_numeric_by_entry(numeric_entry)?;
                Ok(Arc::new(TabledSortedNumericDocValues::new(
                    ordinals,
                    &ss.table,
                    &ss.table_offsets,
                )))
            }
            _ => bail!(IllegalArgument(format!(
                "Unknown format {} of SortedNumeric field {}",
                ss.format, field.name
            ))),
        }
    }

    fn get_sorted_set(&self, field: &FieldInfo) -> Result<Arc<dyn SortedSetDocValuesProvider>> {
        let my_format = self
            .sorted_sets
            .get(&field.name)
            .ok_or_else(|| IllegalArgument(format!("No SortedSet field named {}", &field.name)))?
            .format;

        match my_format {
            Lucene54DocValuesFormat::SORTED_SINGLE_VALUED => {
                let values = self.get_sorted(field)?;
                let boxed = SingletonSortedSetDVProvider::new(values);
                Ok(Arc::new(boxed))
            }

            Lucene54DocValuesFormat::SORTED_WITH_ADDRESSES => {
                Ok(Arc::from(self.get_sorted_set_with_addresses(field)?))
            }
            Lucene54DocValuesFormat::SORTED_SET_TABLE => {
                Ok(Arc::from(self.get_sorted_set_table(field)?))
            }
            _ => bail!(IllegalArgument(format!(
                "Unknown SortedSetEntry.format {} of field {}",
                my_format, field.name
            ))),
        }
    }

    fn get_docs_with_field(&self, field: &FieldInfo) -> Result<Box<dyn BitsMut>> {
        match field.doc_values_type {
            DocValuesType::SortedSet => {
                let dv = self.get_sorted_set(field)?.get()?;
                Ok(DocValues::docs_with_value_sorted_set(dv, self.max_doc))
            }
            DocValuesType::SortedNumeric => {
                let dv = self.get_sorted_numeric(field)?.get()?;
                Ok(DocValues::docs_with_value_sorted_numeric(dv, self.max_doc))
            }
            DocValuesType::Sorted => {
                let dv = self.get_sorted(field)?.get()?;
                Ok(DocValues::docs_with_value_sorted(dv, self.max_doc))
            }
            DocValuesType::Binary => {
                let be = self.binaries.get(&field.name).ok_or_else(|| {
                    IllegalArgument(format!("No binary field named {}", field.name))
                })?;
                Ok(self
                    .get_live_bits(be.missing_offset, self.max_doc as usize)?
                    .into_bits_mut())
            }
            DocValuesType::Numeric => {
                let ne = self.numerics.get(&field.name).ok_or_else(|| {
                    IllegalArgument(format!("No numeric field named {} found", field.name))
                })?;
                if ne.format == Lucene54DocValuesFormat::SPARSE_COMPRESSED {
                    Ok(Box::new(self.get_sparse_live_bits_by_entry(&ne)?))
                } else {
                    Ok(self
                        .get_live_bits(ne.missing_offset, self.max_doc as usize)?
                        .into_bits_mut())
                }
            }
            _ => bail!(IllegalArgument(format!(
                "Unknown DocValuesType {:?} for field {}",
                field.doc_values_type, field.name
            ))),
        }
    }
    fn check_integrity(&self) -> Result<()> {
        //        let mut input = self.data.as_ref().clone()?;
        //        let input: &mut dyn IndexInput = input.borrow_mut();
        // codec_util::checksum_entire_file(input)?;
        Ok(())
    }

    fn get_merge_instance(&self) -> Result<Box<dyn DocValuesProducer>> {
        Ok(Box::new(Lucene54DocValuesProducer::copy_from(self)?))
    }
}
