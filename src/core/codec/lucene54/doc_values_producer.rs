use core::codec::codec_util;
use core::codec::lucene54::{self, NumberType};
use core::codec::DocValuesProducer;
use core::index::BinaryDocValues;
use core::index::NumericDocValues;
use core::index::SortedNumericDocValues;
use core::index::{AddressedRandomAccessOrds, SortedSetDocValues, TabledRandomAccessOrds};
use core::index::{AddressedSortedNumericDocValues, TabledSortedNumericDocValues};
use core::index::{CompressedBinaryDocValues, FixedBinaryDocValues, VariableBinaryDocValues};
use core::index::{DocValues, DocValuesType};
use core::index::{FieldInfo, FieldInfos};
use core::index::{SegmentInfo, SegmentReadState};
use core::index::{SortedDocValues, TailoredSortedDocValues};
use core::store::{BufferedChecksumIndexInput, IndexInput};
use core::util::{PagedBytes, PagedBytesReader};

use core::index::segment_file_name;
use core::util::packed::MonotonicBlockPackedReader;
use core::util::packed::{DirectMonotonicMeta, DirectMonotonicReader, DirectReader};
use core::util::LongValues;
use core::util::{BitsRef, LiveBits, MatchAllBits, MatchNoBits, SparseBits};
use core::util::{DeltaLongValues, GcdLongValues, LiveLongValues, SparseLongValues, TableLongValues};

use error::ErrorKind::{CorruptIndex, IllegalArgument};
use error::Result;

use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub struct ReverseTermsIndex {
    pub term_addresses: MonotonicBlockPackedReader,
    pub terms: PagedBytesReader,
}

pub type ReverseTermsIndexRef = Arc<ReverseTermsIndex>;

type NumericEntryLink = Arc<NumericEntry>;

/// meta-data entry for a numeric docvalues field
struct NumericEntry {
    /// offset to the bitset representing docsWithField, or -1 if no documents have missing
    /// values
    missing_offset: i64,
    offset: i64,
    end_offset: i64,
    count: i64,
    bits_per_value: i32,
    format: i32,
    min_value: i64,
    gcd: i64,
    num_docs_with_value: i64,
    number_type: NumberType,
    table: Vec<i64>,
    monotonic_meta: Option<Arc<DirectMonotonicMeta>>,
    non_missing_values: Option<NumericEntryLink>,
}

impl NumericEntry {
    pub fn new() -> Self {
        NumericEntry {
            missing_offset: 0,
            offset: 0,
            end_offset: 0,
            count: 0,
            bits_per_value: 0,
            format: 0,
            min_value: 0,
            gcd: 0,
            num_docs_with_value: 0,
            number_type: NumberType::VALUE,
            table: Vec::new(),
            monotonic_meta: None,
            non_missing_values: None,
        }
    }
}

/// metadata entry for a binary docvalues field
#[derive(Clone)]
pub struct BinaryEntry {
    missing_offset: i64,
    offset: i64,
    pub count: i64,
    min_length: i32,
    pub max_length: i32,
    // offset to the addressing data that maps a value to its slice of the byte[]
    addresses_offset: i64,
    addresses_end_offset: i64,

    reverse_index_offset: i64,

    // packed ints version used to encode addressing information
    packed_ints_version: i32,
    // packed ints blocksize
    block_size: i32,

    format: i32,
    addresses_meta: Option<Arc<DirectMonotonicMeta>>,
}

impl Default for BinaryEntry {
    fn default() -> Self {
        BinaryEntry {
            missing_offset: 0,
            offset: 0,
            count: 0,
            min_length: 0,
            max_length: 0,
            addresses_offset: 0,
            addresses_end_offset: 0,
            reverse_index_offset: 0,
            packed_ints_version: 0,
            block_size: 0,
            format: 0,
            addresses_meta: None,
        }
    }
}

struct SortedSetEntry {
    format: i32,
    table: Vec<i64>,
    table_offsets: Vec<i32>,
}

impl SortedSetEntry {
    pub fn new() -> Self {
        SortedSetEntry {
            format: 0,
            table: Vec::new(),
            table_offsets: Vec::new(),
        }
    }
}

pub struct Lucene54DocValuesProducer {
    #[allow(dead_code)]
    num_fields: i32,
    max_doc: i32,
    data: Box<IndexInput>,
    merging: bool,
    numerics: HashMap<String, NumericEntryLink>,
    binaries: HashMap<String, BinaryEntry>,
    sorted_sets: HashMap<String, SortedSetEntry>,
    sorted_numerics: HashMap<String, SortedSetEntry>,
    ords: HashMap<String, NumericEntryLink>,
    ord_indexes: HashMap<String, NumericEntryLink>,
    address_instances: RwLock<HashMap<String, Arc<MonotonicBlockPackedReader>>>,
    reverse_index_instances: RwLock<HashMap<String, Arc<ReverseTermsIndex>>>,
}

impl Lucene54DocValuesProducer {
    pub fn new(
        state: &SegmentReadState,
        data_codec: &str,
        data_ext: &str,
        meta_codec: &str,
        meta_ext: &str,
    ) -> Result<Lucene54DocValuesProducer> {
        let meta_name =
            segment_file_name(&state.segment_info.name, &state.segment_suffix, meta_ext);
        // read in the entries from the metadata file
        let input = state.directory.open_input(&meta_name, &state.context)?;
        let mut checksum_input = Box::new(BufferedChecksumIndexInput::new(input));

        let version = codec_util::check_index_header(
            checksum_input.as_mut(),
            meta_codec,
            lucene54::VERSION_START,
            lucene54::VERSION_CURRENT,
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
            checksum_input.as_mut(),
            &state.field_infos,
            &state.segment_info,
            &mut numerics,
            &mut binaries,
            &mut sorted_sets,
            &mut sorted_numerics,
            &mut ords,
            &mut ord_indexes,
        )?;

        codec_util::check_footer(checksum_input.as_mut())?;
        let data_name =
            segment_file_name(&state.segment_info.name, &state.segment_suffix, data_ext);
        let mut data = state.directory.open_input(&data_name, &state.context)?;
        let version2 = codec_util::check_index_header(
            data.as_mut(),
            data_codec,
            lucene54::VERSION_START,
            lucene54::VERSION_CURRENT,
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

    #[allow(too_many_arguments)]
    fn read_fields(
        meta: &mut IndexInput,
        infos: &FieldInfos,
        segment_info: &SegmentInfo,
        numerics: &mut HashMap<String, NumericEntryLink>,
        binaries: &mut HashMap<String, BinaryEntry>,
        sorted_sets: &mut HashMap<String, SortedSetEntry>,
        sorted_numerics: &mut HashMap<String, SortedSetEntry>,
        ords: &mut HashMap<String, NumericEntryLink>,
        ord_indexes: &mut HashMap<String, NumericEntryLink>,
    ) -> Result<i32> {
        let mut num_fields = 0;
        let mut field_number = meta.read_vint()?;
        while field_number != -1 {
            num_fields += 1;
            let info = infos
                .field_info_by_number(field_number)
                .ok_or_else(|| IllegalArgument(format!("invalid field number: {}", field_number)))?;
            let dv_type = meta.read_byte()?;
            match dv_type {
                lucene54::NUMERIC => {
                    let entry =
                        Lucene54DocValuesProducer::read_numeric_entry(info, segment_info, meta)?;
                    match entry {
                        Some(n) => numerics.insert(info.name.clone(), n),
                        _ => unreachable!(),
                    };
                }

                lucene54::BINARY => {
                    let b = Lucene54DocValuesProducer::read_binary_entry(info, meta)?;
                    binaries.insert(info.name.clone(), b);
                }

                lucene54::SORTED => {
                    Lucene54DocValuesProducer::read_sorted_field(
                        info,
                        segment_info,
                        meta,
                        binaries,
                        ords,
                    )?;
                }

                lucene54::SORTED_SET => {
                    let ss = Lucene54DocValuesProducer::read_sorted_set_entry(meta)?;
                    let ss_fmt = ss.format;
                    sorted_sets.insert(info.name.clone(), ss);
                    match ss_fmt {
                        lucene54::SORTED_WITH_ADDRESSES => {
                            Lucene54DocValuesProducer::read_sorted_set_field_with_addresses(
                                info,
                                segment_info,
                                meta,
                                binaries,
                                ords,
                                ord_indexes,
                            )?;
                        }

                        lucene54::SORTED_SET_TABLE => {
                            Lucene54DocValuesProducer::read_sorted_set_field_with_table(
                                info,
                                segment_info,
                                meta,
                                binaries,
                                ords,
                            )?;
                        }

                        lucene54::SORTED_SINGLE_VALUED => {
                            if meta.read_vint()? != field_number {
                                bail!(CorruptIndex(format!(
                                    "sorted_set entry for field {} is corrupt",
                                    info.name
                                )));
                            }

                            if meta.read_byte()? != lucene54::SORTED {
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

                lucene54::SORTED_NUMERIC => {
                    let ss = Lucene54DocValuesProducer::read_sorted_set_entry(meta)?;
                    let ss_fmt = ss.format;
                    sorted_numerics.insert(info.name.clone(), ss);
                    match ss_fmt {
                        lucene54::SORTED_WITH_ADDRESSES => {
                            if meta.read_vint()? != field_number {
                                bail!(CorruptIndex(format!(
                                    "sorted_numeric entry for field {} is corrupt",
                                    info.name
                                )));
                            }
                            if meta.read_byte()? != lucene54::NUMERIC {
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

                            if meta.read_vint()? != field_number {
                                bail!(CorruptIndex(format!(
                                    "sorted_numeric entry for field {} is corrupt",
                                    info.name
                                )));
                            }

                            if meta.read_byte()? != lucene54::NUMERIC {
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

                        lucene54::SORTED_SET_TABLE => {
                            if meta.read_vint()? != field_number {
                                bail!(CorruptIndex(format!(
                                    "sorted_numeric entry for field {} is corrupt",
                                    info.name
                                )));
                            }
                            if meta.read_byte()? != lucene54::NUMERIC {
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

                        lucene54::SORTED_SINGLE_VALUED => {
                            if meta.read_vint()? != field_number {
                                bail!(CorruptIndex(format!(
                                    "sorted_numeric entry for field {} is corrupt",
                                    info.name
                                )));
                            }
                            if meta.read_byte()? != lucene54::NUMERIC {
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
    fn read_numeric_entry(
        info: &FieldInfo,
        segment_info: &SegmentInfo,
        meta: &mut IndexInput,
    ) -> Result<Option<NumericEntryLink>> {
        let mut entry = NumericEntry::new();
        entry.format = meta.read_vint()?;
        entry.missing_offset = meta.read_long()?;
        if entry.format == lucene54::SPARSE_COMPRESSED {
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
            lucene54::CONST_COMPRESSED => {
                entry.min_value = meta.read_long()?;
                if entry.count > i64::from(::std::i32::MAX) {
                    bail!(CorruptIndex(format!(
                        "illegal CONST_COMPRESSED count: {}",
                        entry.count
                    )));
                }
            }
            lucene54::GCD_COMPRESSED => {
                entry.min_value = meta.read_long()?;
                entry.gcd = meta.read_long()?;
                entry.bits_per_value = meta.read_vint()?;
            }
            lucene54::TABLE_COMPRESSED => {
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
            lucene54::DELTA_COMPRESSED => {
                entry.min_value = meta.read_long()?;
                entry.bits_per_value = meta.read_vint()?;
            }
            lucene54::MONOTONIC_COMPRESSED => {
                let block_shift = meta.read_vint()?;
                let monotonic_meta = Arc::new(DirectMonotonicReader::load_meta(
                    meta,
                    i64::from(segment_info.max_doc + 1),
                    block_shift,
                )?);
                entry.monotonic_meta = Some(monotonic_meta);
            }
            lucene54::SPARSE_COMPRESSED => {
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
                if field_number != info.number {
                    bail!(CorruptIndex(format!(
                        "Field number mismatch: {} != {}",
                        field_number, info.number
                    )));
                }
                let dv_format = meta.read_byte()?;
                if dv_format != lucene54::NUMERIC {
                    bail!(CorruptIndex(format!(
                        "Format mismatch: {} != {}",
                        dv_format,
                        lucene54::NUMERIC
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

    fn read_binary_entry(_info: &FieldInfo, meta: &mut IndexInput) -> Result<BinaryEntry> {
        let mut entry = BinaryEntry::default();
        entry.format = meta.read_vint()?;
        entry.missing_offset = meta.read_long()?;
        entry.min_length = meta.read_vint()?;
        entry.max_length = meta.read_vint()?;
        entry.count = meta.read_vlong()?;
        entry.offset = meta.read_long()?;
        match entry.format {
            lucene54::BINARY_FIXED_UNCOMPRESSED => {}
            lucene54::BINARY_PREFIX_COMPRESSED => {
                entry.addresses_offset = meta.read_long()?;
                entry.packed_ints_version = meta.read_vint()?;
                entry.block_size = meta.read_vint()?;
                entry.reverse_index_offset = meta.read_long()?;
            }
            lucene54::BINARY_VARIABLE_UNCOMPRESSED => {
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

    fn read_sorted_set_entry(meta: &mut IndexInput) -> Result<SortedSetEntry> {
        let mut entry = SortedSetEntry::new();
        entry.format = meta.read_vint()?;
        match entry.format {
            lucene54::SORTED_SET_TABLE => {
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

            lucene54::SORTED_SINGLE_VALUED => {
                // do nothing
            }

            lucene54::SORTED_WITH_ADDRESSES => {
                // do nothing
            }
            _ => {
                bail!(CorruptIndex(format!("unknown format: {}", entry.format)));
            }
        }
        Ok(entry)
    }

    fn read_sorted_field(
        info: &FieldInfo,
        segment_info: &SegmentInfo,
        meta: &mut IndexInput,
        binaries: &mut HashMap<String, BinaryEntry>,
        ords: &mut HashMap<String, NumericEntryLink>,
    ) -> Result<()> {
        // sorted = binary + numeric
        if meta.read_vint()? != info.number {
            bail!(CorruptIndex(format!(
                "sorted entry for field {} is corrupt",
                info.name
            )));
        }

        if meta.read_byte()? != lucene54::BINARY {
            bail!(CorruptIndex(format!(
                "sorted entry for field {} is corrupt",
                info.name
            )));
        }

        let b = Lucene54DocValuesProducer::read_binary_entry(info, meta)?;
        binaries.insert(info.name.clone(), b);
        if meta.read_vint()? != info.number {
            bail!(CorruptIndex(format!(
                "sorted entry for field {} is corrupt",
                info.name
            )));
        }

        if meta.read_byte()? != lucene54::NUMERIC {
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

    fn read_sorted_set_field_with_addresses(
        info: &FieldInfo,
        segment_info: &SegmentInfo,
        meta: &mut IndexInput,
        binaries: &mut HashMap<String, BinaryEntry>,
        ords: &mut HashMap<String, NumericEntryLink>,
        ord_indexes: &mut HashMap<String, NumericEntryLink>,
    ) -> Result<()> {
        // sorted_set = binary + numeric (addresses) + ord_index
        if meta.read_vint()? != info.number {
            bail!(CorruptIndex(format!(
                "sorted_set entry for field {} is corrupt",
                info.name
            )));
        }

        if meta.read_byte()? != lucene54::BINARY {
            bail!(CorruptIndex(format!(
                "sorted_set entry for field {} is corrupt",
                info.name
            )));
        }

        let b = Lucene54DocValuesProducer::read_binary_entry(info, meta)?;
        binaries.insert(info.name.clone(), b);

        if meta.read_vint()? != info.number {
            bail!(CorruptIndex(format!(
                "sorted_set entry for field {} is corrupt",
                info.name
            )));
        }

        if meta.read_byte()? != lucene54::NUMERIC {
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

        if meta.read_vint()? != info.number {
            bail!(CorruptIndex(format!(
                "sorted_set entry for field {} is corrupt",
                info.name
            )));
        }

        if meta.read_byte()? != lucene54::NUMERIC {
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

    fn read_sorted_set_field_with_table(
        info: &FieldInfo,
        segment_info: &SegmentInfo,
        meta: &mut IndexInput,
        binaries: &mut HashMap<String, BinaryEntry>,
        ords: &mut HashMap<String, NumericEntryLink>,
    ) -> Result<()> {
        // sorted_set_table = binary + ord_set table + ordset index
        if meta.read_vint()? != info.number {
            bail!(CorruptIndex(format!(
                "sorted_set entry for field {} is corrupt",
                info.name
            )));
        }
        if meta.read_byte()? != lucene54::BINARY {
            bail!(CorruptIndex(format!(
                "sorted_set entry for field {} is corrupt",
                info.name
            )));
        }

        let b = Lucene54DocValuesProducer::read_binary_entry(info, meta)?;
        binaries.insert(info.name.clone(), b);

        if meta.read_vint()? != info.number {
            bail!(CorruptIndex(format!(
                "sorted_set entry for field {} is corrupt",
                info.name
            )));
        }

        if meta.read_byte()? != lucene54::NUMERIC {
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

    fn get_live_bits(&self, offset: i64, count: usize) -> Result<BitsRef> {
        Ok(match offset as i32 {
            lucene54::ALL_MISSING => Arc::new(MatchNoBits::new(count)),
            lucene54::ALL_LIVE => Arc::new(MatchAllBits::new(count)),
            _ => {
                let data = self.data.as_ref().clone()?;
                let boxed = LiveBits::new(data.as_ref(), offset, count)?;
                Arc::new(boxed)
            }
        })
    }
}

impl Lucene54DocValuesProducer {
    fn get_numeric_const_compressed(&self, entry: &NumericEntryLink) -> Result<LiveLongValues> {
        let constant = entry.min_value;
        let inbox = self.get_live_bits(entry.missing_offset, entry.count as usize)?;
        Ok(LiveLongValues::new(inbox, constant))
    }

    fn get_numeric_delta_compressed(&self, entry: &NumericEntryLink) -> Result<DeltaLongValues> {
        let slice = self.data
            .random_access_slice(entry.offset, entry.end_offset - entry.offset)?;
        let slice = Arc::from(slice);
        let delta = entry.min_value;
        let inbox = DirectReader::get_instance(slice, entry.bits_per_value, 0)?;
        Ok(DeltaLongValues::new(inbox, delta))
    }

    fn get_numeric_gcd_compressed(&self, entry: &NumericEntryLink) -> Result<GcdLongValues> {
        let slice = self.data
            .random_access_slice(entry.offset, entry.end_offset - entry.offset)?;
        let slice = Arc::from(slice);
        let base = entry.min_value;
        let mult = entry.gcd;
        let inbox = DirectReader::get_instance(slice, entry.bits_per_value, 0)?;
        Ok(GcdLongValues::new(inbox, base, mult))
    }

    fn get_numeric_table_compressed(&self, entry: &NumericEntryLink) -> Result<TableLongValues> {
        let data = self.data.as_ref().clone()?;
        let slice = data.random_access_slice(entry.offset, entry.end_offset - entry.offset)?;
        let slice = Arc::from(slice);
        let table = entry.table.clone();
        let ords = DirectReader::get_instance(slice, entry.bits_per_value, 0)?;
        Ok(TableLongValues::new(ords, table))
    }

    fn get_numeric_sparse_compressed(&self, entry: &NumericEntryLink) -> Result<SparseLongValues> {
        let docs_with_field = Arc::new(self.get_sparse_live_bits_by_entry(&entry)?);

        debug_assert!(!entry.non_missing_values.is_none());

        let non_missing_values = entry.non_missing_values.as_ref().unwrap();
        let values = self.get_numeric_by_entry(non_missing_values)?;
        let missing_value = match entry.number_type {
            lucene54::NumberType::ORDINAL => -1_i64,
            lucene54::NumberType::VALUE => 0_i64,
        };
        Ok(SparseLongValues::new(
            docs_with_field,
            values,
            missing_value,
        ))
    }

    fn get_numeric_by_entry_outbound(
        &self,
        entry: &NumericEntryLink,
    ) -> Result<Box<NumericDocValues>> {
        let fmt = entry.format;
        match fmt {
            lucene54::CONST_COMPRESSED => {
                let live_lv = self.get_numeric_const_compressed(entry)?;
                Ok(Box::new(live_lv))
            }
            lucene54::DELTA_COMPRESSED => {
                let delta_lv = self.get_numeric_delta_compressed(entry)?;
                Ok(Box::new(delta_lv))
            }
            lucene54::GCD_COMPRESSED => {
                let gcd_lv = self.get_numeric_gcd_compressed(entry)?;
                Ok(Box::new(gcd_lv))
            }
            lucene54::TABLE_COMPRESSED => {
                let table_lv = self.get_numeric_table_compressed(entry)?;
                Ok(Box::new(table_lv))
            }
            lucene54::SPARSE_COMPRESSED => {
                let sparse_lv = self.get_numeric_sparse_compressed(entry)?;
                Ok(Box::new(sparse_lv))
            }
            _ => bail!(IllegalArgument(format!(
                "Unknown numeric entry format: {}",
                fmt
            ))),
        }
    }

    fn get_numeric_by_entry(&self, entry: &NumericEntryLink) -> Result<Box<LongValues>> {
        let fmt = entry.format;
        match fmt {
            lucene54::CONST_COMPRESSED => {
                let live_lv = self.get_numeric_const_compressed(entry)?;
                Ok(Box::new(live_lv))
            }
            lucene54::DELTA_COMPRESSED => {
                let delta_lv = self.get_numeric_delta_compressed(entry)?;
                Ok(Box::new(delta_lv))
            }
            lucene54::GCD_COMPRESSED => {
                let gcd_lv = self.get_numeric_gcd_compressed(entry)?;
                Ok(Box::new(gcd_lv))
            }
            lucene54::TABLE_COMPRESSED => {
                let table_lv = self.get_numeric_table_compressed(entry)?;
                Ok(Box::new(table_lv))
            }
            lucene54::SPARSE_COMPRESSED => {
                let sparse_lv = self.get_numeric_sparse_compressed(entry)?;
                Ok(Box::new(sparse_lv))
            }
            _ => bail!(IllegalArgument(format!(
                "Unknown numeric entry format: {}",
                fmt
            ))),
        }
    }

    fn get_sparse_live_bits_by_entry(&self, entry: &NumericEntry) -> Result<SparseBits> {
        let length = entry.offset - entry.missing_offset;

        let doc_ids_data = self.data.random_access_slice(entry.missing_offset, length)?;
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
        let data = self.data.as_ref().slice(
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
    ) -> Result<VariableBinaryDocValues> {
        let addresses_length = bytes.addresses_end_offset - bytes.addresses_offset;
        let meta_ref = bytes
            .addresses_meta
            .as_ref()
            .ok_or_else(|| IllegalArgument("addresses_meta None???".to_owned()))?;
        let meta = Arc::clone(meta_ref);

        let addresses_data = self.data
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
        let data: &mut IndexInput = data.borrow_mut();
        data.seek(bytes.addresses_offset)?;
        let size =
            ((bytes.count + i64::from(lucene54::INTERVAL_MASK)) >> lucene54::INTERVAL_SHIFT) as u64;
        let addresses = MonotonicBlockPackedReader::new(
            data,
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
        let data: &mut IndexInput = data.borrow_mut();
        data.seek(bytes.reverse_index_offset)?;
        let size = (bytes.count + i64::from(lucene54::REVERSE_INTERVAL_MASK))
            >> lucene54::REVERSE_INTERVAL_SHIFT;
        let term_addresses = MonotonicBlockPackedReader::new(
            data.borrow_mut(),
            bytes.packed_ints_version,
            bytes.block_size as usize,
            size as u64,
            false,
        )?;
        let data_size = data.read_vlong()?;
        let mut paged_bytes = PagedBytes::new(15);
        paged_bytes.copy(data.borrow_mut(), data_size)?;
        paged_bytes.freeze(true);
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
        let slice = self.data
            .slice("terms", bytes.offset, bytes.addresses_offset - bytes.offset)?;
        CompressedBinaryDocValues::new(bytes, addresses, index, slice)
    }
}

impl Lucene54DocValuesProducer {
    fn get_ord_index_instance(&self, entry: &NumericEntryLink) -> Result<Box<LongValues>> {
        let data = self.data
            .random_access_slice(entry.offset, entry.end_offset - entry.offset)?;
        if let Some(ref meta) = entry.monotonic_meta {
            DirectMonotonicReader::get_instance(meta.as_ref(), &Arc::from(data))
        } else {
            unreachable!();
        }
    }

    fn get_sorted_set_with_addresses(&self, field: &FieldInfo) -> Result<Box<SortedSetDocValues>> {
        let value_count = self.binaries
            .get(&field.name)
            .ok_or_else(|| IllegalArgument(format!("No binary field named {}", field.name)))?
            .count as usize;

        let ordinals;
        {
            let ord_entry = self.ords
                .get(&field.name)
                .ok_or_else(|| IllegalArgument(format!("No ords field named {}", &field.name)))?;

            ordinals = self.get_numeric_by_entry(ord_entry)?;
        }

        let ord_index;
        {
            let ord_index_entry = self.ord_indexes
                .get(&field.name)
                .ok_or_else(|| IllegalArgument(format!("No OrdIndex field named {}", field.name)))?;
            ord_index = self.get_ord_index_instance(ord_index_entry)?;
        }

        let bytes = self.binaries
            .get(&field.name)
            .ok_or_else(|| IllegalArgument(format!("No binary field named {}", field.name)))?
            .clone();

        let my_format = bytes.format;

        match my_format {
            lucene54::BINARY_FIXED_UNCOMPRESSED => {
                let binary = Box::new(self.get_fixed_binary(field, &bytes)?);
                let boxed =
                    AddressedRandomAccessOrds::new(binary, ordinals, ord_index, value_count);
                Ok(Box::new(boxed))
            }
            lucene54::BINARY_VARIABLE_UNCOMPRESSED => {
                let binary = Box::new(self.get_variable_binary(field, &bytes)?);
                let boxed =
                    AddressedRandomAccessOrds::new(binary, ordinals, ord_index, value_count);
                Ok(Box::new(boxed))
            }
            lucene54::BINARY_PREFIX_COMPRESSED => {
                let binary = Box::new(self.get_compressed_binary(field, &bytes)?);
                let boxed = AddressedRandomAccessOrds::with_compression(
                    binary,
                    ordinals,
                    ord_index,
                    value_count,
                );
                Ok(Box::new(boxed))
            }
            _ => bail!(IllegalArgument(format!(
                "unknown binary_entry format: {}",
                my_format,
            ))),
        }
    }

    fn get_sorted_set_table(&self, field: &FieldInfo) -> Result<Box<SortedSetDocValues>> {
        let table;
        let table_offsets;
        {
            let ss = self.sorted_sets.get(&field.name).ok_or_else(|| {
                IllegalArgument(format!("No SortedSet field named {}", &field.name))
            })?;

            table = ss.table.clone();
            table_offsets = ss.table_offsets.clone();
        }

        let value_count = self.binaries
            .get(&field.name)
            .ok_or_else(|| IllegalArgument(format!("No binary field named {}", field.name)))?
            .count as usize;

        let ordinals;
        {
            let ord_entry;
            ord_entry = self.ords
                .get(&field.name)
                .ok_or_else(|| IllegalArgument(format!("No ords field named {}", &field.name)))?;
            ordinals = self.get_numeric_by_entry(ord_entry)?;
        }

        let bytes = self.binaries
            .get(&field.name)
            .ok_or_else(|| IllegalArgument(format!("No binary field named {}", field.name)))?
            .clone();

        match bytes.format {
            lucene54::BINARY_FIXED_UNCOMPRESSED => {
                let binary = Box::new(self.get_fixed_binary(field, &bytes)?);
                let boxed = TabledRandomAccessOrds::new(
                    binary,
                    ordinals,
                    table,
                    table_offsets,
                    value_count,
                );
                Ok(Box::new(boxed))
            }
            lucene54::BINARY_VARIABLE_UNCOMPRESSED => {
                let binary = Box::new(self.get_variable_binary(field, &bytes)?);
                let boxed = TabledRandomAccessOrds::new(
                    binary,
                    ordinals,
                    table,
                    table_offsets,
                    value_count,
                );
                Ok(Box::new(boxed))
            }
            lucene54::BINARY_PREFIX_COMPRESSED => {
                let binary = Box::new(self.get_compressed_binary(field, &bytes)?);
                let boxed = TabledRandomAccessOrds::with_compression(
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
    fn get_numeric(&self, field: &FieldInfo) -> Result<Box<NumericDocValues>> {
        let link = self.numerics.get(&field.name).ok_or_else(|| {
            IllegalArgument(format!("No numeric field named {} found", field.name))
        })?;
        let boxed = self.get_numeric_by_entry_outbound(link)?;
        Ok(boxed)
    }

    fn get_binary(&self, field: &FieldInfo) -> Result<Box<BinaryDocValues>> {
        let bytes = self.binaries
            .get(&field.name)
            .ok_or_else(|| IllegalArgument(format!("No binary field named {}", field.name)))?
            .clone();
        let myformat = bytes.format;

        match myformat {
            lucene54::BINARY_FIXED_UNCOMPRESSED => {
                let boxed = self.get_fixed_binary(field, &bytes)?;
                Ok(Box::new(boxed))
            }
            lucene54::BINARY_VARIABLE_UNCOMPRESSED => {
                let boxed = self.get_variable_binary(field, &bytes)?;
                Ok(Box::new(boxed))
            }

            lucene54::BINARY_PREFIX_COMPRESSED => {
                let boxed = self.get_compressed_binary(field, &bytes)?;
                Ok(Box::new(boxed))
            }
            _ => bail!(IllegalArgument(format!(
                "unknown binary_entry format: {}",
                myformat,
            ))),
        }
    }

    fn get_sorted(&self, field: &FieldInfo) -> Result<Box<SortedDocValues>> {
        let value_count = self.binaries
            .get(&field.name)
            .ok_or_else(|| IllegalArgument(format!("No binary field named {}", &field.name)))?
            .count as usize;
        let ordinals;
        {
            let entry = self.ords
                .get(&field.name)
                .ok_or_else(|| IllegalArgument(format!("No ords field named {}", &field.name)))?;
            ordinals = self.get_numeric_by_entry(entry)?;
        }

        let bytes = self.binaries
            .get(&field.name)
            .ok_or_else(|| IllegalArgument(format!("No binary field named {}", field.name)))?
            .clone();

        match bytes.format {
            lucene54::BINARY_FIXED_UNCOMPRESSED => {
                let binary = Box::new(self.get_fixed_binary(field, &bytes)?);
                let boxed = TailoredSortedDocValues::new(ordinals, binary, value_count);
                Ok(Box::new(boxed))
            }
            lucene54::BINARY_VARIABLE_UNCOMPRESSED => {
                let binary = Box::new(self.get_variable_binary(field, &bytes)?);
                let boxed = TailoredSortedDocValues::new(ordinals, binary, value_count);
                Ok(Box::new(boxed))
            }
            lucene54::BINARY_PREFIX_COMPRESSED => {
                let binary = Box::new(self.get_compressed_binary(field, &bytes)?);
                let boxed =
                    TailoredSortedDocValues::with_compression(ordinals, binary, value_count);
                Ok(Box::new(boxed))
            }
            _ => bail!(IllegalArgument(format!(
                "unknown binary_entry format: {}",
                bytes.format
            ))),
        }
    }

    fn get_sorted_numeric(&self, field: &FieldInfo) -> Result<Box<SortedNumericDocValues>> {
        let ss = self.sorted_numerics.get(&field.name).ok_or_else(|| {
            IllegalArgument(format!("No SortedNumeric field named {}", field.name))
        })?;
        match ss.format {
            lucene54::SORTED_SINGLE_VALUED => {
                let numeric_entry = self.numerics.get(&field.name).ok_or_else(|| {
                    IllegalArgument(format!("No Numerics field named {}", field.name))
                })?;
                if numeric_entry.format == lucene54::SPARSE_COMPRESSED {
                    let values = self.get_numeric_sparse_compressed(numeric_entry)?;
                    let docs_with_field = values.docs_with_field_clone();
                    Ok(Box::new(DocValues::singleton_sorted_numeric_doc_values(
                        Box::new(values),
                        docs_with_field,
                    )))
                } else {
                    let offset = numeric_entry.missing_offset;
                    let count = self.max_doc as usize;

                    let values = self.get_numeric_by_entry_outbound(numeric_entry)?;

                    match offset as i32 {
                        lucene54::ALL_MISSING => {
                            let living_room = MatchNoBits::new(count);
                            Ok(Box::new(DocValues::singleton_sorted_numeric_doc_values(
                                values,
                                Arc::new(living_room),
                            )))
                        }
                        lucene54::ALL_LIVE => {
                            let living_room = MatchAllBits::new(count);
                            Ok(Box::new(DocValues::singleton_sorted_numeric_doc_values(
                                values,
                                Arc::new(living_room),
                            )))
                        }
                        _ => {
                            let mut data = self.data.as_ref().clone()?;
                            let living_room = LiveBits::new(data.borrow_mut(), offset, count)?;
                            Ok(Box::new(DocValues::singleton_sorted_numeric_doc_values(
                                values,
                                Arc::new(living_room),
                            )))
                        }
                    }
                }
            }
            lucene54::SORTED_WITH_ADDRESSES => {
                let numeric_entry = self.numerics.get(&field.name).ok_or_else(|| {
                    IllegalArgument(format!("No Numerics field named {}", field.name))
                })?;
                let values: Box<LongValues> = self.get_numeric_by_entry(numeric_entry)?;
                let ord_entry = self.ord_indexes.get(&field.name).ok_or_else(|| {
                    IllegalArgument(format!("no field named {} in ord_indexes", field.name))
                })?;
                let ord_index = self.get_ord_index_instance(ord_entry)?;
                Ok(Box::new(AddressedSortedNumericDocValues::new(
                    values, ord_index,
                )))
            }
            lucene54::SORTED_SET_TABLE => {
                let numeric_entry = self.ords
                    .get(&field.name)
                    .ok_or_else(|| IllegalArgument(format!("No Ords field named {}", field.name)))?;
                let ordinals: Box<LongValues> = self.get_numeric_by_entry(numeric_entry)?;
                Ok(Box::new(TabledSortedNumericDocValues::new(
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

    fn get_sorted_set(&self, field: &FieldInfo) -> Result<Box<SortedSetDocValues>> {
        let my_format = self.sorted_sets
            .get(&field.name)
            .ok_or_else(|| IllegalArgument(format!("No SortedSet field named {}", &field.name)))?
            .format;

        match my_format {
            lucene54::SORTED_SINGLE_VALUED => {
                let values = self.get_sorted(field)?;
                let boxed = DocValues::singleton_sorted_doc_values(values);
                Ok(Box::new(boxed))
            }

            lucene54::SORTED_WITH_ADDRESSES => self.get_sorted_set_with_addresses(field),
            lucene54::SORTED_SET_TABLE => self.get_sorted_set_table(field),
            _ => bail!(IllegalArgument(format!(
                "Unknown SortedSetEntry.format {} of field {}",
                my_format, field.name
            ))),
        }
    }

    fn get_docs_with_field(&self, field: &FieldInfo) -> Result<BitsRef> {
        match field.doc_values_type {
            DocValuesType::SortedSet => {
                let dv = self.get_sorted_set(field)?;
                Ok(DocValues::docs_with_value_sorted_set(dv, self.max_doc))
            }
            DocValuesType::SortedNumeric => {
                let dv = self.get_sorted_numeric(field)?;
                Ok(DocValues::docs_with_value_sorted_numeric(dv, self.max_doc))
            }
            DocValuesType::Sorted => {
                let dv = self.get_sorted(field)?;
                Ok(DocValues::docs_with_value_sorted(dv, self.max_doc))
            }
            DocValuesType::Binary => {
                let be = self.binaries.get(&field.name).ok_or_else(|| {
                    IllegalArgument(format!("No binary field named {}", field.name))
                })?;
                self.get_live_bits(be.offset, self.max_doc as usize)
            }
            DocValuesType::Numeric => {
                let ne = self.numerics.get(&field.name).ok_or_else(|| {
                    IllegalArgument(format!("No numeric field named {} found", field.name))
                })?;
                if ne.format == lucene54::SPARSE_COMPRESSED {
                    Ok(Arc::new(self.get_sparse_live_bits_by_entry(&ne)?))
                } else {
                    self.get_live_bits(ne.missing_offset, self.max_doc as usize)
                }
            }
            _ => bail!(IllegalArgument(format!(
                "Unknown DocValuesType {:?} for field {}",
                field.doc_values_type, field.name
            ))),
        }
    }
    fn check_integrity(&self) -> Result<()> {
        let mut input = self.data.as_ref().clone()?;
        let input: &mut IndexInput = input.borrow_mut();
        codec_util::checksum_entire_file(input)?;
        Ok(())
    }
}
