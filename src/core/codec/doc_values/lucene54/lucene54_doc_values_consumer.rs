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

use core::codec::codec_util;
use core::codec::doc_values::lucene54::{Lucene54DocValuesFormat, NumberType};
use core::codec::doc_values::{hash_vec, DocValuesConsumer, ReusableIterFilter, SetIdIter};
use core::codec::doc_values::{is_single_valued, singleton_view};
use core::codec::field_infos::FieldInfo;
use core::codec::segment_infos::{segment_file_name, SegmentWriteState};
use core::codec::Codec;
use core::store::directory::Directory;
use core::store::io::{DataOutput, IndexOutput, RAMOutputStream};
use core::util::gcd as gcd_func;
use core::util::packed::VERSION_CURRENT as PACKED_VERSION_CURRENT;
use core::util::packed::{
    AbstractBlockPackedWriter, DirectMonotonicWriter, DirectWriter, MonotonicBlockPackedWriter,
};
use core::util::{
    bytes_difference, sort_key_length, BytesRef, Numeric, PagedBytes, ReusableIterator,
};

use error::Result;
use std::collections::{BTreeSet, HashMap, HashSet};

pub struct Lucene54DocValuesConsumer<O: IndexOutput> {
    data: O,
    meta: O,
    max_doc: i32,
}

impl<O: IndexOutput> Lucene54DocValuesConsumer<O> {
    pub fn new<D: Directory, DW: Directory<IndexOutput = O>, C: Codec>(
        state: &SegmentWriteState<D, DW, C>,
        data_codec: &str,
        data_extension: &str,
        meta_codec: &str,
        meta_extension: &str,
    ) -> Result<Self> {
        let data_name = segment_file_name(
            &state.segment_info.name,
            &state.segment_suffix,
            data_extension,
        );
        let mut data = state.directory.create_output(&data_name, &state.context)?;
        codec_util::write_index_header(
            &mut data,
            data_codec,
            Lucene54DocValuesFormat::VERSION_CURRENT,
            state.segment_info.get_id(),
            &state.segment_suffix,
        )?;

        let meta_name = segment_file_name(
            &state.segment_info.name,
            &state.segment_suffix,
            meta_extension,
        );
        let mut meta = state.directory.create_output(&meta_name, &state.context)?;
        codec_util::write_index_header(
            &mut meta,
            meta_codec,
            Lucene54DocValuesFormat::VERSION_CURRENT,
            state.segment_info.get_id(),
            &state.segment_suffix,
        )?;

        let max_doc = state.segment_info.max_doc();

        Ok(Lucene54DocValuesConsumer {
            data,
            meta,
            max_doc,
        })
    }
}

impl<O: IndexOutput> Lucene54DocValuesConsumer<O> {
    // due to this method will call self recursively in some situation with the parameter
    // values wrapped in `ReusableIterFilter`, so if we have to use trait object instead of
    // generic to avoid infinite type resolve like
    // ReusableIterFilter<ReusableIterFilter<ReusableIterFilter<..., P>, P>
    fn add_numeric(
        &mut self,
        field_info: &FieldInfo,
        values: &mut dyn ReusableIterator<Item = Result<Numeric>>,
        number_type: NumberType,
    ) -> Result<()> {
        let mut count = 0i64;
        let mut min_value = i64::max_value();
        let mut max_value = i64::min_value();
        let mut gcd = 0i64;
        let mut missing_count = 0i64;
        let mut missing_ord_count = 0i64;
        let mut zero_count = 0i64;

        let mut unique_values: Option<BTreeSet<i64>> = None;
        match number_type {
            NumberType::VALUE => {
                unique_values = Some(BTreeSet::new());
                loop {
                    let nv = match values.next() {
                        None => {
                            break;
                        }
                        Some(r) => r?,
                    };
                    let mut v = 0i64;
                    if nv.is_null() {
                        missing_count += 1;
                        zero_count += 1;
                    } else {
                        v = nv.long_value();
                        if v == 0 {
                            zero_count += 1;
                        }
                    }

                    if gcd != 1 {
                        if v < i64::min_value() / 2 || v > i64::max_value() / 2 {
                            // in that case v - minValue might overflow and make the GCD computation
                            // return wrong results. Since these extreme values
                            // are unlikely, we just discard GCD computation
                            // for them
                            gcd = 1;
                        } else if count != 0 {
                            // minValue needs to be set first
                            gcd = gcd_func(gcd, v.wrapping_sub(min_value));
                        }
                    }

                    min_value = v.min(min_value);
                    max_value = v.max(max_value);

                    if unique_values.is_some() {
                        unique_values.as_mut().unwrap().insert(v);
                        if unique_values.as_ref().unwrap().len() > 256 {
                            unique_values = None;
                        }
                    }

                    count += 1;
                }
            }
            _ => loop {
                let v = match values.next() {
                    None => {
                        break;
                    }
                    Some(r) => r?.long_value(),
                };
                if v == -1 {
                    missing_ord_count += 1;
                }

                min_value = v.min(min_value);
                max_value = v.max(max_value);

                count += 1;
            },
        }

        debug_assert!(count > 0);
        let delta = max_value.wrapping_sub(min_value);
        let delta_bits_required = DirectWriter::<O>::unsigned_bits_required(delta);
        let table_bits_required = if let Some(ref unique_values) = unique_values {
            DirectWriter::<O>::bits_required((unique_values.len() - 1) as i64)
        } else {
            i32::max_value()
        };

        // 1% of docs or less have a value
        let sparse = match number_type {
            NumberType::VALUE => (missing_count as f64 / count as f64) >= 0.99,
            NumberType::ORDINAL => (missing_ord_count as f64 / count as f64) >= 0.99,
        };

        let format = if unique_values.is_some()
            && count <= i32::max_value() as i64
            && (unique_values.as_ref().unwrap().len() == 1
                || (unique_values.as_ref().unwrap().len() == 2
                    && missing_count > 0
                    && zero_count == missing_count))
        {
            // either one unique value C or two unique values: "missing" and C
            Lucene54DocValuesFormat::CONST_COMPRESSED
        } else if sparse && count >= 1024 {
            // require at least 1024 docs to avoid flipping back and forth when doing NRT search
            Lucene54DocValuesFormat::SPARSE_COMPRESSED
        } else if unique_values.is_some() && table_bits_required < delta_bits_required {
            Lucene54DocValuesFormat::TABLE_COMPRESSED
        } else if gcd != 0 && gcd != 1 {
            let gcd_delta: i64 = (max_value.wrapping_sub(min_value)) / gcd;
            let gcd_bits_required = DirectWriter::<O>::unsigned_bits_required(gcd_delta);

            if gcd_bits_required < delta_bits_required {
                Lucene54DocValuesFormat::GCD_COMPRESSED
            } else {
                Lucene54DocValuesFormat::DELTA_COMPRESSED
            }
        } else {
            Lucene54DocValuesFormat::DELTA_COMPRESSED
        };

        self.meta.write_vint(field_info.number as i32)?;
        self.meta.write_byte(Lucene54DocValuesFormat::NUMERIC)?;
        self.meta.write_vint(format)?;
        if format == Lucene54DocValuesFormat::SPARSE_COMPRESSED {
            self.meta.write_long(self.data.file_pointer())?;
            let num_docs_with_value: i64 = match number_type {
                NumberType::VALUE => count - missing_count,
                NumberType::ORDINAL => count - missing_ord_count,
            };
            values.reset();
            let max_doc =
                self.write_sparse_missing_bitset(values, number_type, num_docs_with_value)?;
            debug_assert!(max_doc == count);
        } else if missing_count == 0 {
            self.meta
                .write_long(Lucene54DocValuesFormat::ALL_LIVE as i64)?;
        } else if missing_count == count {
            self.meta
                .write_long(Lucene54DocValuesFormat::ALL_MISSING as i64)?;
        } else {
            self.meta.write_long(self.data.file_pointer())?;
            values.reset();
            self.write_missing_bitset_numeric(values)?;
        }
        self.meta.write_long(self.data.file_pointer())?;
        self.meta.write_vlong(count)?;

        match format {
            Lucene54DocValuesFormat::CONST_COMPRESSED => {
                debug_assert!(
                    unique_values.is_some() && !unique_values.as_ref().unwrap().is_empty()
                );
                let v_min = *unique_values.as_ref().unwrap().iter().min().unwrap();
                let v_max = *unique_values.as_ref().unwrap().iter().max().unwrap();

                let v = if min_value < 0 { v_min } else { v_max };
                // write the constant (nonzero value in the n=2 case, singleton value otherwise)
                self.meta.write_long(v)?;
            }
            Lucene54DocValuesFormat::GCD_COMPRESSED => {
                self.meta.write_long(min_value)?;
                self.meta.write_long(gcd)?;
                let max_delta = (max_value.wrapping_sub(min_value)) / gcd;
                let bits = DirectWriter::<O>::unsigned_bits_required(max_delta);
                self.meta.write_vint(bits)?;
                let mut quotient_writer =
                    DirectWriter::<O>::get_instance(&mut self.data, count, bits)?;
                values.reset();
                for nv in values {
                    let nv = nv?;
                    let value = if nv.is_null() { 0 } else { nv.long_value() };
                    quotient_writer.add((value.wrapping_sub(min_value)) / gcd)?;
                }
                quotient_writer.finish()?;
            }
            Lucene54DocValuesFormat::DELTA_COMPRESSED => {
                let min_delta = if delta < 0 { 0 } else { min_value };
                self.meta.write_long(min_delta)?;
                self.meta.write_vint(delta_bits_required)?;
                let mut writer =
                    DirectWriter::<O>::get_instance(&mut self.data, count, delta_bits_required)?;
                values.reset();
                for nv in values {
                    let nv = nv?;
                    let value = if nv.is_null() { 0 } else { nv.long_value() };
                    writer.add(value.wrapping_sub(min_delta))?;
                }
                writer.finish()?;
            }
            Lucene54DocValuesFormat::TABLE_COMPRESSED => {
                if let Some(ref unique_values) = unique_values {
                    self.meta.write_vint(unique_values.len() as i32)?;
                    let mut i = 0;
                    let mut encode: HashMap<i64, i32> = HashMap::new();
                    for v in unique_values {
                        self.meta.write_long(*v)?;
                        encode.insert(*v, i);
                        i += 1;
                    }
                    self.meta.write_vint(table_bits_required)?;
                    let mut ords_writer =
                        DirectWriter::get_instance(&mut self.data, count, table_bits_required)?;
                    values.reset();
                    for nv in values {
                        let nv = nv?;
                        let index = if nv.is_null() { 0 } else { nv.long_value() };
                        ords_writer.add((*encode.get(&index).unwrap()) as i64)?;
                    }
                    ords_writer.finish()?;
                }
            }
            Lucene54DocValuesFormat::SPARSE_COMPRESSED => {
                match number_type {
                    NumberType::VALUE => {
                        self.meta.write_byte(0)?;
                        values.reset();
                        let predicate = |nv: &Result<Numeric>| match nv {
                            Ok(e) if e.is_null() => false,
                            _ => true,
                        };
                        let mut values_filter = ReusableIterFilter::new(values, predicate);
                        self.add_numeric(field_info, &mut values_filter, number_type)?;
                    }
                    NumberType::ORDINAL => {
                        self.meta.write_byte(1)?;
                        values.reset();
                        let predicate = |nv: &Result<Numeric>| match nv {
                            Err(_) => false,
                            Ok(v) => !v.is_null() && v.long_value() != -1,
                        };
                        let mut values_filter = ReusableIterFilter::new(values, predicate);
                        self.add_numeric(field_info, &mut values_filter, number_type)?;
                    }
                };
            }
            _ => unreachable!(),
        }
        self.meta.write_long(self.data.file_pointer())
    }

    // TODO: in some cases representing missing with minValue-1 wouldn't take up additional space
    // and so on, but this is very simple, and algorithms only check this for values of 0
    // anyway (doesnt slow down normal decode)
    fn write_missing_bitset_numeric<T: ReusableIterator<Item = Result<Numeric>> + ?Sized>(
        &mut self,
        values: &mut T,
    ) -> Result<()> {
        let mut bits = 0u8;
        let mut count = 0i32;

        for v in values {
            let v = v?;
            if count == 8 {
                self.data.write_byte(bits)?;
                count = 0;
                bits = 0;
            }

            if !v.is_null() {
                bits |= 1 << (count & 7);
            }
            count += 1;
        }

        if count > 0 {
            self.data.write_byte(bits)?;
        }

        Ok(())
    }

    fn write_missing_bitset_bytes(
        &mut self,
        values: &mut impl ReusableIterator<Item = Result<BytesRef>>,
    ) -> Result<()> {
        let mut bits = 0u8;
        let mut count = 0i32;

        for v in values {
            let v = v?;
            if count == 8 {
                self.data.write_byte(bits)?;
                count = 0;
                bits = 0;
            }

            if !v.is_empty() {
                bits |= 1 << (count & 7);
            }
            count += 1;
        }

        if count > 0 {
            self.data.write_byte(bits)?;
        }

        Ok(())
    }

    fn write_sparse_missing_bitset<T: ReusableIterator<Item = Result<Numeric>> + ?Sized>(
        &mut self,
        values: &mut T,
        number_type: NumberType,
        num_docs_with_value: i64,
    ) -> Result<i64> {
        self.meta.write_vlong(num_docs_with_value)?;

        // Write doc IDs that have a value
        self.meta
            .write_vint(Lucene54DocValuesFormat::DIRECT_MONOTONIC_BLOCK_SHIFT)?;
        let mut doc_ids_writer = DirectMonotonicWriter::get_instance(
            &mut self.meta,
            &mut self.data,
            num_docs_with_value,
            Lucene54DocValuesFormat::DIRECT_MONOTONIC_BLOCK_SHIFT,
        )?;
        let mut doc_id = 0;
        for nv in values {
            let nv = nv?;
            match number_type {
                NumberType::VALUE => {
                    if !nv.is_null() {
                        doc_ids_writer.add(doc_id)?;
                    }
                }
                NumberType::ORDINAL => {
                    if nv.long_value() != -1i64 {
                        doc_ids_writer.add(doc_id)?;
                    }
                }
            }

            doc_id += 1;
        }

        doc_ids_writer.finish()?;

        Ok(doc_id)
    }

    fn add_terms_dict(
        &mut self,
        field_info: &FieldInfo,
        values: &mut impl ReusableIterator<Item = Result<BytesRef>>,
    ) -> Result<()> {
        // first check if it's a "fixed-length" terms dict, and compressibility if so
        let mut min_length = i32::max_value();
        let mut max_length = i32::min_value();
        let mut num_values = 0i64;
        let mut previous_value: Vec<u8> = vec![];
        // only valid for fixed-width data, as we have a choice there
        let mut prefix_sum = 0;

        loop {
            let v = match values.next() {
                None => {
                    break;
                }
                Some(r) => r?,
            };
            min_length = min_length.min(v.len() as i32);
            max_length = max_length.max(v.len() as i32);
            if min_length == max_length {
                let term_position =
                    (num_values as i32 & Lucene54DocValuesFormat::INTERVAL_MASK) as i32;
                if term_position == 0 {
                    // first term in block, save it away to compare against the last term later
                    previous_value.resize(v.len(), 0);
                    previous_value.copy_from_slice(v.bytes());
                } else {
                    // last term in block, accumulate shared prefix against first term
                    prefix_sum += bytes_difference(&previous_value, v.bytes());
                }
            }
            num_values += 1;
        }

        // for fixed width data, look at the avg(shared prefix) before deciding how to encode:
        // prefix compression "costs" worst case 2 bytes per term because we must store suffix
        // lengths. so if we share at least 3 bytes on average, always compress.

        // no index needed: not very compressible, direct addressing by mult
        if (min_length == max_length && prefix_sum as i64 <= 3 * (num_values >> Lucene54DocValuesFormat::INTERVAL_SHIFT))
            // low cardinality: waste a few KB of ram, but can't really use fancy index etc
            || ((num_values as i32) < Lucene54DocValuesFormat::REVERSE_INTERVAL_COUNT)
        {
            values.reset();
            self.add_binary_field(field_info, values)?;
        } else {
            // we don't have to handle the empty case header
            debug_assert!(num_values > 0);
            self.meta.write_vint(field_info.number as i32)?;
            self.meta.write_byte(Lucene54DocValuesFormat::BINARY)?;
            self.meta
                .write_vint(Lucene54DocValuesFormat::BINARY_PREFIX_COMPRESSED)?;
            self.meta.write_long(-1i64)?;
            // now write the bytes: sharing prefixes within a block
            let start_fp = self.data.file_pointer();
            // currently, we have to store the delta from expected for every 1/nth term
            // we could avoid this, but it's not much and less overall RAM than the previous
            // approach!
            let mut address_buffer = RAMOutputStream::new(false);

            // buffers up 16 terms
            let mut bytes_buffer = RAMOutputStream::new(false);
            // buffers up block header
            let mut header_buffer = RAMOutputStream::new(false);
            let mut last_term: Vec<u8> = vec![];
            let mut count = 0i64;
            let mut suffix_deltas: Vec<i32> =
                vec![0i32; Lucene54DocValuesFormat::INTERVAL_COUNT as usize];

            {
                let mut term_address = MonotonicBlockPackedWriter::new(
                    Lucene54DocValuesFormat::MONOTONIC_BLOCK_SIZE as usize,
                );
                values.reset();
                loop {
                    let v = match values.next() {
                        None => {
                            break;
                        }
                        Some(r) => r?,
                    };
                    let term_position =
                        (count & Lucene54DocValuesFormat::INTERVAL_MASK as i64) as i32;
                    if term_position == 0 {
                        term_address
                            .add(self.data.file_pointer() - start_fp, &mut address_buffer)?;
                        // abs-encode first term
                        header_buffer.write_vint(v.len() as i32)?;
                        header_buffer.write_bytes(v.bytes(), 0, v.len())?;

                        last_term.resize(v.len(), 0);
                        last_term.copy_from_slice(v.bytes());
                    } else {
                        // prefix-code: we only share at most 255 characters, to encode the length
                        // as a single byte and have random access. Larger
                        // terms just get less compression.
                        let shared_prefix =
                            255.min(bytes_difference(&last_term, v.bytes())) as usize;
                        bytes_buffer.write_byte(shared_prefix as u8)?;
                        bytes_buffer.write_bytes(
                            v.bytes(),
                            shared_prefix,
                            v.len() - shared_prefix,
                        )?;
                        // we can encode one smaller, because terms are unique.
                        suffix_deltas[term_position as usize] =
                            (v.len() - shared_prefix - 1) as i32;
                    }

                    count += 1;
                    // flush block
                    if (count & Lucene54DocValuesFormat::INTERVAL_MASK as i64) == 0 {
                        self.flush_terms_dict_block(
                            &mut header_buffer,
                            &mut bytes_buffer,
                            &suffix_deltas,
                        )?;
                    }
                }

                // flush trailing crap
                let leftover = (count & Lucene54DocValuesFormat::INTERVAL_MASK as i64) as usize;
                if leftover > 0 {
                    for i in leftover..suffix_deltas.capacity() {
                        suffix_deltas[i as usize] = 0;
                    }
                    self.flush_terms_dict_block(
                        &mut header_buffer,
                        &mut bytes_buffer,
                        &suffix_deltas,
                    )?;
                }
                // write addresses of indexed terms
                term_address.finish(&mut address_buffer)?;
            }

            let index_start_fp = self.data.file_pointer();
            address_buffer.write_to(&mut self.data)?;
            self.meta.write_vint(min_length)?;
            self.meta.write_vint(max_length)?;
            self.meta.write_vlong(count)?;
            self.meta.write_long(start_fp)?;
            self.meta.write_long(index_start_fp)?;
            self.meta.write_vint(PACKED_VERSION_CURRENT)?;
            self.meta
                .write_vint(Lucene54DocValuesFormat::MONOTONIC_BLOCK_SIZE)?;

            values.reset();
            self.add_reverse_term_index(field_info, values, max_length)?;
        }

        Ok(())
    }

    /// writes term dictionary "block"
    /// first term is absolute encoded as vint length + bytes.
    /// lengths of subsequent N terms are encoded as either N bytes or N shorts.
    /// in the double-byte case, the first byte is indicated with -1.
    /// subsequent terms are encoded as byte suffixLength + bytes.
    fn flush_terms_dict_block(
        &mut self,
        header_buffer: &mut RAMOutputStream,
        bytes_buffer: &mut RAMOutputStream,
        suffix_deltas: &[i32],
    ) -> Result<()> {
        let mut two_byte = false;
        for i in 1..suffix_deltas.len() {
            if suffix_deltas[i] > 254 {
                two_byte = true;
                break;
            }
        }

        if two_byte {
            header_buffer.write_byte(255u8)?;
            for i in 1..suffix_deltas.len() {
                header_buffer.write_short(suffix_deltas[i] as i16)?;
            }
        } else {
            for i in 1..suffix_deltas.len() {
                header_buffer.write_byte(suffix_deltas[i] as u8)?;
            }
        }

        header_buffer.write_to(&mut self.data)?;
        header_buffer.reset();
        bytes_buffer.write_to(&mut self.data)?;
        bytes_buffer.reset();
        Ok(())
    }

    /// writes reverse term index: used for binary searching a term into a range of 64 blocks
    /// for every 64 blocks (1024 terms) we store a term, trimming any suffix unnecessary for
    /// comparison terms are written as a contiguous bytes, but never spanning 2^15 byte
    /// boundaries.
    fn add_reverse_term_index(
        &mut self,
        _field_info: &FieldInfo,
        values: &mut impl ReusableIterator<Item = Result<BytesRef>>,
        _max_length: i32,
    ) -> Result<()> {
        let mut count = 0i64;
        let mut prior_term: Vec<u8> = vec![];
        let start_fp = self.data.file_pointer();
        let mut paged_bytes = PagedBytes::new(15);
        {
            let mut addresses = MonotonicBlockPackedWriter::new(
                Lucene54DocValuesFormat::MONOTONIC_BLOCK_SIZE as usize,
            );

            for v in values {
                let v = v?;
                let term_position =
                    (count & Lucene54DocValuesFormat::REVERSE_INTERVAL_MASK as i64) as i32;
                if term_position == 0 {
                    let len = sort_key_length(&prior_term, v.bytes());
                    let index_term = BytesRef::new(&v.bytes()[0..len]);
                    addresses.add(
                        paged_bytes.copy_using_length_prefix(&index_term)?,
                        &mut self.data,
                    )?;
                } else if term_position == Lucene54DocValuesFormat::REVERSE_INTERVAL_MASK {
                    prior_term.resize(v.len(), 0);
                    prior_term.copy_from_slice(v.bytes());
                }

                count += 1;
            }

            addresses.finish(&mut self.data)?;
        }
        let num_bytes = paged_bytes.get_pointer();
        paged_bytes.freeze(true)?;
        let mut input = paged_bytes.get_input()?;
        self.meta.write_long(start_fp)?;
        self.data.write_vlong(num_bytes)?;
        self.data.copy_bytes(&mut input, num_bytes as usize)
    }

    fn unique_value_sets(
        doc_to_value_count: &mut impl ReusableIterator<Item = Result<u32>>,
        values: &mut impl ReusableIterator<Item = Result<Numeric>>,
    ) -> Result<Option<Vec<Vec<i64>>>> {
        let mut unique_value_sets: HashSet<u64> = HashSet::new();
        let mut result = vec![];
        let mut total_dict_size = 0;

        for count in doc_to_value_count {
            let count = count? as usize;
            if count > 256 {
                return Ok(None);
            }

            let mut doc_values: Vec<i64> = Vec::with_capacity(count);

            for _i in 0..count {
                let value = values.next().unwrap()?;
                doc_values.push(value.long_value());
            }

            let hash_code = hash_vec(&doc_values);
            if unique_value_sets.contains(&hash_code) {
                continue;
            }

            total_dict_size += doc_values.len();
            if total_dict_size > 256 {
                return Ok(None);
            }

            result.push(doc_values);
            unique_value_sets.insert(hash_code);
        }

        result.sort();

        Ok(Some(result))
    }

    fn write_dictionary(&mut self, unique_value_sets: &[Vec<i64>]) -> Result<()> {
        let mut length_sum = 0;
        for longs in unique_value_sets {
            length_sum += longs.len();
        }

        self.meta.write_int(length_sum as i32)?;
        for value_set in unique_value_sets {
            for v in value_set {
                self.meta.write_long(*v)?;
            }
        }

        self.meta.write_int(unique_value_sets.len() as i32)?;
        for value_set in unique_value_sets {
            self.meta.write_int(value_set.len() as i32)?;
        }

        Ok(())
    }

    fn doc_to_set_id<'a, RI1, RI2>(
        unique_value_sets: &'a [Vec<i64>],
        doc_to_value_count: &'a mut RI1,
        values: &'a mut RI2,
    ) -> SetIdIter<'a, RI1, RI2>
    where
        RI1: ReusableIterator<Item = Result<u32>>,
        RI2: ReusableIterator<Item = Result<Numeric>>,
    {
        let mut set_ids: HashMap<u64, i32> = HashMap::new();
        let mut i = 0;
        for set in unique_value_sets {
            set_ids.insert(hash_vec(set), i);
            i += 1;
        }
        debug_assert_eq!(i as usize, unique_value_sets.len());

        SetIdIter::new(doc_to_value_count, values, set_ids)
    }

    fn add_ord_index(
        &mut self,
        field_info: &FieldInfo,
        values: &mut dyn ReusableIterator<Item = Result<u32>>,
    ) -> Result<()> {
        self.meta.write_vint(field_info.number as i32)?;
        self.meta.write_byte(Lucene54DocValuesFormat::NUMERIC)?;
        self.meta
            .write_vint(Lucene54DocValuesFormat::MONOTONIC_COMPRESSED)?;
        self.meta.write_long(-1)?;
        self.meta.write_long(self.data.file_pointer())?;
        self.meta.write_vlong(self.max_doc as i64)?;
        self.meta
            .write_vint(Lucene54DocValuesFormat::DIRECT_MONOTONIC_BLOCK_SHIFT)?;

        {
            let mut writer = DirectMonotonicWriter::get_instance(
                &mut self.meta,
                &mut self.data,
                self.max_doc as i64 + 1,
                Lucene54DocValuesFormat::DIRECT_MONOTONIC_BLOCK_SHIFT,
            )?;
            let mut addr = 0i64;
            writer.add(addr)?;
            for v in values {
                let v = v?;
                addr += v as i64;
                writer.add(addr)?;
            }
            writer.finish()?;
        }
        self.meta.write_long(self.data.file_pointer())
    }
}

impl<O: IndexOutput> DocValuesConsumer for Lucene54DocValuesConsumer<O> {
    fn add_numeric_field(
        &mut self,
        field_info: &FieldInfo,
        values: &mut impl ReusableIterator<Item = Result<Numeric>>,
    ) -> Result<()> {
        self.add_numeric(field_info, values, NumberType::VALUE)
    }

    fn add_binary_field(
        &mut self,
        field_info: &FieldInfo,
        values: &mut impl ReusableIterator<Item = Result<BytesRef>>,
    ) -> Result<()> {
        // write the bytes data
        self.meta.write_vint(field_info.number as i32)?;
        self.meta.write_byte(Lucene54DocValuesFormat::BINARY)?;
        let mut min_length = i32::max_value();
        let mut max_length = i32::min_value();
        let start_fp = self.data.file_pointer();
        let mut count = 0i64;
        let mut missing_count = 0i64;

        loop {
            let v = match values.next() {
                None => {
                    break;
                }
                Some(r) => r?,
            };
            let length = v.len() as i32;
            if length == 0 {
                missing_count += 1;
            }

            min_length = length.min(min_length);
            max_length = length.max(max_length);
            if length > 0 {
                self.data.write_bytes(v.bytes(), 0, length as usize)?;
            }

            count += 1;
        }

        let v = if min_length == max_length {
            Lucene54DocValuesFormat::BINARY_FIXED_UNCOMPRESSED
        } else {
            Lucene54DocValuesFormat::BINARY_VARIABLE_UNCOMPRESSED
        };
        self.meta.write_vint(v)?;
        if missing_count == 0 {
            self.meta
                .write_long(Lucene54DocValuesFormat::ALL_LIVE as i64)?;
        } else if missing_count == count {
            self.meta
                .write_long(Lucene54DocValuesFormat::ALL_MISSING as i64)?;
        } else {
            self.meta.write_long(self.data.file_pointer())?;
            values.reset();
            self.write_missing_bitset_bytes(values)?;
        }

        self.meta.write_vint(min_length)?;
        self.meta.write_vint(max_length)?;
        self.meta.write_vlong(count)?;
        self.meta.write_long(start_fp)?;

        // if minLength == maxLength, it's a fixed-length bytes, we are done (the addresses are
        // implicit) otherwise, we need to record the length fields...

        if min_length != max_length {
            self.meta.write_long(self.data.file_pointer())?;
            self.meta
                .write_vint(Lucene54DocValuesFormat::DIRECT_MONOTONIC_BLOCK_SHIFT)?;

            {
                let mut writer = DirectMonotonicWriter::get_instance(
                    &mut self.meta,
                    &mut self.data,
                    count + 1,
                    Lucene54DocValuesFormat::DIRECT_MONOTONIC_BLOCK_SHIFT,
                )?;
                let mut addr = 0;
                writer.add(addr)?;
                values.reset();
                for v in values {
                    let v = v?;
                    if !v.is_empty() {
                        addr += v.len() as i64;
                    }
                    writer.add(addr)?;
                }
                writer.finish()?;
            }
            self.meta.write_long(self.data.file_pointer())?;
        }
        Ok(())
    }

    fn add_sorted_field(
        &mut self,
        field_info: &FieldInfo,
        values: &mut impl ReusableIterator<Item = Result<BytesRef>>,
        doc_to_ord: &mut impl ReusableIterator<Item = Result<Numeric>>,
    ) -> Result<()> {
        self.meta.write_vint(field_info.number as i32)?;
        self.meta.write_byte(Lucene54DocValuesFormat::SORTED)?;
        self.add_terms_dict(field_info, values)?;
        self.add_numeric(field_info, doc_to_ord, NumberType::ORDINAL)
    }

    fn add_sorted_numeric_field(
        &mut self,
        field_info: &FieldInfo,
        values: &mut impl ReusableIterator<Item = Result<Numeric>>,
        doc_to_value_count: &mut impl ReusableIterator<Item = Result<u32>>,
    ) -> Result<()> {
        self.meta.write_vint(field_info.number as i32)?;
        self.meta
            .write_byte(Lucene54DocValuesFormat::SORTED_NUMERIC)?;
        if is_single_valued(doc_to_value_count)? {
            self.meta
                .write_vint(Lucene54DocValuesFormat::SORTED_SINGLE_VALUED)?;
            // The field is single-valued, we can encode it as NUMERIC
            self.add_numeric_field(
                field_info,
                &mut singleton_view(doc_to_value_count, values, Numeric::Null),
            )?;
        } else if let Some(ref unique_value_sets) =
            Lucene54DocValuesConsumer::<O>::unique_value_sets(doc_to_value_count, values)?
        {
            self.meta
                .write_vint(Lucene54DocValuesFormat::SORTED_SET_TABLE)?;
            // write the set_id -> values mapping
            self.write_dictionary(unique_value_sets)?;
            // write the doc -> set_id as a numeric field
            doc_to_value_count.reset();
            values.reset();
            self.add_numeric(
                field_info,
                &mut Lucene54DocValuesConsumer::<O>::doc_to_set_id(
                    unique_value_sets,
                    doc_to_value_count,
                    values,
                ),
                NumberType::ORDINAL,
            )?;
        } else {
            self.meta
                .write_vint(Lucene54DocValuesFormat::SORTED_WITH_ADDRESSES)?;
            // write the stream of values as a numeric field
            values.reset();
            self.add_numeric(field_info, values, NumberType::VALUE)?;
            // write the doc -> ord count as a absolute index to the stream
            doc_to_value_count.reset();
            self.add_ord_index(field_info, doc_to_value_count)?;
        }
        Ok(())
    }

    fn add_sorted_set_field(
        &mut self,
        field_info: &FieldInfo,
        values: &mut impl ReusableIterator<Item = Result<BytesRef>>,
        doc_to_ord_count: &mut impl ReusableIterator<Item = Result<u32>>,
        ords: &mut impl ReusableIterator<Item = Result<Numeric>>,
    ) -> Result<()> {
        self.meta.write_vint(field_info.number as i32)?;
        self.meta.write_byte(Lucene54DocValuesFormat::SORTED_SET)?;
        if is_single_valued(doc_to_ord_count)? {
            self.meta
                .write_vint(Lucene54DocValuesFormat::SORTED_SINGLE_VALUED)?;
            // The field is single-valued, we can encode it as NUMERIC
            self.add_sorted_field(
                field_info,
                values,
                &mut singleton_view(doc_to_ord_count, ords, Numeric::Long(-1)),
            )?;
        } else if let Some(ref unique_value_sets) =
            Lucene54DocValuesConsumer::<O>::unique_value_sets(doc_to_ord_count, ords)?
        {
            self.meta
                .write_vint(Lucene54DocValuesFormat::SORTED_SET_TABLE)?;
            // write the set_id -> values mapping
            self.write_dictionary(unique_value_sets)?;
            // write the ord -> byte[] as a binary field
            self.add_terms_dict(field_info, values)?;
            // write the doc -> set_id as a numeric field
            doc_to_ord_count.reset();
            ords.reset();
            self.add_numeric(
                field_info,
                &mut Lucene54DocValuesConsumer::<O>::doc_to_set_id(
                    unique_value_sets,
                    doc_to_ord_count,
                    ords,
                ),
                NumberType::ORDINAL,
            )?;
        } else {
            self.meta
                .write_vint(Lucene54DocValuesFormat::SORTED_WITH_ADDRESSES)?;
            // write the ord -> byte[] as a binary field
            self.add_terms_dict(field_info, values)?;
            // write the stream of values as a numeric field

            ords.reset();
            self.add_numeric(field_info, ords, NumberType::ORDINAL)?;
            // write the doc -> ord count as a absolute index to the stream
            doc_to_ord_count.reset();
            self.add_ord_index(field_info, doc_to_ord_count)?;
        }

        Ok(())
    }
}

impl<O: IndexOutput> Drop for Lucene54DocValuesConsumer<O> {
    fn drop(&mut self) {
        let mut _success = false;
        // write EOF marker
        let _ = self.meta.write_vint(-1);
        // write checksum
        let _ = codec_util::write_footer(&mut self.meta);
        let _ = codec_util::write_footer(&mut self.data);
    }
}
