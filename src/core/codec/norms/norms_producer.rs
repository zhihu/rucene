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

use core::codec::doc_values::NumericDocValues;
use core::codec::field_infos::{FieldInfo, FieldInfos};
use core::codec::norms::norms::{VERSION_CURRENT, VERSION_START};
use core::codec::norms::NormsProducer;
use core::codec::segment_infos::{segment_file_name, SegmentReadState};
use core::codec::{codec_util, Codec};
use core::store::directory::Directory;
use core::store::io::{IndexInput, RandomAccessInput};
use core::util::DocId;
use error::ErrorKind::{CorruptIndex, IllegalArgument};
use error::Result;
use std::collections::HashMap;

#[derive(Debug)]
struct NormsEntry {
    bytes_per_value: u8,
    offset: u64,
}

/// Reader for `Lucene53NormsFormat`
pub struct Lucene53NormsProducer {
    max_doc: DocId,
    data: Box<dyn IndexInput>,
    entries: HashMap<i32, NormsEntry>,
}

impl Lucene53NormsProducer {
    pub fn new<D: Directory, DW: Directory, C: Codec>(
        state: &SegmentReadState<'_, D, DW, C>,
        data_codec: &str,
        data_extension: &str,
        meta_codec: &str,
        meta_extension: &str,
    ) -> Result<Lucene53NormsProducer> {
        let max_doc = state.segment_info.max_doc() as DocId;
        let meta_name = segment_file_name(
            &state.segment_info.name,
            &state.segment_suffix,
            meta_extension,
        );

        // read in the entries from the metadata file.
        let mut checksum_input = state
            .directory
            .open_checksum_input(&meta_name, &state.context)?;
        let meta_version = codec_util::check_index_header(
            &mut checksum_input,
            meta_codec,
            VERSION_START,
            VERSION_CURRENT,
            state.segment_info.get_id(),
            &state.segment_suffix,
        )?;
        let mut entries = HashMap::new();
        Self::read_fields(&mut checksum_input, &state.field_infos, &mut entries)?;
        codec_util::check_footer(&mut checksum_input)?;

        let data_name = segment_file_name(
            &state.segment_info.name,
            &state.segment_suffix,
            data_extension,
        );
        let mut data = state.directory.open_input(&data_name, &state.context)?;
        let data_version = codec_util::check_index_header(
            data.as_mut(),
            data_codec,
            VERSION_START,
            VERSION_CURRENT,
            state.segment_info.get_id(),
            &state.segment_suffix,
        )?;

        if data_version != meta_version {
            bail!(CorruptIndex(format!(
                "Format versions mismatch: meta={}, data={}",
                meta_version, data_version
            )))
        }

        codec_util::retrieve_checksum(data.as_mut())?;

        Ok(Lucene53NormsProducer {
            max_doc,
            data,
            entries,
        })
    }

    fn read_fields<T: IndexInput + ?Sized>(
        input: &mut T,
        infos: &FieldInfos,
        norms: &mut HashMap<i32, NormsEntry>,
    ) -> Result<()> {
        loop {
            let field_num = input.read_vint()?;
            if field_num == -1 {
                break;
            }
            let field_info = infos
                .field_info_by_number(field_num as u32)
                .ok_or_else(|| IllegalArgument(format!("Invalid field number: {}", field_num)))?;
            if !field_info.has_norms() {
                bail!(CorruptIndex(format!("Invalid field: {}", field_info.name)))
            }
            let bytes_per_value = input.read_byte()?;
            match bytes_per_value {
                0 | 1 | 2 | 4 | 8 => {}
                _ => {
                    bail!(CorruptIndex(format!("Invalid field number: {}", field_num)));
                }
            }
            let offset = input.read_long()? as u64;
            norms.insert(
                field_info.number as i32,
                NormsEntry {
                    bytes_per_value,
                    offset,
                },
            );
        }
        Ok(())
    }
}

impl NormsProducer for Lucene53NormsProducer {
    fn norms(&self, field: &FieldInfo) -> Result<Box<dyn NumericDocValues>> {
        debug_assert!(self.entries.contains_key(&(field.number as i32)));

        let entry = &self.entries[&(field.number as i32)];
        if entry.bytes_per_value == 0 {
            return Ok(Box::new(ScalarNumericDocValue(entry.offset as i64)));
        }
        match entry.bytes_per_value {
            1 => {
                let slice = self
                    .data
                    .random_access_slice(entry.offset as i64, i64::from(self.max_doc))?;
                let consumer: fn(&dyn RandomAccessInput, DocId) -> Result<i64> =
                    move |slice, doc_id| slice.read_byte(u64::from(doc_id as u32)).map(i64::from);
                Ok(Box::new(RandomAccessNumericDocValues::new(slice, consumer)))
            }
            2 => {
                let slice = self
                    .data
                    .random_access_slice(entry.offset as i64, i64::from(self.max_doc) * 2)?;
                let consumer: fn(&dyn RandomAccessInput, DocId) -> Result<i64> =
                    move |slice, doc_id| {
                        slice
                            .read_short(u64::from(doc_id as u32) << 1)
                            .map(i64::from)
                    };
                Ok(Box::new(RandomAccessNumericDocValues::new(slice, consumer)))
            }
            4 => {
                let slice = self
                    .data
                    .random_access_slice(entry.offset as i64, i64::from(self.max_doc) * 4)?;
                let consumer: fn(&dyn RandomAccessInput, DocId) -> Result<i64> =
                    move |slice, doc_id| {
                        slice.read_int(u64::from(doc_id as u32) << 2).map(i64::from)
                    };
                Ok(Box::new(RandomAccessNumericDocValues::new(slice, consumer)))
            }
            8 => {
                let slice = self
                    .data
                    .random_access_slice(entry.offset as i64, i64::from(self.max_doc) * 8)?;
                let consumer: fn(&dyn RandomAccessInput, DocId) -> Result<i64> =
                    move |slice, doc_id| {
                        slice
                            .read_long(u64::from(doc_id as u32) << 3)
                            .map(i64::from)
                    };
                Ok(Box::new(RandomAccessNumericDocValues::new(slice, consumer)))
            }
            x => bail!(CorruptIndex(format!("Invalid norm bytes size: {}", x))),
        }
    }
}

struct ScalarNumericDocValue(i64);

impl NumericDocValues for ScalarNumericDocValue {
    fn get(&self, _doc_id: DocId) -> Result<i64> {
        Ok(self.0)
    }
}

struct RandomAccessNumericDocValues<F>
where
    F: Fn(&dyn RandomAccessInput, DocId) -> Result<i64> + Send,
{
    input: Box<dyn RandomAccessInput>,
    consumer: F,
}

impl<F> RandomAccessNumericDocValues<F>
where
    F: Fn(&dyn RandomAccessInput, DocId) -> Result<i64> + Send,
{
    fn new(input: Box<dyn RandomAccessInput>, consumer: F) -> RandomAccessNumericDocValues<F> {
        RandomAccessNumericDocValues { input, consumer }
    }
}

impl<F> NumericDocValues for RandomAccessNumericDocValues<F>
where
    F: Fn(&dyn RandomAccessInput, DocId) -> Result<i64> + Send + Sync,
{
    fn get(&self, doc_id: DocId) -> Result<i64> {
        (&self.consumer)(self.input.as_ref(), doc_id)
    }
}
