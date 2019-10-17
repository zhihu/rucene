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

use core::codec::field_infos::FieldInfo;
use core::codec::norms::norms;
use core::codec::norms::NormsConsumer;
use core::codec::segment_infos::{segment_file_name, SegmentWriteState};
use core::codec::{codec_util, Codec};
use core::store::directory::Directory;
use core::store::io::IndexOutput;
use core::util::Numeric;
use core::util::ReusableIterator;

use error::Result;

/// Writer for `Lucene53NormsFormat`
pub struct Lucene53NormsConsumer<O: IndexOutput> {
    data: O,
    meta: O,
    max_doc: i32,
}

impl<O: IndexOutput> Lucene53NormsConsumer<O> {
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
            norms::VERSION_CURRENT,
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
            norms::VERSION_CURRENT,
            state.segment_info.get_id(),
            &state.segment_suffix,
        )?;

        let max_doc = state.segment_info.max_doc();

        Ok(Lucene53NormsConsumer {
            data,
            meta,
            max_doc,
        })
    }
}

impl<O: IndexOutput> Lucene53NormsConsumer<O> {
    fn add_constant(&mut self, constant: i64) -> Result<()> {
        self.meta.write_byte(0 as u8)?;
        self.meta.write_long(constant)
    }

    fn add_byte(
        &mut self,
        min_value: i64,
        max_value: i64,
        values: &mut impl ReusableIterator<Item = Result<Numeric>>,
    ) -> Result<()> {
        let len = if min_value >= i8::min_value() as i64 && max_value <= i8::max_value() as i64 {
            1
        } else if min_value >= i16::min_value() as i64 && max_value <= i16::max_value() as i64 {
            2
        } else if min_value >= i32::min_value() as i64 && max_value <= i32::max_value() as i64 {
            4
        } else {
            8
        };
        self.meta.write_byte(len as u8)?;
        self.meta.write_long(self.data.file_pointer())?;
        while let Some(Ok(nv)) = values.next() {
            match len {
                1 => self.data.write_byte(nv.byte_value() as u8)?,
                2 => self.data.write_short(nv.short_value())?,
                4 => self.data.write_int(nv.int_value())?,
                8 => self.data.write_long(nv.long_value())?,
                _ => unreachable!(),
            }
        }
        values.reset();
        Ok(())
    }
}

impl<O: IndexOutput> NormsConsumer for Lucene53NormsConsumer<O> {
    fn add_norms_field(
        &mut self,
        field_info: &FieldInfo,
        values: &mut impl ReusableIterator<Item = Result<Numeric>>,
    ) -> Result<()> {
        self.meta.write_vint(field_info.number as i32)?;
        let mut min_value = i64::max_value();
        let mut max_value = i64::min_value();
        let mut count = 0;
        while let Some(nv) = values.next() {
            let v = nv?.long_value();
            min_value = v.min(min_value);
            max_value = v.max(max_value);
            count += 1;
        }
        values.reset();
        if count != self.max_doc as usize {
            bail!(
                "illegal norms data for field {}, expected count={}, got={}",
                field_info.name,
                self.max_doc,
                count
            );
        }
        if min_value == max_value {
            self.add_constant(min_value)?;
        } else {
            self.add_byte(min_value, max_value, values)?;
        }
        Ok(())
    }
}

impl<O: IndexOutput> Drop for Lucene53NormsConsumer<O> {
    fn drop(&mut self) {
        let mut _success = false;
        // write EOF marker
        let _ = self.meta.write_vint(-1);
        // write checksum
        let _ = codec_util::write_footer(&mut self.meta);
        let _ = codec_util::write_footer(&mut self.data);
    }
}
