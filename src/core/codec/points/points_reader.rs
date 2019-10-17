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

use error::{ErrorKind, Result};

use core::codec::field_infos::FieldInfos;
use core::codec::points::Lucene60PointsWriter;
use core::codec::points::PointsFormat;
use core::codec::points::{IntersectVisitor, PointValues};
use core::codec::segment_infos::{segment_file_name, SegmentReadState, SegmentWriteState};
use core::codec::{codec_util, Codec};
use core::store::directory::Directory;
use core::store::io::DataInput;
use core::util::bkd::BKDReader;

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

pub const DATA_CODEC_NAME: &str = "Lucene60PointsFormatData";
pub const META_CODEC_NAME: &str = "Lucene60PointsFormatMeta";

// Filename extension for the leaf blocks
pub const DATA_EXTENSION: &str = "dim";
// Filename extension for the index per field
pub const INDEX_EXTENSION: &str = "dii";

pub const DATA_VERSION_START: i32 = 0;
pub const DATA_VERSION_CURRENT: i32 = DATA_VERSION_START;

pub const INDEX_VERSION_START: i32 = 0;
pub const INDEX_VERSION_CURRENT: i32 = INDEX_VERSION_START;

#[derive(Copy, Clone)]
pub struct Lucene60PointsFormat;

impl PointsFormat for Lucene60PointsFormat {
    type Reader = Lucene60PointsReader;
    fn fields_writer<D: Directory, DW: Directory, C: Codec>(
        &self,
        state: &SegmentWriteState<D, DW, C>,
    ) -> Result<Lucene60PointsWriter<D, DW, C>> {
        Lucene60PointsWriter::new(state)
    }

    fn fields_reader<'a, D: Directory, DW: Directory, C: Codec>(
        &self,
        state: &SegmentReadState<'a, D, DW, C>,
    ) -> Result<Self::Reader> {
        Lucene60PointsReader::new(state)
    }
}

pub struct Lucene60PointsReader {
    // data_in: Box<dyn IndexInput>,
    field_infos: Arc<FieldInfos>,
    pub readers: HashMap<i32, BKDReader>,
}

impl Lucene60PointsReader {
    pub fn new<D: Directory, DW: Directory, C: Codec>(
        read_state: &SegmentReadState<'_, D, DW, C>,
    ) -> Result<Lucene60PointsReader> {
        // let field_infos = Arc::clone(&read_state.field_infos);
        let index_file_name = segment_file_name(
            &read_state.segment_info.name,
            &read_state.segment_suffix,
            INDEX_EXTENSION,
        );
        let mut field_to_file_offset: HashMap<i32, i64> = HashMap::new();

        // read index file
        let mut index_in = read_state
            .directory
            .open_checksum_input(&index_file_name, read_state.context)?;
        codec_util::check_index_header(
            &mut index_in,
            META_CODEC_NAME,
            INDEX_VERSION_START,
            INDEX_VERSION_CURRENT,
            &read_state.segment_info.id,
            &read_state.segment_suffix,
        )?;
        let count = index_in.read_vint()?;
        for _ in 0..count {
            let field_number = index_in.read_vint()?;
            let fp = index_in.read_vlong()?;
            field_to_file_offset.insert(field_number, fp);
        }
        codec_util::check_footer(&mut index_in)?;

        let data_file_name = segment_file_name(
            &read_state.segment_info.name,
            &read_state.segment_suffix,
            DATA_EXTENSION,
        );

        let mut data_in = read_state
            .directory
            .open_input(&data_file_name, read_state.context)?;
        codec_util::check_index_header(
            data_in.as_mut(),
            DATA_CODEC_NAME,
            DATA_VERSION_START,
            DATA_VERSION_CURRENT,
            &read_state.segment_info.id,
            &read_state.segment_suffix,
        )?;
        // NOTE: data file is too costly to verify checksum against all the bytes on open,
        // but for now we at least verify proper structure of the checksum footer: which looks
        // for FOOTER_MAGIC + algorithmID. This is cheap and can detect some forms of corruption
        // such as file truncation.
        codec_util::retrieve_checksum(data_in.as_mut())?;

        let mut readers = HashMap::new();
        for (field_number, fp) in &field_to_file_offset {
            data_in.seek(*fp)?;
            let reader = BKDReader::new(Arc::from(data_in.as_ref().clone()?))?;
            readers.insert(*field_number, reader);
        }

        Ok(Lucene60PointsReader {
            field_infos: Arc::clone(&read_state.field_infos),
            readers,
        })
    }

    pub fn bkd_reader(&self, field_name: &str) -> Result<Option<&BKDReader>> {
        if let Some(field_info) = self.field_infos.field_info_by_name(field_name) {
            if field_info.point_dimension_count == 0 {
                bail!(ErrorKind::IllegalArgument(format!(
                    "field '{}' did not index point values!",
                    field_name
                )));
            }
            Ok(self.readers.get(&(field_info.number as i32)))
        } else {
            bail!(ErrorKind::IllegalArgument(format!(
                "field '{}' is unrecognized!",
                field_name
            )));
        }
    }
}

impl PointValues for Lucene60PointsReader {
    fn intersect(&self, field_name: &str, visitor: &mut impl IntersectVisitor) -> Result<()> {
        // Schema ghost corner case!  This field did index points in the past, but
        // now all docs having this point field were deleted in this segment:
        self.bkd_reader(field_name)?
            .map(|reader| reader.intersect(visitor))
            .unwrap_or(Ok(()))
    }

    fn min_packed_value(&self, field_name: &str) -> Result<Vec<u8>> {
        // Schema ghost corner case!  This field did index points in the past, but
        // now all docs having this point field were deleted in this segment:
        Ok(self
            .bkd_reader(field_name)?
            .map(|reader| reader.min_packed_value.clone())
            .unwrap_or_default())
    }

    fn max_packed_value(&self, field_name: &str) -> Result<Vec<u8>> {
        // Schema ghost corner case!  This field did index points in the past, but
        // now all docs having this point field were deleted in this segment:
        Ok(self
            .bkd_reader(field_name)?
            .map(|reader| reader.max_packed_value.clone())
            .unwrap_or_default())
    }

    fn num_dimensions(&self, field_name: &str) -> Result<usize> {
        // Schema ghost corner case!  This field did index points in the past, but
        // now all docs having this point field were deleted in this segment:
        Ok(self
            .bkd_reader(field_name)?
            .map(|reader| reader.num_dims)
            .unwrap_or(0usize))
    }

    fn bytes_per_dimension(&self, field_name: &str) -> Result<usize> {
        // Schema ghost corner case!  This field did index points in the past, but
        // now all docs having this point field were deleted in this segment:
        Ok(self
            .bkd_reader(field_name)?
            .map(|reader| reader.bytes_per_dim)
            .unwrap_or(0usize))
    }

    fn size(&self, field_name: &str) -> Result<i64> {
        // Schema ghost corner case!  This field did index points in the past, but
        // now all docs having this point field were deleted in this segment:
        Ok(self
            .bkd_reader(field_name)?
            .map(|reader| reader.point_count as i64)
            .unwrap_or(0i64))
    }

    fn doc_count(&self, field_name: &str) -> Result<i32> {
        // Schema ghost corner case!  This field did index points in the past, but
        // now all docs having this point field were deleted in this segment:
        Ok(self
            .bkd_reader(field_name)?
            .map(|reader| reader.doc_count)
            .unwrap_or(0i32))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
