use error::*;

use core::codec::codec_util;
use core::codec::format::PointsFormat;
use core::codec::writer::PointsWriter;
use core::index::point_values::{IntersectVisitor, PointValues};
use core::index::segment_file_name;
use core::index::{FieldInfos, SegmentReadState, SegmentWriteState};
use core::store::IndexInput;
use core::util::bkd::BKDReader;
use std::collections::HashMap;
use std::sync::Arc;

const DATA_CODEC_NAME: &str = "Lucene60PointsFormatData";
const META_CODEC_NAME: &str = "Lucene60PointsFormatMeta";

// Filename extension for the leaf blocks
const DATA_EXTENSION: &str = "dim";
// Filename extension for the index per field
const INDEX_EXTENSION: &str = "dii";

const DATA_VERSION_START: i32 = 0;
const DATA_VERSION_CURRENT: i32 = DATA_VERSION_START;

const INDEX_VERSION_START: i32 = 0;
const INDEX_VERSION_CURRENT: i32 = INDEX_VERSION_START;

pub struct Lucene60PointsFormat;

impl Default for Lucene60PointsFormat {
    fn default() -> Lucene60PointsFormat {
        Lucene60PointsFormat {}
    }
}

impl PointsFormat for Lucene60PointsFormat {
    fn fields_writer(&self, _state: &SegmentWriteState) -> Result<Box<PointsWriter>> {
        unimplemented!()
    }

    fn fields_reader(&self, state: &SegmentReadState) -> Result<Box<PointValues>> {
        Ok(Box::new(Lucene60PointsReader::new(state)?))
    }
}

pub struct Lucene60PointsReader {
    data_in: Box<IndexInput>,
    field_infos: Arc<FieldInfos>,
    readers: HashMap<i32, BKDReader>,
}

impl Lucene60PointsReader {
    pub fn new(read_state: &SegmentReadState) -> Result<Lucene60PointsReader> {
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
            .open_checksum_input(&index_file_name, &read_state.context)?;
        codec_util::check_index_header(
            index_in.as_mut(),
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
        codec_util::check_footer(index_in.as_mut())?;

        let data_file_name = segment_file_name(
            &read_state.segment_info.name,
            &read_state.segment_suffix,
            DATA_EXTENSION,
        );

        let mut data_in = read_state
            .directory
            .open_input(&data_file_name, &read_state.context)?;
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
            data_in,
            field_infos: Arc::clone(&read_state.field_infos),
            readers,
        })
    }

    pub fn bkd_reader(&self, field_name: &str) -> Result<Option<&BKDReader>> {
        if let Some(field_info) = self.field_infos.field_info_by_name(field_name) {
            if field_info.point_dimension_count <= 0 {
                bail!(ErrorKind::IllegalArgument(format!(
                    "field '{}' did not index point values!",
                    field_name
                )));
            }
            Ok(self.readers.get(&field_info.number))
        } else {
            bail!(ErrorKind::IllegalArgument(format!(
                "field '{}' is unrecognized!",
                field_name
            )));
        }
    }
}

impl PointValues for Lucene60PointsReader {
    fn intersect(&self, field_name: &str, visitor: &mut IntersectVisitor) -> Result<()> {
        // Schema ghost corner case!  This field did index points in the past, but
        // now all docs having this point field were deleted in this segment:
        self.bkd_reader(field_name)?
            .map(|reader| reader.intersect(visitor))
            .unwrap_or(Ok(()))
    }

    fn min_packed_value(&self, field_name: &str) -> Result<Vec<u8>> {
        // Schema ghost corner case!  This field did index points in the past, but
        // now all docs having this point field were deleted in this segment:
        Ok(self.bkd_reader(field_name)?
            .map(|reader| reader.min_packed_value.clone())
            .unwrap_or_default())
    }

    fn max_packed_value(&self, field_name: &str) -> Result<Vec<u8>> {
        // Schema ghost corner case!  This field did index points in the past, but
        // now all docs having this point field were deleted in this segment:
        Ok(self.bkd_reader(field_name)?
            .map(|reader| reader.max_packed_value.clone())
            .unwrap_or_default())
    }

    fn num_dimensions(&self, field_name: &str) -> Result<usize> {
        // Schema ghost corner case!  This field did index points in the past, but
        // now all docs having this point field were deleted in this segment:
        Ok(self.bkd_reader(field_name)?
            .map(|reader| reader.num_dims)
            .unwrap_or(0usize))
    }

    fn bytes_per_dimension(&self, field_name: &str) -> Result<usize> {
        // Schema ghost corner case!  This field did index points in the past, but
        // now all docs having this point field were deleted in this segment:
        Ok(self.bkd_reader(field_name)?
            .map(|reader| reader.bytes_per_dim)
            .unwrap_or(0usize))
    }

    fn size(&self, field_name: &str) -> Result<i64> {
        // Schema ghost corner case!  This field did index points in the past, but
        // now all docs having this point field were deleted in this segment:
        Ok(self.bkd_reader(field_name)?
            .map(|reader| reader.point_count as i64)
            .unwrap_or(0i64))
    }

    fn doc_count(&self, field_name: &str) -> Result<i32> {
        // Schema ghost corner case!  This field did index points in the past, but
        // now all docs having this point field were deleted in this segment:
        Ok(self.bkd_reader(field_name)?
            .map(|reader| reader.doc_count)
            .unwrap_or(0i32))
    }
}
