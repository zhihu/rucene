use core::codec::codec_util;
use core::codec::lucene53::norms;
use core::codec::NormsConsumer;
use core::index::{segment_file_name, FieldInfo, SegmentWriteState};
use core::store::IndexOutput;
use core::util::numeric::Numeric;
use core::util::ReusableIterator;

use error::Result;

pub struct Lucene53NormsConsumer {
    data: Box<IndexOutput>,
    meta: Box<IndexOutput>,
    max_doc: i32,
}

impl Lucene53NormsConsumer {
    pub fn new(
        state: &SegmentWriteState,
        data_codec: &str,
        data_extension: &str,
        meta_codec: &str,
        meta_extension: &str,
    ) -> Result<Lucene53NormsConsumer> {
        let data_name = segment_file_name(
            &state.segment_info.name,
            &state.segment_suffix,
            data_extension,
        );
        let mut data = state.directory.create_output(&data_name, &state.context)?;
        codec_util::write_index_header(
            data.as_mut(),
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
            meta.as_mut(),
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
    fn add_constant(&mut self, constant: i64) {
        self.meta.write_byte(0 as u8);
        self.meta.write_long(constant);
    }

    fn add_byte(
        &mut self,
        min_value: i64,
        max_value: i64,
        values: &mut ReusableIterator<Item = Result<Numeric>>,
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
        self.meta.write_long(self.data.file_pointer());
        loop {
            if let Some(Ok(nv)) = values.next() {
                match len {
                    1 => self.data.write_byte(nv.byte_value() as u8)?,
                    2 => self.data.write_short(nv.short_value())?,
                    4 => self.data.write_int(nv.int_value())?,
                    8 => self.data.write_long(nv.long_value())?,
                    _ => unreachable!(),
                }
            } else {
                break;
            }
        }
        values.reset();
        Ok(())
    }
}

impl NormsConsumer for Lucene53NormsConsumer {
    fn add_norms_field(
        &mut self,
        field_info: &FieldInfo,
        values: &mut ReusableIterator<Item = Result<Numeric>>,
    ) -> Result<()> {
        self.meta.write_vint(field_info.number as i32);
        let mut min_value = i64::max_value();
        let mut max_value = i64::min_value();
        let mut count = 0;
        loop {
            if let Some(nv) = values.next() {
                let v = nv?.long_value();
                min_value = v.min(min_value);
                max_value = v.max(max_value);
                count += 1;
            } else {
                break;
            }
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
            self.add_constant(min_value);
        } else {
            self.add_byte(min_value, max_value, values);
        }
        Ok(())
    }
}

impl Drop for Lucene53NormsConsumer {
    fn drop(&mut self) {
        let mut _success = false;
        // write EOF marker
        let _ = self.meta.write_vint(-1);
        // write checksum
        let _ = codec_util::write_footer(self.meta.as_mut());
        let _ = codec_util::write_footer(self.data.as_mut());
    }
}
