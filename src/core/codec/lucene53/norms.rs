use core::codec::codec_util;
use core::codec::format::NormsFormat;
use core::codec::NormsProducer;
use core::index::{segment_file_name, FieldInfo, FieldInfos, SegmentReadState};
use core::index::{NumericDocValues, NumericDocValuesContext};
use core::store::IndexInput;
use core::store::RandomAccessInput;
use core::util::DocId;
use error::ErrorKind::{CorruptIndex, IllegalArgument};
use error::*;
use std::collections::HashMap;
use std::sync::Arc;

const DATA_CODEC: &str = "Lucene53NormsData";
const DATA_EXTENSION: &str = "nvd";
const METADATA_CODEC: &str = "Lucene53NormsMetadata";
const METADATA_EXTENSION: &str = "nvm";
const VERSION_START: i32 = 0;
const VERSION_CURRENT: i32 = VERSION_START;

pub struct Lucene53NormsFormat;

impl Default for Lucene53NormsFormat {
    fn default() -> Lucene53NormsFormat {
        Lucene53NormsFormat {}
    }
}

impl NormsFormat for Lucene53NormsFormat {
    fn norms_producer(&self, state: &SegmentReadState) -> Result<Box<NormsProducer>> {
        Ok(Box::new(Lucene53NormsProducer::new(
            state,
            DATA_CODEC,
            DATA_EXTENSION,
            METADATA_CODEC,
            METADATA_EXTENSION,
        )?))
    }
}

struct NormsEntry {
    bytes_per_value: u8,
    offset: u64,
}

struct Lucene53NormsProducer {
    max_doc: DocId,
    data: Arc<IndexInput>,
    entries: HashMap<i32, NormsEntry>,
}

impl Lucene53NormsProducer {
    pub fn new(
        state: &SegmentReadState,
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

        let mut meta_version = -1;

        // read in the entries from the metadata file.
        let mut checksum_input = state
            .directory
            .open_checksum_input(&meta_name, &state.context)?;
        let result: Result<i32> = codec_util::check_index_header(
            checksum_input.as_mut(),
            meta_codec,
            VERSION_START,
            VERSION_CURRENT,
            state.segment_info.get_id(),
            &state.segment_suffix,
        );
        let mut entries = HashMap::new();
        if let Ok(v) = result {
            meta_version = v;
            Self::read_fields(checksum_input.as_mut(), &state.field_infos, &mut entries)?;
        }
        codec_util::check_footer(checksum_input.as_mut())?;

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
            data: Arc::from(data),
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
                .field_info_by_number(field_num)
                .ok_or_else(|| IllegalArgument(format!("Invalid field number: {}", field_num)))?;
            if !field_info.has_norms() {
                bail!(CorruptIndex(format!("Invalid field: {}", &field_info.name)))
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
                field_info.number,
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
    fn norms(&self, field: &FieldInfo) -> Result<Box<NumericDocValues>> {
        debug_assert!(self.entries.contains_key(&field.number));

        let entry = &self.entries[&field.number];
        if entry.bytes_per_value == 0 {
            return Ok(Box::new(ScalarNumericDocValue(entry.offset as i64)));
        }
        match entry.bytes_per_value {
            1 => {
                let slice = self.data
                    .random_access_slice(entry.offset as i64, i64::from(self.max_doc))?;
                let consumer: fn(&RandomAccessInput, DocId) -> Result<i64> =
                    move |slice, doc_id| slice.read_byte(i64::from(doc_id)).map(i64::from);
                Ok(Box::new(RandomAccessNumericDocValues::new(slice, consumer)))
            }
            2 => {
                let slice = self.data
                    .random_access_slice(entry.offset as i64, i64::from(self.max_doc) * 2)?;
                let consumer: fn(&RandomAccessInput, DocId) -> Result<i64> =
                    move |slice, doc_id| slice.read_short(i64::from(doc_id) << 1).map(i64::from);
                Ok(Box::new(RandomAccessNumericDocValues::new(slice, consumer)))
            }
            4 => {
                let slice = self.data
                    .random_access_slice(entry.offset as i64, i64::from(self.max_doc) * 4)?;
                let consumer: fn(&RandomAccessInput, DocId) -> Result<i64> =
                    move |slice, doc_id| slice.read_int(i64::from(doc_id) << 2).map(i64::from);
                Ok(Box::new(RandomAccessNumericDocValues::new(slice, consumer)))
            }
            8 => {
                let slice = self.data
                    .random_access_slice(entry.offset as i64, i64::from(self.max_doc) * 8)?;
                let consumer: fn(&RandomAccessInput, DocId) -> Result<i64> =
                    move |slice, doc_id| slice.read_long(i64::from(doc_id) << 3).map(i64::from);
                Ok(Box::new(RandomAccessNumericDocValues::new(slice, consumer)))
            }
            x => bail!(CorruptIndex(format!("Invalid norm bytes size: {}", x))),
        }
    }
}

struct ScalarNumericDocValue(i64);

impl NumericDocValues for ScalarNumericDocValue {
    fn get_with_ctx(
        &self,
        ctx: NumericDocValuesContext,
        _doc_id: DocId,
    ) -> Result<(i64, NumericDocValuesContext)> {
        Ok((self.0, ctx))
    }
}

struct RandomAccessNumericDocValues<F>
where
    F: Fn(&RandomAccessInput, DocId) -> Result<i64> + Send,
{
    input: Arc<RandomAccessInput>,
    consumer: F,
}

impl<F> RandomAccessNumericDocValues<F>
where
    F: Fn(&RandomAccessInput, DocId) -> Result<i64> + Send,
{
    fn new(input: Box<RandomAccessInput>, consumer: F) -> RandomAccessNumericDocValues<F> {
        RandomAccessNumericDocValues {
            input: Arc::from(input),
            consumer,
        }
    }
}

impl<F> NumericDocValues for RandomAccessNumericDocValues<F>
where
    F: Fn(&RandomAccessInput, DocId) -> Result<i64> + Send + Sync,
{
    fn get_with_ctx(
        &self,
        ctx: NumericDocValuesContext,
        doc_id: DocId,
    ) -> Result<(i64, NumericDocValuesContext)> {
        let consumer = &self.consumer;
        consumer(self.input.as_ref(), doc_id).map(|x| (x, ctx))
    }
}
