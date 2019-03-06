use std::collections::hash_map::Entry as HashMapEntry;
use std::collections::{BTreeMap, HashMap};
use std::mem;
use std::sync::Arc;

use core::codec::format::{self, doc_values_format_for_name, DocValuesFormat};
use core::codec::lucene54::Lucene54DocValuesFormat;
use core::codec::{DocValuesConsumer, DocValuesProducer, DocValuesProducerRef};
use core::index::BinaryDocValues;
use core::index::NumericDocValues;
use core::index::SortedDocValues;
use core::index::SortedNumericDocValues;
use core::index::SortedSetDocValues;
use core::index::{DocValuesType, FieldInfo};
use core::index::{SegmentReadState, SegmentWriteState};
use core::util::byte_ref::BytesRef;
use core::util::numeric::Numeric;
use core::util::{BitsRef, ReusableIterator};

use error::ErrorKind::{IllegalArgument, IllegalState};
use error::Result;

const PER_FIELD_NAME: &str = "PerFieldDV40";

/// `FieldInfo` attribute name used to store the format name for each field
pub const PER_FIELD_VALUE_FORMAT_KEY: &str = "PerFieldDocValuesFormat.format";

/// `FieldInfo` attribute name used to store the segment suffix name for each field
pub const PER_FIELD_VALUE_SUFFIX_KEY: &str = "PerFieldDocValuesFormat.suffix";

fn get_suffix(format: &str, suffix: &str) -> String {
    format!("{}_{}", format, suffix)
}

fn get_full_segment_suffix(outer_segment_suffix: &str, segment_suffix: String) -> String {
    if outer_segment_suffix.is_empty() {
        segment_suffix
    } else {
        format!("{}_{}", outer_segment_suffix, segment_suffix)
    }
}

pub struct PerFieldDocValuesFormat {
    name: String,
}

impl Default for PerFieldDocValuesFormat {
    fn default() -> Self {
        PerFieldDocValuesFormat {
            name: PER_FIELD_NAME.to_string(),
        }
    }
}

impl DocValuesFormat for PerFieldDocValuesFormat {
    fn name(&self) -> &str {
        &self.name
    }

    fn fields_producer(&self, state: &SegmentReadState) -> Result<Box<DocValuesProducer>> {
        let boxed = DocValuesFieldsReader::new(state)?;
        Ok(Box::new(boxed))
    }

    fn fields_consumer(&self, state: &SegmentWriteState) -> Result<Box<DocValuesConsumer>> {
        Ok(Box::new(DocValuesFieldsWriter::new(state)))
    }
}

pub struct DocValuesFieldsReader {
    fields: BTreeMap<String, DocValuesProducerRef>,
}

impl DocValuesFieldsReader {
    pub fn new(state: &SegmentReadState) -> Result<DocValuesFieldsReader> {
        let mut fields = BTreeMap::new();
        let mut formats = HashMap::new();
        for (name, info) in &state.field_infos.by_name {
            if info.doc_values_type == DocValuesType::Null {
                continue;
            }
            let attrs = info.attributes.read().unwrap();
            if let Some(format) = attrs.get(PER_FIELD_VALUE_FORMAT_KEY) {
                let suffix = attrs.get(PER_FIELD_VALUE_SUFFIX_KEY);
                match suffix {
                    None => bail!(IllegalState(format!(
                        "Missing attribute {} for field: {}",
                        PER_FIELD_VALUE_SUFFIX_KEY, name
                    ))),
                    Some(suffix) => {
                        let dv_format = format::doc_values_format_for_name(format)?;
                        let segment_suffix = get_full_segment_suffix(
                            &state.segment_suffix,
                            get_suffix(format, suffix),
                        );

                        match formats.entry(segment_suffix.clone()) {
                            HashMapEntry::Occupied(occupied) => {
                                fields.insert(name.to_string(), Arc::clone(occupied.get()));
                            }
                            HashMapEntry::Vacant(mut vacant) => {
                                let segment_read_state =
                                    SegmentReadState::with_suffix(state, &segment_suffix);
                                let dv_producer = dv_format.fields_producer(&segment_read_state)?;
                                let dv_producer = Arc::from(dv_producer);
                                vacant.insert(Arc::clone(&dv_producer));
                                fields.insert(name.to_string(), dv_producer);
                            }
                        }
                    }
                }
            }
        }

        Ok(DocValuesFieldsReader { fields })
    }

    fn copy_for_merge(producer: &DocValuesFieldsReader) -> Result<Self> {
        let mut new_fields = BTreeMap::new();
        for (field, prod) in &producer.fields {
            new_fields.insert(field.clone(), Arc::from(prod.get_merge_instance()?));
        }
        Ok(DocValuesFieldsReader { fields: new_fields })
    }
}

impl DocValuesProducer for DocValuesFieldsReader {
    fn get_numeric(&self, field: &FieldInfo) -> Result<Arc<NumericDocValues>> {
        match self.fields.get(&field.name) {
            Some(producer) => producer.get_numeric(field),
            None => bail!(IllegalArgument(format!{
                "DocValuesType of field {} isn't Numeric",
                field.name
            })),
        }
    }

    fn get_binary(&self, field: &FieldInfo) -> Result<Arc<BinaryDocValues>> {
        match self.fields.get(&field.name) {
            Some(producer) => producer.get_binary(field),
            None => bail!(IllegalArgument(format!{
                "DocValuesType of field {} isn't Binary",
                field.name
            })),
        }
    }

    fn get_sorted(&self, field: &FieldInfo) -> Result<Arc<SortedDocValues>> {
        match self.fields.get(&field.name) {
            Some(producer) => producer.get_sorted(field),
            None => bail!(IllegalArgument(format!{
                "DocValuesType of field {} isn't Sorted",
                field.name
            })),
        }
    }

    fn get_sorted_numeric(&self, field: &FieldInfo) -> Result<Arc<SortedNumericDocValues>> {
        match self.fields.get(&field.name) {
            Some(producer) => producer.get_sorted_numeric(field),
            None => bail!(IllegalArgument(format!{
                "DocValuesType of field {} isn't SortedNumeric",
                field.name
            })),
        }
    }

    fn get_sorted_set(&self, field: &FieldInfo) -> Result<Arc<SortedSetDocValues>> {
        match self.fields.get(&field.name) {
            Some(producer) => producer.get_sorted_set(field),
            None => bail!(IllegalArgument(format!{
                "DocValuesType of field {} isn't SortedSet",
                field.name
            })),
        }
    }

    fn get_docs_with_field(&self, field: &FieldInfo) -> Result<BitsRef> {
        match self.fields.get(&field.name) {
            Some(producer) => producer.get_docs_with_field(field),
            None => bail!(IllegalArgument(format!{
                "field {} for get_docs_with_field",
                field.name
            })),
        }
    }

    fn check_integrity(&self) -> Result<()> {
        // for format in self.formats.values() {
        // format.lock()?.check_integrity()?;
        // }
        Ok(())
    }

    fn get_merge_instance(&self) -> Result<Box<DocValuesProducer>> {
        Ok(Box::new(DocValuesFieldsReader::copy_for_merge(self)?))
    }
}

struct ConsumerAndSuffix {
    consumer: Box<DocValuesConsumer>,
    suffix: i32,
}

struct DocValuesFieldsWriter {
    formats: HashMap<String, ConsumerAndSuffix>,
    suffixes: HashMap<String, i32>,
    segment_write_state: SegmentWriteState,
}

impl DocValuesFieldsWriter {
    fn new(state: &SegmentWriteState) -> Self {
        DocValuesFieldsWriter {
            formats: HashMap::new(),
            suffixes: HashMap::new(),
            segment_write_state: state.clone(),
        }
    }

    fn get_instance(&mut self, field: &FieldInfo) -> Result<&mut DocValuesConsumer> {
        let mut format: Option<Box<DocValuesFormat>> = None;
        if field.dv_gen != -1 {
            // this means the field never existed in that segment, yet is applied updates
            if let Some(format_name) = field.attribute(PER_FIELD_VALUE_FORMAT_KEY) {
                format = Some(doc_values_format_for_name(&format_name)?);
            }
        }
        if format.is_none() {
            // TODO hard code for `PerFieldDocValuesFormat.getDocValuesFormatForField`
            format = Some(Box::new(Lucene54DocValuesFormat::default()));
        }

        let format = format.unwrap();
        let format_name = format.name().to_string();
        let prev = field.put_attribute(PER_FIELD_VALUE_FORMAT_KEY.to_string(), format_name.clone());
        if field.dv_gen == -1 && prev.is_some() {
            bail!(IllegalState(format!(
                "found existing value for {}, field={}, old={}, new={}",
                PER_FIELD_VALUE_FORMAT_KEY,
                field.name,
                prev.unwrap(),
                &format_name
            )));
        }

        let mut suffix: Option<i32> = None;
        if self.formats.contains_key(&format_name) {
            // we've already seen this format, so just grab its suffix
            debug_assert!(self.suffixes.contains_key(&format_name));
            suffix = Some(self.formats[&format_name].suffix);
        } else {
            // First time we are seeing this format; create a new instance
            if field.dv_gen != -1 {
                // even when dvGen is != -1, it can still be a new field, that never
                // existed in the segment, and therefore doesn't have the recorded
                // attributes yet.
                if let Some(suffix_att) = field.attribute(PER_FIELD_VALUE_SUFFIX_KEY) {
                    suffix = Some(suffix_att.parse()?);
                }
            }

            if suffix.is_none() {
                // bump the suffix
                suffix = self.suffixes.get(&format_name).map(|s| *s);
                if suffix.is_none() {
                    suffix = Some(0);
                } else {
                    suffix = Some(suffix.unwrap() + 1);
                }
            }
            self.suffixes.insert(format_name.clone(), suffix.unwrap());

            let segment_suffix = get_full_segment_suffix(
                &self.segment_write_state.segment_suffix,
                get_suffix(&format_name, &format!("{}", suffix.unwrap())),
            );
            let old_suffix =
                mem::replace(&mut self.segment_write_state.segment_suffix, segment_suffix);
            let consumer = ConsumerAndSuffix {
                consumer: format.fields_consumer(&self.segment_write_state)?,
                suffix: suffix.unwrap(),
            };
            self.formats.insert(format_name.clone(), consumer);
            self.segment_write_state.segment_suffix = old_suffix;
        }

        if let Some(p) = field.put_attribute(
            PER_FIELD_VALUE_SUFFIX_KEY.to_string(),
            format!("{}", suffix.unwrap()),
        ) {
            bail!(IllegalState(format!(
                "found existing value for {}, field={}, old={}, new={}",
                PER_FIELD_VALUE_SUFFIX_KEY,
                field.name,
                p,
                suffix.unwrap()
            )));
        }

        // TODO: we should only provide the "slice" of FIS
        // that this DVF actually sees ...
        Ok(self.formats
            .get_mut(&format_name)
            .unwrap()
            .consumer
            .as_mut())
    }
}

impl DocValuesConsumer for DocValuesFieldsWriter {
    fn add_numeric_field(
        &mut self,
        field_info: &FieldInfo,
        values: &mut ReusableIterator<Item = Result<Numeric>>,
    ) -> Result<()> {
        self.get_instance(field_info)?
            .add_numeric_field(field_info, values)
    }

    fn add_binary_field(
        &mut self,
        field_info: &FieldInfo,
        values: &mut ReusableIterator<Item = Result<BytesRef>>,
    ) -> Result<()> {
        self.get_instance(field_info)?
            .add_binary_field(field_info, values)
    }

    fn add_sorted_field(
        &mut self,
        field_info: &FieldInfo,
        values: &mut ReusableIterator<Item = Result<BytesRef>>,
        doc_to_ord: &mut ReusableIterator<Item = Result<Numeric>>,
    ) -> Result<()> {
        self.get_instance(field_info)?
            .add_sorted_field(field_info, values, doc_to_ord)
    }

    fn add_sorted_numeric_field(
        &mut self,
        field_info: &FieldInfo,
        values: &mut ReusableIterator<Item = Result<Numeric>>,
        doc_to_value_count: &mut ReusableIterator<Item = Result<u32>>,
    ) -> Result<()> {
        self.get_instance(field_info)?.add_sorted_numeric_field(
            field_info,
            values,
            doc_to_value_count,
        )
    }

    fn add_sorted_set_field(
        &mut self,
        field_info: &FieldInfo,
        values: &mut ReusableIterator<Item = Result<BytesRef>>,
        doc_to_ord_count: &mut ReusableIterator<Item = Result<u32>>,
        ords: &mut ReusableIterator<Item = Result<Numeric>>,
    ) -> Result<()> {
        self.get_instance(field_info)?.add_sorted_set_field(
            field_info,
            values,
            doc_to_ord_count,
            ords,
        )
    }
}
