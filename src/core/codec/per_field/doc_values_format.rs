use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use core::codec::format::{self, DocValuesFormat};
use core::codec::{DocValuesProducer, DocValuesProducerRef};
use core::index::BinaryDocValues;
use core::index::NumericDocValues;
use core::index::SegmentReadState;
use core::index::SortedDocValues;
use core::index::SortedNumericDocValues;
use core::index::SortedSetDocValues;
use core::index::{DocValuesType, FieldInfo};
use core::util::BitsRef;

use error::ErrorKind::{IllegalArgument, IllegalState};
use error::Result;

const PER_FIELD_NAME: &str = "PerFieldDV40";

/// `FieldInfo` attribute name used to store the format name for each field
const PER_FIELD_FORMAT_KEY: &str = "PerFieldDocValuesFormat.format";

/// `FieldInfo` attribute name used to store the segment suffix name for each field
const PER_FIELD_SUFFIX_KEY: &str = "PerFieldDocValuesFormat.suffix";

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
}

pub struct DocValuesFieldsReader {
    fields: BTreeMap<String, DocValuesProducerRef>,
    formats: HashMap<String, DocValuesProducerRef>,
}

impl DocValuesFieldsReader {
    pub fn new(state: &SegmentReadState) -> Result<DocValuesFieldsReader> {
        let mut fields = BTreeMap::new();
        let mut formats = HashMap::new();
        for (name, info) in &state.field_infos.by_name {
            if info.doc_values_type == DocValuesType::Null {
                continue;
            }
            if let Some(format) = info.attributes.get(PER_FIELD_FORMAT_KEY) {
                if let Some(suffix) = info.attributes.get(PER_FIELD_SUFFIX_KEY) {
                    let dv_format = format::doc_values_format_for_name(format)?;
                    let segment_suffix =
                        get_full_segment_suffix(&state.segment_suffix, get_suffix(format, suffix));

                    if !formats.contains_key(&segment_suffix) {
                        let segment_read_state =
                            SegmentReadState::with_suffix(state, &segment_suffix);
                        formats.insert(
                            segment_suffix.clone(),
                            Arc::from(dv_format.fields_producer(&segment_read_state)?),
                        );
                    }
                    fields.insert(
                        name.to_string(),
                        Arc::clone(formats.get(&segment_suffix).as_ref().unwrap()),
                    );
                } else {
                    bail!(IllegalState(format!(
                        "Missing attribute {} for field: {}",
                        PER_FIELD_SUFFIX_KEY, name
                    )));
                }
            }
        }

        Ok(DocValuesFieldsReader { fields, formats })
    }
}

impl DocValuesProducer for DocValuesFieldsReader {
    fn get_numeric(&self, field: &FieldInfo) -> Result<Box<NumericDocValues>> {
        match self.fields.get(&field.name) {
            Some(producer) => producer.get_numeric(field),
            None => bail!(IllegalArgument(format!{
                "DocValuesType of field {} isn't Numeric",
                field.name
            })),
        }
    }

    fn get_binary(&self, field: &FieldInfo) -> Result<Box<BinaryDocValues>> {
        match self.fields.get(&field.name) {
            Some(producer) => producer.get_binary(field),
            None => bail!(IllegalArgument(format!{
                "DocValuesType of field {} isn't Binary",
                field.name
            })),
        }
    }

    fn get_sorted(&self, field: &FieldInfo) -> Result<Box<SortedDocValues>> {
        match self.fields.get(&field.name) {
            Some(producer) => producer.get_sorted(field),
            None => bail!(IllegalArgument(format!{
                "DocValuesType of field {} isn't Sorted",
                field.name
            })),
        }
    }

    fn get_sorted_numeric(&self, field: &FieldInfo) -> Result<Box<SortedNumericDocValues>> {
        match self.fields.get(&field.name) {
            Some(producer) => producer.get_sorted_numeric(field),
            None => bail!(IllegalArgument(format!{
                "DocValuesType of field {} isn't SortedNumeric",
                field.name
            })),
        }
    }

    fn get_sorted_set(&self, field: &FieldInfo) -> Result<Box<SortedSetDocValues>> {
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
        for format in self.formats.values() {
            format.check_integrity()?;
        }
        Ok(())
    }
}
