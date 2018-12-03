use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use core::codec::format::{postings_format_for_name, PostingsFormat};
use core::codec::{FieldsProducer, FieldsProducerRef};
use core::index::term::TermsRef;
use core::index::Fields;
use core::index::{IndexOptions, SegmentReadState};
use error::*;

/// Name of this {@link PostingsFormat}. */
// const PER_FIELD_NAME: &str = "PerField40";
/// {@link FieldInfo} attribute name used to store the
/// format name for each field. */
const PER_FIELD_FORMAT_KEY: &str = "PerFieldPostingsFormat.format";

/// segment suffix name for each field. */
const PER_FIELD_SUFFIX_KEY: &str = "PerFieldPostingsFormat.suffix";

fn get_suffix(format: &str, suffix: &str) -> String {
    format!("{}_{}", format, suffix)
}

/// Enables per field postings support.
/// <p>
/// Note, when extending this class, the name ({@link #getName}) is
/// written into the index. In order for the field to be read, the
/// name must resolve to your implementation via {@link #forName(String)}.
/// This method uses Java's
/// {@link ServiceLoader Service Provider Interface} to resolve format names.
/// <p>
/// Files written by each posting format have an additional suffix containing the
/// format name. For example, in a per-field configuration instead of <tt>_1.prx</tt>
/// filenames would look like <tt>_1_Lucene40_0.prx</tt>.
/// @see ServiceLoader
/// @lucene.experimental
///
pub struct PerFieldPostingsFormat {}

impl Default for PerFieldPostingsFormat {
    fn default() -> PerFieldPostingsFormat {
        PerFieldPostingsFormat {}
    }
}

impl PostingsFormat for PerFieldPostingsFormat {
    fn fields_producer(&self, state: &SegmentReadState) -> Result<Box<FieldsProducer>> {
        Ok(Box::new(PerFieldFieldsReader::new(state)?))
    }
}

struct PerFieldFieldsReader {
    fields: BTreeMap<String, FieldsProducerRef>,
    formats: HashMap<String, FieldsProducerRef>,
    segment: String,
}

impl PerFieldFieldsReader {
    fn new(state: &SegmentReadState) -> Result<PerFieldFieldsReader> {
        let mut fields = BTreeMap::new();
        let mut formats = HashMap::new();
        for (name, info) in &state.field_infos.by_name {
            if let IndexOptions::Null = info.index_options {
                continue;
            }
            if let Some(format) = info.attributes.get(PER_FIELD_FORMAT_KEY) {
                if let Some(suffix) = info.attributes.get(PER_FIELD_SUFFIX_KEY) {
                    let postings_format = postings_format_for_name(format)?;
                    let suffix = get_suffix(format, suffix);
                    if !formats.contains_key(&suffix) {
                        let state = SegmentReadState::with_suffix(state, &suffix);
                        formats.insert(
                            suffix.clone(),
                            Arc::from(postings_format.fields_producer(&state)?),
                        );
                    }
                    fields.insert(
                        name.clone(),
                        Arc::clone(formats.get(&suffix).as_ref().unwrap()),
                    );
                } else {
                    bail!(
                        "Illegal State: missing attribute: {} for field {}",
                        PER_FIELD_SUFFIX_KEY,
                        name
                    );
                }
            }
        }
        let segment = state.segment_info.name.clone();
        Ok(PerFieldFieldsReader {
            fields,
            formats,
            segment,
        })
    }

    fn terms_impl(&self, field: &str) -> Result<Option<TermsRef>> {
        match self.fields.get(field) {
            Some(producer) => producer.terms(field),
            None => Ok(None),
        }
    }

    fn size_impl(&self) -> usize {
        self.fields.len()
    }
}

impl fmt::Display for PerFieldFieldsReader {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Segment: {}", &self.segment)
    }
}

impl FieldsProducer for PerFieldFieldsReader {
    fn check_integrity(&self) -> Result<()> {
        for producer in self.formats.values() {
            producer.check_integrity()?;
        }
        Ok(())
    }
    fn get_merge_instance(&self) -> Result<FieldsProducerRef> {
        unimplemented!()
    }
}

impl Fields for PerFieldFieldsReader {
    fn fields(&self) -> Vec<String> {
        self.fields.keys().cloned().collect()
    }
    fn terms(&self, field: &str) -> Result<Option<TermsRef>> {
        self.terms_impl(field)
    }
    fn size(&self) -> usize {
        self.size_impl()
    }
}
