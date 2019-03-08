use std::collections::HashMap;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::mem;
use std::sync::Arc;

use core::codec::format::{postings_format_for_name, PostingsFormat};
use core::codec::lucene50::Lucene50PostingsFormat;
use core::codec::{FieldsConsumer, FieldsProducer, FieldsProducerRef};
use core::index::Fields;
use core::index::TermsRef;
use core::index::{IndexOptions, SegmentReadState, SegmentWriteState};
use error::*;

/// Name of this {@link PostingsFormat}. */
// const PER_FIELD_NAME: &str = "PerField40";
/// {@link FieldInfo} attribute name used to store the
/// format name for each field. */
pub const PER_FIELD_POSTING_FORMAT_KEY: &str = "PerFieldPostingsFormat.format";

/// segment suffix name for each field. */
pub const PER_FIELD_POSTING_SUFFIX_KEY: &str = "PerFieldPostingsFormat.suffix";

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
#[derive(Hash, Ord, PartialOrd, Eq, PartialEq)]
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

    fn fields_consumer(&self, state: &SegmentWriteState) -> Result<Box<FieldsConsumer>> {
        Ok(Box::new(PerFieldFieldsWriter::new(state)))
    }

    fn name(&self) -> &str {
        "PerField40"
    }
}

struct PerFieldFieldsReader {
    fields: BTreeMap<String, FieldsProducerRef>,
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
            if let Some(format) = info
                .attributes
                .read()
                .unwrap()
                .get(PER_FIELD_POSTING_FORMAT_KEY)
            {
                if let Some(suffix) = info
                    .attributes
                    .read()
                    .unwrap()
                    .get(PER_FIELD_POSTING_SUFFIX_KEY)
                {
                    if !formats.contains_key(suffix) {
                        formats.insert(suffix.clone(), postings_format_for_name(format)?);
                    }
                    let postings_format = formats.get(suffix);
                    let postings_format = postings_format.as_ref().unwrap();
                    let suffix = get_suffix(format, suffix);
                    let state = SegmentReadState::with_suffix(state, &suffix);
                    fields.insert(
                        name.clone(),
                        Arc::from(postings_format.fields_producer(&state)?),
                    );
                } else {
                    bail!(
                        "Illegal State: missing attribute: {} for field {}",
                        PER_FIELD_POSTING_SUFFIX_KEY,
                        name
                    );
                }
            }
        }
        let segment = state.segment_info.name.clone();
        Ok(PerFieldFieldsReader { fields, segment })
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
        for producer in self.fields.values() {
            producer.check_integrity()?;
        }
        Ok(())
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

#[allow(dead_code)]
struct FieldsGroup {
    fields: BTreeSet<String>,
    suffix: usize,
    state: SegmentWriteState,
}

struct PerFieldFieldsWriter {
    write_state: SegmentWriteState,
}

impl PerFieldFieldsWriter {
    pub fn new(write_state: &SegmentWriteState) -> Self {
        PerFieldFieldsWriter {
            write_state: write_state.clone(),
        }
    }

    fn get_full_segment_suffix(&self, outer_segment_suffix: &str, suffix: String) -> String {
        debug_assert!(outer_segment_suffix.is_empty());
        suffix
    }
}

impl FieldsConsumer for PerFieldFieldsWriter {
    // TODO this is only one postings format currently (lucene50).
    // And we assume that we always use one format all the time.
    // so we won't implement this like lucene
    // when the format changes, it's easy to change the hard code then.
    fn write(&mut self, fields: &Fields) -> Result<()> {
        // this is only one format, so suffix is always "0"
        let segment_suffix =
            self.get_full_segment_suffix(&self.write_state.segment_suffix, "Lucene50_0".into());
        // always use lucene50, so just hard code it.
        let format = Lucene50PostingsFormat::default();

        let old_suffix = mem::replace(&mut self.write_state.segment_suffix, segment_suffix);

        let mut consumer = format.fields_consumer(&self.write_state)?;
        consumer.write(fields)?;

        self.write_state.segment_suffix = old_suffix;

        Ok(())
    }
}
