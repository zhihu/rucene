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

use std::collections::HashMap;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::mem;
use std::sync::Arc;

use core::codec::postings::blocktree::FieldReaderRef;
use core::codec::postings::{
    postings_format_for_name, FieldsConsumer, FieldsConsumerEnum, FieldsProducer,
    FieldsProducerEnum, Lucene50PostingsFormat, PostingsFormat,
};
use core::codec::segment_infos::{SegmentReadState, SegmentWriteState};
use core::codec::{Codec, Fields};
use core::doc::IndexOptions;
use core::store::directory::Directory;
use error::Result;

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
#[derive(Copy, Clone)]
pub struct PerFieldPostingsFormat;

impl Default for PerFieldPostingsFormat {
    fn default() -> PerFieldPostingsFormat {
        PerFieldPostingsFormat {}
    }
}

impl PostingsFormat for PerFieldPostingsFormat {
    type FieldsProducer = Arc<PerFieldFieldsReader>;
    fn fields_producer<D: Directory, DW: Directory, C: Codec>(
        &self,
        state: &SegmentReadState<'_, D, DW, C>,
    ) -> Result<Self::FieldsProducer> {
        Ok(Arc::new(PerFieldFieldsReader::new(state)?))
    }

    fn fields_consumer<D: Directory, DW: Directory, C: Codec>(
        &self,
        state: &SegmentWriteState<D, DW, C>,
    ) -> Result<FieldsConsumerEnum<D, DW, C>> {
        Ok(FieldsConsumerEnum::PerField(PerFieldFieldsWriter::new(
            state,
        )))
    }

    fn name(&self) -> &str {
        "PerField40"
    }
}

pub struct PerFieldFieldsReader {
    fields: BTreeMap<String, Arc<FieldsProducerEnum>>,
    segment: String,
}

impl PerFieldFieldsReader {
    fn new<D: Directory, DW: Directory, C: Codec>(
        state: &SegmentReadState<'_, D, DW, C>,
    ) -> Result<PerFieldFieldsReader> {
        let mut fields = BTreeMap::new();
        let mut formats = HashMap::new();
        for (name, info) in &state.field_infos.by_name {
            if let IndexOptions::Null = info.index_options {
                continue;
            }

            let attrs = info.attributes.read().unwrap();
            if let Some(format) = attrs.get(PER_FIELD_POSTING_FORMAT_KEY) {
                if let Some(suffix) = attrs.get(PER_FIELD_POSTING_SUFFIX_KEY) {
                    let segment_suffix = get_suffix(&format, suffix);

                    if !formats.contains_key(&segment_suffix) {
                        let postings_format = postings_format_for_name(format)?;
                        let state = SegmentReadState::with_suffix(state, &segment_suffix);
                        formats.insert(
                            segment_suffix.clone(),
                            Arc::new(postings_format.fields_producer(&state)?),
                        );
                    }

                    if let Some(field_producer) = formats.get(&segment_suffix) {
                        fields.insert(name.clone(), field_producer.clone());
                    }
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

    fn terms_impl(&self, field: &str) -> Result<Option<FieldReaderRef>> {
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
    type Terms = FieldReaderRef;
    fn fields(&self) -> Vec<String> {
        self.fields.keys().cloned().collect()
    }
    fn terms(&self, field: &str) -> Result<Option<Self::Terms>> {
        self.terms_impl(field)
    }
    fn size(&self) -> usize {
        self.size_impl()
    }
}

#[allow(dead_code)]
struct FieldsGroup<D: Directory, DW: Directory, C: Codec> {
    fields: BTreeSet<String>,
    suffix: usize,
    state: SegmentWriteState<D, DW, C>,
}

pub struct PerFieldFieldsWriter<D: Directory, DW: Directory, C: Codec> {
    write_state: SegmentWriteState<D, DW, C>,
}

impl<D: Directory, DW: Directory, C: Codec> PerFieldFieldsWriter<D, DW, C> {
    pub fn new(write_state: &SegmentWriteState<D, DW, C>) -> Self {
        PerFieldFieldsWriter {
            write_state: write_state.clone(),
        }
    }

    fn get_full_segment_suffix(&self, outer_segment_suffix: &str, suffix: String) -> String {
        debug_assert!(outer_segment_suffix.is_empty());
        suffix
    }
}

impl<D: Directory, DW: Directory, C: Codec> FieldsConsumer for PerFieldFieldsWriter<D, DW, C> {
    // TODO this is only one postings format currently (lucene50).
    // And we assume that we always use one format all the time.
    // so we won't implement this like lucene
    // when the format changes, it's easy to change the hard code then.
    fn write(&mut self, fields: &impl Fields) -> Result<()> {
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
