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

mod norms_producer;

pub use self::norms_producer::*;

mod norms_consumer;

pub use self::norms_consumer::*;

mod norms;

pub use self::norms::*;

mod norm_values_writer;

pub use self::norm_values_writer::*;

use core::codec::doc_values::{EmptyNumericDocValues, NumericDocValues};
use core::codec::field_infos::FieldInfo;
use core::codec::norms::Lucene53NormsConsumer;
use core::codec::segment_infos::{SegmentReadState, SegmentWriteState};
use core::codec::*;
use core::index::merge::{
    doc_id_merger_of, DocIdMerger, DocIdMergerEnum, DocIdMergerSub, DocIdMergerSubBase,
    LiveDocsDocMap, MergeState,
};
use core::store::directory::Directory;
use core::store::io::IndexOutput;
use core::util::{Numeric, ReusableIterator};

use core::search::NO_MORE_DOCS;
use error::Result;
use std::sync::Arc;

/// Encodes/decodes per-document score normalization values.
pub trait NormsFormat {
    type NormsProducer: NormsProducer;
    /// Returns a {@link NormsProducer} to read norms from the index.
    /// <p>
    /// NOTE: by the time this call returns, it must hold open any files it will
    /// need to use; else, those files may be deleted. Additionally, required files
    /// may be deleted during the execution of this call before there is a chance
    /// to open them. Under these circumstances an IOException should be thrown by
    /// the implementation. IOExceptions are expected and will automatically cause
    /// a retry of the segment opening logic with the newly revised segments.
    fn norms_producer<D: Directory, DW: Directory, C: Codec>(
        &self,
        state: &SegmentReadState<'_, D, DW, C>,
    ) -> Result<Self::NormsProducer>;

    fn norms_consumer<D: Directory, DW: Directory, C: Codec>(
        &self,
        state: &SegmentWriteState<D, DW, C>,
    ) -> Result<NormsConsumerEnum<DW::IndexOutput>>;
}

pub enum NormsConsumerEnum<O: IndexOutput> {
    Lucene53(Lucene53NormsConsumer<O>),
}

impl<O: IndexOutput> NormsConsumer for NormsConsumerEnum<O> {
    fn add_norms_field(
        &mut self,
        field_info: &FieldInfo,
        values: &mut impl ReusableIterator<Item = Result<Numeric>>,
    ) -> Result<()> {
        match self {
            NormsConsumerEnum::Lucene53(w) => w.add_norms_field(field_info, values),
        }
    }

    fn merge_norms_field<D: Directory, C: Codec>(
        &mut self,
        field_info: &FieldInfo,
        merge_state: &MergeState<D, C>,
        to_merge: Vec<Box<dyn NumericDocValues>>,
    ) -> Result<()> {
        match self {
            NormsConsumerEnum::Lucene53(w) => {
                w.merge_norms_field(field_info, merge_state, to_merge)
            }
        }
    }

    fn merge<D: Directory, C: Codec>(&mut self, merge_state: &mut MergeState<D, C>) -> Result<()> {
        match self {
            NormsConsumerEnum::Lucene53(w) => w.merge(merge_state),
        }
    }
}

pub trait NormsConsumer {
    fn add_norms_field(
        &mut self,
        field_info: &FieldInfo,
        values: &mut impl ReusableIterator<Item = Result<Numeric>>,
    ) -> Result<()>;

    fn merge_norms_field<D: Directory, C: Codec>(
        &mut self,
        field_info: &FieldInfo,
        merge_state: &MergeState<D, C>,
        to_merge: Vec<Box<dyn NumericDocValues>>,
    ) -> Result<()> {
        let mut iter = NormsValuesMergeIter::new(merge_state, to_merge)?;
        self.add_norms_field(field_info, &mut iter)
    }

    fn merge<D: Directory, C: Codec>(&mut self, merge_state: &mut MergeState<D, C>) -> Result<()> {
        for producer in &merge_state.norms_producers {
            if let Some(producer) = producer.as_ref() {
                producer.check_integrity()?;
            }
        }

        for merge_field_info in merge_state
            .merge_field_infos
            .as_ref()
            .unwrap()
            .by_number
            .values()
        {
            if merge_field_info.has_norms() {
                let mut to_merge = vec![];
                for i in 0..merge_state.norms_producers.len() {
                    let mut norms: Box<dyn NumericDocValues> =
                        Box::new(EmptyNumericDocValues::default());
                    if let Some(ref norm_producer) = merge_state.norms_producers[i] {
                        if let Some(field_info) =
                            merge_state.fields_infos[i].field_info_by_name(&merge_field_info.name)
                        {
                            if field_info.has_norms() {
                                norms = norm_producer.norms(field_info)?;
                            }
                        }
                    }
                    to_merge.push(norms);
                }
                self.merge_norms_field(merge_field_info, merge_state, to_merge)?;
            }
        }
        Ok(())
    }
}

struct NormsValuesSub {
    values: Box<dyn NumericDocValues>,
    doc_id: i32,
    max_doc: i32,
    base: DocIdMergerSubBase,
}

impl NormsValuesSub {
    fn new(doc_map: Arc<LiveDocsDocMap>, values: Box<dyn NumericDocValues>, max_doc: i32) -> Self {
        let base = DocIdMergerSubBase::new(doc_map);
        NormsValuesSub {
            values,
            doc_id: -1,
            max_doc,
            base,
        }
    }
}

impl DocIdMergerSub for NormsValuesSub {
    fn next_doc(&mut self) -> Result<i32> {
        self.doc_id += 1;
        if self.doc_id == self.max_doc {
            Ok(NO_MORE_DOCS)
        } else {
            Ok(self.doc_id)
        }
    }

    fn base(&self) -> &DocIdMergerSubBase {
        &self.base
    }

    fn base_mut(&mut self) -> &mut DocIdMergerSubBase {
        &mut self.base
    }

    fn reset(&mut self) {
        self.base.reset();
        self.doc_id = -1;
    }
}

struct NormsValuesMergeIter {
    doc_id_merger: DocIdMergerEnum<NormsValuesSub>,
    next_value: Numeric,
    next_is_set: bool,
}

impl NormsValuesMergeIter {
    fn new<D: Directory + 'static, C: Codec>(
        merge_state: &MergeState<D, C>,
        to_merge: Vec<Box<dyn NumericDocValues>>,
    ) -> Result<Self> {
        let mut subs = Vec::with_capacity(to_merge.len());
        for (i, dv) in to_merge.into_iter().enumerate() {
            subs.push(NormsValuesSub::new(
                Arc::clone(&merge_state.doc_maps[i]),
                dv,
                merge_state.max_docs[i],
            ));
        }
        let doc_id_merger = doc_id_merger_of(subs, merge_state.needs_index_sort)?;
        Ok(NormsValuesMergeIter {
            doc_id_merger,
            next_value: Numeric::Null,
            next_is_set: false,
        })
    }
    fn has_next(&mut self) -> Result<bool> {
        Ok(self.next_is_set || self.set_next()?)
    }

    fn set_next(&mut self) -> Result<bool> {
        if let Some(sub) = self.doc_id_merger.next()? {
            self.next_is_set = true;
            let value = sub.values.get_mut(sub.doc_id)?;
            self.next_value = Numeric::Long(value);
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

impl Iterator for NormsValuesMergeIter {
    type Item = Result<Numeric>;

    fn next(&mut self) -> Option<Result<Numeric>> {
        match self.has_next() {
            Err(e) => Some(Err(e)),
            Ok(true) => {
                debug_assert!(self.next_is_set);
                self.next_is_set = false;
                Some(Ok(self.next_value))
            }
            Ok(false) => None,
        }
    }
}

impl ReusableIterator for NormsValuesMergeIter {
    fn reset(&mut self) {
        self.doc_id_merger.reset().unwrap();
        self.next_is_set = false;
        self.next_value = Numeric::Null;
    }
}

/// Abstract API that produces field normalization values
pub trait NormsProducer {
    fn norms(&self, field: &FieldInfo) -> Result<Box<dyn NumericDocValues>>;
    fn check_integrity(&self) -> Result<()> {
        // codec_util::checksum_entire_file(input)?;
        Ok(())
    }
}

impl<T: NormsProducer> NormsProducer for Arc<T> {
    fn norms(&self, field: &FieldInfo) -> Result<Box<dyn NumericDocValues>> {
        (**self).norms(field)
    }
    fn check_integrity(&self) -> Result<()> {
        (**self).check_integrity()
    }
}
