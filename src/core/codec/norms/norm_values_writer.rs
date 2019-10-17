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

use core::codec::doc_values::{NumericDVIter, NumericDocValuesWriter};
use core::codec::field_infos::FieldInfo;
use core::codec::norms::NormsConsumer;
use core::codec::segment_infos::SegmentWriteState;
use core::codec::{Codec, SorterDocMap};
use core::util::packed::COMPACT;
use core::util::packed::{
    LongValuesIterator, PackedLongValuesBuilder, PackedLongValuesBuilderType, DEFAULT_PAGE_SIZE,
};
use core::util::{BitSet, FixedBitSet};
use core::util::{Bits, DocId, Numeric, ReusableIterator};

use core::store::directory::Directory;
use error::Result;

const MISSING: i64 = 0;

pub struct NormValuesWriter {
    pending: PackedLongValuesBuilder,
    docs_with_field: FixedBitSet,
    field_info: FieldInfo,
    last_doc: DocId,
}

impl NormValuesWriter {
    pub fn new(field_info: &FieldInfo) -> Self {
        NormValuesWriter {
            pending: PackedLongValuesBuilder::new(
                DEFAULT_PAGE_SIZE,
                COMPACT as f32,
                PackedLongValuesBuilderType::Delta,
            ),
            docs_with_field: FixedBitSet::new(64),
            field_info: field_info.clone(),
            last_doc: -1,
        }
    }

    pub fn add_value(&mut self, doc_id: DocId, value: i64) {
        debug_assert!(self.last_doc < doc_id);
        self.docs_with_field.ensure_capacity(doc_id as usize);
        self.docs_with_field.set(doc_id as usize);
        self.pending.add(value);
        self.last_doc = doc_id;
    }

    pub fn finish(&mut self, _num_doc: i32) {}

    pub fn flush<D: Directory, DW: Directory, C: Codec, NC: NormsConsumer>(
        &mut self,
        state: &SegmentWriteState<D, DW, C>,
        sort_map: Option<&impl SorterDocMap>,
        consumer: &mut NC,
    ) -> Result<()> {
        let max_doc = state.segment_info.max_doc;
        let values = self.pending.build();
        if let Some(sort_map) = sort_map {
            let sorted = NumericDocValuesWriter::sort_doc_values(
                max_doc,
                sort_map,
                &self.docs_with_field,
                values.iterator(),
            );
            let mut iter = NumericDVIter::new(sorted);
            consumer.add_norms_field(&self.field_info, &mut iter)
        } else {
            let mut iter =
                NumericIter::new(values.iterator(), &self.docs_with_field, max_doc as usize);
            consumer.add_norms_field(&self.field_info, &mut iter)
        }
    }
}

struct NumericIter<'a> {
    values_iter: LongValuesIterator<'a>,
    docs_with_field: &'a FixedBitSet,
    upto: usize,
    max_doc: usize,
}

impl<'a> NumericIter<'a> {
    fn new(
        values_iter: LongValuesIterator<'a>,
        docs_with_field: &'a FixedBitSet,
        max_doc: usize,
    ) -> NumericIter<'a> {
        NumericIter {
            values_iter,
            docs_with_field,
            upto: 0,
            max_doc,
        }
    }
}

impl<'a> Iterator for NumericIter<'a> {
    type Item = Result<Numeric>;

    fn next(&mut self) -> Option<Result<Numeric>> {
        if self.upto < self.max_doc {
            let v = if self.upto >= self.docs_with_field.len()
                || !self.docs_with_field.get(self.upto).unwrap()
            {
                MISSING
            } else {
                self.values_iter.next().unwrap()
            };
            self.upto += 1;
            Some(Ok(Numeric::Long(v)))
        } else {
            None
        }
    }
}

impl<'a> ReusableIterator for NumericIter<'a> {
    fn reset(&mut self) {
        self.values_iter.reset();
        self.upto = 0;
    }
}
