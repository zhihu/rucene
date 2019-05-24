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

use core::codec::{Codec, NormsConsumer};
use core::index::FieldInfo;
use core::index::SegmentWriteState;
use core::util::packed::{
    LongValuesIterator, PackedLongValuesBuilder, PackedLongValuesBuilderType, DEFAULT_PAGE_SIZE,
};
use core::util::packed_misc::COMPACT;
use core::util::{DocId, Numeric, ReusableIterator};

use core::store::Directory;
use error::Result;

const MISSING: i64 = 0;

pub struct NormValuesWriter {
    pending: PackedLongValuesBuilder,
    field_info: FieldInfo,
}

impl NormValuesWriter {
    pub fn new(field_info: &FieldInfo) -> Self {
        NormValuesWriter {
            pending: PackedLongValuesBuilder::new(
                DEFAULT_PAGE_SIZE,
                COMPACT as f32,
                PackedLongValuesBuilderType::Delta,
            ),
            field_info: field_info.clone(),
        }
    }

    pub fn add_value(&mut self, doc_id: DocId, value: i64) {
        for _ in self.pending.size()..doc_id as i64 {
            self.pending.add(MISSING);
        }
        self.pending.add(value);
    }

    pub fn finish(&mut self, _num_doc: i32) {}

    pub fn flush<D: Directory, DW: Directory, C: Codec, NC: NormsConsumer>(
        &mut self,
        state: &SegmentWriteState<D, DW, C>,
        consumer: &mut NC,
    ) -> Result<()> {
        let max_doc = state.segment_info.max_doc as usize;
        let values = self.pending.build();
        let mut iter = NumericIter::new(values.iterator(), max_doc, values.size() as usize);
        consumer.add_norms_field(&self.field_info, &mut iter)?;
        Ok(())
    }
}

struct NumericIter<'a> {
    values_iter: LongValuesIterator<'a>,
    upto: usize,
    max_doc: usize,
    size: usize,
}

impl<'a> NumericIter<'a> {
    fn new(values_iter: LongValuesIterator<'a>, max_doc: usize, size: usize) -> NumericIter {
        NumericIter {
            values_iter,
            upto: 0,
            max_doc,
            size,
        }
    }
}

impl<'a> Iterator for NumericIter<'a> {
    type Item = Result<Numeric>;

    fn next(&mut self) -> Option<Result<Numeric>> {
        if self.upto < self.max_doc {
            let v = if self.upto < self.size {
                self.values_iter.next().unwrap()
            } else {
                MISSING
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
