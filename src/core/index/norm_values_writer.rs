use core::codec::NormsConsumer;
use core::index::FieldInfo;
use core::index::SegmentWriteState;
use core::util::packed::LongValuesIterator;
use core::util::packed::{PackedLongValuesBuilder, PackedLongValuesBuilderType, DEFAULT_PAGE_SIZE};
use core::util::packed_misc::COMPACT;
use core::util::{DocId, Numeric, ReusableIterator};

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

    pub fn flush(&mut self, state: &SegmentWriteState, consumer: &mut NormsConsumer) -> Result<()> {
        let max_doc = state.segment_info.max_doc as usize;
        let values = self.pending.build();
        let mut iter = NumericIter::new(values.iterator(), max_doc, values.size() as usize);
        consumer.add_norms_field(&self.field_info, &mut iter)?;
        Ok(())
    }
}

struct NumericIter {
    values_iter: LongValuesIterator,
    upto: usize,
    max_doc: usize,
    size: usize,
}

impl NumericIter {
    fn new(values_iter: LongValuesIterator, max_doc: usize, size: usize) -> NumericIter {
        NumericIter {
            values_iter,
            upto: 0,
            max_doc,
            size,
        }
    }
}

impl Iterator for NumericIter {
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

impl ReusableIterator for NumericIter {
    fn reset(&mut self) {
        self.values_iter.reset();
        self.upto = 0;
    }
}
