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

use core::codec::Codec;
use core::index::{
    BinaryDocValuesRef, MultiTermIterator, NumericDocValues, NumericDocValuesContext,
    NumericDocValuesRef, ReaderSlice, SearchLeafReader, SingletonSortedNumericDocValues,
    SingletonSortedSetDocValues, SortedDocValues, SortedDocValuesRef, SortedNumericDocValues,
    SortedNumericDocValuesRef, SortedSetDocValues, SortedSetDocValuesRef, TermIterator,
    TermIteratorIndex, NO_MORE_ORDS,
};
use core::util::bit_util::BitsRequired;
use core::util::packed::{
    PackedLongValues, PackedLongValuesBuilder, PackedLongValuesBuilderType, DEFAULT_PAGE_SIZE,
};
use core::util::packed_misc::{get_mutable_by_ratio, Mutable, MutableEnum, Reader, COMPACT};
use core::util::{
    Bits, BitsContext, BitsRef, DocId, IdentityLongValues, LongValues, LongValuesContext,
};

use error::Result;

use std::rc::Rc;
use std::sync::Arc;

pub struct DocValues;

impl DocValues {
    pub fn singleton_sorted_doc_values<T: SortedDocValues>(
        dv: T,
    ) -> SingletonSortedSetDocValues<T> {
        SingletonSortedSetDocValues::new(dv)
    }

    pub fn singleton_sorted_numeric_doc_values(
        numeric_doc_values_in: Box<dyn NumericDocValues>,
        docs_with_field: BitsRef,
    ) -> SingletonSortedNumericDocValues {
        SingletonSortedNumericDocValues::new(numeric_doc_values_in, docs_with_field)
    }

    pub fn docs_with_value_sorted(dv: Arc<dyn SortedDocValues>, max_doc: i32) -> BitsRef {
        Arc::new(SortedDocValuesBits { dv, max_doc })
    }

    pub fn docs_with_value_sorted_set(dv: Arc<dyn SortedSetDocValues>, max_doc: i32) -> BitsRef {
        Arc::new(SortedSetDocValuesBits { dv, max_doc })
    }

    pub fn docs_with_value_sorted_numeric(
        dv: Arc<dyn SortedNumericDocValues>,
        max_doc: i32,
    ) -> BitsRef {
        Arc::new(SortedNumericDocValuesBits { dv, max_doc })
    }

    pub fn get_docs_with_field<C: Codec>(
        reader: &SearchLeafReader<C>,
        field: &str,
    ) -> Result<BitsRef> {
        reader.get_docs_with_field(field)
    }

    pub fn get_numeric<C: Codec>(
        reader: &SearchLeafReader<C>,
        field: &str,
    ) -> Result<NumericDocValuesRef> {
        reader.get_numeric_doc_values(field)
    }

    pub fn get_binary<C: Codec>(
        reader: &SearchLeafReader<C>,
        field: &str,
    ) -> Result<BinaryDocValuesRef> {
        reader.get_binary_doc_values(field)
    }

    pub fn get_sorted<C: Codec>(
        reader: &SearchLeafReader<C>,
        field: &str,
    ) -> Result<SortedDocValuesRef> {
        reader.get_sorted_doc_values(field)
    }

    pub fn get_sorted_numeric<C: Codec>(
        reader: &SearchLeafReader<C>,
        field: &str,
    ) -> Result<SortedNumericDocValuesRef> {
        reader.get_sorted_numeric_doc_values(field)
    }

    pub fn get_sorted_set<C: Codec>(
        reader: &SearchLeafReader<C>,
        field: &str,
    ) -> Result<SortedSetDocValuesRef> {
        reader.get_sorted_set_doc_values(field)
    }

    pub fn unwrap_singleton(dv: &SortedNumericDocValuesRef) -> Result<Option<NumericDocValuesRef>> {
        let val = dv.get_numeric_doc_values();
        Ok(val)
    }
}

struct SortedDocValuesBits {
    dv: Arc<dyn SortedDocValues>,
    max_doc: i32,
}

impl Bits for SortedDocValuesBits {
    fn get_with_ctx(&self, ctx: BitsContext, index: usize) -> Result<(bool, BitsContext)> {
        let ord = self.dv.get_ord(index as DocId)?;
        Ok((ord >= 0, ctx))
    }

    fn len(&self) -> usize {
        self.max_doc as usize
    }
}

struct SortedSetDocValuesBits {
    dv: Arc<dyn SortedSetDocValues>,
    max_doc: i32,
}

impl Bits for SortedSetDocValuesBits {
    fn get_with_ctx(&self, ctx: BitsContext, index: usize) -> Result<(bool, BitsContext)> {
        let mut dv_ctx = self.dv.set_document(index as DocId)?;
        let ord = self.dv.next_ord(&mut dv_ctx)?;
        Ok((ord != NO_MORE_ORDS, ctx))
    }
    fn len(&self) -> usize {
        self.max_doc as usize
    }
}

struct SortedNumericDocValuesBits {
    dv: Arc<dyn SortedNumericDocValues>,
    max_doc: i32,
}

impl Bits for SortedNumericDocValuesBits {
    fn get_with_ctx(&self, ctx: BitsContext, index: usize) -> Result<(bool, BitsContext)> {
        let dv_ctx = self
            .dv
            .set_document(ctx.map(|c| (0, 0, Some(c))), index as DocId)?;
        Ok((self.dv.count(&dv_ctx) != 0, ctx))
    }
    fn len(&self) -> usize {
        self.max_doc as usize
    }
}

/// maps per-segment ordinals to/from global ordinal space
// TODO: we could also have a utility method to merge Terms[] and use size() as a weight when we
// need it TODO: use more efficient packed ints structures?
// TODO: pull this out? it's pretty generic (maps between N ord()-enabled TermsEnums)
pub struct OrdinalMap {
    // globalOrd -> (globalOrd - segmentOrd) where segmentOrd is the the ordinal in
    // the first segment that contains this term
    global_ord_deltas: PackedLongValues,
    // globalOrd -> first segment container
    first_segments: PackedLongValues,
    // for every segment, segmentOrd -> globalOrd
    segment_to_global_ords: Vec<Rc<LongValues>>,
    // the map from/to segment ids
    segment_map: SegmentMap,
}

impl OrdinalMap {
    pub fn build<T: TermIterator>(
        subs: Vec<Option<T>>,
        weights: Vec<usize>,
        acceptable_overhead_ratio: f32,
    ) -> Result<Self> {
        debug_assert_eq!(subs.len(), weights.len());
        let segment_map = SegmentMap::new(weights);
        Self::new(subs, segment_map, acceptable_overhead_ratio)
    }

    fn new<T: TermIterator>(
        mut subs: Vec<Option<T>>,
        segment_map: SegmentMap,
        acceptable_overhead_ratio: f32,
    ) -> Result<Self> {
        let num_subs = subs.len();
        let mut global_ord_deltas_builder = PackedLongValuesBuilder::new(
            DEFAULT_PAGE_SIZE,
            COMPACT,
            PackedLongValuesBuilderType::Monotonic,
        );
        let mut first_segments_builder = PackedLongValuesBuilder::new(
            DEFAULT_PAGE_SIZE,
            COMPACT,
            PackedLongValuesBuilderType::Default,
        );
        let mut ord_deltas = Vec::with_capacity(num_subs);
        for _i in 0..num_subs {
            ord_deltas.push(PackedLongValuesBuilder::new(
                DEFAULT_PAGE_SIZE,
                COMPACT,
                PackedLongValuesBuilderType::Monotonic,
            ));
        }
        let mut ord_delta_bits = vec![0i64; num_subs];
        let mut segment_ords = vec![0i64; num_subs];
        let mut slices = Vec::with_capacity(num_subs);
        let mut indexes = Vec::with_capacity(num_subs);
        for i in 0..num_subs {
            slices.push(ReaderSlice::new(0, 0, i));
            let idx = segment_map.new_to_old(i as i32) as usize;
            debug_assert!(subs[idx].is_some());
            let sub = subs[idx].take().unwrap();
            indexes.push(TermIteratorIndex::new(sub, i));
        }
        let mut mte = MultiTermIterator::new(slices);
        mte.reset(indexes)?;
        let mut global_ord = 0;
        loop {
            if mte.next()?.is_some() {
                let mut first_segment_index = i32::max_value() as usize;
                let mut global_ord_delta = i64::max_value();
                for i in 0..mte.num_top {
                    let segment_index = mte.subs[mte.top_indexes[i]].index;
                    let segment_ord = mte.subs[mte.top_indexes[i]].terms.as_mut().unwrap().ord()?;
                    let delta = global_ord - segment_ord;
                    // We compute the least segment where the term occurs. In case the
                    // first segment contains most (or better all) values, this will
                    // help save significant memory
                    if segment_index < first_segment_index {
                        first_segment_index = segment_index;
                        global_ord_delta = delta;
                    }

                    // for each per-segment ord, map it back to the global term.
                    while segment_ords[segment_index] <= segment_ord {
                        ord_delta_bits[segment_index] |= delta;
                        ord_deltas[segment_index].add(delta);
                        segment_ords[segment_index] += 1;
                    }
                }
                // for each unique term, just mark the first segment index/delta where it occurs
                debug_assert!(first_segment_index < segment_ords.len());
                first_segments_builder.add(first_segment_index as i64);
                global_ord_deltas_builder.add(global_ord_delta);
                global_ord += 1;
            } else {
                break;
            }
        }

        let first_segments = first_segments_builder.build();
        let global_ord_deltas = global_ord_deltas_builder.build();

        let mut segment_to_global_ords: Vec<Rc<LongValues>> = Vec::with_capacity(subs.len());
        let mut i = 0;
        for mut d in ord_deltas {
            let deltas = d.build();
            if ord_delta_bits[i] == 0 {
                // segment ords perfectly match global ordinals
                // likely in case of low cardinalities and large segments
                segment_to_global_ords.push(Rc::new(IdentityLongValues {}));
            } else {
                let bits_required = if ord_delta_bits[i] < 0 {
                    64
                } else {
                    ord_delta_bits[i].bits_required() as i32
                };
                let monotonic_bits = deltas.ram_bytes_used_estimate() * 8;
                let packed_bits = bits_required as i64 * deltas.size();
                if deltas.size() < i32::max_value() as i64
                    && packed_bits as f32
                        <= monotonic_bits as f32 * (1.0 + acceptable_overhead_ratio)
                {
                    // monotonic compression mostly adds overhead, let's keep the mapping in plain
                    // packed ints
                    let size = deltas.size();
                    let mut new_deltas = get_mutable_by_ratio(
                        size as usize,
                        bits_required,
                        acceptable_overhead_ratio,
                    );
                    let mut cnt = 0;
                    for v in deltas.iterator() {
                        new_deltas.set(cnt, v);
                        cnt += 1;
                    }
                    debug_assert_eq!(cnt as i64, size);
                    segment_to_global_ords.push(Rc::new(MutableAsLongValues {
                        mutable: new_deltas,
                    }));
                } else {
                    segment_to_global_ords
                        .push(Rc::new(PackedLongValuesWrapper { values: deltas }));
                }
            }
            i += 1;
        }
        Ok(OrdinalMap {
            global_ord_deltas,
            first_segments,
            segment_to_global_ords,
            segment_map,
        })
    }

    pub fn value_count(&self) -> i64 {
        self.global_ord_deltas.size()
    }

    pub fn first_segment_number(&self, global_ord: i64) -> i32 {
        let new = self.first_segments.get64(global_ord).unwrap() as i32;
        let res = self.segment_map.new_to_old(new);
        res
    }

    pub fn first_segment_ord(&self, global_ord: i64) -> i64 {
        global_ord - self.global_ord_deltas.get64(global_ord).unwrap()
    }

    pub fn get_global_ords(&self, index: usize) -> Rc<LongValues> {
        let i = self.segment_map.old_to_new(index as i32) as usize;
        Rc::clone(&self.segment_to_global_ords[i])
    }
}

#[derive(Debug)]
struct SegmentMap {
    new_to_old: Vec<i32>,
    old_to_new: Vec<i32>,
}

impl SegmentMap {
    fn new(weights: Vec<usize>) -> Self {
        let new_to_old = Self::map(&weights);
        let old_to_new = Self::inverse(&new_to_old);
        SegmentMap {
            new_to_old,
            old_to_new,
        }
    }

    fn new_to_old(&self, segment: i32) -> i32 {
        self.new_to_old[segment as usize]
    }

    fn old_to_new(&self, segment: i32) -> i32 {
        self.old_to_new[segment as usize]
    }

    fn map(weights: &[usize]) -> Vec<i32> {
        let mut new_to_old: Vec<i32> = (0..weights.len() as i32).collect();
        new_to_old.sort_by(|i, j| weights[*j as usize].cmp(&weights[*i as usize]));
        new_to_old
    }

    // inverse the map
    fn inverse(map: &[i32]) -> Vec<i32> {
        let mut inverse = vec![0i32; map.len()];
        for i in 0..map.len() {
            inverse[map[i] as usize] = i as i32;
        }
        inverse
    }
}

struct MutableAsLongValues {
    mutable: MutableEnum,
}

impl LongValues for MutableAsLongValues {
    fn get64_with_ctx(
        &self,
        ctx: LongValuesContext,
        index: i64,
    ) -> Result<(i64, LongValuesContext)> {
        Ok((index + self.mutable.get(index as usize), ctx))
    }
}

impl NumericDocValues for MutableAsLongValues {
    fn get_with_ctx(
        &self,
        ctx: NumericDocValuesContext,
        doc_id: DocId,
    ) -> Result<(i64, NumericDocValuesContext)> {
        Ok((self.get64(doc_id as i64)?, ctx))
    }
}

struct PackedLongValuesWrapper {
    values: PackedLongValues,
}

impl LongValues for PackedLongValuesWrapper {
    fn get64_with_ctx(
        &self,
        ctx: LongValuesContext,
        index: i64,
    ) -> Result<(i64, LongValuesContext)> {
        let (ret, ctx) = self.values.get64_with_ctx(ctx, index)?;
        Ok((index + ret, ctx))
    }
}

impl NumericDocValues for PackedLongValuesWrapper {
    fn get_with_ctx(
        &self,
        ctx: NumericDocValuesContext,
        doc_id: DocId,
    ) -> Result<(i64, NumericDocValuesContext)> {
        Ok((self.get64(doc_id as i64)?, ctx))
    }
}
