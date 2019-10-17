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

mod doc_ids_writer;

pub use self::doc_ids_writer::*;

mod bkd_reader;

pub use self::bkd_reader::*;

mod bkd_writer;

pub use self::bkd_writer::*;

mod heap_point;

use self::heap_point::*;

mod offline_point;

use self::offline_point::*;

use error::{ErrorKind::IllegalState, Result};

use core::util::DocId;

use core::codec::points::MutablePointsReader;
use core::store::directory::Directory;
use core::store::io::{DataOutput, IndexOutput, IndexOutputRef, InvalidIndexOutput};
use core::util::bit_util::{pop_array, BitsRequired, UnsignedShift};
use core::util::math;
use core::util::selector::{DefaultIntroSelector, RadixSelector};
use core::util::sorter::{check_range, MSBRadixSorter, MSBSorter, Sorter};

use std::cmp::Ordering;
use std::io;

pub const BKD_CODEC_NAME: &str = "BKD";
pub const BKD_VERSION_COMPRESSED_DOC_IDS: i32 = 1;
pub const BKD_VERSION_COMPRESSED_VALUES: i32 = 2;
pub const BKD_VERSION_IMPLICIT_SPLIT_DIM_1D: i32 = 3;
pub const BKD_VERSION_PACKED_INDEX: i32 = 4;
pub const BKD_VERSION_START: i32 = 0;
pub const BKD_VERSION_CURRENT: i32 = BKD_VERSION_PACKED_INDEX;

#[derive(PartialOrd, PartialEq)]
pub enum PointType {
    Offline,
    Heap,
    Other,
}

pub trait PointReader {
    fn next(&mut self) -> Result<bool>;
    fn packed_value(&self) -> &[u8];
    fn ord(&self) -> i64;
    fn doc_id(&self) -> DocId;
    fn mark_ords(&mut self, count: i64, ord_bit_set: &mut LongBitSet) -> Result<()> {
        for _ in 0..count {
            let result = self.next()?;
            if !result {
                bail!(IllegalState("did not see enough points from reader".into()));
            } else {
                let ord = self.ord();
                debug_assert_eq!(ord_bit_set.get(ord), false);
                ord_bit_set.set(ord);
            }
        }

        Ok(())
    }
    fn split(
        &mut self,
        count: i64,
        right_tree: &mut LongBitSet,
        left: &mut impl PointWriter,
        right: &mut impl PointWriter,
        do_clear_bits: bool,
    ) -> Result<i64> {
        // Partition this source according to how the splitDim split the values:
        let mut right_count = 0i64;
        for _ in 0..count {
            let result = self.next()?;
            debug_assert_eq!(result, true);
            let packed_value = self.packed_value();
            let ord = self.ord();
            let doc_id = self.doc_id();
            if right_tree.get(ord) {
                right.append(packed_value, ord, doc_id)?;
                right_count += 1;
                if do_clear_bits {
                    right_tree.clear(ord);
                }
            } else {
                left.append(&packed_value, ord, doc_id)?;
            }
        }

        Ok(right_count)
    }
}

pub enum PointReaderEnum {
    Heap(HeapPointReader),
    Offline(OfflinePointReader),
}

impl PointReader for PointReaderEnum {
    fn next(&mut self) -> Result<bool> {
        match self {
            PointReaderEnum::Heap(h) => h.next(),
            PointReaderEnum::Offline(o) => o.next(),
        }
    }
    fn packed_value(&self) -> &[u8] {
        match self {
            PointReaderEnum::Heap(h) => h.packed_value(),
            PointReaderEnum::Offline(o) => o.packed_value(),
        }
    }
    fn ord(&self) -> i64 {
        match self {
            PointReaderEnum::Heap(h) => h.ord(),
            PointReaderEnum::Offline(o) => o.ord(),
        }
    }
    fn doc_id(&self) -> DocId {
        match self {
            PointReaderEnum::Heap(h) => h.doc_id(),
            PointReaderEnum::Offline(o) => o.doc_id(),
        }
    }
    fn mark_ords(&mut self, count: i64, ord_bit_set: &mut LongBitSet) -> Result<()> {
        match self {
            PointReaderEnum::Heap(h) => h.mark_ords(count, ord_bit_set),
            PointReaderEnum::Offline(o) => o.mark_ords(count, ord_bit_set),
        }
    }
    fn split(
        &mut self,
        count: i64,
        right_tree: &mut LongBitSet,
        left: &mut impl PointWriter,
        right: &mut impl PointWriter,
        do_clear_bits: bool,
    ) -> Result<i64> {
        match self {
            PointReaderEnum::Heap(h) => h.split(count, right_tree, left, right, do_clear_bits),
            PointReaderEnum::Offline(o) => o.split(count, right_tree, left, right, do_clear_bits),
        }
    }
}

pub trait PointWriter {
    // add lifetime here, this is a reference type
    type IndexOutput: IndexOutput;
    type PointReader: PointReader;
    fn append(&mut self, packed_value: &[u8], ord: i64, doc_id: DocId) -> Result<()>;
    fn destory(&mut self) -> Result<()>;
    fn point_reader(&self, start_point: usize, length: usize) -> Result<Self::PointReader>;
    fn shared_point_reader(
        &mut self,
        start_point: usize,
        length: usize,
    ) -> Result<&mut Self::PointReader>;
    fn point_type(&self) -> PointType;
    fn index_output(&mut self) -> Self::IndexOutput;
    fn set_count(&mut self, count: i64);
    fn close(&mut self) -> Result<()>;

    fn try_as_heap_writer(&mut self) -> &mut HeapPointWriter {
        unimplemented!()
    }
    fn clone(&self) -> Self;
}

enum PointWriterEnum<D: Directory> {
    Heap(HeapPointWriter),
    Offline(OfflinePointWriter<D>),
}

impl<D: Directory> PointWriter for PointWriterEnum<D> {
    type IndexOutput = PointWriterOutput<D>;
    type PointReader = PointReaderEnum;

    fn append(&mut self, packed_value: &[u8], ord: i64, doc_id: DocId) -> Result<()> {
        match self {
            PointWriterEnum::Heap(h) => h.append(packed_value, ord, doc_id),
            PointWriterEnum::Offline(o) => o.append(packed_value, ord, doc_id),
        }
    }
    fn destory(&mut self) -> Result<()> {
        match self {
            PointWriterEnum::Heap(h) => h.destory(),
            PointWriterEnum::Offline(o) => o.destory(),
        }
    }
    fn point_reader(&self, start_point: usize, length: usize) -> Result<Self::PointReader> {
        match self {
            PointWriterEnum::Heap(h) => h.point_reader(start_point, length),
            PointWriterEnum::Offline(o) => o.point_reader(start_point, length),
        }
    }
    fn shared_point_reader(
        &mut self,
        start_point: usize,
        length: usize,
    ) -> Result<&mut Self::PointReader> {
        match self {
            PointWriterEnum::Heap(h) => h.shared_point_reader(start_point, length),
            PointWriterEnum::Offline(o) => o.shared_point_reader(start_point, length),
        }
    }
    fn point_type(&self) -> PointType {
        match self {
            PointWriterEnum::Heap(h) => h.point_type(),
            PointWriterEnum::Offline(o) => o.point_type(),
        }
    }
    fn index_output(&mut self) -> PointWriterOutput<D> {
        match self {
            PointWriterEnum::Heap(h) => PointWriterOutput::Heap(h.index_output()),
            PointWriterEnum::Offline(o) => PointWriterOutput::Offline(o.index_output()),
        }
    }
    fn set_count(&mut self, count: i64) {
        match self {
            PointWriterEnum::Heap(h) => h.set_count(count),
            PointWriterEnum::Offline(o) => o.set_count(count),
        }
    }
    fn close(&mut self) -> Result<()> {
        match self {
            PointWriterEnum::Heap(h) => h.close(),
            PointWriterEnum::Offline(o) => o.close(),
        }
    }

    fn try_as_heap_writer(&mut self) -> &mut HeapPointWriter {
        match self {
            PointWriterEnum::Heap(h) => h.try_as_heap_writer(),
            PointWriterEnum::Offline(o) => o.try_as_heap_writer(),
        }
    }
    fn clone(&self) -> Self {
        match self {
            PointWriterEnum::Heap(h) => PointWriterEnum::Heap(h.clone()),
            PointWriterEnum::Offline(o) => PointWriterEnum::Offline(o.clone()),
        }
    }
}

pub enum PointWriterOutput<D: Directory> {
    Heap(IndexOutputRef<InvalidIndexOutput>),
    Offline(IndexOutputRef<D::TempOutput>),
}

impl<D: Directory> IndexOutput for PointWriterOutput<D> {
    fn name(&self) -> &str {
        match self {
            PointWriterOutput::Heap(h) => h.name(),
            PointWriterOutput::Offline(o) => o.name(),
        }
    }
    fn file_pointer(&self) -> i64 {
        match self {
            PointWriterOutput::Heap(h) => h.file_pointer(),
            PointWriterOutput::Offline(o) => o.file_pointer(),
        }
    }
    fn checksum(&self) -> Result<i64> {
        match self {
            PointWriterOutput::Heap(h) => h.checksum(),
            PointWriterOutput::Offline(o) => o.checksum(),
        }
    }
}

impl<D: Directory> DataOutput for PointWriterOutput<D> {}

impl<D: Directory> io::Write for PointWriterOutput<D> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            PointWriterOutput::Heap(h) => h.write(buf),
            PointWriterOutput::Offline(o) => o.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            PointWriterOutput::Heap(h) => h.flush(),
            PointWriterOutput::Offline(o) => o.flush(),
        }
    }
}

/// BitSet of fixed length (num_bits), backed by accessible ({@link #getBits})
/// Vec<i64>, accessed with a long index. Use it only if you intend to store more
/// than 2.1B bits, otherwise you should use `FixedBitSet`.
///
/// @lucene.internal
pub struct LongBitSet {
    // Array of longs holding the bits
    bits: Vec<i64>,
    // The number of bits in use
    num_bits: i64,
    num_words: usize,
}

impl LongBitSet {
    /// returns the number of 64 bit words it would take to hold numBits
    pub fn bits2words(num_bits: i64) -> usize {
        // I.e.: get the word-offset of the last bit and add one (make sure to use >> so 0 returns
        // 0!)
        (((num_bits - 1) >> 6) + 1) as usize
    }

    pub fn new(num_bits: i64) -> LongBitSet {
        let num_words = LongBitSet::bits2words(num_bits);
        LongBitSet {
            bits: vec![0; num_words],
            num_bits,
            num_words,
        }
    }

    /// Returns number of set bits.  NOTE: this visits every long in the
    /// backing bits array, and the result is not internally cached.
    pub fn cardinality(&self) -> usize {
        pop_array(&self.bits, 0, self.num_words)
    }

    pub fn get(&self, index: i64) -> bool {
        debug_assert!(index >= 0 && index < self.num_bits);
        let word_num = (index >> 6) as usize;
        // signed shift will keep a negative index and force an
        // array-index-out-of-bounds-exception, removing the need for an explicit check.
        let mask = 1i64 << (index & 0x3fi64);
        (self.bits[word_num] & mask) != 0
    }

    pub fn set(&mut self, index: i64) {
        debug_assert!(index >= 0 && index < self.num_bits);
        let word_num = (index >> 6) as usize;
        let mask = 1i64 << (index & 0x3fi64);
        unsafe {
            *self.bits.as_mut_ptr().add(word_num) |= mask;
        }
    }

    pub fn clear(&mut self, index: i64) {
        debug_assert!(index >= 0 && index < self.num_bits);
        let word_num = (index >> 6) as usize;
        let mask = 1i64 << (index & 0x3fi64);
        unsafe {
            *self.bits.as_mut_ptr().add(word_num) &= !mask;
        }
    }
}

pub struct UtilMSBIntroSorter<P: MutablePointsReader> {
    k: i32,
    packed_bytes_length: i32,
    pivot_doc: i32,
    pivot: Vec<u8>,
    scratch: Vec<u8>,
    // TODO: we use raw pointer because the `fallback_sorter` method need to return a copy of this
    // reader, thus there will be 2 mut ref for reader in conflict with the borrow-checker rule
    reader: *mut P,
    bits_per_doc_id: i32,
}

impl<P: MutablePointsReader> UtilMSBIntroSorter<P> {
    pub fn new(k: i32, packed_bytes_length: i32, reader: *mut P, bits_per_doc_id: i32) -> Self {
        UtilMSBIntroSorter {
            k,
            packed_bytes_length,
            pivot_doc: 0,
            pivot: vec![0u8; packed_bytes_length as usize + 1],
            scratch: vec![0u8; packed_bytes_length as usize + 1],
            reader,
            bits_per_doc_id,
        }
    }

    #[inline]
    #[allow(clippy::mut_from_ref)]
    fn reader(&self) -> &mut P {
        unsafe { &mut *self.reader }
    }
}

impl<P: MutablePointsReader> MSBSorter for UtilMSBIntroSorter<P> {
    type Fallback = UtilMSBIntroSorter<P>;
    fn byte_at(&self, i: i32, k: i32) -> Option<u8> {
        let v = if k < self.packed_bytes_length {
            self.reader().byte_at(i, k)
        } else {
            let shift = self.bits_per_doc_id - ((k - self.packed_bytes_length + 1) << 3);
            (self
                .reader()
                .doc_id(i)
                .unsigned_shift(0.max(shift) as usize)
                & 0xff) as u8
        };
        Some(v)
    }

    fn msb_swap(&mut self, i: i32, j: i32) {
        self.reader().swap(i, j);
    }

    fn fallback_sorter(&mut self, k: i32) -> UtilMSBIntroSorter<P> {
        UtilMSBIntroSorter::new(
            k,
            self.packed_bytes_length,
            self.reader,
            self.bits_per_doc_id,
        )
    }
}

impl<P: MutablePointsReader> Sorter for UtilMSBIntroSorter<P> {
    fn compare(&mut self, i: i32, j: i32) -> Ordering {
        self.set_pivot(i);
        self.compare_pivot(j)
    }

    fn swap(&mut self, i: i32, j: i32) {
        self.reader().swap(i, j)
    }

    fn sort(&mut self, from: i32, to: i32) {
        check_range(from, to);

        self.quick_sort(from, to, 2 * math::log((to - from) as i64, 2));
    }

    fn set_pivot(&mut self, i: i32) {
        unsafe {
            (*self.reader).value(i, &mut self.pivot);
            self.pivot_doc = (*self.reader).doc_id(i);
        }
    }

    fn compare_pivot(&mut self, j: i32) -> Ordering {
        if self.k < self.packed_bytes_length {
            unsafe {
                (*self.reader).value(j, &mut self.scratch);
            }

            let count = (self.packed_bytes_length - self.k) as usize;
            let offset = self.k as usize;
            let cmp = self.pivot[offset..offset + count].cmp(&self.scratch[offset..offset + count]);
            if cmp != Ordering::Equal {
                return cmp;
            }
        }

        self.pivot_doc.cmp(&self.reader().doc_id(j))
    }
}

pub struct DimIntroSorter<'a, 'b, P: MutablePointsReader> {
    num_bytes_to_compare: i32,
    offset: i32,
    pivot_doc: i32,
    pivot: &'a mut Vec<u8>,
    scratch2: &'b mut Vec<u8>,
    reader: P,
}

impl<'a, 'b, P: MutablePointsReader> DimIntroSorter<'a, 'b, P> {
    pub fn new(
        num_bytes_to_compare: i32,
        offset: i32,
        scratch1: &'a mut Vec<u8>,
        scratch2: &'b mut Vec<u8>,
        reader: P,
    ) -> Self {
        DimIntroSorter {
            num_bytes_to_compare,
            offset,
            pivot_doc: -1,
            pivot: scratch1,
            scratch2,
            reader,
        }
    }
}

impl<'a, 'b, P: MutablePointsReader> Sorter for DimIntroSorter<'a, 'b, P> {
    fn compare(&mut self, i: i32, j: i32) -> Ordering {
        self.set_pivot(i);
        self.compare_pivot(j)
    }

    fn swap(&mut self, i: i32, j: i32) {
        self.reader.swap(i, j)
    }

    fn sort(&mut self, from: i32, to: i32) {
        check_range(from, to);
        self.quick_sort(from, to, 2 * ((((to - from) as f64).log2()) as i32));
    }

    fn set_pivot(&mut self, i: i32) {
        self.reader.value(i, &mut self.pivot);
        self.pivot_doc = self.reader.doc_id(i);
    }

    fn compare_pivot(&mut self, j: i32) -> Ordering {
        self.reader.value(j, &mut self.scratch2);

        let offset = self.offset as usize;
        let cmp = self.pivot[offset..offset + self.num_bytes_to_compare as usize]
            .cmp(&self.scratch2[offset..offset + self.num_bytes_to_compare as usize]);
        if cmp != Ordering::Equal {
            cmp
        } else {
            self.pivot_doc.cmp(&self.reader.doc_id(j))
        }
    }
}

pub struct MutablePointsReaderUtils {}

impl MutablePointsReaderUtils {
    pub fn sort(
        max_doc: i32,
        packed_bytes_length: i32,
        reader: &mut impl MutablePointsReader,
        from: i32,
        to: i32,
    ) {
        let bit_per_doc_id = (max_doc - 1).bits_required() as i32;
        let intro_sorter =
            UtilMSBIntroSorter::new(0, packed_bytes_length as i32, reader, bit_per_doc_id);
        let mut msb_sorter =
            MSBRadixSorter::new(packed_bytes_length + (bit_per_doc_id + 7) / 8, intro_sorter);
        msb_sorter.sort(from, to);
    }

    #[allow(clippy::too_many_arguments)]
    pub fn sort_by_dim(
        sorted_dim: i32,
        bytes_per_dim: i32,
        common_prefix_lengths: &[i32],
        reader: impl MutablePointsReader,
        from: i32,
        to: i32,
        scratch1: &mut Vec<u8>,
        scratch2: &mut Vec<u8>,
    ) {
        let offset = sorted_dim * bytes_per_dim + common_prefix_lengths[sorted_dim as usize];
        let num_bytes_to_compare = bytes_per_dim - common_prefix_lengths[sorted_dim as usize];

        let mut intro_sorter =
            DimIntroSorter::new(num_bytes_to_compare, offset, scratch1, scratch2, reader);
        intro_sorter.sort(from, to);
    }

    #[allow(clippy::too_many_arguments)]
    pub fn partition(
        max_doc: i32,
        split_dim: i32,
        bytes_per_dim: i32,
        common_prefix_len: i32,
        reader: impl MutablePointsReader,
        from: i32,
        to: i32,
        mid: i32,
        scratch1: &mut Vec<u8>,
        scratch2: &mut Vec<u8>,
    ) {
        let offset = split_dim * bytes_per_dim + common_prefix_len;
        let cmp_bytes = bytes_per_dim - common_prefix_len;
        let bit_per_doc_id = (max_doc - 1).bits_required() as i32;

        let selector = DefaultIntroSelector::new(
            cmp_bytes,
            offset as usize,
            scratch1,
            scratch2,
            reader,
            bit_per_doc_id,
        );

        let mut radix_selector = RadixSelector::new(cmp_bytes + (bit_per_doc_id + 7) / 8, selector);

        radix_selector.select_radix(from, to, mid);
    }
}
