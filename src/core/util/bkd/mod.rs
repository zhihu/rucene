mod doc_ids_writer;

pub use self::doc_ids_writer::*;

mod bkd_reader;

pub use self::bkd_reader::*;

mod bkd_writer;

pub use self::bkd_writer::*;

mod heap_point;

pub use self::heap_point::*;

mod offline_point;

pub use self::offline_point::*;

use error::*;

use core::util::DocId;

use core::codec::MutablePointsReader;
use core::store::IndexOutput;

use core::util::bit_util::{pop_array, UnsignedShift};
use core::util::packed::packed_misc::unsigned_bits_required;
use core::util::selector::{DefaultIntroSelector, RadixSelector};
use core::util::sorter::{check_range, MSBRadixSorter, MSBSorter, Sorter};

use std::cmp::Ordering;

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
    fn packed_value(&self) -> Vec<u8>;
    fn ord(&self) -> i64;
    fn doc_id(&self) -> DocId;
    fn mark_ords(&mut self, count: i64, ord_bit_set: &mut LongBitSet) -> Result<()> {
        for _ in 0..count {
            let result = self.next()?;
            if !result {
                bail!("did not see enough points from reader");
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
        left: &mut PointWriter,
        right: &mut PointWriter,
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
                right.append(&packed_value, ord, doc_id)?;
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

pub trait PointWriter {
    fn append(&mut self, packed_value: &[u8], ord: i64, doc_id: DocId) -> Result<()>;
    fn destory(&mut self) -> Result<()>;
    fn point_reader(&self, start_point: usize, length: usize) -> Result<Box<PointReader>>;
    fn shared_point_reader(
        &mut self,
        start_point: usize,
        length: usize,
        to_close_heroically: &mut Vec<Box<PointReader>>,
    ) -> Result<&mut PointReader>;
    fn point_type(&self) -> PointType;
    fn index_output(&mut self) -> &mut IndexOutput;
    fn set_count(&mut self, count: i64);
    fn close(&mut self) -> Result<()>;

    fn try_as_heap_writer(&mut self) -> &mut HeapPointWriter {
        unimplemented!()
    }
    fn clone(&self) -> Box<PointWriter>;
}

/// BitSet of fixed length (numBits), backed by accessible ({@link #getBits})
/// long[], accessed with a long index. Use it only if you intend to store more
/// than 2.1B bits, otherwise you should use {@link FixedBitSet}.
///
/// @lucene.internal
///
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
            *self.bits.as_mut_ptr().offset(word_num as isize) |= mask;
        }
    }

    pub fn clear(&mut self, index: i64) {
        debug_assert!(index >= 0 && index < self.num_bits);
        let word_num = (index >> 6) as usize;
        let mask = 1i64 << (index & 0x3fi64);
        unsafe {
            *self.bits.as_mut_ptr().offset(word_num as isize) &= !mask;
        }
    }
}

pub struct UtilMSBIntroSorter {
    k: i32,
    packed_bytes_length: i32,
    pivot_doc: i32,
    pivot: Vec<u8>,
    scratch: Vec<u8>,
    reader: Box<MutablePointsReader>,
    bits_per_doc_id: i32,
}

impl UtilMSBIntroSorter {
    pub fn new(
        k: i32,
        packed_bytes_length: i32,
        reader: Box<MutablePointsReader>,
        bits_per_doc_id: i32,
    ) -> UtilMSBIntroSorter {
        UtilMSBIntroSorter {
            k,
            packed_bytes_length,
            pivot_doc: -1,
            pivot: vec![0u8; packed_bytes_length as usize + 1],
            scratch: vec![0u8; packed_bytes_length as usize + 1],
            reader,
            bits_per_doc_id,
        }
    }
}

impl MSBSorter for UtilMSBIntroSorter {
    type Fallback = UtilMSBIntroSorter;
    fn byte_at(&self, i: i32, k: i32) -> Option<u8> {
        let v = if k < self.packed_bytes_length {
            self.reader.byte_at(i, k)
        } else {
            let shift = self.bits_per_doc_id - ((k - self.packed_bytes_length + 1) << 3);
            (self.reader.doc_id(i).unsigned_shift(0.max(shift) as usize) & 0xff) as u8
        };
        Some(v)
    }

    fn msb_swap(&mut self, i: i32, j: i32) {
        self.reader.swap(i, j);
    }

    fn fallback_sorter(&mut self, k: i32) -> UtilMSBIntroSorter {
        UtilMSBIntroSorter::new(
            k,
            self.packed_bytes_length,
            self.reader.clone(),
            self.bits_per_doc_id,
        )
    }
}

impl Sorter for UtilMSBIntroSorter {
    fn swap(&mut self, i: i32, j: i32) {
        self.reader.swap(i, j)
    }

    fn sort(&mut self, from: i32, to: i32) {
        check_range(from, to);
        self.quick_sort(from, to, 2 * ((((to - from) as f64).log2()) as i32));
    }

    fn compare(&mut self, i: i32, j: i32) -> Ordering {
        self.set_pivot(i);
        self.compare_pivot(j)
    }

    fn set_pivot(&mut self, i: i32) {
        self.reader.value(i, &mut self.pivot);
        self.pivot_doc = self.reader.doc_id(i);
    }

    fn compare_pivot(&mut self, j: i32) -> Ordering {
        if self.k < self.packed_bytes_length {
            self.reader.value(j, &mut self.scratch);

            let count = (self.packed_bytes_length - self.k) as usize;
            let offset = self.k as usize;
            let cmp = self.pivot[offset..offset + count].cmp(&self.scratch[offset..offset + count]);
            if cmp != Ordering::Equal {
                return cmp;
            }
        }

        self.pivot_doc.cmp(&self.reader.doc_id(j))
    }
}

pub struct DimIntroSorter<'a, 'b> {
    num_bytes_to_compare: i32,
    offset: i32,
    pivot_doc: i32,
    pivot: &'a mut Vec<u8>,
    scratch2: &'b mut Vec<u8>,
    reader: Box<MutablePointsReader>,
}

impl<'a, 'b> DimIntroSorter<'a, 'b> {
    pub fn new(
        num_bytes_to_compare: i32,
        offset: i32,
        scratch1: &'a mut Vec<u8>,
        scratch2: &'b mut Vec<u8>,
        reader: Box<MutablePointsReader>,
    ) -> DimIntroSorter<'a, 'b> {
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

impl<'a, 'b> Sorter for DimIntroSorter<'a, 'b> {
    fn swap(&mut self, i: i32, j: i32) {
        self.reader.swap(i, j)
    }

    fn sort(&mut self, from: i32, to: i32) {
        check_range(from, to);
        self.quick_sort(from, to, 2 * ((((to - from) as f64).log2()) as i32));
    }

    fn compare(&mut self, i: i32, j: i32) -> Ordering {
        self.set_pivot(i);
        self.compare_pivot(j)
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
        reader: Box<MutablePointsReader>,
        from: i32,
        to: i32,
    ) {
        let bit_per_doc_id = unsigned_bits_required((max_doc - 1) as i64);
        let intro_sorter =
            UtilMSBIntroSorter::new(0, packed_bytes_length as i32, reader, bit_per_doc_id);
        let mut msb_sorter =
            MSBRadixSorter::new(packed_bytes_length + (bit_per_doc_id + 7) / 8, intro_sorter);
        msb_sorter.sort(from, to);
    }

    pub fn sort_by_dim(
        sorted_dim: i32,
        bytes_per_dim: i32,
        common_prefix_lengths: &Vec<i32>,
        reader: Box<MutablePointsReader>,
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

    pub fn partition(
        max_doc: i32,
        split_dim: i32,
        bytes_per_dim: i32,
        common_prefix_len: i32,
        reader: Box<MutablePointsReader>,
        from: i32,
        to: i32,
        mid: i32,
        scratch1: &mut Vec<u8>,
        scratch2: &mut Vec<u8>,
    ) {
        let offset = split_dim * bytes_per_dim + common_prefix_len;
        let cmp_bytes = bytes_per_dim - common_prefix_len;
        let bit_per_doc_id = unsigned_bits_required((max_doc - 1) as i64);

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
