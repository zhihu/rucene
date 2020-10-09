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

use std::sync::Arc;

use core::search::{DocIterator, NO_MORE_DOCS};
use core::util::bit_util::{self, UnsignedShift};
use core::util::{Bits, BitsRef};

use error::{ErrorKind, Result};
use std::intrinsics::volatile_set_memory;

pub trait ImmutableBitSet: Bits {
    /// Return the number of bits that are set.
    /// this method is likely to run in linear time
    fn cardinality(&self) -> usize;

    fn approximate_cardinality(&self) -> usize {
        self.cardinality()
    }

    /// Returns the index of the first set bit starting at the index specified.
    /// `DocIdSetIterator#NO_MORE_DOCS` is returned if there are no more set bits.
    fn next_set_bit(&self, index: usize) -> i32;

    fn assert_unpositioned(&self, iter: &dyn DocIterator) -> Result<()> {
        if iter.doc_id() != -1 {
            bail!(ErrorKind::IllegalState(format!(
                "This operation only works with an unpositioned iterator, got current position = \
                 {}",
                iter.doc_id()
            )))
        }
        Ok(())
    }
}

pub struct BitSetIterator<'a, S> {
    bit_set: &'a S,
    current: i32,
}

impl<'a, S: ImmutableBitSet> BitSetIterator<'a, S> {
    pub fn new(bit_set: &'a S) -> Self {
        Self {
            bit_set,
            current: -1,
        }
    }
}

impl<'a, S: ImmutableBitSet + 'a> Iterator for BitSetIterator<'a, S> {
    type Item = i32;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current + 1 >= self.bit_set.len() as i32 {
            None
        } else {
            self.current = self.bit_set.next_set_bit((self.current + 1) as usize);
            if self.current == NO_MORE_DOCS {
                None
            } else {
                Some(self.current)
            }
        }
    }
}

/// Base implementation for a bit set.
pub trait BitSet: ImmutableBitSet {
    fn set(&mut self, i: usize);

    fn batch_set(&mut self, start: usize, end: usize) {
        for i in start..end {
            self.set(i);
        }
    }

    fn clear(&mut self, index: usize);

    /// Clears a range of bits.
    fn clear_batch(&mut self, start_index: usize, end_index: usize);

    /// Does in-place OR of the bits provided by the iterator. The state of the
    /// iterator after this operation terminates is undefined.
    fn or(&mut self, iter: &mut dyn DocIterator) -> Result<()> {
        self.assert_unpositioned(iter)?;
        loop {
            let doc = iter.next()?;
            if doc == NO_MORE_DOCS {
                break;
            }
            self.set(doc as usize);
        }
        Ok(())
    }

    fn as_fixed_bit_set(&self) -> &FixedBitSet {
        unimplemented!()
    }
}

/// BitSet of fixed length (numBits), backed by accessible `#getBits`
/// Vec<i64>, accessed with an int index, implementing `Bits` and
/// `DocIdSet`. If you need to manage more than 2.1B bits, use
/// `LongBitSet`.
pub struct FixedBitSet {
    pub bits: Vec<i64>,
    // Array of longs holding the bits
    pub num_bits: usize,
    // The number of bits in use
    pub num_words: usize,
    // The exact number of longs needed to hold numBits (<= bits.length)
}

impl Default for FixedBitSet {
    fn default() -> Self {
        FixedBitSet {
            bits: Vec::with_capacity(0),
            num_bits: 0,
            num_words: 0,
        }
    }
}

impl FixedBitSet {
    /// Creates a new LongBitSet.
    /// The internally allocated long array will be exactly the size needed to accommodate the
    /// numBits specified. @param numBits the number of bits needed
    pub fn new(num_bits: usize) -> FixedBitSet {
        let num_words = bits2words(num_bits);
        let bits = vec![0; num_words];
        FixedBitSet {
            num_bits,
            bits,
            num_words,
        }
    }

    /// Creates a new LongBitSet using the provided Vec<i64> array as backing store.
    /// The storedBits array must be large enough to accommodate the numBits specified, but may be
    /// larger. In that case the 'extra' or 'ghost' bits must be clear (or they may provoke
    /// spurious side-effects) @param storedBits the array to use as backing store
    /// @param numBits the number of bits actually needed
    pub fn copy_from(stored_bits: Vec<i64>, num_bits: usize) -> Result<FixedBitSet> {
        let num_words = bits2words(num_bits);
        if num_words > stored_bits.len() {
            bail!(ErrorKind::IllegalArgument(format!(
                "The given long array is too small  to hold {} bits.",
                num_bits
            )));
        }

        let bits = FixedBitSet {
            bits: stored_bits,
            num_words,
            num_bits,
        };
        assert!(bits.verify_ghost_bits_clear());
        Ok(bits)
    }

    /// If the given {@link FixedBitSet} is large enough to hold {@code numBits+1},
    /// returns the given bits, otherwise returns a new {@link FixedBitSet} which
    /// can hold the requested number of bits.
    ///
    /// NOTE: the returned bit set reuses the underlying Vec<i64> of
    /// the given `bits` if possible. Also, calling {@link #length()} on the
    /// returned bits may return a value greater than {@code numBits}.
    pub fn ensure_capacity(&mut self, num_bits: usize) {
        if num_bits >= self.num_bits {
            // Depends on the ghost bits being clear!
            // (Otherwise, they may become visible in the new instance)
            let num_words = bits2words(num_bits);
            if num_words >= self.bits.len() {
                self.bits.resize(num_words + 1usize, 0i64);
                self.num_words = num_words + 1usize;
                self.num_bits = self.num_words << 6usize;
            }
        }
    }

    pub fn resize(&mut self, num_bits: usize) {
        let num_words = bits2words(num_bits);
        if num_words != self.bits.len() {
            self.bits.resize(num_words, 0);
            self.num_words = num_words;
            self.num_bits = self.num_words << 6usize;
        }
    }

    pub fn encode_size(&self) -> usize {
        self.num_words << 3
    }

    pub fn count_ones_before_index2(
        &self,
        doc_upto: i32,
        bit_index: usize,
        end_index: usize,
    ) -> u32 {
        let mut count = doc_upto as u32;
        if end_index > bit_index {
            let mut start_high = bit_index >> 6;
            let end_high = end_index >> 6;
            if start_high < end_high {
                let remain = self.bits[start_high] as usize >> (bit_index & 0x3F);
                if remain != 0 {
                    count += (remain as i64).count_ones();
                }
                start_high += 1;
                for i in start_high..end_high {
                    if self.bits[i] != 0 {
                        count += self.bits[i].count_ones();
                    }
                }
                let low_value = end_index & 0x3F;
                if low_value > 0 {
                    let value = self.bits[end_high] & ((1usize << low_value) - 1) as i64;
                    if value > 0 {
                        count += value.count_ones();
                    }
                }
            } else {
                let end_remain = end_index & 0x3F;
                if end_remain > 0 {
                    let value = self.bits[end_high] & ((1usize << end_remain) - 1) as i64;
                    if value > 0 {
                        let bit_remain = bit_index & 0x3F;
                        let value = value >> bit_remain as i64;
                        if value > 0 {
                            count += value.count_ones();
                        }
                    }
                }
            }
        }
        count
    }

    pub fn count_ones_before_index(&self, end_index: usize) -> u32 {
        let mut count = 0;
        if end_index > 0 {
            let index = end_index - 1;
            let max = index >> 6;
            for i in 0..max {
                count += self.bits[i].count_ones();
            }
            let num_bits = (index & 0x3Fusize) + 1;
            count += if num_bits == 64 {
                self.bits[max].count_ones()
            } else {
                (self.bits[max] & ((1usize << num_bits) - 1) as i64).count_ones()
            };
        }
        count
    }

    #[inline]
    pub fn clear_all(&mut self) {
        unsafe {
            volatile_set_memory(self.bits.as_mut_ptr(), 0, self.bits.len());
        }
    }

    /// Checks if the bits past numBits are clear. Some methods rely on this implicit
    /// assumption: search for "Depends on the ghost bits being clear!"
    /// @return true if the bits past numBits are clear.
    fn verify_ghost_bits_clear(&self) -> bool {
        for i in self.num_words..self.bits.len() {
            if self.bits[i] != 0 {
                return false;
            }
        }
        if self.num_bits.trailing_zeros() >= 6 {
            return true;
        }
        let mask = -1i64 << (self.num_bits & 0x3f);
        (self.bits[self.num_words - 1] & mask) == 0
    }

    pub fn flip(&mut self, start_index: usize, end_index: usize) {
        debug_assert!(start_index < self.num_bits);
        debug_assert!(end_index <= self.num_bits);
        if end_index <= start_index {
            return;
        }
        let start_word = start_index >> 6;
        let end_word = (end_index - 1) >> 6;

        let start_mask = !((-1i64) << (start_index & 0x3fusize)) as i64;
        let end_mask = !((-1i64).unsigned_shift((64usize - (end_index & 0x3fusize)) & 0x3fusize));

        if start_word == end_word {
            self.bits[start_word] ^= start_mask | end_mask;
            return;
        }

        // optimize tight loop with unsafe
        self.bits[start_word] ^= start_mask;
        unsafe {
            let ptr = self.bits.as_mut_ptr();
            for i in start_word + 1..end_word {
                let e = ptr.add(i);
                *e = !*e;
            }
        }
        self.bits[end_word] ^= end_mask;
    }

    /// returns true if the sets have any elements in common
    pub fn intersects(&self, other: &FixedBitSet) -> bool {
        // Depends on the ghost bits being clear!
        let pos = self.num_words.min(other.num_words);
        for i in 0..pos {
            if (self.bits[i] & other.bits[i]) != 0 {
                return true;
            }
        }
        false
    }

    pub fn set_or(&mut self, other: &FixedBitSet) {
        self.do_or(&other.bits, other.num_words);
    }

    fn do_or(&mut self, other_arr: &[i64], other_num_words: usize) {
        assert!(other_num_words <= self.num_words);
        let this_arr = &mut self.bits;
        for i in 0..other_num_words {
            this_arr[i] |= other_arr[i];
        }
    }
}

impl ImmutableBitSet for FixedBitSet {
    fn cardinality(&self) -> usize {
        bit_util::pop_array(&self.bits, 0, self.num_words)
    }

    fn next_set_bit(&self, index: usize) -> i32 {
        // Depends on the ghost bits being clear!
        debug_assert!(index < self.num_bits);
        let mut i = index >> 6;
        // skip all the bits to the right of index
        let word = unsafe { *self.bits.as_ptr().add(i) } >> (index & 0x3fusize);

        if word != 0 {
            return (index as u32 + word.trailing_zeros()) as i32;
        }

        unsafe {
            let bits_ptr = self.bits.as_ptr();
            loop {
                i += 1;
                if i >= self.num_words {
                    break;
                }
                let word = *bits_ptr.add(i);
                if word != 0 {
                    return ((i << 6) as u32 + word.trailing_zeros()) as i32;
                }
            }
        }
        NO_MORE_DOCS
    }
}

impl BitSet for FixedBitSet {
    #[inline]
    fn set(&mut self, index: usize) {
        debug_assert!(index < self.num_bits);
        let word_num = index >> 6;
        let mask = 1i64 << (index & 0x3fusize);
        unsafe {
            *self.bits.as_mut_ptr().add(word_num) |= mask;
        }
    }

    fn batch_set(&mut self, start_index: usize, end_index: usize) {
        debug_assert!(start_index < self.num_bits && end_index <= self.num_bits);
        if start_index >= end_index {
            return;
        }

        let start_word = start_index >> 6;
        let end_word = (end_index - 1) >> 6;

        let start_mask = (-1i64) << (start_index & 0x3fusize) as i64;
        let end_mask = (-1i64).unsigned_shift((64usize - (end_index & 0x3fusize)) & 0x3fusize);

        if start_word == end_word {
            self.bits[start_word] |= start_mask & end_mask;
            return;
        }

        self.bits[start_word] |= start_mask;
        for i in start_word + 1..end_word {
            self.bits[i] = -1;
        }
        self.bits[end_word] |= end_mask;
    }

    fn clear(&mut self, index: usize) {
        debug_assert!(index < self.num_bits);
        let word = index >> 6;
        let mask = !(1i64 << (index & 0x3fusize)) as i64;
        self.bits[word] &= mask;
    }

    fn clear_batch(&mut self, start_index: usize, end_index: usize) {
        debug_assert!(start_index <= self.num_bits);
        debug_assert!(end_index <= self.num_bits);
        if end_index <= start_index {
            return;
        }
        let start_word = start_index >> 6;
        let end_word = (end_index - 1) >> 6;

        // invert mask since we ar clear
        let start_mask = !((-1i64) << (start_index & 0x3fusize) as i64);
        let end_mask = !((-1i64).unsigned_shift((64usize - (end_index & 0x3fusize)) & 0x3fusize));

        if start_word == end_word {
            self.bits[start_word] &= start_mask | end_mask;
            return;
        }

        self.bits[start_word] &= start_mask;
        for i in start_word + 1..end_word {
            self.bits[i] = 0i64;
        }
        self.bits[end_word] &= end_mask;
    }

    fn as_fixed_bit_set(&self) -> &FixedBitSet {
        self
    }
}

impl Bits for FixedBitSet {
    #[inline]
    fn get(&self, index: usize) -> Result<bool> {
        debug_assert!(index < self.num_bits);
        let i = index >> 6; // div 64
                            // signed shift will keep a negative index and force an
                            // array-index-out-of-bounds-exception, removing the need for an explicit check.
        let mask = 1i64 << (index & 0x3fusize);
        Ok(unsafe { *self.bits.as_ptr().add(i) & mask != 0 })
    }

    fn len(&self) -> usize {
        self.num_bits
    }

    fn as_bit_set(&self) -> &dyn BitSet {
        self
    }

    fn as_bit_set_mut(&mut self) -> &mut dyn BitSet {
        self
    }

    fn clone_box(&self) -> BitsRef {
        Arc::new(Self::copy_from(self.bits.clone(), self.num_bits).unwrap())
    }
}

/// returns the number of 64 bit words it would take to hold numBits
pub fn bits2words(num_bits: usize) -> usize {
    let num_bits = num_bits as i32;
    // I.e.: get the word-offset of the last bit and add one (make sure to use >> so 0 returns 0!)
    (((num_bits - 1) >> 6) + 1) as usize
}
