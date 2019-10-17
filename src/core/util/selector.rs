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

use core::codec::points::MutablePointsReader;
use core::util::bit_util::UnsignedShift;

use std::cmp::Ordering;

pub trait Selector {
    fn select(&mut self, from: i32, to: i32, k: i32);
    fn swap(&mut self, i: i32, j: i32);
    fn set_pivot(&mut self, i: i32);
    fn compare_pivot(&mut self, j: i32) -> Ordering;

    fn compare(&mut self, i: i32, j: i32) -> Ordering {
        self.set_pivot(i);
        self.compare_pivot(j)
    }

    fn reset_k(&mut self, k: i32);
    fn byte_at(&self, _i: i32, _k: i32) -> i32 {
        unimplemented!()
    }

    fn quick_select(&mut self, from: i32, to: i32, k: i32, max_depth: i32) {
        debug_assert!(from <= k && k < to);
        if to - from == 1 {
            return;
        }

        let max_depth = max_depth - 1;
        if max_depth < 0 {
            // slowSelect(from, to, k);
            // return;
        }

        let mid = (from + to).unsigned_shift(1);
        // heuristic: we use the median of the values at from, to-1 and mid as a pivot
        if self.compare(from, to - 1) == Ordering::Greater {
            self.swap(from, to - 1);
        }

        if self.compare(to - 1, mid) == Ordering::Greater {
            self.swap(to - 1, mid);
            if self.compare(from, to - 1) == Ordering::Greater {
                self.swap(from, to - 1);
            }
        }

        self.set_pivot(to - 1);

        let mut left = from + 1;
        let mut right = to - 2;
        loop {
            while self.compare_pivot(left) == Ordering::Greater {
                left += 1;
            }

            while left < right && self.compare_pivot(right) != Ordering::Greater {
                right -= 1;
            }

            if left < right {
                self.swap(left, right);
                right -= 1;
            } else {
                break;
            }
        }

        self.swap(left, to - 1);

        if left == k {
            return;
        } else if left < k {
            self.quick_select(left + 1, to, k, max_depth);
        } else {
            self.quick_select(from, left, k, max_depth);
        }
    }
}

// after that many levels of recursion we fall back to introsort anyway
// this is used as a protection against the fact that radix sort performs
// worse when there are long common prefixes (probably because of cache
// locality)
const LEVEL_THRESHOLD: usize = 8;
// size of histograms: 256 + 1 to indicate that the string is finished
const HISTOGRAM_SIZE: usize = 257;
// buckets below this size will be sorted with introsort
const LENGTH_THRESHOLD: usize = 100;

pub struct DefaultIntroSelector<'a, 'b, P: MutablePointsReader> {
    k: i32,
    num_bytes_to_compare: i32,
    offset: usize,
    pivot: &'a mut Vec<u8>,
    pivot_doc: i32,
    scratch2: &'b mut Vec<u8>,
    reader: P,
    bits_per_doc_id: i32,
}

impl<'a, 'b, P: MutablePointsReader> DefaultIntroSelector<'a, 'b, P> {
    pub fn new(
        num_bytes_to_compare: i32,
        offset: usize,
        pivot: &'a mut Vec<u8>,
        scratch2: &'b mut Vec<u8>,
        reader: P,
        bits_per_doc_id: i32,
    ) -> Self {
        DefaultIntroSelector {
            k: 0,
            num_bytes_to_compare,
            offset,
            pivot,
            pivot_doc: -1,
            scratch2,
            reader,
            bits_per_doc_id,
        }
    }
}

impl<'a, 'b, P: MutablePointsReader> Selector for DefaultIntroSelector<'a, 'b, P> {
    fn select(&mut self, from: i32, to: i32, k: i32) {
        debug_assert!(from <= k && k < to);
        self.quick_select(from, to, k, 2 * ((((to - from) as f64).log2()) as i32));
    }

    fn swap(&mut self, i: i32, j: i32) {
        self.reader.swap(i, j);
    }

    fn set_pivot(&mut self, i: i32) {
        self.reader.value(i, &mut self.pivot);
        self.pivot_doc = self.reader.doc_id(i);
    }

    fn compare_pivot(&mut self, j: i32) -> Ordering {
        if self.k < self.num_bytes_to_compare {
            self.reader.value(j, &mut self.scratch2);
            let offset = self.offset;

            let len = self.num_bytes_to_compare as usize;
            let cmp = self.pivot[offset..offset + len].cmp(&self.scratch2[offset..offset + len]);
            if cmp != Ordering::Equal {
                return cmp;
            }
        }

        self.pivot_doc.cmp(&self.reader.doc_id(j))
    }

    fn reset_k(&mut self, k: i32) {
        self.k = k;
    }

    fn byte_at(&self, i: i32, k: i32) -> i32 {
        if k < self.num_bytes_to_compare {
            self.reader.byte_at(i, k) as i32
        } else {
            let shift = self.bits_per_doc_id - ((k - self.num_bytes_to_compare + 1) << 3);
            self.reader.doc_id(i).unsigned_shift(0.max(shift) as usize) & 0xff
        }
    }
}

pub struct RadixSelector<T: Selector> {
    histogram: Vec<i32>,
    common_prefix: Vec<i32>,
    max_length: i32,
    selector: T,
}

impl<T: Selector> RadixSelector<T> {
    pub fn new(max_length: i32, selector: T) -> RadixSelector<T> {
        let capacity = max_length.min(24);
        let mut common_prefix: Vec<i32> = Vec::with_capacity(capacity as usize);
        for _ in 0..capacity {
            common_prefix.push(0i32);
        }

        RadixSelector {
            histogram: vec![0i32; HISTOGRAM_SIZE],
            common_prefix,
            max_length,
            selector,
        }
    }

    pub fn select_radix(&mut self, from: i32, to: i32, k: i32) {
        debug_assert!(from <= k && k < to);
        self.select_inner(from, to, k, 0, 0);
    }

    fn select_inner(&mut self, from: i32, to: i32, k: i32, d: i32, l: i32) {
        if to - from <= LENGTH_THRESHOLD as i32 || d >= LEVEL_THRESHOLD as i32 {
            self.selector.reset_k(d);
            self.selector.select(from, to, k);
        } else {
            self.radix_select(from, to, k, d, l);
        }
    }

    fn radix_select(&mut self, from: i32, to: i32, k: i32, d: i32, l: i32) {
        let common_prefix_length =
            self.compute_common_prefix_length_and_build_histogram(from, to, d);
        if common_prefix_length > 0 {
            // if there are no more chars to compare or if all entries fell into the
            // first bucket (which means strings are shorter than k) then we are done
            // otherwise recurse

            if k + common_prefix_length < self.max_length && self.histogram[0] < to - from {
                self.radix_select(from, to, k, d + common_prefix_length, l);
            }

            return;
        }

        debug_assert!(self.assert_histogram(common_prefix_length));

        let mut bucket_from = from;
        for bucket in 0..HISTOGRAM_SIZE {
            let bucket_to = bucket_from + self.histogram[bucket];

            if bucket_to > k {
                self.partition(from, to, bucket as i32, bucket_from, bucket_to, d);

                if bucket != 0 && d + 1 < self.max_length {
                    // all elements in bucket 0 are equal so we only need to recurse if bucket != 0
                    self.select_inner(bucket_from, bucket_to, k, d + 1, l + 1);
                }

                return;
            }

            bucket_from = bucket_to;
        }
    }

    fn assert_histogram(&self, common_prefix_length: i32) -> bool {
        let mut number_of_uniqe_bytes = 0;
        for freq in &self.histogram {
            if *freq > 0 {
                number_of_uniqe_bytes += 1;
            }
        }

        if number_of_uniqe_bytes == 1 {
            debug_assert!(common_prefix_length >= 1);
        } else {
            debug_assert!(common_prefix_length == 0);
        }

        true
    }

    fn get_bucket(&self, i: i32, k: i32) -> usize {
        (self.selector.byte_at(i, k) + 1) as usize
    }

    fn compute_common_prefix_length_and_build_histogram(
        &mut self,
        from: i32,
        to: i32,
        k: i32,
    ) -> i32 {
        let mut common_prefix_length = self.common_prefix.len().min((self.max_length - k) as usize);
        for j in 0..common_prefix_length {
            let b = self.selector.byte_at(from, k + j as i32);
            self.common_prefix[j] = b;
            if b == -1 {
                common_prefix_length = j + 1;
                break;
            }
        }

        let i = 0;
        'outer: for i in from + 1..to {
            for j in 0..common_prefix_length {
                let b = self.selector.byte_at(i, k + j as i32);
                if b != self.common_prefix[j] {
                    common_prefix_length = j;
                    if common_prefix_length == 0 {
                        self.histogram[(self.common_prefix[0] + 1) as usize] = i - from;
                        self.histogram[(b + 1) as usize] = 1;
                        break 'outer;
                    }
                    break;
                }
            }
        }

        if i < to {
            debug_assert!(common_prefix_length == 0);
            self.build_histogram(i + 1, to, k);
        } else {
            debug_assert!(common_prefix_length > 0);
            self.histogram[(self.common_prefix[0] + 1) as usize] = to - from;
        }

        common_prefix_length as i32
    }

    fn build_histogram(&mut self, from: i32, to: i32, k: i32) {
        for i in from..to {
            let bucket = self.get_bucket(i, k);
            self.histogram[bucket] += 1;
        }
    }

    fn partition(
        &mut self,
        from: i32,
        to: i32,
        bucket: i32,
        bucket_from: i32,
        bucket_to: i32,
        d: i32,
    ) {
        let mut left = from;
        let mut right = to - 1;
        let mut slot = bucket_from;

        loop {
            let mut left_bucket = self.get_bucket(left, d) as i32;
            let mut right_bucket = self.get_bucket(right, d) as i32;

            while left_bucket <= bucket && left < bucket_from {
                if left_bucket == bucket {
                    self.selector.swap(left, slot);
                    slot += 1;
                } else {
                    left += 1;
                }

                left_bucket = self.get_bucket(left, d) as i32;
            }

            while right_bucket >= bucket && right >= bucket_to {
                if right_bucket == bucket {
                    self.selector.swap(right, slot);
                    slot += 1;
                } else {
                    right -= 1;
                }

                right_bucket = self.get_bucket(right, d) as i32;
            }

            if left < bucket_from && right >= bucket_to {
                self.selector.swap(left, right);
                left += 1;
                right -= 1;
            } else {
                debug_assert!(left == bucket_from && right == bucket_to - 1);
                break;
            }
        }
    }
}
