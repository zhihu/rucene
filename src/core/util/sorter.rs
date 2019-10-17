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

use core::util::bit_util::UnsignedShift;

use std::cmp::Ordering;

pub const INSERTION_SORT_THRESHOLD: usize = 30;
pub const BINARY_SORT_THRESHOLD: usize = 20;
pub const LSB_HISTOGRAM_SIZE: usize = 256;

/// A LSB Radix sorter for unsigned int values.
pub struct LSBRadixSorter {
    histogram: [i32; LSB_HISTOGRAM_SIZE],
}

impl LSBRadixSorter {
    fn build_histogram(array: &[i32], len: usize, histogram: &mut [i32], shift: usize) {
        for i in array.iter().take(len) {
            let b = i.unsigned_shift(shift) & 0xffi32;
            histogram[b as usize] += 1;
        }
    }

    fn sum_histogram(histogram: &mut [i32]) {
        let mut accum = 0i32;
        for count in histogram.iter_mut().take(LSB_HISTOGRAM_SIZE) {
            let temp = *count;
            *count = accum;
            accum += temp;
        }
    }

    fn reorder(array: &[i32], len: usize, histogram: &mut [i32], shift: u32, dest: &mut [i32]) {
        for v in array.iter().take(len) {
            let b = ((*v >> shift) & 0xffi32) as usize;
            dest[histogram[b] as usize] = *v;
            histogram[b] += 1;
        }
    }

    fn sort_histogram(
        array: &[i32],
        len: usize,
        histogram: &mut [i32],
        shift: usize,
        dest: &mut [i32],
    ) -> bool {
        for ent in histogram.iter_mut() {
            *ent = 0;
        }

        LSBRadixSorter::build_histogram(array, len, histogram, shift);
        if histogram[0] == len as i32 {
            return false;
        }
        LSBRadixSorter::sum_histogram(histogram);
        LSBRadixSorter::reorder(array, len, histogram, shift as u32, dest);
        true
    }

    pub fn sort(&mut self, num_bits: usize, array: &mut [i32], len: usize) {
        if len < INSERTION_SORT_THRESHOLD {
            array[0..len].sort();
            return;
        }

        let mut buffer = vec![0i32; len];
        let mut shift = 0usize;
        let mut origin = true;
        loop {
            if shift >= num_bits {
                break;
            }

            if origin {
                if LSBRadixSorter::sort_histogram(
                    array,
                    len,
                    &mut self.histogram[..],
                    shift,
                    &mut buffer,
                ) {
                    origin = !origin;
                }
            } else if LSBRadixSorter::sort_histogram(
                &buffer,
                len,
                &mut self.histogram[..],
                shift,
                array,
            ) {
                origin = !origin;
            }
            shift += 8;
        }

        if !origin {
            array[0..len].copy_from_slice(&buffer[0..len]);
        }
    }
}

impl Default for LSBRadixSorter {
    fn default() -> Self {
        let histogram = [0i32; LSB_HISTOGRAM_SIZE];
        LSBRadixSorter { histogram }
    }
}

pub trait Sorter {
    fn compare(&mut self, i: i32, j: i32) -> Ordering;
    fn swap(&mut self, i: i32, j: i32);
    fn sort(&mut self, from: i32, to: i32);
    fn set_pivot(&mut self, i: i32);
    fn compare_pivot(&mut self, j: i32) -> Ordering;

    fn merge_in_place(&mut self, from: i32, mid: i32, to: i32) {
        if from == mid || mid == to || self.compare(mid - 1, mid) != Ordering::Greater {
            return;
        } else if to - from == 2 {
            self.swap(mid - 1, mid);
            return;
        }

        let mut from = from;
        while self.compare(from, mid) != Ordering::Greater {
            from += 1;
        }

        let mut to = to;
        while self.compare(mid - 1, to - 1) != Ordering::Greater {
            to -= 1;
        }

        let first_cut;
        let second_cut;
        let len11;
        let len22;
        if mid - from > to - mid {
            len11 = (mid - from).unsigned_shift(1);
            first_cut = from + len11;
            second_cut = self.lower(mid, to, first_cut);
            len22 = second_cut - mid;
        } else {
            len22 = (to - mid).unsigned_shift(1);
            second_cut = mid + len22;
            first_cut = self.upper(from, mid, second_cut);
            // len11 = first_cut + from;
        }

        self.rotate(first_cut, mid, second_cut);
        let new_mid = first_cut + len22;
        self.merge_in_place(from, first_cut, new_mid);
        self.merge_in_place(new_mid, second_cut, to);
    }

    fn lower(&mut self, from: i32, to: i32, val: i32) -> i32 {
        let mut from = from;
        let mut len = to - from;
        while len > 0 {
            let half = len.unsigned_shift(1);
            let mid = from + half;
            if self.compare(mid, val) == Ordering::Less {
                from = mid + 1;
                len = len - half - 1;
            } else {
                len = half;
            }
        }

        from
    }
    fn upper(&mut self, from: i32, to: i32, val: i32) -> i32 {
        let mut from = from;
        let mut len = to - from;
        while len > 0 {
            let half = len.unsigned_shift(1);
            let mid = from + half;
            if self.compare(val, mid) == Ordering::Less {
                len = half;
            } else {
                from = mid + 1;
                len = len - half - 1;
            }
        }

        from
    }

    fn lower2(&mut self, from: i32, to: i32, val: i32) -> i32 {
        let mut f = to - 1;
        let mut t = to;
        while f > from {
            if self.compare(f, val) == Ordering::Less {
                return self.lower(f, t, val);
            }

            let delta = t - f;
            t = f;
            f -= delta << 1;
        }

        self.lower(from, t, val)
    }

    fn upper2(&mut self, from: i32, to: i32, val: i32) -> i32 {
        let mut f = from;
        let mut t = f + 1;
        while t < to {
            if self.compare(t, val) == Ordering::Greater {
                return self.upper(f, t, val);
            }

            let delta = t - f;
            f = t;
            t += delta << 1;
        }

        self.upper(f, to, val)
    }

    fn reverse(&mut self, from: i32, to: i32) {
        let mut to = to - 1;
        let mut from = from;
        while from < to {
            self.swap(from, to);
            from += 1;
            to -= 1;
        }
    }

    fn rotate(&mut self, low: i32, mid: i32, high: i32) {
        debug_assert!(low < mid && mid < high);
        if low == mid || mid == high {
            return;
        }

        if mid - low == high - mid {
            let mut mid = mid;
            let mut low = low;
            while mid < high {
                self.swap(low, mid);
                low += 1;
                mid += 1;
            }
        } else {
            self.reverse(low, mid);
            self.reverse(mid, high);
            self.reverse(low, high);
        }
    }

    fn binary_sort(&mut self, from: i32, to: i32) {
        let mut i = from + 1;
        while i < to {
            self.set_pivot(i);
            let mut l = from;
            let mut h = i - 1;
            while l <= h {
                let mid = (l + h).unsigned_shift(1);
                let cmp = self.compare_pivot(mid);
                if cmp == Ordering::Less {
                    h = mid - 1;
                } else {
                    l = mid + 1;
                }
            }

            let mut j = i;
            while j > l {
                self.swap(j - 1, j);
                j -= 1;
            }

            i += 1;
        }
    }

    fn heap_sort(&mut self, from: i32, to: i32) {
        if to - from <= 1 {
            return;
        }

        self.heapify(from, to);
        let mut end = to - 1;
        while end > from {
            self.swap(from, end);
            self.sift_down(from, from, end);
            end -= 1;
        }
    }

    fn heapify(&mut self, from: i32, to: i32) {
        let mut i = heap_parent(from, to - 1);
        while i >= from {
            self.sift_down(i, from, to);
            i -= 1;
        }
    }

    fn sift_down(&mut self, mut i: i32, from: i32, to: i32) {
        let mut left_child = heap_child(from, i);
        while left_child < to {
            let right_child = left_child + 1;
            if self.compare(i, left_child) == Ordering::Less {
                if right_child < to && self.compare(left_child, right_child) == Ordering::Less {
                    self.swap(i, right_child);
                    i = right_child;
                } else {
                    self.swap(i, left_child);
                    i = left_child;
                }
            } else if right_child < to && self.compare(i, right_child) == Ordering::Less {
                self.swap(i, right_child);
                i = right_child;
            } else {
                break;
            }
            left_child = heap_child(from, i);
        }
    }

    fn quick_sort(&mut self, from: i32, to: i32, max_depth: i32) {
        let max_depth = max_depth - 1;
        if to - from < BINARY_SORT_THRESHOLD as i32 {
            self.binary_sort(from, to);
            return;
        } else if max_depth < 0 {
            self.heap_sort(from, to);
            return;
        }

        let mid = (from + to).unsigned_shift(1);
        if self.compare(from, mid) == Ordering::Greater {
            self.swap(from, mid);
        }

        if self.compare(mid, to - 1) == Ordering::Greater {
            self.swap(mid, to - 1);
            if self.compare(from, mid) == Ordering::Greater {
                self.swap(from, mid);
            }
        }

        let mut left = from + 1;
        let mut right = to - 2;

        self.set_pivot(mid);
        loop {
            while self.compare_pivot(right) == Ordering::Less {
                right -= 1;
            }

            while left < right && self.compare_pivot(left) != Ordering::Less {
                left += 1;
            }

            if left < right {
                self.swap(left, right);
                right -= 1;
            } else {
                break;
            }
        }

        self.quick_sort(from, left + 1, max_depth);
        self.quick_sort(left + 1, to, max_depth);
    }
}

pub fn heap_parent(from: i32, i: i32) -> i32 {
    (i - 1 - from).unsigned_shift(1) + from
}

pub fn heap_child(from: i32, i: i32) -> i32 {
    ((i - from) << 1) + 1 + from
}

pub fn check_range(from: i32, to: i32) {
    assert!(from <= to);
}

pub trait MSBSorter {
    type Fallback: Sorter;
    fn byte_at(&self, i: i32, k: i32) -> Option<u8>;
    fn msb_swap(&mut self, i: i32, j: i32);
    fn fallback_sorter(&mut self, k: i32) -> Self::Fallback;
}

pub struct DefaultMSBIntroSorter {
    k: i32,
    max_length: i32,
    pivot: Vec<u8>,
    pivot_len: usize,
}

impl DefaultMSBIntroSorter {
    pub fn new(k: i32, max_length: i32) -> DefaultMSBIntroSorter {
        DefaultMSBIntroSorter {
            k,
            max_length,
            pivot: vec![0u8; max_length as usize + 1],
            pivot_len: 0,
        }
    }
}

impl MSBSorter for DefaultMSBIntroSorter {
    type Fallback = DefaultMSBIntroSorter;
    fn byte_at(&self, _i: i32, _k: i32) -> Option<u8> {
        unimplemented!()
    }
    fn msb_swap(&mut self, _i: i32, _j: i32) {
        unimplemented!()
    }
    fn fallback_sorter(&mut self, k: i32) -> Self::Fallback {
        DefaultMSBIntroSorter::new(k, self.max_length)
    }
}

impl Sorter for DefaultMSBIntroSorter {
    fn compare(&mut self, i: i32, j: i32) -> Ordering {
        for o in self.k..self.max_length {
            let b1 = self.byte_at(i, o);
            let b2 = self.byte_at(j, o);
            if b1 != b2 {
                return b1.cmp(&b2);
            } else if b1.is_none() {
                break;
            }
        }

        Ordering::Equal
    }

    fn swap(&mut self, i: i32, j: i32) {
        self.msb_swap(i, j)
    }

    fn sort(&mut self, from: i32, to: i32) {
        check_range(from, to);
        self.quick_sort(from, to, 2 * ((((to - from) as f64).log2()) as i32));
    }

    fn set_pivot(&mut self, i: i32) {
        self.pivot_len = 0;
        for o in self.k..self.max_length {
            if let Some(b) = self.byte_at(i, o) {
                self.pivot[self.pivot_len] = b as u8;
                self.pivot_len += 1;
            } else {
                break;
            }
        }
    }

    fn compare_pivot(&mut self, j: i32) -> Ordering {
        for o in 0..self.pivot_len {
            let b1 = self.pivot[o];
            if let Some(b2) = self.byte_at(j, self.k + o as i32) {
                if b1 != b2 {
                    return b1.cmp(&b2);
                }
            } else {
                return Ordering::Greater;
            }
        }

        if self.k + self.pivot_len as i32 == self.max_length {
            Ordering::Equal
        } else if self.byte_at(j, self.k + self.pivot_len as i32).is_some() {
            Ordering::Less
        } else {
            Ordering::Equal
        }
    }
}

// after that many levels of recursion we fall back to introsort anyway
// this is used as a protection against the fact that radix sort performs
// worse when there are long common prefixes (probably because of cache
// locality)
const LEVEL_THRESHOLD: usize = 8;
// size of histograms: 256 + 1 to indicate that the string is finished
const MSB_HISTOGRAM_SIZE: usize = 257;
// buckets below this size will be sorted with introsort
const LENGTH_THRESHOLD: usize = 100;

pub struct MSBRadixSorter<T: MSBSorter> {
    histograms: Vec<Vec<i32>>,
    end_offsets: Vec<i32>,
    common_prefix: Vec<i32>,
    max_length: i32,
    msb_sorter: T,
}

impl<T: MSBSorter> MSBRadixSorter<T> {
    pub fn new(max_length: i32, msb_sorter: T) -> MSBRadixSorter<T> {
        let mut histograms = Vec::with_capacity(LEVEL_THRESHOLD);
        for _ in 0..LEVEL_THRESHOLD {
            histograms.push(vec![0i32; MSB_HISTOGRAM_SIZE]);
        }

        let capacity = max_length.min(24);
        let mut common_prefix: Vec<i32> = Vec::with_capacity(capacity as usize);
        for _ in 0..capacity {
            common_prefix.push(0i32);
        }

        MSBRadixSorter {
            histograms,
            end_offsets: vec![0i32; MSB_HISTOGRAM_SIZE],
            common_prefix,
            max_length,
            msb_sorter,
        }
    }

    pub fn sort(&mut self, from: i32, to: i32) {
        check_range(from, to);
        self.sort_inner(from, to, 0, 0);
    }

    pub fn sort_inner(&mut self, from: i32, to: i32, k: i32, l: i32) {
        if to - from <= LENGTH_THRESHOLD as i32 || l >= LEVEL_THRESHOLD as i32 {
            let mut intro_sorter = self.msb_sorter.fallback_sorter(k);
            intro_sorter.sort(from, to);
        } else {
            self.radix_sort(from, to, k, l);
        }
    }

    // k the character number to compare
    // l the level of recursion
    pub fn radix_sort(&mut self, from: i32, to: i32, k: i32, l: i32) {
        self.histograms[l as usize] = vec![0i32; MSB_HISTOGRAM_SIZE];
        let common_prefix_length =
            self.compute_common_prefix_length_and_build_histogram(from, to, k, l);
        if common_prefix_length > 0 {
            // if there are no more chars to compare or if all entries fell into the
            // first bucket (which means strings are shorter than k) then we are done
            // otherwise recurse

            if k + common_prefix_length < self.max_length
                && self.histograms[l as usize][0] < to - from
            {
                self.radix_sort(from, to, k + common_prefix_length, l);
            }

            return;
        }

        debug_assert!(self.assert_histogram(common_prefix_length, l));
        self.sum_histogram(l);
        self.reorder(from, to, k, l);

        let end_offsets: Vec<i32> = self.histograms[l as usize].clone();
        if k + 1 < self.max_length {
            // recurse on all but the first bucket since all keys are equals in this
            // bucket (we already compared all bytes)
            let mut prev = end_offsets[0];
            let mut i = 1;
            while i < MSB_HISTOGRAM_SIZE {
                let h = end_offsets[i];
                let bucket_len = h - prev;
                if bucket_len > 1 {
                    self.sort_inner(from + prev, from + h, k + 1, l + 1);
                }

                prev = h;
                i += 1;
            }
        }
    }

    fn assert_histogram(&self, common_prefix_length: i32, l: i32) -> bool {
        let mut number_of_uniqe_bytes = 0;
        for freq in &self.histograms[l as usize] {
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
        let byte = self.msb_sorter.byte_at(i, k);
        if let Some(b) = byte {
            b as usize + 1
        } else {
            (-1 + 1) as usize
        }
    }

    fn compute_common_prefix_length_and_build_histogram(
        &mut self,
        from: i32,
        to: i32,
        k: i32,
        l: i32,
    ) -> i32 {
        let mut common_prefix_length = self.common_prefix.len().min((self.max_length - k) as usize);
        for j in 0..common_prefix_length {
            let b = self.msb_sorter.byte_at(from, k + j as i32);
            self.common_prefix[j] = b.map(|v| v as i32).unwrap_or(-1);
            if b.is_none() {
                common_prefix_length = j + 1;
                break;
            }
        }

        let mut index = from + 1;
        'outer: while index < to {
            for j in 0..common_prefix_length {
                let b = self
                    .msb_sorter
                    .byte_at(index, k + j as i32)
                    .map_or(-1, |v| v as i32);
                if b != self.common_prefix[j] {
                    common_prefix_length = j;
                    if common_prefix_length == 0 {
                        self.histograms[l as usize][(self.common_prefix[0] + 1) as usize] =
                            index - from;
                        self.histograms[l as usize][(b + 1) as usize] = 1;
                        break 'outer;
                    }
                    break;
                }
            }

            index += 1;
        }

        if index < to {
            debug_assert!(common_prefix_length == 0);
            self.build_histogram(index + 1, to, k, l);
        } else {
            debug_assert!(common_prefix_length > 0);
            self.histograms[l as usize][(self.common_prefix[0] + 1) as usize] = to - from;
        }

        common_prefix_length as i32
    }

    fn build_histogram(&mut self, from: i32, to: i32, k: i32, l: i32) {
        for i in from..to {
            let bucket = self.get_bucket(i, k);
            self.histograms[l as usize][bucket] += 1;
        }
    }

    fn sum_histogram(&mut self, l: i32) {
        let mut accum = 0;
        for i in 0..MSB_HISTOGRAM_SIZE {
            let count = self.histograms[l as usize][i];
            self.histograms[l as usize][i] = accum;
            accum += count;
            self.end_offsets[i] = accum;
        }
    }

    fn reorder(&mut self, from: i32, _to: i32, k: i32, l: i32) {
        for i in 0..MSB_HISTOGRAM_SIZE {
            let limit = self.end_offsets[i];
            let mut h1 = self.histograms[l as usize][i];
            while h1 < limit {
                let b = self.get_bucket(from + h1, k);
                let h2 = self.histograms[l as usize][b];
                self.histograms[l as usize][b] += 1;
                self.msb_sorter.msb_swap(from + h1, from + h2);
                h1 = self.histograms[l as usize][i];
            }
        }
    }
}
