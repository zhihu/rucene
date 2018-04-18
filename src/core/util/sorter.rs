use core::util::bit_util::UnsignedShift;

const INSERTION_SORT_THRESHOLD: usize = 30;
const HISTOGRAM_SIZE: usize = 256;

/// A LSB Radix sorter for unsigned int values.
pub struct LSBRadixSorter {
    histogram: [i32; HISTOGRAM_SIZE],
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
        for count in histogram.iter_mut().take(HISTOGRAM_SIZE) {
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
        let histogram = [0i32; HISTOGRAM_SIZE];
        LSBRadixSorter { histogram }
    }
}
