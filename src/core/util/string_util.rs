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

use rand::{thread_rng, Rng};

/// length in bytes of an ID
pub const ID_LENGTH: usize = 16;

/// Generates a non-cryptographic globally unique id.
pub fn random_id() -> [u8; ID_LENGTH] {
    let mut id = [0u8; ID_LENGTH];
    thread_rng().fill(&mut id);
    id
}

pub fn id2str(id: &[u8]) -> String {
    let strs: Vec<String> = id.iter().map(|b| format!("{:02X}", b)).collect();
    strs.join("")
}

pub fn bytes_subtract(bytes_per_dim: usize, dim: usize, a: &[u8], b: &[u8], result: &mut [u8]) {
    let start = dim * bytes_per_dim;
    let end = start + bytes_per_dim;
    let mut borrow = 0;
    let mut i = end - 1;
    while i >= start {
        let mut diff: i32 = (a[i] as u32 as i32) - (b[i] as u32 as i32) - borrow;
        if diff < 0 {
            diff += 256;
            borrow = 1;
        } else {
            borrow = 0;
        }

        result[i - start] = diff as u8;
        i -= 1;
    }

    if borrow != 0 {
        panic!("a<b")
    }
}

/// Compares two {@link BytesRef}, element by element, and returns the
/// number of elements common to both arrays (from the start of each).
pub fn bytes_difference(left: &[u8], right: &[u8]) -> i32 {
    let len = left.len().min(right.len());
    for i in 0..len {
        if left[i] != right[i] {
            return i as i32;
        }
    }

    len as i32
}

/// Returns the length of {@code currentTerm} needed for use as a sort key.
/// so that {@link BytesRef#compareTo(BytesRef)} still returns the same result.
/// This method assumes currentTerm comes after priorTerm.
pub fn sort_key_length(prior_term: &[u8], current_term: &[u8]) -> usize {
    let current_term_offset = 0usize;
    let prior_term_offset = 0usize;
    let limit = prior_term.len().min(current_term.len());

    for i in 0..limit {
        if prior_term[prior_term_offset + i] != current_term[current_term_offset + i] {
            return i + 1;
        }
    }

    current_term.len().min(1 + prior_term.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_id2str() {
        let v = vec![65u8, 97u8, 4u8, 127u8];
        let strv = id2str(&v[..]);
        assert_eq!("4161047F", strv);
    }
}
