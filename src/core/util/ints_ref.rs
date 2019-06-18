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

use error::{ErrorKind, Result};
use std::cmp::{min, Ord, Ordering, PartialOrd};
use std::hash::{Hash, Hasher};

use core::util::bit_util::UnsignedShift;
// const EMPTY_INTS: [i32; 0] = [0i32; 0];

/// represents &[i32], as a slice (offset + length) into an
/// existing Vec<i32>.
#[derive(Clone)]
pub struct IntsRef {
    ints: *const [i32],
    pub offset: usize,
    pub length: usize,
}

impl IntsRef {
    pub fn new(ints: &[i32], offset: usize, length: usize) -> Self {
        let res = IntsRef {
            ints: ints as *const [i32],
            offset,
            length,
        };
        assert!(res.is_valid().is_ok());
        res
    }

    pub fn ints(&self) -> &[i32] {
        unsafe { &(*self.ints) }
    }

    fn is_valid(&self) -> Result<()> {
        let ints = self.ints();
        if self.length > self.ints().len() {
            bail!(ErrorKind::IllegalState(format!(
                "length is out of bounds: {}",
                self.length
            )));
        }
        if self.offset > ints.len() {
            bail!(ErrorKind::IllegalState(format!(
                "offset out of bounds: {}",
                self.offset
            )));
        }
        if self.offset + self.length > ints.len() {
            bail!(ErrorKind::IllegalState(format!(
                "offset+length out of bounds: offset={}, length:{}, bounds: {}",
                self.offset,
                self.length,
                ints.len()
            )));
        }
        Ok(())
    }

    //    pub fn deep_copy_of(other: &IntsRef) -> IntsRef {
    //
    //    }
}

impl Hash for IntsRef {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let ints = self.ints();
        for i in self.offset..self.offset + self.length {
            state.write_i32(ints[i])
        }
    }
}

impl PartialEq for IntsRef {
    fn eq(&self, other: &IntsRef) -> bool {
        if self.length == other.length {
            let ints1 = &self.ints()[self.offset..self.offset + self.length];
            let ints2 = &other.ints()[other.offset..other.offset + other.length];
            for i in 0..self.length {
                if ints1[i] != ints2[i] {
                    return false;
                }
            }
            true
        } else {
            false
        }
    }
}

impl Eq for IntsRef {}

impl PartialOrd for IntsRef {
    fn partial_cmp(&self, other: &IntsRef) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IntsRef {
    fn cmp(&self, other: &Self) -> Ordering {
        let length = min(self.length, other.length);

        let ints1 = &self.ints()[self.offset..self.offset + length];
        let ints2 = &other.ints()[other.offset..other.offset + length];
        for i in 0..length {
            let c = ints1[i].cmp(&ints2[i]);
            if c != Ordering::Equal {
                return c;
            }
        }
        (self.length - other.length).cmp(&0)
    }
}

#[derive(Default)]
pub struct IntsRefBuilder {
    ints: Vec<i32>,
    pub offset: usize,
    pub length: usize,
}

impl IntsRefBuilder {
    pub fn new() -> IntsRefBuilder {
        Default::default()
    }

    pub fn ints(&self) -> &[i32] {
        &self.ints
    }

    pub fn set_length(&mut self, length: usize) {
        self.length = length;
    }

    pub fn clear(&mut self) {
        self.set_length(0);
    }

    pub fn int_at(&self, offset: usize) -> i32 {
        self.ints[offset]
    }

    pub fn set_int(&mut self, offset: usize, value: i32) {
        self.ints[offset] = value;
    }

    pub fn append(&mut self, i: i32) {
        let new_len = self.length + 1;
        self.grow(new_len);
        self.ints[self.length] = i;
        self.length += 1;
    }

    pub fn grow(&mut self, new_length: usize) {
        if self.ints.len() < new_length {
            self.ints.resize(new_length, 0);
        }
    }

    pub fn copy_ints(&mut self, other: &[i32], offset: usize, length: usize) {
        self.grow(length);
        self.ints[0..length].copy_from_slice(&other[offset..offset + length]);
        self.length = length;
    }

    pub fn copy_ints_ref(&mut self, ints: &IntsRef) {
        self.copy_ints(ints.ints(), ints.offset, ints.length)
    }

    pub fn get(&self) -> IntsRef {
        debug_assert_eq!(self.offset, 0);
        IntsRef {
            ints: self.ints.as_ref() as *const [i32],
            offset: 0,
            length: self.length,
        }
    }
}

pub fn to_ints_ref(input: &[u8], scratch: &mut IntsRefBuilder) -> IntsRef {
    scratch.clear();
    for b in input {
        scratch.append(*b as u32 as i32);
    }
    scratch.get()
}

pub struct LongsPtr {
    pub longs: *mut Vec<i64>,
    pub offset: usize,
    pub length: usize,
}

impl LongsPtr {
    pub fn new(longs: &mut Vec<i64>, offset: usize, length: usize) -> LongsPtr {
        LongsPtr {
            longs: longs as *mut Vec<i64>,
            offset,
            length,
        }
    }

    #[allow(clippy::mut_from_ref)]
    pub fn longs(&self) -> &mut Vec<i64> {
        unsafe { &mut (*self.longs) }
    }

    pub fn hash_code(&self) -> i32 {
        let prime = 31;
        let mut result = 0;
        let end = self.offset + self.length;
        for i in self.offset..end {
            result = prime * result + (self.longs()[i] ^ self.longs()[i].unsigned_shift(32)) as i32;
        }

        result
    }

    pub fn cmp_to(&self, other: &LongsPtr) -> Ordering {
        let mut a_upto = self.offset;
        let mut b_upto = other.offset;
        let a_stop = a_upto + self.length.min(other.length);

        while a_upto < a_stop {
            let a = self.longs()[a_upto];
            let b = other.longs()[b_upto];

            if a > b {
                return Ordering::Greater;
            } else if a < b {
                return Ordering::Less;
            }

            a_upto += 1;
            b_upto += 1;
        }

        // One is a prefix of the other, or, they are equal:
        if self.length < other.length {
            Ordering::Less
        } else if self.length > other.length {
            Ordering::Greater
        } else {
            Ordering::Equal
        }
    }
}
