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

use core::util::packed::Reader;

pub struct PackedIntsNullReader {
    value_count: usize,
}

impl PackedIntsNullReader {
    pub fn new(value_count: usize) -> PackedIntsNullReader {
        PackedIntsNullReader { value_count }
    }
}

impl Reader for PackedIntsNullReader {
    fn get(&self, _doc_id: usize) -> i64 {
        0
    }

    // FIXME: usize-> docId
    fn bulk_get(&self, index: usize, output: &mut [i64], len: usize) -> usize {
        assert!(index < self.value_count);
        let len = ::std::cmp::min(len, self.value_count - index);
        unsafe {
            let slice = output.as_mut_ptr();
            ::std::ptr::write_bytes(slice, 0, len);
        }
        len
    }
    fn size(&self) -> usize {
        self.value_count
    }
}
