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

pub fn fill_slice<T: Copy>(array: &mut [T], value: T) {
    for i in array {
        *i = value;
    }
}

pub fn over_size(size: usize) -> usize {
    let mut size = size;
    let mut extra = size >> 3;
    if extra < 3 {
        // for very small arrays, where constant overhead of
        // realloc is presumably relatively high, we grow
        // faster
        extra = 3;
    }
    size += extra;
    size
}
