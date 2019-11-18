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

pub type DocId = i32;

pub mod bkd;
pub mod external;
pub mod fst;
pub mod packed;

mod numeric;

pub use self::numeric::*;

mod variant_value;

pub use self::variant_value::*;

mod bits;

pub use self::bits::*;

mod version;

pub use self::version::*;

mod paged_bytes;

pub use self::paged_bytes::*;

mod doc_id_set_builder;

pub use self::doc_id_set_builder::*;

mod context;

pub use self::context::*;

mod counter;

pub use self::counter::*;

mod bytes_ref;

pub use self::bytes_ref::*;

mod bit_set;

pub use self::bit_set::*;

mod bit_util;

pub use self::bit_util::*;

mod byte_block_pool;

pub use self::byte_block_pool::*;

mod byte_slice_reader;

pub use self::byte_slice_reader::*;

mod bytes_ref_hash;

pub use self::bytes_ref_hash::*;

mod doc_id_set;

pub use self::doc_id_set::*;

mod int_block_pool;

pub use self::int_block_pool::*;

mod ints_ref;

pub use self::ints_ref::*;

mod math;

pub use self::math::*;

mod selector;

pub use self::selector::*;

mod small_float;

pub use self::small_float::*;

mod sorter;

pub use self::sorter::*;

mod string_util;

pub use self::string_util::*;

mod compression;

pub use self::compression::*;

mod disi;

pub use self::disi::*;

use std::ops::Deref;

use core::codec::doc_values::NumericDocValues;

use error::Result;

// a iterator that can be used over and over by call reset
pub trait ReusableIterator: Iterator {
    fn reset(&mut self);
}

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

pub const BM25_SIMILARITY_IDF: &str = "idf";

pub struct DerefWrapper<T>(pub T);

impl<T> Deref for DerefWrapper<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Abstraction over an array of longs.
///
/// This class extends `NumericDocValues` so that we don't need to add another
/// level of abstraction every time we want eg. to use the `PackedInts`
/// utility classes to represent a `NumericDocValues` instance.
pub trait LongValues: NumericDocValues {
    fn get64(&self, index: i64) -> Result<i64>;

    fn get64_mut(&mut self, index: i64) -> Result<i64> {
        self.get64(index)
    }
}

pub trait CloneableLongValues: LongValues {
    fn cloned(&self) -> Box<dyn CloneableLongValues>;

    fn cloned_lv(&self) -> Box<dyn LongValues>;
}

impl<T: LongValues + Clone + 'static> CloneableLongValues for T {
    fn cloned(&self) -> Box<dyn CloneableLongValues> {
        Box::new(self.clone())
    }

    fn cloned_lv(&self) -> Box<dyn LongValues> {
        Box::new(self.clone())
    }
}
