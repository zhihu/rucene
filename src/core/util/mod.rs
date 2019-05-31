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

pub mod numeric;

mod variant_value;
pub use self::variant_value::*;

mod long_values;
pub use self::long_values::*;

pub mod packed;
pub use self::packed::packed_misc;

mod bits;
pub use self::bits::*;

mod sparse_bits;
pub use self::sparse_bits::*;

mod version;
pub use self::version::*;

mod paged_bytes;
pub use self::paged_bytes::*;

mod doc_id_set_builder;
pub use self::doc_id_set_builder::*;

mod compute_time;
pub use self::compute_time::*;

mod context;
pub use self::context::*;

mod counter;
pub use self::counter::*;

mod volatile;
pub use self::volatile::Volatile;

mod reference_manager;
pub use self::reference_manager::*;

mod byte_ref;
pub use self::byte_ref::*;

pub mod array;
pub mod binary_heap;
pub mod bit_set;
pub mod bit_util;
pub mod bkd;
pub mod byte_block_pool;
pub mod bytes_ref_hash;
pub mod doc_id_set;
pub mod external;
pub mod fst;
pub mod int_block_pool;
pub mod ints_ref;
pub mod io;
pub mod math;
pub mod offline_sorter;
pub mod selector;
pub mod small_float;
pub mod sorter;
pub mod string_util;
pub mod thread_pool;

use std::ops::Deref;

// a iterator that can be used over and over by call reset
pub trait ReusableIterator: Iterator {
    fn reset(&mut self);
}

pub fn fill_slice<T: Copy>(array: &mut [T], value: T) {
    for i in array {
        *i = value;
    }
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
