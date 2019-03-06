pub type DocId = i32;

pub mod numeric;

pub use self::numeric::*;

mod variant_value;

pub use self::variant_value::*;

mod long_values;

pub use self::long_values::*;

pub mod packed;

pub use self::packed::packed_misc;

pub mod bits;

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

pub mod context;

pub use self::context::*;

mod counter;
pub use self::counter::*;

mod volatile;
pub use self::volatile::Volatile;

mod reference_manager;
pub use self::reference_manager::*;

pub mod array;
pub mod binary_heap;
pub mod bit_set;
pub mod bit_util;
pub mod bkd;
pub mod byte_block_pool;
pub mod byte_ref;
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

// a iterator that can be used over and over by call reset
pub trait ReusableIterator: Iterator {
    fn reset(&mut self);
}

pub fn fill_slice<T: Copy>(array: &mut [T], value: T) {
    for i in array {
        *i = value;
    }
}

// true if p1/p2 is the same reference
pub fn ptr_eq<T: ?Sized>(p1: &T, p2: &T) -> bool {
    p1 as *const T == p2 as *const T
}

pub const BM25_SIMILARITY_IDF: &str = "idf";
