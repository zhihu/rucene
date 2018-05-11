#![recursion_limit = "1024"]
#![cfg_attr(feature = "clippy", feature(plugin))]
#![cfg_attr(feature = "clippy", plugin(clippy))]
#![cfg_attr(not(feature = "clippy"), allow(unknown_lints))]
#![feature(const_max_value, vec_resize_default, option_filter, exact_size_is_empty)]
#![feature(hash_map_remove_entry)]
#![feature(fnbox)]

#[macro_use]
extern crate error_chain;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;

extern crate byteorder;
extern crate bytes;
extern crate crc;
extern crate crossbeam_channel;
extern crate flate2;
extern crate memmap;
extern crate num_traits;
extern crate thread_local;

pub mod core;
pub mod error;
