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

#![recursion_limit = "1024"]
#![cfg_attr(feature = "clippy", feature(plugin))]
#![cfg_attr(feature = "clippy", plugin(clippy))]
#![cfg_attr(not(feature = "clippy"), allow(unknown_lints))]
#![feature(exact_size_is_empty)]
#![feature(drain_filter)]
#![feature(hashmap_internals)]
#![feature(integer_atomics)]
#![feature(vec_remove_item)]
#![feature(specialization)]
#![allow(clippy::cast_lossless)]
#![feature(fn_traits)]
#![feature(maybe_uninit_ref)]
#![feature(maybe_uninit_extra)]
#![feature(in_band_lifetimes)]
#![feature(vec_into_raw_parts)]
#![feature(core_intrinsics)]
#![feature(stmt_expr_attributes)]

#[macro_use]
extern crate error_chain;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
extern crate rand;
extern crate regex;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;

extern crate alloc;
extern crate byteorder;
extern crate bytes;
extern crate crc;
extern crate crossbeam;
extern crate fasthash;
extern crate flate2;
extern crate memmap;
extern crate num_cpus;
extern crate num_traits;
extern crate smallvec;
extern crate thread_local;
extern crate unicode_reader;
#[macro_use]
extern crate crunchy;

pub mod core;
pub mod error;
