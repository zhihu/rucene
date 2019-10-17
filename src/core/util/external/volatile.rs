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

// The MIT License (MIT)
//
// Copyright (c) Philipp Oppermann <dev@phil-opp.com>
//
// Permission is hereby granted, free of charge, to any
// person obtaining a copy of this software and associated
// documentation files (the "Software"), to deal in the
// Software without restriction, including without
// limitation the rights to use, copy, modify, merge,
// publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software
// is furnished to do so

// copy from https://github.com/embed-rs/volatile/blob/master/src/lib.rs

//! Provides wrapper types `Volatile`, `ReadOnly`, `WriteOnly`, `ReadWrite`, which wrap any
//! copy-able type and allows for volatile memory access to wrapped value. Volatile memory accesses
//! are never optimized away by the compiler, and are useful in many low-level systems programming
//! and concurrent contexts.
//!
//! The wrapper types *do not* enforce any atomicity guarantees; to also get atomicity, consider
//! looking at the `Atomic` wrapper type found in `libcore` or `libstd`.
//!
//! These wrappers do not depend on the standard library and never panic.
//!
//! # Dealing with Volatile Pointers
//!
//! Frequently, one may have to deal with volatile pointers, eg, writes to specific memory
//! locations. The canonical way to solve this is to cast the pointer to a volatile wrapper
//! directly, eg:
//!
//! ```rust
//! use rucene::core::util::external::Volatile;
//!
//! let mut_ptr = 0xFEE00000 as *mut u32;
//!
//! let volatile_ptr = mut_ptr as *mut Volatile<u32>;
//! ```
//!
//! and then perform operations on the pointer as usual in a volatile way. This method works as all
//! of the volatile wrapper types are the same size as their contained values.

use std::ptr;

/// A wrapper type around a volatile variable, which allows for volatile reads and writes
/// to the contained value. The stored type needs to be `Copy`, as volatile reads and writes
/// take and return copies of the value.
///
/// The size of this struct is the same as the size of the contained type.
#[derive(Debug)]
#[repr(transparent)]
pub struct Volatile<T: Copy>(T);

impl<T: Copy> Volatile<T> {
    /// Construct a new volatile instance wrapping the given value.
    ///
    /// This method never panics.
    #[cfg(feature = "const_fn")]
    pub const fn new(value: T) -> Volatile<T> {
        Volatile(value)
    }

    /// Construct a new volatile instance wrapping the given value.
    ///
    /// This method never panics.
    #[cfg(not(feature = "const_fn"))]
    pub fn new(value: T) -> Volatile<T> {
        Volatile(value)
    }

    /// Performs a volatile read of the contained value, returning a copy
    /// of the read value. Volatile reads are guaranteed not to be optimized
    /// away by the compiler, but by themselves do not have atomic ordering
    /// guarantees. To also get atomicity, consider looking at the `Atomic` wrapper type.
    ///
    /// This method never panics.
    pub fn read(&self) -> T {
        // UNSAFE: Safe, as we know that our internal value exists.
        unsafe { ptr::read_volatile(&self.0) }
    }

    /// Performs a volatile write, setting the contained value to the given value `value`. Volatile
    /// writes are guaranteed to not be optimized away by the compiler, but by themselves do not
    /// have atomic ordering guarantees. To also get atomicity, consider looking at the `Atomic`
    /// wrapper type.
    ///
    /// This method never panics.
    ///
    /// TODO, we force convert immutable reference to mutable pointer, because
    /// we needn't guarantee the race condition if multi-write at the same time,
    /// else we need to use Atomic instead
    pub fn write(&self, value: T) {
        // UNSAFE: Safe, as we know that our internal value exists.
        unsafe { ptr::write_volatile(&self.0 as *const T as *mut T, value) };
    }

    /// Performs a volatile read of the contained value, passes a mutable reference to it to the
    /// function `f`, and then performs a volatile write of the (potentially updated) value back to
    /// the contained value.
    ///
    /// Ths method never panics.
    pub fn update<F>(&self, f: F)
    where
        F: FnOnce(&mut T),
    {
        let mut value = self.read();
        f(&mut value);
        self.write(value);
    }
}

impl<T: Copy> Clone for Volatile<T> {
    fn clone(&self) -> Self {
        Volatile(self.read())
    }
}
