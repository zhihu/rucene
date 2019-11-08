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

// TODO: copy from package `crossbeam-epoch` from it's not a public module
// we use this to manage callback functions

use std::mem::MaybeUninit;
use std::{fmt, mem, ptr};

/// Number of words a piece of `Data` can hold.
///
/// Three words should be enough for the majority of cases. For example, you can fit inside it the
/// function pointer together with a fat pointer representing an object that needs to be destroyed.
const DATA_WORDS: usize = 3;

/// Some space to keep a `FnOnce()` object on the stack.
type Data = [usize; DATA_WORDS];

/// A `FnOnce()` that is stored inline if small, or otherwise boxed on the heap.
///
/// This is a handy way of keeping an unsized `FnOnce()` within a sized structure.
pub struct Deferred {
    call: unsafe fn(*mut u8),
    data: MaybeUninit<Data>,
}

impl fmt::Debug for Deferred {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "Deferred {{ ... }}")
    }
}

impl Drop for Deferred {
    fn drop(&mut self) {
        unsafe {
            ptr::drop_in_place(self.data.as_mut_ptr());
        }
    }
}

impl Deferred {
    /// Constructs a new `Deferred` from a `FnOnce()`.
    #[allow(clippy::cast_ptr_alignment)]
    pub fn new<F: FnOnce() + 'static>(f: F) -> Self {
        let size = mem::size_of::<F>();
        let align = mem::align_of::<F>();

        unsafe {
            if size <= mem::size_of::<Data>() && align <= mem::align_of::<Data>() {
                let mut data = MaybeUninit::<Data>::uninit();
                ptr::write(data.as_mut_ptr() as *mut F, f);

                unsafe fn call<F: FnOnce()>(raw: *mut u8) {
                    let f: F = ptr::read(raw as *mut F);
                    f();
                }

                Deferred {
                    call: call::<F>,
                    data,
                }
            } else {
                let b: Box<F> = Box::new(f);
                let mut data = MaybeUninit::<Data>::uninit();
                ptr::write(data.as_mut_ptr() as *mut Box<F>, b);

                unsafe fn call<F: FnOnce()>(raw: *mut u8) {
                    let b: Box<F> = ptr::read(raw as *mut Box<F>);
                    (*b)();
                }

                Deferred {
                    call: call::<F>,
                    data,
                }
            }
        }
    }

    /// Calls the function.
    #[inline]
    pub fn call(mut self) {
        let call = self.call;
        unsafe { call(self.data.as_mut_ptr() as *mut u8) };
    }
}
