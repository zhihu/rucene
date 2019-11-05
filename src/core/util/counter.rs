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

use std::sync::atomic::{AtomicI64, Ordering};

/// Simple counter trait
pub trait Count {
    fn add_get(&mut self, delta: i64) -> i64;

    fn get(&self) -> i64;
}

struct SerialCounter {
    count: i64,
}

impl Count for SerialCounter {
    fn add_get(&mut self, delta: i64) -> i64 {
        self.count += delta;
        self.count
    }

    fn get(&self) -> i64 {
        self.count
    }
}

struct AtomicCounter {
    count: AtomicI64,
}

impl Count for AtomicCounter {
    fn add_get(&mut self, delta: i64) -> i64 {
        self.count.fetch_add(delta, Ordering::Release);
        self.get()
    }

    fn get(&self) -> i64 {
        self.count.load(Ordering::Acquire)
    }
}

enum CounterEnum {
    Serial(Box<SerialCounter>),
    Atomic(Box<AtomicCounter>),
    Borrowed(*mut dyn Count),
    // TODO unsafe use for borrow a exist counter
}

impl Count for CounterEnum {
    fn add_get(&mut self, delta: i64) -> i64 {
        match *self {
            CounterEnum::Serial(ref mut s) => s.add_get(delta),
            CounterEnum::Atomic(ref mut s) => s.add_get(delta),
            CounterEnum::Borrowed(b) => unsafe { (*b).add_get(delta) },
        }
    }

    fn get(&self) -> i64 {
        match *self {
            CounterEnum::Serial(ref s) => s.get(),
            CounterEnum::Atomic(ref s) => s.get(),
            CounterEnum::Borrowed(b) => unsafe { (*b).get() },
        }
    }
}

pub struct Counter {
    count: CounterEnum,
}

impl Default for Counter {
    fn default() -> Self {
        Self::new(false)
    }
}

impl Counter {
    pub fn new(thread_safe: bool) -> Self {
        let count = if thread_safe {
            CounterEnum::Atomic(Box::new(AtomicCounter {
                count: AtomicI64::new(0),
            }))
        } else {
            CounterEnum::Serial(Box::new(SerialCounter { count: 0 }))
        };
        Counter { count }
    }

    pub fn borrow(counter: &dyn Count) -> Self {
        Counter {
            count: CounterEnum::Borrowed(counter as *const dyn Count as *mut dyn Count),
        }
    }

    fn borrow_raw(counter: *mut dyn Count) -> Self {
        Counter {
            count: CounterEnum::Borrowed(counter),
        }
    }

    // TODO this copy while share the inner count of self,
    // so it is not safe if self's lifetime is shorter than the copy one
    pub unsafe fn shallow_copy(&self) -> Counter {
        match self.count {
            CounterEnum::Borrowed(b) => Counter::borrow_raw(b),
            CounterEnum::Atomic(ref a) => Counter::borrow(a.as_ref() as &dyn Count),
            CounterEnum::Serial(ref s) => Counter::borrow(s.as_ref() as &dyn Count),
        }
    }

    pub fn ptr(&self) -> *const dyn Count {
        match self.count {
            CounterEnum::Serial(ref s) => s.as_ref(),
            CounterEnum::Atomic(ref s) => s.as_ref(),
            CounterEnum::Borrowed(b) => b,
        }
    }
}

impl Count for Counter {
    fn add_get(&mut self, delta: i64) -> i64 {
        self.count.add_get(delta)
    }

    fn get(&self) -> i64 {
        self.count.get()
    }
}
