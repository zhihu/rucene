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
        self.count.fetch_add(delta, Ordering::AcqRel);
        self.get()
    }

    fn get(&self) -> i64 {
        self.count.load(Ordering::Acquire)
    }
}

enum CounterEnum {
    Serial(SerialCounter),
    Atomic(AtomicCounter),
    Borrowed(*mut Count),
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
            CounterEnum::Atomic(AtomicCounter {
                count: AtomicI64::new(0),
            })
        } else {
            CounterEnum::Serial(SerialCounter { count: 0 })
        };
        Counter { count }
    }

    pub fn borrow(counter: &Count) -> Self {
        Counter {
            count: CounterEnum::Borrowed(counter as *const Count as *mut Count),
        }
    }

    fn borrow_raw(counter: *mut Count) -> Self {
        Counter {
            count: CounterEnum::Borrowed(counter),
        }
    }

    // TODO this copy while share the inner count of self,
    // so it is not safe if self's lifetime is shorter than the copy one
    pub unsafe fn shallow_copy(&self) -> Counter {
        if let CounterEnum::Borrowed(b) = self.count {
            Counter::borrow_raw(b)
        } else {
            Counter::borrow(&self.count)
        }
    }

    pub fn ptr(&self) -> *const Count {
        match self.count {
            CounterEnum::Serial(ref _s) => self,
            CounterEnum::Atomic(ref _s) => self,
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
