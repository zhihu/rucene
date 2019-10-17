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

use std::cmp::max;
use std::sync::Mutex;

use core::codec::Codec;
use core::search::query::{Weight, CONSTANT, MATCH_ALL, POINT_RANGE, TERM};

use error::Result;

/// A policy defining which filters should be cached.
///
/// Implementations of this class must be thread-safe.
pub trait QueryCachingPolicy<C: Codec>: Send + Sync {
    /// Callback that is called every time that a cached filter is used.
    ///  This is typically useful if the policy wants to track usage statistics
    ///  in order to make decisions.
    fn on_use(&self, weight: &dyn Weight<C>);

    /// Whether the given {@link Query} is worth caching.
    ///  This method will be called by the {@link QueryCache} to know whether to
    ///  cache. It will first attempt to load a {@link DocIdSet} from the cache.
    ///  If it is not cached yet and this method returns <tt>true</tt> then a
    ///  cache entry will be generated. Otherwise an uncached scorer will be
    ///  returned.
    fn should_cache(&self, weight: &dyn Weight<C>) -> Result<bool>;
}

pub struct AlwaysCacheQueryCachingPolicy;

impl Default for AlwaysCacheQueryCachingPolicy {
    fn default() -> Self {
        AlwaysCacheQueryCachingPolicy {}
    }
}

impl<C: Codec> QueryCachingPolicy<C> for AlwaysCacheQueryCachingPolicy {
    fn on_use(&self, _weight: &dyn Weight<C>) {}

    fn should_cache(&self, _weight: &dyn Weight<C>) -> Result<bool> {
        Ok(true)
    }
}

const SENTINEL: u32 = u32::max_value();

/// A `QueryCachingPolicy` that tracks usage statistics of recently-used
/// filters in order to decide on which filters are worth caching.
pub struct UsageTrackingQueryCachingPolicy {
    recently_used_filters: Mutex<FrequencyTrackingRingBuffer>,
}

impl UsageTrackingQueryCachingPolicy {
    pub fn new(history_size: usize) -> UsageTrackingQueryCachingPolicy {
        let recently_used_filters =
            Mutex::new(FrequencyTrackingRingBuffer::new(history_size, SENTINEL));
        UsageTrackingQueryCachingPolicy {
            recently_used_filters,
        }
    }

    fn is_costly<C: Codec>(w: &dyn Weight<C>) -> bool {
        // TODO currently only PointRangeQuery is costly
        w.actual_query_type() == POINT_RANGE
    }

    fn is_cheap<C: Codec>(w: &dyn Weight<C>) -> bool {
        w.actual_query_type() == TERM
    }

    fn cache_min_frequency<C: Codec>(&self, w: &dyn Weight<C>) -> u32 {
        if Self::is_costly(w) {
            2
        } else if Self::is_cheap(w) {
            20
        } else {
            5
        }
    }

    pub fn frequency<C: Codec>(&self, w: &dyn Weight<C>) -> u32 {
        debug_assert!(w.actual_query_type() != CONSTANT);
        let hash_code = w.hash_code();
        if let Ok(recently_used_filters) = self.recently_used_filters.lock() {
            recently_used_filters.frequency(hash_code)
        } else {
            0
        }
    }
}

impl<C: Codec> QueryCachingPolicy<C> for UsageTrackingQueryCachingPolicy {
    fn on_use(&self, weight: &dyn Weight<C>) {
        debug_assert!(weight.query_type() != CONSTANT);
        let hash_code = weight.hash_code();
        if let Ok(mut recently_used_filters) = self.recently_used_filters.lock() {
            recently_used_filters.add(hash_code);
        }
    }

    fn should_cache(&self, weight: &dyn Weight<C>) -> Result<bool> {
        if weight.actual_query_type() == MATCH_ALL {
            return Ok(false);
        }

        let frequency = self.frequency(weight);
        let min_frequency = self.cache_min_frequency(weight);

        Ok(frequency >= min_frequency)
    }
}

impl Default for UsageTrackingQueryCachingPolicy {
    fn default() -> Self {
        UsageTrackingQueryCachingPolicy::new(1024)
    }
}

/// A ring buffer that tracks the frequency of the integers that it contains.
/// This is typically useful to track the hash codes of popular recently-used
/// items.
///
/// This data-structure requires 22 bytes per entry on average (between 16 and
/// 28).
pub struct FrequencyTrackingRingBuffer {
    max_size: usize,
    buffer: Vec<u32>,
    position: usize,
    frequencies: IntBag,
}

impl FrequencyTrackingRingBuffer {
    /// Create a new ring buffer that will contain at most `maxSize` items.
    /// This buffer will initially contain `maxSize` times the
    /// `sentinel` value.
    pub fn new(max_size: usize, sentinel: u32) -> FrequencyTrackingRingBuffer {
        assert!(max_size >= 2, "maxSize must be at least 2");
        let buffer = vec![sentinel; max_size];
        let mut frequencies = IntBag::new(max_size);
        for _ in 0..max_size {
            frequencies.add(sentinel);
        }
        debug_assert!(frequencies.frequency(sentinel) == max_size as u32);
        FrequencyTrackingRingBuffer {
            max_size,
            buffer,
            position: 0,
            frequencies,
        }
    }

    /// Add a new item to this ring buffer, potentially removing the oldest
    /// entry from this buffer if it is already full.
    pub fn add(&mut self, i: u32) {
        // remove the previous value
        let removed = self.buffer[self.position];
        let removed_from_bag = self.frequencies.remove(removed);
        debug_assert!(removed_from_bag);
        // add the new value
        self.buffer[self.position] = i;
        self.frequencies.add(i);
        // increment the position
        self.position = (self.position + 1) % self.max_size;
    }

    /// returns the frequency of the provided key in the ring buffer.
    pub fn frequency(&self, key: u32) -> u32 {
        self.frequencies.frequency(key)
    }
}

/// A bag of integers.
/// Since in the context of the ring buffer the maximum size is known up-front
/// there is no need to worry about resizing the underlying storage.
struct IntBag {
    keys: Vec<u32>,
    freqs: Vec<u32>,
    mask: usize,
}

impl IntBag {
    pub fn new(max_size: usize) -> IntBag {
        // load factor of 2/3 and round up to the next power of two
        let capacity = max(2, max_size * 3 / 2).next_power_of_two();
        debug_assert!(capacity > max_size);
        let keys = vec![0u32; capacity];
        let freqs = vec![0u32; capacity];
        let mask = capacity - 1;

        IntBag { keys, freqs, mask }
    }

    /// Return the frequency of the give key in the bag.
    fn frequency(&self, key: u32) -> u32 {
        let mut slot = key as usize & self.mask;
        loop {
            if self.keys[slot] == key {
                return self.freqs[slot];
            } else if self.freqs[slot] == 0 {
                return 0;
            }
            slot = (slot + 1) & self.mask
        }
    }

    /// Increment the frequency of the given key by 1 and return its new frequency.
    fn add(&mut self, key: u32) -> u32 {
        let mut slot = key as usize & self.mask;
        loop {
            if self.freqs[slot] == 0 {
                self.keys[slot] = key;
                self.freqs[slot] = 1;
                return 1;
            } else if self.keys[slot] == key {
                self.freqs[slot] += 1;
                return self.freqs[slot];
            }
            slot = (slot + 1) & self.mask
        }
    }

    /// Decrement the frequency of the given key by one, or do nothing if the
    /// key is not present in the bag. Returns true iff the key was contained
    /// in the bag.
    fn remove(&mut self, key: u32) -> bool {
        let mut slot = key as usize & self.mask;
        loop {
            if self.freqs[slot] == 0 {
                return false;
            } else if self.keys[slot] == key {
                self.freqs[slot] -= 1;
                if self.freqs[slot] == 0 {
                    // removed
                    self.relocate_adjacent_keys(slot);
                }
                return true;
            }
            slot = (slot + 1) & self.mask
        }
    }

    fn relocate_adjacent_keys(&mut self, free_slot: usize) {
        let mut free_slot = free_slot;
        let mut slot = (free_slot + 1) & self.mask;
        loop {
            let freq = self.freqs[slot];
            if freq == 0 {
                // end of the collision chain, we're done
                break;
            }
            let key = self.keys[slot];
            // the slot where `key` should be if there were no collisions
            let expected_slot = key as usize & self.mask;
            // if the free slot is between the expected slot and the slot where the
            // key is, then we can relocate there
            if Self::between(expected_slot, slot, free_slot) {
                self.keys[free_slot] = key;
                self.freqs[free_slot] = freq;
                // slot is the new free slot
                self.freqs[slot] = 0;
                free_slot = slot;
            }
            slot = (slot + 1) & self.mask;
        }
    }

    /// Given a chain of occupied slots between <code>chainStart</code>
    /// and <code>chainEnd</code>, return whether <code>slot</code> is
    /// between the start and end of the chain.
    fn between(chain_start: usize, chain_end: usize, slot: usize) -> bool {
        if chain_start <= chain_end {
            chain_start <= slot && slot <= chain_end
        } else {
            // the chain is across the end of the array
            slot >= chain_start || slot <= chain_end
        }
    }
}
