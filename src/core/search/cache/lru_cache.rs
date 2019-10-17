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
// Copyright (c) 2016 Christian W. Briones
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

//! A fixed-size cache with LRU expiration criteria.
//!
use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;

pub struct CacheEntry<K, V> {
    key: K,
    value: Option<V>,
    next: Option<usize>,
    prev: Option<usize>,
}

/// A fixed-size cache.
pub struct LRUCache<K, V> {
    table: HashMap<K, usize>,
    entries: Vec<CacheEntry<K, V>>,
    free_indexes: Vec<usize>,
    first: Option<usize>,
    last: Option<usize>,
    capacity: usize,
}

impl<K: Clone + Hash + Eq, V> LRUCache<K, V> {
    /// Creates a new cache that can hold the specified number of elements.
    pub fn with_capacity(cap: usize) -> Self {
        LRUCache {
            table: HashMap::with_capacity(cap),
            entries: Vec::with_capacity(cap),
            free_indexes: Vec::with_capacity(cap),
            first: None,
            last: None,
            capacity: cap,
        }
    }

    /// Inserts a key-value pair into the cache and returns the previous value, if any.
    ///
    /// If there is no room in the cache the oldest item will be removed.
    ///
    /// ```
    /// use rucene::core::search::cache::LRUCache;
    ///
    /// let mut cache = LRUCache::with_capacity(2);
    /// assert_eq!(cache.insert("foo", 1), None);
    /// assert_eq!(cache.insert("foo", 2), Some(1));
    /// cache.insert("bar", 1);
    /// cache.insert("baz", 2);
    ///
    /// assert!(cache.contains_key(&"baz"));
    /// assert!(cache.contains_key(&"bar"));
    /// assert!(!cache.contains_key(&"foo"));
    /// ```
    #[allow(clippy::map_entry)]
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        if self.table.contains_key(&key) {
            self.access(&key);
            let entry = &mut self.entries[self.first.unwrap()];
            let old = entry.value.take();
            entry.value = Some(value);
            old
        } else {
            self.ensure_room();
            // Update old head
            let idx = self
                .free_indexes
                .pop()
                .unwrap_or_else(|| self.entries.len());
            if let Some(ent) = self.first {
                let prev = Some(idx);
                self.entries[ent].prev = prev;
            };

            // This is the new head
            let ent = CacheEntry {
                key: key.clone(),
                value: Some(value),
                next: self.first,
                prev: None,
            };
            if idx == self.entries.len() {
                self.entries.push(ent);
            } else {
                self.entries[idx] = ent;
            }

            self.add_head_idx(idx);
            self.table.insert(key, idx);
            None
        }
    }

    /// Removes the item associated with `key` from the cache and returns its value, if any.
    ///
    /// # Example
    /// ```
    /// use rucene::core::search::cache::LRUCache;
    ///
    /// let mut cache: LRUCache<&str, _> = LRUCache::with_capacity(10);
    /// assert_eq!(cache.remove(&"foo"), None);
    /// cache.insert("foo", 1);
    /// assert_eq!(cache.remove(&"foo"), Some(1));
    /// ```
    pub fn remove<Q: ?Sized>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.table.remove(&key).map(|idx| {
            self.remove_from_list(idx);
            self.free_indexes.push(idx);
            self.entries[idx].value.take().unwrap()
        })
    }

    /// Retrieves a reference to the item associated with `key` from the cache
    /// without promoting it.
    ///
    /// # Example
    /// ```
    /// use rucene::core::search::cache::LRUCache;
    ///
    /// let mut cache: LRUCache<&str, _> = LRUCache::with_capacity(2);
    /// cache.insert("foo", 1);
    ///
    /// // "foo" will not be promoted, and will then be removed first.
    /// assert_eq!(cache.peek(&"foo"), Some(&1));
    /// cache.insert("bar", 2);
    /// cache.insert("baz", 3);
    /// assert!(!cache.contains_key(&"foo"));
    /// ```
    pub fn peek<Q: ?Sized>(&mut self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let entries = &self.entries;
        self.table
            .get(key)
            .and_then(move |i| entries[*i].value.as_ref())
    }

    /// Retrieves a reference to the item associated with `key` from the cache.
    ///
    /// # Example
    /// ```
    /// use rucene::core::search::cache::LRUCache;
    ///
    /// let mut cache: LRUCache<&str, _> = LRUCache::with_capacity(2);
    /// assert_eq!(cache.get(&"foo"), None);
    /// cache.insert("foo", 1);
    /// assert_eq!(cache.get(&"foo"), Some(&1));
    /// ```
    pub fn get<Q: ?Sized>(&mut self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        if self.contains_key(key) {
            self.access(key);
        }
        self.peek(key)
    }

    /// Retrieves a mutable reference to the item associated with `key` from the cache.
    ///
    /// # Example
    /// ```
    /// use rucene::core::search::cache::LRUCache;
    ///
    /// let mut cache: LRUCache<&str, _> = LRUCache::with_capacity(10);
    /// cache.insert("foo", 1);
    /// {
    ///     let foo = cache.get_mut(&"foo").unwrap();
    ///     *foo = 2;
    /// }
    /// assert_eq!(cache.get(&"foo"), Some(&2));
    /// ```
    pub fn get_mut<Q: ?Sized>(&mut self, key: &Q) -> Option<&mut V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        if self.contains_key(key) {
            self.access(key);
        }
        let entries = &mut self.entries;
        self.table
            .get(key)
            .and_then(move |i| entries[*i].value.as_mut())
    }

    /// Returns the number of elements currently in the cache.
    pub fn len(&self) -> usize {
        self.table.len()
    }

    /// Returns true if the cache contains no elements.
    ///
    /// # Example
    /// ```
    /// use rucene::core::search::cache::LRUCache;
    ///
    /// let mut cache = LRUCache::with_capacity(10);
    /// assert!(cache.is_empty());
    ///
    /// cache.insert("foo", 1);
    /// assert!(!cache.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    /// Returns true if the cache is at full capacity. Any subsequent insertions of keys not
    /// already present will eject the oldest element from the cache.
    pub fn is_full(&self) -> bool {
        self.table.len() == self.capacity
    }

    /// Promotes the specified key to the top of the cache.
    fn access<Q: ?Sized>(&mut self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let i = self.table[key];
        self.remove_from_list(i);
        self.add_head_idx(i);
    }

    fn add_head_idx(&mut self, i: usize) {
        if let Some(f) = self.first {
            self.entries[f].prev = Some(i);
        }
        self.entries[i].prev = None;
        self.entries[i].next = self.first;
        self.first = Some(i);
        self.last = self.last.or(self.first);
    }

    /// Returns true if the key is in the cache.
    ///
    /// This does not promote its position in the cache.
    ///
    /// ```
    /// use rucene::core::search::cache::LRUCache;
    ///
    /// let mut cache = LRUCache::with_capacity(10);
    /// assert_eq!(cache.contains_key(&10), false);
    /// cache.insert(10, "foo");
    /// assert_eq!(cache.contains_key(&10), true);
    /// ```
    pub fn contains_key<Q: ?Sized>(&mut self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.table.contains_key(key)
    }

    /// Removes an item from the linked list.
    fn remove_from_list(&mut self, i: usize) {
        let (prev, next) = {
            let entry = &self.entries[i];
            (entry.prev, entry.next)
        };
        match (prev, next) {
            // Item was in the middle of the list
            (Some(j), Some(k)) => {
                {
                    let first = &mut self.entries[j];
                    first.next = next;
                }
                let second = &mut self.entries[k];
                second.prev = prev;
            }
            // Item was at the end of the list
            (Some(j), None) => {
                let first = &mut self.entries[j];
                first.next = None;
                self.last = prev;
            }
            // Item was at front
            (None, Some(k)) => {
                let second = &mut self.entries[k];
                second.prev = prev;
                self.first = next;
            }
            (None, None) => {
                self.first = None;
                self.last = None;
            }
        }
    }

    fn ensure_room(&mut self) {
        if self.capacity == self.len() {
            self.remove_last();
        }
    }

    /// Removes the oldest item in the cache.
    pub fn remove_last(&mut self) -> Option<K> {
        let key = if let Some(idx) = self.last {
            self.remove_from_list(idx);
            let k = &self.entries[idx].key;
            self.free_indexes.push(idx);
            self.table.remove_entry(k).map(|(key, _v)| key)
        } else {
            None
        };
        if self.last.is_none() {
            self.first = None;
        }
        key
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    #[test]
    fn test_lru_cache_insert() {
        let mut cache: LRUCache<String, i32> = LRUCache::with_capacity(3);
        cache.insert("1".into(), 1);
        assert_eq!(cache.peek("1"), Some(&1));
        cache.insert("2".into(), 2);
        assert_eq!(cache.peek("1"), Some(&1));
        assert_eq!(cache.peek("2"), Some(&2));
        cache.insert("3".into(), 3);
        assert_eq!(cache.peek("1"), Some(&1));
        assert_eq!(cache.peek("2"), Some(&2));
        assert_eq!(cache.peek("3"), Some(&3));
        cache.insert("4".into(), 4);
        assert_eq!(cache.peek("1"), None);
        assert_eq!(cache.peek("2"), Some(&2));
        assert_eq!(cache.peek("3"), Some(&3));
        assert_eq!(cache.peek("4"), Some(&4));
        cache.get("2");
        cache.insert("5".into(), 5);
        assert_eq!(cache.peek("2"), Some(&2));
        assert_eq!(cache.peek("3"), None);
        assert_eq!(cache.peek("4"), Some(&4));
        assert_eq!(cache.peek("5"), Some(&5));
        assert_eq!(cache.entries.len(), 3);
        cache.remove("2");
        assert_eq!(cache.peek("2"), None);
        cache.insert("6".into(), 6);
        assert_eq!(cache.peek("6"), Some(&6));
        assert_eq!(cache.entries.len(), 3);
    }
}
