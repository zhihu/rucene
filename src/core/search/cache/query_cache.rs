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

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use core::index::reader::LeafReaderContext;
use core::search::cache::{LRUCache, QueryCachingPolicy};
use core::search::collector::Collector;
use core::search::scorer::{BulkScorer, ConstantScoreScorer};
use core::search::Explanation;
use core::search::{
    query::Weight, scorer::two_phase_next, scorer::Scorer, DocIdSet, DocIterator, NO_MORE_DOCS,
};
use core::util::external::Deferred;
use core::util::UnsignedShift;
use core::util::{
    BitDocIdSet, BitSetDocIterator, DocIdSetDocIterEnum, DocIdSetEnum, NotDocIdSet,
    ShortArrayDocIdSet,
};
use core::util::{BitSet, FixedBitSet, ImmutableBitSet};
use core::util::{Bits, DocId};

use core::codec::Codec;
use error::Result;

// A cache for queries.
pub trait QueryCache<C: Codec>: Send + Sync {
    ///
    // Return a wrapper around the provided `weight` that will cache
    // matching docs per-segment accordingly to the given `policy`.
    // NOTE: The returned weight will only be equivalent if scores are not needed.
    // see Collector#needs_scores()
    //
    fn do_cache(
        &self,
        weight: Box<dyn Weight<C>>,
        policy: Arc<dyn QueryCachingPolicy<C>>,
    ) -> Box<dyn Weight<C>>;
}

/// cache nothing
#[derive(Default)]
pub struct NoCacheQueryCache;

impl NoCacheQueryCache {
    pub fn new() -> Self {
        Default::default()
    }
}

impl<C: Codec> QueryCache<C> for NoCacheQueryCache {
    fn do_cache(
        &self,
        weight: Box<dyn Weight<C>>,
        _policy: Arc<dyn QueryCachingPolicy<C>>,
    ) -> Box<dyn Weight<C>> {
        weight
    }
}

struct LeafCache {
    _key: String,
    leaf_cache: HashMap<String, CacheDocIdSetEnum>,
}

impl LeafCache {
    pub fn new(_key: String) -> LeafCache {
        LeafCache {
            _key,
            leaf_cache: HashMap::new(),
        }
    }

    pub fn get(&self, query_key: &str) -> Result<Option<CachedDocIdSetIterEnum>> {
        match self.leaf_cache.get(query_key) {
            Some(set) => set.iterator(),
            None => Ok(None),
        }
    }

    pub fn put_if_absent(&mut self, query_key: &str, set: CacheDocIdSetEnum) {
        if !self.leaf_cache.contains_key(query_key) {
            self.leaf_cache.insert(query_key.to_string(), set);
        }
    }

    pub fn remove(&mut self, query_key: &str) {
        self.leaf_cache.remove(query_key);
    }
}

struct CacheData {
    // maps queries that are contained in the cache to a singleton so that this
    // cache does not store several copies of the same query
    pub unique_queries: LRUCache<String, String>,
    // The contract between this set and the per-leaf caches is that per-leaf caches
    // are only allowed to store sub-sets of the queries that are contained in
    // mostRecentlyUsedQueries. This is why write operations are performed under a lock
    // pub most_recently_used_queries: HashSet<Query>,
    pub cache: HashMap<String, LeafCache>,

    max_size: usize,
    min_size: i32,
    min_size_ratio: f32,
}

impl CacheData {
    fn test<C: Codec>(&self, leaf_reader: &LeafReaderContext<'_, C>) -> Result<bool> {
        if leaf_reader.reader.max_doc() < self.min_size {
            Ok(false)
        } else {
            Ok(
                leaf_reader.reader.max_doc() as f32 / leaf_reader.parent.max_doc() as f32
                    >= self.min_size_ratio,
            )
        }
    }

    /// Whether evictions are required.
    fn requires_eviction(&self) -> Result<bool> {
        Ok(self.unique_queries.len() >= self.max_size)
    }

    fn get<C: Codec>(
        &mut self,
        query_key: &str,
        leaf_reader: &LeafReaderContext<'_, C>,
    ) -> Result<Option<CachedDocIdSetIterEnum>> {
        if let Some(leaf_cache) = self.cache.get(leaf_reader.reader.core_cache_key()) {
            if let Some(singleton) = self.unique_queries.get(&query_key.to_string()) {
                // this get call moves the query to the most-recently-used position
                return leaf_cache.get(singleton);
            }
        }
        Ok(None)
    }

    // return true if new LeafCache is added to process core reader drop listener
    pub fn put_if_absent<C: Codec>(
        &mut self,
        query_key: &str,
        leaf_reader: &LeafReaderContext<'_, C>,
        set: CacheDocIdSetEnum,
    ) -> Result<bool> {
        self.evict_if_necessary()?;

        let query_key = if self.unique_queries.contains_key(&query_key.to_string()) {
            self.unique_queries
                .get(&query_key.to_string())
                .unwrap()
                .to_string()
        } else {
            self.unique_queries
                .insert(query_key.to_string(), query_key.to_string());
            query_key.to_string()
        };

        let key = leaf_reader.reader.core_cache_key();
        let new_entry = if self.cache.contains_key(key) {
            false
        } else {
            let leaf_cache = LeafCache::new(key.into());
            self.cache.insert(key.into(), leaf_cache);
            true
        };

        {
            let leaf_cache = self.cache.get_mut(key).unwrap();
            leaf_cache.put_if_absent(&query_key, set);
        }

        Ok(new_entry)
    }

    fn evict_if_necessary(&mut self) -> Result<()> {
        if self.requires_eviction()? {
            loop {
                if !self.requires_eviction()? {
                    break;
                }

                if let Some(key) = self.unique_queries.remove_last() {
                    self.on_eviction(&key);
                } else {
                    bail!(
                        "Removal from the cache failed! This is probably due to a query which has \
                         been modified after having been put into the cache or a badly \
                         implemented clone()."
                    );
                }
            }
        }

        Ok(())
    }

    fn on_eviction(&mut self, query_key: &str) {
        for leaf_cache in self.cache.values_mut() {
            leaf_cache.remove(query_key);
        }
    }
}

/// A [`QueryCache`] that evicts queries using a LRU (least-recently-used)
/// eviction policy in order to remain under a given maximum size and number of
/// bytes used.
///
/// This class is thread-safe.
///
/// Note that query eviction runs in linear time with the total number of
/// segments that have cache entries so this cache works best with a
/// `QueryCachingPolicy` caching policies that only cache on "large"
/// segments, and it is advised to not share this cache across too many indices.
pub struct LRUQueryCache {
    cache_data: Arc<RwLock<CacheData>>,
}

impl LRUQueryCache {
    pub fn new(max_size: usize) -> LRUQueryCache {
        // let max_size = 10;
        let cache_data = CacheData {
            unique_queries: LRUCache::with_capacity(max_size),
            cache: HashMap::new(),
            max_size,
            min_size: 10000,
            min_size_ratio: 0.03f32,
        };

        LRUQueryCache {
            cache_data: Arc::new(RwLock::new(cache_data)),
        }
    }
}

impl<C: Codec> QueryCache<C> for LRUQueryCache {
    fn do_cache(
        &self,
        weight: Box<dyn Weight<C>>,
        policy: Arc<dyn QueryCachingPolicy<C>>,
    ) -> Box<dyn Weight<C>> {
        if weight.query_type() == CACHING_QUERY_TYPE_STR {
            weight
        } else {
            Box::new(CachingWrapperWeight::new(
                Arc::clone(&self.cache_data),
                weight,
                policy,
            ))
        }
    }
}

struct CachingWrapperWeight<C: Codec> {
    cache_data: Arc<RwLock<CacheData>>,
    weight: Box<dyn Weight<C>>,
    policy: Arc<dyn QueryCachingPolicy<C>>,
    used: AtomicBool,
    query_key: String,
    hash_code: u32,
}

impl<C: Codec> CachingWrapperWeight<C> {
    fn new(
        cache_data: Arc<RwLock<CacheData>>,
        weight: Box<dyn Weight<C>>,
        policy: Arc<dyn QueryCachingPolicy<C>>,
    ) -> CachingWrapperWeight<C> {
        let query_key = format!("{}", weight);
        let mut hasher = DefaultHasher::new();
        query_key.hash(&mut hasher);
        CachingWrapperWeight {
            cache_data,
            weight,
            policy,
            used: AtomicBool::new(false),
            query_key,
            hash_code: hasher.finish() as u32,
        }
    }

    ///
    // Default cache implementation: uses {@link RoaringDocIdSet} for sets that
    // have a density &lt; 1% and a {@link BitDocIdSet} over a {@link FixedBitSet}
    // otherwise.
    //
    fn cache_impl<'a, S>(
        &self,
        scorer: &mut BulkScorer<'a, S>,
        max_doc: i32,
    ) -> Result<CacheDocIdSetEnum>
    where
        S: Scorer + ?Sized + 'a,
    {
        // FixedBitSet is faster for dense sets and will enable the random-access
        // optimization in ConjunctionDISI
        if scorer.scorer().cost() * 100 > max_doc as usize {
            Ok(CacheDocIdSetEnum::Bit(
                self.cache_into_bitset(scorer, max_doc)?,
            ))
        } else {
            Ok(CacheDocIdSetEnum::Roaring(
                self.cache_into_roaring_docid_set(scorer, max_doc)?,
            ))
        }
    }

    fn cache_into_bitset<'a, S>(
        &self,
        scorer: &mut BulkScorer<'a, S>,
        max_doc: i32,
    ) -> Result<BitDocIdSet<FixedBitSet>>
    where
        S: Scorer + ?Sized + 'a,
    {
        let mut leaf_collector = BitSetLeafCollector {
            bit_set: FixedBitSet::new(max_doc as usize),
            cost: 0,
        };

        let bits: Option<FixedBitSet> = None;
        scorer.score(
            &mut leaf_collector,
            bits.as_ref(),
            0,
            NO_MORE_DOCS,
            NO_MORE_DOCS as usize,
        )?;

        Ok(BitDocIdSet::new(
            Arc::new(leaf_collector.bit_set),
            leaf_collector.cost as usize,
        ))
    }

    fn cache_into_roaring_docid_set<'a, S>(
        &self,
        scorer: &mut BulkScorer<S>,
        max_doc: i32,
    ) -> Result<RoaringDocIdSet>
    where
        S: Scorer + ?Sized + 'a,
    {
        let mut leaf_collector = DocIdSetLeafCollector {
            doc_id_set: RoaringDocIdSetBuilder::new(max_doc),
        };

        let bits: Option<FixedBitSet> = None;
        scorer.score(
            &mut leaf_collector,
            bits.as_ref(),
            0,
            NO_MORE_DOCS,
            NO_MORE_DOCS as usize,
        )?;

        Ok(leaf_collector.doc_id_set.build())
    }

    fn should_cache(&self, leaf_reader: &LeafReaderContext<'_, C>) -> Result<bool> {
        self.cache_data.read()?.test(leaf_reader)
    }

    fn cache(
        &self,
        leaf_reader: &LeafReaderContext<'_, C>,
        query_key: &str,
    ) -> Result<Option<CachedDocIdSetIterEnum>> {
        match self.weight.create_scorer(leaf_reader)? {
            Some(mut scorer) => {
                let mut bulk_scorer = BulkScorer::new(scorer.as_mut());
                let max_doc = leaf_reader.reader.max_doc();

                let doc_id_set = self.cache_impl(&mut bulk_scorer, max_doc)?;

                let iter = doc_id_set.iterator()?;
                if self
                    .cache_data
                    .write()?
                    .put_if_absent(query_key, leaf_reader, doc_id_set)?
                {
                    let key = leaf_reader.reader.core_cache_key().to_owned();
                    let cache_data = Arc::clone(&self.cache_data);
                    leaf_reader
                        .reader
                        .add_core_drop_listener(Deferred::new(move || {
                            let core_key = key;
                            cache_data.write().unwrap().cache.remove(&core_key);
                        }))
                }

                Ok(iter)
            }
            None => Ok(None),
        }
    }
}

static CACHING_QUERY_TYPE_STR: &str = "CachingWrapperWeight";

impl<C: Codec> Weight<C> for CachingWrapperWeight<C> {
    fn create_scorer(
        &self,
        leaf_reader: &LeafReaderContext<'_, C>,
    ) -> Result<Option<Box<dyn Scorer>>> {
        if !self.used.compare_and_swap(false, true, Ordering::AcqRel) {
            self.policy.on_use(self)
        }

        // Short-circuit: Check whether this segment is eligible for caching
        // before we take a lock because of #get
        if !self.should_cache(leaf_reader)? {
            return self.weight.create_scorer(leaf_reader);
        }

        {
            // If the lock is already busy, prefer using the uncached version than waiting
            match self.cache_data.try_write() {
                Ok(mut cache_data) => {
                    if let Some(disi) = cache_data.get(&self.query_key, leaf_reader)? {
                        let cost = disi.cost();
                        return Ok(Some(Box::new(ConstantScoreScorer::new(0.0f32, disi, cost))));
                    }
                }
                _ => {
                    return self.weight.create_scorer(leaf_reader);
                }
            }
        }

        if self.policy.should_cache(self)? {
            match self.cache(leaf_reader, &self.query_key)? {
                Some(disi) => {
                    let cost = disi.cost();
                    Ok(Some(Box::new(ConstantScoreScorer::new(0.0f32, disi, cost))))
                }
                None => Ok(None),
            }
        } else {
            self.weight.create_scorer(leaf_reader)
        }
    }

    fn hash_code(&self) -> u32 {
        self.hash_code
    }

    fn query_type(&self) -> &'static str {
        CACHING_QUERY_TYPE_STR
    }

    fn actual_query_type(&self) -> &'static str {
        self.weight.query_type()
    }

    fn normalize(&mut self, norm: f32, boost: f32) {
        self.weight.normalize(norm, boost)
    }

    fn value_for_normalization(&self) -> f32 {
        self.weight.value_for_normalization()
    }

    fn needs_scores(&self) -> bool {
        self.weight.needs_scores()
    }

    fn explain(&self, reader: &LeafReaderContext<'_, C>, doc: DocId) -> Result<Explanation> {
        let exists = if let Some(mut iterator) = self.weight.create_scorer(reader)? {
            if iterator.support_two_phase() {
                two_phase_next(iterator.as_mut())? == doc && iterator.matches()?
            } else {
                iterator.advance(doc)? == doc
            }
        } else {
            false
        };

        if exists {
            Ok(Explanation::new(
                true,
                1.0f32,
                format!("{}, product of:", self.weight),
                vec![
                    Explanation::new(true, 1.0f32, "boost".to_string(), vec![]),
                    Explanation::new(true, 1.0f32, "queryNorm".to_string(), vec![]),
                ],
            ))
        } else {
            Ok(Explanation::new(
                false,
                0.0f32,
                format!("{} doesn't match id {}", self.weight, doc),
                vec![],
            ))
        }
    }
}

impl<C: Codec> fmt::Display for CachingWrapperWeight<C> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CachingWrapperWeight(weight: {})", self.weight)
    }
}

struct BitSetLeafCollector {
    bit_set: FixedBitSet,
    cost: i64,
}

impl Collector for BitSetLeafCollector {
    fn needs_scores(&self) -> bool {
        false
    }

    fn collect<S: Scorer + ?Sized>(&mut self, doc: DocId, _scorer: &mut S) -> Result<()> {
        self.cost += 1;
        self.bit_set.set(doc as usize);

        Ok(())
    }
}

struct DocIdSetLeafCollector {
    doc_id_set: RoaringDocIdSetBuilder,
}

impl Collector for DocIdSetLeafCollector {
    fn needs_scores(&self) -> bool {
        false
    }

    fn collect<S: Scorer + ?Sized>(&mut self, doc: DocId, _scorer: &mut S) -> Result<()> {
        self.doc_id_set.add_doc(doc)
    }
}

// Number of documents in a block
static BLOCK_SIZE: usize = 1 << 16;
// The maximum length for an array, beyond that point we switch to a bitset
static MAX_ARRAY_LENGTH: usize = 1 << 12;

///
// {@link DocIdSet} implementation inspired from http://roaringbitmap.org/
//
// The space is divided into blocks of 2^16 bits and each block is encoded
// independently. In each block, if less than 2^12 bits are set, then
// documents are simply stored in a short[]. If more than 2^16-2^12 bits are
// set, then the inverse of the set is encoded in a simple short[]. Otherwise
// a {@link FixedBitSet} is used.
//
// @lucene.internal
//
struct RoaringDocIdSet {
    doc_id_sets: Arc<[Option<DocIdSetEnum>]>,
    cardinality: usize,
}

impl RoaringDocIdSet {
    fn new(doc_id_sets: Vec<Option<DocIdSetEnum>>, cardinality: usize) -> RoaringDocIdSet {
        RoaringDocIdSet {
            doc_id_sets: Arc::from(doc_id_sets.into_boxed_slice()),
            cardinality,
        }
    }
}

struct RoaringDocIdSetBuilder {
    doc_id_sets: Vec<Option<DocIdSetEnum>>,
    cardinality: usize,

    max_doc: i32,
    last_doc_id: DocId,
    current_block: i32,
    current_block_cardinality: usize,

    // We start by filling the buffer and when it's full we copy the content of
    // the buffer to the FixedBitSet and put further documents in that bitset
    buffer: Vec<u16>,
    dense_buffer: Option<Box<FixedBitSet>>,
}

impl RoaringDocIdSetBuilder {
    fn new(max_doc: i32) -> RoaringDocIdSetBuilder {
        let length = (max_doc + (1 << 16) - 1).unsigned_shift(16);
        let mut doc_id_sets = Vec::with_capacity(length as usize);
        for _ in 0..length {
            doc_id_sets.push(None);
        }

        RoaringDocIdSetBuilder {
            doc_id_sets,
            cardinality: 0,
            max_doc,
            last_doc_id: -1,
            current_block: -1,
            current_block_cardinality: 0,
            buffer: vec![0u16; MAX_ARRAY_LENGTH as usize],
            dense_buffer: None,
        }
    }

    fn flush(&mut self) {
        assert!(self.current_block_cardinality <= BLOCK_SIZE);

        let current_block = self.current_block;
        let current_block_cardinality = self.current_block_cardinality;

        if current_block_cardinality <= MAX_ARRAY_LENGTH {
            // Use sparse encoding
            assert!(self.dense_buffer.is_none());
            if current_block_cardinality > 0 {
                let mut docs: Vec<u16> = vec![0u16; current_block_cardinality];
                docs.copy_from_slice(&self.buffer[0..current_block_cardinality]);

                self.doc_id_sets[current_block as usize] = Some(DocIdSetEnum::ShortArray(
                    ShortArrayDocIdSet::new(docs, current_block_cardinality),
                ));
            }
        } else {
            assert!(self.dense_buffer.is_some());
            assert_eq!(
                self.dense_buffer.as_mut().unwrap().cardinality(),
                self.current_block_cardinality
            );

            if self.dense_buffer.as_mut().unwrap().len() == BLOCK_SIZE as usize
                && BLOCK_SIZE - self.current_block_cardinality < MAX_ARRAY_LENGTH
            {
                let dense_buffer = self.dense_buffer.as_mut().unwrap();
                // Doc ids are very dense, inverse the encoding
                let mut exclude_docs =
                    vec![0u16; (BLOCK_SIZE - self.current_block_cardinality) as usize];
                let num_bits = dense_buffer.num_bits;
                dense_buffer.flip(0, num_bits);

                let mut exclude_doc = -1;
                unsafe {
                    let ptr = exclude_docs.as_mut_ptr();
                    for i in 0..exclude_docs.len() {
                        exclude_doc = dense_buffer.next_set_bit((exclude_doc + 1) as usize);
                        debug_assert_ne!(exclude_doc, NO_MORE_DOCS);
                        *ptr.add(i) = exclude_doc as u16;
                    }
                }

                assert!(
                    exclude_doc as usize + 1 == dense_buffer.len()
                        || dense_buffer.next_set_bit((exclude_doc + 1) as usize) == NO_MORE_DOCS
                );

                let length = exclude_docs.len();
                self.doc_id_sets[self.current_block as usize] =
                    Some(DocIdSetEnum::NotDocId(NotDocIdSet::new(
                        ShortArrayDocIdSet::new(exclude_docs, length),
                        BLOCK_SIZE as i32,
                    )));
            } else {
                // Neither sparse nor super dense, use a fixed bit set
                let dense_buf = self.dense_buffer.take().unwrap();
                self.doc_id_sets[self.current_block as usize] =
                    Some(DocIdSetEnum::BitDocId(BitDocIdSet::new(
                        Arc::from(dense_buf),
                        self.current_block_cardinality as usize,
                    )));
            }
        }

        self.cardinality += self.current_block_cardinality;
        self.dense_buffer = None;
        self.current_block_cardinality = 0;
    }

    ///
    // Add a new doc-id to this builder.
    // NOTE: doc ids must be added in order.
    //
    pub fn add_doc(&mut self, doc_id: i32) -> Result<()> {
        if doc_id < self.last_doc_id {
            bail!(
                "Doc ids must be added in-order, got {} which is <= lastDocID={}",
                doc_id,
                self.last_doc_id
            );
        }

        let block = doc_id.unsigned_shift(16);
        if block != self.current_block {
            // we went to a different block, let's flush what we buffered and start from fresh
            self.flush();
            self.current_block = block;
        }

        if self.current_block_cardinality < MAX_ARRAY_LENGTH {
            unsafe {
                *self.buffer.as_mut_ptr().add(self.current_block_cardinality) = doc_id as u16
            };
        } else {
            if self.dense_buffer.is_none() {
                // the buffer is full, let's move to a fixed bit set
                let num_bits = (1i32 << 16).min(self.max_doc - (block << 16));
                let mut fixed_bit_set = Box::new(FixedBitSet::new(num_bits as usize));
                for doc in &self.buffer {
                    fixed_bit_set.set(*doc as usize);
                }

                self.dense_buffer = Some(fixed_bit_set);
            }

            self.dense_buffer
                .as_mut()
                .unwrap()
                .set((doc_id & 0xFFFF) as usize);
        }

        self.last_doc_id = doc_id;
        self.current_block_cardinality += 1;

        Ok(())
    }

    pub fn build(mut self) -> RoaringDocIdSet {
        self.flush();
        RoaringDocIdSet::new(self.doc_id_sets, self.cardinality)
    }
}

impl DocIdSet for RoaringDocIdSet {
    type Iter = RoaringDocIterator;
    fn iterator(&self) -> Result<Option<Self::Iter>> {
        if self.cardinality == 0 {
            Ok(None)
        } else {
            Ok(Some(RoaringDocIterator::new(
                self.doc_id_sets.clone(),
                self.cardinality,
            )))
        }
    }
}

struct RoaringDocIterator {
    doc_id_sets: Arc<[Option<DocIdSetEnum>]>,
    doc: DocId,
    block: i32,
    cardinality: usize,
    sub: Option<DocIdSetDocIterEnum>,
}

impl RoaringDocIterator {
    fn new(doc_id_sets: Arc<[Option<DocIdSetEnum>]>, cardinality: usize) -> Self {
        RoaringDocIterator {
            doc_id_sets,
            doc: -1,
            block: -1,
            cardinality,
            // init as stub
            sub: Some(DocIdSetDocIterEnum::default()),
        }
    }

    fn first_doc_from_next_block(&mut self) -> Result<DocId> {
        loop {
            self.block += 1;
            if self.block as usize >= self.doc_id_sets.len() {
                self.sub = None;
                self.doc = NO_MORE_DOCS;

                return Ok(self.doc);
            } else if self.doc_id_sets[self.block as usize].is_some() {
                self.sub = self.doc_id_sets[self.block as usize]
                    .as_ref()
                    .unwrap()
                    .iterator()?;
                let sub_next = self.sub.as_mut().unwrap().next()?;
                debug_assert_ne!(sub_next, NO_MORE_DOCS);

                self.doc = (self.block << 16) | sub_next;
                return Ok(self.doc);
            }
        }
    }
}

impl DocIterator for RoaringDocIterator {
    fn doc_id(&self) -> DocId {
        self.doc
    }

    fn next(&mut self) -> Result<DocId> {
        let sub_next = self.sub.as_mut().unwrap().next()?;
        if sub_next == NO_MORE_DOCS {
            return self.first_doc_from_next_block();
        }

        self.doc = (self.block << 16) | sub_next;
        Ok(self.doc)
    }

    fn advance(&mut self, target: DocId) -> Result<DocId> {
        let target_block = target.unsigned_shift(16);

        if target_block != self.block {
            self.block = target_block;
            if self.block as usize > self.doc_id_sets.len() {
                self.sub = None;
                self.doc = NO_MORE_DOCS;

                return Ok(self.doc);
            }

            if self.doc_id_sets[self.block as usize].is_none() {
                return self.first_doc_from_next_block();
            }

            self.sub = self.doc_id_sets[self.block as usize]
                .as_ref()
                .unwrap()
                .iterator()?;
        }

        let sub_next = self.sub.as_mut().unwrap().advance(target & 0xFFFF)?;
        if sub_next == NO_MORE_DOCS {
            return self.first_doc_from_next_block();
        }

        self.doc = (self.block << 16) | sub_next;
        Ok(self.doc)
    }

    fn cost(&self) -> usize {
        self.cardinality as usize
    }
}

enum CacheDocIdSetEnum {
    Bit(BitDocIdSet<FixedBitSet>),
    Roaring(RoaringDocIdSet),
}

impl DocIdSet for CacheDocIdSetEnum {
    type Iter = CachedDocIdSetIterEnum;

    fn iterator(&self) -> Result<Option<Self::Iter>> {
        match self {
            CacheDocIdSetEnum::Bit(i) => {
                if let Some(iter) = i.iterator()? {
                    Ok(Some(CachedDocIdSetIterEnum::Bit(iter)))
                } else {
                    Ok(None)
                }
            }
            CacheDocIdSetEnum::Roaring(i) => {
                if let Some(iter) = i.iterator()? {
                    Ok(Some(CachedDocIdSetIterEnum::Roaring(iter)))
                } else {
                    Ok(None)
                }
            }
        }
    }
}

enum CachedDocIdSetIterEnum {
    Bit(BitSetDocIterator<FixedBitSet>),
    Roaring(RoaringDocIterator),
}

impl DocIterator for CachedDocIdSetIterEnum {
    fn doc_id(&self) -> DocId {
        match self {
            CachedDocIdSetIterEnum::Bit(i) => i.doc_id(),
            CachedDocIdSetIterEnum::Roaring(i) => i.doc_id(),
        }
    }

    fn next(&mut self) -> Result<DocId> {
        match self {
            CachedDocIdSetIterEnum::Bit(i) => i.next(),
            CachedDocIdSetIterEnum::Roaring(i) => i.next(),
        }
    }

    fn advance(&mut self, target: i32) -> Result<DocId> {
        match self {
            CachedDocIdSetIterEnum::Bit(i) => i.advance(target),
            CachedDocIdSetIterEnum::Roaring(i) => i.advance(target),
        }
    }

    fn slow_advance(&mut self, target: i32) -> Result<DocId> {
        match self {
            CachedDocIdSetIterEnum::Bit(i) => i.slow_advance(target),
            CachedDocIdSetIterEnum::Roaring(i) => i.slow_advance(target),
        }
    }

    fn cost(&self) -> usize {
        match self {
            CachedDocIdSetIterEnum::Bit(i) => i.cost(),
            CachedDocIdSetIterEnum::Roaring(i) => i.cost(),
        }
    }

    fn matches(&mut self) -> Result<bool> {
        match self {
            CachedDocIdSetIterEnum::Bit(i) => i.matches(),
            CachedDocIdSetIterEnum::Roaring(i) => i.matches(),
        }
    }

    fn match_cost(&self) -> f32 {
        match self {
            CachedDocIdSetIterEnum::Bit(i) => i.match_cost(),
            CachedDocIdSetIterEnum::Roaring(i) => i.match_cost(),
        }
    }

    fn approximate_next(&mut self) -> Result<DocId> {
        match self {
            CachedDocIdSetIterEnum::Bit(i) => i.approximate_next(),
            CachedDocIdSetIterEnum::Roaring(i) => i.approximate_next(),
        }
    }

    fn approximate_advance(&mut self, target: i32) -> Result<DocId> {
        match self {
            CachedDocIdSetIterEnum::Bit(i) => i.approximate_advance(target),
            CachedDocIdSetIterEnum::Roaring(i) => i.approximate_advance(target),
        }
    }
}
