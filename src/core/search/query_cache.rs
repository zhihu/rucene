use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::mem;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use core::search::lru_cache::LRUCache;

use core::index::LeafReader;
use core::search::bulk_scorer::BulkScorer;
use core::search::cache_policy::QueryCachingPolicy;
use core::search::collector::{Collector, LeafCollector, SearchCollector};
use core::search::explanation::Explanation;
use core::search::match_all::ConstantScoreScorer;
use core::search::two_phase_next;
use core::search::Weight;
use core::search::{DocIdSet, DocIterator, EmptyDocIterator};
use core::search::{MatchNoDocScorer, Scorer, NO_MORE_DOCS};
use core::util::bit_set::{BitSet, FixedBitSet, ImmutableBitSet, ImmutableBitSetRef};
use core::util::bit_util::UnsignedShift;
use core::util::doc_id_set::BitDocIdSet;
use core::util::Bits;
use core::util::DocId;

use error::*;

///
// A cache for queries.
//
// @see LRUQueryCache
// @lucene.experimental
//
pub trait QueryCache: Send + Sync {
    ///
    // Return a wrapper around the provided <code>weight</code> that will cache
    // matching docs per-segment accordingly to the given <code>policy</code>.
    // NOTE: The returned weight will only be equivalent if scores are not needed.
    // @see Collector#needsScores()
    //
    fn do_cache(&self, weight: Box<Weight>, policy: Arc<QueryCachingPolicy>) -> Box<Weight>;
}

/// cache nothing
pub struct NoCacheQueryCache;

impl NoCacheQueryCache {
    pub fn new() -> Self {
        NoCacheQueryCache {}
    }
}

impl QueryCache for NoCacheQueryCache {
    fn do_cache(&self, weight: Box<Weight>, _policy: Arc<QueryCachingPolicy>) -> Box<Weight> {
        weight
    }
}

///
// A {@link QueryCache} that evicts queries using a LRU (least-recently-used)
// eviction policy in order to remain under a given maximum size and number of
// bytes used.
//
// This class is thread-safe.
//
// Note that query eviction runs in linear time with the total number of
// segments that have cache entries so this cache works best withCa
// {@link QueryCachingPolicy caching policies} that only cache on "large"
// segments, and it is advised to not share this cache across too many indices.
//
// A default query cache and policy instance is used in IndexSearcher. If you want to replace those
// defaults it is typically done like this:
// <pre class="prettyprint">
//   final int maxNumberOfCachedQueries = 256;
//   final long maxRamBytesUsed = 50 * 1024L * 1024L; // 50MB
//   // these cache and policy instances can be shared across several queries and readers
//   // it is fine to eg. store them into static variables
//   final QueryCache queryCache = new LRUQueryCache(maxNumberOfCachedQueries, maxRamBytesUsed);
//   final QueryCachingPolicy defaultCachingPolicy = new UsageTrackingQueryCachingPolicy();
//   indexSearcher.setQueryCache(queryCache);
//   indexSearcher.setQueryCachingPolicy(defaultCachingPolicy);
// </pre>
//
// This cache exposes some global statistics ({@link #getHitCount() hit count},
// {@link #getMissCount() miss count}, {@link #getCacheSize() number of cache
// entries}, {@link #getCacheCount() total number of DocIdSets that have ever
// been cached}, {@link #getEvictionCount() number of evicted entries}). In
// case you would like to have more fine-grained statistics, such as per-index
// or per-query-class statistics, it is possible to override various callbacks:
// {@link #onHit}, {@link #onMiss},
// {@link #onQueryCache}, {@link #onQueryEviction},
// {@link #onDocIdSetCache}, {@link #onDocIdSetEviction} and {@link #onClear}.
// It is better to not perform heavy computations in these methods though since
// they are called synchronously and under a lock.
//
// @see QueryCachingPolicy
// @lucene.experimental
//

pub struct LeafCache {
    _key: String,
    leaf_cache: HashMap<String, Box<DocIdSet>>,
}

impl LeafCache {
    pub fn new(_key: String) -> LeafCache {
        LeafCache {
            _key,
            leaf_cache: HashMap::new(),
        }
    }

    pub fn get(&self, query_key: &str) -> Result<Option<Box<DocIterator>>> {
        match self.leaf_cache.get(query_key) {
            Some(set) => set.iterator(),
            None => Ok(None),
        }
    }

    pub fn put_if_absent(&mut self, query_key: &str, set: Box<DocIdSet>) {
        if !self.leaf_cache.contains_key(query_key) {
            self.leaf_cache.insert(query_key.to_string(), set);
        }
    }

    pub fn remove(&mut self, query_key: &str) {
        self.leaf_cache.remove(query_key);
    }
}

pub struct CacheData {
    // maps queries that are contained in the cache to a singleton so that this
    // cache does not store several copies of the same query
    pub unique_queries: LRUCache<String, String>,
    // The contract between this set and the per-leaf caches is that per-leaf caches
    // are only allowed to store sub-sets of the queries that are contained in
    // mostRecentlyUsedQueries. This is why write operations are performed under a lock
    // pub most_recently_used_queries: HashSet<Query>,
    pub cache: HashMap<String, LeafCache>,

    max_size: usize,
    max_doc: i32,
    min_size: i32,
    min_size_ratio: f32,
}

impl CacheData {
    fn test(&self, leaf_reader: &LeafReader) -> Result<bool> {
        if leaf_reader.max_doc() < self.min_size {
            Ok(false)
        } else {
            Ok(leaf_reader.max_doc() as f32 / self.max_doc as f32 >= self.min_size_ratio)
        }
    }

    /// Whether evictions are required.
    fn requires_eviction(&self) -> Result<bool> {
        Ok(self.unique_queries.len() >= self.max_size)
    }

    pub fn get(
        &mut self,
        query_key: &str,
        leaf_reader: &LeafReader,
    ) -> Result<Option<Box<DocIterator>>> {
        if let Some(leaf_cache) = self.cache.get(leaf_reader.core_cache_key()) {
            if let Some(singleton) = self.unique_queries.get(&query_key.to_string()) {
                // this get call moves the query to the most-recently-used position
                return leaf_cache.get(singleton);
            }
        }
        Ok(None)
    }

    pub fn put_if_absent(
        &mut self,
        query_key: &str,
        leaf_reader: &LeafReader,
        set: Box<DocIdSet>,
    ) -> Result<()> {
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

        let key = leaf_reader.core_cache_key();
        if self.cache.contains_key(key) {
        } else {
            let leaf_cache = LeafCache::new(key.into());
            self.cache.insert(key.into(), leaf_cache);
        }

        {
            let leaf_cache = self.cache.get_mut(key).unwrap();
            leaf_cache.put_if_absent(&query_key, set);
        }

        Ok(())
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

pub struct LRUQueryCache {
    cache_data: Arc<RwLock<CacheData>>,
}

impl LRUQueryCache {
    pub fn new(max_size: usize, max_doc: i32) -> LRUQueryCache {
        // let max_size = 10;
        let cache_data = CacheData {
            unique_queries: LRUCache::with_capacity(max_size),
            cache: HashMap::new(),
            max_size,
            max_doc,
            min_size: 10000,
            min_size_ratio: 0.03f32,
        };

        LRUQueryCache {
            cache_data: Arc::new(RwLock::new(cache_data)),
        }
    }
}

impl QueryCache for LRUQueryCache {
    fn do_cache(&self, weight: Box<Weight>, policy: Arc<QueryCachingPolicy>) -> Box<Weight> {
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

pub struct CachingWrapperWeight {
    cache_data: Arc<RwLock<CacheData>>,
    weight: Box<Weight>,
    policy: Arc<QueryCachingPolicy>,
    used: AtomicBool,
    query_key: String,
    hash_code: u32,
}

impl CachingWrapperWeight {
    pub fn new(
        cache_data: Arc<RwLock<CacheData>>,
        weight: Box<Weight>,
        policy: Arc<QueryCachingPolicy>,
    ) -> CachingWrapperWeight {
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
    fn cache_impl(&self, scorer: &mut BulkScorer, max_doc: i32) -> Result<Box<DocIdSet>> {
        // FixedBitSet is faster for dense sets and will enable the random-access
        // optimization in ConjunctionDISI
        if scorer.scorer.cost() * 100 > max_doc as usize {
            self.cache_into_bitset(scorer, max_doc)
        } else {
            self.cache_into_roaring_docid_set(scorer, max_doc)
        }
    }

    fn cache_into_bitset(&self, scorer: &mut BulkScorer, max_doc: i32) -> Result<Box<DocIdSet>> {
        let mut leaf_collector = BitSetLeafCollector {
            bit_set: FixedBitSet::new(max_doc as usize),
            cost: 0,
        };

        scorer.score(&mut leaf_collector, None, 0, NO_MORE_DOCS)?;

        Ok(Box::new(BitDocIdSet::new(
            Box::new(leaf_collector.bit_set),
            leaf_collector.cost as usize,
        )))
    }

    fn cache_into_roaring_docid_set(
        &self,
        scorer: &mut BulkScorer,
        max_doc: i32,
    ) -> Result<Box<DocIdSet>> {
        let mut leaf_collector = DocIdSetLeafCollector {
            doc_id_set: RoaringDocIdSetBuilder::new(max_doc),
        };

        scorer.score(&mut leaf_collector, None, 0, NO_MORE_DOCS)?;

        Ok(Box::new(leaf_collector.doc_id_set.build()))
    }

    fn should_cache(&self, leaf_reader: &LeafReader) -> Result<bool> {
        self.cache_data.read()?.test(leaf_reader)
    }

    fn cache(&self, leaf_reader: &LeafReader, query_key: &str) -> Result<Option<Box<DocIterator>>> {
        let mut scorer = self.weight.create_scorer(leaf_reader)?;
        let mut bulk_scorer = BulkScorer::new(scorer.as_mut());
        let max_doc = leaf_reader.max_doc();

        let doc_id_set = self.cache_impl(&mut bulk_scorer, max_doc)?;

        let iter = doc_id_set.iterator();
        self.cache_data
            .write()?
            .put_if_absent(query_key, leaf_reader, doc_id_set)?;

        iter
    }
}

static CACHING_QUERY_TYPE_STR: &str = "CachingWrapperWeight";

impl Weight for CachingWrapperWeight {
    fn create_scorer(&self, leaf_reader: &LeafReader) -> Result<Box<Scorer>> {
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
                        return Ok(Box::new(ConstantScoreScorer::new(0.0f32, disi, cost)));
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
                    Ok(Box::new(ConstantScoreScorer::new(0.0f32, disi, cost)))
                }
                None => Ok(Box::new(MatchNoDocScorer::default())),
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

    fn explain(&self, reader: &LeafReader, doc: DocId) -> Result<Explanation> {
        let mut iterator = self.weight.create_scorer(reader)?;
        let exists = if iterator.support_two_phase() {
            two_phase_next(iterator.as_mut())? == doc && iterator.matches()?
        } else {
            iterator.advance(doc)? == doc
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

impl fmt::Display for CachingWrapperWeight {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CachingWrapperWeight(weight: {})", self.weight)
    }
}

pub struct BitSetLeafCollector {
    bit_set: FixedBitSet,
    cost: i64,
}

impl SearchCollector for BitSetLeafCollector {
    fn set_next_reader(&mut self, _reader_ord: usize, _reader: &LeafReader) -> Result<()> {
        Ok(())
    }

    fn support_parallel(&self) -> bool {
        false
    }

    fn leaf_collector(&mut self, _reader: &LeafReader) -> Result<Box<LeafCollector>> {
        unimplemented!()
    }

    fn finish(&mut self) -> Result<()> {
        unimplemented!()
    }
}

impl Collector for BitSetLeafCollector {
    fn needs_scores(&self) -> bool {
        false
    }

    fn collect(&mut self, doc: DocId, _scorer: &mut Scorer) -> Result<()> {
        self.cost += 1;
        self.bit_set.set(doc as usize);

        Ok(())
    }
}

pub struct DocIdSetLeafCollector {
    doc_id_set: RoaringDocIdSetBuilder,
}

impl SearchCollector for DocIdSetLeafCollector {
    fn set_next_reader(&mut self, _reader_ord: usize, _reader: &LeafReader) -> Result<()> {
        Ok(())
    }

    fn support_parallel(&self) -> bool {
        false
    }

    fn leaf_collector(&mut self, _reader: &LeafReader) -> Result<Box<LeafCollector>> {
        unimplemented!()
    }

    fn finish(&mut self) -> Result<()> {
        unimplemented!()
    }
}

impl Collector for DocIdSetLeafCollector {
    fn needs_scores(&self) -> bool {
        false
    }

    fn collect(&mut self, doc: DocId, _scorer: &mut Scorer) -> Result<()> {
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
pub struct RoaringDocIdSet {
    doc_id_sets: Arc<Vec<Option<Box<DocIdSet>>>>,
    cardinality: usize,
}

impl RoaringDocIdSet {
    pub fn new(doc_id_sets: Vec<Option<Box<DocIdSet>>>, cardinality: usize) -> RoaringDocIdSet {
        RoaringDocIdSet {
            doc_id_sets: Arc::new(doc_id_sets),
            cardinality,
        }
    }
}

pub struct RoaringDocIdSetBuilder {
    doc_id_sets: Vec<Option<Box<DocIdSet>>>,
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
    pub fn new(max_doc: i32) -> RoaringDocIdSetBuilder {
        let length = (max_doc + (1 << 16) - 1).unsigned_shift(16);
        let mut doc_id_sets: Vec<Option<Box<DocIdSet>>> = Vec::with_capacity(length as usize);
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

    pub fn cardinality(&self) -> usize {
        self.cardinality
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

                self.doc_id_sets[current_block as usize] = Some(Box::new(ShortArrayDocIdSet::new(
                    docs,
                    current_block_cardinality,
                )));
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
                        *ptr.offset(i as isize) = exclude_doc as u16;
                    }
                }

                assert!(
                    exclude_doc as usize + 1 == dense_buffer.len()
                        || dense_buffer.next_set_bit((exclude_doc + 1) as usize) == NO_MORE_DOCS
                );

                let length = exclude_docs.len();
                self.doc_id_sets[self.current_block as usize] = Some(Box::new(NotDocIdSet::new(
                    Box::new(ShortArrayDocIdSet::new(exclude_docs, length)),
                    BLOCK_SIZE as i32,
                )));
            } else {
                // Neither sparse nor super dense, use a fixed bit set
                let dense_buf = mem::replace(&mut self.dense_buffer, None);
                self.doc_id_sets[self.current_block as usize] = Some(Box::new(BitDocIdSet::new(
                    dense_buf.unwrap(),
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
                *self.buffer
                    .as_mut_ptr()
                    .offset(self.current_block_cardinality as isize) = doc_id as u16
            };
        } else {
            if self.dense_buffer.is_none() {
                // the buffer is full, let's move to a fixed bit set
                let num_bits = (1i32 << 16).min(self.max_doc - (block << 16));
                let mut fixed_bit_set = FixedBitSet::new(num_bits as usize);
                for doc in &self.buffer {
                    fixed_bit_set.set(*doc as usize);
                }

                self.dense_buffer = Some(Box::new(fixed_bit_set));
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

    pub fn build(&mut self) -> RoaringDocIdSet {
        self.flush();
        let doc_id_set = mem::replace(&mut self.doc_id_sets, Vec::new());
        RoaringDocIdSet::new(doc_id_set, self.cardinality)
    }
}

impl DocIdSet for RoaringDocIdSet {
    fn iterator(&self) -> Result<Option<Box<DocIterator>>> {
        if self.cardinality == 0 {
            Ok(None)
        } else {
            Ok(Some(Box::new(RoaringDocIterator::new(
                self.doc_id_sets.clone(),
                self.cardinality,
            ))))
        }
    }
    fn bits(&self) -> Result<Option<ImmutableBitSetRef>> {
        unimplemented!()
    }
}

pub struct RoaringDocIterator {
    doc_id_sets: Arc<Vec<Option<Box<DocIdSet>>>>,
    doc: DocId,
    block: i32,
    cardinality: usize,
    sub: Option<Box<DocIterator>>,
}

impl RoaringDocIterator {
    pub fn new(
        doc_id_sets: Arc<Vec<Option<Box<DocIdSet>>>>,
        cardinality: usize,
    ) -> RoaringDocIterator {
        RoaringDocIterator {
            doc_id_sets,
            doc: -1,
            block: -1,
            cardinality,
            sub: Some(Box::new(EmptyDocIterator::default())),
        }
    }

    fn first_doc_from_next_block(&mut self) -> Result<(DocId)> {
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

pub struct ShortArrayDocIdSet {
    docs: Arc<Vec<u16>>,
    length: usize,
}

impl ShortArrayDocIdSet {
    pub fn new(docs: Vec<u16>, length: usize) -> ShortArrayDocIdSet {
        ShortArrayDocIdSet {
            docs: Arc::new(docs),
            length,
        }
    }
}

impl DocIdSet for ShortArrayDocIdSet {
    fn iterator(&self) -> Result<Option<Box<DocIterator>>> {
        Ok(Some(Box::new(ShortArrayDocIterator::new(
            self.docs.clone(),
            self.length,
        ))))
    }
    fn bits(&self) -> Result<Option<ImmutableBitSetRef>> {
        unimplemented!()
    }
}

pub struct ShortArrayDocIterator {
    docs: Arc<Vec<u16>>,
    length: usize,
    i: i32,
    doc: DocId,
}

impl ShortArrayDocIterator {
    pub fn new(docs: Arc<Vec<u16>>, length: usize) -> ShortArrayDocIterator {
        ShortArrayDocIterator {
            docs,
            length,
            i: -1,
            doc: -1,
        }
    }
}

impl DocIterator for ShortArrayDocIterator {
    fn doc_id(&self) -> DocId {
        self.doc
    }

    fn next(&mut self) -> Result<DocId> {
        self.i += 1;
        if self.i as usize >= self.length {
            Ok(NO_MORE_DOCS)
        } else {
            self.doc = i32::from(unsafe { *self.docs.as_ptr().offset(self.i as isize) });
            Ok(self.doc)
        }
    }

    fn advance(&mut self, target: DocId) -> Result<DocId> {
        self.doc = if (self.i + 1) as usize >= self.length {
            NO_MORE_DOCS
        } else {
            let adv = match self.docs[(self.i + 1) as usize..self.length]
                .binary_search(&(target as u16))
            {
                Ok(x) => x,
                Err(e) => e,
            };
            self.i += (adv + 1) as i32;
            if self.i < self.length as i32 {
                i32::from(unsafe { *self.docs.as_ptr().offset(self.i as isize) })
            } else {
                NO_MORE_DOCS
            }
        };

        Ok(self.doc)
    }

    fn cost(&self) -> usize {
        self.length
    }
}

pub struct NotDocIdSet {
    set: Box<DocIdSet>,
    max_doc: i32,
}

impl NotDocIdSet {
    pub fn new(set: Box<DocIdSet>, max_doc: i32) -> NotDocIdSet {
        NotDocIdSet { set, max_doc }
    }
}

impl DocIdSet for NotDocIdSet {
    fn iterator(&self) -> Result<Option<Box<DocIterator>>> {
        match self.set.iterator()? {
            Some(iter) => Ok(Some(Box::new(NotDocIterator::new(iter, self.max_doc)))),
            _ => Ok(None),
        }
    }
    fn bits(&self) -> Result<Option<ImmutableBitSetRef>> {
        self.set.bits()
    }
}

pub struct NotDocIterator {
    max_doc: i32,
    doc: DocId,
    next_skipped_doc: i32,
    iterator: Box<DocIterator>,
}

impl NotDocIterator {
    pub fn new(iterator: Box<DocIterator>, max_doc: i32) -> NotDocIterator {
        NotDocIterator {
            max_doc,
            doc: -1,
            next_skipped_doc: -1,
            iterator,
        }
    }
}

impl DocIterator for NotDocIterator {
    fn doc_id(&self) -> DocId {
        self.doc
    }

    fn next(&mut self) -> Result<DocId> {
        let adv = self.doc + 1;
        self.advance(adv)
    }

    fn advance(&mut self, target: DocId) -> Result<DocId> {
        self.doc = target;

        if self.doc > self.next_skipped_doc {
            self.next_skipped_doc = self.iterator.advance(self.doc)?;
        }

        loop {
            if self.doc >= self.max_doc {
                self.doc = NO_MORE_DOCS;
                return Ok(self.doc);
            }

            debug_assert!(self.doc <= self.next_skipped_doc);
            if self.doc != self.next_skipped_doc {
                return Ok(self.doc);
            }

            self.doc += 1;
            self.next_skipped_doc = self.iterator.next()?;
        }
    }

    fn cost(&self) -> usize {
        self.max_doc as usize
    }
}
