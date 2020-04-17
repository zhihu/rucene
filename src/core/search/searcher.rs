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

use std::collections::HashMap;
use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

use crossbeam::channel::{unbounded, Receiver, Sender};

use core::codec::postings::FieldsProducer;
use core::codec::{Codec, Terms};
use core::codec::{TermIterator, TermState};
use core::doc::{IndexOptions, Term};
use core::index::reader::{IndexReader, LeafReaderContext, LeafReaderContextPtr, SearchLeafReader};
use core::search::cache::{
    LRUQueryCache, QueryCache, QueryCachingPolicy, UsageTrackingQueryCachingPolicy,
};
use core::search::collector::{self, Collector, ParallelLeafCollector, SearchCollector};
use core::search::explanation::Explanation;
use core::search::query::{ConstantScoreQuery, MatchAllDocsQuery, Query, TermQuery, Weight};
use core::search::scorer::{BulkScorer, Scorer};
use core::search::similarity::{
    BM25Similarity, SimScorer, SimWeight, Similarity, SimilarityProducer,
};
use core::search::statistics::{CollectionStatistics, TermStatistics};
use core::search::NO_MORE_DOCS;
use core::util::external::{DefaultContext, ThreadPool, ThreadPoolBuilder};
use core::util::{Bits, DocId, KeyedContext};

use error::{Error, ErrorKind, Result};

const MAX_DOCS_PER_SLICE: i32 = 250_000;
const MAX_SEGMENTS_PER_SLICE: i32 = 20;
const MIN_PARALLEL_SLICES: i32 = 3;

const DEFAULT_DISMATCH_NEXT_LIMIT: usize = 500_000;

pub struct TermContext<S: TermState> {
    pub doc_freq: i32,
    pub total_term_freq: i64,
    pub states: Vec<(DocId, S)>,
}

impl<S: TermState> TermContext<S> {
    pub fn new<TI, Tm, FP, C, IR>(reader: &IR) -> TermContext<S>
    where
        TI: TermIterator<TermState = S>,
        Tm: Terms<Iterator = TI>,
        FP: FieldsProducer<Terms = Tm> + Clone,
        C: Codec<FieldsProducer = FP>,
        IR: IndexReader<Codec = C> + ?Sized,
    {
        let doc_freq = 0;
        let total_term_freq = 0;
        let states = Vec::with_capacity(reader.leaves().len());
        TermContext {
            doc_freq,
            total_term_freq,
            states,
        }
    }

    pub fn build<TI, Tm, FP, C, IR>(&mut self, reader: &IR, term: &Term) -> Result<()>
    where
        TI: TermIterator<TermState = S>,
        Tm: Terms<Iterator = TI>,
        FP: FieldsProducer<Terms = Tm> + Clone,
        C: Codec<FieldsProducer = FP>,
        IR: IndexReader<Codec = C> + ?Sized,
    {
        for reader in reader.leaves() {
            if let Some(terms) = reader.reader.terms(&term.field)? {
                let mut terms_enum = terms.iterator()?;
                if terms_enum.seek_exact(&term.bytes)? {
                    // TODO add TermStates if someone need it
                    let doc_freq = terms_enum.doc_freq()?;
                    let total_term_freq = terms_enum.total_term_freq()?;
                    self.accumulate_statistics(doc_freq, total_term_freq as i64);
                    self.states
                        .push((reader.doc_base(), terms_enum.term_state()?));
                }
            }
        }

        Ok(())
    }

    fn accumulate_statistics(&mut self, doc_freq: i32, total_term_freq: i64) {
        self.doc_freq += doc_freq;
        if self.total_term_freq >= 0 && total_term_freq >= 0 {
            self.total_term_freq += total_term_freq
        } else {
            self.total_term_freq = -1
        }
    }

    pub fn get_term_state<C: Codec>(&self, reader: &LeafReaderContext<'_, C>) -> Option<&S> {
        for (doc_base, state) in &self.states {
            if *doc_base == reader.doc_base {
                return Some(state);
            }
        }
        None
    }

    pub fn term_states(&self) -> HashMap<DocId, S> {
        let mut term_states = HashMap::new();
        for (doc_base, term_state) in &self.states {
            term_states.insert(*doc_base, term_state.clone());
        }

        term_states
    }
}

/// Implements search over a single IndexReader.
///
/// For performance reasons, if your index is unchanging, you
/// should share a single IndexSearcher instance across
/// multiple searches instead of creating a new one
/// per-search.  If your index has changed and you wish to
/// see the changes reflected in searching, you should
/// use `DirectoryReader::open`
/// to obtain a new reader and
/// then create a new IndexSearcher from that.  Also, for
/// low-latency turnaround it's best to use a near-real-time
/// reader.
/// Once you have a new `IndexReader`, it's relatively
/// cheap to create a new IndexSearcher from it.
///
/// *NOTE:* `IndexSearcher` instances are completely
/// thread safe, meaning multiple threads can call any of its
/// methods, concurrently.  If your application requires
/// external synchronization, you should *not*
/// synchronize on the `IndexSearcher` instance.

pub struct DefaultSimilarityProducer;

impl<C: Codec> SimilarityProducer<C> for DefaultSimilarityProducer {
    fn create(&self, _field: &str) -> Box<dyn Similarity<C>> {
        Box::new(BM25Similarity::default())
    }
}

/// A search-time `Similarity` that does not make use of scoring factors
/// and may be used when scores are not needed.
pub struct NonScoringSimilarity;

impl<C: Codec> Similarity<C> for NonScoringSimilarity {
    fn compute_weight(
        &self,
        _collection_stats: &CollectionStatistics,
        _term_stats: &[TermStatistics],
        _context: Option<&KeyedContext>,
        _boost: f32,
    ) -> Box<dyn SimWeight<C>> {
        Box::new(NonScoringSimWeight {})
    }
}

impl fmt::Display for NonScoringSimilarity {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "non-scoring")
    }
}

struct NonScoringSimWeight;

impl<C: Codec> SimWeight<C> for NonScoringSimWeight {
    fn get_value_for_normalization(&self) -> f32 {
        1.0f32
    }

    fn normalize(&mut self, _query_norm: f32, _boost: f32) {}

    fn sim_scorer(&self, _reader: &SearchLeafReader<C>) -> Result<Box<dyn SimScorer>> {
        Ok(Box::new(NonScoringSimScorer {}))
    }
}

struct NonScoringSimScorer;

impl SimScorer for NonScoringSimScorer {
    fn score(&mut self, _doc: i32, _freq: f32) -> Result<f32> {
        Ok(0f32)
    }

    fn compute_slop_factor(&self, _distance: i32) -> f32 {
        1.0f32
    }
}

/// trait that used for build `Weight` and `Similarity` for `Query`.
pub trait SearchPlanBuilder<C: Codec> {
    /// num docs of the reader in searcher, same as IndexSearcher::reader()::num_docs()
    fn num_docs(&self) -> i32;

    /// max doc of the reader in searcher, same as IndexSearcher::reader()::max_doc()
    fn max_doc(&self) -> i32;

    /// Creates a `Weight` for the given query, potentially adding caching
    /// if possible and configured.
    fn create_weight(&self, query: &dyn Query<C>, needs_scores: bool)
        -> Result<Box<dyn Weight<C>>>;

    /// Creates a normalized weight for a top-level `Query`.
    /// The query is rewritten by this method and `Query#createWeight` called,
    /// afterwards the `Weight` is normalized. The returned `Weight`
    /// can then directly be used to get a `Scorer`.
    fn create_normalized_weight(
        &self,
        query: &dyn Query<C>,
        needs_scores: bool,
    ) -> Result<Box<dyn Weight<C>>>;

    fn similarity(&self, field: &str, needs_scores: bool) -> Box<dyn Similarity<C>>;

    fn term_statistics(&self, term: &Term) -> Result<TermStatistics>;

    fn collections_statistics(&self, field: &str) -> Option<&CollectionStatistics>;
}

pub trait IndexSearcher<C: Codec>: SearchPlanBuilder<C> {
    type Reader: IndexReader<Codec = C> + ?Sized;
    fn reader(&self) -> &Self::Reader;

    fn search<S>(&self, query: &dyn Query<C>, collector: &mut S) -> Result<()>
    where
        S: SearchCollector;

    fn search_parallel<S>(&self, query: &dyn Query<C>, collector: &mut S) -> Result<()>
    where
        S: SearchCollector;

    fn count(&self, query: &dyn Query<C>) -> Result<i32>;

    fn explain(&self, query: &dyn Query<C>, doc: DocId) -> Result<Explanation>;
}

///  Implements search over a single IndexReader.
///
/// Applications usually need only call the inherited
/// `search(Query,Collector)` method. For
/// performance reasons, if your index is unchanging, you
/// should share a single IndexSearcher instance across
/// multiple searches instead of creating a new one
/// per-search.  If your index has changed and you wish to
/// see the changes reflected in searching, you should
/// use `StandardDirectoryReader#openIfChanged()`
/// to obtain a new reader and then create a new IndexSearcher from that.
/// Also, for low-latency turnaround it's best to use a near-real-time
/// reader ({@link DirectoryReader#open(IndexWriter)}).
/// Once you have a new `IndexReader`, it's relatively
/// cheap to create a new IndexSearcher from it.
pub struct DefaultIndexSearcher<
    C: Codec,
    R: IndexReader<Codec = C> + ?Sized,
    IR: Deref<Target = R>,
    SP: SimilarityProducer<C>,
> {
    reader: IR,
    thread_pool: Option<Arc<ThreadPool<DefaultContext>>>,
    // used for concurrent search - each slice holds a set of LeafReader's ord that
    // executed within one thread.
    leaf_ord_slices: Vec<Vec<usize>>,

    query_cache: Arc<dyn QueryCache<C>>,
    cache_policy: Arc<dyn QueryCachingPolicy<C>>,

    sim_producer: SP,
    collection_statistics: HashMap<String, CollectionStatistics>,

    // dismatch next limit to break.
    next_limit: usize,
}

impl<C: Codec, R: IndexReader<Codec = C> + ?Sized, IR: Deref<Target = R>>
    DefaultIndexSearcher<C, R, IR, DefaultSimilarityProducer>
{
    pub fn new(
        reader: IR,
        next_limit: Option<usize>,
    ) -> DefaultIndexSearcher<C, R, IR, DefaultSimilarityProducer> {
        Self::with_similarity(reader, DefaultSimilarityProducer {}, next_limit)
    }
}

impl<C, R, IR, SP> DefaultIndexSearcher<C, R, IR, SP>
where
    C: Codec,
    R: IndexReader<Codec = C> + ?Sized,
    IR: Deref<Target = R>,
    SP: SimilarityProducer<C>,
{
    pub fn with_similarity(
        reader: IR,
        sim_producer: SP,
        next_limit: Option<usize>,
    ) -> DefaultIndexSearcher<C, R, IR, SP> {
        let mut leaves = reader.leaves();
        leaves.sort_by(|l1, l2| l2.reader.max_doc().cmp(&l1.reader.max_doc()));

        let mut collection_statistics: HashMap<String, CollectionStatistics> = HashMap::new();

        for leaf_reader in leaves.iter() {
            for (field_name, field_info) in &leaf_reader.reader.field_infos().by_name {
                // use top-max-doc segment instead.
                if field_info.index_options == IndexOptions::Null
                    || collection_statistics.contains_key(field_name)
                {
                    continue;
                }

                let mut doc_count = 0i32;
                let mut sum_doc_freq = 0i64;
                let mut sum_total_term_freq = 0i64;

                if let Ok(Some(terms)) = leaf_reader.reader.terms(field_name) {
                    if let Ok(dc) = terms.doc_count() {
                        doc_count = dc;
                    }
                    if let Ok(s) = terms.sum_doc_freq() {
                        sum_doc_freq = s;
                    }
                    if let Ok(s) = terms.sum_total_term_freq() {
                        sum_total_term_freq = s;
                    }
                }
                let field_stat = CollectionStatistics::new(
                    field_name.clone(),
                    leaf_reader.doc_base(),
                    reader.max_doc() as i64,
                    doc_count as i64,
                    sum_total_term_freq,
                    sum_doc_freq,
                );

                collection_statistics.insert(field_name.clone(), field_stat);
            }
        }

        DefaultIndexSearcher {
            reader,
            sim_producer,
            query_cache: Arc::new(LRUQueryCache::new(1000)),
            cache_policy: Arc::new(UsageTrackingQueryCachingPolicy::default()),
            collection_statistics,
            thread_pool: None,
            leaf_ord_slices: vec![],
            next_limit: next_limit.unwrap_or(DEFAULT_DISMATCH_NEXT_LIMIT),
        }
    }

    pub fn with_thread_pool(&mut self, num_threads: usize) {
        // at least 2 thread to support parallel
        if num_threads > 1 {
            let thread_pool = ThreadPoolBuilder::with_default_factory("search".into())
                .thread_count(num_threads)
                .build();
            self.set_thread_pool(Arc::new(thread_pool));
        }
    }

    pub fn set_thread_pool(&mut self, pool: Arc<ThreadPool<DefaultContext>>) {
        self.thread_pool = Some(pool);
        self.leaf_ord_slices = Self::slice(
            self.reader.leaves(),
            MAX_DOCS_PER_SLICE,
            MAX_SEGMENTS_PER_SLICE,
            MIN_PARALLEL_SLICES,
        );
    }

    pub fn set_query_cache(&mut self, cache: Arc<dyn QueryCache<C>>) {
        self.query_cache = cache;
    }

    pub fn set_query_cache_policy(&mut self, cache_policy: Arc<dyn QueryCachingPolicy<C>>) {
        self.cache_policy = cache_policy;
    }

    fn do_search<S: Scorer + ?Sized, T: Collector, B: Bits + ?Sized>(
        scorer: &mut S,
        collector: &mut T,
        live_docs: &B,
        next_limit: usize,
    ) -> Result<()> {
        let mut bulk_scorer = BulkScorer::new(scorer);
        match bulk_scorer.score(collector, Some(live_docs), 0, NO_MORE_DOCS, next_limit) {
            Err(Error(ErrorKind::Collector(collector::ErrorKind::CollectionTerminated), _)) => {
                // Collection was terminated prematurely
                Ok(())
            }
            Err(Error(ErrorKind::Collector(collector::ErrorKind::LeafCollectionTerminated), _))
            | Ok(_) => {
                // Leaf collection was terminated prematurely,
                // continue with the following leaf
                Ok(())
            }
            Err(e) => {
                // something goes wrong, stop search and return error!
                Err(e)
            }
        }
    }

    // segregate leaf readers amongst multiple slices
    fn slice(
        mut leaves: Vec<LeafReaderContext<'_, C>>,
        max_docs_per_slice: i32,
        max_segments_per_slice: i32,
        min_parallel_slices: i32,
    ) -> Vec<Vec<usize>> {
        if leaves.is_empty() {
            return vec![];
        }

        // reverse order by leaf reader max doc
        leaves.sort_by(|l1, l2| l1.reader.max_doc().cmp(&l2.reader.max_doc()));

        let mut slices = vec![];
        let mut doc_sum = 0;
        let mut ords = vec![];

        if leaves.len() <= min_parallel_slices as usize {
            for ctx in &leaves {
                slices.push(vec![ctx.ord]);
            }
            return slices;
        }

        let mut total_docs = 0;
        for ctx in &leaves {
            total_docs += ctx.reader.max_doc();
        }

        let reserved = max_docs_per_slice.min(total_docs / min_parallel_slices);

        for ctx in &leaves {
            let max_doc = ctx.reader.max_doc();
            if max_doc >= reserved {
                slices.push(vec![ctx.ord]);
            } else {
                if doc_sum + max_doc > reserved || ords.len() >= max_segments_per_slice as usize {
                    ords.sort();
                    slices.push(ords);
                    ords = vec![];
                    doc_sum = 0;
                }
                ords.push(ctx.ord);
                doc_sum += ctx.reader.max_doc();
            }
        }
        if !ords.is_empty() {
            ords.sort();
            slices.push(ords);
        }
        slices
    }
}

impl<C, R, IR, SP> IndexSearcher<C> for DefaultIndexSearcher<C, R, IR, SP>
where
    C: Codec,
    R: IndexReader<Codec = C> + ?Sized,
    IR: Deref<Target = R>,
    SP: SimilarityProducer<C>,
{
    type Reader = R;
    #[inline]
    fn reader(&self) -> &R {
        &*self.reader
    }

    /// Lower-level search API.
    fn search<S>(&self, query: &dyn Query<C>, collector: &mut S) -> Result<()>
    where
        S: SearchCollector,
    {
        let weight = self.create_weight(query, collector.needs_scores())?;

        for reader in self.reader.leaves() {
            if let Some(mut scorer) = weight.create_scorer(&reader)? {
                // some in running segment maybe wrong, just skip it!
                // TODO maybe we should matching more specific error type
                if let Err(e) = collector.set_next_reader(&reader) {
                    error!(
                        "set next reader for leaf {} failed!, {:?}",
                        reader.reader.name(),
                        e
                    );
                    continue;
                }
                let live_docs = reader.reader.live_docs();

                match Self::do_search(&mut *scorer, collector, live_docs.as_ref(), self.next_limit)
                {
                    Ok(()) => {}
                    Err(Error(
                        ErrorKind::Collector(collector::ErrorKind::CollectionTimeout),
                        _,
                    )) => {
                        // Collection timeout, we must terminate the search
                        break;
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            }
        }

        Ok(())
    }

    fn search_parallel<S>(&self, query: &dyn Query<C>, collector: &mut S) -> Result<()>
    where
        S: SearchCollector,
    {
        if collector.support_parallel() && self.leaf_ord_slices.len() > 1 {
            debug_assert!(self.thread_pool.is_some());
            let thread_pool = self.thread_pool.as_ref().unwrap();
            let weight = self.create_weight(query, collector.needs_scores())?;
            let leaf_readers = self.reader.leaves();

            collector.init_parallel();

            for leaf_slice in &self.leaf_ord_slices {
                let mut scorer_and_collectors = vec![];

                for ord in leaf_slice.iter() {
                    let leaf_ctx = &leaf_readers[*ord];
                    match collector.leaf_collector(leaf_ctx) {
                        Ok(leaf_collector) => {
                            let w = &weight as *const Box<dyn Weight<C>> as u64;
                            let reader = unsafe { ::std::mem::transmute(leaf_ctx.reader) };
                            let parent = unsafe { ::std::mem::transmute(leaf_ctx.parent) };
                            let leaf_ctx_ptr = LeafReaderContextPtr::new(
                                leaf_ctx.ord,
                                leaf_ctx.doc_base,
                                reader,
                                parent,
                            );
                            scorer_and_collectors.push((w, leaf_ctx_ptr, leaf_collector));
                        }
                        Err(e) => {
                            error!(
                                "create leaf collector for leaf {} failed with '{:?}'",
                                leaf_ctx.reader.name(),
                                e
                            );
                        }
                    }
                }

                if !scorer_and_collectors.is_empty() {
                    let next_limit = self.next_limit;

                    thread_pool.execute(move |_| {
                        for (w, leaf_ctx_ptr, mut collector) in scorer_and_collectors {
                            let weight = unsafe { &*(w as *const Box<dyn Weight<C>>) };
                            let reader = unsafe { &(*leaf_ctx_ptr.reader) };
                            let parent = unsafe { &(*leaf_ctx_ptr.parent) };
                            let leaf_ctx = LeafReaderContext::new(
                                parent,
                                reader,
                                leaf_ctx_ptr.ord,
                                leaf_ctx_ptr.doc_base,
                            );

                            if let Some(mut scorer) =
                                weight.create_scorer(&leaf_ctx).unwrap_or(None)
                            {
                                let live_docs = leaf_ctx.reader.live_docs();

                                let should_terminate = match Self::do_search(
                                    scorer.as_mut(),
                                    &mut collector,
                                    live_docs.as_ref(),
                                    next_limit,
                                ) {
                                    Ok(()) => false,
                                    Err(Error(
                                        ErrorKind::Collector(
                                            collector::ErrorKind::CollectionTimeout,
                                        ),
                                        _,
                                    )) => {
                                        // Collection timeout, we must terminate the search
                                        true
                                    }
                                    Err(e) => {
                                        error!(
                                            "do search parallel failed by '{:?}', may return \
                                             partial result",
                                            e
                                        );
                                        true
                                    }
                                };
                                if let Err(e) = collector.finish_leaf() {
                                    error!(
                                        "finish search parallel failed by '{:?}', may return \
                                         partial result",
                                        e
                                    );
                                }
                                if should_terminate {
                                    break;
                                }
                            }
                        }
                    });
                }
            }
            return collector.finish_parallel();
        }
        self.search(query, collector)
    }

    fn count(&self, query: &dyn Query<C>) -> Result<i32> {
        let mut query = query;
        while let Some(constant_query) = query.as_any().downcast_ref::<ConstantScoreQuery<C>>() {
            query = constant_query.get_raw_query();
        }

        if query.as_any().downcast_ref::<MatchAllDocsQuery>().is_some() {
            return Ok(self.reader().num_docs());
        } else if let Some(term_query) = query.as_any().downcast_ref::<TermQuery>() {
            if !self.reader().has_deletions() {
                let term = &term_query.term;
                let mut count = 0;
                for leaf in self.reader().leaves() {
                    count += leaf.reader.doc_freq(term)?;
                }
                return Ok(count);
            }
        }

        let mut collector = TotalHitCountCollector::new();
        self.search_parallel(query, &mut collector)?;
        Ok(collector.total_hits())
    }

    fn explain(&self, query: &dyn Query<C>, doc: DocId) -> Result<Explanation> {
        let reader = self.reader.leaf_reader_for_doc(doc);
        let live_docs = reader.reader.live_docs();
        if !live_docs.get((doc - reader.doc_base()) as usize)? {
            Ok(Explanation::new(
                false,
                0.0f32,
                format!("Document {} if deleted", doc),
                vec![],
            ))
        } else {
            self.create_normalized_weight(query, true)?
                .explain(&reader, doc - reader.doc_base())
        }
    }
}

impl<C, R, IR, SP> SearchPlanBuilder<C> for DefaultIndexSearcher<C, R, IR, SP>
where
    C: Codec,
    R: IndexReader<Codec = C> + ?Sized,
    IR: Deref<Target = R>,
    SP: SimilarityProducer<C>,
{
    fn num_docs(&self) -> i32 {
        self.reader.num_docs()
    }

    fn max_doc(&self) -> i32 {
        self.reader.max_doc()
    }

    /// Creates a {@link Weight} for the given query, potentially adding caching
    /// if possible and configured.
    fn create_weight(
        &self,
        query: &dyn Query<C>,
        needs_scores: bool,
    ) -> Result<Box<dyn Weight<C>>> {
        let mut weight = query.create_weight(self, needs_scores)?;
        // currently not to use query_cache.
        if false && !needs_scores {
            weight = self
                .query_cache
                .do_cache(weight, Arc::clone(&self.cache_policy));
        }
        Ok(weight)
    }

    /// Creates a normalized weight for a top-level `Query`.
    /// The query is rewritten by this method and `Query#createWeight` called,
    /// afterwards the `Weight` is normalized. The returned `Weight`
    /// can then directly be used to get a `Scorer`.
    fn create_normalized_weight(
        &self,
        query: &dyn Query<C>,
        needs_scores: bool,
    ) -> Result<Box<dyn Weight<C>>> {
        let weight = self.create_weight(query, needs_scores)?;
        //        let v = weight.value_for_normalization();
        //        let mut norm: f32 = self.similarity("", needs_scores).query_norm(v, None);
        //        if norm.is_finite() || norm.is_nan() {
        //            norm = 1.0f32;
        //        }
        //        weight.normalize(norm, 1.0f32);
        Ok(weight)
    }

    fn similarity(&self, field: &str, needs_scores: bool) -> Box<dyn Similarity<C>> {
        if needs_scores {
            self.sim_producer.create(field)
        } else {
            Box::new(NonScoringSimilarity {})
        }
    }

    fn term_statistics(&self, term: &Term) -> Result<TermStatistics> {
        let doc_base = if let Some(field_stat) = self.collection_statistics.get(&term.field) {
            field_stat.doc_base
        } else {
            return Ok(TermStatistics::new(
                term.bytes.clone(),
                self.reader.max_doc() as i64,
                -1,
            ));
        };

        let mut doc_freq = 0;
        let mut total_term_freq = 0;

        for leaf_reader in self.reader.leaves() {
            if leaf_reader.doc_base() < doc_base {
                continue;
            } else if leaf_reader.doc_base() > doc_base {
                break;
            }

            if let Some(terms) = leaf_reader.reader.terms(&term.field)? {
                let mut terms_enum = terms.iterator()?;
                if terms_enum.seek_exact(&term.bytes)? {
                    doc_freq = terms_enum.doc_freq()?;
                    total_term_freq = terms_enum.total_term_freq()?;
                }
            }
        }

        Ok(TermStatistics::new(
            term.bytes.clone(),
            doc_freq as i64,
            total_term_freq,
        ))
    }

    fn collections_statistics(&self, field: &str) -> Option<&CollectionStatistics> {
        self.collection_statistics.get(field)
    }
}

struct TotalHitCountCollector {
    total_hits: i32,
    channel: Option<(Sender<i32>, Receiver<i32>)>,
}

impl TotalHitCountCollector {
    pub fn new() -> Self {
        TotalHitCountCollector {
            total_hits: 0,
            channel: None,
        }
    }

    pub fn total_hits(&self) -> i32 {
        self.total_hits
    }
}

impl SearchCollector for TotalHitCountCollector {
    type LC = TotalHitsCountLeafCollector;
    fn set_next_reader<C: Codec>(&mut self, _reader: &LeafReaderContext<'_, C>) -> Result<()> {
        Ok(())
    }

    fn support_parallel(&self) -> bool {
        true
    }

    fn init_parallel(&mut self) {
        if self.channel.is_none() {
            self.channel = Some(unbounded());
        }
    }

    fn leaf_collector<C: Codec>(
        &self,
        _reader: &LeafReaderContext<'_, C>,
    ) -> Result<TotalHitsCountLeafCollector> {
        Ok(TotalHitsCountLeafCollector {
            count: 0,
            sender: self.channel.as_ref().unwrap().0.clone(),
        })
    }

    fn finish_parallel(&mut self) -> Result<()> {
        let channel = self.channel.take();
        // iff all the `weight.create_scorer(leaf_reader)` return None, the channel won't
        // inited and thus stay None
        if let Some((sender, receiver)) = channel {
            drop(sender);
            while let Ok(v) = receiver.recv() {
                self.total_hits += v;
            }
        }

        Ok(())
    }
}

impl Collector for TotalHitCountCollector {
    fn needs_scores(&self) -> bool {
        false
    }

    fn collect<S: Scorer + ?Sized>(&mut self, _doc: i32, _scorer: &mut S) -> Result<()> {
        self.total_hits += 1;
        Ok(())
    }
}

struct TotalHitsCountLeafCollector {
    count: i32,
    sender: Sender<i32>,
}

impl Collector for TotalHitsCountLeafCollector {
    fn needs_scores(&self) -> bool {
        false
    }

    fn collect<S: Scorer + ?Sized>(&mut self, _doc: i32, _scorer: &mut S) -> Result<()> {
        self.count += 1;
        Ok(())
    }
}

impl ParallelLeafCollector for TotalHitsCountLeafCollector {
    fn finish_leaf(&mut self) -> Result<()> {
        self.sender.send(self.count).map_err(|e| {
            ErrorKind::IllegalState(format!(
                "channel unexpected closed before search complete with err: {:?}",
                e
            ))
            .into()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::codec::tests::TestCodec;
    use core::index::tests::*;
    use core::search::collector::*;
    use core::search::query::TermQuery;
    use core::search::tests::*;
    use core::util::DocId;

    struct MockQuery {
        docs: Vec<DocId>,
    }

    impl MockQuery {
        pub fn new(docs: Vec<DocId>) -> MockQuery {
            MockQuery { docs }
        }
    }

    impl<C: Codec> Query<C> for MockQuery {
        fn create_weight(
            &self,
            _searcher: &dyn SearchPlanBuilder<C>,
            _needs_scores: bool,
        ) -> Result<Box<dyn Weight<C>>> {
            Ok(Box::new(create_mock_weight(self.docs.clone())))
        }

        fn extract_terms(&self) -> Vec<TermQuery> {
            unimplemented!()
        }

        fn as_any(&self) -> &dyn (::std::any::Any) {
            unreachable!()
        }
    }

    impl fmt::Display for MockQuery {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "MockQuery")
        }
    }

    #[test]
    fn test_early_terminating_search() {
        let leaf_reader1 = MockLeafReader::new(0);
        let leaf_reader2 = MockLeafReader::new(10);
        let leaf_reader3 = MockLeafReader::new(20);
        let index_reader: Arc<dyn IndexReader<Codec = TestCodec>> =
            Arc::new(MockIndexReader::new(vec![
                leaf_reader1,
                leaf_reader2,
                leaf_reader3,
            ]));

        let mut top_collector = TopDocsCollector::new(3);
        {
            let mut early_terminating_collector = EarlyTerminatingSortingCollector::new(3);
            {
                let mut chained_collector =
                    ChainedCollector::new(&mut early_terminating_collector, &mut top_collector);
                let query = MockQuery::new(vec![1, 5, 3, 4, 2]);
                {
                    let searcher = DefaultIndexSearcher::new(index_reader, None);
                    searcher.search(&query, &mut chained_collector).unwrap();
                }
            }

            assert_eq!(early_terminating_collector.early_terminated(), true);
        }

        let top_docs = top_collector.top_docs();
        assert_eq!(top_docs.total_hits(), 9);

        let score_docs = top_docs.score_docs();
        assert_eq!(score_docs.len(), 3);
        assert!((score_docs[0].score() - 5f32) < ::std::f32::EPSILON);
        assert!((score_docs[1].score() - 5f32) < ::std::f32::EPSILON);
        assert!((score_docs[2].score() - 5f32) < ::std::f32::EPSILON);
    }
}
