use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, RwLock};

use core::index::multi_fields::MultiFields;
use core::index::Term;
use core::index::TermContext;
use core::index::{IndexReader, LeafReader};
use core::search::bm25_similarity::BM25Similarity;
use core::search::bulk_scorer::BulkScorer;
use core::search::cache_policy::{QueryCachingPolicy, UsageTrackingQueryCachingPolicy};
use core::search::collector;
use core::search::collector::{Collector, SearchCollector};
use core::search::lru_query_cache::{LRUQueryCache, QueryCache};
use core::search::statistics::{CollectionStatistics, TermStatistics};
use core::search::{Query, Weight, NO_MORE_DOCS, Scorer};
use core::search::{SimScorer, SimWeight, Similarity, SimilarityProducer};
use core::util::bits::BitsRef;
use core::util::thread_pool::{DefaultContext, ThreadPool, ThreadPoolBuilder};
use error::*;

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
///
///

struct DefaultSimilarityProducer;

impl SimilarityProducer for DefaultSimilarityProducer {
    fn create(&self, _field: &str) -> Box<Similarity> {
        Box::new(BM25Similarity::default())
    }
}

pub struct NonScoringSimilarity;

impl Similarity for NonScoringSimilarity {
    fn compute_weight(
        &self,
        _collection_stats: &CollectionStatistics,
        _term_stats: &[TermStatistics],
    ) -> Box<SimWeight> {
        Box::new(NonScoringSimWeight {})
    }
}

impl fmt::Display for NonScoringSimilarity {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "non-scoring")
    }
}

pub struct NonScoringSimWeight;

impl SimWeight for NonScoringSimWeight {
    fn get_value_for_normalization(&self) -> f32 {
        1.0f32
    }

    fn normalize(&mut self, _query_norm: f32, _boost: f32) {}

    fn sim_scorer(&self, _reader: &LeafReader) -> Result<Box<SimScorer>> {
        Ok(Box::new(NonScoringSimScorer {}))
    }
}

pub struct NonScoringSimScorer;

impl SimScorer for NonScoringSimScorer {
    fn score(&mut self, _doc: i32, _freq: f32) -> Result<f32> {
        Ok(0f32)
    }

    fn compute_slop_factor(&self, _distance: i32) -> f32 {
        1.0f32
    }
}

pub struct IndexSearcher {
    pub reader: Arc<IndexReader>,
    sim_producer: Box<SimilarityProducer>,
    query_cache: Box<QueryCache>,
    #[allow(dead_code)]
    cache_policy: Arc<QueryCachingPolicy>,
    collection_statistics: RwLock<HashMap<String, CollectionStatistics>>,
    thread_pool: Option<ThreadPool<DefaultContext>>
}

impl IndexSearcher {
    pub fn new(reader: Arc<IndexReader>) -> IndexSearcher {
        Self::with_similarity(reader, Box::new(DefaultSimilarityProducer {}))
    }

    pub fn with_similarity(
        reader: Arc<IndexReader>,
        sim_producer: Box<SimilarityProducer>,
    ) -> IndexSearcher {
        let max_doc = reader.max_doc();
        IndexSearcher {
            reader,
            sim_producer,
            query_cache: Box::new(LRUQueryCache::new(1000, max_doc)),
            cache_policy: Arc::new(UsageTrackingQueryCachingPolicy::default()),
            collection_statistics: RwLock::new(HashMap::new()),
            thread_pool: None
        }
    }

    pub fn set_thread_pool(&mut self, num_threads: usize) {
        if num_threads > 0 {
            let thread_pool = ThreadPoolBuilder::with_default_factory("search".into())
                .thread_count(num_threads).build();
            self.thread_pool = Some(thread_pool);
        }
    }

    /// Lower-level search API.
    pub fn search(&self, query: &Query, collector: &mut SearchCollector) -> Result<()> {
        let weight = self.create_weight(query, collector.needs_scores())?;

        for (ord, reader) in self.reader.leaves().iter().enumerate() {
            let mut scorer = weight.create_scorer(*reader)?;
            // some in running segment maybe wrong, just skip it!
            // TODO maybe we should matching more specific error type
            if let Err(e) = collector.set_next_reader(ord, *reader) {
                error!("set next reader failed!, {:?}", e);
                continue;
            }
            let live_docs = reader.live_docs();

            Self::do_search(scorer, collector, &live_docs)?;
        }

        Ok(())
    }

    fn do_search<T: Collector + ?Sized>(
        scorer: Box<Scorer>,
        collector: &mut T,
        live_docs: &BitsRef
    ) -> Result<()> {
        let mut scorer = scorer;
        let mut bulk_scorer = BulkScorer::new(scorer.as_mut());
        {
            match bulk_scorer.score(collector, Some(live_docs.as_ref()), 0, NO_MORE_DOCS) {
                Err(Error(
                        ErrorKind::Collector(collector::ErrorKind::CollectionTerminated),
                        _,
                    )) => {
                    // Collection was terminated prematurely
                }
                Err(Error(
                        ErrorKind::Collector(collector::ErrorKind::LeafCollectionTerminated),
                        _,
                    ))
                | Ok(_) => {
                    // Leaf collection was terminated prematurely,
                    // continue with the following leaf
                }
                Err(e) => {
                    // something goes wrong, stop search and return error!
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    pub fn search_parallel(
        &self,
        query: &Query,
        collector: &mut SearchCollector
    ) -> Result<()> {
        if self.reader.leaves().len() > 1 {
            if let Some(ref thread_pool) = self.thread_pool {
                let weight = self.create_weight(query, collector.needs_scores())?;

                for (_ord, reader) in self.reader.leaves().iter().enumerate() {
                    let scorer = weight.create_scorer(*reader)?;
                    let leaf_collector = collector.leaf_collector(*reader);
                    let live_docs = reader.live_docs();
                    thread_pool.execute(move |_ctx| {
                        let mut collector = leaf_collector;
                        if let Err(e) = Self::do_search(scorer, collector.as_mut(), &live_docs) {
                            error!("do search parallel failed by '{:?}', may return partial result", e);
                        }
                        if let Err(e) = collector.finish_leaf() {
                            error!("finish search parallel failed by '{:?}', may return partial result", e);
                        }
                    })
                }
                return collector.finish();
            }
        }
        self.search(query, collector)
    }

    /// Creates a {@link Weight} for the given query, potentially adding caching
    /// if possible and configured.
    pub fn create_weight(&self, query: &Query, needs_scores: bool) -> Result<Box<Weight>> {
        let mut weight = query.create_weight(self, needs_scores)?;
        if !needs_scores {
            weight = self.query_cache
                .do_cache(weight, Arc::clone(&self.cache_policy));
        }
        Ok(weight)
    }

    /// Creates a normalized weight for a top-level `Query`.
    /// The query is rewritten by this method and `Query#createWeight` called,
    /// afterwards the `Weight` is normalized. The returned `Weight`
    /// can then directly be used to get a `Scorer`.
    ///
    pub fn create_normalized_weight(
        &self,
        query: &Query,
        needs_scores: bool,
    ) -> Result<Box<Weight>> {
        let mut weight = self.create_weight(query, needs_scores)?;
        let v = weight.value_for_normalization();
        let mut norm: f32 = self.similarity("", needs_scores).query_norm(v);
        if norm.is_finite() || norm.is_nan() {
            norm = 1.0f32;
        }
        weight.normalize(norm, 1.0f32);
        Ok(weight)
    }

    pub fn similarity(&self, field: &str, needs_scores: bool) -> Box<Similarity> {
        if needs_scores {
            self.sim_producer.create(field)
        } else {
            Box::new(NonScoringSimilarity {})
        }
    }

    pub fn term_statistics(&self, term: Term, context: &TermContext) -> TermStatistics {
        TermStatistics::new(
            term.bytes,
            i64::from(context.doc_freq),
            context.total_term_freq,
        )
    }

    pub fn collections_statistics(&self, field: &str) -> Result<CollectionStatistics> {
        {
            let statistics = self.collection_statistics.read().unwrap();
            if let Some(stat) = statistics.get(field) {
                return Ok(stat.clone());
            }
        }
        // slow path
        let reader = self.reader.as_ref();

        let mut doc_count = 0i32;
        let mut sum_total_term_freq = 0i64;
        let mut sum_doc_freq = 0i64;
        if let Some(terms) = MultiFields::get_terms(reader, field)? {
            doc_count = terms.doc_count()?;
            sum_total_term_freq = terms.sum_total_term_freq()?;
            sum_doc_freq = terms.sum_doc_freq()?;
        }
        let stat = CollectionStatistics::new(
            field.into(),
            i64::from(reader.max_doc()),
            i64::from(doc_count),
            sum_total_term_freq,
            sum_doc_freq,
        );

        let mut statistics = self.collection_statistics.write().unwrap();
        statistics.insert(field.into(), stat);
        Ok(statistics[field].clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::index::tests::*;
    use core::search::collector::early_terminating::*;
    use core::search::collector::top_docs::*;
    use core::search::term_query::TermQuery;
    use core::search::tests::*;
    use core::search::*;
    use core::util::DocId;
    use std::sync::atomic::Ordering;

    pub const MOCK_QUERY: &str = "mock";

    struct MockQuery {
        docs: Vec<DocId>,
    }

    impl MockQuery {
        pub fn new(docs: Vec<DocId>) -> MockQuery {
            MockQuery { docs }
        }
    }

    impl Query for MockQuery {
        fn create_weight(
            &self,
            _searcher: &IndexSearcher,
            _needs_scores: bool,
        ) -> Result<Box<Weight>> {
            Ok(create_mock_weight(self.docs.clone()))
        }

        fn extract_terms(&self) -> Vec<TermQuery> {
            unimplemented!()
        }

        fn query_type(&self) -> &'static str {
            MOCK_QUERY
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
        let index_reader = Arc::new(MockIndexReader::new(vec![
            leaf_reader1,
            leaf_reader2,
            leaf_reader3,
        ]));

        let mut top_collector = TopDocsCollector::new(3);
        {
            let mut early_terminating_collector = EarlyTerminatingSortingCollector::new(
                &mut top_collector, 3);

            let query = MockQuery::new(vec![1, 5, 3, 4, 2]);
            {
                let searcher = IndexSearcher::new(index_reader);
                searcher.search(&query, &mut early_terminating_collector).unwrap();
            }

            assert_eq!(early_terminating_collector.early_terminated.load(Ordering::Relaxed), true);
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
