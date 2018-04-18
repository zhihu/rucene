use std::sync::Arc;

use core::index::multi_fields::MultiFields;
use core::index::IndexReader;
use core::index::Term;
use core::index::TermContext;
use core::search::bulk_scorer::BulkScorer;
use core::search::collector;
use core::search::collector::Collector;
use core::search::statistics::{CollectionStatistics, TermStatistics};
use core::search::SimilarityEnum;
use core::search::{Query, NO_MORE_DOCS};
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
const DEFAULT_SIMILARITY_ENUM: SimilarityEnum = SimilarityEnum::BM25 { k1: 1.2, b: 0.75 };

pub struct IndexSearcher {
    pub reader: Arc<IndexReader>,
    sim_enum: SimilarityEnum,
}

impl IndexSearcher {
    pub fn new(reader: Arc<IndexReader>) -> IndexSearcher {
        IndexSearcher {
            reader,
            sim_enum: DEFAULT_SIMILARITY_ENUM,
        }
    }

    /// Lower-level search API.
    ///
    /// `LeafCollector::collect(DocId)` is called for every matching document.
    pub fn search(&self, query: &Query, collector: &mut Collector) -> Result<()> {
        let weight = query.create_weight(self, collector.need_scores())?;

        for (ord, reader) in self.reader.leaves().iter().enumerate() {
            let mut scorer = weight.create_scorer(*reader)?;
            let mut bulk_scorer = BulkScorer::new(scorer.as_mut());
            {
                // some in running segment maybe wrong, just skip it!
                // TODO maybe we should matching more specific error type
                if let Err(e) = collector.set_next_reader(ord, *reader) {
                    error!("set next reader failed!, {:?}", e);
                    continue;
                }

                let live_docs = reader.live_docs();
                match bulk_scorer.score(collector, &live_docs, 0, NO_MORE_DOCS) {
                    Err(Error(
                        ErrorKind::Collector(collector::ErrorKind::CollectionTerminated),
                        _,
                    )) => {
                        // Collection was terminated prematurely
                        break;
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
        }

        Ok(())
    }

    pub fn similarity(&self) -> SimilarityEnum {
        self.sim_enum
    }

    pub fn term_statistics(&self, term: Term, context: &TermContext) -> TermStatistics {
        TermStatistics::new(
            term.bytes,
            i64::from(context.doc_freq),
            context.total_term_freq,
        )
    }

    pub fn collections_statistics(&self, field: String) -> Result<CollectionStatistics> {
        let reader = self.reader.as_ref();

        let mut doc_count = 0i32;
        let mut sum_total_term_freq = 0i64;
        let mut sum_doc_freq = 0i64;
        if let Some(terms) = MultiFields::get_terms(reader, &field)? {
            doc_count = terms.doc_count()?;
            sum_total_term_freq = terms.sum_total_term_freq()?;
            sum_doc_freq = terms.sum_doc_freq()?;
        }
        Ok(CollectionStatistics::new(
            field,
            i64::from(reader.max_doc()),
            i64::from(doc_count),
            sum_total_term_freq,
            sum_doc_freq,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::index::tests::*;
    use core::search::collector::chain::*;
    use core::search::collector::early_terminating::*;
    use core::search::collector::top_docs::*;
    use core::search::tests::*;
    use core::search::*;

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
        let mut early_terminating_collector = EarlyTerminatingSortingCollector::new(3);

        let query = MockQuery::new(vec![1, 5, 3, 4, 2]);
        {
            let searcher = IndexSearcher::new(index_reader);
            let mut collector =
                ChainedCollector::new(vec![&mut top_collector, &mut early_terminating_collector]);
            searcher.search(&query, &mut collector).unwrap();
        }

        assert_eq!(early_terminating_collector.early_terminated, true);

        let top_docs = top_collector.top_docs();
        assert_eq!(top_docs.total_hits(), 9);

        let score_docs = top_docs.score_docs();
        assert_eq!(score_docs.len(), 3);
        assert!((score_docs[0].score() - 5f32) < ::std::f32::EPSILON);
        assert!((score_docs[1].score() - 5f32) < ::std::f32::EPSILON);
        assert!((score_docs[2].score() - 5f32) < ::std::f32::EPSILON);
    }
}
