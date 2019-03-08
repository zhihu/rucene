use core::index::LeafReader;
use core::search::explanation::Explanation;
use core::search::searcher::IndexSearcher;
use core::search::term_query::TermQuery;
use core::search::{two_phase_next, DocIterator, FeatureResult};
use core::search::{Query, Scorer, Weight};
use core::util::context::IndexedContext;
use core::util::DocId;
use error::Result;

use std::fmt;
use std::sync::Arc;

const FILTER_QUERY: &str = "filter_query";

pub trait FilterFunction: fmt::Display {
    fn leaf_function(&self, leaf_reader: &LeafReader) -> Result<Box<LeafFilterFunction>>;
}

pub trait LeafFilterFunction: Send + Sync {
    fn matches(&mut self, doc_id: DocId) -> Result<bool>;
}

pub struct FilterQuery {
    query: Box<Query>,
    filters: Vec<Arc<FilterFunction>>,
}

impl FilterQuery {
    pub fn new(query: Box<Query>, filters: Vec<Arc<FilterFunction>>) -> Self {
        FilterQuery { query, filters }
    }
}

impl Query for FilterQuery {
    fn create_weight(&self, searcher: &IndexSearcher, needs_scores: bool) -> Result<Box<Weight>> {
        let mut filters = Vec::with_capacity(self.filters.len());
        for f in &self.filters {
            filters.push(Arc::clone(f));
        }
        Ok(Box::new(FilterWeight {
            weight: self.query.create_weight(searcher, needs_scores)?,
            filters,
        }))
    }

    fn extract_terms(&self) -> Vec<TermQuery> {
        self.query.extract_terms()
    }

    fn query_type(&self) -> &'static str {
        FILTER_QUERY
    }

    fn as_any(&self) -> &::std::any::Any {
        self
    }
}

impl fmt::Display for FilterQuery {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let filters_fmt: Vec<String> = self
            .filters
            .as_slice()
            .iter()
            .map(|q| format!("{}", q))
            .collect();
        write!(
            f,
            "FilterQuery(query: {}, filter: {})",
            &self.query,
            filters_fmt.join(", ")
        )
    }
}

struct FilterWeight {
    weight: Box<Weight>,
    filters: Vec<Arc<FilterFunction>>,
}

impl Weight for FilterWeight {
    fn create_scorer(&self, reader: &LeafReader) -> Result<Box<Scorer>> {
        let scorer = self.weight.create_scorer(reader)?;
        let mut filters = Vec::with_capacity(self.filters.len());
        for filter in &self.filters {
            filters.push(filter.leaf_function(reader)?);
        }
        Ok(Box::new(FilterScorer { scorer, filters }))
    }

    fn query_type(&self) -> &'static str {
        FILTER_QUERY
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
        self.weight.explain(reader, doc)
    }
}

impl fmt::Display for FilterWeight {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let filters_fmt: Vec<String> = self
            .filters
            .as_slice()
            .iter()
            .map(|q| format!("{}", q))
            .collect();
        write!(
            f,
            "FilterQuery(query: {}, filter: {})",
            &self.weight,
            filters_fmt.join(", ")
        )
    }
}

struct FilterScorer {
    scorer: Box<Scorer>,
    filters: Vec<Box<LeafFilterFunction>>,
}

impl DocIterator for FilterScorer {
    fn doc_id(&self) -> DocId {
        self.scorer.doc_id()
    }

    fn next(&mut self) -> Result<DocId> {
        self.approximate_next()?;
        two_phase_next(self)
    }

    fn advance(&mut self, target: DocId) -> Result<DocId> {
        self.approximate_advance(target)?;
        two_phase_next(self)
    }

    fn cost(&self) -> usize {
        self.scorer.cost()
    }

    fn matches(&mut self) -> Result<bool> {
        let doc = self.doc_id();
        for filter in &mut self.filters {
            if !filter.matches(doc)? {
                return Ok(false);
            }
        }
        self.scorer.matches()
    }

    fn match_cost(&self) -> f32 {
        // TODO, currently not sure how to compare func cost with doc iterator cost
        1.0 * self.filters.len() as f32
    }

    fn approximate_next(&mut self) -> Result<DocId> {
        self.scorer.approximate_next()
    }

    fn approximate_advance(&mut self, target: DocId) -> Result<DocId> {
        self.scorer.approximate_advance(target)
    }
}

impl Scorer for FilterScorer {
    fn score(&mut self) -> Result<f32> {
        self.scorer.score()
    }

    fn support_two_phase(&self) -> bool {
        true
    }

    fn score_context(&mut self) -> Result<IndexedContext> {
        self.scorer.score_context()
    }

    fn score_feature(&mut self) -> Result<Vec<FeatureResult>> {
        self.scorer.score_feature()
    }
}
