use core::index::LeafReader;
use core::search::searcher::IndexSearcher;
use core::search::term_query::TermQuery;
use core::search::{DocIterator, Query, Scorer, Weight, NO_MORE_DOCS};
use core::util::DocId;
use error::*;
use std::fmt;

pub const MATCH_ALL: &str = "match_all";

pub struct MatchAllDocsQuery;

impl Query for MatchAllDocsQuery {
    fn create_weight(&self, _searcher: &IndexSearcher, _needs_scores: bool) -> Result<Box<Weight>> {
        Ok(Box::new(MatchAllDocsWeight::default()))
    }

    fn extract_terms(&self) -> Vec<TermQuery> {
        unimplemented!()
    }

    fn query_type(&self) -> &'static str {
        MATCH_ALL
    }
}

impl fmt::Display for MatchAllDocsQuery {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MatchAllDocsQuery()")
    }
}

struct MatchAllDocsWeight {
    weight: f32,
    norm: f32,
}

impl Default for MatchAllDocsWeight {
    fn default() -> Self {
        MatchAllDocsWeight {
            weight: 0f32,
            norm: 1f32,
        }
    }
}

impl Weight for MatchAllDocsWeight {
    fn create_scorer(&self, leaf_reader: &LeafReader) -> Result<Box<Scorer>> {
        Ok(Box::new(ConstantScoreScorer {
            score: self.weight,
            iterator: Box::new(AllDocsIterator::new(leaf_reader.max_doc())),
            cost: leaf_reader.max_doc() as usize,
        }))
    }

    fn query_type(&self) -> &'static str {
        MATCH_ALL
    }

    fn normalize(&mut self, norm: f32, boost: f32) {
        self.norm = norm;
        self.weight = norm * boost;
    }

    fn value_for_normalization(&self) -> f32 {
        self.weight * self.weight
    }

    fn needs_scores(&self) -> bool {
        false
    }
}

impl fmt::Display for MatchAllDocsWeight {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MatchAllDocsWeight()")
    }
}

pub struct ConstantScoreScorer<T: DocIterator + Send + Sync + ?Sized> {
    score: f32,
    iterator: Box<T>,
    cost: usize,
}

impl<T: DocIterator + Send + Sync + ?Sized> ConstantScoreScorer<T> {
    pub fn new(score: f32, iterator: Box<T>, cost: usize) -> ConstantScoreScorer<T> {
        ConstantScoreScorer {
            score,
            iterator,
            cost,
        }
    }
}

impl<T: DocIterator + Send + Sync + ?Sized> Scorer for ConstantScoreScorer<T> {
    fn score(&mut self) -> Result<f32> {
        Ok(self.score)
    }
}

impl<T: DocIterator + Send + Sync + ?Sized> DocIterator for ConstantScoreScorer<T> {
    fn doc_id(&self) -> DocId {
        self.iterator.doc_id()
    }

    fn next(&mut self) -> Result<DocId> {
        self.iterator.next()
    }

    fn advance(&mut self, target: DocId) -> Result<DocId> {
        self.iterator.advance(target)
    }

    fn cost(&self) -> usize {
        self.cost
    }
}

pub struct AllDocsIterator {
    doc: DocId,
    max_doc: DocId,
}

impl AllDocsIterator {
    pub fn new(max_doc: DocId) -> AllDocsIterator {
        assert!(max_doc >= 0);
        AllDocsIterator { doc: -1, max_doc }
    }
}

impl Scorer for AllDocsIterator {
    fn score(&mut self) -> Result<f32> {
        Ok(1f32)
    }
}

impl DocIterator for AllDocsIterator {
    fn doc_id(&self) -> DocId {
        self.doc
    }

    fn next(&mut self) -> Result<DocId> {
        let target = self.doc + 1;
        self.advance(target)
    }

    fn advance(&mut self, target: DocId) -> Result<DocId> {
        self.doc = if target >= self.max_doc {
            NO_MORE_DOCS
        } else {
            target
        };
        Ok(self.doc)
    }

    fn cost(&self) -> usize {
        1usize.max(self.max_doc as usize)
    }
}

pub const CONSTANT: &str = "constant";

pub struct ConstantScoreQuery {
    pub query: Box<Query>,
    boost: f32,
}

impl ConstantScoreQuery {
    pub fn new(query: Box<Query>) -> ConstantScoreQuery {
        ConstantScoreQuery { query, boost: 0f32 }
    }

    pub fn with_boost(query: Box<Query>, boost: f32) -> ConstantScoreQuery {
        ConstantScoreQuery { query, boost }
    }
}

impl fmt::Display for ConstantScoreQuery {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ConstantScoreQuery(sub_query: {}, weight: {})",
            self.query, self.boost
        )
    }
}

impl Query for ConstantScoreQuery {
    fn create_weight(&self, searcher: &IndexSearcher, needs_scores: bool) -> Result<Box<Weight>> {
        let weight = searcher.create_weight(self.query.as_ref(), false)?;
        if needs_scores {
            Ok(Box::new(ConstantScoreWeight::new(weight, self.boost)))
        } else {
            Ok(weight)
        }
    }

    fn extract_terms(&self) -> Vec<TermQuery> {
        vec![]
    }

    fn query_type(&self) -> &'static str {
        CONSTANT
    }
}

struct ConstantScoreWeight {
    sub_weight: Box<Weight>,
    query_norm: f32,
    query_weight: f32,
}

impl ConstantScoreWeight {
    pub fn new(sub_weight: Box<Weight>, boost: f32) -> ConstantScoreWeight {
        ConstantScoreWeight {
            sub_weight,
            query_weight: boost,
            query_norm: 1.0f32,
        }
    }
}

impl Weight for ConstantScoreWeight {
    fn create_scorer(&self, leaf_reader: &LeafReader) -> Result<Box<Scorer>> {
        let inner_scorer = self.sub_weight.create_scorer(leaf_reader)?;
        let cost = inner_scorer.cost();
        Ok(Box::new(ConstantScoreScorer {
            score: self.query_weight,
            iterator: inner_scorer,
            cost,
        }))
    }

    fn query_type(&self) -> &'static str {
        CONSTANT
    }

    fn normalize(&mut self, norm: f32, boost: f32) {
        self.query_weight = norm * boost;
        self.query_norm = norm;
    }

    fn value_for_normalization(&self) -> f32 {
        self.query_weight * self.query_weight
    }

    fn needs_scores(&self) -> bool {
        false
    }
}

impl fmt::Display for ConstantScoreWeight {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ConstantScoreWeight(sub_weight: {}, query_weight: {}, query_norm: {})",
            self.sub_weight, self.query_weight, self.query_norm
        )
    }
}
