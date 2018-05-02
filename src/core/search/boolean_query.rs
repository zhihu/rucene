use std::fmt;

use core::index::LeafReader;
use core::search::conjunction::ConjunctionScorer;
use core::search::disjunction::DisjunctionSumScorer;
use core::search::match_all::ConstantScoreQuery;
use core::search::req_opt::ReqOptScorer;
use core::search::searcher::IndexSearcher;
use core::search::term_query::TermQuery;
use core::search::{Query, Scorer, Weight};
use error::*;

pub struct BooleanQuery {
    must_queries: Vec<Box<Query>>,
    should_queries: Vec<Box<Query>>,
    filter_queries: Vec<Box<Query>>,
    minimum_should_match: i32,
}

pub const BOOLEAN: &str = "boolean";

impl BooleanQuery {
    pub fn build(
        musts: Vec<Box<Query>>,
        shoulds: Vec<Box<Query>>,
        filters: Vec<Box<Query>>,
    ) -> Result<Box<Query>> {
        let minimum_should_match = if musts.is_empty() { 1 } else { 0 };
        let mut musts = musts;
        let mut shoulds = shoulds;
        let mut filters = filters;
        if musts.len() + shoulds.len() + filters.len() == 0 {
            bail!("boolean query should at least contain one inner query!");
        }
        if musts.len() + shoulds.len() + filters.len() == 1 {
            let query = if musts.len() == 1 {
                musts.remove(0)
            } else if shoulds.len() == 1 {
                shoulds.remove(0)
            } else {
                Box::new(ConstantScoreQuery::with_boost(filters.remove(0), 0f32))
            };
            return Ok(query);
        }
        Ok(Box::new(BooleanQuery {
            must_queries: musts,
            should_queries: shoulds,
            filter_queries: filters,
            minimum_should_match,
        }))
    }

    fn queries_to_str(&self, queries: &[Box<Query>]) -> String {
        let query_strs: Vec<String> = queries.iter().map(|q| format!("{}", q)).collect();
        query_strs.join(", ")
    }
}

impl Query for BooleanQuery {
    fn create_weight(&self, searcher: &IndexSearcher, needs_scores: bool) -> Result<Box<Weight>> {
        let mut must_weights =
            Vec::with_capacity(self.must_queries.len() + self.filter_queries.len());
        for q in &self.must_queries {
            must_weights.push(searcher.create_weight(q.as_ref(), needs_scores)?);
        }
        for q in &self.filter_queries {
            must_weights.push(searcher.create_weight(q.as_ref(), false)?);
        }
        let mut should_weights = Vec::new();
        for q in &self.should_queries {
            should_weights.push(searcher.create_weight(q.as_ref(), needs_scores)?);
        }

        Ok(Box::new(BooleanWeight::new(
            must_weights,
            should_weights,
            needs_scores,
        )))
    }

    fn extract_terms(&self) -> Vec<TermQuery> {
        let mut term_query_list: Vec<TermQuery> = vec![];

        for query in &self.must_queries {
            for term_query in query.extract_terms() {
                term_query_list.push(term_query);
            }
        }

        for query in &self.should_queries {
            for term_query in query.extract_terms() {
                term_query_list.push(term_query);
            }
        }

        term_query_list
    }

    fn query_type(&self) -> &'static str {
        BOOLEAN
    }
}

impl fmt::Display for BooleanQuery {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let must_str = self.queries_to_str(&self.must_queries);
        let should_str = self.queries_to_str(&self.should_queries);
        let filters_str = self.queries_to_str(&self.filter_queries);
        write!(
            f,
            "BooleanQuery(must: [{}], should: [{}], filters: [{}], match: {})",
            must_str, should_str, filters_str, self.minimum_should_match
        )
    }
}

pub struct BooleanWeight {
    must_weights: Vec<Box<Weight>>,
    should_weights: Vec<Box<Weight>>,
    #[allow(dead_code)]
    minimum_should_match: i32,
    needs_scores: bool,
}

impl BooleanWeight {
    pub fn new(
        musts: Vec<Box<Weight>>,
        shoulds: Vec<Box<Weight>>,
        needs_scores: bool,
    ) -> BooleanWeight {
        let minimum_should_match = if musts.is_empty() { 1 } else { 0 };
        BooleanWeight {
            must_weights: musts,
            should_weights: shoulds,
            minimum_should_match,
            needs_scores,
        }
    }

    fn build_scorers(
        &self,
        weights: &[Box<Weight>],
        leaf_reader: &LeafReader,
    ) -> Result<Vec<Box<Scorer>>> {
        let mut result = Vec::with_capacity(weights.len());
        for weight in weights {
            result.push(weight.create_scorer(leaf_reader)?)
        }
        Ok(result)
    }

    fn weights_to_str(&self, weights: &[Box<Weight>]) -> String {
        let weight_strs: Vec<String> = weights.iter().map(|q| format!("{}", q)).collect();
        weight_strs.join(", ")
    }
}

impl Weight for BooleanWeight {
    fn create_scorer(&self, leaf_reader: &LeafReader) -> Result<Box<Scorer>> {
        let must_scorer: Option<Box<Scorer>> = if !self.must_weights.is_empty() {
            let mut scorers = self.build_scorers(&self.must_weights, leaf_reader)?;
            if scorers.len() > 1 {
                Some(Box::new(ConjunctionScorer::new(scorers)))
            } else {
                Some(scorers.remove(0))
            }
        } else {
            None
        };
        let should_scorer: Option<Box<Scorer>> = if !self.should_weights.is_empty() {
            let mut scorers = self.build_scorers(&self.should_weights, leaf_reader)?;
            if scorers.len() > 1 {
                Some(Box::new(DisjunctionSumScorer::new(scorers)))
            } else {
                Some(scorers.remove(0))
            }
        } else {
            None
        };
        debug_assert!(must_scorer.is_some() || should_scorer.is_some());
        if must_scorer.is_none() {
            Ok(should_scorer.unwrap())
        } else if should_scorer.is_none() {
            Ok(must_scorer.unwrap())
        } else {
            Ok(Box::new(ReqOptScorer::new(
                must_scorer.unwrap(),
                should_scorer.unwrap(),
            )))
        }
    }

    fn query_type(&self) -> &'static str {
        BOOLEAN
    }

    fn normalize(&mut self, norm: f32, boost: f32) {
        for must in &mut self.must_weights {
            must.normalize(norm, boost);
        }
        for should in &mut self.should_weights {
            should.normalize(norm, boost);
        }
    }

    fn value_for_normalization(&self) -> f32 {
        let mut sum = 0f32;
        for weight in &self.must_weights {
            if weight.needs_scores() {
                sum += weight.value_for_normalization();
            }
        }
        sum
    }

    fn needs_scores(&self) -> bool {
        self.needs_scores
    }
}

impl fmt::Display for BooleanWeight {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let must_str = self.weights_to_str(&self.must_weights);
        let should_str = self.weights_to_str(&self.should_weights);
        write!(
            f,
            "BooleanWeight(must: [{}], should: [{}], min match: {}, needs score: {})",
            must_str, should_str, self.minimum_should_match, self.needs_scores
        )
    }
}
