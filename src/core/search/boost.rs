use error::*;
use std::fmt;
use std::f32;

use core::index::LeafReader;
use core::search::term_query::TermQuery;
use core::search::searcher::IndexSearcher;
use core::search::{Query, Scorer, Weight};

const BOOST_QUERY: &str = "boost";

pub struct BoostQuery {
    query: Box<Query>,
    boost: f32
}

impl BoostQuery {
    pub fn build(query: Box<Query>, boost: f32) -> Box<Query> {
        if (boost - 1.0f32).abs() <= f32::EPSILON {
            query
        } else {
            Box::new(BoostQuery{
                query, boost
            })
        }
    }
}

impl Query for BoostQuery {
    fn create_weight(&self, searcher: &IndexSearcher, needs_scores: bool) -> Result<Box<Weight>> {
        let mut weight = self.query.create_weight(searcher, needs_scores)?;
        weight.normalize(1.0f32, self.boost);
        Ok(Box::new(BoostWeight::new(weight, self.boost)))
    }

    fn extract_terms(&self) -> Vec<TermQuery> {
        self.query.extract_terms()
    }

    fn query_type(&self) -> &'static str {
        BOOST_QUERY
    }
}

impl fmt::Display for BoostQuery {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "BoostQuery(query: {}, boost: {})", &self.query, self.boost)
    }
}

pub struct BoostWeight {
    weight: Box<Weight>,
    boost: f32
}

impl BoostWeight {
    pub fn new(weight: Box<Weight>, boost: f32) -> BoostWeight {
        assert!((boost - 1.0f32).abs() > f32::EPSILON);

        BoostWeight {
            weight,
            boost
        }
    }
}

impl Weight for BoostWeight {
    fn create_scorer(&self, leaf_reader: &LeafReader) -> Result<Box<Scorer>> {
        self.weight.create_scorer(leaf_reader)
    }

    fn query_type(&self) -> &'static str {
        BOOST_QUERY
    }

    fn actual_query_type(&self) -> &'static str {
        self.weight.query_type()
    }

    fn normalize(&mut self, norm: f32, boost: f32) {
        self.weight.normalize(norm, boost * self.boost)
    }

    fn value_for_normalization(&self) -> f32 {
        self.weight.value_for_normalization()
    }

    fn needs_scores(&self) -> bool {
        self.weight.needs_scores()
    }
}

impl fmt::Display for BoostWeight {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "BoostWeight(weight: {}, boost: {})", &self.weight, self.boost)
    }
}
