use core::index::{LeafReader, Term, TermContext};
use core::search::explanation::Explanation;
use core::search::searcher::IndexSearcher;
use core::search::spans::span::{build_sim_weight, PostingsFlag};
use core::search::spans::span::{SpanQuery, SpanWeight, Spans};
use core::search::term_query::TermQuery;
use core::search::{Query, Scorer, SimWeight, Weight};
use core::util::DocId;

use error::Result;

use std::collections::HashMap;
use std::f32;
use std::fmt;
use std::rc::Rc;

const SPAN_BOOST_QUERY: &str = "span_boost";

/// counterpart of `BoostQuery` for spans
pub struct SpanBoostQuery {
    query: Box<SpanQuery>,
    boost: f32,
}

impl SpanBoostQuery {
    pub fn new(query: Box<SpanQuery>, boost: f32) -> Self {
        assert!((boost - 1.0f32).abs() > f32::EPSILON);
        SpanBoostQuery { query, boost }
    }

    fn span_boost_weight(&self, searcher: &IndexSearcher) -> Result<SpanBoostWeight> {
        let mut weight = self.query.span_weight(searcher, true)?;
        let mut terms = HashMap::new();
        weight.extract_term_contexts(&mut terms);
        weight.do_normalize(1.0, self.boost);
        SpanBoostWeight::new(self, terms, searcher, true)
    }
}

impl SpanQuery for SpanBoostQuery {
    fn field(&self) -> &str {
        self.query.field()
    }

    fn span_weight(&self, searcher: &IndexSearcher, needs_scores: bool) -> Result<Box<SpanWeight>> {
        if !needs_scores {
            self.query.span_weight(searcher, needs_scores)
        } else {
            let weight = self.span_boost_weight(searcher)?;
            Ok(Box::new(weight))
        }
    }
}

impl Query for SpanBoostQuery {
    fn create_weight(&self, searcher: &IndexSearcher, needs_scores: bool) -> Result<Box<Weight>> {
        if !needs_scores {
            self.query.create_weight(searcher, needs_scores)
        } else {
            let weight = self.span_boost_weight(searcher)?;
            Ok(Box::new(weight))
        }
    }

    fn extract_terms(&self) -> Vec<TermQuery> {
        self.query.extract_terms()
    }

    fn query_type(&self) -> &'static str {
        SPAN_BOOST_QUERY
    }
}

impl fmt::Display for SpanBoostQuery {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SpanBoostQuery(query: {}, boost: {})",
            &self.query, self.boost
        )
    }
}

pub struct SpanBoostWeight {
    sim_weight: Option<Box<SimWeight>>,
    weight: Box<SpanWeight>,
    boost: f32,
}

impl SpanBoostWeight {
    pub fn new(
        query: &SpanBoostQuery,
        term_contexts: HashMap<Term, Rc<TermContext>>,
        searcher: &IndexSearcher,
        needs_scores: bool,
    ) -> Result<Self> {
        let sim_weight = build_sim_weight(query.field(), searcher, term_contexts)?;
        let weight = query.query.span_weight(searcher, needs_scores)?;
        Ok(SpanBoostWeight {
            sim_weight,
            weight,
            boost: query.boost,
        })
    }
}

impl SpanWeight for SpanBoostWeight {
    fn sim_weight(&self) -> Option<&SimWeight> {
        self.sim_weight.as_ref().map(|x| &**x)
    }

    fn sim_weight_mut(&mut self) -> Option<&mut SimWeight> {
        if let Some(ref mut sim_weight) = self.sim_weight {
            Some(sim_weight.as_mut())
        } else {
            None
        }
    }

    fn get_spans(
        &self,
        reader: &LeafReader,
        required_postings: &PostingsFlag,
    ) -> Result<Option<Box<Spans>>> {
        self.weight.get_spans(reader, required_postings)
    }

    fn extract_term_contexts(&self, contexts: &mut HashMap<Term, Rc<TermContext>>) {
        self.weight.extract_term_contexts(contexts)
    }
}

impl Weight for SpanBoostWeight {
    fn create_scorer(&self, leaf_reader: &LeafReader) -> Result<Box<Scorer>> {
        self.do_create_scorer(leaf_reader)
    }

    fn query_type(&self) -> &'static str {
        SPAN_BOOST_QUERY
    }

    fn normalize(&mut self, norm: f32, boost: f32) {
        let b = boost * self.boost;
        self.do_normalize(norm, b)
    }

    fn value_for_normalization(&self) -> f32 {
        self.do_value_for_normalization()
    }

    fn needs_scores(&self) -> bool {
        true
    }

    fn explain(&self, reader: &LeafReader, doc: DocId) -> Result<Explanation> {
        self.weight.explain_span(reader, doc)
    }
}

impl fmt::Display for SpanBoostWeight {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SpanBoostWeight(weight: {}, boost: {})",
            &self.weight, self.boost
        )
    }
}
