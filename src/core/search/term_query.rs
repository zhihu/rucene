use error::*;
use std::boxed::Box;
use std::collections::HashMap;
use std::fmt;

use core::index::term::TermState;
use core::index::LeafReader;
use core::index::{Term, TermContext};
use core::index::{POSTINGS_FREQS, POSTINGS_NONE};
use core::search::posting_iterator::EmptyPostingIterator;
use core::search::searcher::IndexSearcher;
use core::search::statistics::*;
use core::search::term_scorer::TermScorer;
use core::search::{DocIterator, Query, Scorer, Similarity, SimWeight, Weight};
use core::util::DocId;

pub const TERM: &str = "term";

#[derive(Clone, Debug, PartialEq)]
pub struct TermQuery {
    pub term: Term,
    pub boost: f32,
}

impl TermQuery {
    pub fn new(term: Term, boost: f32) -> TermQuery {
        TermQuery { term, boost }
    }
}

impl Query for TermQuery {
    fn create_weight(&self, searcher: &IndexSearcher, needs_scores: bool) -> Result<Box<Weight>> {
        let reader = searcher.reader.as_ref();
        let mut term_context = TermContext::new(reader);
        term_context.build(reader, &self.term)?;
        let max_doc = i64::from(reader.max_doc());
        let (term_stats, collection_stats) = if needs_scores {
            (
                searcher.term_statistics(self.term.clone(), &term_context),
                searcher.collections_statistics(self.term.field.clone())?,
            )
        } else {
            (
                TermStatistics::new(self.term.bytes.clone(), max_doc, -1),
                CollectionStatistics::new(self.term.field.clone(), max_doc, -1, -1, -1),
            )
        };
        let similarity = searcher.similarity(&self.term.field);
        let sim_weight = similarity.compute_weight(&collection_stats, &term_stats);
        Ok(Box::new(TermWeight::new(
            self.term.clone(),
            term_context.states.into_iter().collect(),
            self.boost,
            similarity,
            sim_weight,
            needs_scores,
        )))
    }

    fn extract_terms(&self) -> Vec<TermQuery> {
        vec![self.clone()]
    }

    fn query_type(&self) -> &'static str {
        TERM
    }
}

impl fmt::Display for TermQuery {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "TermQuery(field: {}, term: {}, boost: {})",
            &self.term.field(),
            &self.term.text().unwrap(),
            self.boost
        )
    }
}

pub struct TermWeight {
    term: Term,
    boost: f32,
    similarity: Box<Similarity>,
    sim_weight: Box<SimWeight>,
    needs_scores: bool,
    term_states: HashMap<DocId, Box<TermState>>,
}

impl TermWeight {
    pub fn new(
        term: Term,
        term_states: HashMap<DocId, Box<TermState>>,
        boost: f32,
        similarity: Box<Similarity>,
        sim_weight: Box<SimWeight>,
        needs_scores: bool,
    ) -> TermWeight {
        TermWeight {
            term,
            boost,
            similarity,
            sim_weight,
            needs_scores,
            term_states,
        }
    }

    fn create_doc_iterator(&self, reader: &LeafReader, flags: i32) -> Result<Box<DocIterator>> {
        if let Some(state) = self.term_states.get(&reader.doc_base()) {
            reader.docs_from_state(&self.term, state.as_ref(), flags)
        } else {
            Ok(Box::new(EmptyPostingIterator::default()))
        }
    }
}

impl Weight for TermWeight {
    fn create_scorer(&self, reader: &LeafReader) -> Result<Box<Scorer>> {
        let _norms = reader.norm_values(&self.term.field);
        let sim_scorer = self.sim_weight.sim_scorer(reader)?;

        let flags = if self.needs_scores {
            POSTINGS_FREQS
        } else {
            POSTINGS_NONE
        };

        Ok(Box::new(TermScorer::new(
            sim_scorer,
            self.create_doc_iterator(reader, i32::from(flags))?,
            self.boost,
        )))
    }

    fn query_type(&self) -> &'static str {
        TERM
    }
}

impl fmt::Display for TermWeight {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "TermWeight(field: {}, term: {}, boost: {}, similarity: {}, need_score: {})",
            &self.term.field(),
            &self.term.text().unwrap(),
            self.boost,
            &self.similarity,
            self.needs_scores
        )
    }
}
