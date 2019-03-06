use core::index::TermIterator;
use core::index::{LeafReader, Term, TermContext};
use core::search::explanation::Explanation;
use core::search::posting_iterator::PostingIterator;
use core::search::searcher::IndexSearcher;
use core::search::spans::span::{build_sim_weight, PostingsFlag, NO_MORE_POSITIONS};
use core::search::spans::span::{SpanCollector, SpanQuery, SpanWeight, Spans};
use core::search::term_query::TermQuery;
use core::search::{DocIterator, Query, Scorer, SimWeight, Weight, NO_MORE_DOCS};
use core::util::{DocId, KeyedContext};

use error::{ErrorKind, Result};

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

const SPAN_TERM_QUERY: &str = "span_term";

/// Matches spans containing a term.
/// This should not be used for terms that are indexed at position Integer.MAX_VALUE.
pub struct SpanTermQuery {
    pub term: Term,
    pub ctx: Option<KeyedContext>,
}

impl SpanTermQuery {
    pub fn new<T: Into<Option<KeyedContext>>>(term: Term, ctx: T) -> Self {
        let ctx = ctx.into();
        SpanTermQuery { term, ctx }
    }
}

impl Query for SpanTermQuery {
    fn create_weight(&self, searcher: &IndexSearcher, needs_scores: bool) -> Result<Box<Weight>> {
        let term_context = searcher.term_state(&self.term)?;
        Ok(Box::new(SpanTermWeight::new(
            self,
            term_context,
            searcher,
            self.ctx.clone(),
            needs_scores,
        )?))
    }

    fn extract_terms(&self) -> Vec<TermQuery> {
        vec![TermQuery::new(
            self.term.clone(),
            1.0f32,
            Clone::clone(&self.ctx),
        )]
    }

    fn query_type(&self) -> &'static str {
        SPAN_TERM_QUERY
    }

    fn as_any(&self) -> &::std::any::Any {
        self
    }
}

impl SpanQuery for SpanTermQuery {
    fn field(&self) -> &str {
        self.term.field()
    }

    fn span_weight(&self, searcher: &IndexSearcher, needs_scores: bool) -> Result<Box<SpanWeight>> {
        let term_context = searcher.term_state(&self.term)?;
        Ok(Box::new(SpanTermWeight::new(
            self,
            term_context,
            searcher,
            self.ctx.clone(),
            needs_scores,
        )?))
    }

    fn ctx(&self) -> Option<KeyedContext> {
        self.ctx.clone()
    }
}

impl fmt::Display for SpanTermQuery {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SpanTermQuery(field: {}, term: {})",
            self.term.field,
            self.term.text().unwrap()
        )
    }
}

pub struct TermSpans {
    postings: Box<PostingIterator>,
    term: Term,
    doc: DocId,
    freq: i32,
    count: i32,
    position: i32,
    positions_cost: f32,
}

impl TermSpans {
    pub fn new(postings: Box<PostingIterator>, term: Term, positions_cost: f32) -> Self {
        TermSpans {
            postings,
            term,
            doc: -1,
            freq: 0,
            count: 0,
            position: -1,
            positions_cost,
        }
    }

    fn set_doc(&mut self) -> Result<()> {
        if self.doc != NO_MORE_DOCS {
            self.freq = self.postings.freq()?;
            assert!(self.freq >= 1);
            self.count = 0;
        }
        self.position = -1;
        Ok(())
    }
}

impl DocIterator for TermSpans {
    fn doc_id(&self) -> i32 {
        self.doc
    }

    fn next(&mut self) -> Result<i32> {
        self.doc = self.postings.next()?;
        self.set_doc()?;
        Ok(self.doc)
    }

    fn advance(&mut self, target: DocId) -> Result<i32> {
        assert!(target > self.doc);
        self.doc = self.postings.advance(target)?;
        self.set_doc()?;
        Ok(self.doc)
    }

    fn cost(&self) -> usize {
        self.postings.cost()
    }
}

impl Spans for TermSpans {
    fn next_start_position(&mut self) -> Result<i32> {
        if self.count == self.freq {
            assert_ne!(self.position, NO_MORE_POSITIONS);
            self.position = NO_MORE_POSITIONS;
            return Ok(self.position);
        }
        let prev_position = self.position;
        self.position = self.postings.next_position()?;
        debug_assert!(self.position >= prev_position);
        debug_assert_ne!(self.position, NO_MORE_POSITIONS); // int end_position not possible
        self.count += 1;
        Ok(self.position)
    }

    fn start_position(&self) -> i32 {
        self.position
    }

    fn end_position(&self) -> i32 {
        if self.position == -1 {
            -1
        } else if self.position != NO_MORE_POSITIONS {
            self.position + 1
        } else {
            NO_MORE_POSITIONS
        }
    }

    fn width(&self) -> i32 {
        0
    }

    fn collect(&mut self, collector: &mut SpanCollector) -> Result<()> {
        collector.collect_leaf(self.postings.as_mut(), self.position, &self.term)
    }

    fn positions_cost(&self) -> f32 {
        self.positions_cost
    }

    fn support_two_phase(&self) -> bool {
        false
    }
}

/// A guess of the relative cost of dealing with the term positions
/// when using a SpanNearQuery instead of a PhraseQuery.
const PHRASE_TO_SPAN_TERM_POSITIONS_COST: f32 = 4.0;
const TERM_POSNS_SEEK_OPS_PER_DOC: i32 = 128;
const TERM_OPS_PER_POS: i32 = 7;

pub struct SpanTermWeight {
    term: Term,
    sim_weight: Option<Box<SimWeight>>,
    term_context: Arc<TermContext>,
}

impl SpanTermWeight {
    pub fn new(
        query: &SpanTermQuery,
        term_context: Arc<TermContext>,
        searcher: &IndexSearcher,
        ctx: Option<KeyedContext>,
        needs_scores: bool,
    ) -> Result<Self> {
        let mut term_contexts = HashMap::new();
        if needs_scores {
            term_contexts.insert(query.term.clone(), Arc::clone(&term_context));
        }
        let sim_weight = build_sim_weight(query.term.field(), searcher, term_contexts, ctx)?;
        Ok(SpanTermWeight {
            term: query.term.clone(),
            sim_weight,
            term_context,
        })
    }

    fn term_positions_cost(terms_iter: &mut TermIterator) -> Result<f32> {
        let doc_freq = terms_iter.doc_freq()?;
        assert!(doc_freq > 0);
        let total_term_freq = terms_iter.total_term_freq()?; // -1 when not available
        let exp_occurrences_in_match_doc = if total_term_freq < i64::from(doc_freq) {
            1.0
        } else {
            total_term_freq as f64 / f64::from(doc_freq)
        };
        Ok(TERM_POSNS_SEEK_OPS_PER_DOC as f32
            + exp_occurrences_in_match_doc as f32 * TERM_OPS_PER_POS as f32)
    }
}

impl SpanWeight for SpanTermWeight {
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
        if let Some(state) = self.term_context.get_term_state(reader) {
            if let Some(terms) = reader.terms(self.term.field())? {
                if !terms.has_positions()? {
                    bail!(ErrorKind::IllegalState(format!(
                        "field '{}' was indexed without position data; cannot run SpanTermQuery \
                         (term={:?})",
                        &self.term.field,
                        &self.term.text()
                    )));
                }
                let mut terms_iter = terms.iterator()?;
                terms_iter.seek_exact_state(&self.term.bytes, state)?;
                let postings =
                    terms_iter.postings_with_flags(required_postings.required_postings())?;
                let positions_cost = Self::term_positions_cost(terms_iter.as_mut())?
                    + PHRASE_TO_SPAN_TERM_POSITIONS_COST;
                return Ok(Some(Box::new(TermSpans::new(
                    postings,
                    self.term.clone(),
                    positions_cost,
                ))));
            }
        }
        Ok(None)
    }

    fn extract_term_contexts(&self, contexts: &mut HashMap<Term, Arc<TermContext>>) {
        contexts.insert(self.term.clone(), Arc::clone(&self.term_context));
    }
}

impl Weight for SpanTermWeight {
    fn create_scorer(&self, leaf_reader: &LeafReader) -> Result<Box<Scorer>> {
        self.do_create_scorer(leaf_reader)
    }

    fn query_type(&self) -> &'static str {
        SPAN_TERM_QUERY
    }

    fn normalize(&mut self, norm: f32, boost: f32) {
        self.do_normalize(norm, boost)
    }

    fn value_for_normalization(&self) -> f32 {
        self.do_value_for_normalization()
    }

    fn needs_scores(&self) -> bool {
        true
    }

    fn explain(&self, reader: &LeafReader, doc: DocId) -> Result<Explanation> {
        self.explain_span(reader, doc)
    }
}

impl fmt::Display for SpanTermWeight {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SpanTermWeight(field: {}, term: {})",
            self.term.field,
            self.term.text().unwrap()
        )
    }
}
