use core::index::{LeafReader, Term, TermContext};
use core::search::disi::DisiPriorityQueue;
use core::search::explanation::Explanation;
use core::search::searcher::IndexSearcher;
use core::search::spans::span::{build_sim_weight, term_contexts, PostingsFlag, NO_MORE_POSITIONS};
use core::search::spans::span::{SpanCollector, SpanQuery, SpanWeight, Spans};
use core::search::term_query::TermQuery;
use core::search::{DocIterator, Query, Scorer, SimWeight, Weight};
use core::util::DocId;

use error::{ErrorKind, Result};

use std::cmp::{max, Ordering};
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

const SPAN_OR_QUERY: &str = "span_or";

pub struct SpanOrQuery {
    field: String,
    clauses: Vec<Box<SpanQuery>>,
}

impl SpanOrQuery {
    pub fn new(clauses: Vec<Box<SpanQuery>>) -> Result<Self> {
        if clauses.len() < 2 {
            bail!(ErrorKind::IllegalArgument(
                "clauses length must not be smaller than 2!".into()
            ));
        }
        for i in 0..clauses.len() - 1 {
            if clauses[i].field() != clauses[i + 1].field() {
                bail!(ErrorKind::IllegalArgument(
                    "Clauses must have same field.".into()
                ));
            }
        }
        let field = clauses[0].field().to_string();
        Ok(SpanOrQuery { field, clauses })
    }

    fn span_or_weight(&self, searcher: &IndexSearcher, needs_scores: bool) -> Result<SpanOrWeight> {
        let mut sub_weights = Vec::with_capacity(self.clauses.len());
        for clause in &self.clauses {
            sub_weights.push(clause.span_weight(searcher, needs_scores)?);
        }
        let term_contexts = if needs_scores {
            term_contexts(&sub_weights)
        } else {
            HashMap::new()
        };
        SpanOrWeight::new(self, sub_weights, searcher, term_contexts)
    }
}

impl SpanQuery for SpanOrQuery {
    fn field(&self) -> &str {
        &self.field
    }

    fn span_weight(&self, searcher: &IndexSearcher, needs_scores: bool) -> Result<Box<SpanWeight>> {
        Ok(Box::new(self.span_or_weight(searcher, needs_scores)?))
    }
}

impl Query for SpanOrQuery {
    fn create_weight(&self, searcher: &IndexSearcher, needs_scores: bool) -> Result<Box<Weight>> {
        Ok(Box::new(self.span_or_weight(searcher, needs_scores)?))
    }

    fn extract_terms(&self) -> Vec<TermQuery> {
        self.clauses
            .iter()
            .flat_map(|c| c.extract_terms())
            .collect()
    }

    fn query_type(&self) -> &'static str {
        SPAN_OR_QUERY
    }
}

impl fmt::Display for SpanOrQuery {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let clauses = {
            let query_strs: Vec<String> = self.clauses.iter().map(|q| format!("{}", q)).collect();
            query_strs.join(", ")
        };
        write!(
            f,
            "SpanOrQuery(field: {}, clauses: [{}])",
            self.field, clauses
        )
    }
}

pub struct SpanOrWeight {
    sim_weight: Option<Box<SimWeight>>,
    sub_weights: Vec<Box<SpanWeight>>,
}

impl SpanOrWeight {
    pub fn new(
        query: &SpanOrQuery,
        sub_weights: Vec<Box<SpanWeight>>,
        searcher: &IndexSearcher,
        terms: HashMap<Term, Arc<TermContext>>,
    ) -> Result<Self> {
        assert!(sub_weights.len() >= 2);
        let sim_weight = build_sim_weight(query.field(), searcher, terms, None)?;
        Ok(SpanOrWeight {
            sim_weight,
            sub_weights,
        })
    }
}

impl SpanWeight for SpanOrWeight {
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
        let mut sub_spans = Vec::with_capacity(self.sub_weights.len());
        for w in &self.sub_weights {
            if let Some(span) = w.get_spans(reader, required_postings)? {
                sub_spans.push(span);
            }
        }
        if sub_spans.len() <= 1 {
            return Ok(sub_spans.pop());
        }

        let capasity = sub_spans.len();
        let by_doc_queue = DisiPriorityQueue::new(sub_spans);

        Ok(Some(Box::new(SpanOrSpans::new(
            by_doc_queue,
            BinaryHeap::with_capacity(capasity),
        ))))
    }

    fn extract_term_contexts(&self, contexts: &mut HashMap<Term, Arc<TermContext>>) {
        for spans in &self.sub_weights {
            spans.extract_term_contexts(contexts)
        }
    }
}

impl Weight for SpanOrWeight {
    fn create_scorer(&self, leaf_reader: &LeafReader) -> Result<Box<Scorer>> {
        self.do_create_scorer(leaf_reader)
    }

    fn query_type(&self) -> &'static str {
        SPAN_OR_QUERY
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

impl fmt::Display for SpanOrWeight {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let weights = {
            let query_strs: Vec<String> =
                self.sub_weights.iter().map(|q| format!("{}", q)).collect();
            query_strs.join(", ")
        };
        write!(f, "SpanOrWeight(sub_weight: [{}])", weights)
    }
}

struct SpanOrSpans {
    by_doc_queue: DisiPriorityQueue<Spans>,
    by_position_queue: BinaryHeap<SpansElement>,
    top_position_spans_valid: bool,
    positions_cost: f32,
    last_doc_two_phase_matched: DocId,
    match_cost: f32,
    cost: usize,
    support_two_phase: bool,
}

unsafe impl Send for SpanOrSpans {}

unsafe impl Sync for SpanOrSpans {}

impl SpanOrSpans {
    pub fn new(
        by_doc_queue: DisiPriorityQueue<Spans>,
        by_position_queue: BinaryHeap<SpansElement>,
    ) -> Self {
        let (positions_cost, match_cost, cost) = {
            let mut sum_position_cost = 0.0f32;
            let mut sum_approx_cost = 0.0f32;
            let mut sum_weight = 0.0f32;
            let mut cost = 0;
            for spans in &by_doc_queue {
                let cost_weight = max(spans.cost(), 1) as f32;
                sum_position_cost += spans.positions_cost() * cost_weight;
                sum_approx_cost += spans.match_cost() * cost_weight;
                sum_weight += cost_weight;
                cost += spans.cost();
            }
            (
                sum_position_cost / sum_weight,
                sum_approx_cost / sum_weight,
                cost,
            )
        };

        let support_two_phase = (&by_doc_queue)
            .into_iter()
            .any(|spans| spans.support_two_phase());

        SpanOrSpans {
            by_doc_queue,
            by_position_queue,
            top_position_spans_valid: false,
            positions_cost,
            last_doc_two_phase_matched: -1,
            match_cost,
            cost,
            support_two_phase,
        }
    }

    fn two_phase_current_doc_matches(&mut self) -> Result<bool> {
        let mut list_at_current_doc = self.by_doc_queue.top_list();
        // remove the head of the list as long as it does not match
        let current_doc = list_at_current_doc.doc;
        while list_at_current_doc.inner().support_two_phase() {
            if list_at_current_doc.inner_mut().matches()? {
                // use this spans for positions at current doc:
                list_at_current_doc.last_approx_match_doc = current_doc;
                break;
            }
            // do not use this spans for positions at current doc:
            list_at_current_doc.last_approx_non_match_doc = current_doc;
            if list_at_current_doc.next.is_null() {
                return Ok(false);
            }
            unsafe {
                list_at_current_doc = &mut (*list_at_current_doc.next);
            }
        }
        self.last_doc_two_phase_matched = current_doc;
        self.top_position_spans_valid = false;
        Ok(true)
    }

    fn fill_position_queue(&mut self) -> Result<()> {
        // cualled at first next_start_position
        assert_eq!(self.by_position_queue.len(), 0);
        // add all matching Spans at current doc to by_position_queue
        let mut list_at_current_doc = self.by_doc_queue.top_list();
        loop {
            let spans_at_doc_match = self.last_doc_two_phase_matched != list_at_current_doc.doc
                || !list_at_current_doc.inner().support_two_phase()
                || (list_at_current_doc.last_approx_non_match_doc != list_at_current_doc.doc
                    && (list_at_current_doc.last_approx_match_doc == list_at_current_doc.doc
                        || list_at_current_doc.inner_mut().matches()?));

            if spans_at_doc_match {
                assert_eq!(
                    list_at_current_doc.inner().doc_id(),
                    list_at_current_doc.doc
                );
                assert_eq!(list_at_current_doc.inner().start_position(), -1);
                list_at_current_doc.inner_mut().next_start_position()?;
                assert_ne!(
                    list_at_current_doc.inner().start_position(),
                    NO_MORE_POSITIONS
                );
                self.by_position_queue
                    .push(SpansElement::new(list_at_current_doc.inner_mut() as *mut Spans));
            }
            if list_at_current_doc.next.is_null() {
                break;
            }
            unsafe {
                list_at_current_doc = &mut *list_at_current_doc.next;
            }
        }
        assert!(!self.by_position_queue.is_empty());
        Ok(())
    }
}

impl Spans for SpanOrSpans {
    fn next_start_position(&mut self) -> Result<i32> {
        if !self.top_position_spans_valid {
            self.by_position_queue.clear();
            self.fill_position_queue()?;
            self.top_position_spans_valid = true;
        } else {
            self.by_position_queue
                .peek_mut()
                .unwrap()
                .spans()
                .next_start_position()?;
            self.top_position_spans_valid = true;
        }
        Ok(self.by_position_queue
            .peek()
            .unwrap()
            .spans()
            .start_position())
    }

    fn start_position(&self) -> i32 {
        if self.top_position_spans_valid {
            self.by_position_queue
                .peek()
                .unwrap()
                .spans()
                .start_position()
        } else {
            -1
        }
    }

    fn end_position(&self) -> i32 {
        if self.top_position_spans_valid {
            self.by_position_queue
                .peek()
                .unwrap()
                .spans()
                .end_position()
        } else {
            -1
        }
    }

    fn width(&self) -> i32 {
        self.by_position_queue.peek().unwrap().spans().width()
    }

    fn collect(&mut self, collector: &mut SpanCollector) -> Result<()> {
        if self.top_position_spans_valid {
            self.by_position_queue
                .peek_mut()
                .unwrap()
                .spans()
                .collect(collector)
        } else {
            Ok(())
        }
    }

    fn positions_cost(&self) -> f32 {
        self.positions_cost
    }

    fn support_two_phase(&self) -> bool {
        self.support_two_phase
    }
}

impl DocIterator for SpanOrSpans {
    fn doc_id(&self) -> i32 {
        self.by_doc_queue.peek().doc
    }

    fn next(&mut self) -> Result<i32> {
        self.top_position_spans_valid = false;
        let current_doc = self.by_doc_queue.peek().doc;
        loop {
            self.by_doc_queue.peek_mut().next_doc()?;
            if self.by_doc_queue.peek().doc != current_doc {
                break;
            }
        }
        Ok(self.by_doc_queue.peek().doc)
    }

    fn advance(&mut self, target: i32) -> Result<i32> {
        self.top_position_spans_valid = false;
        loop {
            self.by_doc_queue.peek_mut().advance(target)?;
            if self.by_doc_queue.peek().doc >= target {
                break;
            }
        }
        Ok(self.by_doc_queue.peek().doc)
    }

    fn cost(&self) -> usize {
        self.cost
    }

    fn matches(&mut self) -> Result<bool> {
        self.two_phase_current_doc_matches()
    }

    fn match_cost(&self) -> f32 {
        self.match_cost
    }

    fn approximate_next(&mut self) -> Result<DocId> {
        if self.support_two_phase {
            let doc = self.by_doc_queue.peek().doc;
            loop {
                self.by_doc_queue.peek_mut().approximate_next()?;
                if self.by_doc_queue.peek().doc() != doc {
                    break;
                }
            }
            Ok(self.by_doc_queue.peek().doc)
        } else {
            self.next()
        }
    }

    fn approximate_advance(&mut self, target: DocId) -> Result<DocId> {
        if self.support_two_phase {
            loop {
                self.by_doc_queue.peek_mut().approximate_advance(target)?;
                if self.by_doc_queue.peek().doc() >= target {
                    break;
                }
            }
            Ok(self.by_doc_queue.peek().doc)
        } else {
            self.advance(target)
        }
    }
}

struct SpansElement {
    spans_prt: *mut Spans,
}

impl SpansElement {
    fn new(spans_prt: *mut Spans) -> Self {
        SpansElement { spans_prt }
    }

    #[allow(mut_from_ref)]
    fn spans(&self) -> &mut Spans {
        unsafe { &mut *self.spans_prt }
    }
}

impl Eq for SpansElement {}

impl PartialEq for SpansElement {
    fn eq(&self, other: &SpansElement) -> bool {
        unsafe { (*self.spans_prt).doc_id().eq(&(*other.spans_prt).doc_id()) }
    }
}

impl Ord for SpansElement {
    fn cmp(&self, other: &Self) -> Ordering {
        // reversed order by doc id for BinaryHeap
        unsafe { (*other.spans_prt).doc_id().cmp(&(*self.spans_prt).doc_id()) }
    }
}

impl PartialOrd for SpansElement {
    fn partial_cmp(&self, other: &SpansElement) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
