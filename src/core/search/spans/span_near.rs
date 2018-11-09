use core::index::{LeafReader, Term, TermContext};
use core::search::explanation::Explanation;
use core::search::searcher::IndexSearcher;
use core::search::spans::span::{build_sim_weight, PostingsFlag, NO_MORE_POSITIONS};
use core::search::spans::span::{term_contexts, ConjunctionSpanBase, ConjunctionSpans};
use core::search::spans::span::{SpanCollector, SpanQuery, SpanWeight, Spans};
use core::search::term_query::TermQuery;
use core::search::{DocIterator, Query, Scorer, SimWeight, Weight, NO_MORE_DOCS};
use core::util::{DocId, KeyedContext, BM25_SIMILARITY_IDF};

use error::{ErrorKind, Result};

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

pub struct SpanNearQueryBuilder {
    ordered: bool,
    field: String,
    clauses: Vec<Box<SpanQuery>>,
    slop: i32,
}

impl SpanNearQueryBuilder {
    pub fn new(field: String, ordered: bool) -> Self {
        SpanNearQueryBuilder {
            ordered,
            field,
            clauses: vec![],
            slop: 0,
        }
    }

    pub fn add_clause(mut self, clause: Box<SpanQuery>) -> Result<Self> {
        if clause.field() != self.field {
            bail!(ErrorKind::IllegalArgument(format!(
                "clause field is '{}' not equal with field '{}'",
                clause.field(),
                &self.field
            )));
        }
        self.clauses.push(clause);
        Ok(self)
    }

    pub fn add_gap(mut self, width: i32) -> Result<Self> {
        if !self.ordered {
            bail!(ErrorKind::IllegalArgument(
                "Gaps can only be added to ordered near queries".into()
            ));
        }
        self.clauses
            .push(Box::new(SpanGapQuery::new(self.field.clone(), width)));
        Ok(self)
    }

    pub fn slop(mut self, slop: i32) -> Self {
        self.slop = slop;
        self
    }

    pub fn build(self) -> SpanNearQuery {
        SpanNearQuery::new(self.clauses, self.slop, self.ordered).unwrap()
    }
}

const SPAN_NEAR_QUERY: &str = "span_near";

pub struct SpanNearQuery {
    clauses: Vec<Box<SpanQuery>>,
    slop: i32,
    in_order: bool,
    field: String,
}

impl SpanNearQuery {
    pub fn new(clauses: Vec<Box<SpanQuery>>, slop: i32, in_order: bool) -> Result<Self> {
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
        Ok(SpanNearQuery {
            clauses,
            slop,
            in_order,
            field,
        })
    }

    fn merge_idf_ctx(ctx1: Option<KeyedContext>, ctx2: Option<KeyedContext>) -> Option<KeyedContext> {
        if ctx1.is_none() {
            ctx2
        } else if ctx2.is_none() {
            ctx1
        } else {
            let mut ctx = KeyedContext::default();
            let w = ctx1.map_or(
                0f32,
                |v| v.get_float(BM25_SIMILARITY_IDF).unwrap_or(0f32))
                +
                ctx2.map_or(
                    0f32,
                    |v| v.get_float(BM25_SIMILARITY_IDF).unwrap_or(0f32));
            ctx.set_float(BM25_SIMILARITY_IDF, w);
            Some(ctx)
        }
    }

    fn span_near_weight(
        &self,
        searcher: &IndexSearcher,
        needs_scores: bool,
    ) -> Result<SpanNearWeight> {
        let mut sub_weights = Vec::with_capacity(self.clauses.len());
        let mut ctx = None;
        for clause in &self.clauses {
            sub_weights.push(clause.span_weight(searcher, needs_scores)?);
            ctx = Self::merge_idf_ctx(ctx, clause.ctx());
        }
        let term_contexts = if needs_scores {
            term_contexts(&sub_weights)
        } else {
            HashMap::new()
        };
        SpanNearWeight::new(self, sub_weights, searcher, term_contexts, ctx)
    }
}

impl SpanQuery for SpanNearQuery {
    fn field(&self) -> &str {
        &self.field
    }

    fn span_weight(&self, searcher: &IndexSearcher, needs_scores: bool) -> Result<Box<SpanWeight>> {
        let weight = self.span_near_weight(searcher, needs_scores)?;
        Ok(Box::new(weight))
    }
}

impl Query for SpanNearQuery {
    fn create_weight(&self, searcher: &IndexSearcher, needs_scores: bool) -> Result<Box<Weight>> {
        let weight = self.span_near_weight(searcher, needs_scores)?;
        Ok(Box::new(weight))
    }

    fn extract_terms(&self) -> Vec<TermQuery> {
        self.clauses
            .iter()
            .flat_map(|c| c.extract_terms())
            .collect()
    }

    fn query_type(&self) -> &'static str {
        SPAN_NEAR_QUERY
    }
}

impl fmt::Display for SpanNearQuery {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let clauses = {
            let query_strs: Vec<String> = self.clauses.iter().map(|q| format!("{}", q)).collect();
            query_strs.join(", ")
        };
        write!(
            f,
            "SpanNearQuery(clauses: [{}], slop: {}, in_order: {})",
            clauses, self.slop, self.in_order
        )
    }
}

pub struct SpanNearWeight {
    field: String,
    in_order: bool,
    slop: i32,
    sim_weight: Option<Box<SimWeight>>,
    sub_weights: Vec<Box<SpanWeight>>,
}

impl SpanNearWeight {
    pub fn new(
        query: &SpanNearQuery,
        sub_weights: Vec<Box<SpanWeight>>,
        searcher: &IndexSearcher,
        terms: HashMap<Term, Arc<TermContext>>,
        ctx: Option<KeyedContext>,
    ) -> Result<Self> {
        let field = query.field().to_string();
        let sim_weight = build_sim_weight(query.field(), searcher, terms, ctx)?;
        Ok(SpanNearWeight {
            field,
            sim_weight,
            sub_weights,
            in_order: query.in_order,
            slop: query.slop,
        })
    }
}

impl SpanWeight for SpanNearWeight {
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
        if reader.terms(&self.field)?.is_some() {
            let mut sub_spans = Vec::with_capacity(self.sub_weights.len());
            for w in &self.sub_weights {
                if let Some(span) = w.get_spans(reader, required_postings)? {
                    sub_spans.push(span);
                } else {
                    // all required
                    return Ok(None);
                }
            }
            // all near_spans require at least two sub_spans
            if self.in_order {
                return Ok(Some(Box::new(NearSpansOrdered::new(self.slop, sub_spans)?)));
            } else {
                return Ok(Some(NearSpansUnordered::new(self.slop, sub_spans)?));
            }
        }
        Ok(None)
    }

    fn extract_term_contexts(&self, contexts: &mut HashMap<Term, Arc<TermContext>>) {
        for weight in &self.sub_weights {
            weight.extract_term_contexts(contexts)
        }
    }
}

impl Weight for SpanNearWeight {
    fn create_scorer(&self, leaf_reader: &LeafReader) -> Result<Box<Scorer>> {
        self.do_create_scorer(leaf_reader)
    }

    fn query_type(&self) -> &'static str {
        SPAN_NEAR_QUERY
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

impl fmt::Display for SpanNearWeight {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let weights = {
            let query_strs: Vec<String> =
                self.sub_weights.iter().map(|q| format!("{}", q)).collect();
            query_strs.join(", ")
        };
        write!(
            f,
            "SpanNearQuery(field: {}, sub_weight: [{}], in_order: {})",
            &self.field, weights, self.in_order
        )
    }
}

struct NearSpansUnordered {
    conjunction_span: ConjunctionSpanBase,
    sub_span_cells: Vec<SpansCell>,
    allowed_slop: i32,
    span_position_queue: BinaryHeap<SpansCellElement>,
    total_span_length: i32,
    max_end_position_cell_idx: usize,
}

impl NearSpansUnordered {
    #[allow(explicit_counter_loop)]
    // TODO return Box<> to make sure the span_cell's pointer to parent won't change
    pub fn new(allowed_slop: i32, sub_spans: Vec<Box<Spans>>) -> Result<Box<NearSpansUnordered>> {
        let mut sub_spans = sub_spans;
        let conjunction_span = ConjunctionSpanBase::new(&mut sub_spans)?;
        let mut res = Box::new(NearSpansUnordered {
            conjunction_span,
            sub_span_cells: Vec::with_capacity(sub_spans.len()),
            allowed_slop,
            span_position_queue: BinaryHeap::with_capacity(sub_spans.len()),
            total_span_length: 0,
            max_end_position_cell_idx: 0,
        });
        let mut index = 0usize;
        for spans in sub_spans {
            let span_cell = SpansCell::new(res.as_mut() as *mut NearSpansUnordered, spans, index);
            index += 1;
            res.sub_span_cells.push(span_cell);
        }
        res.single_cell_to_positions_queue();
        Ok(res)
    }

    fn single_cell_to_positions_queue(&mut self) {
        assert_eq!(self.sub_span_cells[0].doc_id(), -1);
        assert_eq!(self.sub_span_cells[0].start_position(), -1);
        let ele = SpansCellElement::new(&mut self.sub_span_cells[0]);
        self.span_position_queue.push(ele);
    }

    fn sub_span_cells_to_positions_queue(&mut self) -> Result<()> {
        self.span_position_queue.clear();
        let queue = &mut self.span_position_queue;
        for cell in &mut self.sub_span_cells {
            debug_assert_eq!(cell.start_position(), -1);
            cell.next_start_position()?;
            debug_assert_ne!(cell.start_position(), NO_MORE_POSITIONS);
            let ele = SpansCellElement::new(cell);
            queue.push(ele);
        }
        Ok(())
    }

    fn at_match(&self) -> bool {
        let min_cell = self.min_cell();
        let max_cell = &self.sub_span_cells[self.max_end_position_cell_idx];
        debug_assert_eq!(min_cell.doc_id(), max_cell.doc_id());
        max_cell.end_position() - min_cell.start_position() - self.total_span_length
            <= self.allowed_slop
    }

    fn min_cell(&self) -> &SpansCell {
        self.span_position_queue.peek().unwrap().spans()
    }
}

impl ConjunctionSpans for NearSpansUnordered {
    fn conjunction_span_base(&self) -> &ConjunctionSpanBase {
        &self.conjunction_span
    }

    fn conjunction_span_base_mut(&mut self) -> &mut ConjunctionSpanBase {
        &mut self.conjunction_span
    }

    fn two_phase_current_doc_matches(&mut self) -> Result<bool> {
        // at doc with all sub_spans
        self.sub_span_cells_to_positions_queue()?;
        loop {
            if self.at_match() {
                self.conjunction_span.first_in_current_doc = true;
                self.conjunction_span.one_exhausted_in_current_doc = false;
                return Ok(true);
            }
            let mut min_cell = self.span_position_queue.peek_mut().unwrap();
            debug_assert_ne!(min_cell.spans_mut().start_position(), NO_MORE_POSITIONS);
            let next = min_cell.spans_mut().next_start_position()?;
            // exhausted a sub_span in current doc
            if next == NO_MORE_POSITIONS {
                return Ok(false);
            }
        }
    }
}

impl Spans for NearSpansUnordered {
    fn next_start_position(&mut self) -> Result<i32> {
        if self.conjunction_span.first_in_current_doc {
            self.conjunction_span.first_in_current_doc = false;
            return Ok(self.min_cell().start_position());
        }
        // initially at current doc
        while self.min_cell().start_position() == -1 {
            self.span_position_queue
                .peek_mut()
                .unwrap()
                .spans_mut()
                .next_start_position()?;
        }

        debug_assert_ne!(self.min_cell().start_position(), NO_MORE_POSITIONS);
        loop {
            if self.span_position_queue
                .peek_mut()
                .unwrap()
                .spans_mut()
                .next_start_position()? == NO_MORE_POSITIONS
            {
                self.conjunction_span.one_exhausted_in_current_doc = true;
                return Ok(NO_MORE_POSITIONS);
            }
            if self.at_match() {
                return Ok(self.min_cell().start_position());
            }
        }
    }

    fn start_position(&self) -> i32 {
        if self.conjunction_span.first_in_current_doc {
            -1
        } else if self.conjunction_span.one_exhausted_in_current_doc {
            NO_MORE_POSITIONS
        } else {
            self.min_cell().start_position()
        }
    }

    fn end_position(&self) -> i32 {
        if self.conjunction_span.first_in_current_doc {
            -1
        } else if self.conjunction_span.one_exhausted_in_current_doc {
            NO_MORE_POSITIONS
        } else {
            self.sub_span_cells[self.max_end_position_cell_idx].end_position()
        }
    }

    fn width(&self) -> i32 {
        self.sub_span_cells[self.max_end_position_cell_idx].start_position()
            - self.min_cell().start_position()
    }

    fn collect(&mut self, collector: &mut SpanCollector) -> Result<()> {
        for cell in &mut self.sub_span_cells {
            cell.collect(collector)?;
        }
        Ok(())
    }

    fn positions_cost(&self) -> f32 {
        // support_two_phase always return `true`
        unreachable!()
    }

    fn support_two_phase(&self) -> bool {
        true
    }
}

conjunction_span_doc_iter!(NearSpansUnordered);

struct SpansCell {
    parent: *mut NearSpansUnordered,
    spans: Box<Spans>,
    index: usize,
    // index of this span in parent's `sub_span_cells` vec
    span_length: i32,
}

unsafe impl Send for SpansCell {}

unsafe impl Sync for SpansCell {}

impl SpansCell {
    pub fn new(parent: *mut NearSpansUnordered, spans: Box<Spans>, index: usize) -> Self {
        SpansCell {
            parent,
            spans,
            index,
            span_length: -1,
        }
    }

    fn adjust_length(&mut self) {
        // subtract old, possibly from a previous doc
        if self.span_length != -1 {
            unsafe {
                (*self.parent).total_span_length -= self.span_length;
            }
        }
        debug_assert_ne!(self.spans.start_position(), NO_MORE_POSITIONS);
        self.span_length = self.end_position() - self.start_position();
        debug_assert!(self.span_length >= 0);
        // add new
        unsafe {
            (*self.parent).total_span_length += self.span_length;
        }
    }

    fn adjust_max(&mut self) {
        unsafe {
            let max_index = (*self.parent).max_end_position_cell_idx;
            let max_cell = &(*self.parent).sub_span_cells[max_index];
            debug_assert_eq!(self.doc_id(), max_cell.doc_id());
            if self.end_position() > max_cell.end_position() {
                (*self.parent).max_end_position_cell_idx = self.index;
            }
        }
    }
}

impl Spans for SpansCell {
    fn next_start_position(&mut self) -> Result<i32> {
        let res = self.spans.next_start_position()?;
        if res != NO_MORE_POSITIONS {
            self.adjust_length();
        }
        self.adjust_max();
        Ok(res)
    }

    fn start_position(&self) -> i32 {
        self.spans.start_position()
    }

    fn end_position(&self) -> i32 {
        self.spans.end_position()
    }

    fn width(&self) -> i32 {
        self.spans.width()
    }

    fn collect(&mut self, collector: &mut SpanCollector) -> Result<()> {
        self.spans.collect(collector)
    }

    fn positions_cost(&self) -> f32 {
        // support_two_phase always return `true`
        self.spans.positions_cost()
    }

    fn support_two_phase(&self) -> bool {
        self.spans.support_two_phase()
    }
}

impl DocIterator for SpansCell {
    fn doc_id(&self) -> i32 {
        self.spans.doc_id()
    }

    fn next(&mut self) -> Result<i32> {
        self.spans.next()
    }

    fn advance(&mut self, target: i32) -> Result<i32> {
        self.spans.advance(target)
    }

    fn cost(&self) -> usize {
        self.spans.cost()
    }
}

// ref of `SpansCell` in PriorityQueue
struct SpansCellElement {
    spans: *mut SpansCell,
}

unsafe impl Send for SpansCellElement {}

unsafe impl Sync for SpansCellElement {}

impl SpansCellElement {
    fn new(span: &mut SpansCell) -> Self {
        SpansCellElement {
            spans: span as *mut SpansCell,
        }
    }

    fn spans(&self) -> &SpansCell {
        unsafe { &*self.spans }
    }

    fn spans_mut(&mut self) -> &mut SpansCell {
        unsafe { &mut *self.spans }
    }
}

impl Eq for SpansCellElement {}

impl PartialEq for SpansCellElement {
    fn eq(&self, other: &SpansCellElement) -> bool {
        unsafe { (*self.spans).index.eq(&(*other.spans).index) }
    }
}

impl Ord for SpansCellElement {
    fn cmp(&self, other: &Self) -> Ordering {
        // reversed ordering for priority queue
        unsafe {
            let span1 = &mut (*self.spans).spans;
            let span2 = &mut (*other.spans).spans;
            debug_assert_eq!(span1.doc_id(), span2.doc_id());
            let start1 = span1.start_position();
            let start2 = span2.start_position();
            if start1 == start2 {
                span2.end_position().cmp(&span1.end_position())
            } else {
                start2.cmp(&start1)
            }
        }
    }
}

impl PartialOrd for SpansCellElement {
    fn partial_cmp(&self, other: &SpansCellElement) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A Spans that is formed from the ordered subspans of a SpanNearQuery
/// where the subspans do not overlap and have a maximum slop between them.
///
/// The formed spans only contains minimum slop matches.<br>
/// The matching slop is computed from the distance(s) between
/// the non overlapping matching Spans.<br>
/// Successive matches are always formed from the successive Spans
/// of the SpanNearQuery.
///
/// The formed spans may contain overlaps when the slop is at least 1.
/// For example, when querying using
/// t1 t2 t3
/// with slop at least 1, the fragment:
/// t1 t2 t1 t3 t2 t3
/// matches twice:
/// t1 t2 .. t3
///       t1 .. t2 t3
///
/// Expert:
/// Only public for subclassing.  Most implementations should not need this class
///
pub struct NearSpansOrdered {
    conjunction_span: ConjunctionSpanBase,
    sub_spans: Vec<Box<Spans>>,
    match_start: i32,
    match_end: i32,
    match_width: i32,
    allowed_slop: i32,
}

impl NearSpansOrdered {
    pub fn new(allowed_slop: i32, sub_spans: Vec<Box<Spans>>) -> Result<Self> {
        let mut sub_spans = sub_spans;
        let conjunction_span = ConjunctionSpanBase::new(&mut sub_spans)?;
        Ok(NearSpansOrdered {
            conjunction_span,
            sub_spans,
            match_start: -1,
            match_end: -1,
            match_width: -1,
            allowed_slop,
        })
    }

    fn unpositioned(&self) -> bool {
        self.sub_spans
            .iter()
            .all(|span| span.start_position() == -1)
    }

    ///
    // Order the subSpans within the same document by using nextStartPosition on all subSpans
    // after the first as little as necessary.
    // Return true when the subSpans could be ordered in this way,
    // otherwise at least one is exhausted in the current doc.
    //
    fn stretch_to_order(&mut self) -> Result<bool> {
        self.match_start = self.sub_spans[0].start_position();
        debug_assert_ne!(self.sub_spans[0].start_position(), NO_MORE_POSITIONS);
        debug_assert_ne!(self.sub_spans[0].end_position(), NO_MORE_POSITIONS);
        self.match_width = 0;
        for i in 1..self.sub_spans.len() {
            debug_assert_ne!(self.sub_spans[i].start_position(), NO_MORE_POSITIONS);
            debug_assert_ne!(self.sub_spans[i].end_position(), NO_MORE_POSITIONS);
            let prev_end_position = self.sub_spans[i - 1].end_position();
            if self.sub_spans[i].advance_position(prev_end_position)? == NO_MORE_POSITIONS {
                self.conjunction_span.one_exhausted_in_current_doc = true;
                return Ok(false);
            }
            self.match_width += self.sub_spans[i].start_position() - prev_end_position;
        }
        self.match_end = self.sub_spans[self.sub_spans.len() - 1].end_position();
        Ok(true)
    }
}

impl ConjunctionSpans for NearSpansOrdered {
    fn conjunction_span_base(&self) -> &ConjunctionSpanBase {
        &self.conjunction_span
    }

    fn conjunction_span_base_mut(&mut self) -> &mut ConjunctionSpanBase {
        &mut self.conjunction_span
    }

    fn two_phase_current_doc_matches(&mut self) -> Result<bool> {
        debug_assert!(self.unpositioned());
        self.conjunction_span.one_exhausted_in_current_doc = false;
        while (self.sub_spans[0].next_start_position()? != NO_MORE_POSITIONS)
            && !self.conjunction_span.one_exhausted_in_current_doc
        {
            if self.stretch_to_order()? && self.match_width <= self.allowed_slop {
                self.conjunction_span.first_in_current_doc = true;
                return Ok(true);
            }
        }
        Ok(false)
    }
}

impl Spans for NearSpansOrdered {
    fn next_start_position(&mut self) -> Result<i32> {
        if self.conjunction_span.first_in_current_doc {
            self.conjunction_span.first_in_current_doc = false;
            return Ok(self.match_start);
        }
        self.conjunction_span.one_exhausted_in_current_doc = false;
        while self.sub_spans[0].next_start_position()? != NO_MORE_POSITIONS
            && !self.conjunction_span.one_exhausted_in_current_doc
        {
            if self.stretch_to_order()? && (self.match_width <= self.allowed_slop) {
                return Ok(self.match_start);
            }
        }
        self.match_start = NO_MORE_POSITIONS;
        self.match_end = NO_MORE_POSITIONS;
        Ok(NO_MORE_POSITIONS)
    }

    fn start_position(&self) -> i32 {
        if self.conjunction_span.first_in_current_doc {
            -1
        } else {
            self.match_start
        }
    }

    fn end_position(&self) -> i32 {
        if self.conjunction_span.first_in_current_doc {
            -1
        } else {
            self.match_end
        }
    }

    fn width(&self) -> i32 {
        self.match_width
    }

    fn collect(&mut self, collector: &mut SpanCollector) -> Result<()> {
        for span in &mut self.sub_spans {
            span.collect(collector)?;
        }
        Ok(())
    }

    fn positions_cost(&self) -> f32 {
        unreachable!()
    }

    fn support_two_phase(&self) -> bool {
        true
    }
}

conjunction_span_doc_iter!(NearSpansOrdered);

const SPAN_GAP_QUERY: &str = "span_gap";

pub struct SpanGapQuery {
    field: String,
    width: i32,
}

impl SpanGapQuery {
    pub fn new(field: String, width: i32) -> Self {
        SpanGapQuery { field, width }
    }
}

impl Query for SpanGapQuery {
    fn create_weight(&self, searcher: &IndexSearcher, _needs_scores: bool) -> Result<Box<Weight>> {
        let weight = SpanGapWeight::new(self, searcher, self.width)?;
        Ok(Box::new(weight))
    }

    fn extract_terms(&self) -> Vec<TermQuery> {
        vec![]
    }

    fn query_type(&self) -> &'static str {
        SPAN_GAP_QUERY
    }
}

impl SpanQuery for SpanGapQuery {
    fn field(&self) -> &str {
        &self.field
    }

    fn span_weight(
        &self,
        searcher: &IndexSearcher,
        _needs_scores: bool,
    ) -> Result<Box<SpanWeight>> {
        let weight = SpanGapWeight::new(self, searcher, self.width)?;
        Ok(Box::new(weight))
    }
}

impl fmt::Display for SpanGapQuery {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SpanGapQuery(field: {}, width: {})",
            &self.field, self.width
        )
    }
}

struct SpanGapWeight {
    width: i32,
    sim_weight: Option<Box<SimWeight>>,
}

impl SpanGapWeight {
    pub fn new(query: &SpanGapQuery, searcher: &IndexSearcher, width: i32) -> Result<Self> {
        let sim_weight = build_sim_weight(query.field(), searcher, HashMap::new(), None)?;
        Ok(SpanGapWeight { width, sim_weight })
    }
}

impl SpanWeight for SpanGapWeight {
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
        _reader: &LeafReader,
        _required_postings: &PostingsFlag,
    ) -> Result<Option<Box<Spans>>> {
        Ok(Some(Box::new(GapSpans::new(self.width))))
    }

    fn extract_term_contexts(&self, _contexts: &mut HashMap<Term, Arc<TermContext>>) {}
}

impl Weight for SpanGapWeight {
    fn create_scorer(&self, leaf_reader: &LeafReader) -> Result<Box<Scorer>> {
        self.do_create_scorer(leaf_reader)
    }

    fn query_type(&self) -> &'static str {
        SPAN_GAP_QUERY
    }

    fn normalize(&mut self, norm: f32, boost: f32) {
        if let Some(ref mut sim_weight) = self.sim_weight {
            sim_weight.normalize(norm, boost)
        }
    }

    fn value_for_normalization(&self) -> f32 {
        if let Some(ref sim_weight) = self.sim_weight {
            sim_weight.get_value_for_normalization()
        } else {
            1.0
        }
    }

    fn needs_scores(&self) -> bool {
        false
    }

    fn explain(&self, reader: &LeafReader, doc: DocId) -> Result<Explanation> {
        self.explain_span(reader, doc)
    }
}

impl fmt::Display for SpanGapWeight {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SpanGapWeight(width: {})", self.width)
    }
}

struct GapSpans {
    doc: DocId,
    pos: i32,
    width: i32,
}

impl GapSpans {
    pub fn new(width: i32) -> Self {
        GapSpans {
            doc: -1,
            pos: -1,
            width,
        }
    }

    fn skip_to_position(&mut self, position: i32) {
        self.pos = position;
    }
}

impl Spans for GapSpans {
    fn next_start_position(&mut self) -> Result<i32> {
        self.pos += 1;
        Ok(self.pos)
    }

    fn start_position(&self) -> i32 {
        self.pos
    }

    fn end_position(&self) -> i32 {
        self.pos + self.width
    }

    fn width(&self) -> i32 {
        self.width
    }

    fn collect(&mut self, _collector: &mut SpanCollector) -> Result<()> {
        Ok(())
    }

    fn positions_cost(&self) -> f32 {
        0.0
    }

    fn support_two_phase(&self) -> bool {
        false
    }

    fn advance_position(&mut self, position: i32) -> Result<i32> {
        self.skip_to_position(position);
        Ok(self.pos)
    }
}

impl DocIterator for GapSpans {
    fn doc_id(&self) -> i32 {
        self.doc
    }

    fn next(&mut self) -> Result<i32> {
        self.pos = -1;
        self.doc += 1;
        Ok(self.doc)
    }

    fn advance(&mut self, target: i32) -> Result<i32> {
        self.pos = -1;
        self.doc = target;
        Ok(self.doc)
    }

    fn cost(&self) -> usize {
        0
    }
}
