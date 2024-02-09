// Copyright 2019 Zhizhesihai (Beijing) Technology Limited.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use core::codec::{Codec, CodecEnum, CodecPostingIterator};
use core::doc::Term;
use core::index::reader::LeafReaderContext;
use core::search::explanation::Explanation;
use core::search::query::spans::{
    build_sim_weight, PostingsFlag, SpanQueryEnum, SpanWeightEnum, SpansEnum, NO_MORE_POSITIONS,
};
use core::search::query::spans::{ConjunctionSpanBase, ConjunctionSpans};
use core::search::query::spans::{SpanCollector, SpanQuery, SpanWeight, Spans};
use core::search::searcher::SearchPlanBuilder;
use core::search::{
    query::Query, query::TermQuery, query::Weight, scorer::Scorer, similarity::SimWeight,
    DocIterator, NO_MORE_DOCS,
};
use core::util::{DocId, KeyedContext, BM25_SIMILARITY_IDF};

use error::{ErrorKind, Result};

use core::codec::PostingIterator;
use core::search::query::spans::span::term_keys;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fmt;
use std::mem::MaybeUninit;
use std::ptr;

pub struct SpanNearQueryBuilder {
    ordered: bool,
    field: String,
    clauses: Vec<SpanQueryEnum>,
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

    pub fn add_clause(mut self, clause: SpanQueryEnum) -> Result<Self> {
        if SpanQuery::<CodecEnum>::field(&clause) != self.field {
            bail!(ErrorKind::IllegalArgument(format!(
                "clause field is '{}' not equal with field '{}'",
                SpanQuery::<CodecEnum>::field(&clause),
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
        self.clauses.push(SpanQueryEnum::Gap(SpanGapQuery::new(
            self.field.clone(),
            width,
        )));
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
    clauses: Vec<SpanQueryEnum>,
    slop: i32,
    in_order: bool,
    field: String,
}

impl SpanNearQuery {
    pub fn new(clauses: Vec<SpanQueryEnum>, slop: i32, in_order: bool) -> Result<Self> {
        if clauses.len() < 2 {
            bail!(ErrorKind::IllegalArgument(
                "clauses length must not be smaller than 2!".into()
            ));
        }
        for i in 0..clauses.len() - 1 {
            if SpanQuery::<CodecEnum>::field(&clauses[i])
                != SpanQuery::<CodecEnum>::field(&clauses[i + 1])
            {
                bail!(ErrorKind::IllegalArgument(
                    "Clauses must have same field.".into()
                ));
            }
        }
        let field = SpanQuery::<CodecEnum>::field(&clauses[0]).to_string();
        Ok(SpanNearQuery {
            clauses,
            slop,
            in_order,
            field,
        })
    }

    fn merge_idf_ctx(
        ctx1: Option<KeyedContext>,
        ctx2: Option<KeyedContext>,
    ) -> Option<KeyedContext> {
        if ctx1.is_none() {
            ctx2
        } else if ctx2.is_none() {
            ctx1
        } else {
            let mut ctx = KeyedContext::default();
            let w = ctx1.map_or(0f32, |v| v.get_float(BM25_SIMILARITY_IDF).unwrap_or(0f32))
                + ctx2.map_or(0f32, |v| v.get_float(BM25_SIMILARITY_IDF).unwrap_or(0f32));
            ctx.set_float(BM25_SIMILARITY_IDF, w);
            Some(ctx)
        }
    }

    fn span_near_weight<C: Codec>(
        &self,
        searcher: &dyn SearchPlanBuilder<C>,
        needs_scores: bool,
    ) -> Result<SpanNearWeight<C>> {
        let mut sub_weights = Vec::with_capacity(self.clauses.len());
        let mut ctx = None;
        for clause in &self.clauses {
            sub_weights.push(clause.span_weight(searcher, needs_scores)?);
            ctx = Self::merge_idf_ctx(ctx, SpanQuery::<C>::ctx(clause));
        }
        let terms = if needs_scores {
            term_keys(&sub_weights)
        } else {
            Vec::new()
        };
        SpanNearWeight::new(self, sub_weights, searcher, terms, ctx)
    }
}

impl<C: Codec> SpanQuery<C> for SpanNearQuery {
    type Weight = SpanNearWeight<C>;

    fn span_weight(
        &self,
        searcher: &dyn SearchPlanBuilder<C>,
        needs_scores: bool,
    ) -> Result<Self::Weight> {
        self.span_near_weight(searcher, needs_scores)
    }

    fn field(&self) -> &str {
        &self.field
    }
}

impl<C: Codec> Query<C> for SpanNearQuery {
    fn create_weight(
        &self,
        searcher: &dyn SearchPlanBuilder<C>,
        needs_scores: bool,
    ) -> Result<Box<dyn Weight<C>>> {
        let weight = self.span_near_weight(searcher, needs_scores)?;
        Ok(Box::new(weight))
    }

    fn extract_terms(&self) -> Vec<TermQuery> {
        self.clauses
            .iter()
            .flat_map(|c| Query::<C>::extract_terms(c))
            .collect()
    }

    fn as_any(&self) -> &dyn (::std::any::Any) {
        self
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

pub struct SpanNearWeight<C: Codec> {
    field: String,
    in_order: bool,
    slop: i32,
    sim_weight: Option<Box<dyn SimWeight<C>>>,
    sub_weights: Vec<SpanWeightEnum<C>>,
}

impl<C: Codec> SpanNearWeight<C> {
    pub fn new<IS: SearchPlanBuilder<C> + ?Sized>(
        query: &SpanNearQuery,
        sub_weights: Vec<SpanWeightEnum<C>>,
        searcher: &IS,
        terms: Vec<Term>,
        ctx: Option<KeyedContext>,
    ) -> Result<Self> {
        let field = SpanQuery::<C>::field(query).to_string();
        let sim_weight = build_sim_weight(&field, searcher, terms, ctx)?;
        Ok(SpanNearWeight {
            field,
            sim_weight,
            sub_weights,
            in_order: query.in_order,
            slop: query.slop,
        })
    }
}

impl<C: Codec> SpanWeight<C> for SpanNearWeight<C> {
    fn sim_weight(&self) -> Option<&dyn SimWeight<C>> {
        self.sim_weight.as_ref().map(|x| &**x)
    }

    fn sim_weight_mut(&mut self) -> Option<&mut dyn SimWeight<C>> {
        if let Some(ref mut sim_weight) = self.sim_weight {
            Some(sim_weight.as_mut())
        } else {
            None
        }
    }

    fn get_spans(
        &self,
        ctx: &LeafReaderContext<'_, C>,
        required_postings: &PostingsFlag,
    ) -> Result<Option<SpansEnum<CodecPostingIterator<C>>>> {
        if ctx.reader.terms(&self.field)?.is_some() {
            let mut sub_spans = Vec::with_capacity(self.sub_weights.len());
            for w in &self.sub_weights {
                if let Some(span) = w.get_spans(ctx, required_postings)? {
                    sub_spans.push(span);
                } else {
                    // all required
                    return Ok(None);
                }
            }
            // all near_spans require at least two sub_spans
            if self.in_order {
                return Ok(Some(SpansEnum::NearOrdered(NearSpansOrdered::new(
                    self.slop, sub_spans,
                )?)));
            } else {
                return Ok(Some(SpansEnum::NearUnordered(NearSpansUnordered::new(
                    self.slop, sub_spans,
                )?)));
            }
        }
        Ok(None)
    }

    fn extract_term_keys(&self, terms: &mut Vec<Term>) {
        for weight in &self.sub_weights {
            weight.extract_term_keys(terms)
        }
    }
}

impl<C: Codec> Weight<C> for SpanNearWeight<C> {
    fn create_scorer(&self, ctx: &LeafReaderContext<'_, C>) -> Result<Option<Box<dyn Scorer>>> {
        self.do_create_scorer(ctx)
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

    fn explain(&self, reader: &LeafReaderContext<'_, C>, doc: DocId) -> Result<Explanation> {
        self.explain_span(reader, doc)
    }
}

impl<C: Codec> fmt::Display for SpanNearWeight<C> {
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

pub struct NearSpansUnordered<P: PostingIterator> {
    conjunction_span: MaybeUninit<ConjunctionSpanBase<P>>,
    sub_span_cells: Vec<SpansCell<P>>,
    allowed_slop: i32,
    span_position_queue: BinaryHeap<SpansCellElement<P>>,
    total_span_length: i32,
    max_end_position_cell_idx: usize,
}

impl<P: PostingIterator> Drop for NearSpansUnordered<P> {
    fn drop(&mut self) {
        unsafe {
            ptr::drop_in_place(self.conjunction_span.as_mut_ptr());
        }
    }
}

impl<P: PostingIterator> NearSpansUnordered<P> {
    #[allow(clippy::explicit_counter_loop)]
    // TODO return Box<> to make sure the span_cell's pointer to parent won't change
    pub fn new(
        allowed_slop: i32,
        sub_spans: Vec<SpansEnum<P>>,
    ) -> Result<Box<NearSpansUnordered<P>>> {
        let mut res = Box::new(NearSpansUnordered {
            conjunction_span: MaybeUninit::<ConjunctionSpanBase<P>>::uninit(),
            sub_span_cells: Vec::with_capacity(sub_spans.len()),
            allowed_slop,
            span_position_queue: BinaryHeap::with_capacity(sub_spans.len()),
            total_span_length: 0,
            max_end_position_cell_idx: 0,
        });
        let mut index = 0usize;
        for spans in sub_spans {
            let span_cell =
                SpansCell::new(res.as_mut() as *mut NearSpansUnordered<P>, spans, index);
            index += 1;
            res.sub_span_cells.push(span_cell);
        }
        let conjunction_span = {
            let spans: Vec<_> = res
                .sub_span_cells
                .iter_mut()
                .map(|s| &mut s.spans)
                .collect();
            ConjunctionSpanBase::new(spans)?
        };

        res.conjunction_span.write(conjunction_span);
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

    fn min_cell(&self) -> &SpansCell<P> {
        self.span_position_queue.peek().unwrap().spans()
    }
}

impl<P: PostingIterator> ConjunctionSpans<P> for NearSpansUnordered<P> {
    fn conjunction_span_base(&self) -> &ConjunctionSpanBase<P> {
        unsafe { self.conjunction_span.assume_init_ref() }
    }

    fn conjunction_span_base_mut(&mut self) -> &mut ConjunctionSpanBase<P> {
        unsafe { self.conjunction_span.assume_init_mut() }
    }

    fn two_phase_current_doc_matches(&mut self) -> Result<bool> {
        // at doc with all sub_spans
        self.sub_span_cells_to_positions_queue()?;
        loop {
            if self.at_match() {
                unsafe {
                    self.conjunction_span.assume_init_mut().first_in_current_doc = true;
                    self.conjunction_span.assume_init_mut().one_exhausted_in_current_doc = false;
                }
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

impl<P: PostingIterator> Spans for NearSpansUnordered<P> {
    fn next_start_position(&mut self) -> Result<i32> {
        unsafe {
            if self.conjunction_span.assume_init_ref().first_in_current_doc {
                self.conjunction_span.assume_init_mut().first_in_current_doc = false;
                return Ok(self.min_cell().start_position());
            }
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
            if self
                .span_position_queue
                .peek_mut()
                .unwrap()
                .spans_mut()
                .next_start_position()?
                == NO_MORE_POSITIONS
            {
                unsafe {
                    self.conjunction_span.assume_init_mut().one_exhausted_in_current_doc = true;
                }
                return Ok(NO_MORE_POSITIONS);
            }
            if self.at_match() {
                return Ok(self.min_cell().start_position());
            }
        }
    }

    fn start_position(&self) -> i32 {
        unsafe {
            if self.conjunction_span.assume_init_ref().first_in_current_doc {
                -1
            } else if self.conjunction_span.assume_init_ref().one_exhausted_in_current_doc {
                NO_MORE_POSITIONS
            } else {
                self.min_cell().start_position()
            }
        }
    }

    fn end_position(&self) -> i32 {
        unsafe {
            if self.conjunction_span.assume_init_ref().first_in_current_doc {
                -1
            } else if self.conjunction_span.assume_init_ref().one_exhausted_in_current_doc {
                NO_MORE_POSITIONS
            } else {
                self.sub_span_cells[self.max_end_position_cell_idx].end_position()
            }
        }
    }

    fn width(&self) -> i32 {
        self.sub_span_cells[self.max_end_position_cell_idx].start_position()
            - self.min_cell().start_position()
    }

    fn collect(&mut self, collector: &mut impl SpanCollector) -> Result<()> {
        for cell in &mut self.sub_span_cells {
            cell.collect(collector)?;
        }
        Ok(())
    }

    fn positions_cost(&self) -> f32 {
        // support_two_phase always return `true`
        unreachable!()
    }
}

conjunction_span_doc_iter!(NearSpansUnordered<P: PostingIterator>);

pub struct SpansCell<P: PostingIterator> {
    parent: *mut NearSpansUnordered<P>,
    spans: SpansEnum<P>,
    index: usize,
    // index of this span in parent's `sub_span_cells` vec
    span_length: i32,
}

unsafe impl<P: PostingIterator> Send for SpansCell<P> {}

impl<P: PostingIterator> SpansCell<P> {
    pub fn new(parent: *mut NearSpansUnordered<P>, spans: SpansEnum<P>, index: usize) -> Self {
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

impl<P: PostingIterator> Spans for SpansCell<P> {
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

    fn collect(&mut self, collector: &mut impl SpanCollector) -> Result<()> {
        self.spans.collect(collector)
    }

    fn positions_cost(&self) -> f32 {
        // support_two_phase always return `true`
        self.spans.positions_cost()
    }
}

impl<P: PostingIterator> DocIterator for SpansCell<P> {
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

    fn matches(&mut self) -> Result<bool> {
        self.spans.matches()
    }

    fn match_cost(&self) -> f32 {
        self.spans.match_cost()
    }

    fn support_two_phase(&self) -> bool {
        self.spans.support_two_phase()
    }

    fn approximate_next(&mut self) -> Result<DocId> {
        self.spans.approximate_next()
    }

    fn approximate_advance(&mut self, target: i32) -> Result<DocId> {
        self.spans.approximate_advance(target)
    }
}

// ref of `SpansCell` in PriorityQueue
struct SpansCellElement<P: PostingIterator> {
    spans: *mut SpansCell<P>,
}

unsafe impl<P: PostingIterator> Send for SpansCellElement<P> {}

impl<P: PostingIterator> SpansCellElement<P> {
    fn new(span: &mut SpansCell<P>) -> Self {
        SpansCellElement {
            spans: span as *mut SpansCell<P>,
        }
    }

    fn spans(&self) -> &SpansCell<P> {
        unsafe { &*self.spans }
    }

    fn spans_mut(&mut self) -> &mut SpansCell<P> {
        unsafe { &mut *self.spans }
    }
}

impl<P: PostingIterator> Eq for SpansCellElement<P> {}

impl<P: PostingIterator> PartialEq for SpansCellElement<P> {
    fn eq(&self, other: &SpansCellElement<P>) -> bool {
        unsafe { (*self.spans).index.eq(&(*other.spans).index) }
    }
}

impl<P: PostingIterator> Ord for SpansCellElement<P> {
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

impl<P: PostingIterator> PartialOrd for SpansCellElement<P> {
    fn partial_cmp(&self, other: &SpansCellElement<P>) -> Option<Ordering> {
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
pub struct NearSpansOrdered<P: PostingIterator> {
    conjunction_span: ConjunctionSpanBase<P>,
    sub_spans: Vec<SpansEnum<P>>,
    match_start: i32,
    match_end: i32,
    match_width: i32,
    allowed_slop: i32,
}

impl<P: PostingIterator> NearSpansOrdered<P> {
    pub fn new(allowed_slop: i32, mut sub_spans: Vec<SpansEnum<P>>) -> Result<Self> {
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

impl<P: PostingIterator> ConjunctionSpans<P> for NearSpansOrdered<P> {
    fn conjunction_span_base(&self) -> &ConjunctionSpanBase<P> {
        &self.conjunction_span
    }

    fn conjunction_span_base_mut(&mut self) -> &mut ConjunctionSpanBase<P> {
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

impl<P: PostingIterator> Spans for NearSpansOrdered<P> {
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

    fn collect(&mut self, collector: &mut impl SpanCollector) -> Result<()> {
        for span in &mut self.sub_spans {
            span.collect(collector)?;
        }
        Ok(())
    }

    fn positions_cost(&self) -> f32 {
        unreachable!()
    }
}

conjunction_span_doc_iter!(NearSpansOrdered<P: PostingIterator>);

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

impl<C: Codec> Query<C> for SpanGapQuery {
    fn create_weight(
        &self,
        searcher: &dyn SearchPlanBuilder<C>,
        _needs_scores: bool,
    ) -> Result<Box<dyn Weight<C>>> {
        let weight = SpanGapWeight::new(self, searcher, self.width)?;
        Ok(Box::new(weight))
    }

    fn extract_terms(&self) -> Vec<TermQuery> {
        vec![]
    }

    fn as_any(&self) -> &dyn (::std::any::Any) {
        self
    }
}

impl<C: Codec> SpanQuery<C> for SpanGapQuery {
    type Weight = SpanGapWeight<C>;

    fn span_weight(
        &self,
        searcher: &dyn SearchPlanBuilder<C>,
        _needs_scores: bool,
    ) -> Result<Self::Weight> {
        SpanGapWeight::new(self, searcher, self.width)
    }

    fn field(&self) -> &str {
        &self.field
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

pub struct SpanGapWeight<C: Codec> {
    width: i32,
    sim_weight: Option<Box<dyn SimWeight<C>>>,
}

impl<C: Codec> SpanGapWeight<C> {
    pub fn new<IS: SearchPlanBuilder<C> + ?Sized>(
        query: &SpanGapQuery,
        searcher: &IS,
        width: i32,
    ) -> Result<Self> {
        let sim_weight =
            build_sim_weight(SpanQuery::<C>::field(query), searcher, Vec::new(), None)?;
        Ok(SpanGapWeight { width, sim_weight })
    }
}

impl<C: Codec> SpanWeight<C> for SpanGapWeight<C> {
    fn sim_weight(&self) -> Option<&dyn SimWeight<C>> {
        self.sim_weight.as_ref().map(|x| &**x)
    }

    fn sim_weight_mut(&mut self) -> Option<&mut dyn SimWeight<C>> {
        if let Some(ref mut sim_weight) = self.sim_weight {
            Some(sim_weight.as_mut())
        } else {
            None
        }
        // lifetime mismatch
        // self.sim_weight.as_mut().map(|x| &mut **x)
    }

    fn get_spans(
        &self,
        _reader: &LeafReaderContext<'_, C>,
        _required_postings: &PostingsFlag,
    ) -> Result<Option<SpansEnum<CodecPostingIterator<C>>>> {
        Ok(Some(SpansEnum::Gap(GapSpans::new(self.width))))
    }

    fn extract_term_keys(&self, _terms: &mut Vec<Term>) {}
}

impl<C: Codec> Weight<C> for SpanGapWeight<C> {
    fn create_scorer(&self, ctx: &LeafReaderContext<'_, C>) -> Result<Option<Box<dyn Scorer>>> {
        self.do_create_scorer(ctx)
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

    fn explain(&self, reader: &LeafReaderContext<'_, C>, doc: DocId) -> Result<Explanation> {
        self.explain_span(reader, doc)
    }
}

impl<C: Codec> fmt::Display for SpanGapWeight<C> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SpanGapWeight(width: {})", self.width)
    }
}

pub struct GapSpans {
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

    fn collect(&mut self, _collector: &mut impl SpanCollector) -> Result<()> {
        Ok(())
    }

    fn positions_cost(&self) -> f32 {
        0.0
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
