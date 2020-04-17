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
use core::search::query::spans::{SpanCollector, SpanQuery, SpanWeight, Spans};
use core::search::searcher::SearchPlanBuilder;
use core::search::{
    query::Query, query::TermQuery, query::Weight, scorer::Scorer, similarity::SimWeight,
    DocIterator,
};
use core::util::DisiPriorityQueue;
use core::util::DocId;

use error::{ErrorKind, Result};

use core::codec::PostingIterator;
use core::search::query::spans::span::term_keys;
use std::cmp::{max, Ordering};
use std::collections::BinaryHeap;
use std::fmt;

const SPAN_OR_QUERY: &str = "span_or";

pub struct SpanOrQuery {
    field: String,
    clauses: Vec<SpanQueryEnum>,
}

impl SpanOrQuery {
    pub fn new(clauses: Vec<SpanQueryEnum>) -> Result<Self> {
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
        Ok(SpanOrQuery { field, clauses })
    }

    fn span_or_weight<C: Codec>(
        &self,
        searcher: &dyn SearchPlanBuilder<C>,
        needs_scores: bool,
    ) -> Result<SpanOrWeight<C>> {
        let mut sub_weights = Vec::with_capacity(self.clauses.len());
        for clause in &self.clauses {
            sub_weights.push(clause.span_weight(searcher, needs_scores)?);
        }
        let terms = if needs_scores {
            term_keys(&sub_weights)
        } else {
            Vec::new()
        };
        SpanOrWeight::new(self, sub_weights, searcher, terms)
    }
}

impl<C: Codec> SpanQuery<C> for SpanOrQuery {
    type Weight = SpanOrWeight<C>;

    fn span_weight(
        &self,
        searcher: &dyn SearchPlanBuilder<C>,
        needs_scores: bool,
    ) -> Result<Self::Weight> {
        self.span_or_weight(searcher, needs_scores)
    }

    fn field(&self) -> &str {
        &self.field
    }
}

impl<C: Codec> Query<C> for SpanOrQuery {
    fn create_weight(
        &self,
        searcher: &dyn SearchPlanBuilder<C>,
        needs_scores: bool,
    ) -> Result<Box<dyn Weight<C>>> {
        Ok(Box::new(self.span_or_weight(searcher, needs_scores)?))
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

pub struct SpanOrWeight<C: Codec> {
    sim_weight: Option<Box<dyn SimWeight<C>>>,
    sub_weights: Vec<SpanWeightEnum<C>>,
}

impl<C: Codec> SpanOrWeight<C> {
    pub fn new<IS: SearchPlanBuilder<C> + ?Sized>(
        query: &SpanOrQuery,
        sub_weights: Vec<SpanWeightEnum<C>>,
        searcher: &IS,
        terms: Vec<Term>,
    ) -> Result<Self> {
        assert!(sub_weights.len() >= 2);
        let sim_weight = build_sim_weight(SpanQuery::<C>::field(query), searcher, terms, None)?;
        Ok(SpanOrWeight {
            sim_weight,
            sub_weights,
        })
    }
}

impl<C: Codec> SpanWeight<C> for SpanOrWeight<C> {
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
        reader: &LeafReaderContext<'_, C>,
        required_postings: &PostingsFlag,
    ) -> Result<Option<SpansEnum<CodecPostingIterator<C>>>> {
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

        Ok(Some(SpansEnum::Or(SpanOrSpans::new(
            by_doc_queue,
            BinaryHeap::with_capacity(capasity),
        ))))
    }

    fn extract_term_keys(&self, terms: &mut Vec<Term>) {
        for spans in &self.sub_weights {
            spans.extract_term_keys(terms)
        }
    }
}

impl<C: Codec> Weight<C> for SpanOrWeight<C> {
    fn create_scorer(&self, ctx: &LeafReaderContext<'_, C>) -> Result<Option<Box<dyn Scorer>>> {
        self.do_create_scorer(ctx)
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

    fn explain(&self, reader: &LeafReaderContext<'_, C>, doc: DocId) -> Result<Explanation> {
        self.explain_span(reader, doc)
    }
}

impl<C: Codec> fmt::Display for SpanOrWeight<C> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let weights = {
            let query_strs: Vec<String> =
                self.sub_weights.iter().map(|q| format!("{}", q)).collect();
            query_strs.join(", ")
        };
        write!(f, "SpanOrWeight(sub_weight: [{}])", weights)
    }
}

pub struct SpanOrSpans<P: PostingIterator> {
    by_doc_queue: DisiPriorityQueue<SpansEnum<P>>,
    by_position_queue: BinaryHeap<SpansElement<P>>,
    top_position_spans_valid: bool,
    positions_cost: f32,
    last_doc_two_phase_matched: DocId,
    match_cost: f32,
    cost: usize,
    support_two_phase: bool,
}

unsafe impl<P: PostingIterator> Send for SpanOrSpans<P> {}

impl<P: PostingIterator> SpanOrSpans<P> {
    fn new(
        by_doc_queue: DisiPriorityQueue<SpansEnum<P>>,
        by_position_queue: BinaryHeap<SpansElement<P>>,
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
                self.by_position_queue.push(SpansElement::new(
                    list_at_current_doc.inner_mut() as *mut SpansEnum<P>
                ));
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

impl<P: PostingIterator> Spans for SpanOrSpans<P> {
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
        Ok(self
            .by_position_queue
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

    fn collect(&mut self, collector: &mut impl SpanCollector) -> Result<()> {
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
}

impl<P: PostingIterator> DocIterator for SpanOrSpans<P> {
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

    fn support_two_phase(&self) -> bool {
        self.support_two_phase
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

struct SpansElement<P: PostingIterator> {
    spans_ptr: *mut SpansEnum<P>,
}

impl<P: PostingIterator> SpansElement<P> {
    fn new(spans_ptr: *mut SpansEnum<P>) -> Self {
        SpansElement { spans_ptr }
    }

    #[allow(clippy::mut_from_ref)]
    #[inline]
    fn spans(&self) -> &mut SpansEnum<P> {
        unsafe { &mut *self.spans_ptr }
    }
}

impl<P: PostingIterator> Eq for SpansElement<P> {}

impl<P: PostingIterator> PartialEq for SpansElement<P> {
    fn eq(&self, other: &SpansElement<P>) -> bool {
        self.spans().doc_id().eq(&other.spans().doc_id())
    }
}

impl<P: PostingIterator> Ord for SpansElement<P> {
    fn cmp(&self, other: &Self) -> Ordering {
        // reversed order by doc id for BinaryHeap
        other.spans().doc_id().cmp(&self.spans().doc_id())
    }
}

impl<P: PostingIterator> PartialOrd for SpansElement<P> {
    fn partial_cmp(&self, other: &SpansElement<P>) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
