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

use core::codec::PostingIterator;
use core::codec::{Codec, CodecPostingIterator};
use core::codec::{TermIterator, Terms};
use core::doc::Term;
use core::index::reader::LeafReaderContext;
use core::search::explanation::Explanation;
use core::search::query::spans::{build_sim_weight, PostingsFlag, SpansEnum, NO_MORE_POSITIONS};
use core::search::query::spans::{SpanCollector, SpanQuery, SpanWeight, Spans};
use core::search::searcher::SearchPlanBuilder;
use core::search::{
    query::Query, query::TermQuery, query::Weight, scorer::Scorer, similarity::SimWeight,
    DocIterator, NO_MORE_DOCS,
};
use core::util::{DocId, KeyedContext};

use error::{ErrorKind, Result};

use std::fmt;

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

impl<C: Codec> Query<C> for SpanTermQuery {
    fn create_weight(
        &self,
        searcher: &dyn SearchPlanBuilder<C>,
        needs_scores: bool,
    ) -> Result<Box<dyn Weight<C>>> {
        Ok(Box::new(SpanTermWeight::new(
            self,
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

    fn as_any(&self) -> &dyn (::std::any::Any) {
        self
    }
}

impl<C: Codec> SpanQuery<C> for SpanTermQuery {
    type Weight = SpanTermWeight<C>;

    fn span_weight(
        &self,
        searcher: &dyn SearchPlanBuilder<C>,
        needs_scores: bool,
    ) -> Result<Self::Weight> {
        Ok(SpanTermWeight::new(
            self,
            searcher,
            self.ctx.clone(),
            needs_scores,
        )?)
    }

    fn field(&self) -> &str {
        &self.term.field
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

pub struct TermSpans<T: PostingIterator> {
    postings: T,
    term: Term,
    doc: DocId,
    freq: i32,
    count: i32,
    position: i32,
    positions_cost: f32,
}

impl<T: PostingIterator> TermSpans<T> {
    pub fn new(postings: T, term: Term, positions_cost: f32) -> Self {
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

impl<T: PostingIterator> DocIterator for TermSpans<T> {
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

impl<T: PostingIterator> Spans for TermSpans<T> {
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

    fn collect(&mut self, collector: &mut impl SpanCollector) -> Result<()> {
        collector.collect_leaf(&self.postings, self.position, &self.term)
    }

    fn positions_cost(&self) -> f32 {
        self.positions_cost
    }
}

/// A guess of the relative cost of dealing with the term positions
/// when using a SpanNearQuery instead of a PhraseQuery.
const PHRASE_TO_SPAN_TERM_POSITIONS_COST: f32 = 4.0;
const TERM_POSNS_SEEK_OPS_PER_DOC: i32 = 128;
const TERM_OPS_PER_POS: i32 = 7;

pub struct SpanTermWeight<C: Codec> {
    term: Term,
    sim_weight: Option<Box<dyn SimWeight<C>>>,
}

impl<C: Codec> SpanTermWeight<C> {
    pub fn new<IS: SearchPlanBuilder<C> + ?Sized>(
        query: &SpanTermQuery,
        searcher: &IS,
        ctx: Option<KeyedContext>,
        _needs_scores: bool,
    ) -> Result<Self> {
        let sim_weight =
            build_sim_weight(query.term.field(), searcher, vec![query.term.clone()], ctx)?;
        Ok(SpanTermWeight {
            term: query.term.clone(),
            sim_weight,
        })
    }

    fn term_positions_cost(terms_iter: &mut impl TermIterator) -> Result<f32> {
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

impl<C: Codec> SpanWeight<C> for SpanTermWeight<C> {
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
        if let Some(terms) = reader.reader.terms(self.term.field())? {
            if !terms.has_positions()? {
                bail!(ErrorKind::IllegalState(format!(
                    "field '{}' was indexed without position data; cannot run SpanTermQuery \
                     (term={:?})",
                    &self.term.field,
                    &self.term.text()
                )));
            }
            let mut terms_iter = terms.iterator()?;
            terms_iter.seek_exact(&self.term.bytes)?;
            let postings = terms_iter.postings_with_flags(required_postings.required_postings())?;
            let positions_cost =
                Self::term_positions_cost(&mut terms_iter)? + PHRASE_TO_SPAN_TERM_POSITIONS_COST;
            return Ok(Some(SpansEnum::Term(TermSpans::new(
                postings,
                self.term.clone(),
                positions_cost,
            ))));
        }
        Ok(None)
    }

    fn extract_term_keys(&self, terms: &mut Vec<Term>) {
        terms.push(self.term.clone());
    }
}

impl<C: Codec> Weight<C> for SpanTermWeight<C> {
    fn create_scorer(&self, ctx: &LeafReaderContext<'_, C>) -> Result<Option<Box<dyn Scorer>>> {
        self.do_create_scorer(ctx)
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

    fn explain(&self, reader: &LeafReaderContext<'_, C>, doc: DocId) -> Result<Explanation> {
        self.explain_span(reader, doc)
    }
}

impl<C: Codec> fmt::Display for SpanTermWeight<C> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SpanTermWeight(field: {}, term: {})",
            self.term.field,
            self.term.text().unwrap()
        )
    }
}
