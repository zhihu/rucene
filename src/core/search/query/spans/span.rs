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

use core::codec::{Codec, CodecPostingIterator};
use core::codec::{PostingIterator, PostingIteratorFlags};
use core::doc::Term;
use core::index::reader::{LeafReaderContext, SearchLeafReader};
use core::search::explanation::Explanation;
use core::search::query::spans::{
    GapSpans, NearSpansOrdered, NearSpansUnordered, SpanGapQuery, SpanGapWeight, SpanNearQuery,
    SpanNearWeight,
};
use core::search::query::spans::{SpanBoostQuery, SpanBoostWeight, SpanBoostWeightEnum};
use core::search::query::spans::{SpanOrQuery, SpanOrSpans, SpanOrWeight};
use core::search::query::spans::{SpanTermQuery, SpanTermWeight, TermSpans};
use core::search::query::{Query, TermQuery, Weight};
use core::search::scorer::{ConjunctionScorer, Scorer};
use core::search::searcher::SearchPlanBuilder;
use core::search::similarity::{SimScorer, SimWeight};
use core::search::{DocIterator, NO_MORE_DOCS};
use core::util::{DocId, KeyedContext};

use error::{ErrorKind, Result};

use core::search::statistics::CollectionStatistics;
use std::fmt;

pub fn term_keys<C: Codec, T: SpanWeight<C>>(weights: &[T]) -> Vec<Term> {
    let mut terms = Vec::new();
    for w in weights {
        w.extract_term_keys(&mut terms);
    }
    terms
}

/// Base class for span-based queries.
pub trait SpanQuery<C: Codec>: Query<C> {
    type Weight: SpanWeight<C>;

    fn span_weight(
        &self,
        searcher: &dyn SearchPlanBuilder<C>,
        needs_scores: bool,
    ) -> Result<Self::Weight>;

    /// Returns the name of the field matched by this query.
    fn field(&self) -> &str;

    fn ctx(&self) -> Option<KeyedContext> {
        None
    }
}

pub enum SpanQueryEnum {
    Term(SpanTermQuery),
    Gap(SpanGapQuery),
    Or(SpanOrQuery),
    Near(SpanNearQuery),
    Boost(SpanBoostQuery),
}

impl<C: Codec> SpanQuery<C> for SpanQueryEnum {
    type Weight = SpanWeightEnum<C>;

    fn span_weight(
        &self,
        searcher: &dyn SearchPlanBuilder<C>,
        needs_scores: bool,
    ) -> Result<Self::Weight> {
        let weight = match self {
            SpanQueryEnum::Term(q) => SpanWeightEnum::Term(q.span_weight(searcher, needs_scores)?),
            SpanQueryEnum::Gap(q) => SpanWeightEnum::Gap(q.span_weight(searcher, needs_scores)?),
            SpanQueryEnum::Or(q) => SpanWeightEnum::Or(q.span_weight(searcher, needs_scores)?),
            SpanQueryEnum::Near(q) => SpanWeightEnum::Near(q.span_weight(searcher, needs_scores)?),
            SpanQueryEnum::Boost(q) => q.span_weight(searcher, needs_scores)?,
        };
        Ok(weight)
    }

    fn field(&self) -> &str {
        match self {
            SpanQueryEnum::Term(q) => SpanQuery::<C>::field(q),
            SpanQueryEnum::Gap(q) => SpanQuery::<C>::field(q),
            SpanQueryEnum::Or(q) => SpanQuery::<C>::field(q),
            SpanQueryEnum::Near(q) => SpanQuery::<C>::field(q),
            SpanQueryEnum::Boost(q) => SpanQuery::<C>::field(q),
        }
    }

    fn ctx(&self) -> Option<KeyedContext> {
        match self {
            SpanQueryEnum::Term(q) => SpanQuery::<C>::ctx(q),
            SpanQueryEnum::Gap(q) => SpanQuery::<C>::ctx(q),
            SpanQueryEnum::Or(q) => SpanQuery::<C>::ctx(q),
            SpanQueryEnum::Near(q) => SpanQuery::<C>::ctx(q),
            SpanQueryEnum::Boost(q) => SpanQuery::<C>::ctx(q),
        }
    }
}

impl<C: Codec> Query<C> for SpanQueryEnum {
    fn create_weight(
        &self,
        searcher: &dyn SearchPlanBuilder<C>,
        needs_scores: bool,
    ) -> Result<Box<dyn Weight<C>>> {
        match self {
            SpanQueryEnum::Term(q) => q.create_weight(searcher, needs_scores),
            SpanQueryEnum::Gap(q) => q.create_weight(searcher, needs_scores),
            SpanQueryEnum::Or(q) => q.create_weight(searcher, needs_scores),
            SpanQueryEnum::Near(q) => q.create_weight(searcher, needs_scores),
            SpanQueryEnum::Boost(q) => q.create_weight(searcher, needs_scores),
        }
    }

    fn extract_terms(&self) -> Vec<TermQuery> {
        match self {
            SpanQueryEnum::Term(q) => Query::<C>::extract_terms(q),
            SpanQueryEnum::Gap(q) => Query::<C>::extract_terms(q),
            SpanQueryEnum::Or(q) => Query::<C>::extract_terms(q),
            SpanQueryEnum::Near(q) => Query::<C>::extract_terms(q),
            SpanQueryEnum::Boost(q) => Query::<C>::extract_terms(q),
        }
    }

    fn as_any(&self) -> &dyn (::std::any::Any) {
        match self {
            SpanQueryEnum::Term(q) => Query::<C>::as_any(q),
            SpanQueryEnum::Gap(q) => Query::<C>::as_any(q),
            SpanQueryEnum::Or(q) => Query::<C>::as_any(q),
            SpanQueryEnum::Near(q) => Query::<C>::as_any(q),
            SpanQueryEnum::Boost(q) => Query::<C>::as_any(q),
        }
    }
}

impl fmt::Display for SpanQueryEnum {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SpanQueryEnum::Term(q) => write!(f, "SpanQueryEnum({})", q),
            SpanQueryEnum::Gap(q) => write!(f, "SpanQueryEnum({})", q),
            SpanQueryEnum::Or(q) => write!(f, "SpanQueryEnum({})", q),
            SpanQueryEnum::Near(q) => write!(f, "SpanQueryEnum({})", q),
            SpanQueryEnum::Boost(q) => write!(f, "SpanQueryEnum({})", q),
        }
    }
}

pub const NO_MORE_POSITIONS: i32 = i32::max_value();

/// Iterates through combinations of start/end positions per-doc.
/// Each start/end position represents a range of term positions within the current document.
/// These are enumerated in order, by increasing document number, within that by
/// increasing start position and finally by increasing end position.
pub trait Spans: DocIterator {
    /// Returns the next start position for the current doc.
    /// There is always at least one start/end position per doc.
    /// After the last start/end position at the current doc this returns `NO_MORE_POSITIONS`.
    fn next_start_position(&mut self) -> Result<i32>;

    /// Returns the start position in the current doc, or -1 when {@link #nextStartPosition} was
    /// not yet called on the current doc. After the last start/end position at the current doc
    /// this returns `NO_MORE_POSITIONS`.
    fn start_position(&self) -> i32;

    ///
    // Returns the end position for the current start position, or -1 when {@link
    // #nextStartPosition} was not yet called on the current doc. After the last start/end
    // position at the current doc this returns `NO_MORE_POSITIONS`.
    //
    fn end_position(&self) -> i32;

    /// Return the width of the match, which is typically used to compute
    /// the {@link SimScorer#compute_slop_factor slop factor}. It is only legal
    /// to call this method when the iterator is on a valid doc ID and positioned.
    /// The return value must be positive, and lower values means that the match is
    /// better.
    fn width(&self) -> i32;

    /// Collect postings data from the leaves of the current Spans.
    ///
    /// This method should only be called after {@link #next_start_position()}, and before
    /// `NO_MORE_POSITIONS` has been reached.
    ///
    /// @param collector a SpanCollector
    fn collect(&mut self, collector: &mut impl SpanCollector) -> Result<()>;

    ////
    /// Return an estimation of the cost of using the positions of
    /// this `Spans` for any single document
    /// The returned value is independent of the current document.
    fn positions_cost(&self) -> f32;

    /// Called before the current doc's frequency is calculated
    fn do_start_current_doc(&mut self) -> Result<()> {
        Ok(())
    }

    /// Called each time the scorer's SpanScorer is advanced during frequency calculation
    fn do_current_spans(&mut self) -> Result<()> {
        Ok(())
    }

    fn advance_position(&mut self, position: i32) -> Result<i32> {
        while self.start_position() < position {
            self.next_start_position()?;
        }
        Ok(self.start_position())
    }
}

pub enum SpansEnum<P: PostingIterator> {
    Gap(GapSpans),
    NearOrdered(NearSpansOrdered<P>),
    NearUnordered(Box<NearSpansUnordered<P>>),
    Or(SpanOrSpans<P>),
    Term(TermSpans<P>),
}

impl<P: PostingIterator> Spans for SpansEnum<P> {
    fn next_start_position(&mut self) -> Result<i32> {
        match self {
            SpansEnum::Gap(s) => s.next_start_position(),
            SpansEnum::NearOrdered(s) => s.next_start_position(),
            SpansEnum::NearUnordered(s) => s.next_start_position(),
            SpansEnum::Or(s) => s.next_start_position(),
            SpansEnum::Term(s) => s.next_start_position(),
        }
    }

    fn start_position(&self) -> i32 {
        match self {
            SpansEnum::Gap(s) => s.start_position(),
            SpansEnum::NearOrdered(s) => s.start_position(),
            SpansEnum::NearUnordered(s) => s.start_position(),
            SpansEnum::Or(s) => s.start_position(),
            SpansEnum::Term(s) => s.start_position(),
        }
    }

    fn end_position(&self) -> i32 {
        match self {
            SpansEnum::Gap(s) => s.end_position(),
            SpansEnum::NearOrdered(s) => s.end_position(),
            SpansEnum::NearUnordered(s) => s.end_position(),
            SpansEnum::Or(s) => s.end_position(),
            SpansEnum::Term(s) => s.end_position(),
        }
    }

    fn width(&self) -> i32 {
        match self {
            SpansEnum::Gap(s) => s.width(),
            SpansEnum::NearOrdered(s) => s.width(),
            SpansEnum::NearUnordered(s) => s.width(),
            SpansEnum::Or(s) => s.width(),
            SpansEnum::Term(s) => s.width(),
        }
    }

    fn collect(&mut self, collector: &mut impl SpanCollector) -> Result<()> {
        match self {
            SpansEnum::Gap(s) => s.collect(collector),
            SpansEnum::NearOrdered(s) => s.collect(collector),
            SpansEnum::NearUnordered(s) => s.collect(collector),
            SpansEnum::Or(s) => s.collect(collector),
            SpansEnum::Term(s) => s.collect(collector),
        }
    }

    fn positions_cost(&self) -> f32 {
        match self {
            SpansEnum::Gap(s) => s.positions_cost(),
            SpansEnum::NearOrdered(s) => s.positions_cost(),
            SpansEnum::NearUnordered(s) => s.positions_cost(),
            SpansEnum::Or(s) => s.positions_cost(),
            SpansEnum::Term(s) => s.positions_cost(),
        }
    }

    fn do_start_current_doc(&mut self) -> Result<()> {
        match self {
            SpansEnum::Gap(s) => s.do_start_current_doc(),
            SpansEnum::NearOrdered(s) => s.do_start_current_doc(),
            SpansEnum::NearUnordered(s) => s.do_start_current_doc(),
            SpansEnum::Or(s) => s.do_start_current_doc(),
            SpansEnum::Term(s) => s.do_start_current_doc(),
        }
    }

    fn do_current_spans(&mut self) -> Result<()> {
        match self {
            SpansEnum::Gap(s) => s.do_current_spans(),
            SpansEnum::NearOrdered(s) => s.do_current_spans(),
            SpansEnum::NearUnordered(s) => s.do_current_spans(),
            SpansEnum::Or(s) => s.do_current_spans(),
            SpansEnum::Term(s) => s.do_current_spans(),
        }
    }

    fn advance_position(&mut self, position: i32) -> Result<i32> {
        match self {
            SpansEnum::Gap(s) => s.advance_position(position),
            SpansEnum::NearOrdered(s) => s.advance_position(position),
            SpansEnum::NearUnordered(s) => s.advance_position(position),
            SpansEnum::Or(s) => s.advance_position(position),
            SpansEnum::Term(s) => s.advance_position(position),
        }
    }
}

impl<P: PostingIterator> DocIterator for SpansEnum<P> {
    fn doc_id(&self) -> DocId {
        match self {
            SpansEnum::Gap(s) => s.doc_id(),
            SpansEnum::NearOrdered(s) => s.doc_id(),
            SpansEnum::NearUnordered(s) => s.doc_id(),
            SpansEnum::Or(s) => s.doc_id(),
            SpansEnum::Term(s) => s.doc_id(),
        }
    }

    fn next(&mut self) -> Result<DocId> {
        match self {
            SpansEnum::Gap(s) => s.next(),
            SpansEnum::NearOrdered(s) => s.next(),
            SpansEnum::NearUnordered(s) => s.next(),
            SpansEnum::Or(s) => s.next(),
            SpansEnum::Term(s) => s.next(),
        }
    }

    fn advance(&mut self, target: DocId) -> Result<DocId> {
        match self {
            SpansEnum::Gap(s) => s.advance(target),
            SpansEnum::NearOrdered(s) => s.advance(target),
            SpansEnum::NearUnordered(s) => s.advance(target),
            SpansEnum::Or(s) => s.advance(target),
            SpansEnum::Term(s) => s.advance(target),
        }
    }

    fn slow_advance(&mut self, target: DocId) -> Result<DocId> {
        match self {
            SpansEnum::Gap(s) => s.slow_advance(target),
            SpansEnum::NearOrdered(s) => s.slow_advance(target),
            SpansEnum::NearUnordered(s) => s.slow_advance(target),
            SpansEnum::Or(s) => s.slow_advance(target),
            SpansEnum::Term(s) => s.slow_advance(target),
        }
    }

    fn cost(&self) -> usize {
        match self {
            SpansEnum::Gap(s) => s.cost(),
            SpansEnum::NearOrdered(s) => s.cost(),
            SpansEnum::NearUnordered(s) => s.cost(),
            SpansEnum::Or(s) => s.cost(),
            SpansEnum::Term(s) => s.cost(),
        }
    }

    fn matches(&mut self) -> Result<bool> {
        match self {
            SpansEnum::Gap(s) => s.matches(),
            SpansEnum::NearOrdered(s) => s.matches(),
            SpansEnum::NearUnordered(s) => s.matches(),
            SpansEnum::Or(s) => s.matches(),
            SpansEnum::Term(s) => s.matches(),
        }
    }

    fn match_cost(&self) -> f32 {
        match self {
            SpansEnum::Gap(s) => s.match_cost(),
            SpansEnum::NearOrdered(s) => s.match_cost(),
            SpansEnum::NearUnordered(s) => s.match_cost(),
            SpansEnum::Or(s) => s.match_cost(),
            SpansEnum::Term(s) => s.match_cost(),
        }
    }

    fn support_two_phase(&self) -> bool {
        match self {
            SpansEnum::Gap(s) => s.support_two_phase(),
            SpansEnum::NearOrdered(s) => s.support_two_phase(),
            SpansEnum::NearUnordered(s) => s.support_two_phase(),
            SpansEnum::Or(s) => s.support_two_phase(),
            SpansEnum::Term(s) => s.support_two_phase(),
        }
    }

    fn approximate_next(&mut self) -> Result<DocId> {
        match self {
            SpansEnum::Gap(s) => s.approximate_next(),
            SpansEnum::NearOrdered(s) => s.approximate_next(),
            SpansEnum::NearUnordered(s) => s.approximate_next(),
            SpansEnum::Or(s) => s.approximate_next(),
            SpansEnum::Term(s) => s.approximate_next(),
        }
    }

    fn approximate_advance(&mut self, target: DocId) -> Result<DocId> {
        match self {
            SpansEnum::Gap(s) => s.approximate_advance(target),
            SpansEnum::NearOrdered(s) => s.approximate_advance(target),
            SpansEnum::NearUnordered(s) => s.approximate_advance(target),
            SpansEnum::Or(s) => s.approximate_advance(target),
            SpansEnum::Term(s) => s.approximate_advance(target),
        }
    }
}

/// An interface defining the collection of postings information from the leaves
/// of a Spans
pub trait SpanCollector {
    /// Collect information from postings
    fn collect_leaf(
        &mut self,
        postings: &impl PostingIterator,
        position: i32,
        term: &Term,
    ) -> Result<()>;

    /// Call to indicate that the driving Spans has moved to a new position
    fn reset(&mut self);
}

/// Enumeration defining what postings information should be retrieved from the
/// index for a given Spans
pub enum PostingsFlag {
    Positions,
    Payloads,
    Offsets,
}

impl PostingsFlag {
    pub fn required_postings(&self) -> u16 {
        match *self {
            PostingsFlag::Positions => PostingIteratorFlags::POSITIONS,
            PostingsFlag::Payloads => PostingIteratorFlags::PAYLOADS,
            PostingsFlag::Offsets => PostingIteratorFlags::ALL,
        }
    }
}

/// a basic `Scorer` over `Spans`
pub struct SpanScorer<S: Spans> {
    spans: S,
    doc_scorer: Option<Box<dyn SimScorer>>,
    /// accumulated sloppy freq (computed in setFreqCurrentDoc)
    freq: f32,
    /// number of matches (computed in setFreqCurrentDoc)
    num_matches: i32,
    /// last doc we called set_freq_current_doc() for
    last_scored_doc: DocId,
}

impl<S: Spans> SpanScorer<S> {
    pub fn new(spans: S, doc_scorer: Option<Box<dyn SimScorer>>) -> Self {
        SpanScorer {
            spans,
            doc_scorer,
            freq: 0.0,
            num_matches: 0,
            last_scored_doc: -1,
        }
    }

    fn ensure_freq(&mut self) -> Result<()> {
        let current_doc = self.doc_id();
        if self.last_scored_doc != current_doc {
            self.set_freq_current_doc()?;
            self.last_scored_doc = current_doc;
        }
        Ok(())
    }

    fn set_freq_current_doc(&mut self) -> Result<()> {
        self.freq = 0.0;
        self.num_matches = 0;
        self.spans.do_start_current_doc()?;

        let mut prev_start_pos = -1;
        let mut prev_end_pos = -1;

        let mut start_pos = self.spans.next_start_position()?;
        assert_ne!(start_pos, NO_MORE_POSITIONS);
        while start_pos != NO_MORE_POSITIONS {
            debug_assert!(start_pos >= prev_start_pos);
            let end_pos = self.spans.end_position();
            debug_assert_ne!(end_pos, NO_MORE_POSITIONS);

            debug_assert!((start_pos != prev_start_pos) || (end_pos >= prev_end_pos));
            self.num_matches += 1;
            if let Some(ref mut doc_scorer) = self.doc_scorer {
                self.freq += doc_scorer.compute_slop_factor(self.spans.width());
                self.spans.do_current_spans()?;
                prev_start_pos = start_pos;
                prev_end_pos = end_pos;
                start_pos = self.spans.next_start_position()?;
            } else {
                self.freq = 1.0;
                return Ok(());
            }
        }

        Ok(())
    }

    fn score_current_doc(&mut self) -> Result<f32> {
        debug_assert!(self.doc_scorer.is_some());
        let doc = self.doc_id();
        let freq = self.freq;
        self.doc_scorer.as_mut().unwrap().score(doc, freq)
    }
}

impl<S: Spans> Scorer for SpanScorer<S> {
    fn score(&mut self) -> Result<f32> {
        self.ensure_freq()?;
        self.score_current_doc()
    }
}

impl<S: Spans> DocIterator for SpanScorer<S> {
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

    fn approximate_next(&mut self) -> Result<i32> {
        self.spans.approximate_next()
    }

    fn approximate_advance(&mut self, target: i32) -> Result<i32> {
        self.spans.approximate_advance(target)
    }
}

#[allow(clippy::implicit_hasher)]
pub fn build_sim_weight<C: Codec, IS: SearchPlanBuilder<C> + ?Sized>(
    field: &str,
    searcher: &IS,
    terms: Vec<Term>,
    ctx: Option<KeyedContext>,
) -> Result<Option<Box<dyn SimWeight<C>>>> {
    if field.is_empty() || terms.is_empty() {
        return Ok(None);
    }

    let similarity = searcher.similarity(field, !terms.is_empty());
    let mut term_stats = Vec::with_capacity(terms.len());
    for term in terms.iter() {
        term_stats.push(searcher.term_statistics(term)?);
    }

    let collection_stats = if let Some(stat) = searcher.collections_statistics(field) {
        stat.clone()
    } else {
        CollectionStatistics::new(field.to_string(), 0, searcher.max_doc() as i64, -1, -1, -1)
    };

    Ok(Some(similarity.compute_weight(
        &collection_stats,
        &term_stats,
        ctx.as_ref(),
        1.0f32,
    )))
}

/// Expert-only.  Public for use by other weight implementations
pub trait SpanWeight<C: Codec>: Weight<C> {
    fn sim_weight(&self) -> Option<&dyn SimWeight<C>>;

    fn sim_weight_mut(&mut self) -> Option<&mut dyn SimWeight<C>>;

    /// Expert: Return a Spans object iterating over matches from this Weight
    fn get_spans(
        &self,
        reader: &LeafReaderContext<'_, C>,
        required_postings: &PostingsFlag,
    ) -> Result<Option<SpansEnum<CodecPostingIterator<C>>>>;

    /// Collect all Terms used by this Weight
    fn extract_term_keys(&self, terms: &mut Vec<Term>);

    fn do_create_scorer(&self, ctx: &LeafReaderContext<'_, C>) -> Result<Option<Box<dyn Scorer>>> {
        if let Some(spans) = self.get_spans(ctx, &PostingsFlag::Positions)? {
            let doc_scorer = self.sim_scorer(ctx.reader)?;
            Ok(Some(Box::new(SpanScorer::new(spans, doc_scorer))))
        } else {
            Ok(None)
        }
    }

    fn do_value_for_normalization(&self) -> f32 {
        if let Some(sim_weight) = self.sim_weight() {
            sim_weight.get_value_for_normalization()
        } else {
            1.0
        }
    }

    fn do_normalize(&mut self, query_norm: f32, boost: f32) {
        if let Some(sim_weight) = self.sim_weight_mut() {
            sim_weight.normalize(query_norm, boost)
        }
    }

    fn sim_scorer(&self, reader: &SearchLeafReader<C>) -> Result<Option<Box<dyn SimScorer>>> {
        let sim_scorer = if let Some(ref sim_weight) = self.sim_weight() {
            Some(sim_weight.sim_scorer(reader)?)
        } else {
            None
        };
        Ok(sim_scorer)
    }

    fn explain_span(&self, reader: &LeafReaderContext<'_, C>, doc: DocId) -> Result<Explanation> {
        if let Some(spans) = self.get_spans(reader, &PostingsFlag::Positions)? {
            let mut scorer = SpanScorer::new(spans, self.sim_scorer(reader.reader)?);

            if scorer.advance(doc)? == doc {
                if let Some(ref w) = self.sim_weight() {
                    scorer.ensure_freq()?;
                    let freq = scorer.freq;
                    let freq_expl =
                        Explanation::new(true, freq, format!("phraseFreq={}", freq), vec![]);
                    let score_expl = w.explain(reader.reader, doc, freq_expl)?;
                    let score_expl_value = score_expl.value();

                    return Ok(Explanation::new(
                        true,
                        score_expl_value,
                        format!("weight({} in {}), result of:", self, doc),
                        vec![score_expl],
                    ));
                };
            }
        }

        Ok(Explanation::new(
            false,
            0.0f32,
            "no matching term".to_string(),
            vec![],
        ))
    }
}

pub enum SpanWeightEnum<C: Codec> {
    Term(SpanTermWeight<C>),
    Gap(SpanGapWeight<C>),
    Boost(SpanBoostWeight<C>),
    Near(SpanNearWeight<C>),
    Or(SpanOrWeight<C>),
}

impl<C: Codec> SpanWeight<C> for SpanWeightEnum<C> {
    fn sim_weight(&self) -> Option<&dyn SimWeight<C>> {
        match self {
            SpanWeightEnum::Term(w) => w.sim_weight(),
            SpanWeightEnum::Gap(w) => w.sim_weight(),
            SpanWeightEnum::Or(w) => w.sim_weight(),
            SpanWeightEnum::Near(w) => w.sim_weight(),
            SpanWeightEnum::Boost(w) => w.sim_weight(),
        }
    }

    fn sim_weight_mut(&mut self) -> Option<&mut dyn SimWeight<C>> {
        match self {
            SpanWeightEnum::Term(w) => w.sim_weight_mut(),
            SpanWeightEnum::Gap(w) => w.sim_weight_mut(),
            SpanWeightEnum::Or(w) => w.sim_weight_mut(),
            SpanWeightEnum::Near(w) => w.sim_weight_mut(),
            SpanWeightEnum::Boost(w) => w.sim_weight_mut(),
        }
    }

    fn get_spans(
        &self,
        reader: &LeafReaderContext<'_, C>,
        required_postings: &PostingsFlag,
    ) -> Result<Option<SpansEnum<CodecPostingIterator<C>>>> {
        match self {
            SpanWeightEnum::Term(w) => w.get_spans(reader, required_postings),
            SpanWeightEnum::Gap(w) => w.get_spans(reader, required_postings),
            SpanWeightEnum::Or(w) => w.get_spans(reader, required_postings),
            SpanWeightEnum::Near(w) => w.get_spans(reader, required_postings),
            SpanWeightEnum::Boost(w) => w.get_spans(reader, required_postings),
        }
    }

    fn extract_term_keys(&self, terms: &mut Vec<Term>) {
        match self {
            SpanWeightEnum::Term(w) => w.extract_term_keys(terms),
            SpanWeightEnum::Gap(w) => w.extract_term_keys(terms),
            SpanWeightEnum::Or(w) => w.extract_term_keys(terms),
            SpanWeightEnum::Near(w) => w.extract_term_keys(terms),
            SpanWeightEnum::Boost(w) => w.extract_term_keys(terms),
        }
    }

    fn do_create_scorer(&self, ctx: &LeafReaderContext<'_, C>) -> Result<Option<Box<dyn Scorer>>> {
        match self {
            SpanWeightEnum::Term(w) => w.do_create_scorer(ctx),
            SpanWeightEnum::Gap(w) => w.do_create_scorer(ctx),
            SpanWeightEnum::Or(w) => w.do_create_scorer(ctx),
            SpanWeightEnum::Near(w) => w.do_create_scorer(ctx),
            SpanWeightEnum::Boost(w) => w.do_create_scorer(ctx),
        }
    }

    fn do_value_for_normalization(&self) -> f32 {
        match self {
            SpanWeightEnum::Term(w) => w.do_value_for_normalization(),
            SpanWeightEnum::Gap(w) => w.do_value_for_normalization(),
            SpanWeightEnum::Or(w) => w.do_value_for_normalization(),
            SpanWeightEnum::Near(w) => w.do_value_for_normalization(),
            SpanWeightEnum::Boost(w) => w.do_value_for_normalization(),
        }
    }

    fn do_normalize(&mut self, query_norm: f32, boost: f32) {
        match self {
            SpanWeightEnum::Term(w) => w.do_normalize(query_norm, boost),
            SpanWeightEnum::Gap(w) => w.do_normalize(query_norm, boost),
            SpanWeightEnum::Or(w) => w.do_normalize(query_norm, boost),
            SpanWeightEnum::Near(w) => w.do_normalize(query_norm, boost),
            SpanWeightEnum::Boost(w) => w.do_normalize(query_norm, boost),
        }
    }

    fn sim_scorer(&self, reader: &SearchLeafReader<C>) -> Result<Option<Box<dyn SimScorer>>> {
        match self {
            SpanWeightEnum::Term(w) => w.sim_scorer(reader),
            SpanWeightEnum::Gap(w) => w.sim_scorer(reader),
            SpanWeightEnum::Or(w) => w.sim_scorer(reader),
            SpanWeightEnum::Near(w) => w.sim_scorer(reader),
            SpanWeightEnum::Boost(w) => w.sim_scorer(reader),
        }
    }

    fn explain_span(&self, reader: &LeafReaderContext<'_, C>, doc: i32) -> Result<Explanation> {
        match self {
            SpanWeightEnum::Term(w) => w.explain_span(reader, doc),
            SpanWeightEnum::Gap(w) => w.explain_span(reader, doc),
            SpanWeightEnum::Or(w) => w.explain_span(reader, doc),
            SpanWeightEnum::Near(w) => w.explain_span(reader, doc),
            SpanWeightEnum::Boost(w) => w.explain_span(reader, doc),
        }
    }
}

impl<C: Codec> Weight<C> for SpanWeightEnum<C> {
    fn create_scorer(
        &self,
        leaf_reader: &LeafReaderContext<'_, C>,
    ) -> Result<Option<Box<dyn Scorer>>> {
        match self {
            SpanWeightEnum::Term(w) => w.create_scorer(leaf_reader),
            SpanWeightEnum::Gap(w) => w.create_scorer(leaf_reader),
            SpanWeightEnum::Or(w) => w.create_scorer(leaf_reader),
            SpanWeightEnum::Near(w) => w.create_scorer(leaf_reader),
            SpanWeightEnum::Boost(w) => w.create_scorer(leaf_reader),
        }
    }

    fn hash_code(&self) -> u32 {
        match self {
            SpanWeightEnum::Term(w) => w.hash_code(),
            SpanWeightEnum::Gap(w) => w.hash_code(),
            SpanWeightEnum::Or(w) => w.hash_code(),
            SpanWeightEnum::Near(w) => w.hash_code(),
            SpanWeightEnum::Boost(w) => w.hash_code(),
        }
    }

    fn query_type(&self) -> &'static str {
        "SpanQueryEnum"
    }

    fn actual_query_type(&self) -> &'static str {
        match self {
            SpanWeightEnum::Term(w) => w.actual_query_type(),
            SpanWeightEnum::Gap(w) => w.actual_query_type(),
            SpanWeightEnum::Or(w) => w.actual_query_type(),
            SpanWeightEnum::Near(w) => w.actual_query_type(),
            SpanWeightEnum::Boost(w) => w.actual_query_type(),
        }
    }

    fn normalize(&mut self, norm: f32, boost: f32) {
        match self {
            SpanWeightEnum::Term(w) => w.normalize(norm, boost),
            SpanWeightEnum::Gap(w) => w.normalize(norm, boost),
            SpanWeightEnum::Or(w) => w.normalize(norm, boost),
            SpanWeightEnum::Near(w) => w.normalize(norm, boost),
            SpanWeightEnum::Boost(w) => w.normalize(norm, boost),
        }
    }

    fn value_for_normalization(&self) -> f32 {
        match self {
            SpanWeightEnum::Term(w) => w.value_for_normalization(),
            SpanWeightEnum::Gap(w) => w.value_for_normalization(),
            SpanWeightEnum::Or(w) => w.value_for_normalization(),
            SpanWeightEnum::Near(w) => w.value_for_normalization(),
            SpanWeightEnum::Boost(w) => w.value_for_normalization(),
        }
    }

    fn needs_scores(&self) -> bool {
        match self {
            SpanWeightEnum::Term(w) => w.needs_scores(),
            SpanWeightEnum::Gap(w) => w.needs_scores(),
            SpanWeightEnum::Or(w) => w.needs_scores(),
            SpanWeightEnum::Near(w) => w.needs_scores(),
            SpanWeightEnum::Boost(w) => w.needs_scores(),
        }
    }

    fn explain(&self, reader: &LeafReaderContext<'_, C>, doc: DocId) -> Result<Explanation> {
        match self {
            SpanWeightEnum::Term(w) => w.explain(reader, doc),
            SpanWeightEnum::Gap(w) => w.explain(reader, doc),
            SpanWeightEnum::Or(w) => w.explain(reader, doc),
            SpanWeightEnum::Near(w) => w.explain(reader, doc),
            SpanWeightEnum::Boost(w) => w.explain(reader, doc),
        }
    }
}

impl<C: Codec> fmt::Display for SpanWeightEnum<C> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SpanWeightEnum::Term(w) => write!(f, "SpanWeightEnum({})", w),
            SpanWeightEnum::Gap(w) => write!(f, "SpanWeightEnum({})", w),
            SpanWeightEnum::Or(w) => write!(f, "SpanWeightEnum({})", w),
            SpanWeightEnum::Near(w) => write!(f, "SpanWeightEnum({})", w),
            SpanWeightEnum::Boost(w) => write!(f, "SpanWeightEnum({})", w),
        }
    }
}

impl<C: Codec> From<SpanBoostWeightEnum<C>> for SpanWeightEnum<C> {
    fn from(w: SpanBoostWeightEnum<C>) -> Self {
        match w {
            SpanBoostWeightEnum::Term(w) => SpanWeightEnum::Term(w),
            SpanBoostWeightEnum::Gap(w) => SpanWeightEnum::Gap(w),
            SpanBoostWeightEnum::Or(w) => SpanWeightEnum::Or(w),
            SpanBoostWeightEnum::Near(w) => SpanWeightEnum::Near(w),
        }
    }
}

/// a raw pointer to spans as scorer
/// only used for build a ConjunctionScorer
pub struct SpansAsScorer<T: PostingIterator> {
    spans: *mut SpansEnum<T>,
}

impl<T: PostingIterator> SpansAsScorer<T> {
    #[inline]
    #[allow(clippy::mut_from_ref)]
    fn spans(&self) -> &mut SpansEnum<T> {
        unsafe { &mut *self.spans }
    }
}

unsafe impl<T: PostingIterator> Send for SpansAsScorer<T> {}

impl<T: PostingIterator> Scorer for SpansAsScorer<T> {
    fn score(&mut self) -> Result<f32> {
        unreachable!()
    }
}

impl<T: PostingIterator> DocIterator for SpansAsScorer<T> {
    fn doc_id(&self) -> i32 {
        self.spans().doc_id()
    }

    fn next(&mut self) -> Result<i32> {
        self.spans().next()
    }

    fn advance(&mut self, target: i32) -> Result<i32> {
        self.spans().advance(target)
    }

    fn slow_advance(&mut self, target: i32) -> Result<i32> {
        self.spans().slow_advance(target)
    }

    fn cost(&self) -> usize {
        self.spans().cost()
    }

    fn matches(&mut self) -> Result<bool> {
        self.spans().matches()
    }

    fn match_cost(&self) -> f32 {
        self.spans().match_cost()
    }

    fn support_two_phase(&self) -> bool {
        self.spans().support_two_phase()
    }

    fn approximate_next(&mut self) -> Result<i32> {
        self.spans().approximate_next()
    }

    fn approximate_advance(&mut self, target: i32) -> Result<i32> {
        self.spans().approximate_advance(target)
    }
}

pub struct ConjunctionSpanBase<T: PostingIterator> {
    pub conjunction: ConjunctionScorer<SpansAsScorer<T>>,
    // use to move to next doc with all clauses
    /// a first start position is available in current doc for next_start_position
    pub first_in_current_doc: bool,
    pub one_exhausted_in_current_doc: bool,
    // one sub_spans exhausted in current doc
    pub two_phase_match_cost: f32,
}

impl<P: PostingIterator> ConjunctionSpanBase<P> {
    pub fn new<'a, I, T>(sub_spans: T) -> Result<Self>
    where
        P: 'a,
        I: Iterator<Item = &'a mut SpansEnum<P>>,
        T: IntoIterator<Item = &'a mut SpansEnum<P>, IntoIter = I>,
    {
        let scorers: Vec<_> = sub_spans
            .into_iter()
            .map(|spans| SpansAsScorer { spans })
            .collect();
        if scorers.len() < 2 {
            bail!(ErrorKind::IllegalArgument(format!(
                "there must be at least 2 sub spans! but only {} given!",
                scorers.len(),
            )));
        }

        let two_phase_match_cost = Self::two_phase_match_cost(&scorers);
        let conjunction = ConjunctionScorer::new(scorers);
        Ok(ConjunctionSpanBase {
            conjunction,
            first_in_current_doc: true,
            // ensure for doc -1 that start/end positions are -1
            one_exhausted_in_current_doc: false,
            two_phase_match_cost,
        })
    }

    pub fn two_phase_match_cost(spans: &[SpansAsScorer<P>]) -> f32 {
        spans
            .iter()
            .map(|s| {
                if s.support_two_phase() {
                    s.match_cost()
                } else {
                    s.spans().positions_cost()
                }
            })
            .sum()
    }
}

/// Common super class for multiple sub spans required in a document.
pub trait ConjunctionSpans<P: PostingIterator>: Spans {
    fn conjunction_span_base(&self) -> &ConjunctionSpanBase<P>;

    fn conjunction_span_base_mut(&mut self) -> &mut ConjunctionSpanBase<P>;

    fn two_phase_current_doc_matches(&mut self) -> Result<bool>;

    fn next_to_match_doc(&mut self) -> Result<DocId> {
        self.conjunction_span_base_mut()
            .one_exhausted_in_current_doc = false;
        loop {
            if self.two_phase_current_doc_matches()? {
                return Ok(self.doc_id());
            }
            if self.conjunction_span_base_mut().conjunction.next()? == NO_MORE_DOCS {
                return Ok(NO_MORE_DOCS);
            }
        }
    }
}

// TODO 本来希望实现为 impl<T: ConjunctionSpans> DocIterator for T 这样，
// 但是由于和 disjunction.rs 里面的
// impl<T> DocIterator for T where T: DisjunctionScorer + Scorer 冲突
// 所以目前只能实现为 macro
macro_rules! conjunction_span_doc_iter {
    ($ty:ident < $( $N:ident $(: $b0:ident $(+$b:ident)* )* ),* >) => {
        impl< $( $N $(: $b0 $(+$b)* ),* ),* > DocIterator for $ty< $( $N ),* > {
            fn doc_id(&self) -> i32 {
                self.conjunction_span_base().conjunction.doc_id()
            }

            fn next(&mut self) -> Result<i32> {
                let next = self.conjunction_span_base_mut().conjunction.next()?;
                if next == NO_MORE_DOCS {
                    Ok(NO_MORE_DOCS)
                } else {
                    self.next_to_match_doc()
                }
            }

            fn advance(&mut self, target: DocId) -> Result<i32> {
                let next = self
                    .conjunction_span_base_mut()
                    .conjunction
                    .advance(target)?;
                if next == NO_MORE_DOCS {
                    Ok(NO_MORE_DOCS)
                } else {
                    self.next_to_match_doc()
                }
            }

            fn slow_advance(&mut self, target: DocId) -> Result<i32> {
                self.conjunction_span_base_mut()
                    .conjunction
                    .slow_advance(target)
            }

            fn cost(&self) -> usize {
                self.conjunction_span_base().conjunction.cost()
            }

            fn matches(&mut self) -> Result<bool> {
                self.two_phase_current_doc_matches()
            }

            fn match_cost(&self) -> f32 {
                self.conjunction_span_base().two_phase_match_cost
            }

            fn support_two_phase(&self) -> bool {
                true
            }

            fn approximate_next(&mut self) -> Result<i32> {
                self.conjunction_span_base_mut().conjunction.next()
            }

            fn approximate_advance(&mut self, target: i32) -> Result<i32> {
                self.conjunction_span_base_mut().conjunction.advance(target)
            }
        }
    };
}
