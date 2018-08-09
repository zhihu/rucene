use core::index::LeafReader;
use core::index::{Term, TermContext};
use core::search::conjunction::ConjunctionScorer;
use core::search::explanation::Explanation;
use core::search::posting_iterator::{self, PostingIterator};
use core::search::searcher::IndexSearcher;
use core::search::{DocIterator, MatchNoDocScorer, Query, Scorer, SimScorer, SimWeight, Weight,
                   NO_MORE_DOCS};
use core::util::DocId;
use error::{ErrorKind, Result};

use std::collections::HashMap;
use std::rc::Rc;

pub fn term_contexts(weights: &[Box<SpanWeight>]) -> HashMap<Term, Rc<TermContext>> {
    let mut terms = HashMap::new();
    for w in weights {
        w.extract_term_contexts(&mut terms);
    }
    terms
}

/// Base class for span-based queries.
pub trait SpanQuery: Query {
    /// Returns the name of the field matched by this query.
    fn field(&self) -> &str;

    fn span_weight(&self, searcher: &IndexSearcher, needs_scores: bool) -> Result<Box<SpanWeight>>;
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
    ///
    fn next_start_position(&mut self) -> Result<i32>;

    /// Returns the start position in the current doc, or -1 when {@link #nextStartPosition} was
    /// not yet called on the current doc. After the last start/end position at the current doc
    /// this returns `NO_MORE_POSITIONS`.
    ///
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
    ///
    fn width(&self) -> i32;

    /// Collect postings data from the leaves of the current Spans.
    ///
    /// This method should only be called after {@link #next_start_position()}, and before
    /// `NO_MORE_POSITIONS` has been reached.
    ///
    /// @param collector a SpanCollector
    ///
    fn collect(&mut self, collector: &mut SpanCollector) -> Result<()>;

    ////
    /// Return an estimation of the cost of using the positions of
    /// this `Spans` for any single document
    /// The returned value is independent of the current document.
    ///
    fn positions_cost(&self) -> f32;

    /// Called before the current doc's frequency is calculated
    fn do_start_current_doc(&mut self) -> Result<()> {
        Ok(())
    }

    /// Called each time the scorer's SpanScorer is advanced during frequency calculation
    fn do_current_spans(&mut self) -> Result<()> {
        Ok(())
    }

    /// support two phase
    fn support_two_phase(&self) -> bool;

    fn advance_position(&mut self, position: i32) -> Result<i32> {
        while self.start_position() < position {
            self.next_start_position()?;
        }
        Ok(self.start_position())
    }
}

/// An interface defining the collection of postings information from the leaves
/// of a Spans
pub trait SpanCollector {
    /// Collect information from postings
    fn collect_leaf(
        &mut self,
        postings: &PostingIterator,
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
    pub fn required_postings(&self) -> i16 {
        match *self {
            PostingsFlag::Positions => posting_iterator::POSTING_ITERATOR_FLAG_POSITIONS,
            PostingsFlag::Payloads => posting_iterator::POSTING_ITERATOR_FLAG_PAYLOADS,
            PostingsFlag::Offsets => {
                posting_iterator::POSTING_ITERATOR_FLAG_PAYLOADS
                    | posting_iterator::POSTING_ITERATOR_FLAG_OFFSETS
            }
        }
    }
}

pub struct SpanScorer {
    spans: Box<Spans>,
    doc_scorer: Option<Box<SimScorer>>,
    /// accumulated sloppy freq (computed in setFreqCurrentDoc)
    freq: f32,
    /// number of matches (computed in setFreqCurrentDoc)
    num_matches: i32,
    /// last doc we called set_freq_current_doc() for
    last_scored_doc: DocId,
}

impl SpanScorer {
    pub fn new(spans: Box<Spans>, doc_scorer: Option<Box<SimScorer>>) -> Self {
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

    fn score_current_doc(&mut self) -> Result<(f32)> {
        debug_assert!(self.doc_scorer.is_some());
        let doc = self.doc_id();
        let freq = self.freq;
        self.doc_scorer.as_mut().unwrap().score(doc, freq)
    }
}

impl Scorer for SpanScorer {
    fn score(&mut self) -> Result<f32> {
        self.ensure_freq()?;
        self.score_current_doc()
    }

    fn support_two_phase(&self) -> bool {
        self.spans.support_two_phase()
    }
}

impl DocIterator for SpanScorer {
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

    fn approximate_next(&mut self) -> Result<i32> {
        self.spans.approximate_next()
    }

    fn approximate_advance(&mut self, target: i32) -> Result<i32> {
        self.spans.approximate_advance(target)
    }
}

#[allow(implicit_hasher)]
pub fn build_sim_weight(
    field: &str,
    searcher: &IndexSearcher,
    term_contexts: HashMap<Term, Rc<TermContext>>,
) -> Result<Option<Box<SimWeight>>> {
    if field.is_empty() || term_contexts.is_empty() {
        return Ok(None);
    }

    let similarity = searcher.similarity(field, !term_contexts.is_empty());
    let mut term_stats = Vec::with_capacity(term_contexts.len());
    for (term, ctx) in term_contexts {
        term_stats.push(searcher.term_statistics(term, ctx.as_ref()));
    }
    let collection_stats = searcher.collections_statistics(field)?;
    Ok(Some(similarity.compute_weight(
        &collection_stats,
        &term_stats,
        None,
    )))
}

pub trait SpanWeight: Weight {
    fn sim_weight(&self) -> Option<&SimWeight>;

    fn sim_weight_mut(&mut self) -> Option<&mut SimWeight>;

    /// Expert: Return a Spans object iterating over matches from this Weight
    fn get_spans(
        &self,
        reader: &LeafReader,
        required_postings: &PostingsFlag,
    ) -> Result<Option<Box<Spans>>>;

    /// Collect all TermContexts used by this Weight
    fn extract_term_contexts(&self, contexts: &mut HashMap<Term, Rc<TermContext>>);

    fn do_create_scorer(&self, reader: &LeafReader) -> Result<Box<Scorer>> {
        if let Some(spans) = self.get_spans(reader, &PostingsFlag::Positions)? {
            let doc_scorer = self.sim_scorer(reader)?;
            Ok(Box::new(SpanScorer::new(spans, doc_scorer)))
        } else {
            // TODO maybe return None is better, then `Scorer` trait should be changed
            Ok(Box::new(MatchNoDocScorer::default()))
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

    fn sim_scorer(&self, reader: &LeafReader) -> Result<Option<Box<SimScorer>>> {
        let sim_scorer = if let Some(ref sim_weight) = self.sim_weight() {
            Some(sim_weight.sim_scorer(reader)?)
        } else {
            None
        };
        Ok(sim_scorer)
    }

    fn explain_span(&self, reader: &LeafReader, doc: DocId) -> Result<Explanation> {
        if let Some(spans) = self.get_spans(reader, &PostingsFlag::Positions)? {
            let mut scorer = SpanScorer::new(spans, self.sim_scorer(reader)?);

            if scorer.advance(doc)? == doc {
                match self.sim_weight() {
                    Some(ref w) => {
                        scorer.ensure_freq()?;
                        let freq = scorer.freq;
                        let freq_expl =
                            Explanation::new(true, freq, format!("phraseFreq={}", freq), vec![]);
                        let score_expl = w.explain(reader, doc, freq_expl)?;
                        let score_expl_value = score_expl.value();

                        return Ok(Explanation::new(
                            true,
                            score_expl_value,
                            format!("weight({} in {}), result of:", self, doc),
                            vec![score_expl],
                        ));
                    }
                    None => {}
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

/// a raw pointer to spans as scorer
/// only used for build a ConjunctionScorer
struct SpansAsScorer {
    spans: *mut Spans,
}

unsafe impl Send for SpansAsScorer {}

unsafe impl Sync for SpansAsScorer {}

impl Scorer for SpansAsScorer {
    fn score(&mut self) -> Result<f32> {
        unreachable!()
    }

    fn support_two_phase(&self) -> bool {
        unsafe { (*self.spans).support_two_phase() }
    }
}

impl DocIterator for SpansAsScorer {
    fn doc_id(&self) -> i32 {
        unsafe { (*self.spans).doc_id() }
    }

    fn next(&mut self) -> Result<i32> {
        unsafe { (*self.spans).next() }
    }

    fn advance(&mut self, target: i32) -> Result<i32> {
        unsafe { (*self.spans).advance(target) }
    }

    fn slow_advance(&mut self, target: i32) -> Result<i32> {
        unsafe { (*self.spans).slow_advance(target) }
    }

    fn cost(&self) -> usize {
        unsafe { (*self.spans).cost() }
    }

    fn matches(&mut self) -> Result<bool> {
        unsafe { (*self.spans).matches() }
    }

    fn match_cost(&self) -> f32 {
        unsafe { (*self.spans).match_cost() }
    }

    fn approximate_next(&mut self) -> Result<i32> {
        unsafe { (*self.spans).approximate_next() }
    }

    fn approximate_advance(&mut self, target: i32) -> Result<i32> {
        unsafe { (*self.spans).approximate_advance(target) }
    }
}

pub struct ConjunctionSpanBase {
    pub conjunction: ConjunctionScorer,
    // use to move to next doc with all clauses
    /// a first start position is available in current doc for next_start_position
    pub first_in_current_doc: bool,
    pub one_exhausted_in_current_doc: bool,
    // one sub_spans exhausted in current doc
    pub two_phase_match_cost: f32,
}

impl ConjunctionSpanBase {
    pub fn new(sub_spans: &mut [Box<Spans>]) -> Result<Self> {
        if sub_spans.len() < 2 {
            bail!(ErrorKind::IllegalArgument(format!(
                "there must be at least 2 sub spans! but only {} given!",
                sub_spans.len()
            )));
        }
        let conjunction = {
            let mut scorers: Vec<Box<Scorer>> = Vec::with_capacity(sub_spans.len());
            for spans in sub_spans.iter_mut() {
                scorers.push(Box::new(SpansAsScorer {
                    spans: spans.as_mut() as *mut Spans,
                }));
            }
            ConjunctionScorer::new(scorers)
        };
        let two_phase_match_cost = Self::two_phase_match_cost(sub_spans);
        Ok(ConjunctionSpanBase {
            conjunction,
            first_in_current_doc: true,
            // ensure for doc -1 that start/end positions are -1
            one_exhausted_in_current_doc: false,
            two_phase_match_cost,
        })
    }

    pub fn two_phase_match_cost(spans: &[Box<Spans>]) -> f32 {
        spans
            .iter()
            .map(|s| {
                if s.support_two_phase() {
                    s.match_cost()
                } else {
                    s.positions_cost()
                }
            })
            .sum()
    }
}

/// Common super class for multiple sub spans required in a document.
pub trait ConjunctionSpans: Spans {
    fn conjunction_span_base(&self) -> &ConjunctionSpanBase;

    fn conjunction_span_base_mut(&mut self) -> &mut ConjunctionSpanBase;

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
    ($t:ty) => {
        impl DocIterator for $t {
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
                let next = self.conjunction_span_base_mut()
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

            fn approximate_next(&mut self) -> Result<i32> {
                self.conjunction_span_base_mut().conjunction.next()
            }

            fn approximate_advance(&mut self, target: i32) -> Result<i32> {
                self.conjunction_span_base_mut().conjunction.advance(target)
            }
        }
    };
}
