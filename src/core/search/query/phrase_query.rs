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

use error::{ErrorKind, Result};
use std::boxed::Box;
use std::cmp::{min, Ord, Ordering};
use std::collections::BinaryHeap;
use std::collections::{HashMap, HashSet};
use std::f32;
use std::fmt;

use core::codec::{Codec, CodecTermState};
use core::codec::{PostingIterator, PostingIteratorFlags};
use core::codec::{TermIterator, Terms};
use core::doc::Term;
use core::index::reader::LeafReaderContext;
use core::search::explanation::Explanation;
use core::search::query::{Query, TermQuery, Weight};
use core::search::scorer::{two_phase_next, ConjunctionScorer, Scorer};
use core::search::searcher::SearchPlanBuilder;
use core::search::similarity::{SimScorer, SimWeight, Similarity};
use core::search::statistics::{CollectionStatistics, TermStatistics};
use core::search::{DocIterator, NO_MORE_DOCS};
use core::util::{BitSet, FixedBitSet, ImmutableBitSet};
use core::util::{Bits, DocId, KeyedContext};

pub const PHRASE: &str = "phrase";

/// A Query that matches documents containing a particular sequence of terms.
///
/// A PhraseQuery is built by QueryParser for input like `"new york"`.
///
/// This query may be combined with other terms or queries with a {@link BooleanQuery}.
///
/// *NOTE*:
/// All terms in the phrase must match, even those at the same position. If you
/// have terms at the same position, perhaps synonyms, you probably want `MultiPhraseQuery`
/// instead which only requires one term at a position to match.
///
/// Also, Leading holes don't have any particular meaning for this query
/// and will be ignored.
#[derive(Clone, Debug)]
pub struct PhraseQuery {
    field: String,
    terms: Vec<Term>,
    positions: Vec<i32>,
    slop: i32,
    ctx: Option<KeyedContext>,
    ctxs: Option<Vec<KeyedContext>>,
}

impl PhraseQuery {
    pub fn new<T: Into<Option<Vec<KeyedContext>>>, S: Into<Option<KeyedContext>>>(
        terms: Vec<Term>,
        positions: Vec<i32>,
        slop: i32,
        ctx: S,
        ctxs: T,
    ) -> Result<PhraseQuery> {
        let ctx = ctx.into();
        let ctxs = ctxs.into();
        debug_assert_eq!(
            terms.len(),
            positions.len(),
            "Must have as many terms as positions"
        );
        debug_assert_eq!(
            terms.len(),
            ctxs.as_ref().map(Vec::len).unwrap_or_else(|| terms.len()),
            "Must have as many terms as positions"
        );
        assert!(slop >= 0, format!("Slop must be >= 0, got {}", slop));
        if terms.len() < 2 {
            bail!(ErrorKind::IllegalArgument(
                "phrase query terms should not be less than 2!".into()
            ));
        }
        for i in 1..terms.len() {
            debug_assert_eq!(
                terms[i - 1].field,
                terms[i].field,
                "All terms should have the same field"
            );
        }
        for pos in &positions {
            debug_assert!(*pos >= 0, format!("Positions must be >= 0, got {}", pos));
        }
        for i in 1..positions.len() {
            debug_assert!(
                positions[i - 1] <= positions[i],
                format!(
                    "Positions should not go backwards, got {} before {}",
                    positions[i - 1],
                    positions[i]
                )
            );
        }
        // normalize positions
        let mut positions = positions;
        let first = positions[0];
        for pos in &mut positions {
            *pos -= first;
        }

        let field = terms[0].field.clone();

        Ok(PhraseQuery {
            field,
            terms,
            positions,
            slop,
            ctx,
            ctxs,
        })
    }

    pub fn build<T: Into<Option<Vec<KeyedContext>>>, S: Into<Option<KeyedContext>>>(
        terms: Vec<Term>,
        slop: i32,
        ctx: S,
        ctxs: T,
    ) -> Result<PhraseQuery> {
        let positions = Self::increment_positions(terms.len());
        Self::new(terms, positions, slop, ctx, ctxs)
    }

    fn increment_positions(length: usize) -> Vec<i32> {
        (0..length as i32).collect()
    }
}

impl<C: Codec> Query<C> for PhraseQuery {
    fn create_weight(
        &self,
        searcher: &dyn SearchPlanBuilder<C>,
        needs_scores: bool,
    ) -> Result<Box<dyn Weight<C>>> {
        debug_assert!(
            self.positions.len() >= 2,
            "PhraseWeight does not support less than 2 terms, call rewrite first"
        );
        debug_assert_eq!(
            self.positions[0], 0,
            "PhraseWeight requires that the first position is 0, call rewrite first"
        );

        let max_doc = i64::from(searcher.max_doc());
        let mut term_states = Vec::with_capacity(self.terms.len());
        let mut term_stats: Vec<TermStatistics> = Vec::with_capacity(self.terms.len());

        for i in 0..self.terms.len() {
            let term_context = searcher.term_state(&self.terms[i])?;

            term_stats.push(searcher.term_statistics(&self.terms[i], term_context.as_ref()));
            term_states.push(term_context.term_states());
        }

        let collection_stats = if needs_scores {
            searcher.collections_statistics(&self.field)?
        } else {
            CollectionStatistics::new(self.field.clone(), max_doc, -1, -1, -1)
        };

        let similarity = searcher.similarity(&self.field, needs_scores);

        let sim_weight =
            similarity.compute_weight(&collection_stats, &term_stats, self.ctx.as_ref(), 1.0f32);

        Ok(Box::new(PhraseWeight::new(
            self.field.clone(),
            self.terms.clone(),
            self.positions.clone(),
            self.slop,
            similarity,
            sim_weight,
            needs_scores,
            term_states,
        )))
    }

    fn extract_terms(&self) -> Vec<TermQuery> {
        let mut term_query_list: Vec<TermQuery> = vec![];
        let ctxs = self
            .ctxs
            .as_ref()
            .map(|ctxs| Clone::clone(ctxs).into_iter().map(Some).collect())
            .unwrap_or_else(|| vec![None; self.terms.len()]);

        for (term, ctx) in self.terms.iter().zip(ctxs.into_iter()) {
            term_query_list.push(TermQuery::new(term.clone(), 1.0f32, ctx));
        }

        term_query_list
    }

    fn as_any(&self) -> &dyn (::std::any::Any) {
        self
    }
}

impl fmt::Display for PhraseQuery {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "PhraseQuery(field: {}, terms: {:?}, positions: {:?}, slop: {})",
            &self.field, &self.terms, &self.positions, self.slop,
        )
    }
}

pub static TERM_POSNS_SEEK_OPS_PER_DOC: i32 = 128;
pub static TERM_OPS_PER_POS: i32 = 7;

struct PhraseWeight<C: Codec> {
    field: String,
    terms: Vec<Term>,
    positions: Vec<i32>,
    slop: i32,
    similarity: Box<dyn Similarity<C>>,
    sim_weight: Box<dyn SimWeight<C>>,
    needs_scores: bool,
    term_states: Vec<HashMap<DocId, CodecTermState<C>>>,
}

impl<C: Codec> PhraseWeight<C> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        field: String,
        terms: Vec<Term>,
        positions: Vec<i32>,
        slop: i32,
        similarity: Box<dyn Similarity<C>>,
        sim_weight: Box<dyn SimWeight<C>>,
        needs_scores: bool,
        term_states: Vec<HashMap<DocId, CodecTermState<C>>>,
    ) -> PhraseWeight<C> {
        PhraseWeight {
            field,
            terms,
            positions,
            slop,
            similarity,
            sim_weight,
            needs_scores,
            term_states,
        }
    }

    fn term_positions_cost(&self, term_iter: &mut impl TermIterator) -> Result<f32> {
        let doc_freq = term_iter.doc_freq()?;
        debug_assert!(doc_freq > 0);
        let total_term_freq = term_iter.total_term_freq()?; // -1 when not available
        let exp_occurrences_in_matching_doc = if total_term_freq < i64::from(doc_freq) {
            1.0f32
        } else {
            total_term_freq as f32 / doc_freq as f32
        };

        Ok(TERM_POSNS_SEEK_OPS_PER_DOC as f32
            + exp_occurrences_in_matching_doc * TERM_OPS_PER_POS as f32)
    }
}

impl<C: Codec> Weight<C> for PhraseWeight<C> {
    fn create_scorer(
        &self,
        reader_context: &LeafReaderContext<'_, C>,
    ) -> Result<Option<Box<dyn Scorer>>> {
        debug_assert!(!self.terms.len() >= 2);

        let mut postings_freqs = Vec::with_capacity(self.terms.len());
        let mut term_iter = if let Some(field_terms) = reader_context.reader.terms(&self.field)? {
            debug_assert!(
                field_terms.has_positions()?,
                format!(
                    "field {} was indexed without position data; cannot run PhraseQuery \
                     (phrase={:?})",
                    self.field, self.terms
                )
            );
            field_terms.iterator()?
        } else {
            return Ok(None);
        };

        let mut total_match_cost = 0f32;
        for i in 0..self.terms.len() {
            let postings = if let Some(state) = self.term_states[i].get(&reader_context.doc_base) {
                term_iter.seek_exact_state(self.terms[i].bytes.as_ref(), state)?;
                total_match_cost += self.term_positions_cost(&mut term_iter)?;

                term_iter.postings_with_flags(PostingIteratorFlags::POSITIONS)?
            } else {
                return Ok(None);
            };

            postings_freqs.push(PostingsAndFreq::new(
                postings,
                self.positions[i],
                &self.terms[i],
            ));
        }

        let sim_scorer = self.sim_weight.sim_scorer(reader_context.reader)?;
        let scorer: Box<dyn Scorer> = if self.slop == 0 {
            // sort by increasing docFreq order
            // optimize exact case

            postings_freqs.sort();
            Box::new(ExactPhraseScorer::new(
                postings_freqs,
                sim_scorer,
                self.needs_scores,
                total_match_cost,
            ))
        } else {
            Box::new(SloppyPhraseScorer::new(
                postings_freqs,
                self.slop,
                sim_scorer,
                self.needs_scores,
                total_match_cost,
            ))
        };
        Ok(Some(scorer))
    }

    fn query_type(&self) -> &'static str {
        PHRASE
    }

    fn normalize(&mut self, norm: f32, boost: f32) {
        self.sim_weight.normalize(norm, boost)
    }

    fn value_for_normalization(&self) -> f32 {
        self.sim_weight.get_value_for_normalization()
    }

    fn needs_scores(&self) -> bool {
        self.needs_scores
    }

    fn explain(&self, reader: &LeafReaderContext<'_, C>, doc: DocId) -> Result<Explanation> {
        debug_assert!(!self.terms.len() >= 2);

        let mut matched = true;
        let mut postings_freqs = Vec::with_capacity(self.terms.len());
        let mut term_iter = if let Some(field_terms) = reader.reader.terms(&self.field)? {
            debug_assert!(
                field_terms.has_positions()?,
                format!(
                    "field {} was indexed without position data; cannot run PhraseQuery \
                     (phrase={:?})",
                    self.field, self.terms
                )
            );
            Some(field_terms.iterator()?)
        } else {
            matched = false;
            None
        };

        let mut total_match_cost = 0f32;
        for i in 0..self.terms.len() {
            if let Some(state) = self.term_states[i].get(&reader.doc_base()) {
                if let Some(ref mut term_iter) = term_iter {
                    term_iter.seek_exact_state(self.terms[i].bytes.as_ref(), state)?;
                    total_match_cost += self.term_positions_cost(term_iter)?;

                    let postings =
                        term_iter.postings_with_flags(PostingIteratorFlags::POSITIONS)?;
                    postings_freqs.push(PostingsAndFreq::new(
                        postings,
                        self.positions[i],
                        &self.terms[i],
                    ));
                }
            } else {
                matched = false;
            }
        }

        if matched {
            let sim_scorer = self.sim_weight.sim_scorer(reader.reader)?;
            if self.slop == 0 {
                postings_freqs.sort();
                let mut scorer = ExactPhraseScorer::new(
                    postings_freqs,
                    sim_scorer,
                    self.needs_scores,
                    total_match_cost,
                );

                if scorer.advance(doc)? == doc {
                    let freq = scorer.freq as f32;
                    let freq_expl =
                        Explanation::new(true, freq, format!("phraseFreq={}", freq), vec![]);
                    let score_expl = self.sim_weight.explain(reader.reader, doc, freq_expl)?;

                    return Ok(Explanation::new(
                        true,
                        score_expl.value(),
                        format!("weight({} in {}), result of:", self, doc),
                        vec![score_expl],
                    ));
                }
            } else {
                let mut scorer = SloppyPhraseScorer::new(
                    postings_freqs,
                    self.slop,
                    sim_scorer,
                    self.needs_scores,
                    total_match_cost,
                );

                if scorer.advance(doc)? == doc {
                    let freq = scorer.sloppy_freq;
                    let freq_expl =
                        Explanation::new(true, freq, format!("phraseFreq={}", freq), vec![]);
                    let score_expl = self.sim_weight.explain(reader.reader, doc, freq_expl)?;

                    return Ok(Explanation::new(
                        true,
                        score_expl.value(),
                        format!("weight({} in {}), result of:", self, doc),
                        vec![score_expl],
                    ));
                }
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

impl<C: Codec> fmt::Display for PhraseWeight<C> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "PhraseWeight(field: {}, terms: {:?}, positions: {:?}, similarity: {}, need_score: {})",
            &self.field, &self.terms, &self.positions, &self.similarity, self.needs_scores
        )
    }
}

struct PostingsAndFreq<T: PostingIterator> {
    pub postings: T,
    pub pos: i32,
    pub terms: Vec<Term>,
    pub nterms: i32,
    // for faster comparisons
}

impl<T: PostingIterator> PostingsAndFreq<T> {
    fn new(postings: T, pos: i32, term: &Term) -> Self {
        PostingsAndFreq {
            postings,
            pos,
            terms: vec![term.clone()],
            nterms: 1,
        }
    }
}

impl<T: PostingIterator> Ord for PostingsAndFreq<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(&other).unwrap()
    }
}

impl<T: PostingIterator> PartialOrd for PostingsAndFreq<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.pos != other.pos {
            return Some(self.pos.cmp(&other.pos));
        }

        if self.nterms != other.nterms {
            return Some(self.nterms.cmp(&other.nterms));
        }

        if self.nterms == 0 {
            return Some(Ordering::Equal);
        }

        for i in 0..self.terms.len() {
            let res = if self.terms[i].field.eq(&other.terms[i].field) {
                self.terms[i].bytes.cmp(&other.terms[i].bytes)
            } else {
                self.terms[i].field.cmp(&other.terms[i].field)
            };

            if res != Ordering::Equal {
                return Some(res);
            }
        }

        Some(Ordering::Equal)
    }
}

impl<T: PostingIterator> Eq for PostingsAndFreq<T> {}

impl<T: PostingIterator> PartialEq for PostingsAndFreq<T> {
    fn eq(&self, other: &Self) -> bool {
        self == other
    }
}

pub struct PostingsAndPosition {
    postings: *mut dyn PostingIterator,
    pos: i32,
    offset: i32,
    freq: i32,
    up_to: i32,
}

unsafe impl Send for PostingsAndPosition {}

impl PostingsAndPosition {
    pub fn new(postings: *mut dyn PostingIterator, offset: i32) -> PostingsAndPosition {
        PostingsAndPosition {
            postings,
            pos: -1,
            offset,
            freq: 0,
            up_to: 1,
        }
    }
}

pub struct ExactPhraseScorer<T: PostingIterator> {
    freq: i32,
    needs_scores: bool,
    match_cost: f32,
    postings: Vec<PostingsAndPosition>,
    doc_scorer: Box<dyn SimScorer>,
    conjunction: ConjunctionScorer<PostingsIterAsScorer<T>>,
}

impl<T: PostingIterator + 'static> ExactPhraseScorer<T> {
    fn new(
        postings: Vec<PostingsAndFreq<T>>,
        doc_scorer: Box<dyn SimScorer>,
        needs_scores: bool,
        match_cost: f32,
    ) -> Self {
        let mut iterators = Vec::with_capacity(postings.len());
        let mut postings_and_positions: Vec<PostingsAndPosition> =
            Vec::with_capacity(postings.len());

        for (i, posing) in postings.into_iter().enumerate() {
            let iterator = posing.postings;
            iterators.push(PostingsIterAsScorer { iterator });
            postings_and_positions.push(PostingsAndPosition::new(
                &mut iterators[i].iterator,
                posing.pos,
            ));
        }

        let conjunction = ConjunctionScorer::new(iterators);

        ExactPhraseScorer {
            freq: 0,
            needs_scores,
            match_cost,
            postings: postings_and_positions,
            doc_scorer,
            conjunction,
        }
    }

    fn advance_position(posting: &mut PostingsAndPosition, target: i32) -> Result<bool> {
        while posting.pos < target {
            if posting.up_to == posting.freq {
                return Ok(false);
            } else {
                unsafe {
                    posting.pos = (*posting.postings).next_position()?;
                }
                posting.up_to += 1;
            }
        }

        Ok(true)
    }

    fn phrase_freq(&mut self) -> Result<i32> {
        // reset state
        for posting in &mut self.postings {
            unsafe {
                posting.freq = (*posting.postings).freq()?;
                posting.pos = (*posting.postings).next_position()?;
            }

            posting.up_to = 1;
        }

        let mut freq = 0;
        let mut lead = self.postings.remove(0);

        'advanceHead: loop {
            let phrase_pos = lead.pos - lead.offset;
            for posting in &mut self.postings {
                let expected_pos = phrase_pos + posting.offset;

                // advance up to the same position as the lead
                if !Self::advance_position(posting, expected_pos)? {
                    break 'advanceHead;
                }

                if posting.pos != expected_pos {
                    // we advanced too far
                    let target = posting.pos - posting.offset + lead.offset;

                    if Self::advance_position(&mut lead, target)? {
                        continue 'advanceHead;
                    } else {
                        break 'advanceHead;
                    }
                }
            }

            freq += 1;
            if !self.needs_scores {
                break;
            }

            if lead.up_to == lead.freq {
                break;
            }

            unsafe {
                lead.pos = (*lead.postings).next_position()?;
            }
            lead.up_to += 1;
        }

        self.postings.insert(0, lead);

        self.freq = freq;
        Ok(self.freq)
    }

    pub fn do_next(&mut self, doc_id: DocId) -> Result<DocId> {
        let mut doc = doc_id;
        loop {
            if doc == NO_MORE_DOCS {
                return Ok(NO_MORE_DOCS);
            } else if self.matches()? {
                return Ok(doc);
            }

            doc = self.conjunction.next()?;
        }
    }
}

impl<T: PostingIterator + 'static> Scorer for ExactPhraseScorer<T> {
    fn score(&mut self) -> Result<f32> {
        let doc_id = self.conjunction.doc_id();
        let freq = self.freq as f32;
        self.doc_scorer.score(doc_id, freq)
    }
}

impl<T: PostingIterator + 'static> DocIterator for ExactPhraseScorer<T> {
    fn doc_id(&self) -> DocId {
        self.conjunction.doc_id()
    }

    fn next(&mut self) -> Result<DocId> {
        let doc_id = self.conjunction.next()?;
        self.do_next(doc_id)
    }

    fn advance(&mut self, target: DocId) -> Result<DocId> {
        let doc_id = self.conjunction.advance(target)?;
        self.do_next(doc_id)
    }

    fn cost(&self) -> usize {
        self.conjunction.cost()
    }

    fn matches(&mut self) -> Result<bool> {
        Ok(self.phrase_freq()? > 0)
    }

    fn match_cost(&self) -> f32 {
        self.match_cost
    }

    /// advance to the next approximate match doc
    fn approximate_next(&mut self) -> Result<DocId> {
        self.conjunction.next()
    }

    /// Advances to the first approximate doc beyond the current doc
    fn approximate_advance(&mut self, target: DocId) -> Result<DocId> {
        self.conjunction.advance(target)
    }
}

/// Position of a term in a document that takes into account the term offset within the phrase.
struct PhrasePositions {
    pub position: i32,
    // position in doc
    pub count: i32,
    // remaining pos in this doc
    pub offset: i32,
    // position in phrase
    pub ord: i32,
    // unique across all PhrasePositions instances
    pub postings: *mut dyn PostingIterator,
    // stream of docs & positions
    pub next_pp_idx: i32,
    // used to make list
    pub rpt_group: i32,
    // >=0 indicates that this is a repeating PP
    pub rpt_ind: i32,
    // index in the rptGroup
    pub terms: Vec<Term>,
    // for repetitions initialization
}

unsafe impl Send for PhrasePositions {}

impl PhrasePositions {
    fn new(postings: *mut dyn PostingIterator, offset: i32, ord: i32, terms: Vec<Term>) -> Self {
        PhrasePositions {
            position: 0,
            count: 0,
            offset,
            ord,
            postings,
            next_pp_idx: -1,
            rpt_group: -1,
            rpt_ind: 0,
            terms,
        }
    }

    fn first_position(&mut self) -> Result<()> {
        unsafe {
            self.count = (*self.postings).freq()?;
        }
        self.next_position()?; // read first pos
        Ok(())
    }

    /// Go to next location of this term current document, and set
    /// <code>position</code> as <code>location - offset</code>, so that a
    /// matching exact phrase is easily identified when all PhrasePositions
    /// have exactly the same <code>position</code>.
    fn next_position(&mut self) -> Result<bool> {
        if self.count > 0 {
            // read subsequent pos's
            self.count -= 1;
            unsafe {
                self.position = (*self.postings).next_position()? - self.offset;
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

impl fmt::Debug for PhrasePositions {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "offset: {}, position: {}, count: {}",
            self.offset, self.position, self.count
        )?;
        if self.rpt_group >= 0 {
            write!(f, " , rpt: {}, rpt_index: {}", self.rpt_group, self.rpt_ind)?;
        }
        Ok(())
    }
}

/// PhrasePositions element in priority queue
struct PPElement {
    pub index: usize,
    // index in SloppyPhraseScorer.phrasePositions
    pub pp: *const PhrasePositions,
    // pointer to target
}

unsafe impl Send for PPElement {}

impl fmt::Debug for PPElement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        unsafe { write!(f, "index: {}, pp: ({:?})", self.index, *self.pp) }
    }
}

impl PartialOrd for PPElement {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // reversed ordering for priority queue
        unsafe {
            let position_cmp = (*other.pp).position.cmp(&(*self.pp).position);
            if position_cmp != Ordering::Equal {
                Some(position_cmp)
            } else {
                let offset_cmp = (*other.pp).offset.cmp(&(*self.pp).offset);
                if offset_cmp != Ordering::Equal {
                    Some(offset_cmp)
                } else {
                    Some((*other.pp).ord.cmp(&(*self.pp).ord))
                }
            }
        }
    }
}

impl Ord for PPElement {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl PartialEq for PPElement {
    fn eq(&self, other: &Self) -> bool {
        self.index.eq(&other.index)
    }
}

impl Eq for PPElement {}

// TODO a fake scorer struct used for `ConjunctionScorer`
struct PostingsIterAsScorer<T: PostingIterator> {
    pub iterator: T,
}

impl<T: PostingIterator> Scorer for PostingsIterAsScorer<T> {
    fn score(&mut self) -> Result<f32> {
        unreachable!()
    }
}

impl<T: PostingIterator> DocIterator for PostingsIterAsScorer<T> {
    fn doc_id(&self) -> i32 {
        self.iterator.doc_id()
    }

    fn next(&mut self) -> Result<i32> {
        self.iterator.next()
    }

    fn advance(&mut self, target: i32) -> Result<i32> {
        self.iterator.advance(target)
    }

    fn cost(&self) -> usize {
        self.iterator.cost()
    }
}

pub struct SloppyPhraseScorer<T: PostingIterator> {
    conjunction: ConjunctionScorer<PostingsIterAsScorer<T>>,
    // a conjunction doc id set iterator
    phrase_positions: Vec<PhrasePositions>,
    sloppy_freq: f32,
    // phrase frequency in current doc as computed by phraseFreq().
    doc_scorer: Box<dyn SimScorer>,
    slop: i32,
    num_postings: usize,
    pq: BinaryHeap<PPElement>,
    // for advancing min position
    end: i32,
    // current largest phrase position
    has_rpts: bool,
    // flag indicating that there are repetitions (as checked in
    // first candidate doc)
    checked_rpts: bool,
    // flag to only check for repetitions in first candidate doc
    has_multi_term_rpts: bool,
    // in each group are PPs that repeats each other (i.e. same term), sorted by (query) offset
    // value are index of related pp in self.phrase_positions
    rpt_group: Vec<Vec<usize>>,
    rpt_stack: Vec<usize>,
    // temporary stack for switching colliding repeating pps
    num_matches: i32,
    needs_scores: bool,
    match_cost: f32,
}

impl<T: PostingIterator + 'static> SloppyPhraseScorer<T> {
    fn new(
        postings: Vec<PostingsAndFreq<T>>,
        slop: i32,
        doc_scorer: Box<dyn SimScorer>,
        needs_scores: bool,
        match_cost: f32,
    ) -> Self {
        let num_postings = postings.len();
        let mut doc_iterators = Vec::with_capacity(num_postings);
        let mut phrase_positions = Vec::with_capacity(num_postings);
        for (idx, posting) in postings.into_iter().enumerate() {
            let iterator = posting.postings;
            doc_iterators.push(PostingsIterAsScorer { iterator });
            phrase_positions.push(PhrasePositions::new(
                &mut doc_iterators[idx].iterator,
                posting.pos,
                idx as i32,
                posting.terms.clone(),
            ));
        }
        let conjunction = ConjunctionScorer::new(doc_iterators);
        let pq = BinaryHeap::with_capacity(num_postings);
        SloppyPhraseScorer {
            conjunction,
            phrase_positions,
            sloppy_freq: 0f32,
            doc_scorer,
            slop,
            num_postings,
            pq,
            end: 0,
            has_rpts: false,
            checked_rpts: false,
            has_multi_term_rpts: false,
            rpt_group: Vec::new(),
            rpt_stack: Vec::new(),
            num_matches: 0,
            needs_scores,
            match_cost,
        }
    }

    /// Score a candidate doc for all slop-valid position-combinations (matches)
    /// encountered while traversing/hopping the PhrasePositions.
    /// <br> The score contribution of a match depends on the distance:
    /// <br> - highest score for distance=0 (exact match).
    /// <br> - score gets lower as distance gets higher.
    /// <br>Example: for query "a b"~2, a document "x a b a y" can be scored twice:
    /// once for "a b" (distance=0), and once for "b a" (distance=2).
    /// <br>Possibly not all valid combinations are encountered, because for efficiency
    /// we always propagate the least PhrasePosition. This allows to base on
    /// PriorityQueue and move forward faster.
    /// As result, for example, document "a b c b a"
    /// would score differently for queries "a b c"~4 and "c b a"~4, although
    /// they really are equivalent.
    /// Similarly, for doc "a b c b a f g", query "c b"~2
    /// would get same score as "g f"~2, although "c b"~2 could be matched twice.
    /// We may want to fix this in the future (currently not, for performance reasons).
    fn phrase_freq(&mut self) -> Result<f32> {
        if !self.init_phrase_positions()? {
            return Ok(0.0f32);
        }
        let mut freq = 0.0f32;
        self.num_matches = 0;
        let mut pp_idx = self.pq.pop().unwrap().index;
        let mut match_length = self.end - self.phrase_positions[pp_idx].position;
        let top_idx = self.pq.peek().unwrap().index;
        let mut next = self.phrase_positions[top_idx].position;
        while self.advance_pp(pp_idx)? {
            if self.has_rpts && !self.advance_rpts(pp_idx)? {
                break; //  pps exhausted
            }
            if self.phrase_positions[pp_idx].position > next {
                // done minimizing current match-length
                if match_length <= self.slop as i32 {
                    freq += self.doc_scorer.compute_slop_factor(match_length);
                    self.num_matches += 1;
                    if !self.needs_scores {
                        return Ok(freq);
                    }
                }
                let ele = self.pp_element(pp_idx);
                self.pq.push(ele);
                pp_idx = self.pq.pop().unwrap().index;
                next = self.phrase_positions[self.pq.peek().unwrap().index].position;
                match_length = self.end - self.phrase_positions[pp_idx].position;
            } else {
                let match_length2 = self.end - self.phrase_positions[pp_idx].position;
                match_length = min(match_length, match_length2);
            }
        }
        if match_length <= self.slop {
            freq += self.doc_scorer.compute_slop_factor(match_length); // score match
            self.num_matches += 1;
        }
        Ok(freq)
    }

    /// Initialize PhrasePositions in place.
    /// A one time initialization for this scorer (on first doc matching all terms):
    ///  - Check if there are repetitions
    ///  - If there are, find groups of repetitions.
    /// Examples:
    ///  1. no repetitions: "ho my"~2
    ///  2. repetitions: "ho my my"~2
    ///  3. repetitions: "my ho my"~2
    ///
    /// @return false if PPs are exhausted (and so current doc will not be a match)
    fn init_phrase_positions(&mut self) -> Result<bool> {
        self.end = i32::min_value();
        if !self.checked_rpts {
            return self.init_first_time();
        }
        if !self.has_rpts {
            self.init_simple()?;
            return Ok(true);
        }
        self.init_complex()
    }

    /// no repeats: simplest case, and most common.
    /// It is important to keep this piece of the code simple and efficient
    fn init_simple(&mut self) -> Result<()> {
        self.pq.clear();
        // position pps and build queue from list
        for idx in 0..self.num_postings {
            self.phrase_positions[idx].first_position()?;
            if self.phrase_positions[idx].position > self.end {
                self.end = self.phrase_positions[idx].position;
            }
            let ele = self.pp_element(idx);
            self.pq.push(ele);
        }
        Ok(())
    }

    /// with repeats: not so simple.
    fn init_complex(&mut self) -> Result<bool> {
        self.place_first_positions()?;
        if !self.advance_repeat_groups()? {
            return Ok(false);
        }
        self.fill_queue();
        Ok(true)
    }

    /// move all PPs to their first position
    fn place_first_positions(&mut self) -> Result<()> {
        for pp in &mut self.phrase_positions {
            pp.first_position()?;
        }
        Ok(())
    }

    /// advance a PhrasePosition and update 'end', return false if exhausted
    fn advance_pp(&mut self, pp_idx: usize) -> Result<bool> {
        if !self.phrase_positions[pp_idx].next_position()? {
            return Ok(false);
        }
        let position = self.phrase_positions[pp_idx].position;
        if position > self.end {
            self.end = position;
        }
        Ok(true)
    }

    /// pp was just advanced. If that caused a repeater collision, resolve by advancing the lesser
    /// of the two colliding pps. Note that there can only be one collision, as by the
    /// initialization there were no collisions before pp was advanced.
    fn advance_rpts(&mut self, pp_idx: usize) -> Result<bool> {
        if self.phrase_positions[pp_idx].rpt_group < 0 {
            return Ok(true); // not a repeater
        }
        let mut bits = FixedBitSet::new(
            self.rpt_group[self.phrase_positions[pp_idx].rpt_group as usize].len(),
        );
        let k0 = self.phrase_positions[pp_idx].rpt_ind;
        let mut cur_idx = pp_idx;
        loop {
            let k = self.collide(cur_idx);
            if k < 0 {
                break;
            }
            // always advance the lesser of the (only) two colliding pps
            cur_idx = self.lesser(
                cur_idx,
                self.rpt_group[self.phrase_positions[pp_idx].rpt_group as usize][k as usize],
            );
            if !self.advance_pp(cur_idx)? {
                return Ok(false);
            }
            if k != k0 {
                // careful: mark only those currently in the queue
                bits.ensure_capacity(k as usize);
                bits.set(k as usize);
            }
        }
        // collisions resolved, now re-queue
        // empty (partially) the queue until seeing all pps advanced for resolving collisions
        let mut n = 0usize;
        // TODO would be good if we can avoid calling cardinality() in each iteration!
        let num_bits = bits.len();

        while bits.cardinality() > 0 {
            let pp2_idx = self.pq.pop().unwrap().index;
            self.rpt_stack[n] = pp2_idx;
            n += 1;
            {
                let pp2 = &self.phrase_positions[pp2_idx];
                if pp2.rpt_group >= 0 && pp2.rpt_ind < num_bits as i32 // this bit may not have been set
                    && bits.get(pp2.rpt_ind as usize)?
                {
                    bits.clear_batch(pp2.rpt_ind as usize, (pp2.rpt_ind + 1) as usize);
                }
            }
        }
        // add back to queue
        for i in 0..n {
            let ele = self.pp_element(self.rpt_stack[n - 1 - i]);
            self.pq.push(ele);
        }
        Ok(true)
    }

    /// compare two pps, but only by position and offset
    fn lesser(&self, idx: usize, idx2: usize) -> usize {
        let pp = &self.phrase_positions[idx];
        let pp2 = &self.phrase_positions[idx2];
        if pp.position < pp2.position || (pp.position == pp2.position && pp.offset < pp2.offset) {
            idx
        } else {
            idx2
        }
    }

    /// index of a pp2 colliding with pp, or -1 if none
    fn collide(&self, pp_idx: usize) -> i32 {
        let pp = &self.phrase_positions[pp_idx];
        let tp_pos = Self::tp_pos(pp);
        let rg = &self.rpt_group[pp.rpt_group as usize];
        for i in rg {
            let pp2 = &self.phrase_positions[*i];
            if pp_idx != *i && Self::tp_pos(pp2) == tp_pos {
                return pp2.rpt_ind;
            }
        }
        -1
    }

    fn pp_element(&self, index: usize) -> PPElement {
        let pp = &self.phrase_positions[index] as *const PhrasePositions;
        PPElement { index, pp }
    }

    /// Fill the queue (all pps are already placed
    fn fill_queue(&mut self) {
        self.pq.clear();
        let mut end = self.end;
        for (idx, pq) in self.phrase_positions.iter().enumerate() {
            if pq.position > end {
                end = pq.position;
            }
            let ele = self.pp_element(idx);
            self.pq.push(ele);
        }
        self.end = end;
    }

    ///  At initialization (each doc), each repetition group is sorted by (query) offset.
    /// This provides the start condition: no collisions.
    /// Case 1: no multi-term repeats
    /// It is sufficient to advance each pp in the group by one less than its group index.
    /// So lesser pp is not advanced, 2nd one advance once, 3rd one advanced twice, etc.
    /// Case 2: multi-term repeats
    ///
    /// @return false if PPs are exhausted.
    fn advance_repeat_groups(&mut self) -> Result<bool> {
        for rg_idx in 0..self.rpt_group.len() {
            if self.has_multi_term_rpts {
                // more involved, some may not collide
                let mut incr;
                let mut i = 0;
                while i < self.rpt_group[rg_idx].len() {
                    incr = 1;
                    let pp_idx = self.rpt_group[rg_idx][i];
                    let mut k = self.collide(pp_idx);
                    while k >= 0 {
                        let pp_idx2 = self.lesser(pp_idx, self.rpt_group[rg_idx][k as usize]);
                        // at initialization always advance pp with higher offset
                        if !self.advance_pp(pp_idx2)? {
                            return Ok(false); // exhausted
                        }
                        if self.phrase_positions[pp_idx2].rpt_ind < i as i32 {
                            // should no happen?
                            incr = 0;
                            break;
                        }
                        k = self.collide(pp_idx);
                    }
                    i += incr;
                }
            } else {
                // simpler, we know exactly how much to advance
                for j in 1..self.rpt_group[rg_idx].len() {
                    for _ in 0..j {
                        if !self.phrase_positions[self.rpt_group[rg_idx][j]].next_position()? {
                            return Ok(false); // PPs exhausted
                        }
                    }
                }
            }
        }
        Ok(true)
    }

    /// initialize with checking for repeats. Heavy work, but done only for the first candidate
    /// doc. If there are repetitions, check if multi-term postings (MTP) are involved.
    /// Without MTP, once PPs are placed in the first candidate doc, repeats (and groups) are
    /// visible. With MTP, a more complex check is needed, up-front, as there may be "hidden
    /// collisions". For example P1 has {A,B}, P1 has {B,C}, and the first doc is: "A C B". At
    /// start, P1 would point to "A", p2 to "C", and it will not be identified that P1 and P2
    /// are repetitions of each other. The more complex initialization has two parts:
    /// (1) identification of repetition groups.
    /// (2) advancing repeat groups at the start of the doc.
    /// For (1), a possible solution is to just create a single repetition group,
    /// made of all repeating pps. But this would slow down the check for collisions,
    /// as all pps would need to be checked. Instead, we compute "connected regions"
    /// on the bipartite graph of postings and terms.
    fn init_first_time(&mut self) -> Result<bool> {
        self.checked_rpts = true;
        self.place_first_positions()?;

        let (rpt_terms, terms) = self.repeating_terms();
        self.has_rpts = !terms.is_empty();

        if self.has_rpts {
            self.rpt_stack = vec![0usize; self.num_postings];
            let rgs = self.gather_rpt_groups(&rpt_terms, &terms);
            self.sort_rpt_groups(rgs);
            if !self.advance_repeat_groups()? {
                return Ok(false); // PPs exhausted
            }
        }
        self.fill_queue();
        Ok(true)
    }

    /// sort each repetition group by (query) offset.
    /// Done only once (at first doc) and allows to initialize faster for each doc.
    fn sort_rpt_groups(&mut self, rgs: Vec<Vec<usize>>) {
        for mut rg in rgs {
            rg.sort_by(|idx1, idx2| {
                self.phrase_positions[*idx1]
                    .offset
                    .cmp(&self.phrase_positions[*idx2].offset)
            });
            for (j, idx) in rg.iter().enumerate() {
                // we use this index for efficient re-queuing
                self.phrase_positions[*idx].rpt_ind = j as i32;
            }
            self.rpt_group.push(rg);
        }
    }

    /// Detect repetition groups. Done once - for first doc
    fn gather_rpt_groups(
        &mut self,
        rpt_terms: &HashMap<Term, usize>,
        terms: &[Term],
    ) -> Vec<Vec<usize>> {
        let rpp = self.repeating_pps(rpt_terms);
        let mut res = Vec::new();
        if !self.has_multi_term_rpts {
            // simpler - no multi-terms - can base on positions in first doc
            for i in 0..rpp.len() {
                let idx1 = rpp[i];
                if self.phrase_positions[idx1].rpt_group >= 0 {
                    // already marked as a repetition
                    continue;
                }
                let tp_pos = Self::tp_pos(&self.phrase_positions[idx1]);
                for idx2 in rpp.iter().skip(i + 1) {
                    let idx2 = *idx2;
                    if self.phrase_positions[idx2].rpt_group >= 0 || // already marked as a repetition
                        self.phrase_positions[idx2].offset == self.phrase_positions[idx1].offset || // not a repetition: two PPs are originally in same offset in the query!
                        Self::tp_pos(&self.phrase_positions[idx2]) != tp_pos
                    {
                        continue;
                    }
                    // a repetition
                    let mut g = self.phrase_positions[idx1].rpt_group;
                    if g < 0 {
                        g = res.len() as i32;
                        self.phrase_positions[idx1].rpt_group = g;
                        let mut rl = Vec::with_capacity(2);
                        rl.push(idx1);
                        res.push(rl);
                    }
                    self.phrase_positions[idx2].rpt_group = g;
                    res[g as usize].push(idx2);
                }
            }
        } else {
            // more involved - has multi-terms
            let mut tmp = Vec::new();
            let mut bb = self.pp_terms_bit_sets(&rpp, rpt_terms);
            Self::union_term_groups(&mut bb);
            let tg = self.term_groups(terms, &bb);
            let mut distinct_group_ids = HashSet::new();
            for v in tg.values() {
                distinct_group_ids.insert(*v);
            }
            for _ in 0..distinct_group_ids.len() {
                tmp.push(HashSet::new());
            }
            for pp_idx in &rpp {
                for term_idx in 0..self.phrase_positions[*pp_idx].terms.len() {
                    if rpt_terms.contains_key(&self.phrase_positions[*pp_idx].terms[term_idx]) {
                        let g = tg[&self.phrase_positions[*pp_idx].terms[term_idx]];
                        tmp[g].insert(*pp_idx);
                        assert!(
                            self.phrase_positions[*pp_idx].rpt_group == -1
                                || self.phrase_positions[*pp_idx].rpt_group == g as i32
                        );
                        self.phrase_positions[*pp_idx].rpt_group = g as i32;
                    }
                }
            }
            for hs in &tmp {
                let mut data = Vec::with_capacity(hs.len());
                for v in hs {
                    data.push(*v);
                }
                res.push(data);
            }
        }
        res
    }

    /// Actual position in doc of a PhrasePosition, relies on that position = tpPos - offset)
    fn tp_pos(pp: &PhrasePositions) -> i32 {
        pp.position + pp.offset
    }

    /// find repeating terms and assign them ordinal values
    fn repeating_terms(&self) -> (HashMap<Term, usize>, Vec<Term>) {
        let mut tord = HashMap::new();
        let mut terms = Vec::new();
        let mut tcnt = HashMap::new();
        for pp in &self.phrase_positions {
            for t in &pp.terms {
                let cnt = if let Some(v) = tcnt.get(t) {
                    *v + 1usize
                } else {
                    1usize
                };
                tcnt.insert(t.clone(), cnt);
                if cnt == 2 {
                    let ord = tord.len();
                    tord.insert(t.clone(), ord);
                    terms.push(t.clone());
                }
            }
        }
        (tord, terms)
    }

    /// find repeating pps, and for each, if has multi-terms, update this.hasMultiTermRpts
    fn repeating_pps(&mut self, rpt_terms: &HashMap<Term, usize>) -> Vec<usize> {
        let mut rp = Vec::new();
        let mut has_multi_term_rpts = false;
        for (idx, pp) in self.phrase_positions.iter().enumerate() {
            for t in &pp.terms {
                if rpt_terms.contains_key(t) {
                    rp.push(idx);
                    has_multi_term_rpts |= pp.terms.len() > 1;
                    break;
                }
            }
        }
        self.has_multi_term_rpts |= has_multi_term_rpts;
        rp
    }

    /// bit-sets - for each repeating pp, for each of its repeating terms, the term ordinal values
    /// is set
    fn pp_terms_bit_sets(
        &self,
        rpp_idx: &[usize],
        tord: &HashMap<Term, usize>,
    ) -> Vec<FixedBitSet> {
        let mut bb = Vec::with_capacity(rpp_idx.len());
        for idx in rpp_idx {
            let mut b = FixedBitSet::new(tord.len());
            for t in &self.phrase_positions[*idx].terms {
                if let Some(ord) = tord.get(t) {
                    b.set(*ord);
                }
            }
            bb.push(b);
        }
        bb
    }

    /// union (term group) bit-sets until they are disjoint (O(n^^2)),
    /// and each group have different terms
    fn union_term_groups(bb: &mut Vec<FixedBitSet>) {
        let mut incr;
        let mut i = 0;
        while i < bb.len() - 1 {
            incr = 1;
            let mut j = i + 1;
            while j < bb.len() {
                if bb[i].intersects(&bb[j]) {
                    let bbj = bb.remove(j);
                    bb[i].set_or(&bbj);
                    incr = 0;
                } else {
                    j += 1;
                }
            }
            i += incr;
        }
    }

    /// map each term to the single group that contains it
    fn term_groups(&self, terms: &[Term], bb: &[FixedBitSet]) -> HashMap<Term, usize> {
        let mut tg = HashMap::new();
        for (i, bit) in bb.iter().enumerate() {
            let mut ord = bit.next_set_bit(0);
            while ord != NO_MORE_DOCS {
                tg.insert(terms[ord as usize].clone(), i);
                ord = if ord as usize >= bit.len() {
                    NO_MORE_DOCS
                } else {
                    bit.next_set_bit((ord + 1) as usize)
                };
            }
        }
        tg
    }
}

impl<T: PostingIterator + 'static> Scorer for SloppyPhraseScorer<T> {
    fn score(&mut self) -> Result<f32> {
        let doc_id = self.doc_id();
        self.doc_scorer.score(doc_id, self.sloppy_freq)
    }
}

impl<T: PostingIterator + 'static> DocIterator for SloppyPhraseScorer<T> {
    fn doc_id(&self) -> i32 {
        self.conjunction.doc_id()
    }

    fn next(&mut self) -> Result<i32> {
        self.approximate_next()?;
        two_phase_next(self)
    }

    fn advance(&mut self, target: i32) -> Result<i32> {
        self.approximate_advance(target)?;
        two_phase_next(self)
    }

    fn cost(&self) -> usize {
        self.conjunction.cost()
    }

    fn matches(&mut self) -> Result<bool> {
        let sloppy_freq = self.phrase_freq()?; // check for phrase
        self.sloppy_freq = sloppy_freq;
        Ok(sloppy_freq > f32::EPSILON)
    }

    fn match_cost(&self) -> f32 {
        self.match_cost
    }

    fn support_two_phase(&self) -> bool {
        true
    }

    fn approximate_next(&mut self) -> Result<DocId> {
        self.conjunction.next()
    }

    fn approximate_advance(&mut self, target: i32) -> Result<i32> {
        self.conjunction.advance(target)
    }
}
