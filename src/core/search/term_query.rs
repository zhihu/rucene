use error::*;
use std::boxed::Box;
use std::collections::HashMap;
use std::fmt;

use core::index::term::TermState;
use core::index::LeafReader;
use core::index::{Term, TermContext};
use core::index::{POSTINGS_FREQS, POSTINGS_NONE};
use core::search::explanation::Explanation;
use core::search::posting_iterator::{EmptyPostingIterator, PostingIterator};
use core::search::searcher::IndexSearcher;
use core::search::statistics::*;
use core::search::term_scorer::TermScorer;
use core::search::{Query, Scorer, SimWeight, Similarity, Weight};
use core::util::{DocId, KeyedContext};

pub const TERM: &str = "term";

#[derive(Clone, Debug, PartialEq)]
pub struct TermQuery {
    pub term: Term,
    pub boost: f32,
    pub ctx: Option<KeyedContext>,
}

impl TermQuery {
    pub fn new<T: Into<Option<KeyedContext>>>(term: Term, boost: f32, ctx: T) -> TermQuery {
        let ctx = ctx.into();
        TermQuery { term, boost, ctx }
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
                vec![searcher.term_statistics(self.term.clone(), &term_context)],
                searcher.collections_statistics(&self.term.field)?,
            )
        } else {
            (
                vec![TermStatistics::new(self.term.bytes.clone(), max_doc, -1)],
                CollectionStatistics::new(self.term.field.clone(), max_doc, -1, -1, -1),
            )
        };
        let similarity = searcher.similarity(&self.term.field, needs_scores);
        let sim_weight =
            similarity.compute_weight(&collection_stats, &term_stats, self.ctx.as_ref());
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

    fn create_postings_iterator(
        &self,
        reader: &LeafReader,
        flags: i32,
    ) -> Result<Box<PostingIterator>> {
        if let Some(state) = self.term_states.get(&reader.doc_base()) {
            reader.postings_from_state(&self.term, state.as_ref(), flags)
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
            self.create_postings_iterator(reader, i32::from(flags))?,
            self.boost,
        )))
    }

    fn query_type(&self) -> &'static str {
        TERM
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

    fn explain(&self, reader: &LeafReader, doc: DocId) -> Result<Explanation> {
        let flags = if self.needs_scores {
            POSTINGS_FREQS
        } else {
            POSTINGS_NONE
        };

        let mut postings_iterator = self.create_postings_iterator(reader, i32::from(flags))?;
        let new_doc = postings_iterator.advance(doc)?;
        if new_doc == doc {
            let freq = postings_iterator.freq()? as f32;

            let freq_expl = Explanation::new(true, freq, format!("termFreq={}", freq), vec![]);
            let score_expl = self.sim_weight.explain(reader, doc, freq_expl)?;

            Ok(Explanation::new(
                true,
                score_expl.value(),
                format!(
                    "weight({} in {}) [{}], result of:",
                    self, doc, self.similarity
                ),
                vec![score_expl],
            ))
        } else {
            Ok(Explanation::new(
                false,
                0f32,
                "no matching term".to_string(),
                vec![],
            ))
        }
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
