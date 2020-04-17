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

use error::Result;

use std::fmt;

use core::codec::Codec;
use core::codec::{PostingIterator, PostingIteratorFlags};
use core::doc::Term;
use core::index::reader::LeafReaderContext;
use core::search::explanation::Explanation;
use core::search::query::{Query, Weight};
use core::search::scorer::{Scorer, TermScorer};
use core::search::searcher::SearchPlanBuilder;
use core::search::similarity::{SimWeight, Similarity};
use core::search::statistics::{CollectionStatistics, TermStatistics};
use core::search::DocIterator;

use core::util::{DocId, KeyedContext};

pub const TERM: &str = "term";

/// A Query that matches documents containing a term.
///
/// This may be combined with other terms with a
/// [`BooleanQuery`](../search/struct.BooleanQuery.html)
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

    #[inline]
    pub fn term(&self) -> &Term {
        &self.term
    }
}

impl<C: Codec> Query<C> for TermQuery {
    fn create_weight(
        &self,
        searcher: &dyn SearchPlanBuilder<C>,
        needs_scores: bool,
    ) -> Result<Box<dyn Weight<C>>> {
        let max_doc = searcher.max_doc() as i64;

        let term_stats = if needs_scores {
            vec![searcher.term_statistics(&self.term)?]
        } else {
            vec![TermStatistics::new(self.term.bytes.clone(), max_doc, -1)]
        };

        let collection_stats = if needs_scores {
            if let Some(stat) = searcher.collections_statistics(&self.term.field) {
                stat.clone()
            } else {
                CollectionStatistics::new(self.term.field.clone(), 0, max_doc, -1, -1, -1)
            }
        } else {
            CollectionStatistics::new(self.term.field.clone(), 0, max_doc, -1, -1, -1)
        };

        let similarity = searcher.similarity(&self.term.field, needs_scores);
        let sim_weight = similarity.compute_weight(
            &collection_stats,
            &term_stats,
            self.ctx.as_ref(),
            self.boost,
        );
        Ok(Box::new(TermWeight::new(
            self.term.clone(),
            self.boost,
            similarity,
            sim_weight,
            needs_scores,
        )))
    }

    fn extract_terms(&self) -> Vec<TermQuery> {
        vec![self.clone()]
    }

    fn as_any(&self) -> &dyn (::std::any::Any) {
        self
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

struct TermWeight<C: Codec> {
    term: Term,
    boost: f32,
    similarity: Box<dyn Similarity<C>>,
    sim_weight: Box<dyn SimWeight<C>>,
    needs_scores: bool,
}

impl<C: Codec> TermWeight<C> {
    pub fn new(
        term: Term,
        boost: f32,
        similarity: Box<dyn Similarity<C>>,
        sim_weight: Box<dyn SimWeight<C>>,
        needs_scores: bool,
    ) -> TermWeight<C> {
        TermWeight {
            term,
            boost,
            similarity,
            sim_weight,
            needs_scores,
        }
    }
}

impl<C: Codec> Weight<C> for TermWeight<C> {
    fn create_scorer(&self, reader: &LeafReaderContext<'_, C>) -> Result<Option<Box<dyn Scorer>>> {
        let _norms = reader.reader.norm_values(&self.term.field);
        let sim_scorer = self.sim_weight.sim_scorer(reader.reader)?;

        let flags = if self.needs_scores {
            PostingIteratorFlags::FREQS
        } else {
            PostingIteratorFlags::NONE
        };

        if let Some(postings_iterator) = reader.reader.postings(&self.term, flags as i32)? {
            Ok(Some(Box::new(TermScorer::new(
                sim_scorer,
                postings_iterator,
            ))))
        } else {
            Ok(None)
        }
    }

    fn query_type(&self) -> &'static str {
        TERM
    }

    fn normalize(&mut self, norm: f32, boost: f32) {
        self.sim_weight.normalize(norm, boost * self.boost)
    }

    fn value_for_normalization(&self) -> f32 {
        self.sim_weight.get_value_for_normalization()
    }

    fn needs_scores(&self) -> bool {
        self.needs_scores
    }

    fn explain(&self, reader: &LeafReaderContext<'_, C>, doc: DocId) -> Result<Explanation> {
        let flags = if self.needs_scores {
            PostingIteratorFlags::FREQS
        } else {
            PostingIteratorFlags::NONE
        };

        if let Some(mut postings_iterator) = reader.reader.postings(&self.term, flags as i32)? {
            let new_doc = postings_iterator.advance(doc)?;
            if new_doc == doc {
                let freq = postings_iterator.freq()? as f32;

                let freq_expl = Explanation::new(true, freq, format!("termFreq={}", freq), vec![]);
                let score_expl = self.sim_weight.explain(reader.reader, doc, freq_expl)?;

                return Ok(Explanation::new(
                    true,
                    score_expl.value(),
                    format!(
                        "weight({} in {}) [{}], result of:",
                        self, doc, self.similarity
                    ),
                    vec![score_expl],
                ));
            }
        }
        Ok(Explanation::new(
            false,
            0f32,
            "no matching term".to_string(),
            vec![],
        ))
    }
}

impl<C: Codec> fmt::Display for TermWeight<C> {
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
