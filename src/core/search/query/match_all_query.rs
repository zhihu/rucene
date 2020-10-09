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

use core::codec::Codec;
use core::index::reader::LeafReaderContext;
use core::search::explanation::Explanation;
use core::search::query::{Query, TermQuery, Weight};
use core::search::scorer::{two_phase_next, ConstantScoreScorer, Scorer};
use core::search::searcher::SearchPlanBuilder;
use core::search::{DocIterator, NO_MORE_DOCS};
use core::util::DocId;
use error::Result;
use std::fmt;

pub const MATCH_ALL: &str = "match_all";

/// A query that matches all documents.
pub struct MatchAllDocsQuery;

impl<C: Codec> Query<C> for MatchAllDocsQuery {
    fn create_weight(
        &self,
        _searcher: &dyn SearchPlanBuilder<C>,
        _needs_scores: bool,
    ) -> Result<Box<dyn Weight<C>>> {
        Ok(Box::new(MatchAllDocsWeight::default()))
    }

    fn extract_terms(&self) -> Vec<TermQuery> {
        vec![]
    }

    fn as_any(&self) -> &dyn (::std::any::Any) {
        self
    }
}

impl fmt::Display for MatchAllDocsQuery {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MatchAllDocsQuery()")
    }
}

struct MatchAllDocsWeight {
    weight: f32,
    norm: f32,
}

impl Default for MatchAllDocsWeight {
    fn default() -> Self {
        MatchAllDocsWeight {
            weight: 0f32,
            norm: 1f32,
        }
    }
}

impl<C: Codec> Weight<C> for MatchAllDocsWeight {
    fn create_scorer(
        &self,
        leaf_reader: &LeafReaderContext<'_, C>,
    ) -> Result<Option<Box<dyn Scorer>>> {
        let max_doc = leaf_reader.reader.max_doc();
        Ok(Some(Box::new(ConstantScoreScorer {
            score: self.weight,
            iterator: AllDocsIterator::new(max_doc),
            cost: max_doc as usize,
        })))
    }

    fn query_type(&self) -> &'static str {
        MATCH_ALL
    }

    fn normalize(&mut self, norm: f32, boost: f32) {
        self.norm = norm;
        self.weight = norm * boost;
    }

    fn value_for_normalization(&self) -> f32 {
        self.weight * self.weight
    }

    fn needs_scores(&self) -> bool {
        false
    }

    fn explain(&self, _reader: &LeafReaderContext<'_, C>, _doc: DocId) -> Result<Explanation> {
        Ok(Explanation::new(
            true,
            self.weight,
            format!("{}, product of:", self),
            vec![
                Explanation::new(true, self.weight, "boost".to_string(), vec![]),
                Explanation::new(true, self.norm, "queryNorm".to_string(), vec![]),
            ],
        ))
    }
}

impl fmt::Display for MatchAllDocsWeight {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MatchAllDocsWeight()")
    }
}

/// a `DocIterator` for all docs
pub struct AllDocsIterator {
    doc: DocId,
    max_doc: DocId,
}

impl AllDocsIterator {
    pub fn new(max_doc: DocId) -> AllDocsIterator {
        assert!(max_doc >= 0);
        AllDocsIterator { doc: -1, max_doc }
    }
}

impl Scorer for AllDocsIterator {
    fn score(&mut self) -> Result<f32> {
        Ok(1f32)
    }
}

impl DocIterator for AllDocsIterator {
    fn doc_id(&self) -> DocId {
        self.doc
    }

    fn next(&mut self) -> Result<DocId> {
        let target = self.doc + 1;
        self.advance(target)
    }

    fn advance(&mut self, target: DocId) -> Result<DocId> {
        self.doc = if target >= self.max_doc {
            NO_MORE_DOCS
        } else {
            target
        };
        Ok(self.doc)
    }

    fn cost(&self) -> usize {
        1usize.max(self.max_doc as usize)
    }
}

pub const CONSTANT: &str = "constant";

/// A query that wraps another query and simply returns a constant score equal to
/// 1 for every document that matches the query.
///
/// It therefore simply strips of all scores and always returns 1.
pub struct ConstantScoreQuery<C: Codec> {
    pub query: Box<dyn Query<C>>,
    boost: f32,
}

impl<C: Codec> ConstantScoreQuery<C> {
    pub fn new(query: Box<dyn Query<C>>) -> ConstantScoreQuery<C> {
        ConstantScoreQuery { query, boost: 0f32 }
    }

    pub fn with_boost(query: Box<dyn Query<C>>, boost: f32) -> ConstantScoreQuery<C> {
        ConstantScoreQuery { query, boost }
    }

    pub fn get_raw_query(&self) -> &dyn Query<C> {
        self.query.as_ref()
    }
}

impl<C: Codec> fmt::Display for ConstantScoreQuery<C> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ConstantScoreQuery(sub_query: {}, weight: {})",
            self.query, self.boost
        )
    }
}

impl<C: Codec> Query<C> for ConstantScoreQuery<C> {
    fn create_weight(
        &self,
        searcher: &dyn SearchPlanBuilder<C>,
        needs_scores: bool,
    ) -> Result<Box<dyn Weight<C>>> {
        let weight = searcher.create_weight(self.query.as_ref(), false)?;
        if needs_scores {
            Ok(Box::new(ConstantScoreWeight::new(weight, self.boost)))
        } else {
            Ok(weight)
        }
    }

    fn extract_terms(&self) -> Vec<TermQuery> {
        vec![]
    }

    fn as_any(&self) -> &dyn (::std::any::Any) {
        self
    }
}

struct ConstantScoreWeight<C: Codec> {
    sub_weight: Box<dyn Weight<C>>,
    query_norm: f32,
    query_weight: f32,
}

impl<C: Codec> ConstantScoreWeight<C> {
    pub fn new(sub_weight: Box<dyn Weight<C>>, boost: f32) -> ConstantScoreWeight<C> {
        ConstantScoreWeight {
            sub_weight,
            query_weight: boost,
            query_norm: 1.0f32,
        }
    }
}

impl<C: Codec> Weight<C> for ConstantScoreWeight<C> {
    fn create_scorer(&self, reader: &LeafReaderContext<'_, C>) -> Result<Option<Box<dyn Scorer>>> {
        if let Some(inner_scorer) = self.sub_weight.create_scorer(reader)? {
            let cost = inner_scorer.cost();
            Ok(Some(Box::new(ConstantScoreScorer {
                score: self.query_weight,
                iterator: inner_scorer,
                cost,
            })))
        } else {
            Ok(None)
        }
    }

    fn query_type(&self) -> &'static str {
        CONSTANT
    }

    fn normalize(&mut self, norm: f32, boost: f32) {
        self.query_weight = norm * boost;
        self.query_norm = norm;
    }

    fn value_for_normalization(&self) -> f32 {
        self.query_weight * self.query_weight
    }

    fn needs_scores(&self) -> bool {
        false
    }

    fn explain(&self, reader: &LeafReaderContext<'_, C>, doc: DocId) -> Result<Explanation> {
        let exists = if let Some(mut iterator) = self.sub_weight.create_scorer(reader)? {
            if iterator.support_two_phase() {
                two_phase_next(iterator.as_mut())? == doc && iterator.matches()?
            } else {
                iterator.advance(doc)? == doc
            }
        } else {
            false
        };

        if exists {
            Ok(Explanation::new(
                true,
                self.query_weight,
                format!("{}, product of:", self.sub_weight),
                vec![
                    Explanation::new(true, self.query_weight, "boost".to_string(), vec![]),
                    Explanation::new(true, self.query_norm, "queryNorm".to_string(), vec![]),
                ],
            ))
        } else {
            Ok(Explanation::new(
                false,
                0.0f32,
                format!("{} doesn't match id {}", self.sub_weight, doc),
                vec![],
            ))
        }
    }
}

impl<C: Codec> fmt::Display for ConstantScoreWeight<C> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ConstantScoreWeight(sub_weight: {}, query_weight: {}, query_norm: {})",
            self.sub_weight, self.query_weight, self.query_norm
        )
    }
}
