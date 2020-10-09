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

mod bulk_scorer;

pub use self::bulk_scorer::*;

mod conjunction_scorer;

pub use self::conjunction_scorer::*;

mod disjunction_scorer;

pub use self::disjunction_scorer::*;

mod min_scorer;

pub use self::min_scorer::*;

mod req_opt_scorer;

pub use self::req_opt_scorer::*;

mod req_not_scorer;

pub use self::req_not_scorer::*;

mod rescorer;

pub use self::rescorer::*;

mod term_scorer;

pub use self::term_scorer::*;

mod boosting_scorer;

pub use self::boosting_scorer::*;

mod phrase_scorer;

pub use self::phrase_scorer::*;

use std::collections::HashMap;
use std::i32;

use core::util::{DocId, IndexedContext, VariantValue};

use core::search::query::AllDocsIterator;
use core::search::{DocIterator, NO_MORE_DOCS};
use error::{ErrorKind::IllegalArgument, Result};

pub struct FeatureResult {
    pub extra_params: HashMap<String, VariantValue>,
}

impl FeatureResult {
    pub fn new(params: HashMap<String, VariantValue>) -> FeatureResult {
        FeatureResult {
            extra_params: params,
        }
    }
}

/// Expert: Common scoring functionality for different types of queries.
///
/// A `Scorer` exposes an `iterator()` over documents matching a query in increasing order of doc
/// Id.
///
/// Document scores are computed using a given `Similarity` implementation.
///
/// **NOTE**: The values `f32::NAN`, `f32::NEGATIVE_INFINITY` and `f32::POSITIVE_INFINITY` are
/// not valid scores.  Certain collectors (eg `TopDocCollector`) will not properly collect hits
/// with these scores.
pub trait Scorer: DocIterator {
    /// Returns the score of the current document matching the query.
    /// Initially invalid, until `DocIterator::next()` or
    /// `DocIterator::advance()` is called on the `iterator()`
    /// the first time, or when called from within `LeafCollector::collect`.
    fn score(&mut self) -> Result<f32>;

    fn score_context(&mut self) -> Result<IndexedContext> {
        unimplemented!()
    }

    fn score_feature(&mut self) -> Result<Vec<FeatureResult>> {
        Ok(vec![])
    }
}

impl Scorer for Box<dyn Scorer> {
    fn score(&mut self) -> Result<f32> {
        (**self).score()
    }

    fn score_context(&mut self) -> Result<IndexedContext> {
        (**self).score_context()
    }

    fn score_feature(&mut self) -> Result<Vec<FeatureResult>> {
        (**self).score_feature()
    }
}

impl DocIterator for Box<dyn Scorer> {
    fn doc_id(&self) -> i32 {
        (**self).doc_id()
    }

    fn next(&mut self) -> Result<i32> {
        (**self).next()
    }

    fn advance(&mut self, target: i32) -> Result<i32> {
        (**self).advance(target)
    }

    fn slow_advance(&mut self, target: i32) -> Result<i32> {
        (**self).slow_advance(target)
    }

    fn cost(&self) -> usize {
        (**self).cost()
    }

    fn matches(&mut self) -> Result<bool> {
        (**self).matches()
    }

    fn match_cost(&self) -> f32 {
        (**self).match_cost()
    }

    fn support_two_phase(&self) -> bool {
        (**self).support_two_phase()
    }

    fn approximate_next(&mut self) -> Result<i32> {
        (**self).approximate_next()
    }

    fn approximate_advance(&mut self, target: i32) -> Result<i32> {
        (**self).approximate_advance(target)
    }
}

/// helper function for doc iterator support two phase
pub fn two_phase_next(scorer: &mut dyn Scorer) -> Result<DocId> {
    let mut doc = scorer.doc_id();
    loop {
        if doc == NO_MORE_DOCS {
            return Ok(NO_MORE_DOCS);
        } else if scorer.matches()? {
            return Ok(doc);
        }
        doc = scorer.approximate_next()?;
    }
}

impl Eq for dyn Scorer {}

impl PartialEq for dyn Scorer {
    fn eq(&self, other: &Self) -> bool {
        self == other
    }
}

#[allow(dead_code)]
pub fn scorer_as_bits(max_doc: i32, scorer: Box<dyn Scorer>) -> DocIteratorAsBits {
    DocIteratorAsBits::new(max_doc, scorer)
}

pub struct DocIteratorAsBits {
    iterator: Box<dyn Scorer>,
    previous: DocId,
    previous_matched: bool,
    max_doc: i32,
}

impl DocIteratorAsBits {
    pub fn new(max_doc: i32, scorer: Box<dyn Scorer>) -> DocIteratorAsBits {
        DocIteratorAsBits {
            iterator: scorer,
            previous: -1,
            previous_matched: false,
            max_doc,
        }
    }

    pub fn all_doc(max_doc: i32) -> DocIteratorAsBits {
        let scorer = Box::new(ConstantScoreScorer::new(
            1f32,
            AllDocsIterator::new(max_doc),
            max_doc as usize,
        ));
        DocIteratorAsBits {
            iterator: scorer,
            previous: -1,
            previous_matched: false,
            max_doc,
        }
    }

    pub fn get(&mut self, index: usize) -> Result<bool> {
        let index = index as i32;
        if index < 0 || index >= self.max_doc {
            bail!(IllegalArgument(format!(
                "{} is out of bounds: [0-{}]",
                index, self.max_doc
            )));
        }

        if index < self.previous {
            bail!(IllegalArgument(format!(
                "This Bits instance can only be consumed in order. Got called on [{}] while \
                 previously called on [{}].",
                index, self.previous
            )));
        }

        if index == self.previous {
            return Ok(self.previous_matched);
        }

        self.previous = index;
        let mut doc = self.iterator.doc_id();
        if doc < index {
            doc = self.iterator.advance(index)?;
        }

        self.previous_matched = index == doc;
        Ok(self.previous_matched)
    }

    pub fn len(&self) -> usize {
        self.max_doc as usize
    }

    pub fn is_empty(&self) -> bool {
        self.max_doc <= 0
    }
}

/// A constant-scoring `Scorer`
pub struct ConstantScoreScorer<T: DocIterator> {
    pub score: f32,
    pub iterator: T,
    pub cost: usize,
}

impl<T: DocIterator> ConstantScoreScorer<T> {
    pub fn new(score: f32, iterator: T, cost: usize) -> ConstantScoreScorer<T> {
        ConstantScoreScorer {
            score,
            iterator,
            cost,
        }
    }
}

impl<T: DocIterator> Scorer for ConstantScoreScorer<T> {
    fn score(&mut self) -> Result<f32> {
        Ok(self.score)
    }
}

impl<T: DocIterator> DocIterator for ConstantScoreScorer<T> {
    fn doc_id(&self) -> DocId {
        self.iterator.doc_id()
    }

    fn next(&mut self) -> Result<DocId> {
        self.iterator.next()
    }

    fn advance(&mut self, target: DocId) -> Result<DocId> {
        self.iterator.advance(target)
    }

    fn slow_advance(&mut self, target: i32) -> Result<DocId> {
        self.iterator.slow_advance(target)
    }

    fn cost(&self) -> usize {
        self.cost
    }

    fn approximate_next(&mut self) -> Result<DocId> {
        self.iterator.approximate_next()
    }

    fn approximate_advance(&mut self, target: i32) -> Result<DocId> {
        self.iterator.approximate_advance(target)
    }
}
