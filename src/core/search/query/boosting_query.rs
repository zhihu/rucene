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

use std::any::Any;
use std::fmt;

use core::codec::Codec;
use core::index::reader::LeafReaderContext;
use core::search::explanation::Explanation;
use core::search::query::{Query, TermQuery, Weight};
use core::search::scorer::BoostingScorer;
use core::search::scorer::Scorer;
use core::search::searcher::SearchPlanBuilder;
use core::util::DocId;
use error::Result;

const BOOSTING_QUERY: &str = "boosting";

pub struct BoostingQuery<C: Codec> {
    positive: Box<dyn Query<C>>,
    negative: Box<dyn Query<C>>,
    negative_boost: f32,
}

impl<C: Codec> BoostingQuery<C> {
    pub fn build(
        positive: Box<dyn Query<C>>,
        negative: Box<dyn Query<C>>,
        negative_boost: f32,
    ) -> Box<dyn Query<C>> {
        Box::new(BoostingQuery {
            positive,
            negative,
            negative_boost,
        })
    }
}

impl<C: Codec> Query<C> for BoostingQuery<C> {
    fn create_weight(
        &self,
        searcher: &dyn SearchPlanBuilder<C>,
        needs_scores: bool,
    ) -> Result<Box<dyn Weight<C>>> {
        Ok(Box::new(BoostingWeight::new(
            self.positive.create_weight(searcher, needs_scores)?,
            self.negative.create_weight(searcher, false)?,
            self.negative_boost,
        )))
    }

    fn extract_terms(&self) -> Vec<TermQuery> {
        self.positive.extract_terms()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl<C: Codec> fmt::Display for BoostingQuery<C> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "BoostingQuery(positive: {}, negative: {}, negative_boost: {})",
            &self.positive, &self.negative, self.negative_boost
        )
    }
}

struct BoostingWeight<C: Codec> {
    positive_weight: Box<dyn Weight<C>>,
    negative_weight: Box<dyn Weight<C>>,
    negative_boost: f32,
}

impl<C: Codec> BoostingWeight<C> {
    pub fn new(
        positive_weight: Box<dyn Weight<C>>,
        negative_weight: Box<dyn Weight<C>>,
        negative_boost: f32,
    ) -> BoostingWeight<C> {
        BoostingWeight {
            positive_weight,
            negative_weight,
            negative_boost,
        }
    }
}

impl<C: Codec> Weight<C> for BoostingWeight<C> {
    fn create_scorer(
        &self,
        leaf_reader: &LeafReaderContext<'_, C>,
    ) -> Result<Option<Box<dyn Scorer>>> {
        if let (Some(positive_scorer), Some(negative_scorer)) = (
            self.positive_weight.create_scorer(leaf_reader)?,
            self.negative_weight.create_scorer(leaf_reader)?,
        ) {
            Ok(Some(Box::new(BoostingScorer::new(
                positive_scorer,
                negative_scorer,
                self.negative_boost,
            ))))
        } else {
            Ok(None)
        }
    }

    fn query_type(&self) -> &'static str {
        BOOSTING_QUERY
    }

    fn actual_query_type(&self) -> &'static str {
        BOOSTING_QUERY
    }

    fn normalize(&mut self, norm: f32, boost: f32) {
        self.positive_weight.normalize(norm, boost)
    }

    fn value_for_normalization(&self) -> f32 {
        self.positive_weight.value_for_normalization()
    }

    fn needs_scores(&self) -> bool {
        self.positive_weight.needs_scores()
    }

    fn explain(&self, reader: &LeafReaderContext<'_, C>, doc: DocId) -> Result<Explanation> {
        self.positive_weight.explain(reader, doc)
    }
}

impl<C: Codec> fmt::Display for BoostingWeight<C> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "BoostingWeight(positive: {}, negative: {}, negative_boost: {})",
            &self.positive_weight, &self.negative_weight, self.negative_boost
        )
    }
}
