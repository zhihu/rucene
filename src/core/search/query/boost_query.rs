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
use std::f32;
use std::fmt;

use core::codec::Codec;
use core::index::reader::LeafReaderContext;
use core::search::explanation::Explanation;
use core::search::query::{Query, TermQuery, Weight};
use core::search::scorer::Scorer;
use core::search::searcher::SearchPlanBuilder;
use core::util::DocId;

use error::Result;

const BOOST_QUERY: &str = "boost";

/// A `Query` wrapper that allows to give a boost to the wrapped query.
///
/// Boost values that are less than one will give less importance to this
/// query compared to other ones while values that are greater than one will
/// give more importance to the scores returned by this query.
pub struct BoostQuery<C: Codec> {
    query: Box<dyn Query<C>>,
    boost: f32,
}

impl<C: Codec> BoostQuery<C> {
    pub fn build(query: Box<dyn Query<C>>, boost: f32) -> Box<dyn Query<C>> {
        if (boost - 1.0f32).abs() <= f32::EPSILON {
            query
        } else {
            Box::new(BoostQuery { query, boost })
        }
    }
}

impl<C: Codec> Query<C> for BoostQuery<C> {
    fn create_weight(
        &self,
        searcher: &dyn SearchPlanBuilder<C>,
        needs_scores: bool,
    ) -> Result<Box<dyn Weight<C>>> {
        let mut weight = self.query.create_weight(searcher, needs_scores)?;
        Weight::<C>::normalize(weight.as_mut(), 1.0f32, self.boost);
        // weight.normalize(1.0f32, self.boost);
        Ok(Box::new(BoostWeight::new(weight, self.boost)))
    }

    fn extract_terms(&self) -> Vec<TermQuery> {
        self.query.extract_terms()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl<C: Codec> fmt::Display for BoostQuery<C> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "BoostQuery(query: {}, boost: {})",
            &self.query, self.boost
        )
    }
}

struct BoostWeight<C: Codec> {
    weight: Box<dyn Weight<C>>,
    boost: f32,
}

impl<C: Codec> BoostWeight<C> {
    pub fn new(weight: Box<dyn Weight<C>>, boost: f32) -> BoostWeight<C> {
        assert!((boost - 1.0f32).abs() > f32::EPSILON);

        BoostWeight { weight, boost }
    }
}

impl<C: Codec> Weight<C> for BoostWeight<C> {
    fn create_scorer(
        &self,
        leaf_reader: &LeafReaderContext<'_, C>,
    ) -> Result<Option<Box<dyn Scorer>>> {
        self.weight.create_scorer(leaf_reader)
    }

    fn query_type(&self) -> &'static str {
        BOOST_QUERY
    }

    fn actual_query_type(&self) -> &'static str {
        self.weight.query_type()
    }

    fn normalize(&mut self, norm: f32, boost: f32) {
        self.weight.normalize(norm, boost * self.boost)
    }

    fn value_for_normalization(&self) -> f32 {
        self.weight.value_for_normalization()
    }

    fn needs_scores(&self) -> bool {
        self.weight.needs_scores()
    }

    fn explain(&self, reader: &LeafReaderContext<'_, C>, doc: DocId) -> Result<Explanation> {
        self.weight.explain(reader, doc)
    }
}

impl<C: Codec> fmt::Display for BoostWeight<C> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "BoostWeight(weight: {}, boost: {})",
            &self.weight, self.boost
        )
    }
}
