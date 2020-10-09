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

use core::codec::doc_values::DocValuesIterator;
use core::codec::Codec;
use core::index::reader::LeafReaderContext;
use core::search::explanation::Explanation;
use core::search::query::{Query, TermQuery, Weight};
use core::search::scorer::ConstantScoreScorer;
use core::search::scorer::Scorer;
use core::search::searcher::SearchPlanBuilder;
use core::util::DocId;
use error::Result;

const EXISTS_QUERY: &str = "exists";

pub struct ExistsQuery {
    field: String,
}

impl ExistsQuery {
    pub fn build(field: String) -> ExistsQuery {
        ExistsQuery { field }
    }
}

impl<C: Codec> Query<C> for ExistsQuery {
    fn create_weight(
        &self,
        _searcher: &dyn SearchPlanBuilder<C>,
        _needs_scores: bool,
    ) -> Result<Box<dyn Weight<C>>> {
        Ok(Box::new(ExistsWeight::new(self.field.clone())))
    }

    fn extract_terms(&self) -> Vec<TermQuery> {
        vec![]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl fmt::Display for ExistsQuery {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ExistsQuery(field={})", &self.field)
    }
}

struct ExistsWeight {
    field: String,
    weight: f32,
    norm: f32,
}

impl ExistsWeight {
    pub fn new(field: String) -> ExistsWeight {
        ExistsWeight {
            field,
            weight: 0f32,
            norm: 0f32,
        }
    }
}

impl<C: Codec> Weight<C> for ExistsWeight {
    fn create_scorer(
        &self,
        leaf_reader: &LeafReaderContext<'_, C>,
    ) -> Result<Option<Box<dyn Scorer>>> {
        if let Some(field_info) = leaf_reader.reader.field_info(self.field.as_str()) {
            let cost: i32 = leaf_reader.reader.max_doc();
            let doc_iterator = DocValuesIterator::new(field_info.name.as_str(), cost, leaf_reader);

            return Ok(Some(Box::new(ConstantScoreScorer::new(
                self.weight,
                doc_iterator,
                cost as usize,
            ))));
        }

        Ok(None)
    }

    fn query_type(&self) -> &'static str {
        EXISTS_QUERY
    }

    fn actual_query_type(&self) -> &'static str {
        EXISTS_QUERY
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
            vec![Explanation::new(
                true,
                self.weight,
                "exists".to_string(),
                vec![],
            )],
        ))
    }
}

impl fmt::Display for ExistsWeight {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ExistsWeight(field={}, weight={}, norm={})",
            &self.field, self.weight, self.norm
        )
    }
}
