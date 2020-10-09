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
use core::search::query::{ConstantScoreQuery, MatchAllDocsQuery, Query, TermQuery, Weight};
use core::search::scorer::{
    ConjunctionScorer, DisjunctionSumScorer, ReqNotScorer, ReqOptScorer, Scorer,
};
use core::search::searcher::SearchPlanBuilder;
use core::util::DocId;
use error::{ErrorKind::IllegalArgument, Result};

/// A Query that matches documents matching boolean combinations of other queries.
pub struct BooleanQuery<C: Codec> {
    must_queries: Vec<Box<dyn Query<C>>>,
    should_queries: Vec<Box<dyn Query<C>>>,
    filter_queries: Vec<Box<dyn Query<C>>>,
    must_not_queries: Vec<Box<dyn Query<C>>>,
    min_should_match: i32,
}

pub const BOOLEAN: &str = "boolean";

impl<C: Codec> BooleanQuery<C> {
    pub fn build(
        musts: Vec<Box<dyn Query<C>>>,
        shoulds: Vec<Box<dyn Query<C>>>,
        filters: Vec<Box<dyn Query<C>>>,
        must_nots: Vec<Box<dyn Query<C>>>,
        min_should_match: i32,
    ) -> Result<Box<dyn Query<C>>> {
        let min_should_match = if min_should_match > 0 {
            min_should_match
        } else {
            if musts.is_empty() {
                1
            } else {
                0
            }
        };

        let mut musts = musts;
        let mut shoulds = shoulds;
        let mut filters = filters;
        let must_nots = must_nots;
        if musts.len() + shoulds.len() + filters.len() + must_nots.len() == 0 {
            bail!(IllegalArgument(
                "boolean query should at least contain one inner query!".into()
            ));
        }
        if must_nots.len() == 0 && musts.len() + shoulds.len() + filters.len() == 1 {
            let query = if musts.len() == 1 {
                musts.remove(0)
            } else if shoulds.len() == 1 {
                shoulds.remove(0)
            } else {
                Box::new(ConstantScoreQuery::with_boost(filters.remove(0), 0f32))
            };
            return Ok(query);
        }
        if musts.len() + shoulds.len() + filters.len() == 0 {
            // only must_not exists
            musts.push(Box::new(MatchAllDocsQuery {}));
        }
        Ok(Box::new(BooleanQuery {
            must_queries: musts,
            should_queries: shoulds,
            filter_queries: filters,
            must_not_queries: must_nots,
            min_should_match,
        }))
    }

    fn queries_to_str(&self, queries: &[Box<dyn Query<C>>]) -> String {
        let query_strs: Vec<String> = queries.iter().map(|q| format!("{}", q)).collect();
        query_strs.join(", ")
    }
}

impl<C: Codec> Query<C> for BooleanQuery<C> {
    fn create_weight(
        &self,
        searcher: &dyn SearchPlanBuilder<C>,
        needs_scores: bool,
    ) -> Result<Box<dyn Weight<C>>> {
        let mut must_weights =
            Vec::with_capacity(self.must_queries.len() + self.filter_queries.len());
        for q in &self.must_queries {
            must_weights.push(searcher.create_weight(q.as_ref(), needs_scores)?);
        }
        for q in &self.filter_queries {
            must_weights.push(searcher.create_weight(q.as_ref(), false)?);
        }
        let mut should_weights = Vec::with_capacity(self.should_queries.len());
        for q in &self.should_queries {
            should_weights.push(searcher.create_weight(q.as_ref(), needs_scores)?);
        }
        let mut must_not_weights = Vec::with_capacity(self.must_not_queries.len());
        for q in &self.must_not_queries {
            must_not_weights.push(searcher.create_weight(q.as_ref(), false)?);
        }

        Ok(Box::new(BooleanWeight::new(
            must_weights,
            should_weights,
            must_not_weights,
            needs_scores,
            self.min_should_match,
        )))
    }

    fn extract_terms(&self) -> Vec<TermQuery> {
        let mut term_query_list: Vec<TermQuery> = vec![];

        for query in &self.must_queries {
            for term_query in query.extract_terms() {
                term_query_list.push(term_query);
            }
        }

        for query in &self.should_queries {
            for term_query in query.extract_terms() {
                term_query_list.push(term_query);
            }
        }

        term_query_list
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl<C: Codec> fmt::Display for BooleanQuery<C> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let must_str = self.queries_to_str(&self.must_queries);
        let should_str = self.queries_to_str(&self.should_queries);
        let filters_str = self.queries_to_str(&self.filter_queries);
        let must_not_str = self.queries_to_str(&self.must_not_queries);
        write!(
            f,
            "BooleanQuery(must: [{}], should: [{}], filters: [{}], must_not: [{}], match: {})",
            must_str, should_str, filters_str, must_not_str, self.min_should_match
        )
    }
}

struct BooleanWeight<C: Codec> {
    must_weights: Vec<Box<dyn Weight<C>>>,
    should_weights: Vec<Box<dyn Weight<C>>>,
    must_not_weights: Vec<Box<dyn Weight<C>>>,
    min_should_match: i32,
    needs_scores: bool,
}

impl<C: Codec> BooleanWeight<C> {
    pub fn new(
        musts: Vec<Box<dyn Weight<C>>>,
        shoulds: Vec<Box<dyn Weight<C>>>,
        must_nots: Vec<Box<dyn Weight<C>>>,
        needs_scores: bool,
        min_should_match: i32,
    ) -> BooleanWeight<C> {
        BooleanWeight {
            must_weights: musts,
            should_weights: shoulds,
            must_not_weights: must_nots,
            min_should_match,
            needs_scores,
        }
    }

    fn weights_to_str(&self, weights: &[Box<dyn Weight<C>>]) -> String {
        let weight_strs: Vec<String> = weights.iter().map(|q| format!("{}", q)).collect();
        weight_strs.join(", ")
    }
}

impl<C: Codec> Weight<C> for BooleanWeight<C> {
    fn create_scorer(
        &self,
        leaf_reader: &LeafReaderContext<'_, C>,
    ) -> Result<Option<Box<dyn Scorer>>> {
        let must_scorer: Option<Box<dyn Scorer>> = if !self.must_weights.is_empty() {
            let mut scorers = vec![];
            for weight in &self.must_weights {
                if let Some(scorer) = weight.create_scorer(leaf_reader)? {
                    scorers.push(scorer);
                } else {
                    return Ok(None);
                }
            }
            if scorers.len() > 1 {
                Some(Box::new(ConjunctionScorer::new(scorers)))
            } else {
                Some(scorers.remove(0))
            }
        } else {
            None
        };
        let should_scorer: Option<Box<dyn Scorer>> = {
            let mut scorers = vec![];
            for weight in &self.should_weights {
                if let Some(scorer) = weight.create_scorer(leaf_reader)? {
                    scorers.push(scorer);
                }
            }
            match scorers.len() {
                0 => None,
                1 => Some(scorers.remove(0)),
                _ => Some(Box::new(DisjunctionSumScorer::new(
                    scorers,
                    self.needs_scores,
                    self.min_should_match,
                ))),
            }
        };
        let must_not_scorer: Option<Box<dyn Scorer>> = {
            let mut scorers = vec![];
            for weight in &self.must_not_weights {
                if let Some(scorer) = weight.create_scorer(leaf_reader)? {
                    scorers.push(scorer);
                }
            }
            match scorers.len() {
                0 => None,
                1 => Some(scorers.remove(0)),
                _ => Some(Box::new(DisjunctionSumScorer::new(
                    scorers,
                    false,
                    self.min_should_match,
                ))),
            }
        };

        if let Some(must) = must_scorer {
            if let Some(should) = should_scorer {
                if let Some(must_not) = must_not_scorer {
                    Ok(Some(Box::new(ReqNotScorer::new(
                        Box::new(ReqOptScorer::new(must, should)),
                        must_not,
                    ))))
                } else {
                    Ok(Some(Box::new(ReqOptScorer::new(must, should))))
                }
            } else {
                if let Some(must_not) = must_not_scorer {
                    Ok(Some(Box::new(ReqNotScorer::new(must, must_not))))
                } else {
                    Ok(Some(must))
                }
            }
        } else if let Some(should) = should_scorer {
            if let Some(must_not) = must_not_scorer {
                Ok(Some(Box::new(ReqNotScorer::new(should, must_not))))
            } else {
                Ok(Some(should))
            }
        } else {
            Ok(None)
        }
    }

    fn query_type(&self) -> &'static str {
        BOOLEAN
    }

    fn normalize(&mut self, norm: f32, boost: f32) {
        for must in &mut self.must_weights {
            must.normalize(norm, boost);
        }
        for should in &mut self.should_weights {
            should.normalize(norm, boost);
        }
        for must_not in &mut self.must_not_weights {
            must_not.normalize(norm, boost);
        }
    }

    fn value_for_normalization(&self) -> f32 {
        let mut sum = 0f32;
        for weight in &self.must_weights {
            if weight.needs_scores() {
                sum += weight.value_for_normalization();
            }
        }
        sum
    }

    fn needs_scores(&self) -> bool {
        self.needs_scores
    }

    fn explain(&self, reader: &LeafReaderContext<'_, C>, doc: DocId) -> Result<Explanation> {
        let mut coord = 0;
        let mut max_coord = 0;
        let mut sum = 0.0f32;
        let mut fail = false;
        let mut match_count = 0;
        let mut should_match_count = 0;

        let mut subs: Vec<Explanation> = vec![];
        for w in &self.must_weights {
            let e = w.explain(reader, doc)?;
            max_coord += 1;

            if e.is_match() {
                sum += e.value();
                coord += 1;
                match_count += 1;
                subs.push(e);
            } else {
                fail = true;
                subs.push(Explanation::new(
                    false,
                    0.0f32,
                    format!("no match on required clause ({})", w),
                    vec![e],
                ));
            }
        }

        for w in &self.should_weights {
            let e = w.explain(reader, doc)?;
            max_coord += 1;

            if e.is_match() {
                sum += e.value();
                coord += 1;
                match_count += 1;
                should_match_count += 1;
                subs.push(e);
            }
        }

        if fail {
            Ok(Explanation::new(
                false,
                0.0f32,
                "Failure to meet condition(s) of required/prohibited clause(s)".to_string(),
                subs,
            ))
        } else if match_count == 0 {
            Ok(Explanation::new(
                false,
                0.0f32,
                "No matching clauses".to_string(),
                subs,
            ))
        } else if should_match_count < self.min_should_match {
            Ok(Explanation::new(
                false,
                0.0f32,
                format!(
                    "Failure to match minimum number of optional clauses: {}<{}",
                    should_match_count, self.min_should_match
                ),
                subs,
            ))
        } else {
            // we have a match
            let result = Explanation::new(true, sum, "sum of:".to_string(), subs);

            let coord_factor = 1.0f32;
            if (coord_factor - 1.0).abs() < ::std::f32::EPSILON {
                Ok(Explanation::new(
                    true,
                    sum * coord_factor,
                    "product of:".to_string(),
                    vec![
                        result,
                        Explanation::new(
                            true,
                            coord_factor,
                            format!("coord({}/{})", coord, max_coord + 1),
                            vec![],
                        ),
                    ],
                ))
            } else {
                Ok(result)
            }
        }
    }
}

impl<C: Codec> fmt::Display for BooleanWeight<C> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let must_str = self.weights_to_str(&self.must_weights);
        let should_str = self.weights_to_str(&self.should_weights);
        let must_not_str = self.weights_to_str(&self.must_not_weights);
        write!(
            f,
            "BooleanWeight(must: [{}], should: [{}], must_not: [{}], min match: {}, needs score: \
             {})",
            must_str, should_str, must_not_str, self.min_should_match, self.needs_scores
        )
    }
}
