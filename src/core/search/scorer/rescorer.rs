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

use std::collections::HashMap;
use std::fmt;

use core::codec::Codec;
use core::index::reader::{IndexReader, LeafReaderContext};
use core::search::explanation::Explanation;
use core::search::query::Query;
use core::search::query::Weight;
use core::search::scorer::FeatureResult;
use core::search::searcher::IndexSearcher;
use core::search::sort_field::ScoreDocHit;
use core::search::sort_field::SortFieldType;
use core::search::sort_field::TopDocs;
use core::util::DocId;
use core::util::{IndexedContext, VariantValue};

pub trait BatchScorer {
    fn scores(&self, _score_context: Vec<&IndexedContext>) -> Result<Vec<f32>> {
        unimplemented!()
    }
}

/// A query rescorer interface used to re-rank the Top-K results of a previously
/// executed search.
pub trait Rescorer {
    /// Modifies the result of the previously executed search `TopDocs`
    /// in place based on the given `RescorerContext`
    fn rescore<C: Codec, IS: IndexSearcher<C>>(
        &self,
        searcher: &IS,
        rescore_ctx: &RescoreRequest<C>,
        top_docs: &mut TopDocs,
    ) -> Result<()>;

    fn rescore_features<C: Codec, IS: IndexSearcher<C>>(
        &self,
        searcher: &IS,
        rescore_ctx: &RescoreRequest<C>,
        top_docs: &mut TopDocs,
    ) -> Result<Vec<HashMap<String, VariantValue>>>;

    /// Explains how the score for the specified document was computed.
    fn explain<C: Codec, IS: IndexSearcher<C>>(
        &self,
        searcher: &IS,
        req: &RescoreRequest<C>,
        first: Explanation,
        doc: DocId,
    ) -> Result<Explanation>;
}

pub struct RescoreRequest<C: Codec> {
    query: Box<dyn Query<C>>,
    query_weight: f32,
    rescore_weight: f32,
    rescore_mode: RescoreMode,
    pub window_size: usize,
    pub rescore_movedout: bool,
}

impl<C: Codec> RescoreRequest<C> {
    pub fn new(
        query: Box<dyn Query<C>>,
        query_weight: f32,
        rescore_weight: f32,
        rescore_mode: RescoreMode,
        window_size: usize,
        rescore_movedout: bool,
    ) -> RescoreRequest<C> {
        RescoreRequest {
            query,
            query_weight,
            rescore_weight,
            rescore_mode,
            window_size,
            rescore_movedout,
        }
    }
}

#[derive(Debug, Clone)]
pub enum RescoreMode {
    Avg,
    Max,
    Min,
    Total,
    Multiply,
}

impl RescoreMode {
    pub fn combine(&self, primary: f32, secondary: f32) -> f32 {
        match *self {
            RescoreMode::Avg => (primary + secondary) / 2.0f32,
            RescoreMode::Max => primary.max(secondary),
            RescoreMode::Min => primary.min(secondary),
            RescoreMode::Total => primary + secondary,
            RescoreMode::Multiply => primary * secondary,
        }
    }
}

impl fmt::Display for RescoreMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            RescoreMode::Avg => write!(f, "avg"),
            RescoreMode::Max => write!(f, "max"),
            RescoreMode::Min => write!(f, "min"),
            RescoreMode::Total => write!(f, "sum"),
            RescoreMode::Multiply => write!(f, "product"),
        }
    }
}

#[derive(Default)]
pub struct QueryRescorer;

impl QueryRescorer {
    fn batch_rescore<C: Codec>(
        &self,
        readers: &[LeafReaderContext<'_, C>],
        req: &RescoreRequest<C>,
        hits: &mut [ScoreDocHit],
        weight: &dyn Weight<C>,
        score_field_index: i32,
        batch_scorer: &dyn BatchScorer,
    ) -> Result<()> {
        let mut hit_upto = 0usize;
        let mut end_doc = 0;
        let mut doc_base = 0;
        let mut reader_idx: i32 = -1;
        let mut current_reader_idx = -1;
        let mut scorer = None;
        let mut score_contexts: Vec<Option<IndexedContext>> = Vec::with_capacity(hits.len());
        // first pass, collect score contexts first
        while hit_upto < hits.len() {
            let doc_id = hits[hit_upto].doc_id();
            while doc_id >= end_doc && reader_idx < readers.len() as i32 - 1 {
                // reader_upto += 1;
                reader_idx += 1;
                end_doc = readers[reader_idx as usize].doc_base()
                    + readers[reader_idx as usize].reader.max_doc();
            }

            if reader_idx != current_reader_idx {
                let reader = &readers[reader_idx as usize];
                doc_base = reader.doc_base;
                scorer = weight.create_scorer(reader)?;
                current_reader_idx = reader_idx;
            }

            if let Some(ref mut scorer) = scorer {
                let target_doc = doc_id - doc_base;
                let mut actual_doc = scorer.doc_id();
                if actual_doc < target_doc {
                    actual_doc = scorer.advance(target_doc)?;
                }

                if actual_doc == target_doc {
                    score_contexts.push(Some(scorer.score_context()?));
                } else {
                    // query did not match this doc
                    debug_assert!(actual_doc > target_doc);
                    score_contexts.push(None);
                }
            } else {
                score_contexts.push(None);
            }

            hit_upto += 1;
        }

        let scores = batch_scorer.scores(
            score_contexts
                .iter()
                .filter(|x| x.is_some())
                .map(|x| x.as_ref())
                .map(|x| x.unwrap())
                .collect(),
        )?;

        // second pass, update score
        hit_upto = 0;
        let mut score_upto = 0usize;
        while hit_upto < hits.len() {
            let current_score = hits[hit_upto].score();
            if score_contexts[hit_upto].is_some() {
                hits[hit_upto].set_score(self.combine_score(
                    req,
                    current_score,
                    true,
                    scores[score_upto],
                ));
                score_upto += 1;
            } else {
                hits[hit_upto].set_score(self.combine_score(req, current_score, false, 0.0f32));
            }

            if score_field_index >= 0 {
                match hits[hit_upto] {
                    ScoreDocHit::Field(ref mut f) => {
                        f.fields[score_field_index as usize] = VariantValue::from(f.score);
                    }
                    ScoreDocHit::Score(_) => {
                        unreachable!();
                    }
                }
            }

            hit_upto += 1;
        }
        Ok(())
    }

    fn iterative_rescore<C: Codec>(
        &self,
        readers: &[LeafReaderContext<'_, C>],
        req: &RescoreRequest<C>,
        hits: &mut [ScoreDocHit],
        weight: &dyn Weight<C>,
        score_field_index: i32,
    ) -> Result<()> {
        let mut hit_upto = 0usize;
        let mut end_doc = 0;
        let mut doc_base = 0;
        let mut reader_idx: i32 = -1;
        let mut current_reader_idx = -1;
        let mut scorer = None;

        while hit_upto < hits.len() {
            let doc_id = hits[hit_upto].doc_id();
            let current_score = hits[hit_upto].score();
            while doc_id >= end_doc && reader_idx < readers.len() as i32 - 1 {
                // reader_upto += 1;
                reader_idx += 1;
                end_doc = readers[reader_idx as usize].doc_base()
                    + readers[reader_idx as usize].reader.max_doc();
            }

            if reader_idx != current_reader_idx {
                let reader = &readers[reader_idx as usize];
                doc_base = reader.doc_base();
                scorer = weight.create_scorer(reader)?;
                current_reader_idx = reader_idx;
            }

            if let Some(ref mut scorer) = scorer {
                let target_doc = doc_id - doc_base;
                let mut actual_doc = scorer.doc_id();
                if actual_doc < target_doc {
                    actual_doc = scorer.advance(target_doc)?;
                }

                if actual_doc == target_doc {
                    hits[hit_upto].set_score(self.combine_score(
                        req,
                        current_score,
                        true,
                        scorer.score()?,
                    ));
                } else {
                    // query did not match this doc
                    debug_assert!(actual_doc > target_doc);
                    hits[hit_upto].set_score(self.combine_score(req, current_score, false, 0.0f32));
                }
            } else {
                hits[hit_upto].set_score(self.combine_score(req, current_score, false, 0.0f32));
            }

            if score_field_index >= 0 {
                match hits[hit_upto] {
                    ScoreDocHit::Field(ref mut f) => {
                        f.fields[score_field_index as usize] = VariantValue::from(f.score);
                    }
                    ScoreDocHit::Score(_) => {
                        unreachable!();
                    }
                }
            }

            hit_upto += 1;
        }
        Ok(())
    }

    fn query_rescore<C: Codec, IS: IndexSearcher<C>>(
        &self,
        searcher: &IS,
        req: &RescoreRequest<C>,
        top_docs: &TopDocs,
    ) -> Result<Vec<ScoreDocHit>> {
        let mut hits = top_docs.score_docs().to_vec();
        if hits.len() > req.window_size {
            hits.truncate(req.window_size);
        }
        hits.sort_by(ScoreDocHit::order_by_doc);

        let readers = searcher.reader().leaves();
        let mut score_field_index = -1;
        match *top_docs {
            TopDocs::Field(ref f) => {
                for (index, field) in f.fields.iter().enumerate() {
                    if field.field_type() == SortFieldType::Score {
                        score_field_index = index as i32;
                        break;
                    }
                }
            }
            TopDocs::Collapse(ref c) => {
                for (index, field) in c.fields.iter().enumerate() {
                    if field.field_type() == SortFieldType::Score {
                        score_field_index = index as i32;
                        break;
                    }
                }
            }
            _ => {}
        }

        let weight = req.query.create_weight(searcher, true)?;

        if let Some(batch_scorer) = weight.create_batch_scorer() {
            self.batch_rescore(
                &readers,
                req,
                &mut hits,
                weight.as_ref(),
                score_field_index,
                batch_scorer.as_ref(),
            )?;
        } else {
            self.iterative_rescore(&readers, req, &mut hits, weight.as_ref(), score_field_index)?;
        }

        // TODO: we should do a partial sort (of only topN)
        // instead, but typically the number of hits is
        // smallish:
        hits.sort();
        Ok(hits)
    }

    fn combine_score<C: Codec>(
        &self,
        ctx: &RescoreRequest<C>,
        last_score: f32,
        is_match: bool,
        new_score: f32,
    ) -> f32 {
        if is_match {
            ctx.rescore_mode.combine(
                last_score * ctx.query_weight,
                new_score * ctx.rescore_weight,
            )
        } else {
            // TODO: shouldn't this be up to the ScoreMode?  I.e., we should just invoke
            // ScoreMode.combine, passing 0.0f for the secondary score?
            last_score * ctx.query_weight
        }
    }

    fn combine_docs<C: Codec>(
        &self,
        docs: &mut TopDocs,
        resorted: Vec<ScoreDocHit>,
        ctx: &RescoreRequest<C>,
    ) {
        let rescore_len = resorted.len();
        let mut resorted = resorted;
        // used for collapsing top docs
        let mut doc_idx_map = HashMap::new();
        {
            let hits = docs.score_docs_mut();

            for (i, hit) in hits.iter().enumerate().take(rescore_len) {
                doc_idx_map.insert(hit.doc_id(), i);
            }

            for i in 0..rescore_len {
                hits[rescore_len - 1 - i] = resorted.pop().unwrap();
            }
            if hits.len() > rescore_len {
                for hit in hits.iter_mut().skip(rescore_len) {
                    // TODO: shouldn't this be up to the ScoreMode?  I.e., we should just invoke
                    // ScoreMode.combine, passing 0.0f for the secondary score?
                    let score = hit.score();
                    hit.set_score(score * ctx.query_weight);
                }
            }
        }

        // adjust collapse_values for collapse top docs after rescore
        if let TopDocs::Collapse(ref mut c) = docs {
            // TODO maybe we can prevent clone collapse values
            let mut collapse_value = Vec::with_capacity(c.collapse_values.len());
            for i in 0..rescore_len {
                let idx = &doc_idx_map[&c.score_docs[i].doc_id()];
                collapse_value.push(c.collapse_values[*idx].clone());
            }
            let length = c.collapse_values.len();
            collapse_value.extend(c.collapse_values[rescore_len..length].to_owned());
            c.collapse_values = collapse_value;
        }
    }

    fn explain_inner<C: Codec, IS: IndexSearcher<C>>(
        &self,
        searcher: &IS,
        req: &RescoreRequest<C>,
        first: Explanation,
        doc: DocId,
    ) -> Result<Explanation> {
        let rescore = searcher.explain(req.query.as_ref(), doc)?;
        let rescore_value = rescore.value();
        let first_value = first.value();
        let primary_weight = 1.0f32;

        let prim = if first.is_match() {
            Explanation::new(
                true,
                first_value * primary_weight,
                "product of:".to_string(),
                vec![
                    first,
                    Explanation::new(true, primary_weight, "primaryWeight".to_string(), vec![]),
                ],
            )
        } else {
            Explanation::new(
                false,
                0.0f32,
                "First pass did not match".to_string(),
                vec![first],
            )
        };

        // NOTE: we don't use Lucene's Rescorer.explain because we want to insert our own
        // description with which ScoreMode was used.  Maybe we should add
        // QueryRescorer.explainCombine to Lucene?
        if rescore.is_match() {
            let secondary_weight = 1.0f32;
            let sec = Explanation::new(
                true,
                rescore_value * secondary_weight,
                "product of:".to_string(),
                vec![
                    rescore,
                    Explanation::new(
                        true,
                        secondary_weight,
                        "secondaryWeight".to_string(),
                        vec![],
                    ),
                ],
            );

            Ok(Explanation::new(
                true,
                self.combine_score(req, prim.value(), true, sec.value()),
                "sum of:".to_string(),
                vec![prim, sec],
            ))
        } else {
            Ok(prim)
        }
    }

    fn score_features<C: Codec, IS: IndexSearcher<C>>(
        &self,
        searcher: &IS,
        req: &RescoreRequest<C>,
        top_docs: &TopDocs,
    ) -> Result<(Vec<Option<Vec<FeatureResult>>>, Vec<f32>)> {
        let hits = top_docs.score_docs();
        let readers = searcher.reader().leaves();
        let weight = req.query.create_weight(searcher, true)?;

        let mut hit_upto = 0usize;
        let mut end_doc = 0;
        let mut doc_base = 0;
        let mut reader_idx: i32 = -1;
        let mut current_reader_idx = -1;
        let mut scorer = None;
        let mut score_features: Vec<Option<Vec<FeatureResult>>> = Vec::with_capacity(hits.len());
        let mut previous_scores = Vec::with_capacity(hits.len());
        // first pass, collect score contexts first
        while hit_upto < hits.len() {
            let doc_id = hits[hit_upto].doc_id();
            previous_scores.push(hits[hit_upto].score());
            while doc_id >= end_doc && reader_idx < readers.len() as i32 - 1 {
                // reader_upto += 1;
                reader_idx += 1;
                end_doc = readers[reader_idx as usize].doc_base()
                    + readers[reader_idx as usize].reader.max_doc();
            }

            if reader_idx != current_reader_idx {
                let reader = &readers[reader_idx as usize];
                doc_base = reader.doc_base();
                scorer = weight.create_scorer(reader)?;
                current_reader_idx = reader_idx;
            }

            if let Some(ref mut scorer) = scorer {
                let target_doc = doc_id - doc_base;
                let mut actual_doc = scorer.doc_id();
                if actual_doc < target_doc {
                    actual_doc = scorer.advance(target_doc)?;
                }

                if actual_doc == target_doc {
                    score_features.push(Some(scorer.score_feature()?));
                } else {
                    // query did not match this doc
                    debug_assert!(actual_doc > target_doc);
                    score_features.push(None);
                }
            } else {
                score_features.push(None);
            }

            hit_upto += 1;
        }
        Ok((score_features, previous_scores))
    }
}

impl Rescorer for QueryRescorer {
    fn rescore<C: Codec, IS: IndexSearcher<C>>(
        &self,
        searcher: &IS,
        rescore_req: &RescoreRequest<C>,
        top_docs: &mut TopDocs,
    ) -> Result<()> {
        if top_docs.total_hits() == 0 || top_docs.score_docs().is_empty() {
            return Ok(());
        }

        let rescore_hits = self.query_rescore(searcher, rescore_req, top_docs)?;
        self.combine_docs(top_docs, rescore_hits, rescore_req);

        Ok(())
    }

    fn rescore_features<C: Codec, IS: IndexSearcher<C>>(
        &self,
        searcher: &IS,
        rescore_req: &RescoreRequest<C>,
        top_docs: &mut TopDocs,
    ) -> Result<Vec<HashMap<String, VariantValue>>> {
        if top_docs.total_hits() == 0 || top_docs.score_docs().is_empty() {
            return Ok(Vec::new());
        }
        {
            let hits = top_docs.score_docs_mut();
            if hits.len() > rescore_req.window_size {
                hits.truncate(rescore_req.window_size);
            }
            hits.sort_by(ScoreDocHit::order_by_doc);
        }

        let (score_features, previous_scores) =
            self.score_features(searcher, rescore_req, top_docs)?;
        // only support one function (simple_ltr) in rescore request for now
        let mut result_features = Vec::with_capacity(score_features.len());

        for (feature, &score) in score_features.iter().zip(previous_scores.iter()) {
            match feature {
                Some(function_features) => {
                    let mut feature_map = HashMap::new();
                    for f in function_features.iter() {
                        feature_map.extend(f.extra_params.clone());
                    }
                    feature_map.insert("previous_score".to_string(), VariantValue::from(score));
                    result_features.push(feature_map);
                }
                None => {
                    warn!("query did not match this doc");
                }
            }
        }
        Ok(result_features)
    }

    fn explain<C: Codec, IS: IndexSearcher<C>>(
        &self,
        searcher: &IS,
        req: &RescoreRequest<C>,
        first: Explanation,
        doc: DocId,
    ) -> Result<Explanation> {
        self.explain_inner(searcher, req, first, doc)
    }
}
