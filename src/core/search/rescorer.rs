use error::*;

use core::index::LeafReader;
use core::search::explanation::Explanation;
use core::search::searcher::IndexSearcher;
use core::search::sort_field::SortFieldType;
use core::search::top_docs::ScoreDocHit;
use core::search::top_docs::TopDocs;
use core::search::{BatchScorer, RescoreRequest, Rescorer, Weight};
use core::util::DocId;
use core::util::{IndexedContext, VariantValue};
use std::collections::HashMap;

#[derive(Default)]
pub struct QueryRescorer;

impl QueryRescorer {
    fn batch_rescore(
        &self,
        readers: &[&LeafReader],
        req: &RescoreRequest,
        hits: &mut [ScoreDocHit],
        weight: &Weight,
        score_field_index: i32,
        batch_scorer: &BatchScorer,
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
                    + readers[reader_idx as usize].max_doc();
            }

            if reader_idx != current_reader_idx {
                let reader = readers[reader_idx as usize];
                doc_base = reader.doc_base();
                let current_scorer = weight.create_scorer(reader)?;
                scorer = Some(current_scorer);
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

    fn iterative_rescore(
        &self,
        readers: &[&LeafReader],
        req: &RescoreRequest,
        hits: &mut [ScoreDocHit],
        weight: &Weight,
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
                    + readers[reader_idx as usize].max_doc();
            }

            if reader_idx != current_reader_idx {
                let reader = readers[reader_idx as usize];
                doc_base = reader.doc_base();
                let current_scorer = weight.create_scorer(reader)?;
                scorer = Some(current_scorer);
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

    fn query_rescore(
        &self,
        searcher: &IndexSearcher,
        req: &RescoreRequest,
        top_docs: &TopDocs,
    ) -> Result<Vec<ScoreDocHit>> {
        let mut hits = top_docs.score_docs().clone();
        if hits.len() > req.window_size {
            hits.truncate(req.window_size);
        }
        hits.sort_by(ScoreDocHit::order_by_doc);

        let readers = searcher.reader.leaves();
        let mut score_field_index = -1;
        match *top_docs {
            TopDocs::Field(ref f) => for (index, field) in f.fields.iter().enumerate() {
                if *field.field_type() == SortFieldType::Score {
                    score_field_index = index as i32;
                    break;
                }
            },
            TopDocs::Collapse(ref c) => for (index, field) in c.fields.iter().enumerate() {
                if *field.field_type() == SortFieldType::Score {
                    score_field_index = index as i32;
                    break;
                }
            },
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

    fn combine_score(
        &self,
        ctx: &RescoreRequest,
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

    fn combine_docs(&self, docs: &mut TopDocs, resorted: Vec<ScoreDocHit>, ctx: &RescoreRequest) {
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

    //    fn explain_lucene(
    //        &self,
    //        searcher: &IndexSearcher,
    //        req: &RescoreRequest,
    //        first: Explanation,
    //        doc: DocId,
    //    ) -> Result<Explanation> {
    //        let second = searcher.explain(req.query.as_ref(), doc)?;
    //        let second_value = second.value();
    //        let first_value = first.value();
    //
    //        let score;
    //        let second_expl = if second.is_match() {
    //            score = self.combine_score(req, first_value, true, second_value);
    //            Explanation::new(
    //                true,
    //                second_value,
    //                "second pass score".to_string(),
    //                vec![second],
    //            )
    //        } else {
    //            score = self.combine_score(req, first_value, false, 0.0f32);
    //            Explanation::new(false, 0.0f32, "no second pass score".to_string(), vec![])
    //        };
    //
    //        let first_expl = Explanation::new(
    //            true,
    //            first_value,
    //            "first pass score".to_string(),
    //            vec![first],
    //        );
    //
    //        Ok(Explanation::new(
    //            true,
    //            score,
    //            "combined first and second pass score using Rescorer".to_string(),
    //            vec![first_expl, second_expl],
    //        ))
    //    }

    fn explain_es(
        &self,
        searcher: &IndexSearcher,
        req: &RescoreRequest,
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
}

impl Rescorer for QueryRescorer {
    fn rescore(
        &self,
        searcher: &IndexSearcher,
        rescore_req: &RescoreRequest,
        top_docs: &mut TopDocs,
    ) -> Result<()> {
        if top_docs.total_hits() == 0 || top_docs.score_docs().is_empty() {
            return Ok(());
        }

        let rescore_hits = self.query_rescore(searcher, rescore_req, top_docs)?;

        self.combine_docs(top_docs, rescore_hits, rescore_req);

        Ok(())
    }

    fn explain(
        &self,
        searcher: &IndexSearcher,
        req: &RescoreRequest,
        first: Explanation,
        doc: DocId,
    ) -> Result<Explanation> {
        self.explain_es(searcher, req, first, doc)
    }
}
