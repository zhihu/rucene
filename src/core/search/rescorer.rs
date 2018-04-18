use error::*;

use core::search::searcher::IndexSearcher;
use core::search::sort_field::SortFieldType;
use core::search::top_docs::ScoreDocHit;
use core::search::top_docs::TopDocs;
use core::search::RescoreRequest;
use core::search::Rescorer;
use core::util::VariantValue;
use std::collections::HashMap;

#[derive(Default)]
pub struct QueryRescorer;

impl QueryRescorer {
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

        let mut hit_upto = 0usize;
        // let mut reader_upto = -1;
        let mut end_doc = 0;
        let mut doc_base = 0;
        let readers = searcher.reader.leaves();
        let mut reader_idx: i32 = -1;
        let mut current_reader_idx = -1;
        let mut scorer = None;

        let mut score_field_index = -1;
        match *top_docs {
            TopDocs::Field(ref f) => for (index, field) in f.fields.iter().enumerate() {
                if *field.field_type() == SortFieldType::Score {
                    score_field_index = index as i32;
                }
            },
            TopDocs::Collapse(ref c) => for (index, field) in c.fields.iter().enumerate() {
                if *field.field_type() == SortFieldType::Score {
                    score_field_index = index as i32;
                }
            },
            _ => {}
        }

        let weight = req.query.create_weight(searcher, true)?;
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
}
