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

//! BM25 Similarity. Introduced in Stephen E. Robertson, Steve Walker,
//! Susan Jones, Micheline Hancock-Beaulieu, and Mike Gatford. Okapi at TREC-3.
//! In Proceedings of the Third *T*ext *RE*trieval *C*onference (TREC 1994).
//! Gaithersburg, USA, November 1994.

use error::Result;
use std::fmt;
use std::sync::Arc;

use core::codec::doc_values::NumericDocValues;
use core::codec::field_infos::FieldInvertState;
use core::codec::Codec;
use core::index::reader::SearchLeafReader;
use core::search::explanation::Explanation;
use core::search::similarity::{SimScorer, SimWeight, Similarity};
use core::search::statistics::{CollectionStatistics, TermStatistics};
use core::util::SmallFloat;
use core::util::{DocId, KeyedContext};

lazy_static! {
    static ref NORM_TABLE: [f32; 256] = {
        let mut norm_table: [f32; 256] = [0f32; 256];
        for (i, norm) in norm_table.iter_mut().enumerate().skip(1) {
            let f = SmallFloat::byte315_to_float(i as u8);
            *norm = 1f32 / (f * f);
        }
        norm_table[0] = 1f32 / norm_table[255];
        norm_table
    };
}

pub const DEFAULT_BM25_K1: f32 = 1.2;
pub const DEFAULT_BM25_B: f32 = 0.75;

/// BM25 Similarity.
pub struct BM25Similarity {
    k1: f32,
    b: f32,
}

impl Default for BM25Similarity {
    fn default() -> Self {
        BM25Similarity::new(DEFAULT_BM25_K1, DEFAULT_BM25_B)
    }
}

impl BM25Similarity {
    pub fn new(k1: f32, b: f32) -> BM25Similarity {
        BM25Similarity { k1, b }
    }

    fn sloppy_freq(distance: i32) -> f32 {
        1.0 / (distance as f32 + 1.0)
    }

    /// The default implementation computes the average as sumTotalTermFreq / docCount,
    /// or returns 1 if the index does not store sumTotalTermFreq:
    /// any field that omits frequency information).
    fn avg_field_length(collection_stats: &CollectionStatistics) -> f32 {
        let sum_total_term_freq = collection_stats.sum_total_term_freq;
        if sum_total_term_freq <= 0 {
            1f32
        } else {
            let mut doc_count = collection_stats.doc_count;
            if doc_count == -1 {
                doc_count = collection_stats.max_doc;
            };
            (sum_total_term_freq as f64 / doc_count as f64) as f32
        }
    }

    pub fn compute_norm(state: &FieldInvertState) -> i64 {
        let num_terms = state.length - state.num_overlap;
        BM25Similarity::encode_norm_value(state.boost, num_terms) as i64
    }

    pub fn encode_norm_value(boost: f32, field_length: i32) -> u8 {
        SmallFloat::float_to_byte315(boost / (field_length as f32).sqrt())
    }

    #[inline]
    fn decode_norm_value(b: usize) -> f32 {
        NORM_TABLE[b]
    }

    fn idf(term_stats: &[TermStatistics], collection_stats: &CollectionStatistics) -> f32 {
        let mut idf = 0.0f32;
        let doc_count = if collection_stats.doc_count == -1 {
            collection_stats.max_doc
        } else {
            collection_stats.doc_count
        };

        for term_stat in term_stats {
            let doc_freq = term_stat.doc_freq;
            idf += (1.0 + (doc_count as f64 - doc_freq as f64 + 0.5) / (doc_freq as f64 + 0.5)).ln()
                as f32;
        }

        idf
    }

    fn idf_explain(
        &self,
        collection_stats: &CollectionStatistics,
        term_stats: &[TermStatistics],
    ) -> Explanation {
        let mut idf_total = 0f32;
        let mut details: Vec<Explanation> = vec![];
        for stat in term_stats {
            let doc_freq = stat.doc_freq;
            let doc_count = if collection_stats.doc_count == -1 {
                collection_stats.max_doc
            } else {
                collection_stats.doc_count
            };

            let idf = (1.0 + (doc_count as f64 - doc_freq as f64 + 0.5) / (doc_freq as f64 + 0.5))
                .ln() as f32;
            idf_total += idf;
            details.push(Explanation::new(
                true,
                idf,
                "idf, computed as log(1 + (docCount - docFreq + 0.5) / (docFreq + 0.5)) from:"
                    .to_string(),
                vec![
                    Explanation::new(true, doc_freq as f32, "docFreq".to_string(), vec![]),
                    Explanation::new(true, doc_count as f32, "docCount".to_string(), vec![]),
                ],
            ))
        }

        Explanation::new(true, idf_total, "idf() sum of:".to_string(), details)
    }
}

impl<C: Codec> Similarity<C> for BM25Similarity {
    fn compute_weight(
        &self,
        collection_stats: &CollectionStatistics,
        term_stats: &[TermStatistics],
        _context: Option<&KeyedContext>,
        boost: f32,
    ) -> Box<dyn SimWeight<C>> {
        let avgdl = BM25Similarity::avg_field_length(&collection_stats);
        let idf = BM25Similarity::idf(&term_stats, &collection_stats);
        let field = collection_stats.field.clone();
        let mut cache: [f32; 256] = [0f32; 256];
        for (i, c) in cache.iter_mut().enumerate() {
            *c = self.k1
                * ((1.0 - self.b) + self.b * (BM25Similarity::decode_norm_value(i) / avgdl));
        }

        Box::new(BM25SimWeight::new(
            self.k1,
            self.b,
            idf,
            field,
            cache,
            self.idf_explain(collection_stats, term_stats),
            BM25Similarity::avg_field_length(collection_stats),
            boost,
        ))
    }
}

impl fmt::Display for BM25Similarity {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "BM25Similarity(k1: {}, b: {})", self.k1, self.b)
    }
}

struct BM25SimScorer {
    k1: f32,
    weight: f32,
    cache: Arc<[f32; 256]>,
    norms: Option<Box<dyn NumericDocValues>>,
}

impl BM25SimScorer {
    fn new(weight: &BM25SimWeight, norms: Option<Box<dyn NumericDocValues>>) -> BM25SimScorer {
        BM25SimScorer {
            k1: weight.k1,
            weight: weight.weight,
            cache: Arc::clone(&weight.cache),
            norms,
        }
    }

    pub fn compute_score(&mut self, doc: i32, freq: f32) -> Result<f32> {
        let norm = if let Some(ref mut norms) = self.norms {
            let encode_length = (norms.get(doc)? & 0xFF) as usize;
            self.cache[encode_length]
        } else {
            self.k1
        };

        Ok(self.weight * (self.k1 + 1.0) * freq / (freq + norm))
    }
}

impl SimScorer for BM25SimScorer {
    fn score(&mut self, doc: DocId, freq: f32) -> Result<f32> {
        self.compute_score(doc, freq)
    }

    fn compute_slop_factor(&self, distance: i32) -> f32 {
        BM25Similarity::sloppy_freq(distance)
    }
}

struct BM25SimWeight {
    k1: f32,
    #[allow(dead_code)]
    b: f32,
    idf: f32,
    field: String,
    cache: Arc<[f32; 256]>,
    boost: f32,
    weight: f32,
    idf_explanation: Explanation,
    avg_dl: f32,
}

impl BM25SimWeight {
    #[allow(clippy::too_many_arguments)]
    fn new(
        k1: f32,
        b: f32,
        idf: f32,
        field: String,
        cache: [f32; 256],
        idf_explanation: Explanation,
        avg_dl: f32,
        boost: f32,
    ) -> BM25SimWeight {
        let mut weight = BM25SimWeight {
            k1,
            b,
            idf,
            field,
            cache: Arc::new(cache),
            boost: 1.0,
            weight: 0.0,
            idf_explanation,
            avg_dl,
        };
        weight.do_normalize(boost);
        weight
    }

    fn explain_tf_norm(
        &self,
        doc: DocId,
        freq: Explanation,
        norms: Option<Box<dyn NumericDocValues>>,
    ) -> Result<Explanation> {
        let mut subs: Vec<Explanation> = vec![];

        let freq_value = freq.value();
        subs.push(freq);
        subs.push(Explanation::new(
            true,
            self.k1,
            "parameter k1".to_string(),
            vec![],
        ));

        match norms {
            Some(n) => {
                let doc_len = NORM_TABLE[n.get(doc)? as usize];
                subs.push(Explanation::new(
                    true,
                    self.b,
                    "parameter b".to_string(),
                    vec![],
                ));
                subs.push(Explanation::new(
                    true,
                    self.avg_dl,
                    "avgFieldLength".to_string(),
                    vec![],
                ));
                subs.push(Explanation::new(
                    true,
                    doc_len,
                    "fieldLength".to_string(),
                    vec![],
                ));

                Ok(Explanation::new(
                    true,
                    (freq_value * (self.k1 + 1.0f32))
                        / (freq_value
                            + self.k1 * (1.0f32 - self.b + self.b * doc_len / self.avg_dl)),
                    "tfNorm, computed as (freq * (k1 + 1)) / (freq + k1 * (1 - b + b * \
                     fieldLength / avgFieldLength)) from:"
                        .to_string(),
                    subs,
                ))
            }
            _ => {
                subs.push(Explanation::new(
                    true,
                    0.0f32,
                    "parameter b (norms omitted for field)".to_string(),
                    vec![],
                ));

                Ok(Explanation::new(
                    true,
                    (freq_value * (self.k1 + 1.0f32)) / (freq_value + self.k1),
                    "tfNorm, computed as (freq * (k1 + 1)) / (freq + k1) from:".to_string(),
                    subs,
                ))
            }
        }
    }

    fn explain_score(
        &self,
        doc: DocId,
        freq: Explanation,
        norms: Option<Box<dyn NumericDocValues>>,
    ) -> Result<Explanation> {
        let mut subs: Vec<Explanation> = vec![];

        let boost_explanation = Explanation::new(true, self.boost, "boost".to_string(), vec![]);
        let boost_value = boost_explanation.value();
        if (boost_value - 1.0).abs() < ::std::f32::EPSILON {
            subs.push(boost_explanation);
        }

        let idf_value = self.idf_explanation.value();
        subs.push(self.idf_explanation.clone());

        let freq_string = freq.to_string(0);
        let tf_explanation = self.explain_tf_norm(doc, freq, norms)?;
        let tf_value = tf_explanation.value();
        subs.push(tf_explanation);

        Ok(Explanation::new(
            true,
            boost_value * idf_value * tf_value,
            format!("score(doc={},freq={}), product of:", doc, freq_string),
            subs,
        ))
    }

    fn do_normalize(&mut self, boost: f32) {
        self.boost = boost;
        self.weight = self.idf * boost;
    }
}

impl<C: Codec> SimWeight<C> for BM25SimWeight {
    fn get_value_for_normalization(&self) -> f32 {
        self.weight * self.weight
    }

    fn normalize(&mut self, _query_norm: f32, boost: f32) {
        self.do_normalize(boost)
    }

    fn sim_scorer(&self, reader: &SearchLeafReader<C>) -> Result<Box<dyn SimScorer>> {
        let norm = reader.norm_values(&self.field)?;
        Ok(Box::new(BM25SimScorer::new(self, norm)))
    }

    fn explain(
        &self,
        reader: &SearchLeafReader<C>,
        doc: DocId,
        freq: Explanation,
    ) -> Result<Explanation> {
        let norms = reader.norm_values(&self.field)?;
        self.explain_score(doc, freq, norms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::index::tests::MockLeafReader;

    // copy from Lucene TestBM25Similarity
    #[test]
    fn test_sane_norm_values() {
        for i in 0..256 {
            let len = BM25Similarity::decode_norm_value(i);
            assert!(len >= 0f32);
            assert!(!len.is_nan());
            assert!(!len.is_infinite());
            if i > 0 {
                assert!(len < BM25Similarity::decode_norm_value(i - 1));
            }
        }
    }

    #[test]
    fn test_idf() {
        let collection_stats = CollectionStatistics::new(String::from("world"), 0, 11, -1, 0, 0);
        let term_stats = vec![TermStatistics::new(Vec::new(), 1, -1)];
        assert!(
            (BM25Similarity::idf(&term_stats, &collection_stats) - (8f32).ln())
                < ::std::f32::EPSILON
        );

        let collection_stats = CollectionStatistics::new(String::from("world"), 0, 35, 32, -1, -1);
        let term_stats = vec![TermStatistics::new(Vec::new(), 1, -1)];
        assert!(
            (BM25Similarity::idf(&term_stats, &collection_stats) - (22f32).ln())
                < ::std::f32::EPSILON
        );
    }

    #[test]
    fn test_avg_field_length() {
        let collection_stats = CollectionStatistics::new(String::from("world"), 0, 11, 5, 0, -1);
        assert!((BM25Similarity::avg_field_length(&collection_stats) - 1f32) < ::std::f32::EPSILON);

        let collection_stats = CollectionStatistics::new(String::from("world"), 0, 3, 2, 8, -1);
        assert!((BM25Similarity::avg_field_length(&collection_stats) - 4f32) < ::std::f32::EPSILON);

        let collection_stats = CollectionStatistics::new(String::from("world"), 0, 3, -1, 9, -1);
        assert!((BM25Similarity::avg_field_length(&collection_stats) - 3f32) < ::std::f32::EPSILON);
    }

    #[test]
    fn test_bm25_similarity() {
        let collection_stats = CollectionStatistics::new(String::from("world"), 0, 32, 32, 120, -1);
        let term_stats = vec![TermStatistics::new(Vec::new(), 1, -1)];
        let bm25_sim = BM25Similarity::new(1.2, 0.75);
        let sim_weight = bm25_sim.compute_weight(&collection_stats, &term_stats, None, 1.0f32);

        assert!((sim_weight.get_value_for_normalization() - 9.554_543_5f32) < ::std::f32::EPSILON);

        let leaf_reader = MockLeafReader::new(1);
        let mut sim_scorer = sim_weight.sim_scorer(&leaf_reader).unwrap();

        // same field length
        let score1 = sim_scorer.score(1, 100.0).unwrap();
        let score2 = sim_scorer.score(1, 20.0).unwrap();

        assert!(score1 > score2);

        // same term_freq
        let score1 = sim_scorer.score(1, 10.0).unwrap();
        let score2 = sim_scorer.score(2, 10.0).unwrap();

        assert!(score1 > score2);
    }
}
