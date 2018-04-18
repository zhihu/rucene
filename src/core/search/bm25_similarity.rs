use error::Result;
use std::sync::Arc;

use core::index::field_info::FieldInvertState;
use core::index::LeafReader;
use core::index::NumericDocValues;
use core::search::statistics::CollectionStatistics;
use core::search::statistics::TermStatistics;
use core::search::SimScorer;
use core::search::SimWeight;
use core::search::Similarity;
use core::util::small_float::SmallFloat;
use core::util::DocId;

/// BM25 Similarity. Introduced in Stephen E. Robertson, Steve Walker,
/// Susan Jones, Micheline Hancock-Beaulieu, and Mike Gatford. Okapi at TREC-3.
/// In Proceedings of the Third *T*ext *RE*trieval *C*onference (TREC 1994).
/// Gaithersburg, USA, November 1994.
///
///
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

pub struct BM25Similarity {
    k1: f32,
    b: f32,
}

impl BM25Similarity {
    pub fn new(k1: f32, b: f32) -> BM25Similarity {
        BM25Similarity { k1, b }
    }
    #[allow(dead_code)]
    fn sloppy_freq(distance: i32) -> f32 {
        (1.0 / distance as f32 + 1.0)
    }

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

    pub fn compute_norm(state: &FieldInvertState) -> u8 {
        BM25Similarity::encode_norm_value(state.boost, state.length)
    }

    pub fn encode_norm_value(boost: f32, field_length: i32) -> u8 {
        SmallFloat::float_to_byte315(boost / (field_length as f32).sqrt())
    }

    fn decode_norm_value(b: u8) -> f32 {
        NORM_TABLE[b as usize]
    }

    fn idf(term_stats: &TermStatistics, collection_stats: &CollectionStatistics) -> f32 {
        let doc_freq = term_stats.doc_freq;
        let mut doc_count = collection_stats.doc_count;
        if doc_count == -1 {
            doc_count = collection_stats.max_doc
        };
        (1.0 + (doc_count as f64 - doc_freq as f64 + 0.5) / (doc_freq as f64 + 0.5)).ln() as f32
    }
}

impl Similarity for BM25Similarity {
    type Weight = BM25SimWeight;

    fn compute_weight(
        &self,
        collection_stats: &CollectionStatistics,
        term_stats: &TermStatistics,
    ) -> BM25SimWeight {
        let avgdl = BM25Similarity::avg_field_length(&collection_stats);
        let idf = BM25Similarity::idf(&term_stats, &collection_stats);
        let field = collection_stats.field.clone();
        let mut cache: [f32; 256] = [0f32; 256];
        for (i, c) in cache.iter_mut().enumerate() {
            *c = self.k1
                * ((1.0 - self.b) + self.b * (BM25Similarity::decode_norm_value(i as u8) / avgdl));
        }

        BM25SimWeight::new(self.k1, self.b, idf, field, cache)
    }

    fn sim_scorer(&self, stats: Arc<BM25SimWeight>, reader: &LeafReader) -> Result<Box<SimScorer>> {
        let norm = reader.norm_values(&stats.field)?;
        let boxed = BM25SimScorer::new(stats, norm);
        Ok(Box::new(boxed))
    }
}

pub struct BM25SimScorer {
    weight: Arc<BM25SimWeight>,
    norms: Option<Box<NumericDocValues>>,
}

impl BM25SimScorer {
    fn new(weight: Arc<BM25SimWeight>, norms: Option<Box<NumericDocValues>>) -> BM25SimScorer {
        BM25SimScorer { weight, norms }
    }

    pub fn compute_score(&mut self, doc: i32, freq: f32) -> Result<f32> {
        let mut norm = self.weight.k1;
        if let Some(ref mut norms) = self.norms {
            let encode_length = (norms.get(doc)? & 0xFF) as usize;
            norm = self.weight.cache[encode_length];
        }

        Ok(self.weight.idf * (self.weight.k1 + 1.0) * freq / (freq + norm))
    }
}

impl SimScorer for BM25SimScorer {
    fn score(&mut self, doc: DocId, freq: f32) -> Result<f32> {
        self.compute_score(doc, freq)
    }
}

pub struct BM25SimWeight {
    k1: f32,
    #[allow(dead_code)]
    b: f32,
    idf: f32,
    field: String,
    cache: [f32; 256],
}

impl BM25SimWeight {
    fn new(k1: f32, b: f32, idf: f32, field: String, cache: [f32; 256]) -> BM25SimWeight {
        BM25SimWeight {
            k1,
            b,
            idf,
            field,
            cache,
        }
    }
}

impl SimWeight for BM25SimWeight {}

#[cfg(test)]
mod tests {
    use super::*;
    use core::index::tests::MockLeafReader;
    use std::string::String;

    // copy from Lucene TestBM25Similarity
    #[test]
    fn test_sane_norm_values() {
        for i in 0..256 {
            let len = BM25Similarity::decode_norm_value(i as u8);
            assert!(len >= 0f32);
            assert!(!len.is_nan());
            assert!(!len.is_infinite());
            if i > 0 {
                assert!(len < BM25Similarity::decode_norm_value((i - 1) as u8));
            }
        }
    }

    #[test]
    fn test_idf() {
        let collection_stats = CollectionStatistics::new(String::from("world"), 11, -1, 0, 0);
        let term_stats = TermStatistics::new(Vec::new(), 1, -1);
        assert!(
            (BM25Similarity::idf(&term_stats, &collection_stats) - (8f32).ln())
                < ::std::f32::EPSILON
        );

        let collection_stats = CollectionStatistics::new(String::from("world"), 11, 32, 0, 0);
        let term_stats = TermStatistics::new(Vec::new(), 1, -1);
        assert!(
            (BM25Similarity::idf(&term_stats, &collection_stats) - (22f32).ln())
                < ::std::f32::EPSILON
        );
    }

    #[test]
    fn test_avg_field_length() {
        let collection_stats = CollectionStatistics::new(String::from("world"), 11, 32, 0, 0);
        assert!((BM25Similarity::avg_field_length(&collection_stats) - 1f32) < ::std::f32::EPSILON);

        let collection_stats = CollectionStatistics::new(String::from("world"), 3, 2, 8, 0);
        assert!((BM25Similarity::avg_field_length(&collection_stats) - 4f32) < ::std::f32::EPSILON);

        let collection_stats = CollectionStatistics::new(String::from("world"), 3, -1, 9, 0);
        assert!((BM25Similarity::avg_field_length(&collection_stats) - 3f32) < ::std::f32::EPSILON);
    }

    #[test]
    fn test_bm25_similarity() {
        let collection_stats = CollectionStatistics::new(String::from("world"), 11, 32, 120, 0);
        let term_stats = TermStatistics::new(Vec::new(), 1, -1);
        let bm25_sim = BM25Similarity::new(1.2, 0.75);
        let sim_weight = bm25_sim.compute_weight(&collection_stats, &term_stats);

        assert!((sim_weight.k1 - 1.2) < ::std::f32::EPSILON);
        assert!((sim_weight.b - 0.75) < ::std::f32::EPSILON);
        assert_eq!(sim_weight.field, "world");
        assert!((sim_weight.idf - 22f32.ln()) < ::std::f32::EPSILON);

        let leaf_reader = MockLeafReader::new(1);
        let mut sim_scorer = bm25_sim
            .sim_scorer(Arc::new(sim_weight), &leaf_reader)
            .unwrap();

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
