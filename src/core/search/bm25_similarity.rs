use error::Result;
use std::fmt;
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

pub const DEFAULT_BM25_K1: f32 = 1.2;
pub const DEFAULT_BM25_B: f32 = 0.75;

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

    pub fn compute_norm(state: &FieldInvertState) -> u8 {
        BM25Similarity::encode_norm_value(state.boost, state.length)
    }

    pub fn encode_norm_value(boost: f32, field_length: i32) -> u8 {
        SmallFloat::float_to_byte315(boost / (field_length as f32).sqrt())
    }

    fn decode_norm_value(b: u8) -> f32 {
        NORM_TABLE[b as usize]
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
}

impl Similarity for BM25Similarity {
    fn compute_weight(
        &self,
        collection_stats: &CollectionStatistics,
        term_stats: &[TermStatistics],
    ) -> Box<SimWeight> {
        let avgdl = BM25Similarity::avg_field_length(&collection_stats);
        let idf = BM25Similarity::idf(&term_stats, &collection_stats);
        let field = collection_stats.field.clone();
        let mut cache: [f32; 256] = [0f32; 256];
        for (i, c) in cache.iter_mut().enumerate() {
            *c = self.k1
                * ((1.0 - self.b) + self.b * (BM25Similarity::decode_norm_value(i as u8) / avgdl));
        }

        Box::new(BM25SimWeight::new(self.k1, self.b, idf, field, cache))
    }
}

impl fmt::Display for BM25Similarity {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "BM25Similarity(k1: {}, b: {})", self.k1, self.b)
    }
}

pub struct BM25SimScorer {
    k1: f32,
    weight: f32,
    cache: Arc<[f32; 256]>,
    norms: Option<Box<NumericDocValues>>,
}

impl BM25SimScorer {
    fn new(weight: &BM25SimWeight, norms: Option<Box<NumericDocValues>>) -> BM25SimScorer {
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

pub struct BM25SimWeight {
    k1: f32,
    #[allow(dead_code)]
    b: f32,
    idf: f32,
    field: String,
    cache: Arc<[f32; 256]>,
    boost: f32,
    weight: f32,
}

impl BM25SimWeight {
    fn new(k1: f32, b: f32, idf: f32, field: String, cache: [f32; 256]) -> BM25SimWeight {
        let mut weight = BM25SimWeight {
            k1,
            b,
            idf,
            field,
            cache: Arc::new(cache),
            boost: 1.0,
            weight: 0.0,
        };
        weight.normalize(1.0, 1.0);
        weight
    }
}

impl SimWeight for BM25SimWeight {
    fn get_value_for_normalization(&self) -> f32 {
        self.weight * self.weight
    }

    fn normalize(&mut self, _query_norm: f32, boost: f32) {
        self.boost = boost;
        self.weight = self.idf * boost;
    }

    fn sim_scorer(&self, reader: &LeafReader) -> Result<Box<SimScorer>> {
        let norm = reader.norm_values(&self.field)?;
        Ok(Box::new(BM25SimScorer::new(self, norm)))
    }
}

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
        let term_stats = vec![TermStatistics::new(Vec::new(), 1, -1)];
        assert!(
            (BM25Similarity::idf(&term_stats, &collection_stats) - (8f32).ln())
                < ::std::f32::EPSILON
        );

        let collection_stats = CollectionStatistics::new(String::from("world"), 35, 32, -1, -1);
        let term_stats = vec![TermStatistics::new(Vec::new(), 1, -1)];
        assert!(
            (BM25Similarity::idf(&term_stats, &collection_stats) - (22f32).ln())
                < ::std::f32::EPSILON
        );
    }

    #[test]
    fn test_avg_field_length() {
        let collection_stats = CollectionStatistics::new(String::from("world"), 11, 5, 0, -1);
        assert!((BM25Similarity::avg_field_length(&collection_stats) - 1f32) < ::std::f32::EPSILON);

        let collection_stats = CollectionStatistics::new(String::from("world"), 3, 2, 8, -1);
        assert!((BM25Similarity::avg_field_length(&collection_stats) - 4f32) < ::std::f32::EPSILON);

        let collection_stats = CollectionStatistics::new(String::from("world"), 3, -1, 9, -1);
        assert!((BM25Similarity::avg_field_length(&collection_stats) - 3f32) < ::std::f32::EPSILON);
    }

    #[test]
    fn test_bm25_similarity() {
        let collection_stats = CollectionStatistics::new(String::from("world"), 11, 32, 120, 0);
        let term_stats = vec![TermStatistics::new(Vec::new(), 1, -1)];
        let bm25_sim = BM25Similarity::new(1.2, 0.75);
        let sim_weight = bm25_sim.compute_weight(&collection_stats, &term_stats);

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
