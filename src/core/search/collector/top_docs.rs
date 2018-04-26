use std::collections::BinaryHeap;
use std::f32;
use std::usize;

use core::index::LeafReader;
use core::search::collector::Collector;
use core::search::top_docs::{ScoreDoc, ScoreDocHit, TopDocs, TopScoreDocs};
use core::search::Scorer;
use core::util::DocId;
use error::*;

type ScoreDocPriorityQueue = BinaryHeap<ScoreDoc>;

struct LeafReaderContext {
    ord: usize,
    doc_base: DocId,
}

pub struct TopDocsCollector {
    /// The priority queue which holds the top documents. Note that different
    /// implementations of PriorityQueue give different meaning to 'top documents'.
    /// HitQueue for example aggregates the top scoring documents, while other PQ
    /// implementations may hold documents sorted by other criteria.
    ///
    pq: ScoreDocPriorityQueue,

    estimated_hits: usize,

    /// The total number of documents that the collector encountered.
    total_hits: usize,

    reader_context: Option<LeafReaderContext>,
}

impl TopDocsCollector {
    pub fn new(estimated_hits: usize) -> TopDocsCollector {
        let pq = ScoreDocPriorityQueue::with_capacity(estimated_hits);
        TopDocsCollector {
            pq,
            estimated_hits,
            total_hits: 0,
            reader_context: None,
        }
    }

    /// Returns the top docs that were collected by this collector.
    pub fn top_docs(&mut self) -> TopDocs {
        let size = self.total_hits.min(self.pq.len());
        let mut score_docs = Vec::with_capacity(size);

        for _ in 0..size {
            score_docs.push(ScoreDocHit::Score(self.pq.pop().unwrap()));
        }

        score_docs.reverse();

        TopDocs::Score(TopScoreDocs::new(self.total_hits, score_docs))
    }

    fn add_doc(&mut self, doc_id: DocId, score: f32) {
        debug_assert!(self.pq.len() <= self.estimated_hits);

        self.total_hits += 1;

        let at_capacity = self.pq.len() == self.estimated_hits;
        let reader_context = self.reader_context.as_ref().unwrap();

        if !at_capacity {
            let score_doc = ScoreDoc::new(doc_id, score, reader_context.ord);
            self.pq.push(score_doc);
        } else if let Some(doc) = self.pq.pop() {
            if doc.score < score {
                let score_doc = ScoreDoc::new(doc_id, score, reader_context.ord);
                self.pq.push(score_doc);
            } else {
                self.pq.push(doc);
            }
        }
    }

    fn doc_base(&self) -> DocId {
        self.reader_context.as_ref().unwrap().doc_base
    }
}

impl Collector for TopDocsCollector {
    fn set_next_reader(&mut self, reader_ord: usize, reader: &LeafReader) -> Result<()> {
        let reader_context = LeafReaderContext {
            ord: reader_ord,
            doc_base: reader.doc_base(),
        };
        self.reader_context = Some(reader_context);

        Ok(())
    }

    fn collect(&mut self, doc: DocId, scorer: &mut Scorer) -> Result<()> {
        debug_assert!(self.reader_context.is_some());
        let doc_base = self.doc_base();
        let score = scorer.score()?;
        debug_assert!((score - f32::NEG_INFINITY).abs() >= f32::EPSILON);
        debug_assert!(!score.is_nan());

        self.add_doc(doc + doc_base, score);

        Ok(())
    }

    fn needs_scores(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::search::tests::*;

    use core::index::tests::*;
    use core::search::*;

    #[test]
    fn test_collect() {
        let mut scorer_box = create_mock_scorer(vec![1, 2, 3, 3, 5]);
        let scorer = scorer_box.as_mut();

        let leaf_reader = MockLeafReader::new(0);
        let mut collector = TopDocsCollector::new(3);

        {
            collector.set_next_reader(0, &leaf_reader).unwrap();
            loop {
                let doc = scorer.next().unwrap();
                if doc != NO_MORE_DOCS {
                    collector.collect(doc, scorer).unwrap();
                } else {
                    break;
                }
            }
        }

        let top_docs = collector.top_docs();
        assert_eq!(top_docs.total_hits(), 5);

        let score_docs = top_docs.score_docs();
        assert_eq!(score_docs.len(), 3);
        assert_eq!(score_docs[0].doc_id(), 5);
        assert_eq!(score_docs[1].doc_id(), 3);
        assert_eq!(score_docs[2].doc_id(), 3);
    }
}
