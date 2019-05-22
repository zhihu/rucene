use std::collections::BinaryHeap;
use std::f32;
use std::usize;

use core::codec::Codec;
use core::index::LeafReaderContext;
use core::search::collector::{Collector, ParallelLeafCollector, SearchCollector};
use core::search::top_docs::{ScoreDoc, ScoreDocHit, TopDocs, TopScoreDocs};
use core::search::Scorer;
use core::util::DocId;
use error::{ErrorKind::IllegalState, Result};

use crossbeam::channel::{unbounded, Receiver, Sender};

type ScoreDocPriorityQueue = BinaryHeap<ScoreDoc>;

pub struct TopDocsCollector {
    /// The priority queue which holds the top documents. Note that different
    /// implementations of PriorityQueue give different meaning to 'top documents'.
    /// HitQueue for example aggregates the top scoring documents, while other PQ
    /// implementations may hold documents sorted by other criteria.
    pq: ScoreDocPriorityQueue,

    estimated_hits: usize,

    /// The total number of documents that the collector encountered.
    total_hits: usize,

    cur_doc_base: DocId,

    // TODO used for parallel collect, maybe should be move the new struct for parallel search
    channel: Option<(Sender<ScoreDoc>, Receiver<ScoreDoc>)>,
}

impl TopDocsCollector {
    pub fn new(estimated_hits: usize) -> TopDocsCollector {
        let pq = ScoreDocPriorityQueue::with_capacity(estimated_hits);
        TopDocsCollector {
            pq,
            estimated_hits,
            total_hits: 0,
            cur_doc_base: 0,
            channel: None,
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

        if !at_capacity {
            let score_doc = ScoreDoc::new(doc_id, score);
            self.pq.push(score_doc);
        } else if let Some(mut doc) = self.pq.peek_mut() {
            if doc.score < score {
                doc.reset(doc_id, score);
            }
        }
    }
}

impl SearchCollector for TopDocsCollector {
    type LC = TopDocsLeafCollector;

    fn set_next_reader<C: Codec>(&mut self, reader: &LeafReaderContext<'_, C>) -> Result<()> {
        self.cur_doc_base = reader.doc_base;

        Ok(())
    }

    fn support_parallel(&self) -> bool {
        true
    }

    fn leaf_collector<C: Codec>(
        &mut self,
        reader: &LeafReaderContext<'_, C>,
    ) -> Result<TopDocsLeafCollector> {
        if self.channel.is_none() {
            self.channel = Some(unbounded());
        }
        Ok(TopDocsLeafCollector::new(
            reader.doc_base,
            self.channel.as_ref().unwrap().0.clone(),
        ))
    }

    fn finish_parallel(&mut self) -> Result<()> {
        let channel = self.channel.take();
        // iff all the `weight.create_scorer(leaf_reader)` return None, the channel won't
        // inited and thus stay None
        if let Some((sender, receiver)) = channel {
            drop(sender);
            while let Ok(doc) = receiver.recv() {
                self.add_doc(doc.doc, doc.score)
            }
        }

        Ok(())
    }
}

impl Collector for TopDocsCollector {
    fn needs_scores(&self) -> bool {
        true
    }

    fn collect<S: Scorer + ?Sized>(&mut self, doc: DocId, scorer: &mut S) -> Result<()> {
        let score = scorer.score()?;
        debug_assert!((score - f32::NEG_INFINITY).abs() >= f32::EPSILON);
        debug_assert!(!score.is_nan());

        let id = doc + self.cur_doc_base;
        self.add_doc(id, score);

        Ok(())
    }
}

pub struct TopDocsLeafCollector {
    doc_base: DocId,
    channel: Sender<ScoreDoc>,
}

impl TopDocsLeafCollector {
    pub fn new(doc_base: DocId, channel: Sender<ScoreDoc>) -> TopDocsLeafCollector {
        TopDocsLeafCollector { doc_base, channel }
    }
}

impl ParallelLeafCollector for TopDocsLeafCollector {
    /// may do clean up and notify parent that leaf is ended
    fn finish_leaf(&mut self) -> Result<()> {
        Ok(())
    }
}

impl Collector for TopDocsLeafCollector {
    fn needs_scores(&self) -> bool {
        true
    }

    fn collect<S: Scorer + ?Sized>(&mut self, doc: i32, scorer: &mut S) -> Result<()> {
        let score_doc = ScoreDoc::new(doc + self.doc_base, scorer.score()?);
        self.channel.send(score_doc).map_err(|e| {
            IllegalState(format!(
                "channel unexpected closed before search complete with err: {:?}",
                e
            ))
            .into()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::search::tests::*;

    use core::index::tests::*;
    use core::index::IndexReader;
    use core::search::*;

    #[test]
    fn test_collect() {
        let mut scorer = create_mock_scorer(vec![1, 2, 3, 3, 5]);

        let leaf_reader = MockLeafReader::new(0);
        let index_reader = MockIndexReader::new(vec![leaf_reader]);
        let leaf_reader_context = index_reader.leaves();
        let mut collector = TopDocsCollector::new(3);

        {
            collector.set_next_reader(&leaf_reader_context[0]).unwrap();
            loop {
                let doc = scorer.next().unwrap();
                if doc != NO_MORE_DOCS {
                    collector.collect(doc, &mut scorer).unwrap();
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
