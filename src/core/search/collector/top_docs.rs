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

use std::collections::binary_heap::BinaryHeap;
use std::f32;
use std::mem;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::usize;

use core::codec::Codec;
use core::index::reader::LeafReaderContext;
use core::search::collector::{Collector, ParallelLeafCollector, SearchCollector};
use core::search::scorer::Scorer;
use core::search::sort_field::{ScoreDoc, ScoreDocHit, TopDocs, TopScoreDocs};
use core::util::DocId;
use error::{ErrorKind::IllegalState, Result, ResultExt};

struct TopDocsBaseCollector {
    /// The priority queue which holds the top documents. Note that different
    /// implementations of PriorityQueue give different meaning to 'top documents'.
    /// HitQueue for example aggregates the top scoring documents, while other PQ
    /// implementations may hold documents sorted by other criteria.
    pq: BinaryHeap<ScoreDoc>,

    estimated_hits: usize,

    /// The total number of documents that the collector encountered.
    total_hits: usize,

    cur_doc_base: DocId,
}

impl TopDocsBaseCollector {
    fn new(estimated_hits: usize) -> Self {
        let pq = BinaryHeap::with_capacity(estimated_hits);
        Self {
            pq,
            estimated_hits,
            total_hits: 0,
            cur_doc_base: 0,
        }
    }

    /// Returns the top docs that were collected by this collector.
    fn top_docs(&mut self) -> TopDocs {
        let size = self.total_hits.min(self.pq.len());
        let mut score_docs = Vec::with_capacity(size);

        for _ in 0..size {
            score_docs.push(ScoreDocHit::Score(self.pq.pop().unwrap()));
        }

        score_docs.reverse();
        TopDocs::Score(TopScoreDocs::new(self.total_hits, score_docs))
    }

    fn add_doc(&mut self, doc_id: DocId, score: f32) {
        if self.pq.len() < self.estimated_hits {
            let score_doc = ScoreDoc::new(doc_id, score);
            self.pq.push(score_doc);
        } else if let Some(mut doc) = self.pq.peek_mut() {
            if doc.score < score {
                doc.reset(doc_id, score);
            }
        }
    }
}

impl Collector for TopDocsBaseCollector {
    fn needs_scores(&self) -> bool {
        true
    }

    fn collect<S: Scorer + ?Sized>(&mut self, doc: i32, scorer: &mut S) -> Result<()> {
        let score = scorer.score()?;
        debug_assert!((score - f32::NEG_INFINITY).abs() >= f32::EPSILON);
        debug_assert!(!score.is_nan());

        let id = doc + self.cur_doc_base;
        self.add_doc(id, score);
        self.total_hits += 1;

        Ok(())
    }
}

pub struct TopDocsCollector {
    /// The priority queue which holds the top documents. Note that different
    /// implementations of PriorityQueue give different meaning to 'top documents'.
    /// HitQueue for example aggregates the top scoring documents, while other PQ
    /// implementations may hold documents sorted by other criteria.
    base: TopDocsBaseCollector,

    channel: Option<(Sender<LeafTopDocs>, Receiver<LeafTopDocs>)>,
}

impl TopDocsCollector {
    pub fn new(estimated_hits: usize) -> Self {
        let base = TopDocsBaseCollector::new(estimated_hits);
        Self {
            base,
            channel: None,
        }
    }

    /// Returns the top docs that were collected by this collector.
    pub fn top_docs(&mut self) -> TopDocs {
        self.base.top_docs()
    }

    fn add_doc(&mut self, doc_id: DocId, score: f32) {
        self.base.add_doc(doc_id, score)
    }
}

impl SearchCollector for TopDocsCollector {
    type LC = TopDocsLeafCollector;

    fn set_next_reader<C: Codec>(&mut self, reader: &LeafReaderContext<'_, C>) -> Result<()> {
        self.base.cur_doc_base = reader.doc_base;

        Ok(())
    }

    fn support_parallel(&self) -> bool {
        true
    }

    fn init_parallel(&mut self) {
        if self.channel.is_none() {
            self.channel = Some(channel());
        }
    }

    fn leaf_collector<C: Codec>(
        &self,
        reader: &LeafReaderContext<'_, C>,
    ) -> Result<TopDocsLeafCollector> {
        let mut collector = TopDocsBaseCollector::new(self.base.estimated_hits);
        collector.cur_doc_base = reader.doc_base;
        Ok(TopDocsLeafCollector::new(
            collector,
            self.channel.as_ref().unwrap().0.clone(),
        ))
    }

    fn finish_parallel(&mut self) -> Result<()> {
        let channel = self.channel.take();
        // iff all the `weight.create_scorer(leaf_reader)` return None, the channel won't
        // inited and thus stay None
        if let Some((sender, receiver)) = channel {
            drop(sender);
            while let Ok(docs) = receiver.recv() {
                self.base.total_hits += docs.total_hits;
                for doc in docs.docs {
                    self.add_doc(doc.doc, doc.score);
                }
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
        self.base.collect(doc, scorer)
    }
}

struct LeafTopDocs {
    docs: Vec<ScoreDoc>,
    total_hits: usize,
}

pub struct TopDocsLeafCollector {
    collector: TopDocsBaseCollector,
    channel: Sender<LeafTopDocs>,
}

impl TopDocsLeafCollector {
    fn new(collector: TopDocsBaseCollector, channel: Sender<LeafTopDocs>) -> TopDocsLeafCollector {
        Self { collector, channel }
    }
}

impl ParallelLeafCollector for TopDocsLeafCollector {
    /// may do clean up and notify parent that leaf is ended
    fn finish_leaf(&mut self) -> Result<()> {
        let docs = mem::replace(&mut self.collector.pq, BinaryHeap::new());
        let top_docs = LeafTopDocs {
            // the doc is not sorted, but this is ok.
            docs: docs.into_vec(),
            total_hits: self.collector.total_hits,
        };
        self.channel
            .send(top_docs)
            .chain_err(|| IllegalState("channel unexpected closed before search complete".into()))
    }
}

impl Collector for TopDocsLeafCollector {
    fn needs_scores(&self) -> bool {
        true
    }

    fn collect<S: Scorer + ?Sized>(&mut self, doc: i32, scorer: &mut S) -> Result<()> {
        self.collector.collect(doc, scorer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::search::tests::*;

    use core::index::reader::IndexReader;
    use core::index::tests::*;
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
