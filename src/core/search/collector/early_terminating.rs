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

use core::codec::Codec;
use core::index::reader::LeafReaderContext;
use core::search::collector;
use core::search::collector::{Collector, ParallelLeafCollector, SearchCollector};
use core::search::scorer::Scorer;
use core::util::external::Volatile;
use core::util::DocId;
use error::{ErrorKind, Result};
use std::sync::Arc;

pub struct EarlyTerminatingSortingCollector {
    early_terminated: Arc<Volatile<bool>>,
    num_docs_to_collect_per_reader: usize,
    num_docs_collected_per_reader: usize,
}

impl EarlyTerminatingSortingCollector {
    pub fn new(num_docs_to_collect_per_reader: usize) -> EarlyTerminatingSortingCollector {
        assert!(
            num_docs_to_collect_per_reader > 0,
            format!(
                "num_docs_to_collect_per_reader must always be > 0, got {}",
                num_docs_to_collect_per_reader
            )
        );

        EarlyTerminatingSortingCollector {
            early_terminated: Arc::new(Volatile::new(false)),
            num_docs_to_collect_per_reader,
            num_docs_collected_per_reader: 0,
        }
    }

    pub fn early_terminated(&self) -> bool {
        self.early_terminated.read()
    }
}

impl SearchCollector for EarlyTerminatingSortingCollector {
    type LC = EarlyTerminatingLeafCollector;
    fn set_next_reader<C: Codec>(&mut self, _reader: &LeafReaderContext<'_, C>) -> Result<()> {
        self.num_docs_collected_per_reader = 0;
        Ok(())
    }

    fn support_parallel(&self) -> bool {
        true
    }

    fn leaf_collector<C: Codec>(&self, _reader: &LeafReaderContext<'_, C>) -> Result<Self::LC> {
        assert!(self.support_parallel());
        Ok(EarlyTerminatingLeafCollector::new(
            self.num_docs_to_collect_per_reader,
            Arc::clone(&self.early_terminated),
        ))
    }

    fn finish_parallel(&mut self) -> Result<()> {
        Ok(())
    }
}

impl Collector for EarlyTerminatingSortingCollector {
    fn needs_scores(&self) -> bool {
        false
    }

    fn collect<S: Scorer + ?Sized>(&mut self, _doc: DocId, _scorer: &mut S) -> Result<()> {
        self.num_docs_collected_per_reader += 1;

        if self.num_docs_collected_per_reader > self.num_docs_to_collect_per_reader {
            self.early_terminated.write(true);
            bail!(ErrorKind::Collector(
                collector::ErrorKind::LeafCollectionTerminated,
            ))
        }
        Ok(())
    }
}

/// A `Collector` that early terminates collection of documents on a
/// per-segment basis, if the segment was sorted according to the given
/// `Sort`.
///
/// *NOTE:* the `Collector` detects segments sorted according to a
/// an `IndexWriterConfig#setIndexSort`. Also, it collects up to a specified
/// `num_docs_to_collect_per_reader` from each segment, and therefore is mostly suitable
/// for use in conjunction with collectors such as `TopDocsCollector`, and
/// not e.g. `TotalHitCountCollector`.
///
/// *NOTE*: If you wrap a `TopDocsCollector` that sorts in the same
/// order as the index order, the returned top docs will be correct.
/// However the total of hit count will be vastly underestimated since not all matching documents
/// will have been collected.
pub struct EarlyTerminatingLeafCollector {
    early_terminated: Arc<Volatile<bool>>,
    num_docs_to_collect: usize,
    num_docs_collected: usize,
}

impl EarlyTerminatingLeafCollector {
    pub fn new(
        num_docs_to_collect: usize,
        early_terminated: Arc<Volatile<bool>>,
    ) -> EarlyTerminatingLeafCollector {
        EarlyTerminatingLeafCollector {
            early_terminated,
            num_docs_to_collect,
            num_docs_collected: 0,
        }
    }

    pub fn early_terminated(&self) -> bool {
        self.early_terminated.read()
    }
}

impl ParallelLeafCollector for EarlyTerminatingLeafCollector {
    fn finish_leaf(&mut self) -> Result<()> {
        Ok(())
    }
}

impl Collector for EarlyTerminatingLeafCollector {
    fn needs_scores(&self) -> bool {
        false
    }

    fn collect<S: Scorer + ?Sized>(&mut self, _doc: i32, _scorer: &mut S) -> Result<()> {
        self.num_docs_collected += 1;

        if self.num_docs_collected > self.num_docs_to_collect {
            self.early_terminated.write(true);
            bail!(ErrorKind::Collector(
                collector::ErrorKind::LeafCollectionTerminated,
            ))
        }
        Ok(())
    }
}
