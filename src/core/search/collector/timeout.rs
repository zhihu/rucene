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
use std::time::{Duration, SystemTime};

/// the `TimeoutCollector` collector is used to timeout search requests that
/// take longer than the maximum allowed search time limit.
///
/// After this time is exceeded, the search thread is stopped by return a
/// `CollectionTerminated` error.
///
/// this collector is useful if your search must be complete at some specific time.
/// you can use the `ChainedCollector` to compose this collector with eg.
/// `TopDocsCollector`.
pub struct TimeoutCollector {
    timeout_duration: Duration,
    start_time: SystemTime,
    timeout: Arc<Volatile<bool>>,
}

impl TimeoutCollector {
    pub fn new(timeout_duration: Duration, start_time: SystemTime) -> TimeoutCollector {
        TimeoutCollector {
            timeout_duration,
            start_time,
            timeout: Arc::new(Volatile::new(false)),
        }
    }

    pub fn timeout(&self) -> bool {
        self.timeout.read()
    }
}

impl SearchCollector for TimeoutCollector {
    type LC = TimeoutLeafCollector;

    fn set_next_reader<C: Codec>(&mut self, _reader: &LeafReaderContext<'_, C>) -> Result<()> {
        Ok(())
    }

    fn support_parallel(&self) -> bool {
        true
    }

    fn leaf_collector<C: Codec>(
        &self,
        _reader: &LeafReaderContext<'_, C>,
    ) -> Result<TimeoutLeafCollector> {
        Ok(TimeoutLeafCollector::new(
            self.timeout_duration,
            self.start_time,
            Arc::clone(&self.timeout),
        ))
    }

    fn finish_parallel(&mut self) -> Result<()> {
        Ok(())
    }
}

impl Collector for TimeoutCollector {
    fn needs_scores(&self) -> bool {
        false
    }

    fn collect<S: Scorer + ?Sized>(&mut self, _doc: DocId, _scorer: &mut S) -> Result<()> {
        let now = SystemTime::now();
        if self.start_time < now && now.duration_since(self.start_time)? >= self.timeout_duration {
            self.timeout.write(true);
            bail!(ErrorKind::Collector(
                collector::ErrorKind::CollectionTimeout,
            ))
        }
        Ok(())
    }
}

pub struct TimeoutLeafCollector {
    timeout_duration: Duration,
    start_time: SystemTime,
    timeout: Arc<Volatile<bool>>,
}

impl TimeoutLeafCollector {
    pub fn new(
        timeout_duration: Duration,
        start_time: SystemTime,
        timeout: Arc<Volatile<bool>>,
    ) -> TimeoutLeafCollector {
        TimeoutLeafCollector {
            timeout_duration,
            start_time,
            timeout,
        }
    }
}

impl Collector for TimeoutLeafCollector {
    fn needs_scores(&self) -> bool {
        false
    }

    fn collect<S: Scorer + ?Sized>(&mut self, _doc: i32, _scorer: &mut S) -> Result<()> {
        let now = SystemTime::now();
        if self.start_time < now && now.duration_since(self.start_time)? >= self.timeout_duration {
            self.timeout.write(true);
            bail!(ErrorKind::Collector(
                collector::ErrorKind::CollectionTerminated,
            ))
        }
        Ok(())
    }
}

impl ParallelLeafCollector for TimeoutLeafCollector {
    fn finish_leaf(&mut self) -> Result<()> {
        Ok(())
    }
}
