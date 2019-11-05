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

pub mod directory;
pub mod io;

use error::Result;

use std::sync::Arc;
use std::time::Duration;

/// IOContext holds additional details on the merge/search context and
/// specifies the context in which the Directory is being used for.
#[derive(PartialEq, Eq, Clone, Copy)]
pub enum IOContext {
    Read(bool),
    Default,
    Flush(FlushInfo),
    Merge(MergeInfo),
}

impl IOContext {
    pub const READ: IOContext = IOContext::Read(false);
    pub const READ_ONCE: IOContext = IOContext::Read(true);
    pub fn is_merge(&self) -> bool {
        match self {
            IOContext::Merge(_) => true,
            _ => false,
        }
    }
}

/// A FlushInfo provides information required for a FLUSH context.
///
/// It is used as part of an `IOContext` in case of FLUSH context.
#[derive(PartialEq, Eq, Hash, Clone, Copy)]
pub struct FlushInfo {
    num_docs: u32,
}

impl FlushInfo {
    pub fn new(num_docs: u32) -> Self {
        FlushInfo { num_docs }
    }
}

/// A MergeInfo provides information required for a MERGE context.
///
/// It is used as part of an `IOContext` in case of MERGE context.
#[derive(PartialEq, Eq, Hash, Clone, Copy)]
pub struct MergeInfo {
    total_max_doc: u32,
    estimated_merge_bytes: u64,
    is_external: bool,
    merge_max_num_segments: Option<u32>,
}

impl MergeInfo {
    pub fn new(
        total_max_doc: u32,
        estimated_merge_bytes: u64,
        is_external: bool,
        merge_max_num_segments: Option<u32>,
    ) -> Self {
        MergeInfo {
            total_max_doc,
            estimated_merge_bytes,
            is_external,
            merge_max_num_segments,
        }
    }
}

/// Trait base class to rate limit IO.
///
/// Typically implementations are shared across multiple IndexInputs
/// or IndexOutputs (for example those involved all merging).  Those IndexInputs and
/// IndexOutputs would call {@link #pause} whenever the have read
/// or written more than {@link #getMinPauseCheckBytes} bytes.

pub trait RateLimiter: Sync + Send {
    /// Sets an updated MB per second rate limit.
    fn set_mb_per_sec(&self, mb_per_sec: f64);

    /// The current MB per second rate limit.
    fn mb_per_sec(&self) -> f64;

    /// Pauses, if necessary, to keep the instantaneous IO rate
    /// at or below the target
    ///
    /// Note: the implementation is thread-safe
    fn pause(&self, bytes: u64) -> Result<Duration>;

    /// how many bytes caller should add up isself before invoking `#pause`
    fn min_pause_check_bytes(&self) -> u64;
}

impl RateLimiter for Arc<dyn RateLimiter> {
    fn set_mb_per_sec(&self, mb_per_sec: f64) {
        (**self).set_mb_per_sec(mb_per_sec);
    }

    fn mb_per_sec(&self) -> f64 {
        (**self).mb_per_sec()
    }

    fn pause(&self, bytes: u64) -> Result<Duration> {
        (**self).pause(bytes)
    }

    fn min_pause_check_bytes(&self) -> u64 {
        (**self).min_pause_check_bytes()
    }
}
