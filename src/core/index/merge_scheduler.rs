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
use core::index::index_writer::IndexWriter;
use core::index::merge_policy::{MergePolicy, MergerTrigger};
use core::store::Directory;
use error::Result;

// use std::cmp::Ordering;
// use std::thread::{self, ThreadId};

/// Expert: `IndexWriter` uses an instance
/// implementing this interface to execute the merges
/// selected by a `MergePolicy`.  The default
/// MergeScheduler is `ConcurrentMergeScheduler`.
/// Implementers of sub-classes should make sure that {@link #clone()}
/// returns an independent instance able to work with any `IndexWriter`
/// instance.
pub trait MergeScheduler: Send + Sync + Clone + 'static {
    fn merge<D: Directory, C: Codec, MS: MergeScheduler, MP: MergePolicy>(
        &self,
        writer: &IndexWriter<D, C, MS, MP>,
        trigger: MergerTrigger,
        new_merges_found: bool,
    ) -> Result<()>;

    fn close(&self) -> Result<()>;
}

#[derive(Copy, Clone)]
pub struct SerialMergeScheduler;

impl MergeScheduler for SerialMergeScheduler {
    fn merge<D: Directory, C: Codec, MS: MergeScheduler, MP: MergePolicy>(
        &self,
        writer: &IndexWriter<D, C, MS, MP>,
        _trigger: MergerTrigger,
        _new_merges_found: bool,
    ) -> Result<()> {
        loop {
            if let Some(ref mut merge) = writer.next_merge() {
                writer.merge(merge)?;
            } else {
                break;
            }
        }
        Ok(())
    }

    fn close(&self) -> Result<()> {
        Ok(())
    }
}
