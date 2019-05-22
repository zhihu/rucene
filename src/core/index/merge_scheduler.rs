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
