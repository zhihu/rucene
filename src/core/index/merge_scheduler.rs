use core::index::index_writer::IndexWriter;
use core::index::merge_policy::{MergerTrigger, OneMerge};
use error::Result;

use std::cmp::Ordering;
use std::thread::{self, ThreadId};

/// Expert: `IndexWriter` uses an instance
/// implementing this interface to execute the merges
/// selected by a `MergePolicy`.  The default
/// MergeScheduler is `ConcurrentMergeScheduler`.
/// Implementers of sub-classes should make sure that {@link #clone()}
/// returns an independent instance able to work with any `IndexWriter`
/// instance.
pub trait MergeScheduler {
    fn merge(
        &self,
        writer: &mut IndexWriter,
        trigger: MergerTrigger,
        new_merges_found: bool,
    ) -> Result<()>;

    fn close(&mut self) -> Result<()>;
}

/// Dynamic default for {@code maxThreadCount} and {@code maxMergeCount},
//  used to detect whether the index is backed by an SSD or rotational disk and
//  set {@code maxThreadCount} accordingly.  If it's an SSD,
//  {@code maxThreadCount} is set to {@code max(1, min(4, cpuCoreCount/2))},
//  otherwise 1.  Note that detection only currently works on
//  Linux; other platforms will assume the index is not on an SSD.
const AUTO_DETECT_MERGES_AND_THREADS: Option<u32> = None;

/// A {@link MergeScheduler} that runs each merge using a
//  separate thread.
//
//  Specify the max number of threads that may run at
//  once, and the maximum number of simultaneous merges
//  with {@link #setMaxMergesAndThreads}.
//
//  If the number of merges exceeds the max number of threads
//  then the largest merges are paused until one of the smaller
//  merges completes.
//
//  If more than {@link #getMaxMergeCount} merges are
//  requested then this class will forcefully throttle the
//  incoming threads by pausing until one more more merges
//  complete.
//
//  This class attempts to detect whether the index is
//  on rotational storage (traditional hard drive) or not
//  (e.g. solid-state disk) and changes the default max merge
//  and thread count accordingly.  This detection is currently
//  Linux-only, and relies on the OS to put the right value
//  into /sys/block/&lt;dev&gt;/block/rotational.  For all
//  other operating systems it currently assumes a rotational
//  disk for backwards compatibility.  To enable default
//  settings for spinning or solid state disks for such
//  operating systems, use {@link #setDefaultMaxMergesAndThreads(boolean)}.
//
pub struct ConcurrentMergeScheduler {}

impl ConcurrentMergeScheduler {
    pub fn new() -> Self {
        ConcurrentMergeScheduler {}
    }
}

impl MergeScheduler for ConcurrentMergeScheduler {
    fn merge(
        &self,
        _writer: &mut IndexWriter,
        _trigger: MergerTrigger,
        _new_merges_found: bool,
    ) -> Result<()> {
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

struct MergeThread {
    writer: *mut IndexWriter,
    merge: OneMerge,
    thread_id: ThreadId,
}

impl MergeThread {
    fn new(writer: &mut IndexWriter, merge: OneMerge) -> Self {
        MergeThread {
            writer,
            merge,
            thread_id: thread::current().id(),
        }
    }
}

impl Eq for MergeThread {}

impl PartialEq for MergeThread {
    fn eq(&self, other: &MergeThread) -> bool {
        self.merge == other.merge
    }
}

impl Ord for MergeThread {
    fn cmp(&self, other: &Self) -> Ordering {
        // reverse order by merge_bytes
        other
            .merge
            .estimated_merge_bytes
            .cmp(&self.merge.estimated_merge_bytes)
    }
}

impl PartialOrd for MergeThread {
    fn partial_cmp(&self, other: &MergeThread) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Copy, Clone)]
pub struct SerialMergeScheduler;

impl MergeScheduler for SerialMergeScheduler {
    fn merge(
        &self,
        writer: &mut IndexWriter,
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

    fn close(&mut self) -> Result<()> {
        Ok(())
    }
}
