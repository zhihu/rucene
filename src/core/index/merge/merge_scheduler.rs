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
use core::index;
use core::index::merge::{MergePolicy, MergerTrigger, OneMerge, OneMergeScheduleInfo};
use core::index::writer::IndexWriter;
use core::store::directory::Directory;
use core::store::RateLimiter;

use error::{Error, ErrorKind, Result};

use num_cpus;

use std::cmp::Ordering;
use std::f64;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, Weak};
use std::thread::{self, ThreadId};
use std::time::{Duration, SystemTime};

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
    fn merge<D, C, MP>(
        &self,
        writer: &IndexWriter<D, C, Self, MP>,
        trigger: MergerTrigger,
        new_merges_found: bool,
    ) -> Result<()>
    where
        D: Directory + Send + Sync + 'static,
        C: Codec,
        MP: MergePolicy;

    fn close(&self) -> Result<()>;

    fn merging_thread_count(&self) -> Option<usize> {
        None
    }
}

#[derive(Copy, Clone)]
pub struct SerialMergeScheduler;

impl MergeScheduler for SerialMergeScheduler {
    fn merge<D, C, MP>(
        &self,
        writer: &IndexWriter<D, C, Self, MP>,
        _trigger: MergerTrigger,
        _new_merges_found: bool,
    ) -> Result<()>
    where
        D: Directory + Send + Sync + 'static,
        C: Codec,
        MP: MergePolicy,
    {
        while let Some(ref mut merge) = writer.next_merge() {
            writer.merge(merge)?;
        }
        Ok(())
    }

    fn close(&self) -> Result<()> {
        Ok(())
    }
}

struct ThreadSentinel;

struct MergeTaskInfo {
    merge: OneMergeScheduleInfo,
    thread_id: ThreadId,
    live_sentinel: Weak<ThreadSentinel>,
}

impl MergeTaskInfo {
    fn thread_alive(&self) -> bool {
        self.live_sentinel.upgrade().is_some()
    }
}

impl Eq for MergeTaskInfo {}

impl PartialEq for MergeTaskInfo {
    fn eq(&self, other: &Self) -> bool {
        self.merge.id == other.merge.id
    }
}

impl PartialOrd for MergeTaskInfo {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // reverse order
        other
            .merge
            .estimated_merge_bytes
            .read()
            .partial_cmp(&self.merge.estimated_merge_bytes.read())
    }
}

impl Ord for MergeTaskInfo {
    fn cmp(&self, other: &Self) -> Ordering {
        // reverse order
        other
            .merge
            .estimated_merge_bytes
            .read()
            .cmp(&self.merge.estimated_merge_bytes.read())
    }
}

/// A `MergeScheduler` that runs each merge using a separate thread.
///
/// Specify the max number of threads that may run at once, and the maximum number
/// of simultaneous merges with *max_merge_thread*
///
/// If the number of merges exceeds the max number of threads then the largest merges
/// are paused until one of the smaller merges completes.
///
/// If more than *max_merge_count* merges are requested then this class will forcefully
/// throttle the incoming threads by pausing until one more more merges complete.
#[derive(Clone)]
pub struct ConcurrentMergeScheduler {
    inner: Arc<ConcurrentMergeSchedulerInner>,
}

impl Default for ConcurrentMergeScheduler {
    fn default() -> Self {
        let max_thread_count = 3.min(num_cpus::get() / 2);
        Self::new(max_thread_count)
    }
}

impl ConcurrentMergeScheduler {
    pub fn new(max_thread_count: usize) -> Self {
        if max_thread_count == 0 {
            panic!("max thread count must not be 0");
        }
        Self {
            inner: Arc::new(ConcurrentMergeSchedulerInner::new(max_thread_count)),
        }
    }
}

struct ConcurrentMergeSchedulerInner {
    lock: Mutex<()>,
    cond: Condvar,
    merge_tasks: Vec<MergeTaskInfo>,
    max_merge_count: usize,
    max_thread_count: usize,
    merge_thread_count: usize,
    target_mb_per_sec: f64,
    do_auto_io_throttle: bool,
    force_merge_mb_per_sec: f64,
}

// Floor for IO write rate limit (we will never go any lower than this)
const MIN_MERGE_MB_PER_SEC: f64 = 5.0;

// Ceiling for IO write rate limit (we will never go any higher than this)
const MAX_MERGE_MB_PER_SEC: f64 = 10240.0;

// Initial value for IO write rate limit when do_auto_io_throttle is true
#[allow(dead_code)]
const START_MB_PER_SEC: f64 = 20.0;

// Merges below this size are not counted in the maxThreadCount, i.e. they can freely run in
// their own thread (up until maxMergeCount).
const MIN_BIG_MERGE_MB: f64 = 50.0;

pub const MAX_MERGING_COUNT: usize = 5;

impl ConcurrentMergeSchedulerInner {
    fn new(max_thread_count: usize) -> Self {
        ConcurrentMergeSchedulerInner {
            lock: Mutex::new(()),
            cond: Condvar::new(),
            merge_tasks: vec![],
            max_merge_count: max_thread_count.max(MAX_MERGING_COUNT),
            max_thread_count,
            merge_thread_count: 0,
            target_mb_per_sec: START_MB_PER_SEC,
            do_auto_io_throttle: true,
            force_merge_mb_per_sec: f64::INFINITY,
        }
    }

    #[allow(clippy::mut_from_ref)]
    unsafe fn scheduler_mut(&self, _guard: &MutexGuard<()>) -> &mut ConcurrentMergeSchedulerInner {
        let scheduler =
            self as *const ConcurrentMergeSchedulerInner as *mut ConcurrentMergeSchedulerInner;
        &mut *scheduler
    }

    fn maybe_stall<'a, D, C, MP>(
        &self,
        writer: &IndexWriter<D, C, ConcurrentMergeScheduler, MP>,
        guard: MutexGuard<'a, ()>,
    ) -> (bool, MutexGuard<'a, ()>)
    where
        D: Directory + Send + Sync + 'static,
        C: Codec,
        MP: MergePolicy,
    {
        let thread_id = thread::current().id();
        let mut guard = guard;
        while writer.has_pending_merges() && self.merge_thread_count() >= self.max_merge_count {
            // This means merging has fallen too far behind: we
            // have already created maxMergeCount threads, and
            // now there's at least one more merge pending.
            // Note that only maxThreadCount of
            // those created merge threads will actually be
            // running; the rest will be paused (see
            // updateMergeThreads).  We stall this producer
            // thread to prevent creation of new segments,
            // until merging has caught up:
            if self.merge_tasks.iter().any(|t| t.thread_id == thread_id) {
                // Never stall a merge thread since this blocks the thread from
                // finishing and calling updateMergeThreads, and blocking it
                // accomplishes nothing anyway (it's not really a segment producer):
                return (false, guard);
            }

            // Defensively wait for only .25 seconds in case we are missing a .notify/All somewhere:
            let (g, _) = self
                .cond
                .wait_timeout(guard, Duration::from_millis(25))
                .unwrap();
            guard = g;
        }
        (true, guard)
    }

    fn update_merge_threads(&mut self) {
        let mut active_tasks: Vec<_> = self.merge_tasks.iter().collect();
        active_tasks.sort();

        let tasks_count = active_tasks.len();

        let mut big_merge_count = 0;
        for i in 0..tasks_count {
            if active_tasks[tasks_count - 1 - i]
                .merge
                .estimated_merge_bytes
                .read() as f64
                > MIN_BIG_MERGE_MB * 1024.0 * 1024.0
            {
                big_merge_count = tasks_count - i;
                break;
            }
        }

        for (idx, task) in active_tasks.iter().enumerate() {
            // pause the thread if max_thread_count is smaller than the number of merge threads.
            let do_pause = idx + self.max_thread_count < big_merge_count;

            let new_mb_per_sec = if do_pause {
                0.0
            } else if task.merge.max_num_segments.get().is_some() {
                self.force_merge_mb_per_sec
            } else if !self.do_auto_io_throttle
                || ((task.merge.estimated_merge_bytes.read() as f64)
                    < MIN_BIG_MERGE_MB * 1024.0 * 1024.0)
            {
                f64::INFINITY
            } else {
                self.target_mb_per_sec
            };

            task.merge.rate_limiter.set_mb_per_sec(new_mb_per_sec);
        }
    }

    fn merge_thread_count(&self) -> usize {
        let current_thread = thread::current().id();
        self.merge_tasks
            .iter()
            .filter(|t| {
                t.thread_id != current_thread && t.thread_alive() && !t.merge.rate_limiter.aborted()
            })
            .count()
    }

    fn update_io_throttle<D: Directory + Send + Sync + 'static, C: Codec>(
        &mut self,
        new_merge: &OneMerge<D, C>,
    ) {
        if !self.do_auto_io_throttle {
            return;
        }

        let merge_mb = bytes_to_mb(new_merge.estimated_merge_bytes.read());
        if merge_mb < MIN_BIG_MERGE_MB {
            // Only watch non-trivial merges for throttling; this is safe because the MP must
            // eventually have to do larger merges:
            return;
        }

        let now = SystemTime::now();
        // Simplistic closed-loop feedback control: if we find any other similarly
        // sized merges running, then we are falling behind, so we bump up the
        // IO throttle, else we lower it:
        let new_back_log =
            self.is_back_log(now, new_merge.id, new_merge.estimated_merge_bytes.read());

        let mut cur_back_log = false;
        if !new_back_log {
            if self.merge_tasks.len() > self.max_thread_count {
                // If there are already more than the maximum merge threads allowed, count that as
                // backlog:
                cur_back_log = true;
            } else {
                // Now see if any still-running merges are backlog'd:
                for task in &self.merge_tasks {
                    if self.is_back_log(now, task.merge.id, task.merge.estimated_merge_bytes.read())
                    {
                        cur_back_log = true;
                        break;
                    }
                }
            }
        }

        if new_back_log {
            // This new merge adds to the backlog: increase IO throttle by 20%
            self.target_mb_per_sec = MAX_MERGE_MB_PER_SEC.min(self.target_mb_per_sec * 1.2);
        } else if cur_back_log {
            // We still have an existing backlog; leave the rate as is:
        } else {
            // We are not falling behind: decrease IO throttle by 10%
            self.target_mb_per_sec = MIN_MERGE_MB_PER_SEC.max(self.target_mb_per_sec / 1.1);
        }

        let rate = if new_merge.max_num_segments.get().is_some() {
            self.force_merge_mb_per_sec
        } else {
            self.target_mb_per_sec
        };
        new_merge.rate_limiter.set_mb_per_sec(rate);
    }

    fn is_back_log(&self, now: SystemTime, merge_id: u32, estimated_merge_bytes: u64) -> bool {
        let merge_mb = bytes_to_mb(estimated_merge_bytes);
        for task in &self.merge_tasks {
            let merge_start = task.merge.merge_start_time.read();
            if let Some(start_time) = merge_start {
                if task.merge.id != merge_id
                    && task.merge.estimated_merge_bytes.read() as f64
                        >= MIN_BIG_MERGE_MB * 1024.0 * 1024.0
                    && now > start_time
                    && now.duration_since(start_time).unwrap() > Duration::from_secs(3)
                {
                    let other_merge_mb = bytes_to_mb(task.merge.estimated_merge_bytes.read());
                    let ratio = other_merge_mb / merge_mb;
                    if ratio > 0.3 && ratio < 3.0 {
                        return true;
                    }
                }
            }
        }

        false
    }
}

impl MergeScheduler for ConcurrentMergeScheduler {
    fn merge<D, C, MP>(
        &self,
        writer: &IndexWriter<D, C, Self, MP>,
        trigger: MergerTrigger,
        _new_merges_found: bool,
    ) -> Result<()>
    where
        D: Directory + Send + Sync + 'static,
        C: Codec,
        MP: MergePolicy,
    {
        let mut guard = self.inner.lock.lock().unwrap();
        let scheduler = unsafe { self.inner.scheduler_mut(&guard) };

        if trigger == MergerTrigger::Closing {
            // Disable throttling on close:
            scheduler.target_mb_per_sec = MAX_MERGE_MB_PER_SEC;
            scheduler.update_merge_threads();
        }

        // First, quickly run through the newly proposed merges
        // and add any orthogonal merges (ie a merge not
        // involving segments already pending to be merged) to
        // the queue.  If we are way behind on merging, many of
        // these newly proposed merges will likely already be
        // registered.

        loop {
            let (valid, g) = scheduler.maybe_stall(writer, guard);
            guard = g;
            if !valid {
                break;
            }

            if let Some(merge) = writer.next_merge() {
                scheduler.update_io_throttle(&merge);

                let sentinel = Arc::new(ThreadSentinel);
                let live_sentinel = Arc::downgrade(&sentinel);
                let merge_thread = MergeThread {
                    index_writer: writer.clone(),
                    merge_scheduler: self.clone(),
                    _live_sentinel: sentinel,
                };
                let merge_info = merge.schedule_info();
                let handler = thread::Builder::new()
                    .name(format!(
                        "Rucene Merge Thread #{}",
                        scheduler.merge_thread_count
                    ))
                    .spawn(move || {
                        merge_thread.merge(merge);
                    })
                    .expect("failed to spawn thread");
                scheduler.merge_thread_count += 1;

                let merge_task = MergeTaskInfo {
                    merge: merge_info,
                    thread_id: handler.thread().id(),
                    live_sentinel,
                };
                scheduler.merge_tasks.push(merge_task);
                scheduler.update_merge_threads();
            } else {
                return Ok(());
            }
        }
        Ok(())
    }

    fn close(&self) -> Result<()> {
        // do nothing here
        // this is safe because each MergeThread contain a IndexWriter make sure that
        // IndexWrite live long enough before all the threads finish running
        Ok(())
    }

    fn merging_thread_count(&self) -> Option<usize> {
        Some(self.inner.merge_thread_count())
    }
}

fn bytes_to_mb(bytes: u64) -> f64 {
    bytes as f64 / 1024.0 / 1024.0
}

struct MergeThread<D: Directory + Send + Sync + 'static, C: Codec, MP: MergePolicy> {
    index_writer: IndexWriter<D, C, ConcurrentMergeScheduler, MP>,
    // a sentinel object used by MergeTaskInfo to determine whether target thread is alive
    _live_sentinel: Arc<ThreadSentinel>,
    merge_scheduler: ConcurrentMergeScheduler,
}

impl<D: Directory + Send + Sync + 'static, C: Codec, MP: MergePolicy> MergeThread<D, C, MP> {
    fn merge(&self, mut one_merge: OneMerge<D, C>) {
        match self.do_merge(&mut one_merge) {
            Err(Error(ErrorKind::Index(index::ErrorKind::MergeAborted(_)), _)) => {
                // OK to ignore
            }
            Err(e) => {
                error!(
                    "merge {:?} failed: {:?}, thread will abort immediately",
                    one_merge.segments, e
                );
            }
            Ok(()) => {}
        }
        let l = self.merge_scheduler.inner.lock.lock().unwrap();
        let scheduler_mut = unsafe { self.merge_scheduler.inner.scheduler_mut(&l) };
        scheduler_mut
            .merge_tasks
            .drain_filter(|t| t.merge.id == one_merge.id);
        scheduler_mut.update_merge_threads();
        // In case we had stalled indexing, we can now wake up
        // and possibly unstall:
        scheduler_mut.cond.notify_all();
    }

    fn do_merge(&self, merge: &mut OneMerge<D, C>) -> Result<()> {
        self.index_writer.merge(merge)?;

        // Let CMS run new merges if necessary
        match self
            .merge_scheduler
            .merge(&self.index_writer, MergerTrigger::MergeFinished, true)
        {
            Err(Error(ErrorKind::AlreadyClosed(_), _)) => Ok(()),
            Err(e) => Err(e),
            Ok(()) => Ok(()),
        }
    }
}
