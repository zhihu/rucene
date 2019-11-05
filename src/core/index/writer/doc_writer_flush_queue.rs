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
use core::index::merge::MergePolicy;
use core::index::merge::MergeScheduler;
use core::index::writer::DocumentsWriterDeleteQueue;
use core::index::writer::FrozenBufferedUpdates;
use core::index::writer::IndexWriter;
use core::index::writer::{DocumentsWriterPerThread, FlushedSegment};
use core::store::directory::Directory;

use error::Result;

use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Mutex, MutexGuard};

pub struct DocumentsWriterFlushQueue<D: Directory, C: Codec> {
    queue: Mutex<VecDeque<FlushTicket<D, C>>>,
    // we track tickets separately since count must be present even before the ticket is
    // constructed ie. queue.size would not reflect it.
    ticket_count: AtomicU32,
    purge_lock: Mutex<()>,
}

impl<D: Directory + Send + Sync + 'static, C: Codec> DocumentsWriterFlushQueue<D, C> {
    pub fn new() -> Self {
        DocumentsWriterFlushQueue {
            queue: Mutex::new(VecDeque::with_capacity(10000)),
            ticket_count: AtomicU32::new(0),
            purge_lock: Mutex::new(()),
        }
    }

    pub fn add_deletes(&self, delete_queue: &DocumentsWriterDeleteQueue<C>) {
        let mut queue = self.queue.lock().unwrap();
        // first inc the ticket count - freeze opens a window for any_change() to fail
        self.inc_tickets();

        let frozen_updates = delete_queue.freeze_global_buffer(None);
        queue.push_back(FlushTicket::Global(GlobalDeletesTicket::new(
            frozen_updates,
        )));
    }

    pub fn has_tickets(&self) -> bool {
        self.ticket_count.load(Ordering::Acquire) > 0
    }

    fn inc_tickets(&self) {
        self.ticket_count.fetch_add(1, Ordering::AcqRel);
    }

    fn dec_tickets(&self) {
        self.ticket_count.fetch_sub(1, Ordering::AcqRel);
    }

    pub fn ticket_count(&self) -> u32 {
        self.ticket_count.load(Ordering::Relaxed)
    }

    pub fn add_flush_ticket<MS: MergeScheduler, MP: MergePolicy>(
        &self,
        dwpt: &mut DocumentsWriterPerThread<D, C, MS, MP>,
    ) -> Result<*mut FlushTicket<D, C>> {
        let mut queue = self.queue.lock()?;
        self.inc_tickets();
        match dwpt.prepare_flush() {
            Ok(update) => {
                let ticket = SegmentFlushTicket::new(update);
                queue.push_back(FlushTicket::Segment(Box::new(ticket)));
                Ok(queue.back_mut().unwrap())
            }
            Err(e) => {
                self.dec_tickets();
                Err(e)
            }
        }
    }

    pub fn force_purge<MS: MergeScheduler, MP: MergePolicy>(
        &self,
        writer: &IndexWriter<D, C, MS, MP>,
    ) -> Result<u32> {
        let _lock = self.purge_lock.lock()?;
        self.inner_purge(writer)
    }

    pub fn try_purge<MS: MergeScheduler, MP: MergePolicy>(
        &self,
        writer: &IndexWriter<D, C, MS, MP>,
    ) -> Result<u32> {
        let lock_res = self.purge_lock.try_lock();
        match lock_res {
            Ok(_l) => self.inner_purge(writer),
            _ => Ok(0),
        }
    }

    fn inner_purge<MS: MergeScheduler, MP: MergePolicy>(
        &self,
        writer: &IndexWriter<D, C, MS, MP>,
    ) -> Result<u32> {
        let mut num_purged = 0u32;
        let mut queue: MutexGuard<VecDeque<FlushTicket<D, C>>> = self.queue.lock()?;
        loop {
            let can_publish = if let Some(ref ft) = queue.front_mut() {
                ft.can_publish()
            } else {
                false
            };

            if can_publish {
                num_purged += 1;
                self.ticket_count.fetch_sub(1, Ordering::AcqRel);

                let mut head = queue.pop_front().unwrap();

                // if we block on publish -> lock IW -> lock BufferedDeletes we don't block
                // concurrent segment flushes just because they want to append to the queue.
                // the downside is that we need to force a purge on fullFlush since ther could
                // be a ticket still in the queue.
                head.publish(writer)?;
            } else {
                break;
            }
        }
        Ok(num_purged)
    }
}

trait IFlushTicket<D: Directory + Send + Sync + 'static, C: Codec> {
    fn publish<MS: MergeScheduler, MP: MergePolicy>(
        &mut self,
        writer: &IndexWriter<D, C, MS, MP>,
    ) -> Result<()>;

    fn can_publish(&self) -> bool;

    fn finish_flush<MS: MergeScheduler, MP: MergePolicy>(
        &self,
        index_writer: &IndexWriter<D, C, MS, MP>,
        new_segment: Option<FlushedSegment<D, C>>,
        buffered_update: Option<FrozenBufferedUpdates<C>>,
    ) -> Result<()> {
        // Finish the flushed segment and publish it to IndexWriter
        if let Some(segment) = new_segment {
            debug!(
                "publish_flush_segment seg-private update={:?}",
                segment.segment_updates.as_ref().map(|su| format!("{}", su))
            );

            // Publishes the flushed segment, segment private deletes (if any) and its
            // associated global delete (if present) to IndexWriter.  The actual
            // publishing operation is synced on {@code IW -> BDS} so that the {@link
            // SegmentInfo}'s delete generation is always GlobalPacket_deleteGeneration + 1
            index_writer.publish_flushed_segment(segment, buffered_update)?;
        } else {
            debug_assert!(buffered_update.is_some());
            let update = buffered_update.unwrap();
            if update.any() {
                index_writer.publish_frozen_updates(update)?;
            }
        }
        Ok(())
    }
}

pub struct GlobalDeletesTicket<D: Directory, C: Codec> {
    frozen_updates: Option<FrozenBufferedUpdates<C>>,
    published: bool,
    _dir: PhantomData<D>,
    _codec: PhantomData<C>,
}

impl<D: Directory, C: Codec> GlobalDeletesTicket<D, C> {
    pub fn new(frozen_updates: FrozenBufferedUpdates<C>) -> Self {
        GlobalDeletesTicket {
            frozen_updates: Some(frozen_updates),
            published: false,
            _dir: PhantomData,
            _codec: PhantomData,
        }
    }
}

impl<D, C> IFlushTicket<D, C> for GlobalDeletesTicket<D, C>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
{
    fn publish<MS: MergeScheduler, MP: MergePolicy>(
        &mut self,
        writer: &IndexWriter<D, C, MS, MP>,
    ) -> Result<()> {
        debug_assert!(!self.published);
        let fu = self.frozen_updates.take();
        self.published = true;
        self.finish_flush(writer, None, fu)
    }

    fn can_publish(&self) -> bool {
        true
    }
}

pub struct SegmentFlushTicket<D: Directory, C: Codec> {
    frozen_updates: Option<FrozenBufferedUpdates<C>>,
    published: bool,
    segment: Option<FlushedSegment<D, C>>,
    failed: bool,
}

impl<D: Directory, C: Codec> SegmentFlushTicket<D, C> {
    pub fn new(frozen_updates: FrozenBufferedUpdates<C>) -> Self {
        SegmentFlushTicket {
            frozen_updates: Some(frozen_updates),
            published: false,
            segment: None,
            failed: false,
        }
    }

    pub fn set_segment(&mut self, segment: Option<FlushedSegment<D, C>>) {
        debug_assert!(!self.failed);
        self.segment = segment;
    }

    pub fn set_failed(&mut self) {
        debug_assert!(self.segment.is_none());
        self.failed = true;
    }
}

impl<D, C> IFlushTicket<D, C> for SegmentFlushTicket<D, C>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
{
    fn publish<MS: MergeScheduler, MP: MergePolicy>(
        &mut self,
        writer: &IndexWriter<D, C, MS, MP>,
    ) -> Result<()> {
        debug_assert!(!self.published);
        self.published = true;
        let segment = self.segment.take();
        let frozen = self.frozen_updates.take();

        self.finish_flush(writer, segment, frozen)
    }

    fn can_publish(&self) -> bool {
        self.segment.is_some() || self.failed
    }
}

pub enum FlushTicket<D: Directory, C: Codec> {
    Global(GlobalDeletesTicket<D, C>),
    Segment(Box<SegmentFlushTicket<D, C>>),
}

impl<D: Directory, C: Codec> FlushTicket<D, C> {
    pub fn set_segment(&mut self, segment: Option<FlushedSegment<D, C>>) {
        match self {
            FlushTicket::Segment(s) => {
                s.set_segment(segment);
            }
            _ => {
                unreachable!();
            }
        }
    }

    pub fn set_failed(&mut self) {
        match self {
            FlushTicket::Segment(ref mut s) => {
                s.set_failed();
            }
            _ => {
                unreachable!();
            }
        }
    }
}

impl<D, C> IFlushTicket<D, C> for FlushTicket<D, C>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
{
    fn publish<MS: MergeScheduler, MP: MergePolicy>(
        &mut self,
        writer: &IndexWriter<D, C, MS, MP>,
    ) -> Result<()> {
        match self {
            FlushTicket::Global(g) => g.publish(writer),
            FlushTicket::Segment(s) => s.publish(writer),
        }
    }

    fn can_publish(&self) -> bool {
        match self {
            FlushTicket::Global(g) => g.can_publish(),
            FlushTicket::Segment(s) => s.can_publish(),
        }
    }
}
