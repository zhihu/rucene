use core::index::bufferd_updates::FrozenBufferUpdates;
use core::index::doc_writer_delete_queue::DocumentsWriterDeleteQueue;
use core::index::index_writer::IndexWriter;
use core::index::thread_doc_writer::{DocumentsWriterPerThread, FlushedSegment};

use error::Result;
use std::collections::VecDeque;
use std::mem;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

pub struct DocumentsWriterFlushQueue {
    lock: Arc<Mutex<()>>,
    queue: VecDeque<FlushTicket>,
    // we track tickets separately since count must be present even before the ticket is
    // constructed ie. queue.size would not reflect it.
    ticket_count: AtomicU32,
    purge_lock: Arc<Mutex<()>>,
}

impl DocumentsWriterFlushQueue {
    pub fn new() -> Self {
        DocumentsWriterFlushQueue {
            lock: Arc::new(Mutex::new(())),
            queue: VecDeque::with_capacity(10000),
            ticket_count: AtomicU32::new(0),
            purge_lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn add_deletes(&mut self, delete_queue: &DocumentsWriterDeleteQueue) -> Result<()> {
        let lock = Arc::clone(&self.lock);
        let _l = lock.lock()?;
        // first inc the ticket count - freeze opens a window for any_change() to fail
        self.inc_tickets();

        match delete_queue.freeze_global_buffer(None) {
            Ok(frozen_updates) => {
                self.queue
                    .push_back(FlushTicket::Global(GlobalDeletesTicket::new(
                        frozen_updates,
                    )));
                Ok(())
            }
            Err(e) => {
                self.dec_tickets();
                Err(e)
            }
        }
    }

    pub fn has_tickets(&self) -> bool {
        self.ticket_count.load(Ordering::Acquire) > 0
    }

    fn inc_tickets(&self) {
        let count = self.ticket_count.fetch_add(1, Ordering::AcqRel);
    }

    fn dec_tickets(&self) {
        self.ticket_count.fetch_sub(1, Ordering::AcqRel);
    }

    pub fn add_flush_ticket(&mut self, ticket: SegmentFlushTicket) {
        let lock = Arc::clone(&self.lock);
        let _l = lock.lock().unwrap();
        self.inc_tickets();
        self.queue.push_back(FlushTicket::Segment(ticket));
    }

    fn add_segment(&self, ticket: &mut FlushTicket, segment: FlushedSegment) {
        let lock = Arc::clone(&self.lock);
        let _l = lock.lock().unwrap();
        match ticket {
            FlushTicket::Segment(s) => s.segment = Some(segment),
            _ => unreachable!(),
        }
    }

    pub fn force_purge(&mut self, writer: &mut IndexWriter) -> Result<u32> {
        let lock = Arc::clone(&self.lock);
        let l = lock.lock()?;
        self.inner_purge(writer, &l)
    }

    pub fn try_purge(&mut self, writer: &mut IndexWriter) -> Result<u32> {
        let purge_lock = Arc::clone(&self.purge_lock);
        let lock_res = purge_lock.try_lock();
        match lock_res {
            Ok(l) => {
                let lock = Arc::clone(&self.lock);
                let l = lock.lock()?;
                self.inner_purge(writer, &l)
            }
            _ => Ok(0),
        }
    }

    fn inner_purge(&mut self, writer: &mut IndexWriter, _l: &MutexGuard<()>) -> Result<u32> {
        let mut num_purged = 0u32;
        loop {
            let can_publish = if let Some(ref ft) = self.queue.front_mut() {
                ft.can_publish()
            } else {
                false
            };

            if can_publish {
                num_purged += 1;
                self.ticket_count.fetch_sub(1, Ordering::AcqRel);

                let mut head = self.queue.pop_front().unwrap();

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

trait IFlushTicket {
    fn publish(&mut self, writer: &mut IndexWriter) -> Result<()>;
    fn can_publish(&self) -> bool;

    /// Publishes the flushed segment, segment private deletes (if any) and its
    /// associated global delete (if present) to IndexWriter.  The actual
    /// publishing operation is synced on {@code IW -> BDS} so that the {@link SegmentInfo}'s
    /// delete generation is always GlobalPacket_deleteGeneration + 1
    fn publish_flushed_segment(
        &self,
        index_writer: &mut IndexWriter,
        new_segment: FlushedSegment,
        global_packet: Option<FrozenBufferUpdates>,
    ) -> Result<()> {
        debug!(
            "publish_flush_segment seg-private update={:?}",
            new_segment
                .segment_updates
                .as_ref()
                .map(|su| format!("{}", su))
        );

        if let Some(ref updates) = new_segment.segment_updates {
            debug!("flush: push buffered seg private updates: {}", updates);
        }
        // now publish!
        index_writer.publish_flushed_segment(new_segment, global_packet)
    }

    fn finish_flush(
        &self,
        index_writer: &mut IndexWriter,
        new_segment: Option<FlushedSegment>,
        buffered_update: Option<FrozenBufferUpdates>,
    ) -> Result<()> {
        // Finish the flushed segment and publish it to IndexWriter
        if let Some(segment) = new_segment {
            self.publish_flushed_segment(index_writer, segment, buffered_update)?;
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

pub struct GlobalDeletesTicket {
    frozen_updates: Option<FrozenBufferUpdates>,
    published: bool,
}

impl GlobalDeletesTicket {
    pub fn new(frozen_updates: FrozenBufferUpdates) -> Self {
        GlobalDeletesTicket {
            frozen_updates: Some(frozen_updates),
            published: false,
        }
    }
}

impl IFlushTicket for GlobalDeletesTicket {
    fn publish(&mut self, writer: &mut IndexWriter) -> Result<()> {
        debug_assert!(!self.published);
        let fu = mem::replace(&mut self.frozen_updates, None);
        self.published = true;
        self.finish_flush(writer, None, fu)
    }

    fn can_publish(&self) -> bool {
        true
    }
}

pub struct SegmentFlushTicket {
    frozen_updates: Option<FrozenBufferUpdates>,
    published: bool,
    segment: Option<FlushedSegment>,
    failed: bool,
}

impl SegmentFlushTicket {
    pub fn new(frozen_updates: FrozenBufferUpdates) -> Self {
        SegmentFlushTicket {
            frozen_updates: Some(frozen_updates),
            published: false,
            segment: None,
            failed: false,
        }
    }

    pub fn set_segment(&mut self, segment: Option<FlushedSegment>) {
        debug_assert!(!self.failed);
        self.segment = segment;
    }

    pub fn set_failed(&mut self) {
        debug_assert!(!self.failed);
        self.failed = true;
    }
}

impl IFlushTicket for SegmentFlushTicket {
    fn publish(&mut self, writer: &mut IndexWriter) -> Result<()> {
        debug_assert!(!self.published);
        self.published = true;
        let segment = mem::replace(&mut self.segment, None);
        let frozen = mem::replace(&mut self.frozen_updates, None);

        self.finish_flush(writer, segment, frozen)
    }

    fn can_publish(&self) -> bool {
        self.segment.is_some() || self.failed
    }
}

pub enum FlushTicket {
    Global(GlobalDeletesTicket),
    Segment(SegmentFlushTicket),
}

impl FlushTicket {
    pub fn set_segment(&mut self, segment: Option<FlushedSegment>) {
        match self {
            FlushTicket::Segment(s) => {
                s.segment = segment;
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

impl IFlushTicket for FlushTicket {
    fn publish(&mut self, writer: &mut IndexWriter) -> Result<()> {
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
