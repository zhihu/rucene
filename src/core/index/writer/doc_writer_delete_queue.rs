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
use core::doc::Term;
use core::index::writer::{BufferedUpdates, DocValuesUpdate, FrozenBufferedUpdates};
use core::search::{query::Query, NO_MORE_DOCS};
use core::util::DocId;

use crossbeam::utils::Backoff;
use std::cell::Cell;
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// `DocumentsWriterDeleteQueue` is a non-blocking linked pending deletes
/// queue. In contrast to other queue implementation we only maintain the
/// tail of the queue. A delete queue is always used in a context of a set of
/// DWPTs and a global delete pool. Each of the DWPT and the global pool need to
/// maintain their 'own' head of the queue (as a DeleteSlice instance per
/// `DocumentsWriterPerThread`).
/// The difference between the DWPT and the global pool is that the DWPT starts
/// maintaining a head once it has added its first document since for its segments
/// private deletes only the deletes after that document are relevant. The global
/// pool instead starts maintaining the head once this instance is created by
/// taking the sentinel instance as its initial head.
///
/// Since each `DeleteSlice` maintains its own head and the list is only
/// single linked the garbage collector takes care of pruning the list for us.
/// All nodes in the list that are still relevant should be either directly or
/// indirectly referenced by one of the DWPT's private `DeleteSlice` or by
/// the global `BufferedUpdates` slice.
///
/// Each DWPT as well as the global delete pool maintain their private
/// DeleteSlice instance. In the DWPT case updating a slice is equivalent to
/// atomically finishing the document. The slice update guarantees a "happens
/// before" relationship to all other updates in the same indexing session. When a
/// DWPT updates a document it:
///
/// - consumes a document and finishes its processing
/// - updates its private `DeleteSlice} either by calling `update_slice(DeleteSlice)` or `add(Term,
///   DeleteSlice)` (if the document has a delTerm)
/// - applies all deletes in the slice to its private `BufferedUpdates` and resets it
/// - increments its internal document id
///
/// The DWPT also doesn't apply its current documents delete term until it has
/// updated its delete slice which ensures the consistency of the update. If the
/// update fails before the DeleteSlice could have been updated the deleteTerm
/// will also not be added to its private deletes neither to the global deletes.
pub struct DocumentsWriterDeleteQueue<C: Codec> {
    // current end(latest delete operation) in the delete queue:
    tail: Mutex<Arc<DeleteListNode<C>>>,
    // Used to record deletes against all prior (already written to disk) segments.
    // Whenever any segment flushes, we bundle up this set of deletes and insert
    // into the buffered updates stream before the newly flushed segment(s).
    global_data: Mutex<GlobalData<C>>,
    pub generation: u64,
    next_seq_no: AtomicU64,
    pub max_seq_no: Cell<u64>,
}

struct GlobalData<C: Codec> {
    global_slice: DeleteSlice<C>,
    global_buffered_updates: BufferedUpdates<C>,
}

impl<C: Codec> GlobalData<C> {
    fn apply_global_updates(&mut self, doc_upto: DocId) {
        self.global_slice
            .apply(&mut self.global_buffered_updates, doc_upto);
    }
}

impl<C: Codec> Default for DocumentsWriterDeleteQueue<C> {
    fn default() -> Self {
        // seq_no must start at 1 because some APIs negate this to also return a boolean
        Self::with_generation(0, 1)
    }
}

impl<C: Codec> DocumentsWriterDeleteQueue<C> {
    pub fn with_generation(generation: u64, start_seq_no: u64) -> Self {
        Self::new(
            BufferedUpdates::new("global".into()),
            generation,
            start_seq_no,
        )
    }
    pub fn new(
        global_buffered_updates: BufferedUpdates<C>,
        generation: u64,
        start_seq_no: u64,
    ) -> Self {
        // we use a sentinel instance as our initial tail. No slice will ever try to
        // apply this tail since the head is always omitted
        let tail = Arc::new(DeleteListNode::default());
        let global_slice = DeleteSlice::new(&tail);
        let global_data = GlobalData {
            global_buffered_updates,
            global_slice,
        };
        Self {
            tail: Mutex::new(tail),
            global_data: Mutex::new(global_data),
            generation,
            next_seq_no: AtomicU64::new(start_seq_no),
            max_seq_no: Cell::new(i64::max_value() as u64),
        }
    }

    pub fn add_delete_queries(&self, queries: Vec<Arc<dyn Query<C>>>) -> u64 {
        let node = Arc::new(DeleteListNode::new(DeleteNode::QueryArray(queries)));
        let seq_no = self.add_node(node);
        self.try_apply_global_slice();
        seq_no
    }

    pub fn add_delete_terms(&self, terms: Vec<Term>) -> u64 {
        let node = Arc::new(DeleteListNode::new(DeleteNode::TermArray(terms)));
        let seq_no = self.add_node(node);
        self.try_apply_global_slice();
        seq_no
    }

    pub fn add_doc_values_update(&self, update: Arc<dyn DocValuesUpdate>) -> u64 {
        let node = Arc::new(DeleteListNode::new(DeleteNode::DocValuesUpdate(update)));
        let seq_no = self.add_node(node);
        self.try_apply_global_slice();
        seq_no
    }

    /// invariant for document update
    pub fn add_term_to_slice(&self, term: Term, slice: &mut DeleteSlice<C>) -> u64 {
        let del_node = Arc::new(DeleteListNode::new(DeleteNode::Term(term)));
        let seq_no = self.add_node(del_node.clone());
        // this is an update request where the term is the updated documents
        // delTerm. in that case we need to guarantee that this insert is atomic
        // with regards to the given delete slice. This means if two threads try to
        // update the same document with in turn the same delTerm one of them must
        // win. By taking the node we have created for our del term as the new tail
        // it is guaranteed that if another thread adds the same right after us we
        // will apply this delete next time we update our slice and one of the two
        // competing updates wins!
        slice.slice_tail = del_node;
        debug_assert!(!same_node(&slice.slice_head, &slice.slice_tail));
        self.try_apply_global_slice(); // TODO doing this each time is not necessary maybe
                                       // we can do it just every n times or so?
        seq_no
    }

    // the same logic as std::sync::mpsc::mpsc_queue::Queue#push()
    //
    // NOTE: the add does not always guarantee that head and tail are in the same list,
    // so DeleteListNode::get_next() method must need retry when next is null.
    fn add_node(&self, node: Arc<DeleteListNode<C>>) -> u64 {
        let mut tail = self.tail.lock().unwrap();
        debug_assert!(tail.next.load(Ordering::Acquire).is_null());
        tail.next
            .store(Box::into_raw(Box::new(node.clone())), Ordering::Release);
        *tail = node;
        self.next_sequence_number()
    }

    pub fn any_changes(&self) -> bool {
        let guard = self.global_data.lock().unwrap();
        let tail_node = self.tail.lock().unwrap();
        let next_node = tail_node.next.load(Ordering::Acquire);
        guard.global_buffered_updates.any()
            || !guard.global_slice.is_empty()
            || !same_node(&guard.global_slice.slice_tail, &tail_node)
            || !next_node.is_null()
    }

    pub fn next_sequence_number(&self) -> u64 {
        let no = self.next_seq_no.fetch_add(1, Ordering::AcqRel);
        debug_assert!(no < self.max_seq_no.get());
        no
    }

    fn try_apply_global_slice(&self) {
        if let Ok(mut guard) = self.global_data.try_lock() {
            // The global buffer must be locked but we don't need to update them if
            // there is an update going on right now. It is sufficient to apply the
            // deletes that have been added after the current in-flight global slices
            // tail the next time we can get the lock!
            if self.update_slice_without_seq_no(&mut guard.global_slice) {
                guard.apply_global_updates(NO_MORE_DOCS);
            }
        }
    }

    fn update_slice_without_seq_no(&self, slice: &mut DeleteSlice<C>) -> bool {
        let tail = self.tail.lock().unwrap();
        if !same_node(&slice.slice_tail, &tail) {
            slice.slice_tail = tail.clone();
            true
        } else {
            false
        }
    }

    pub fn freeze_global_buffer(
        &self,
        caller_slice: Option<&mut DeleteSlice<C>>,
    ) -> FrozenBufferedUpdates<C> {
        let mut global_guard = self.global_data.lock().unwrap();
        // Here we freeze the global buffer so we need to lock it, apply all deletes in the
        // queue and reset the global slice to let the GC prune the queue
        let current_tail = self.tail.lock().unwrap();

        if let Some(slice) = caller_slice {
            if !same_node(&current_tail, &slice.slice_tail) {
                slice.slice_tail = current_tail.clone();
            }
        }

        if !same_node(&global_guard.global_slice.slice_tail, &current_tail) {
            global_guard.global_slice.slice_tail = current_tail.clone();
            global_guard.apply_global_updates(NO_MORE_DOCS);
        }
        let packet = FrozenBufferedUpdates::new(&mut global_guard.global_buffered_updates, false);
        global_guard.global_buffered_updates.clear();
        packet
    }

    pub fn new_slice(&self) -> DeleteSlice<C> {
        DeleteSlice::new(&self.tail.lock().unwrap())
    }

    pub fn update_slice(&self, slice: &mut DeleteSlice<C>) -> (u64, bool) {
        let seq_no = self.next_sequence_number();
        let tail = self.tail.lock().unwrap();
        if !same_node(&tail, &slice.slice_tail) {
            // new deletes arrived since we last checked
            slice.slice_tail = tail.clone();
            (seq_no, true)
        } else {
            (seq_no, false)
        }
    }

    pub fn num_global_term_deletes(&self) -> usize {
        self.global_data
            .lock()
            .unwrap()
            .global_buffered_updates
            .num_term_deletes
            .load(Ordering::Acquire)
    }

    pub fn clear(&self) {
        let mut guard = self.global_data.lock().unwrap();
        let current_tail = self.tail.lock().unwrap();
        guard.global_slice.slice_head = current_tail.clone();
        guard.global_slice.slice_tail = current_tail.clone();
        guard.global_buffered_updates.clear();
    }

    pub fn last_sequence_number(&self) -> u64 {
        self.next_seq_no.load(Ordering::Acquire) - 1
    }

    pub fn skip_sequence_number(&self, jump: u64) {
        self.next_seq_no.fetch_add(jump, Ordering::AcqRel);
    }
}

enum DeleteNode<C: Codec> {
    Term(Term),
    TermArray(Vec<Term>),
    QueryArray(Vec<Arc<dyn Query<C>>>),
    DocValuesUpdate(Arc<dyn DocValuesUpdate>),
    // used for sentinel head
    None,
}

impl<C: Codec> DeleteNode<C> {
    fn apply(&self, buffered_deletes: &mut BufferedUpdates<C>, doc_id_upto: DocId) {
        match self {
            DeleteNode::Term(t) => {
                buffered_deletes.add_term(t.clone(), doc_id_upto);
            }
            DeleteNode::TermArray(terms) => {
                for t in terms {
                    buffered_deletes.add_term(t.clone(), doc_id_upto);
                }
            }
            DeleteNode::QueryArray(queries) => {
                for q in queries {
                    buffered_deletes.add_query(q.clone(), doc_id_upto);
                }
            }
            DeleteNode::DocValuesUpdate(update) => {
                buffered_deletes.add_doc_values_update(update.clone(), doc_id_upto);
            }
            DeleteNode::None => {
                unreachable!();
            }
        }
    }
}

struct DeleteListNode<C: Codec> {
    data: DeleteNode<C>,
    next: AtomicPtr<Arc<DeleteListNode<C>>>,
}

impl<C: Codec> Default for DeleteListNode<C> {
    fn default() -> Self {
        Self::new(DeleteNode::None)
    }
}

impl<C: Codec> DeleteListNode<C> {
    fn new(data: DeleteNode<C>) -> Self {
        DeleteListNode {
            data,
            next: AtomicPtr::default(),
        }
    }
}

#[inline]
fn same_node<C: Codec>(n1: &Arc<DeleteListNode<C>>, n2: &Arc<DeleteListNode<C>>) -> bool {
    Arc::ptr_eq(n1, n2)
}

impl<C: Codec> DeleteListNode<C> {
    // because the DWDQ#add_node method need 2 atomic operation to complete,
    // so when logically we need to get a node but the operation is not finished,
    // we will wait for it.
    fn get_next(&self) -> &Arc<DeleteListNode<C>> {
        let backoff = Backoff::new();
        loop {
            let ptr = self.next.load(Ordering::Acquire);
            if !ptr.is_null() {
                return unsafe { &*ptr };
            }
            backoff.snooze();
        }
    }
}

impl<C: Codec> Drop for DeleteListNode<C> {
    fn drop(&mut self) {
        unsafe {
            let mut next = self.next.load(Ordering::Acquire);
            while !next.is_null() {
                let next2 = (*next).next.load(Ordering::Acquire);

                if Arc::strong_count(&(*next)) <= 1 {
                    Arc::get_mut(&mut *next).unwrap().next = AtomicPtr::default();

                    Box::from_raw(next);
                    next = next2;
                } else {
                    Box::from_raw(next);
                    break;
                }
            }
        }
    }
}

pub struct DeleteSlice<C: Codec> {
    // No need to be volatile, slices are thread captive (only accessed by one thread)!
    slice_head: Arc<DeleteListNode<C>>,
    slice_tail: Arc<DeleteListNode<C>>,
}

impl<C: Codec> DeleteSlice<C> {
    fn new(tail: &Arc<DeleteListNode<C>>) -> Self {
        let slice_head = tail.clone();
        let slice_tail = tail.clone();

        DeleteSlice {
            slice_head,
            slice_tail,
        }
    }

    pub fn apply(&mut self, buffered_deletes: &mut BufferedUpdates<C>, doc_id_upto: DocId) {
        if same_node(&self.slice_head, &self.slice_tail) {
            // 0 length slice
            return;
        }

        // When we apply a slice we take the head and get its next as our first
        // item to apply and continue until we applied the tail. If the head and
        // tail in this slice are not equal then there will be at least one more
        // non-null node in the slice!
        {
            let mut current = &self.slice_head;
            loop {
                current = current.get_next();
                current.data.apply(buffered_deletes, doc_id_upto);
                if same_node(current, &self.slice_tail) {
                    break;
                }
            }
        }
        self.reset();
    }

    pub fn reset(&mut self) {
        // Reset to a 0 length slice
        self.slice_head = self.slice_tail.clone();
    }

    pub fn is_empty(&self) -> bool {
        same_node(&self.slice_head, &self.slice_tail)
    }
}
