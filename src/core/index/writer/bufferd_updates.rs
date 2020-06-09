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

use core::codec::segment_infos::{SegmentCommitInfo, SegmentInfos};
use core::codec::PostingIteratorFlags;
use core::codec::{Codec, CodecPostingIterator, CodecTermIterator};
use core::codec::{Fields, SeekStatus, TermIterator, Terms};
use core::doc::{DocValuesType, Term};
use core::index::merge::MergePolicy;
use core::index::reader::{IndexReader, LeafReader};
use core::index::writer::{
    DocValuesUpdate, FieldTermIter, FieldTermIterator, MergedDocValuesUpdatesIterator,
    NumericDocValuesUpdate, PrefixCodedTerms, PrefixCodedTermsBuilder,
};
use core::index::writer::{ReaderPool, ReadersAndUpdates};
use core::search::cache::{NoCacheQueryCache, QueryCache};
use core::search::{query::Query, DocIterator, NO_MORE_DOCS};
use core::search::{DefaultIndexSearcher, IndexSearcher, SearchPlanBuilder};
use core::store::directory::Directory;
use core::store::IOContext;
use core::util::DocId;

use std::cmp::{min, Ordering as CmpOrdering};
use std::collections::{BinaryHeap, HashMap};
use std::fmt;
use std::mem;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;

use core::index::merge::MergeScheduler;
use error::Result;

// Rough logic: del docIDs are List<i32>.  Say list allocates ~2X size (2 * i32),
pub const BYTES_PER_DEL_DOCID: usize = 2 * mem::size_of::<DocId>();

/// Rough logic: hash-map has an array<index> varying load factor (say 2 * usize).
/// Term is object with two Vec(String is actual a Vec), each Vec cost 2 * usize (cap/size) +
/// vec.capasity * byte
pub const BYTES_PER_DEL_TERM: usize = 6 * mem::size_of::<usize>();

/// Rough logic: HashMap has an array<index> and array<entry> varying load factor
/// say (2 * pointer). Entry is (String, (Box<dyn Query>, i32)), String cost 2 * usize +
/// string.len()
/// * byte, Query we offen undercount (say 24 bytes). and we will ignore the empty occupied of hash
/// entry costs.
pub const BYTES_PER_DEL_QUERY_IN_HASH: usize = 4 * mem::size_of::<usize>() + 28;

/// Holds buffered deletes and updates, by docID, term or query for a
/// single segment. This is used to hold buffered pending
/// deletes and updates against the to-be-flushed segment.  Once the
/// deletes and updates are pushed (on flush in DocumentsWriter), they
/// are converted to a FrozenBufferedUpdates instance. */
///
/// NOTE: instances of this class are accessed either via a private
/// instance on DocumentWriterPerThread, or via sync'd code by
/// DocumentsWriterDeleteQueue
pub struct BufferedUpdates<C: Codec> {
    pub num_term_deletes: AtomicUsize,
    // num_numeric_updates: AtomicIsize,
    // num_binary_updates: AtomicIsize,
    pub deleted_terms: HashMap<Term, DocId>,
    // the key is string represent of query, query is share by multi-thread
    pub deleted_queries: HashMap<String, (Arc<dyn Query<C>>, DocId)>,
    pub deleted_doc_ids: Vec<i32>,

    pub doc_values_updates: HashMap<String, HashMap<Vec<u8>, Arc<dyn DocValuesUpdate>>>,
    // For each field we keep an ordered list of NumericUpdates, key'd by the
    // update Term. LinkedHashMap guarantees we will later traverse the map in
    // insertion order (so that if two terms affect the same document, the last
    // one that came in wins), and helps us detect faster if the same Term is
    // used to update the same field multiple times (so we later traverse it
    // only once).
    // numeric_updates: HashMap<String, HashMap<Term, NumericDocValuesUpdate>>,
    //
    // Map<dvField,Map<updateTerm,BinaryUpdate>>
    // For each field we keep an ordered list of BinaryUpdates, key'd by the
    // update Term. LinkedHashMap guarantees we will later traverse the map in
    // insertion order (so that if two terms affect the same document, the last
    // one that came in wins), and helps us detect faster if the same Term is
    // used to update the same field multiple times (so we later traverse it
    // only once).
    // binary_update: HashMap<String, HashMap<Term, BinaryDocValuesUpdate>>,
    // gen: i64,
    pub segment_name: String,
}

impl<C: Codec> BufferedUpdates<C> {
    pub fn new(name: String) -> Self {
        BufferedUpdates {
            num_term_deletes: AtomicUsize::new(0),
            deleted_terms: HashMap::new(),
            deleted_queries: HashMap::new(),
            deleted_doc_ids: vec![],
            doc_values_updates: HashMap::new(),
            segment_name: name,
        }
    }

    pub fn add_doc_id(&mut self, doc_id: DocId) {
        self.deleted_doc_ids.push(doc_id);
    }

    pub fn add_query(&mut self, query: Arc<dyn Query<C>>, doc_id_upto: DocId) {
        let query_str = format!("{}", &query);
        self.deleted_queries.insert(query_str, (query, doc_id_upto));
    }

    pub fn add_term(&mut self, term: Term, doc_id_upto: DocId) {
        if let Some(current) = self.deleted_terms.get(&term) {
            if doc_id_upto <= *current {
                // Only record the new number if it's greater than the
                // current one.  This is important because if multiple
                // threads are replacing the same doc at nearly the
                // same time, it's possible that one thread that got a
                // higher docID is scheduled before the other
                // threads.  If we blindly replace than we can
                // incorrectly get both docs indexed.
                return;
            }
        }

        self.deleted_terms.insert(term, doc_id_upto);
        self.num_term_deletes.fetch_add(1, Ordering::AcqRel);
    }

    pub fn add_doc_values_update(&mut self, update: Arc<dyn DocValuesUpdate>, docid_up_to: DocId) {
        let mut upd = update.clone();
        if docid_up_to < NO_MORE_DOCS {
            // segment private update
            upd = match update.dv_type() {
                DocValuesType::Numeric | DocValuesType::SortedNumeric => {
                    Arc::new(NumericDocValuesUpdate::new(
                        update.term(),
                        update.field(),
                        update.dv_type(),
                        update.numeric(),
                        Some(docid_up_to),
                    ))
                }
                _ => unimplemented!(),
            };
        }
        if let Some(m) = self.doc_values_updates.get_mut(&update.field()) {
            m.insert(update.term().bytes, upd);
        } else {
            let mut m = HashMap::new();
            m.insert(update.term().bytes, upd);
            self.doc_values_updates.insert(update.field(), m);
        }
    }

    pub fn clear(&mut self) {
        self.deleted_terms.clear();
        self.deleted_queries.clear();
        self.deleted_doc_ids.clear();
        self.doc_values_updates.clear();
        self.num_term_deletes.store(0, Ordering::Release);
    }

    pub fn any(&self) -> bool {
        !self.deleted_terms.is_empty()
            || !self.deleted_doc_ids.is_empty()
            || !self.deleted_queries.is_empty()
            || !self.doc_values_updates.is_empty()
    }
}

/// Holds buffered deletes and updates by term or query, once pushed. Pushed
/// deletes/updates are write-once, so we shift to more memory efficient data
/// structure to hold them. We don't hold docIDs because these are applied on
/// flush.
pub struct FrozenBufferedUpdates<C: Codec> {
    terms: Arc<PrefixCodedTerms>,
    // Parallel array of deleted query, and the doc_id_upto for each
    query_and_limits: Vec<(Arc<dyn Query<C>>, DocId)>,
    doc_values_updates: HashMap<String, Vec<Arc<dyn DocValuesUpdate>>>,
    pub num_term_deletes: usize,
    pub num_query_deletes: usize,
    pub num_updates: usize,
    pub gen: u64,
    // assigned by BufferedUpdatesStream once pushed
    // set to true iff this frozen packet represents a segment private delete.
    // in that case is should only have queries
    is_segment_private: bool,
}

impl<C: Codec> fmt::Display for FrozenBufferedUpdates<C> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.num_term_deletes > 0 {
            write!(
                f,
                " {} deleted terms (unique count={})",
                self.num_term_deletes, self.terms.size
            )?;
        }
        if !self.query_and_limits.is_empty() {
            write!(f, " {} deleted queries", self.query_and_limits.len())?;
        }
        Ok(())
    }
}

impl<C: Codec> FrozenBufferedUpdates<C> {
    pub fn new(deletes: &mut BufferedUpdates<C>, is_segment_private: bool) -> Self {
        debug_assert!(!is_segment_private || deletes.deleted_terms.is_empty());

        let mut terms_array = Vec::with_capacity(deletes.deleted_terms.len());
        for (t, _) in deletes.deleted_terms.drain() {
            terms_array.push(t);
        }
        terms_array.sort();

        let mut builder = PrefixCodedTermsBuilder::default();
        for t in terms_array {
            builder.add_term(t);
        }
        let terms = builder.finish();

        let query_and_limits: Vec<_> = deletes
            .deleted_queries
            .drain()
            .map(|(_key, value)| value)
            .collect();

        let mut num_updates: usize = 0;
        let mut dv_updates = HashMap::new();
        for (field, updates) in &mut deletes.doc_values_updates {
            let upds: Vec<_> = updates.drain().map(|(_key, value)| value).collect();
            num_updates += upds.len();
            dv_updates.insert(field.clone(), upds);
        }
        deletes.doc_values_updates.clear();

        let num_query_deletes = query_and_limits.len();

        // TODO if a Term affects multiple fields, we could keep the updates key'd by Term
        // so that it maps to all fields it affects, sorted by their docUpto, and traverse
        // that Term only once, applying the update to all fields that still need to be
        // updated.
        FrozenBufferedUpdates {
            terms: Arc::new(terms),
            query_and_limits,
            doc_values_updates: dv_updates,
            num_term_deletes: deletes.num_term_deletes.load(Ordering::Acquire),
            num_query_deletes,
            num_updates,
            gen: u64::max_value(),
            // used as a sentinel of invalid
            is_segment_private,
        }
    }

    pub fn set_del_gen(&mut self, gen: u64) {
        assert_eq!(self.gen, u64::max_value());
        self.gen = gen;
        self.terms.set_del_gen(gen);
    }

    pub fn any(&self) -> bool {
        self.terms.size > 0
            || !self.query_and_limits.is_empty()
            || !self.doc_values_updates.is_empty()
    }
}

/// Tracks the stream of {@link BufferedDeletes}.
/// When DocumentsWriterPerThread flushes, its buffered
/// deletes and updates are appended to this stream.  We later
/// apply them (resolve them to the actual
/// docIDs, per segment) when a merge is started
/// (only to the to-be-merged segments).  We
/// also apply to all segments when NRT reader is pulled,
/// commit/close is called, or when too many deletes or  updates are
/// buffered and must be flushed (by RAM usage or by count).
///
/// Each packet is assigned a generation, and each flushed or
/// merged segment is also assigned a generation, so we can
/// track which BufferedDeletes packets to apply to any given
/// segment.
pub struct BufferedUpdatesStream<C: Codec> {
    lock: Mutex<()>,
    updates: Mutex<Vec<FrozenBufferedUpdates<C>>>,
    // Starts at 1 so that SegmentInfos that have never had
    // deletes applied (whose bufferedDelGen defaults to 0)
    // will be correct:
    next_gen: AtomicU64,
    // used only by assert:
    last_delete_term: Vec<u8>,
    num_terms: AtomicUsize,
    num_queries: AtomicUsize,
    num_updates: AtomicUsize,
}

impl<C: Codec> Default for BufferedUpdatesStream<C> {
    fn default() -> Self {
        BufferedUpdatesStream {
            lock: Mutex::new(()),
            updates: Mutex::new(vec![]),
            next_gen: AtomicU64::new(1),
            last_delete_term: Vec::with_capacity(0),
            num_terms: AtomicUsize::new(0),
            num_queries: AtomicUsize::new(0),
            num_updates: AtomicUsize::new(0),
        }
    }
}

impl<C: Codec> BufferedUpdatesStream<C> {
    // Append a new packet of buffered deletes to the stream:
    // setting its generation:
    pub fn push(&self, mut packet: FrozenBufferedUpdates<C>) -> Result<u64> {
        // The insert operation must be atomic. If we let threads increment the gen
        // and push the packet afterwards we risk that packets are out of order.
        // With DWPT this is possible if two or more flushes are racing for pushing
        // updates. If the pushed packets get our of order would loose documents
        // since deletes are applied to the wrong segments.
        let _l = self.lock.lock().unwrap();
        packet.set_del_gen(self.next_gen.fetch_add(1, Ordering::AcqRel));
        let mut updates = self.updates.lock()?;
        debug_assert!(packet.any());
        debug_assert!(self.check_delete_stats(&updates));
        debug_assert!(updates.is_empty() || updates.last().unwrap().gen < packet.gen);
        let del_gen = packet.gen;
        self.num_terms
            .fetch_add(packet.num_term_deletes, Ordering::AcqRel);
        self.num_queries
            .fetch_add(packet.num_query_deletes, Ordering::AcqRel);
        self.num_updates
            .fetch_add(packet.num_updates, Ordering::AcqRel);
        updates.push(packet);
        debug_assert!(self.check_delete_stats(&updates));
        Ok(del_gen)
    }

    pub fn clear(&self) -> Result<()> {
        self.updates.lock()?.clear();
        self.next_gen.store(1, Ordering::Release);
        self.num_terms.store(0, Ordering::Release);
        self.num_queries.store(0, Ordering::Release);
        self.num_updates.store(0, Ordering::Release);
        Ok(())
    }

    pub fn any(&self) -> bool {
        self.num_terms.load(Ordering::Acquire) > 0
            || self.num_updates.load(Ordering::Acquire) > 0
            || self.num_queries.load(Ordering::Acquire) > 0
    }

    pub fn num_terms(&self) -> usize {
        self.num_terms.load(Ordering::Acquire)
    }

    pub fn apply_deletes_and_updates<D, MS, MP>(
        &self,
        pool: &ReaderPool<D, C, MS, MP>,
        infos: &[Arc<SegmentCommitInfo<D, C>>],
    ) -> Result<ApplyDeletesResult<D, C>>
    where
        D: Directory + Send + Sync + 'static,
        MS: MergeScheduler,
        MP: MergePolicy,
    {
        let _l = self.lock.lock().unwrap();
        let updates_stream = unsafe {
            let stream = self as *const BufferedUpdatesStream<C> as *mut BufferedUpdatesStream<C>;
            &mut *stream
        };
        let mut seg_states = Vec::with_capacity(infos.len());
        let gen = self.next_gen.load(Ordering::Acquire);
        match updates_stream.do_apply_deletes_and_updates(pool, infos, &mut seg_states) {
            Ok(Some(res)) => {
                return Ok(res);
            }
            Err(e) => {
                if let Err(e1) =
                    updates_stream.close_segment_states(pool, &mut seg_states, false, gen as i64)
                {
                    error!("close segment states failed with '{:?}'", e1);
                }
                return Err(e);
            }
            _ => {}
        }
        updates_stream.close_segment_states(pool, &mut seg_states, true, gen as i64)
    }

    /// Resolves the buffered deleted Term/Query/docIDs, into actual deleted
    /// doc_ids in hte live_docs MutableBits for each SegmentReader
    fn do_apply_deletes_and_updates<D, MS, MP>(
        &mut self,
        pool: &ReaderPool<D, C, MS, MP>,
        infos: &[Arc<SegmentCommitInfo<D, C>>],
        seg_states: &mut Vec<SegmentState<D, C, MS, MP>>,
    ) -> Result<Option<ApplyDeletesResult<D, C>>>
    where
        D: Directory + Send + Sync + 'static,
        MS: MergeScheduler,
        MP: MergePolicy,
    {
        let mut coalesce_updates = CoalescedUpdates::default();

        // We only init these on demand, when we find our first deletes that need to be applied:
        let start = Instant::now();
        let mut total_del_count = 0;
        let mut total_term_visited_count = 0;

        let gen = self.next_gen.fetch_add(1, Ordering::AcqRel) as i64;

        {
            let mut updates = self.updates.lock()?;

            if infos.is_empty() {
                return Ok(Some(ApplyDeletesResult::new(
                    false,
                    gen,
                    Vec::with_capacity(0),
                )));
            }

            debug_assert!(self.check_delete_stats(&updates));

            if !self.any() {
                return Ok(Some(ApplyDeletesResult::new(
                    false,
                    gen,
                    Vec::with_capacity(0),
                )));
            }

            let mut infos = infos.to_vec();
            infos.sort_by(
                |i1: &Arc<SegmentCommitInfo<D, C>>, i2: &Arc<SegmentCommitInfo<D, C>>| {
                    i1.buffered_deletes_gen().cmp(&i2.buffered_deletes_gen())
                },
            );

            let mut infos_idx = infos.len();
            let mut del_idx = updates.len();

            // Backwards merge sort the segment delGens with the packet
            // delGens in the buffered stream:
            while infos_idx > 0 {
                let info = &infos[infos_idx - 1];
                let seg_gen = info.buffered_deletes_gen();

                if del_idx > 0 && seg_gen < updates[del_idx - 1].gen as i64 {
                    if !updates[del_idx - 1].is_segment_private && updates[del_idx - 1].any() {
                        // Only coalesce if we are NOT on a segment private del packet: the
                        // segment private del packet must only apply to segments with the
                        // same delGen.  Yet, if a segment is already deleted from the SI
                        // since it had no more documents remaining after some del packets
                        // younger than its segPrivate packet (higher delGen) have been
                        // applied, the segPrivate packet has not been removed.
                        coalesce_updates.update(&mut updates[del_idx - 1]);
                        // global updates
                        coalesce_updates.collect_doc_values_updates(&mut updates[del_idx - 1]);
                    }
                    del_idx -= 1;
                } else if del_idx > 0 && seg_gen == updates[del_idx - 1].gen as i64 {
                    debug_assert!(updates[del_idx - 1].is_segment_private);
                    if seg_states.is_empty() {
                        self.open_segment_states(pool, &infos, seg_states)?;
                    }

                    let seg_state = &mut seg_states[infos_idx - 1];

                    debug_assert!(pool.info_is_live(info));
                    let mut del_count = 0;

                    // first apply segment-private deletes
                    del_count += Self::apply_query_deletes(
                        updates[del_idx - 1].query_and_limits.iter(),
                        seg_state,
                    )?;

                    // private updates
                    coalesce_updates.collect_doc_values_updates(&mut updates[del_idx - 1]);

                    // ... then coalesced deletes/updates, so that if there is an update
                    // that appears in both, the coalesced updates (carried from
                    // updates ahead of the segment-privates ones) win:
                    if coalesce_updates.has_queries() {
                        del_count += Self::apply_query_deletes(
                            coalesce_updates.query_and_limits(),
                            seg_state,
                        )?;
                    }

                    total_del_count += del_count;

                    // Since we are on a segment private del packet we must not update the
                    // coalescedUpdates here! We can simply advance to the next packet and seginfo.
                    del_idx -= 1;
                    infos_idx -= 1;
                } else {
                    if coalesce_updates.any() {
                        if seg_states.is_empty() {
                            self.open_segment_states(pool, &infos, seg_states)?;
                        }

                        let seg_state = &mut seg_states[infos_idx - 1];

                        // Lock order: IW -> BD -> RP
                        debug_assert!(pool.info_is_live(info));
                        let mut del_count = 0;
                        if coalesce_updates.has_queries() {
                            del_count += Self::apply_query_deletes(
                                coalesce_updates.query_and_limits(),
                                seg_state,
                            )?;
                        }

                        total_del_count += del_count;
                    }

                    infos_idx -= 1;
                }
            }
        }

        // Now apply all term deletes:
        if coalesce_updates.total_term_count > 0 {
            if seg_states.is_empty() {
                self.open_segment_states(pool, infos, seg_states)?;
            }
            total_term_visited_count +=
                self.apply_term_deletes(&coalesce_updates, seg_states.as_mut())?;
        }

        // Now apply doc values updates
        if coalesce_updates.has_updates() {
            if seg_states.is_empty() {
                self.open_segment_states(pool, infos, seg_states)?;
            }
            self.apply_doc_values_updates(&coalesce_updates, seg_states.as_mut())?;
        }

        self.num_updates.store(0, Ordering::Release);
        self.num_queries.store(0, Ordering::Release);

        debug!(
            "BD - apply_deletes took {:?} for {} segments, {} newly deleted docs (query deletes), \
             {} visited terms",
            Instant::now() - start,
            infos.len(),
            total_del_count,
            total_term_visited_count
        );

        // debug_assert!(self.check_delete_stats(&updates));

        Ok(None)
    }

    /// Delete by query
    fn apply_query_deletes<'a, D, MS, MP>(
        queries: impl Iterator<Item = &'a (Arc<dyn Query<C>>, DocId)>,
        seg_state: &mut SegmentState<D, C, MS, MP>,
    ) -> Result<u64>
    where
        D: Directory + Send + Sync + 'static,
        MS: MergeScheduler,
        MP: MergePolicy,
    {
        let mut del_count: u64 = 0;
        let mut rld = seg_state.rld.inner.lock()?;
        let mut searcher = DefaultIndexSearcher::new(Arc::clone(rld.reader()), None);
        let query_cache: Arc<dyn QueryCache<C>> = Arc::new(NoCacheQueryCache::new());
        searcher.set_query_cache(query_cache);
        let reader = searcher.reader().leaves().remove(0);
        for (query, limit) in queries {
            let weight = searcher.create_normalized_weight(query.as_ref(), false)?;
            if let Some(mut scorer) = weight.create_scorer(&reader)? {
                let live_docs = reader.reader.live_docs();
                loop {
                    let doc = scorer.next()?;
                    if doc >= *limit {
                        break;
                    }
                    if !live_docs.get(doc as usize)? {
                        continue;
                    }

                    if !seg_state.any {
                        rld.init_writable_live_docs(&seg_state.rld.info)?;
                        seg_state.any = true;
                    }
                    if rld.delete(doc)? {
                        del_count += 1;
                    }
                }
            }
        }
        Ok(del_count)
    }

    /// Merge sorts the deleted terms and all segments to resolve terms to doc_ids for deletion.
    fn apply_term_deletes<D, MS, MP>(
        &mut self,
        updates: &CoalescedUpdates<C>,
        seg_states: &mut [SegmentState<D, C, MS, MP>],
    ) -> Result<u64>
    where
        D: Directory + Send + Sync + 'static,
        MS: MergeScheduler,
        MP: MergePolicy,
    {
        let start = Instant::now();
        let num_readers = seg_states.len();

        let mut del_term_visited_count: u64 = 0;
        let mut seg_term_visited_count = 0;
        let mut queue = BinaryHeap::new();

        let mut iter = updates.term_iterator()?;
        let mut field = String::with_capacity(0);

        while let Some(term) = iter.next()? {
            if iter.field() != field {
                field = iter.field().to_string();
                queue = BinaryHeap::with_capacity(num_readers);
                for i in 0..num_readers {
                    let terms_option = {
                        let guard = seg_states[i].rld.inner.lock()?;
                        guard.reader().fields()?.terms(&field)?
                    };

                    if let Some(terms) = terms_option {
                        let mut terms_iterator = terms.iterator()?;
                        seg_states[i].term = terms_iterator.next()?;
                        seg_states[i].terms_iterator = Some(terms_iterator);
                        if seg_states[i].term.is_some() {
                            queue.push(SegmentStateRef::new(seg_states, i));
                        }
                    }
                }

                self.last_delete_term.clear();
            }

            self.check_deleted_term(term.bytes());
            self.last_delete_term = term.bytes().to_vec();

            del_term_visited_count += 1;
            let del_gen = iter.del_gen();

            while !queue.is_empty() {
                // Get next term merged across all segments
                let mut should_pop = false;
                {
                    let top = queue.peek_mut().unwrap();
                    let i = top.index;

                    seg_term_visited_count += 1;
                    match term.bytes().cmp(seg_states[i].term.as_ref().unwrap()) {
                        CmpOrdering::Less => {
                            break;
                        }
                        CmpOrdering::Equal => {
                            // fall through
                        }
                        CmpOrdering::Greater => {
                            debug_assert!(seg_states[i].terms_iterator.is_some());
                            match seg_states[i]
                                .terms_iterator
                                .as_mut()
                                .unwrap()
                                .seek_ceil(term.bytes())?
                            {
                                SeekStatus::Found => {
                                    // fall through
                                }
                                SeekStatus::NotFound => {
                                    seg_states[i].term = Some(
                                        seg_states[i]
                                            .terms_iterator
                                            .as_ref()
                                            .unwrap()
                                            .term()?
                                            .to_vec(),
                                    );
                                    continue;
                                }
                                SeekStatus::End => {
                                    should_pop = true;
                                    // pop current
                                }
                            }
                        }
                    }
                }

                if should_pop {
                    queue.pop();
                    continue;
                }

                let idx: usize;
                {
                    let top = queue.peek_mut().unwrap();
                    idx = top.index;
                    debug_assert_ne!(seg_states[idx].del_gen, del_gen);
                    debug_assert!(seg_states[idx].terms_iterator.is_some());

                    if seg_states[idx].del_gen < del_gen {
                        // we don't need term frequencies for this
                        let mut postings = seg_states[idx]
                            .terms_iterator
                            .as_mut()
                            .unwrap()
                            .postings_with_flags(PostingIteratorFlags::NONE)?;

                        loop {
                            let doc_id = postings.next()?;
                            if doc_id == NO_MORE_DOCS {
                                break;
                            }
                            if !seg_states[idx].rld.test_doc_id(doc_id as usize)? {
                                continue;
                            }
                            if !seg_states[idx].any {
                                seg_states[idx].rld.init_writable_live_docs()?;
                                seg_states[idx].any = true;
                            }
                            // NOTE: there is no limit check on the docID
                            // when deleting by Term (unlike by Query)
                            // because on flush we apply all Term deletes to
                            // each segment.  So all Term deleting here is
                            // against prior segments:
                            seg_states[idx].rld.delete(doc_id)?;
                        }
                        seg_states[idx].postings = Some(postings);
                    }

                    seg_states[idx].term =
                        seg_states[idx].terms_iterator.as_mut().unwrap().next()?;
                }

                if seg_states[idx].term.is_none() {
                    queue.pop();
                }
            }
        }

        debug!(
            "applyTermDeletes took {:?} for {} segments and {} packets; {} del terms visited; {} \
             seg terms visited",
            Instant::now() - start,
            num_readers,
            updates.terms.len(),
            del_term_visited_count,
            seg_term_visited_count
        );

        Ok(del_term_visited_count)
    }

    fn apply_doc_values_updates<D, MS, MP>(
        &mut self,
        updates: &CoalescedUpdates<C>,
        seg_states: &mut [SegmentState<D, C, MS, MP>],
    ) -> Result<u64>
    where
        D: Directory + Send + Sync + 'static,
        MS: MergeScheduler,
        MP: MergePolicy,
    {
        for seg_state in seg_states {
            for (field, updates) in &updates.updates {
                // INFO: apply updates in one field of a segment
                let mut need_updates: Vec<(u64, Vec<Arc<dyn DocValuesUpdate>>)> = vec![];
                for upds in updates {
                    if upds.2 && seg_state.del_gen != upds.0 as i64 {
                        // skip other segment's private
                        continue;
                    }
                    if seg_state.del_gen <= upds.0 as i64 {
                        need_updates.push((upds.0, upds.1.clone()));
                    }
                }
                if need_updates.is_empty() {
                    continue;
                }
                let it = MergedDocValuesUpdatesIterator::new(need_updates);

                seg_state
                    .rld
                    .inner
                    .lock()?
                    .add_field_updates(field.clone(), it);
            }
            seg_state.rld.inner.lock()?.write_field_updates()?;
        }
        Ok(0)
    }

    /// Opens SegmentReader and inits SegmentState for each segment.
    fn open_segment_states<D, MS, MP>(
        &self,
        pool: &ReaderPool<D, C, MS, MP>,
        infos: &[Arc<SegmentCommitInfo<D, C>>],
        seg_states: &mut Vec<SegmentState<D, C, MS, MP>>,
    ) -> Result<()>
    where
        D: Directory + Send + Sync + 'static,
        MS: MergeScheduler,
        MP: MergePolicy,
    {
        for info in infos {
            match SegmentState::new(pool, info) {
                Ok(state) => seg_states.push(state),
                Err(e) => {
                    for s in seg_states {
                        if let Err(e) = s.finish(pool) {
                            error!("release segment state failed with: {:?}", e);
                        }
                    }
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    /// Close segment states previously opened with openSegmentStates.
    fn close_segment_states<D, MS, MP>(
        &mut self,
        pool: &ReaderPool<D, C, MS, MP>,
        seg_states: &mut [SegmentState<D, C, MS, MP>],
        success: bool,
        gen: i64,
    ) -> Result<ApplyDeletesResult<D, C>>
    where
        D: Directory + Send + Sync + 'static,
        MS: MergeScheduler,
        MP: MergePolicy,
    {
        let mut first_err = Ok(ApplyDeletesResult::new(false, 0, vec![]));
        let mut total_del_count = 0;
        let mut all_deleted = vec![];

        for seg_state in seg_states {
            if success {
                total_del_count +=
                    seg_state.rld.pending_delete_count() - seg_state.start_del_count as u32;
                seg_state.rld.info.set_buffered_deletes_gen(gen);
                let full_del_count =
                    seg_state.rld.info.del_count() + seg_state.rld.pending_delete_count() as i32;
                debug_assert!(full_del_count <= seg_state.rld.info.info.max_doc);
                if full_del_count == seg_state.rld.info.info.max_doc {
                    all_deleted.push(Arc::clone(&seg_state.rld.info));
                }
            }
            let err = seg_state.finish(pool);
            if let Err(e) = err {
                first_err = Err(e);
            }
        }

        if success && first_err.is_err() {
            return first_err;
        }

        debug!(
            "BD - apply_deletes: {} new delete documents.",
            total_del_count
        );

        Ok(ApplyDeletesResult::new(
            total_del_count > 0,
            gen,
            all_deleted,
        ))
    }

    // used only by assert
    fn check_deleted_term(&self, term: &[u8]) {
        if !term.is_empty() {
            debug_assert!(
                self.last_delete_term.is_empty() || self.last_delete_term.as_slice() <= term
            );
        }
    }

    // only used for assert
    fn check_delete_stats(&self, updates: &[FrozenBufferedUpdates<C>]) -> bool {
        let mut num_terms2 = 0;
        for update in updates {
            num_terms2 += update.num_term_deletes;
        }
        assert_eq!(num_terms2, self.num_terms.load(Ordering::Acquire));
        true
    }

    pub fn get_next_gen(&self) -> u64 {
        let _l = self.lock.lock().unwrap();
        self.next_gen.fetch_add(1, Ordering::AcqRel)
    }

    // Lock order IW -> BD
    /// Remove any BufferDeletes that we on longer need to store because
    /// all segments in the index have had the deletes applied.
    pub fn prune<D: Directory>(&self, segment_infos: &SegmentInfos<D, C>) {
        let _l = self.lock.lock().unwrap();
        let mut updates = self.updates.lock().unwrap();
        debug_assert!(self.check_delete_stats(&updates));

        let mut min_gen = i64::max_value();
        for info in &segment_infos.segments {
            min_gen = min(info.buffered_deletes_gen(), min_gen);
        }

        let limit = updates.len();
        for i in 0..limit {
            if updates[i].gen as i64 >= min_gen {
                self.do_prune(&mut updates, i);
                debug_assert!(self.check_delete_stats(&updates));
                return;
            }
        }
        self.do_prune(&mut updates, limit);
        self.num_queries.store(0, Ordering::Release);
        debug_assert!(!self.any());
        debug_assert!(self.check_delete_stats(&updates));
    }

    fn do_prune(&self, updates: &mut MutexGuard<Vec<FrozenBufferedUpdates<C>>>, idx: usize) {
        if idx > 0 {
            debug!(
                "BD: prune_deletes: prune {} packets; {} packets remain.",
                idx,
                updates.len() - idx
            );
        }
        for i in 0..idx {
            debug_assert!(self.num_terms.load(Ordering::Acquire) >= updates[i].num_term_deletes);
            self.num_terms
                .fetch_sub(updates[i].num_term_deletes, Ordering::AcqRel);
        }
        updates.drain(..idx);
    }
}

pub struct ApplyDeletesResult<D: Directory, C: Codec> {
    // True if any actual deletes took place:
    pub any_deletes: bool,
    // Current gen, for the merged segment:
    pub gen: i64,
    // if non-empty, contains segment that are 100% deleted
    pub all_deleted: Vec<Arc<SegmentCommitInfo<D, C>>>,
}

impl<D: Directory, C: Codec> ApplyDeletesResult<D, C> {
    fn new(any_deletes: bool, gen: i64, all_deleted: Vec<Arc<SegmentCommitInfo<D, C>>>) -> Self {
        ApplyDeletesResult {
            any_deletes,
            gen,
            all_deleted,
        }
    }
}

struct SegmentState<
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
> {
    del_gen: i64,
    rld: Arc<ReadersAndUpdates<D, C, MS, MP>>,
    start_del_count: usize,
    terms_iterator: Option<CodecTermIterator<C>>,
    postings: Option<CodecPostingIterator<C>>,
    term: Option<Vec<u8>>,
    any: bool,
}

impl<D, C, MS, MP> SegmentState<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    fn new(pool: &ReaderPool<D, C, MS, MP>, info: &Arc<SegmentCommitInfo<D, C>>) -> Result<Self> {
        let rld: Arc<ReadersAndUpdates<D, C, MS, MP>> = pool.get_or_create(info)?;
        let start_del_count = rld.pending_delete_count() as usize;
        rld.create_reader_if_not_exist(&IOContext::READ)?;
        let del_gen = info.buffered_deletes_gen();

        Ok(SegmentState {
            del_gen,
            rld,
            start_del_count,
            terms_iterator: None,
            postings: None,
            term: None,
            any: false,
        })
    }

    fn finish(&mut self, pool: &ReaderPool<D, C, MS, MP>) -> Result<()> {
        pool.release(&self.rld)
    }
}

struct SegmentStateRef<
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
> {
    states: *const [SegmentState<D, C, MS, MP>],
    index: usize,
}

impl<D, C, MS, MP> SegmentStateRef<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    fn new(states: &[SegmentState<D, C, MS, MP>], index: usize) -> Self {
        Self { states, index }
    }
}

/// use for iter terms on binary heap
impl<D, C, MS, MP> Ord for SegmentStateRef<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    fn cmp(&self, other: &Self) -> CmpOrdering {
        // reversed order
        unsafe {
            if let Some(ref s) = (*self.states)[self.index].term {
                if let Some(ref o) = (*other.states)[other.index].term {
                    o.cmp(s)
                } else {
                    CmpOrdering::Less
                }
            } else {
                CmpOrdering::Greater
            }
        }
    }
}

impl<D, C, MS, MP> PartialOrd for SegmentStateRef<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl<D, C, MS, MP> Eq for SegmentStateRef<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
}

impl<D, C, MS, MP> PartialEq for SegmentStateRef<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    fn eq(&self, other: &Self) -> bool {
        unsafe {
            (*self.states)[self.index]
                .term
                .eq(&(*other.states)[self.index].term)
        }
    }
}

struct CoalescedUpdates<C: Codec> {
    queries: HashMap<String, (Arc<dyn Query<C>>, DocId)>,
    terms: Vec<Arc<PrefixCodedTerms>>,
    updates: HashMap<String, Vec<(u64, Vec<Arc<dyn DocValuesUpdate>>, bool)>>,
    total_term_count: usize,
}

impl<C: Codec> Default for CoalescedUpdates<C> {
    fn default() -> Self {
        CoalescedUpdates {
            queries: HashMap::new(),
            terms: vec![],
            updates: HashMap::new(),
            total_term_count: 0,
        }
    }
}

impl<C: Codec> CoalescedUpdates<C> {
    fn update(&mut self, up: &mut FrozenBufferedUpdates<C>) {
        self.total_term_count += up.terms.size;
        self.terms.push(Arc::clone(&up.terms));

        for (query, _) in &up.query_and_limits {
            self.queries
                .insert(query.to_string(), (Arc::clone(query), i32::max_value()));
        }
    }

    fn collect_doc_values_updates(&mut self, up: &mut FrozenBufferedUpdates<C>) {
        for (field, updates) in &mut up.doc_values_updates {
            // used for merge
            updates.sort_by(|a, b| a.term().bytes.cmp(&b.term().bytes));
            if let Some(v) = self.updates.get_mut(field) {
                v.push((up.gen, updates.clone(), up.is_segment_private));
            } else {
                self.updates.insert(
                    field.clone(),
                    vec![(up.gen, updates.clone(), up.is_segment_private)],
                );
            }
        }
    }

    fn term_iterator(&self) -> Result<FieldTermIterator> {
        debug_assert!(!self.terms.is_empty());
        FieldTermIterator::build(&self.terms)
    }

    pub fn query_and_limits(&self) -> impl Iterator<Item = &(Arc<dyn Query<C>>, DocId)> {
        self.queries.values()
    }

    pub fn has_queries(&self) -> bool {
        !self.queries.is_empty()
    }

    pub fn has_updates(&self) -> bool {
        !self.updates.is_empty()
    }

    pub fn any(&self) -> bool {
        !self.queries.is_empty() || !self.terms.is_empty() || !self.updates.is_empty()
    }
}
