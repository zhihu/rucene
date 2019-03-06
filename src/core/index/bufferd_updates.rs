use core::index::index_writer::{ReaderPool, ReadersAndUpdates};
use core::index::prefix_code_terms::{FieldTermIter, FieldTermIterator};
use core::index::prefix_code_terms::{PrefixCodedTerms, PrefixCodedTermsBuilder};
use core::index::term::SeekStatus;
use core::index::LeafReader;
use core::index::{EmptyTermIterator, TermIterator};
use core::index::{SegmentCommitInfo, SegmentInfos, Term};
use core::search::posting_iterator::POSTING_ITERATOR_FLAG_NONE;
use core::search::posting_iterator::{EmptyPostingIterator, PostingIterator};
use core::search::query_cache::{NoCacheQueryCache, QueryCache};
use core::search::searcher::{DefaultIndexSearcher, IndexSearcher};
use core::search::{Query, NO_MORE_DOCS};
use core::store::{IOContext, IO_CONTEXT_READ};
use core::util::DocId;

use std::cmp::{min, Ordering as CmpOrdering};
use std::collections::{BinaryHeap, HashMap};
use std::fmt;
use std::mem;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::SystemTime;

use error::Result;

// Rough logic: del docIDs are List<i32>.  Say list allocates ~2X size (2 * i32),
pub const BYTES_PER_DEL_DOCID: usize = 2 * mem::size_of::<DocId>();

/// Rough logic: hash-map has an array<index> varying load factor (say 2 * usize).
/// Term is object with two Vec(String is actual a Vec), each Vec cost 2 * usize (cap/size) +
/// vec.capasity * byte
pub const BYTES_PER_DEL_TERM: usize = 6 * mem::size_of::<usize>();

/// Rough logic: HashMap has an array<index> and array<entry> varying load factor
/// say (2 * pointer). Entry is (String, (Box<Query>, i32)), String cost 2 * usize + string.len()
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
pub struct BufferedUpdates {
    pub num_term_deletes: AtomicUsize,
    // num_numeric_updates: AtomicIsize,
    // num_binary_updates: AtomicIsize,
    //
    // TODO: rename thes three: put "deleted" prefix in front:
    pub deleted_terms: Box<HashMap<Term, DocId>>,
    // the key is string represent of query, query is share by multi-thread
    pub deleted_queries: Box<HashMap<String, (Arc<Query>, DocId)>>,
    pub deleted_doc_ids: Box<Vec<i32>>,
    // Map<dvField,Map<updateTerm,NumericUpdate>>
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
    //
    pub bytes_used: AtomicUsize,
    // gen: i64,
    pub segment_name: String,
}

impl BufferedUpdates {
    pub fn new(name: String) -> Self {
        BufferedUpdates {
            num_term_deletes: AtomicUsize::new(0),
            deleted_terms: Box::new(HashMap::new()),
            deleted_queries: Box::new(HashMap::new()),
            deleted_doc_ids: Box::new(vec![]),
            bytes_used: AtomicUsize::new(0),
            segment_name: name,
        }
    }

    pub fn bytes_used(&self) -> usize {
        self.bytes_used.load(Ordering::Acquire)
    }

    pub fn add_doc_id(&mut self, doc_id: DocId) {
        self.deleted_doc_ids.push(doc_id);
        self.bytes_used
            .fetch_add(BYTES_PER_DEL_TERM, Ordering::AcqRel);
    }

    pub fn add_query(&mut self, query: Arc<Query>, doc_id_upto: DocId) {
        let query_str = format!("{}", &query);
        let query_str_cost = query_str.capacity();
        if self.deleted_queries
            .insert(query_str, (query, doc_id_upto))
            .is_none()
        {
            let cost = BYTES_PER_DEL_QUERY_IN_HASH + query_str_cost;
            self.bytes_used.fetch_add(cost, Ordering::AcqRel);
        }
    }

    pub fn add_term(&mut self, term: Term, doc_id_upto: DocId) {
        if let Some(current) = self.deleted_terms.get(&term) {
            if doc_id_upto < *current {
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

        let cost = BYTES_PER_DEL_TERM + term.field.capacity() + term.bytes.capacity();
        let current = self.deleted_terms.insert(term, doc_id_upto);
        self.num_term_deletes.fetch_add(1, Ordering::AcqRel);
        // note that if current.is_some() then it means there's already a buffered
        // delete on that term, therefore we seem to over-count. this over-counting
        // is done to respect IndexWriterConfig.setMaxBufferedDeleteTerms.
        if current.is_none() {
            self.bytes_used.fetch_add(cost, Ordering::AcqRel);
        }
    }

    pub fn clear(&mut self) {
        self.deleted_terms.clear();
        self.deleted_queries.clear();
        self.deleted_doc_ids.clear();
        self.num_term_deletes.store(0, Ordering::Release);
        self.bytes_used.store(0, Ordering::Release);
    }

    pub fn any(&self) -> bool {
        !self.deleted_terms.is_empty() || !self.deleted_doc_ids.is_empty()
            || !self.deleted_queries.is_empty()
    }
}

/// query we often undercount (say 24 bytes), plus int
const BYTES_PER_DEL_QUERY: usize = 24 + mem::size_of::<i32>();

/// Holds buffered deletes and updates by term or query, once pushed. Pushed
/// deletes/updates are write-once, so we shift to more memory efficient data
/// structure to hold them. We don't hold docIDs because these are applied on
/// flush.
///
pub struct FrozenBufferUpdates {
    terms: Arc<PrefixCodedTerms>,
    // Parallel array of deleted query, and the doc_id_upto for each
    query_and_limits: Vec<(Arc<Query>, DocId)>,
    pub bytes_used: usize,
    pub num_term_deletes: usize,
    pub gen: u64,
    // assigned by BufferedUpdatesStream once pushed
    // set to true iff this frozen packet represents a segment private delete.
    // in that case is should only have queries
    is_segment_private: bool,
}

impl fmt::Display for FrozenBufferUpdates {
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
        if self.bytes_used > 0 {
            write!(f, " bytes_used={}", self.bytes_used)?;
        }
        Ok(())
    }
}

impl FrozenBufferUpdates {
    pub fn new(deletes: &BufferedUpdates, is_segment_private: bool) -> Self {
        debug_assert!(!is_segment_private || deletes.deleted_terms.is_empty());

        let mut terms_array = Vec::with_capacity(deletes.deleted_terms.len());
        for (t, _) in deletes.deleted_terms.as_ref() {
            terms_array.push(t);
        }
        terms_array.sort();

        let mut builder = PrefixCodedTermsBuilder::default();
        for t in terms_array {
            builder.add_term(t);
        }
        let terms = builder.finish();

        let mut query_and_limits = Vec::with_capacity(deletes.deleted_queries.len());
        for (_, (query, limit)) in deletes.deleted_queries.as_ref() {
            query_and_limits.push((Arc::clone(query), *limit));
        }

        // TODO if a Term affects multiple fields, we could keep the updates key'd by Term
        // so that it maps to all fields it affects, sorted by their docUpto, and traverse
        // that Term only once, applying the update to all fields that still need to be
        // updated.
        let bytes_used = terms.ram_bytes_used() + query_and_limits.len() * BYTES_PER_DEL_QUERY;
        FrozenBufferUpdates {
            terms: Arc::new(terms),
            query_and_limits,
            bytes_used,
            num_term_deletes: deletes.num_term_deletes.load(Ordering::Acquire),
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
        self.terms.size > 0 || self.query_and_limits.len() > 0
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
pub struct BufferedUpdatesStream {
    updates: Mutex<Vec<FrozenBufferUpdates>>,
    // Starts at 1 so that SegmentInfos that have never had
    // deletes applied (whose bufferedDelGen defaults to 0)
    // will be correct:
    next_gen: AtomicU64,
    // used only by assert:
    last_delete_term: Vec<u8>,
    bytes_used: AtomicUsize,
    num_terms: AtomicUsize,
}

impl Default for BufferedUpdatesStream {
    fn default() -> Self {
        BufferedUpdatesStream {
            updates: Mutex::new(vec![]),
            next_gen: AtomicU64::new(1),
            last_delete_term: Vec::with_capacity(0),
            bytes_used: AtomicUsize::new(0),
            num_terms: AtomicUsize::new(0),
        }
    }
}

impl BufferedUpdatesStream {
    pub fn new() -> Self {
        Self::default()
    }

    // Append a new packet of buffered deletes to the stream:
    // setting its generation:
    pub fn push(&self, mut packet: FrozenBufferUpdates) -> Result<u64> {
        // The insert operation must be atomic. If we let threads increment the gen
        // and push the packet afterwards we risk that packets are out of order.
        // With DWPT this is possible if two or more flushes are racing for pushing
        // updates. If the pushed packets get our of order would loose documents
        // since deletes are applied to the wrong segments.
        packet.set_del_gen(self.get_next_gen());
        let mut updates = self.updates.lock()?;
        debug_assert!(packet.any());
        debug_assert!(self.check_delete_stats(&updates));
        debug_assert!(updates.is_empty() || updates.last().unwrap().gen < packet.gen);
        let del_gen = packet.gen;
        self.num_terms
            .fetch_add(packet.num_term_deletes, Ordering::AcqRel);
        self.bytes_used
            .fetch_add(packet.bytes_used, Ordering::AcqRel);
        updates.push(packet);
        debug_assert!(self.check_delete_stats(&updates));
        Ok(del_gen)
    }

    pub fn clear(&self) -> Result<()> {
        self.updates.lock()?.clear();
        self.next_gen.store(1, Ordering::Release);
        self.num_terms.store(0, Ordering::Release);
        self.bytes_used.store(0, Ordering::Release);
        Ok(())
    }

    pub fn any(&self) -> bool {
        self.bytes_used.load(Ordering::Acquire) > 0
    }

    pub fn num_terms(&self) -> usize {
        self.num_terms.load(Ordering::Acquire)
    }

    pub fn ram_bytes_used(&self) -> usize {
        self.bytes_used.load(Ordering::Acquire)
    }

    pub fn apply_deletes_and_updates(
        &mut self,
        pool: &ReaderPool,
        infos: &[Arc<SegmentCommitInfo>],
    ) -> Result<ApplyDeletesResult> {
        let mut seg_states = Vec::with_capacity(infos.len());
        let gen = self.next_gen.load(Ordering::Acquire);
        match self.do_apply_deletes_and_updates(pool, infos, &mut seg_states) {
            Ok((res, false)) => {
                return Ok(res);
            }
            Err(e) => {
                if let Err(e1) = self.close_segment_states(pool, &mut seg_states, false, gen as i64)
                {
                    error!("close segment states failed with '{:?}'", e1);
                }
                return Err(e);
            }
            _ => {}
        }
        self.close_segment_states(pool, &mut seg_states, true, gen as i64)
    }

    /// Resolves the buffered deleted Term/Query/docIDs, into actual deleted
    /// doc_ids in hte live_docs MutableBits for each SegmentReader
    fn do_apply_deletes_and_updates(
        &mut self,
        pool: &ReaderPool,
        infos: &[Arc<SegmentCommitInfo>],
        seg_states: &mut Vec<SegmentState>,
    ) -> Result<(ApplyDeletesResult, bool)> {
        let mut coalesce_udpates = CoalescedUpdates::default();

        // We only init these on demand, when we find our first deletes that need to be applied:
        let mut total_del_count = 0;
        let mut total_term_visited_count = 0;

        let gen = self.get_next_gen() as i64;

        {
            let mut updates = self.updates.lock()?;

            if infos.is_empty() {
                return Ok((
                    ApplyDeletesResult::new(false, gen, Vec::with_capacity(0)),
                    false,
                ));
            }

            debug_assert!(self.check_delete_stats(&updates));

            if !self.any() {
                return Ok((
                    ApplyDeletesResult::new(false, gen, Vec::with_capacity(0)),
                    false,
                ));
            }

            let mut infos = infos.to_vec();
            infos.sort_by(|i1: &Arc<SegmentCommitInfo>, i2: &Arc<SegmentCommitInfo>| {
                i1.buffered_deletes_gen().cmp(&i2.buffered_deletes_gen())
            });

            let mut infos_idx = infos.len();
            let mut del_idx = updates.len();

            // Backwards merge sort the segment delGens with the packet
            // delGens in the buffered stream:
            while infos_idx > 0 {
                let seg_gen = infos[infos_idx - 1].buffered_deletes_gen();
                let info = &infos[infos_idx - 1];

                if del_idx > 0 && seg_gen < updates[del_idx - 1].gen as i64 {
                    if !updates[del_idx - 1].is_segment_private && updates[del_idx - 1].any() {
                        // Only coalesce if we are NOT on a segment private del packet: the
                        // segment private del packet must only apply to segments with the
                        // same delGen.  Yet, if a segment is already deleted from the SI
                        // since it had no more documents remaining after some del packets
                        // younger than its segPrivate packet (higher delGen) have been
                        // applied, the segPrivate packet has not been removed.
                        coalesce_udpates.update(&mut updates[del_idx - 1]);
                    }
                    del_idx -= 1;
                } else if del_idx > 0 && seg_gen == updates[del_idx - 1].gen as i64 {
                    debug_assert!(updates[del_idx - 1].is_segment_private);
                    if seg_states.is_empty() {
                        self.open_segment_states(pool, &infos, seg_states)?;
                    }

                    let seg_state = &mut seg_states[infos_idx - 1];

                    debug_assert!(pool.info_is_live(info, None));
                    let mut del_count = 0;

                    // first apply segment-private deletes
                    let mut qals = vec![];
                    for ql in &updates[del_idx - 1].query_and_limits {
                        qals.push(ql);
                    }
                    del_count += Self::apply_query_deletes(qals, seg_state)?;

                    // ... then coalesced deletes/updates, so that if there is an update
                    // that appears in both, the coalesced updates (carried from
                    // updates ahead of the segment-privates ones) win:
                    if coalesce_udpates.any() {
                        del_count += Self::apply_query_deletes(
                            coalesce_udpates.query_and_limits(),
                            seg_state,
                        )?;
                    }

                    total_del_count += del_count;

                    // Since we are on a segment private del packet we must not update the
                    // coalescedUpdates here! We can simply advance to the next packet and seginfo.
                    del_idx -= 1;
                    infos_idx -= 1;
                } else {
                    if coalesce_udpates.any() {
                        if seg_states.is_empty() {
                            self.open_segment_states(pool, &infos, seg_states)?;
                        }

                        let seg_state = &mut seg_states[infos_idx - 1];

                        // Lock order: IW -> BD -> RP
                        debug_assert!(pool.info_is_live(info, None));
                        let mut del_count = 0;
                        del_count += Self::apply_query_deletes(
                            coalesce_udpates.query_and_limits(),
                            seg_state,
                        )?;

                        total_del_count += del_count;
                    }

                    infos_idx -= 1;
                }
            }
        }

        // Now apply all term deletes:
        if coalesce_udpates.total_term_count > 0 {
            if seg_states.is_empty() {
                self.open_segment_states(pool, infos, seg_states)?;
            }
            total_term_visited_count +=
                self.apply_term_deletes(&coalesce_udpates, seg_states.as_mut())?;
        }

        // debug_assert!(self.check_delete_stats(&updates));

        Ok((ApplyDeletesResult::new(false, gen, vec![]), true))
    }

    /// Delete by query
    fn apply_query_deletes(
        queries: Vec<&(Arc<Query>, DocId)>,
        seg_state: &mut SegmentState,
    ) -> Result<u64> {
        if queries.is_empty() {
            return Ok(0);
        }

        let mut del_count: u64 = 0;
        let rld = seg_state.rld.inner.lock()?;
        let seg_reader = rld.reader();
        let mut searcher = DefaultIndexSearcher::new(seg_reader);
        let query_cache: Arc<QueryCache> = Arc::new(NoCacheQueryCache::new());
        searcher.set_query_cache(query_cache);
        let reader = searcher.reader().leaves()[0];
        for (query, limit) in queries {
            let weight = searcher.create_normalized_weight(query.as_ref(), false)?;
            let mut scorer = weight.create_scorer(reader)?;
            let live_docs = reader.live_docs();
            loop {
                let doc = scorer.next()?;
                if doc > *limit {
                    break;
                }
                if !live_docs.get(doc as usize)? {
                    continue;
                }

                if !seg_state.any {
                    seg_state.rld.init_writable_live_docs()?;
                    seg_state.any = true;
                }
                if seg_state.rld.delete(doc)? {
                    del_count += 1;
                }
            }
        }
        Ok(del_count)
    }

    /// Merge sorts the deleted terms and all segments to resolve terms to doc_ids for deletion.
    fn apply_term_deletes(
        &mut self,
        updates: &CoalescedUpdates,
        seg_states: &mut [SegmentState],
    ) -> Result<u64> {
        let start = SystemTime::now();
        let num_readers = seg_states.len();

        let mut del_term_visited_count: u64 = 0;
        let mut seg_term_visited_count = 0;
        let mut queue = BinaryHeap::new();

        let mut iter = updates.term_iterator()?;
        let mut field = String::with_capacity(0);

        loop {
            if let Some(term) = iter.next()? {
                if iter.field() != &field {
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
                            seg_states[i].terms_iterator = terms_iterator;
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
                                match seg_states[i].terms_iterator.seek_ceil(term.bytes())? {
                                    SeekStatus::Found => {
                                        // fall through
                                    }
                                    SeekStatus::NotFound => {
                                        seg_states[i].term =
                                            Some(seg_states[i].terms_iterator.term()?.to_vec());
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

                        if seg_states[idx].del_gen < del_gen {
                            // we don't need term frequencies for this
                            let mut postings = seg_states[idx]
                                .terms_iterator
                                .postings_with_flags(POSTING_ITERATOR_FLAG_NONE)?;

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
                            seg_states[idx].postings = postings;
                        }

                        seg_states[idx].term = seg_states[idx].terms_iterator.next()?;
                    }

                    if seg_states[idx].term.is_none() {
                        queue.pop();
                    }
                }
            } else {
                break;
            }
        }

        debug!(
            "applyTermDeletes took {:?} for {} segments and {} packets; {} del terms visited; {} \
             seg terms visited",
            SystemTime::now().duration_since(start).unwrap(),
            num_readers,
            updates.terms.len(),
            del_term_visited_count,
            seg_term_visited_count
        );

        Ok(del_term_visited_count)
    }

    /// Opens SegmentReader and inits SegmentState for each segment.
    fn open_segment_states(
        &self,
        pool: &ReaderPool,
        infos: &[Arc<SegmentCommitInfo>],
        seg_states: &mut Vec<SegmentState>,
    ) -> Result<()> {
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
    fn close_segment_states(
        &mut self,
        pool: &ReaderPool,
        seg_states: &mut [SegmentState],
        success: bool,
        gen: i64,
    ) -> Result<ApplyDeletesResult> {
        let mut first_err = Ok(ApplyDeletesResult::new(false, 0, vec![]));
        let mut total_del_count = 0;
        let mut all_deleted = vec![];

        for seg_state in seg_states {
            let mut should_push = false;
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

        debug!("apply_deletes: {} new delete documents.", total_del_count);

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
    fn check_delete_stats(&self, updates: &[FrozenBufferUpdates]) -> bool {
        let mut num_terms2 = 0;
        let mut bytes_used2 = 0;
        for update in updates {
            num_terms2 += update.num_term_deletes;
            bytes_used2 += update.bytes_used;
        }
        assert_eq!(num_terms2, self.num_terms.load(Ordering::Acquire));
        assert_eq!(bytes_used2, self.bytes_used.load(Ordering::Acquire));
        true
    }

    pub fn get_next_gen(&self) -> u64 {
        self.next_gen.fetch_add(1, Ordering::AcqRel)
    }

    // Lock order IW -> BD
    /// Remove any BufferDeletes that we on longer need to store because
    /// all segments in the index have had the deletes applied.
    pub fn prune(&mut self, segment_infos: &SegmentInfos) {
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
        debug_assert!(!self.any());
        debug_assert!(self.check_delete_stats(&updates));
    }

    fn do_prune(&self, updates: &mut MutexGuard<Vec<FrozenBufferUpdates>>, idx: usize) {
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
            debug_assert!(self.bytes_used.load(Ordering::Acquire) >= updates[i].bytes_used);
            self.bytes_used
                .fetch_sub(updates[i].bytes_used, Ordering::AcqRel);
        }
        updates.drain(..idx);
    }
}

pub struct ApplyDeletesResult {
    // True if any actual deletes took place:
    pub any_deletes: bool,
    // Current gen, for the merged segment:
    pub gen: i64,
    // if non-empty, contains segment that are 100% deleted
    pub all_deleted: Vec<Arc<SegmentCommitInfo>>,
}

impl ApplyDeletesResult {
    fn new(any_deletes: bool, gen: i64, all_deleted: Vec<Arc<SegmentCommitInfo>>) -> Self {
        ApplyDeletesResult {
            any_deletes,
            gen,
            all_deleted,
        }
    }
}

struct SegmentState {
    del_gen: i64,
    rld: Arc<ReadersAndUpdates>,
    start_del_count: usize,
    terms_iterator: Box<TermIterator>,
    postings: Box<PostingIterator>,
    term: Option<Vec<u8>>,
    any: bool,
}

impl SegmentState {
    fn new(pool: &ReaderPool, info: &Arc<SegmentCommitInfo>) -> Result<Self> {
        let rld: Arc<ReadersAndUpdates> = pool.get_or_create(info)?;
        let start_del_count = rld.pending_delete_count() as usize;
        rld.create_reader_if_not_exist(&IO_CONTEXT_READ)?;
        let del_gen = info.buffered_deletes_gen();

        Ok(SegmentState {
            del_gen,
            rld,
            start_del_count,
            terms_iterator: Box::new(EmptyTermIterator::default()),
            postings: Box::new(EmptyPostingIterator::default()),
            term: None,
            any: false,
        })
    }

    fn finish(&mut self, pool: &ReaderPool) -> Result<()> {
        pool.release(&self.rld, true)
    }
}

struct SegmentStateRef {
    states: *const [SegmentState],
    index: usize,
}

impl SegmentStateRef {
    fn new(states: &[SegmentState], index: usize) -> Self {
        Self { states, index }
    }
}

/// use for iter terms on binary heap
impl Ord for SegmentStateRef {
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

impl PartialOrd for SegmentStateRef {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Eq for SegmentStateRef {}

impl PartialEq for SegmentStateRef {
    fn eq(&self, other: &Self) -> bool {
        unsafe {
            (*self.states)[self.index]
                .term
                .eq(&(*other.states)[self.index].term)
        }
    }
}

struct CoalescedUpdates {
    queries: HashMap<String, (Arc<Query>, DocId)>,
    terms: Vec<Arc<PrefixCodedTerms>>,
    total_term_count: usize,
}

impl Default for CoalescedUpdates {
    fn default() -> Self {
        CoalescedUpdates {
            queries: HashMap::new(),
            terms: vec![],
            total_term_count: 0,
        }
    }
}

impl CoalescedUpdates {
    fn update(&mut self, up: &mut FrozenBufferUpdates) {
        self.total_term_count += up.terms.size;
        self.terms.push(Arc::clone(&up.terms));

        for (query, _) in &up.query_and_limits {
            self.queries
                .insert(query.to_string(), (Arc::clone(query), i32::max_value()));
        }
    }

    fn term_iterator(&self) -> Result<FieldTermIterator> {
        debug_assert!(!self.terms.is_empty());
        FieldTermIterator::build(&self.terms)
    }

    pub fn query_and_limits(&self) -> Vec<&(Arc<Query>, DocId)> {
        self.queries.values().collect()
    }

    pub fn any(&self) -> bool {
        self.queries.len() > 0 || self.terms.len() > 0
    }
}
