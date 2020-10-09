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

use core::codec::segment_infos::SegmentCommitInfo;
use core::codec::segment_infos::SegmentInfos;
use core::codec::Codec;
use core::index::merge::MergeRateLimiter;
use core::index::merge::MergeScheduler;
use core::index::reader::SegmentReader;
use core::index::writer::IndexWriter;
use core::store::directory::Directory;
use core::store::MergeInfo;
use core::util::external::Volatile;

use error::{
    ErrorKind::{IllegalArgument, RuntimeError},
    Result,
};

use core::index::merge::merge_scheduler::MAX_MERGING_COUNT;
use std::cell::Cell;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::f64;
use std::hash::{Hash, Hasher};
use std::ptr;
use std::sync::Arc;
use std::time::SystemTime;

pub const DEFAULT_NO_CFS_RATIO: f64 = 0.0;

pub const DEFAULT_MAX_CFS_SEGMENT_SIZE: u64 = i64::max_value() as u64;

/// Merge is passed to `MergePolicy#find_merge` to indicate the
/// event that triggered the merge.
#[derive(Copy, Clone, PartialEq)]
pub enum MergerTrigger {
    /// Merge was triggered by a segment flush.
    SegmentFlush,
    /// Merge was triggered by a full flush. Full flushes can be caused
    /// by a commit, NRT reader reopen or a close call on the IndexWriter.
    FullFlush,
    /// Merge has been triggered explicitly by the user,
    Explicit,
    /// Merge was triggered by a successfully finished merge.
    MergeFinished,
    /// Merge was triggered by a closing IndexWriter,
    Closing,
}

/// Expert: a MergePolicy determines the sequence of
/// primitive merge operations.
///
/// Whenever the segments in an index have been altered by
/// {@link IndexWriter}, either the addition of a newly
/// flushed segment, addition of many segments from
/// addIndexes* calls, or a previous merge that may now need
/// to cascade, {@link IndexWriter} invokes {@link
/// #findMerges} to give the MergePolicy a chance to pick
/// merges that are now required.  This method returns a
/// {@link MergeSpecification} instance describing the set of
/// merges that should be done, or null if no merges are
/// necessary.  When IndexWriter.forceMerge is called, it calls
/// {@link #findForcedMerges(SegmentInfos,int,Map, IndexWriter)} and the MergePolicy should
/// then return the necessary merges.
///
/// Note that the policy can return more than one merge at
/// a time.  In this case, if the writer is using {@link
/// SerialMergeScheduler}, the merges will be run
/// sequentially but if it is using {@link
/// ConcurrentMergeScheduler} they will be run concurrently.
///
/// The default MergePolicy is {@link TieredMergePolicy}.
pub trait MergePolicy: 'static {
    // Determine what set of merge operations are now necessary on the index.
    // {@link IndexWriter} calls this whenever there is a change to the segments.
    // This call is always synchronized on the {@link IndexWriter} instance so
    // only one thread at a time will call this method.
    // @param mergeTrigger the event that triggered the merge
    // @param segmentInfos
    //          the total set of segments in the index
    // @param writer the IndexWriter to find the merges on
    //
    fn find_merges<D, C, MS, MP>(
        &self,
        merge_trigger: MergerTrigger,
        segment_infos: &SegmentInfos<D, C>,
        writer: &IndexWriter<D, C, MS, MP>,
    ) -> Result<Option<MergeSpecification<D, C>>>
    where
        D: Directory + Send + Sync + 'static,
        C: Codec,
        MS: MergeScheduler,
        MP: MergePolicy;

    ///
    // Determine what set of merge operations is necessary in
    // order to merge to {@code <=} the specified segment count. {@link IndexWriter} calls this
    // when its {@link IndexWriter#forceMerge} method is called. This call is always
    // synchronized on the {@link IndexWriter} instance so only one thread at a
    // time will call this method.
    fn find_forced_merges<D, C, MS, MP>(
        &self,
        segment_infos: &SegmentInfos<D, C>,
        max_segment_count: u32,
        segments_to_merge: &HashMap<Arc<SegmentCommitInfo<D, C>>, bool>,
        writer: &IndexWriter<D, C, MS, MP>,
    ) -> Result<Option<MergeSpecification<D, C>>>
    where
        D: Directory + Send + Sync + 'static,
        C: Codec,
        MS: MergeScheduler,
        MP: MergePolicy;

    /// Determine what set of merge operations is necessary in order to expunge
    /// all deletes from the index.
    fn find_forced_deletes_mergers<D, C, MS, MP>(
        &self,
        segments_infos: &SegmentInfos<D, C>,
        writer: &IndexWriter<D, C, MS, MP>,
    ) -> Result<Option<MergeSpecification<D, C>>>
    where
        D: Directory + Send + Sync + 'static,
        C: Codec,
        MS: MergeScheduler,
        MP: MergePolicy;

    fn max_cfs_segment_size(&self) -> u64;

    fn no_cfs_ratio(&self) -> f64;

    ///
    // Returns true if a new segment (regardless of its origin) should use the
    // compound file format. The default implementation returns <code>true</code>
    // iff the size of the given mergedInfo is less or equal to
    // {@link #getMaxCFSSegmentSizeMB()} and the size is less or equal to the
    // TotalIndexSize * {@link #getNoCFSRatio()} otherwise <code>false</code>.
    //
    fn use_compound_file<D, C, MS, MP>(
        &self,
        infos: &SegmentInfos<D, C>,
        merged_info: &SegmentCommitInfo<D, C>,
        writer: &IndexWriter<D, C, MS, MP>,
    ) -> bool
    where
        D: Directory + Send + Sync + 'static,
        C: Codec,
        MS: MergeScheduler,
        MP: MergePolicy,
    {
        let no_cfs_ratio = self.no_cfs_ratio();
        if no_cfs_ratio <= 0.0 {
            return false;
        }

        let merged_info_size = self.size(merged_info, writer);
        if merged_info_size > self.max_cfs_segment_size() as i64 {
            return false;
        }

        if no_cfs_ratio >= 1.0 {
            return true;
        }

        let mut total_size = 0;
        for info in &infos.segments {
            total_size += self.size(info.as_ref(), writer);
        }

        merged_info_size as f64 <= no_cfs_ratio * total_size as f64
    }

    fn size<D, C, MS, MP>(
        &self,
        info: &SegmentCommitInfo<D, C>,
        writer: &IndexWriter<D, C, MS, MP>,
    ) -> i64
    where
        D: Directory + Send + Sync + 'static,
        C: Codec,
        MS: MergeScheduler,
        MP: MergePolicy,
    {
        let byte_size = info.size_in_bytes();
        let del_count = writer.num_deleted_docs(info);
        let del_ratio = if info.info.max_doc < 0 {
            0.0
        } else {
            del_count as f32 / info.info.max_doc as f32
        };
        if info.info.max_doc <= 0 {
            byte_size
        } else {
            (byte_size as f32 * (1.0 - del_ratio)) as i64
        }
    }

    /// Returns true if this single info is already fully merged (has no
    /// pending deletes, is in the same dir as the writer, and matches the
    /// current compound file setting
    fn is_merged<D, C, MS, MP>(
        &self,
        infos: &SegmentInfos<D, C>,
        info: &SegmentCommitInfo<D, C>,
        writer: &IndexWriter<D, C, MS, MP>,
    ) -> bool
    where
        D: Directory + Send + Sync + 'static,
        C: Codec,
        MS: MergeScheduler,
        MP: MergePolicy,
    {
        let has_deletions = writer.num_deleted_docs(info) > 0;
        !has_deletions
            && ptr::eq(info.info.directory.as_ref(), writer.directory().as_ref())
            && self.use_compound_file(infos, info, writer) == info.info.is_compound_file()
    }
}

///
// A MergeSpecification instance provides the information
// necessary to perform multiple merges.  It simply
// contains a list of {@link OneMerge} instances.
//
pub struct MergeSpecification<D: Directory + Send + Sync + 'static, C: Codec> {
    pub merges: Vec<OneMerge<D, C>>,
}

impl<D: Directory + Send + Sync + 'static, C: Codec> Default for MergeSpecification<D, C> {
    fn default() -> Self {
        MergeSpecification { merges: vec![] }
    }
}

impl<D: Directory + Send + Sync + 'static, C: Codec> MergeSpecification<D, C> {
    fn add(&mut self, merge: OneMerge<D, C>) {
        self.merges.push(merge);
    }
}

/// OneMerge provides the information necessary to perform
/// an individual primitive merge operation, resulting in
/// a single new segment.  The merge spec includes the
/// subset of segments to be merged as well as whether the
/// new segment should use the compound file format.
pub struct OneMerge<D: Directory, C: Codec> {
    pub id: u32,
    pub info: Option<Arc<SegmentCommitInfo<D, C>>>,
    pub register_done: bool,
    pub merge_gen: u64,
    pub is_external: bool,
    pub max_num_segments: Arc<Cell<Option<u32>>>,
    // Estimated size in bytes of the merged segment.
    pub estimated_merge_bytes: Arc<Volatile<u64>>,
    // Sum of sizeInBytes of all SegmentInfos; set by IW.mergeInit
    pub total_merge_bytes: u64,
    pub readers: Vec<Arc<SegmentReader<D, C>>>,
    /// segments to be merged
    pub segments: Vec<Arc<SegmentCommitInfo<D, C>>>,
    /// a private `RateLimiter` for this merge, used to rate limit writes and abort.
    pub rate_limiter: Arc<MergeRateLimiter>,
    pub merge_start_time: Arc<Volatile<Option<SystemTime>>>,
    /// Total number of documents in segments to be merged, not accounting for deletions.
    pub total_max_doc: u32,
    // error: Result<()>
}

unsafe impl<D: Directory + Send + Sync, C: Codec> Send for OneMerge<D, C> {}

unsafe impl<D: Directory + Send + Sync, C: Codec> Sync for OneMerge<D, C> {}

impl<D: Directory, C: Codec> OneMerge<D, C> {
    pub fn new(segments: Vec<Arc<SegmentCommitInfo<D, C>>>, id: u32) -> Result<Self> {
        if segments.is_empty() {
            bail!(RuntimeError("segments must not be empty!".into()));
        }

        let count: i32 = segments.iter().map(|s| s.info.max_doc).sum();
        let rate_limiter = Arc::new(MergeRateLimiter::new());
        Ok(OneMerge {
            id,
            info: None,
            register_done: false,
            merge_gen: 0,
            is_external: false,
            max_num_segments: Arc::new(Cell::new(None)),
            estimated_merge_bytes: Arc::new(Volatile::new(0)),
            total_merge_bytes: 0,
            readers: vec![],
            segments,
            rate_limiter,
            merge_start_time: Arc::new(Volatile::new(None)),
            total_max_doc: count as u32,
        })
    }

    pub fn running_info(&self) -> OneMergeRunningInfo<D, C> {
        OneMergeRunningInfo {
            id: self.id,
            info: self.info.clone(),
            max_num_segments: Arc::clone(&self.max_num_segments),
            estimated_merge_bytes: Arc::clone(&self.estimated_merge_bytes),
            rate_limiter: Arc::clone(&self.rate_limiter),
            merge_start_time: Arc::clone(&self.merge_start_time),
        }
    }

    pub fn schedule_info(&self) -> OneMergeScheduleInfo {
        OneMergeScheduleInfo {
            id: self.id,
            max_num_segments: Arc::clone(&self.max_num_segments),
            estimated_merge_bytes: Arc::clone(&self.estimated_merge_bytes),
            rate_limiter: Arc::clone(&self.rate_limiter),
            merge_start_time: Arc::clone(&self.merge_start_time),
        }
    }

    pub fn store_merge_info(&self) -> MergeInfo {
        MergeInfo::new(
            self.total_max_doc,
            self.estimated_merge_bytes.read(),
            self.is_external,
            self.max_num_segments.get(),
        )
    }
}

impl<D: Directory, C: Codec> Hash for OneMerge<D, C> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // use id for hash
        state.write_u32(self.id);
    }
}

impl<D: Directory, C: Codec> Eq for OneMerge<D, C> {}

impl<D: Directory, C: Codec> PartialEq for OneMerge<D, C> {
    fn eq(&self, other: &OneMerge<D, C>) -> bool {
        self.id == other.id
    }
}

/// used in IndexWriter for OneMerge running status
pub struct OneMergeRunningInfo<D: Directory, C: Codec> {
    pub id: u32,
    pub info: Option<Arc<SegmentCommitInfo<D, C>>>,
    pub max_num_segments: Arc<Cell<Option<u32>>>,
    pub estimated_merge_bytes: Arc<Volatile<u64>>,
    pub rate_limiter: Arc<MergeRateLimiter>,
    pub merge_start_time: Arc<Volatile<Option<SystemTime>>>,
}

/// used in IndexWriter for OneMerge running status
pub struct OneMergeScheduleInfo {
    pub id: u32,
    pub max_num_segments: Arc<Cell<Option<u32>>>,
    pub estimated_merge_bytes: Arc<Volatile<u64>>,
    pub rate_limiter: Arc<MergeRateLimiter>,
    pub merge_start_time: Arc<Volatile<Option<SystemTime>>>,
}

unsafe impl Send for OneMergeScheduleInfo {}

unsafe impl Sync for OneMergeScheduleInfo {}

/// Merges segments of approximately equal size, subject to
/// an allowed number of segments per tier.  This is similar
/// to {@link LogByteSizeMergePolicy}, except this merge
/// policy is able to merge non-adjacent segment, and
/// separates how many segments are merged at once ({@link
/// #setMaxMergeAtOnce}) from how many segments are allowed
/// per tier ({@link #setSegmentsPerTier}).  This merge
/// policy also does not over-merge (i.e. cascade merges).
///
/// For normal merging, this policy first computes a
/// "budget" of how many segments are allowed to be in the
/// index.  If the index is over-budget, then the policy
/// sorts segments by decreasing size (pro-rating by percent
/// deletes), and then finds the least-cost merge.  Merge
/// cost is measured by a combination of the "skew" of the
/// merge (size of largest segment divided by smallest segment),
/// total merge size and percent deletes reclaimed,
/// so that merges with lower skew, smaller size
/// and those reclaiming more deletes, are
/// favored.
///
/// If a merge will produce a segment that's larger than
/// {@link #setMaxMergedSegmentMB}, then the policy will
/// merge fewer segments (down to 1 at once, if that one has
/// deletions) to keep the segment size under budget.
///     
/// NOTE: this policy freely merges non-adjacent
/// segments; if this is a problem, use {@link
/// LogMergePolicy}.
///
/// NOTE: This policy always merges by byte size
/// of the segments, always pro-rates by percent deletes,
/// and does not apply any maximum segment size during
/// forceMerge (unlike {@link LogByteSizeMergePolicy}).

// TODO
//   - we could try to take into account whether a large merge is already running (under CMS) and
//     then bias ourselves towards picking smaller merges if so (or, maybe CMS should do so)
pub struct TieredMergePolicy {
    no_cfs_ratio: f64,
    max_cfs_segment_size: u64,
    max_merge_at_once: u32,
    max_merged_segment_bytes: u64,
    max_merge_at_once_explicit: u32,
    floor_segment_bytes: u32,
    segs_per_tier: f64,
    force_merge_deletes_pct_allowed: f64,
    reclaim_deletes_weight: f64,
}

impl Default for TieredMergePolicy {
    fn default() -> Self {
        TieredMergePolicy {
            no_cfs_ratio: DEFAULT_NO_CFS_RATIO,
            max_cfs_segment_size: DEFAULT_MAX_CFS_SEGMENT_SIZE,
            max_merge_at_once: 10,
            max_merged_segment_bytes: 5 * 1024 * 1024 * 1024,
            max_merge_at_once_explicit: 30,
            floor_segment_bytes: 2 * 1024 * 1024,
            segs_per_tier: 5.0,
            force_merge_deletes_pct_allowed: 10.0,
            reclaim_deletes_weight: 2.0,
        }
    }
}

impl TieredMergePolicy {
    pub fn set_max_merge_at_once(&mut self, v: u32) -> Result<()> {
        if v < 2 {
            bail!(IllegalArgument(format!(
                "max_merge_at_once must be > 1, got {}",
                v
            )));
        }
        self.max_merge_at_once = v;
        Ok(())
    }

    pub fn set_max_merge_at_once_explicit(&mut self, v: u32) -> Result<()> {
        if v < 2 {
            bail!(IllegalArgument(format!(
                "max_merge_at_once_explicit must be > 1, got {}",
                v
            )));
        }
        self.max_merge_at_once_explicit = v;
        Ok(())
    }

    pub fn set_segs_per_tier(&mut self, v: f64) -> Result<()> {
        if v < 0.0 {
            bail!(IllegalArgument(format!(
                "segs_per_tier must be > 0, got {}",
                v
            )));
        }
        self.segs_per_tier = v;
        Ok(())
    }

    pub fn set_max_merged_segment_mb(&mut self, mut v: f64) -> Result<()> {
        if v < 0.0 {
            bail!(IllegalArgument(format!(
                "max_merged_segment_bytes must be >= 0, got {}",
                v
            )));
        }
        v *= 1024.0 * 1024.0;
        let max_bytes = if v > i64::max_value() as f64 {
            i64::max_value() as u64
        } else {
            v as u64
        };
        self.max_merged_segment_bytes = max_bytes;
        Ok(())
    }

    fn floor_size(&self, bytes: i64) -> i64 {
        bytes.max(self.floor_segment_bytes as i64)
    }

    /// Expert: scores one merge; subclasses can override.
    fn score<D, C, MS, MP>(
        &self,
        candidate: &[&Arc<SegmentCommitInfo<D, C>>],
        hit_too_large: bool,
        _merging_bytes: i64,
        writer: &IndexWriter<D, C, MS, MP>,
    ) -> MergeScore
    where
        D: Directory + Send + Sync + 'static,
        C: Codec,
        MS: MergeScheduler,
        MP: MergePolicy,
    {
        let mut total_before_merge_bytes = 0;
        let mut total_after_merge_bytes = 0;
        let mut total_after_merge_bytes_floored = 0;
        for info in candidate {
            let seg_bytes = self.size(info.as_ref(), writer);
            total_after_merge_bytes = seg_bytes;
            total_after_merge_bytes_floored += self.floor_size(seg_bytes);
            total_before_merge_bytes += info.size_in_bytes();
        }

        // Roughly measure "skew" of the merge, i.e. how
        // "balanced" the merge is (whether the segments are
        // about the same size), which can range from
        // 1.0/numSegsBeingMerged (good) to 1.0 (poor). Heavily
        // lopsided merges (skew near 1.0) is no good; it means
        // O(N^2) merge cost over time:
        let skew = if hit_too_large {
            // Pretend the merge has perfect skew; skew doesn't
            // matter in this case because this merge will not
            // "cascade" and so it cannot lead to N^2 merge cost
            // over time:
            1.0 / self.max_merge_at_once as f64
        } else {
            self.floor_size(self.size(candidate[0].as_ref(), writer)) as f64
                / total_after_merge_bytes_floored as f64
        };

        // Strongly favor merges with less skew (smaller
        // mergeScore is better):
        let mut merge_score = skew;

        // Gently favor smaller merges over bigger ones.  We
        // don't want to make this exponent too large else we
        // can end up doing poor merges of small segments in
        // order to avoid the large merges:
        merge_score *= f64::powf(total_after_merge_bytes as f64, 0.05);

        // Strongly favor merges that reclaim deletes:
        let non_del_ratio = total_after_merge_bytes as f64 / total_before_merge_bytes as f64;
        merge_score *= f64::powf(non_del_ratio, self.reclaim_deletes_weight);

        MergeScore::new(merge_score, skew, non_del_ratio)
    }

    fn find_merges_explicit<D, C, MS, MP>(
        &self,
        segment_infos: &SegmentInfos<D, C>,
        writer: &IndexWriter<D, C, MS, MP>,
    ) -> Result<Option<MergeSpecification<D, C>>>
    where
        D: Directory + Send + Sync + 'static,
        C: Codec,
        MS: MergeScheduler,
        MP: MergePolicy,
    {
        if segment_infos.len() < 1 {
            return Ok(None);
        }

        let mut infos_sorted = segment_infos.segments.clone();
        let comparator = SegmentByteSizeDescending::new(writer, self);
        infos_sorted.sort_by(|o1, o2| comparator.compare(o1.as_ref(), o2.as_ref()));

        let mut next_idx = 0;
        let mut info_seg_bytes = vec![];
        let mut merge_bytes = 0;
        for info in infos_sorted.iter() {
            let seg_bytes = self.size(info.as_ref(), writer);
            info_seg_bytes.push(seg_bytes);

            if seg_bytes > (self.max_merged_segment_bytes as f64 * 0.8) as i64 {
                next_idx += 1;
            } else {
                merge_bytes += seg_bytes;
            }
        }

        let min_segment = self.segs_per_tier as i64;
        let reserved_min = if next_idx as i64 >= min_segment {
            self.max_merged_segment_bytes as i64
        } else {
            (merge_bytes / min_segment).min(self.max_merged_segment_bytes as i64)
        };
        info!(
            "merge_bytes={} reserved_min={} next_idx={} info_seg_bytes={:?}",
            merge_bytes, reserved_min, next_idx, info_seg_bytes
        );

        let merging = writer.merging_segments();
        let mut to_be_merged = HashSet::new();
        let mut candidates = vec![];

        for i in next_idx..infos_sorted.len() {
            if merging.contains(&infos_sorted[i].info.name)
                || to_be_merged.contains(&infos_sorted[i].info.name)
            {
                continue;
            }

            let mut next_merges = vec![i];
            let mut curr_merge_bytes = info_seg_bytes[i];
            for j in i + 1..infos_sorted.len() {
                if curr_merge_bytes > self.max_merged_segment_bytes as i64
                    || next_merges.len() >= self.max_merge_at_once as usize
                    || (info_seg_bytes[i]
                        > (self.max_merged_segment_bytes as f64 / self.segs_per_tier) as i64
                        && info_seg_bytes[i] > info_seg_bytes[j] * self.max_merge_at_once as i64)
                {
                    break;
                } else if curr_merge_bytes + info_seg_bytes[j] > reserved_min
                    || merging.contains(&infos_sorted[j].info.name)
                    || to_be_merged.contains(&infos_sorted[j].info.name)
                {
                    continue;
                }

                next_merges.push(j);
                curr_merge_bytes += info_seg_bytes[j];
            }

            if next_merges.len() == 1 {
                continue;
            }

            let next_merges_bytes: Vec<i64> =
                next_merges.iter().map(|i| info_seg_bytes[*i]).collect();

            info!(
                "segment_count={} curr_merge_bytes={} one_merge={:?}",
                next_merges_bytes.len(),
                curr_merge_bytes,
                next_merges_bytes
            );
            let mut segments = Vec::with_capacity(next_merges.len());
            for idx in next_merges {
                segments.push(infos_sorted[idx].clone());
                to_be_merged.insert(infos_sorted[idx].info.name.clone());
            }

            let merge = OneMerge::new(segments, writer.next_merge_id())?;
            candidates.push(merge);

            if candidates.len() > MAX_MERGING_COUNT {
                break;
            }
        }

        let mut spec = MergeSpecification::default();

        loop {
            if let Some(one_merge) = candidates.pop() {
                spec.add(one_merge);
            } else {
                break;
            }
        }

        if spec.merges.is_empty() {
            return Ok(None);
        } else {
            return Ok(Some(spec));
        }
    }
}

impl MergePolicy for TieredMergePolicy {
    fn find_merges<D, C, MS, MP>(
        &self,
        merge_trigger: MergerTrigger,
        segment_infos: &SegmentInfos<D, C>,
        writer: &IndexWriter<D, C, MS, MP>,
    ) -> Result<Option<MergeSpecification<D, C>>>
    where
        D: Directory + Send + Sync + 'static,
        C: Codec,
        MS: MergeScheduler,
        MP: MergePolicy,
    {
        match merge_trigger {
            MergerTrigger::Explicit => {
                return self.find_merges_explicit(segment_infos, writer);
            }
            _ => {}
        }

        if segment_infos.len() == 0 {
            return Ok(None);
        }

        let mut infos_sorted = segment_infos.segments.clone();
        let comparator = SegmentByteSizeDescending::new(writer, self);
        infos_sorted.sort_by(|o1, o2| comparator.compare(o1.as_ref(), o2.as_ref()));

        // Compute total index bytes & print details about the index
        let mut total_index_bytes = 0;
        let mut min_segment_bytes = i64::max_value();
        let mut info_seg_bytes = vec![];
        let mut info_max_docs = vec![];
        for info in &infos_sorted {
            let seg_bytes = self.size(info.as_ref(), writer);
            min_segment_bytes = seg_bytes.min(min_segment_bytes);
            // Accumulate total byte size
            total_index_bytes += seg_bytes;
            info_seg_bytes.push(seg_bytes);
            info_max_docs.push(info.info.max_doc);
        }

        // If we have too-large segments, grace them out of the max_segment_count
        let mut too_big_count = 0;
        while too_big_count < infos_sorted.len() {
            let seg_bytes = info_seg_bytes[too_big_count];
            if seg_bytes < self.max_merged_segment_bytes as i64 / 2 {
                break;
            }
            total_index_bytes -= seg_bytes;
            too_big_count += 1;
        }

        let mut last_seg_bytes = i64::max_value();
        let mut last_level_infos = vec![];
        let mut info_levels = vec![];
        let mut i = too_big_count;
        while i < infos_sorted.len() {
            let seg_bytes = info_seg_bytes[i];
            if last_seg_bytes >= seg_bytes * self.segs_per_tier as i64 {
                if !last_level_infos.is_empty() {
                    info_levels.push(last_level_infos);
                    last_level_infos = vec![];
                }

                last_seg_bytes = seg_bytes;
            }

            last_level_infos.push(seg_bytes);
            i += 1;
        }

        for info_level in &info_levels {
            if info_level.len() < self.segs_per_tier as usize {
                for _i in 0..info_level.len() {
                    total_index_bytes -= info_seg_bytes[too_big_count];
                    too_big_count += 1;
                }
            }
        }

        // Compute max allowed segs in the index
        let mut level_size = self.floor_size(min_segment_bytes);
        let mut bytes_left = total_index_bytes;
        let mut allowed_seg_count = 0.0;
        loop {
            let seg_count_level = bytes_left as f64 / level_size as f64;
            if seg_count_level < self.segs_per_tier {
                allowed_seg_count += seg_count_level.ceil();
                break;
            }
            allowed_seg_count += self.segs_per_tier;
            bytes_left -= (self.segs_per_tier * level_size as f64) as i64;
            level_size *= self.max_merge_at_once as i64;
        }

        let merging = writer.merging_segments();
        let mut to_be_merged = HashSet::new();
        let allowed_seg_count_int = allowed_seg_count as u32;
        let mut spec = MergeSpecification::default();

        // Cycle to possibly select more than one merge:
        loop {
            let mut merging_bytes = 0;

            // Gather eligible segments for merging, ie segments
            // not already being merged and not already picked (by
            // prior iteration of this loop) for merging:
            let mut eligible = vec![];
            for info in &infos_sorted[too_big_count..] {
                if merging.contains(&info.info.name) {
                    merging_bytes += self.size(info.as_ref(), writer);
                } else if !to_be_merged.contains(info) {
                    eligible.push(info);
                }
            }

            let max_merge_is_running = merging_bytes >= self.max_merged_segment_bytes as i64;

            if eligible.is_empty() {
                if spec.merges.is_empty() {
                    return Ok(None);
                } else {
                    return Ok(Some(spec));
                }
            }

            if eligible.len() > allowed_seg_count_int as usize {
                // OK we are over budget -- find best merge!
                let mut best_score = MergeScore::new(f64::INFINITY, 0.0, 0.0);
                let mut best = vec![];
                let mut best_too_large = false;
                let mut best_merge_bytes = 0;

                // Consider all merge starts:
                if eligible.len() >= self.max_merge_at_once as usize {
                    let mut start_idx = 0;
                    while start_idx <= eligible.len() - self.max_merge_at_once as usize {
                        let mut total_after_merge_bytes = 0;
                        let mut candidate = vec![];
                        let mut hit_too_large = false;
                        let mut idx = start_idx;
                        while idx < eligible.len()
                            && candidate.len() < self.max_merge_at_once as usize
                        {
                            let info = eligible[idx];
                            let seg_bytes = self.size(info, writer);

                            if total_after_merge_bytes + seg_bytes
                                > self.max_merged_segment_bytes as i64
                            {
                                hit_too_large = true;
                            // NOTE: we continue, so that we can try
                            // "packing" smaller segments into this merge
                            // to see if we can get closer to the max
                            // size; this in general is not perfect since
                            // this is really "bin packing" and we'd have
                            // to try different permutations.
                            } else {
                                candidate.push(info);
                                total_after_merge_bytes += seg_bytes;
                            }

                            idx += 1;
                        }

                        // We should never see an empty candidate: we iterated over maxMergeAtOnce
                        // segments, and already pre - excluded the too - large segments:
                        debug_assert!(!candidate.is_empty());

                        let score = self.score(&candidate, hit_too_large, merging_bytes, writer);
                        debug!(
                            "maybe={:?}, score={} {}, too_large={} size={} MB",
                            &candidate,
                            score.score(),
                            score.explanation(),
                            hit_too_large,
                            (total_after_merge_bytes as f64) / 1024.0 / 1024.0
                        );
                        // If we are already running a max sized merge
                        // (maxMergeIsRunning), don't allow another max
                        // sized merge to kick off:
                        if score.score() < best_score.score()
                            && (!hit_too_large || !max_merge_is_running)
                        {
                            best = candidate;
                            best_score = score;
                            best_too_large = hit_too_large;
                            best_merge_bytes = total_after_merge_bytes;
                        }

                        start_idx += 1;
                    }
                }

                if best.len() > 1 {
                    let mut segments = Vec::with_capacity(best.len());
                    for s in best {
                        segments.push(Arc::clone(s));
                    }
                    let merge = OneMerge::new(segments, writer.next_merge_id())?;
                    for info in &merge.segments {
                        to_be_merged.insert(Arc::clone(info));
                    }
                    debug!(
                        "add merge={:?} size={} MB, score={} {}, {}",
                        &merge.segments,
                        (best_merge_bytes as f64) / 1024.0 / 1024.0,
                        best_score.score(),
                        best_score.explanation(),
                        if best_too_large { "[max merge]" } else { "" }
                    );
                    spec.add(merge);
                } else if spec.merges.is_empty() {
                    return Ok(None);
                } else {
                    return Ok(Some(spec));
                }
            } else if spec.merges.is_empty() {
                return Ok(None);
            } else {
                return Ok(Some(spec));
            }
        }
    }

    fn find_forced_merges<D, C, MS, MP>(
        &self,
        segment_infos: &SegmentInfos<D, C>,
        max_segment_count: u32,
        segments_to_merge: &HashMap<Arc<SegmentCommitInfo<D, C>>, bool>,
        writer: &IndexWriter<D, C, MS, MP>,
    ) -> Result<Option<MergeSpecification<D, C>>>
    where
        D: Directory + Send + Sync + 'static,
        C: Codec,
        MS: MergeScheduler,
        MP: MergePolicy,
    {
        let mut eligible = vec![];
        let mut force_merge_running = false;
        let merging = &writer.merging_segments();
        let mut segment_is_original = false;

        for info in &segment_infos.segments {
            if let Some(is_original) = segments_to_merge.get(info) {
                segment_is_original = *is_original;
                if !merging.contains(&info.info.name) {
                    eligible.push(info);
                } else {
                    force_merge_running = true;
                }
            }
        }

        if eligible.is_empty() {
            return Ok(None);
        }

        if (max_segment_count > 1 && eligible.len() <= max_segment_count as usize)
            || (max_segment_count == 1
                && eligible.len() == 1
                && (!segment_is_original
                    || self.is_merged(segment_infos, eligible[0].as_ref(), writer)))
        {
            return Ok(None);
        }

        {
            let comparator = SegmentByteSizeDescending::new(writer, self);
            eligible.sort_by(|s1, s2| comparator.compare(s1.as_ref(), s2.as_ref()));
        }

        let mut end = eligible.len();
        let mut spec = MergeSpecification::default();

        // Do full merges, first, backwards:
        while end >= (self.max_merge_at_once_explicit + max_segment_count - 1) as usize {
            let mut segments = vec![];
            for i in end - self.max_merge_at_once_explicit as usize..end {
                segments.push(Arc::clone(eligible[i]));
            }
            let merge = OneMerge::new(segments, writer.next_merge_id())?;
            spec.add(merge);
            end -= self.max_merge_at_once_explicit as usize;
        }

        if spec.merges.is_empty() && !force_merge_running {
            // Do final merge
            let num_to_merge = end + 1 - max_segment_count as usize;
            let mut segments = vec![];
            for i in end - num_to_merge..end {
                segments.push(Arc::clone(eligible[i]));
            }
            let merge = OneMerge::new(segments, writer.next_merge_id())?;
            spec = MergeSpecification::default();
            spec.add(merge);
        }

        if spec.merges.is_empty() {
            Ok(None)
        } else {
            Ok(Some(spec))
        }
    }

    fn find_forced_deletes_mergers<D, C, MS, MP>(
        &self,
        segments_infos: &SegmentInfos<D, C>,
        writer: &IndexWriter<D, C, MS, MP>,
    ) -> Result<Option<MergeSpecification<D, C>>>
    where
        D: Directory + Send + Sync + 'static,
        C: Codec,
        MS: MergeScheduler,
        MP: MergePolicy,
    {
        let mut eligible = vec![];
        let merging = &writer.merging_segments();

        for info in &segments_infos.segments {
            let pct_deletes =
                100.0 * writer.num_deleted_docs(info.as_ref()) as f64 / info.info.max_doc as f64;
            if pct_deletes > self.force_merge_deletes_pct_allowed
                && !merging.contains(&info.info.name)
            {
                eligible.push(info);
            }
        }

        if eligible.is_empty() {
            return Ok(None);
        }

        {
            let comparator = SegmentByteSizeDescending::new(writer, self);
            eligible.sort_by(|s1, s2| comparator.compare(s1.as_ref(), s2.as_ref()));
        }

        let mut start = 0;
        let mut spec = MergeSpecification::default();

        while start < eligible.len() {
            // Don't enforce max merged size here: app is explicitly
            // calling forceMergeDeletes, and knows this may take a
            // long time / produce big segments (like forceMerge):
            let end = eligible
                .len()
                .min(start + self.max_merge_at_once_explicit as usize);
            let mut segments = vec![];
            for i in start..end {
                segments.push(Arc::clone(eligible[i]));
            }
            let merge = OneMerge::new(segments, writer.next_merge_id())?;
            spec.add(merge);
            start = end;
        }
        if spec.merges.is_empty() {
            Ok(None)
        } else {
            Ok(Some(spec))
        }
    }

    fn max_cfs_segment_size(&self) -> u64 {
        self.max_cfs_segment_size
    }

    fn no_cfs_ratio(&self) -> f64 {
        self.no_cfs_ratio
    }
}

struct SegmentByteSizeDescending<
    'a,
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
> {
    writer: &'a IndexWriter<D, C, MS, MP>,
    policy: &'a TieredMergePolicy,
}

impl<'a, D, C, MS, MP> SegmentByteSizeDescending<'a, D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    fn new(writer: &'a IndexWriter<D, C, MS, MP>, policy: &'a TieredMergePolicy) -> Self {
        SegmentByteSizeDescending { writer, policy }
    }
}

impl<'a, D, C, MS, MP> SegmentByteSizeDescending<'a, D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    fn compare(&self, o1: &SegmentCommitInfo<D, C>, o2: &SegmentCommitInfo<D, C>) -> Ordering {
        let sz1 = self.policy.size(o1, self.writer);
        let sz2 = self.policy.size(o2, self.writer);
        let cmp = sz2.cmp(&sz1);
        if cmp != Ordering::Equal {
            cmp
        } else {
            o1.info.name.cmp(&o2.info.name)
        }
    }
}

struct MergeScore {
    merge_score: f64,
    skew: f64,
    non_del_ratio: f64,
}

impl MergeScore {
    fn new(merge_score: f64, skew: f64, non_del_ratio: f64) -> Self {
        MergeScore {
            merge_score,
            skew,
            non_del_ratio,
        }
    }

    /// Returns the score for this merge candidate; lower scores are better.
    fn score(&self) -> f64 {
        self.merge_score
    }

    /// Human readable explanation of how the merge got this score.
    fn explanation(&self) -> String {
        format!("skew {} non_del_ratio: {}", self.skew, self.non_del_ratio)
    }
}
