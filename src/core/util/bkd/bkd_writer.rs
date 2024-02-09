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

use core::codec::points::MutablePointsReader;
use core::codec::points::{IntersectVisitor, Relation};
use core::codec::write_header;
use core::codec::{INT_BYTES, LONG_BYTES};
use core::index::merge::LiveDocsDocMap;
use core::store::directory::{Directory, TrackingDirectoryWrapper};
use core::store::io::{DataOutput, GrowableByteArrayDataOutput, IndexOutput, RAMOutputStream};
use core::util::bit_set::{BitSet, FixedBitSet, ImmutableBitSet};
use core::util::bit_util::UnsignedShift;
use core::util::bkd::{
    bkd_reader::{MergeReader, StubIntersectVisitor},
    BKDReader, DocIdsWriter, HeapPointWriter, LongBitSet, MutablePointsReaderUtils,
    OfflinePointWriter, PointReader, PointType, PointWriter, PointWriterEnum,
};
use core::util::sorter::{check_range, MSBRadixSorter, MSBSorter, Sorter};
use core::util::string_util::bytes_subtract;
use core::util::DocId;

use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;

use error::{
    ErrorKind::{IllegalArgument, IllegalState, UnsupportedOperation},
    Result,
};

pub const CODEC_NAME: &str = "BKD";
pub const VERSION_START: i32 = 0;
pub const VERSION_COMPRESSED_DOC_IDS: i32 = 1;
pub const VERSION_COMPRESSED_VALUES: i32 = 2;
pub const VERSION_IMPLICIT_SPLIT_DIM_1D: i32 = 3;
pub const VERSION_PACKED_INDEX: i32 = 4;
pub const VERSION_CURRENT: i32 = VERSION_PACKED_INDEX;
pub const DEFAULT_MAX_POINTS_IN_LEAF_NODE: i32 = 1024;
pub const DEFAULT_MAX_MB_SORT_IN_HEAP: f32 = 1024.0f32;
pub const MAX_DIMS: i32 = 8;

struct PathSlice<W: PointWriter> {
    writer: W,
    start: i64,
    count: i64,
}

impl<W: PointWriter> PathSlice<W> {
    pub fn new(writer: W, start: i64, count: i64) -> PathSlice<W> {
        PathSlice {
            writer,
            start,
            count,
        }
    }
}

pub struct OneDimIntersectVisitor<'a, D: Directory, O: IndexOutput> {
    one_dim_writer: &'a mut OneDimensionBKDWriter<'a, D, O>,
}

impl<'a, D: Directory, O: IndexOutput> OneDimIntersectVisitor<'a, D, O> {
    pub fn new(
        one_dim_writer: &'a mut OneDimensionBKDWriter<'a, D, O>,
    ) -> OneDimIntersectVisitor<'a, D, O> {
        OneDimIntersectVisitor { one_dim_writer }
    }
}

impl<'a, D: Directory, O: IndexOutput> IntersectVisitor for OneDimIntersectVisitor<'a, D, O> {
    fn visit(&mut self, _doc_id: DocId) -> Result<()> {
        bail!(IllegalState("".into()))
    }

    fn visit_by_packed_value(&mut self, doc_id: DocId, packed_value: &[u8]) -> Result<()> {
        self.one_dim_writer.add(packed_value, doc_id)
    }

    fn compare(&self, _min_packed_value: &[u8], _max_packed_value: &[u8]) -> Relation {
        Relation::CellCrossesQuery
    }
}

pub struct OneDimensionBKDWriter<'a, D: Directory, O: IndexOutput> {
    out: &'a mut O,
    leaf_block_fps: Vec<i64>,
    leaf_block_start_values: Vec<Vec<u8>>,
    leaf_values: Vec<u8>,
    leaf_docs: Vec<i32>,
    value_count: i64,
    leaf_count: usize,
    last_packed_value: Vec<u8>,
    last_doc_id: DocId,

    bkd_writer: *mut BKDWriter<D>,
}

impl<'a, D: Directory, O: IndexOutput> OneDimensionBKDWriter<'a, D, O> {
    pub fn new(
        out: &'a mut O,
        bkd_writer: &mut BKDWriter<D>,
    ) -> Result<OneDimensionBKDWriter<'a, D, O>> {
        if bkd_writer.num_dims != 1 {
            bail!(UnsupportedOperation(Cow::Owned(format!(
                "num_dims must be 1 but got {}",
                bkd_writer.num_dims
            ))));
        }
        if bkd_writer.point_count != 0 {
            bail!(IllegalState("cannot mix add and merge".into()));
        }

        // Catch user silliness:
        if bkd_writer.heap_point_writer.is_none() && bkd_writer.temp_input.is_none() {
            bail!(IllegalState("already finished".into()));
        }

        // Mark that we already finished:
        bkd_writer.heap_point_writer = None;

        Ok(OneDimensionBKDWriter {
            out,
            leaf_block_fps: vec![],
            leaf_block_start_values: vec![],
            leaf_values: vec![
                0u8;
                bkd_writer.max_points_in_leaf_node as usize
                    * bkd_writer.packed_bytes_length
            ],
            leaf_docs: vec![0i32; bkd_writer.max_points_in_leaf_node as usize],
            value_count: 0,
            leaf_count: 0,
            last_packed_value: vec![0u8; bkd_writer.packed_bytes_length],
            last_doc_id: 0,
            bkd_writer,
        })
    }

    pub fn add(&mut self, packed_value: &[u8], doc_id: i32) -> Result<()> {
        let bkd_writer = unsafe { &mut (*self.bkd_writer) };

        debug_assert!(bkd_writer
            .value_in_order(
                self.value_count + self.leaf_count as i64,
                0,
                &self.last_packed_value,
                packed_value,
                doc_id,
                self.last_doc_id,
            )
            .is_ok());

        self.last_packed_value[0..bkd_writer.packed_bytes_length]
            .copy_from_slice(&packed_value[0..bkd_writer.packed_bytes_length]);

        let offset = self.leaf_count * bkd_writer.packed_bytes_length;
        self.leaf_values[offset..offset + bkd_writer.packed_bytes_length]
            .copy_from_slice(&packed_value[..bkd_writer.packed_bytes_length]);
        self.leaf_docs[self.leaf_count] = doc_id;
        bkd_writer.docs_seen.set(doc_id as usize);
        self.leaf_count += 1;

        if self.value_count > bkd_writer.total_point_count {
            bail!(IllegalState(format!(
                "total_point_count={}, was passed when we were created, but we just hit {} values",
                bkd_writer.total_point_count, bkd_writer.point_count
            )));
        }

        if self.leaf_count as i32 == bkd_writer.max_points_in_leaf_node {
            // We write a block once we hit exactly the max count ... this is different from
            // when we write N > 1 dimensional points where we write between max/2 and max per leaf
            // block
            self.write_leaf_block()?;
            self.leaf_count = 0;
        }

        // only assign when asserts are enabled
        debug_assert!(doc_id >= 0);
        self.last_doc_id = doc_id;

        Ok(())
    }

    pub fn finish(&mut self) -> Result<i64> {
        if self.leaf_count > 0 {
            self.write_leaf_block()?;
            self.leaf_count = 0;
        }

        if self.value_count == 0 {
            return Ok(-1);
        }

        let bkd_writer = unsafe { &mut (*self.bkd_writer) };

        bkd_writer.point_count = self.value_count;

        let index_fp = self.out.file_pointer();
        let num_inner_nodes = self.leaf_block_start_values.len();
        let mut index: Vec<u8> = vec![0u8; (1 + num_inner_nodes) * (1 + bkd_writer.bytes_per_dim)];

        bkd_writer.rotate_to_tree(
            1,
            0,
            num_inner_nodes as i32,
            &mut index,
            &self.leaf_block_start_values,
        );
        bkd_writer.write_index(self.out, &self.leaf_block_fps, &mut index)?;

        Ok(index_fp)
    }

    pub fn write_leaf_block(&mut self) -> Result<()> {
        let bkd_writer = unsafe { &mut (*self.bkd_writer) };

        debug_assert!(self.leaf_count != 0);
        if self.value_count == 0 {
            bkd_writer.min_packed_value[0..bkd_writer.packed_bytes_length]
                .copy_from_slice(&self.leaf_values[..bkd_writer.packed_bytes_length]);
        }

        let src_offset = (self.leaf_count - 1) as usize * bkd_writer.packed_bytes_length;
        bkd_writer.max_packed_value[0..bkd_writer.packed_bytes_length].copy_from_slice(
            &self.leaf_values[src_offset..src_offset + bkd_writer.packed_bytes_length],
        );

        self.value_count += self.leaf_count as i64;
        if !self.leaf_block_fps.is_empty() {
            // Save the first (minimum) value in each leaf block except the first, to build the
            // split value index in the end:
            let to_copy = bkd_writer.packed_bytes_length.min(self.leaf_values.len());
            self.leaf_block_start_values
                .push(self.leaf_values[0..to_copy].to_vec());
        }

        self.leaf_block_fps.push(self.out.file_pointer());
        bkd_writer.check_max_leaf_node_count(self.leaf_block_fps.len() as i32)?;

        // Find per-dim common prefix:
        let mut prefix = bkd_writer.bytes_per_dim;
        let offset = (self.leaf_count - 1) as usize * bkd_writer.packed_bytes_length;
        for j in 0..bkd_writer.bytes_per_dim as usize {
            if self.leaf_values[j] != self.leaf_values[offset + j] {
                prefix = j;
                break;
            }
        }

        bkd_writer.common_prefix_lengths[0] = prefix;
        debug_assert!(bkd_writer.scratch_out.position() == 0);

        let scratch_out = (&mut bkd_writer.scratch_out) as *mut GrowableByteArrayDataOutput;
        let common_prefix_lengths: *mut [usize] = bkd_writer.common_prefix_lengths.as_mut();

        bkd_writer.write_leaf_block_docs(
            unsafe { &mut (*scratch_out) },
            &self.leaf_docs,
            0,
            self.leaf_count,
        )?;
        bkd_writer.write_common_prefixes(
            unsafe { &mut *scratch_out },
            unsafe { &mut *common_prefix_lengths },
            &mut self.leaf_values,
        )?;

        let mut packed_values: Vec<&[u8]> = Vec::with_capacity(self.leaf_count);
        for i in 0..self.leaf_count {
            let offset = bkd_writer.packed_bytes_length * i;
            packed_values.push(&self.leaf_values[offset..offset + bkd_writer.packed_bytes_length]);
        }

        let from = (self.leaf_count - 1) * bkd_writer.packed_bytes_length;
        let to = self.leaf_count * bkd_writer.packed_bytes_length;
        debug_assert!(bkd_writer.value_in_order_and_bounds(
            self.leaf_count,
            0,
            &self.leaf_values[0..bkd_writer.packed_bytes_length],
            &self.leaf_values[from..to],
            &packed_values,
            &self.leaf_docs,
        ));

        bkd_writer.write_leaf_block_packed_values(
            unsafe { &mut (*scratch_out) },
            unsafe { &mut (*common_prefix_lengths) },
            self.leaf_count,
            0,
            &mut packed_values,
        )?;
        let length = bkd_writer.scratch_out.position();
        self.out
            .write_bytes(&bkd_writer.scratch_out.bytes, 0, length)?;
        bkd_writer.scratch_out.reset();

        Ok(())
    }
}

pub struct BKDWriter<D: Directory> {
    _bytes_per_doc: usize,
    num_dims: usize,
    bytes_per_dim: usize,
    packed_bytes_length: usize,
    temp_dir: TrackingDirectoryWrapper<D, Arc<D>>,
    temp_file_name_prefix: String,
    _max_mb_sort_in_heap: f64,
    scratch_diff: Vec<u8>,
    scratch1: Vec<u8>,
    scratch2: Vec<u8>,
    //_scratch_ref1: Vec<u8>,
    // _scratch_ref2: Vec<u8>,
    common_prefix_lengths: Vec<usize>,

    docs_seen: FixedBitSet,

    offline_point_writer: Option<OfflinePointWriter<D>>,
    heap_point_writer: Option<HeapPointWriter>,

    temp_input: Option<String>,
    max_points_in_leaf_node: i32,
    max_points_sort_in_heap: i32,

    pub point_count: i64,
    total_point_count: i64,
    long_ords: bool,

    single_value_per_doc: bool,

    max_doc: i32,

    min_packed_value: Vec<u8>,
    max_packed_value: Vec<u8>,
    scratch_out: GrowableByteArrayDataOutput,
}

impl<D: Directory> BKDWriter<D> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        max_doc: i32,
        temp_dir: Arc<D>,
        temp_file_name_prefix: &str,
        num_dims: usize,
        bytes_per_dim: usize,
        max_points_in_leaf_node: i32,
        max_mb_sort_in_heap: f64,
        total_point_count: i64,
        single_value_per_doc: bool,
    ) -> Result<BKDWriter<D>> {
        Self::verify_params(
            num_dims,
            max_points_in_leaf_node,
            max_mb_sort_in_heap,
            total_point_count,
        )?;
        // We use tracking dir to deal with removing files on exception, so each place that
        // creates temp files doesn't need crazy try/finally/sucess logic:
        // If we may have more than 1+Integer.MAX_VALUE values, then we must encode ords with long
        // (8 bytes), else we can use int (4 bytes).
        let long_ords = total_point_count > i32::max_value() as i64;

        let packed_bytes_length = num_dims * bytes_per_dim;
        // dimensional values (numDims * bytesPerDim) + ord (int or long) + docID (int)
        let bytes_per_doc = if single_value_per_doc {
            // Lucene only supports up to 2.1 docs, so we better not need longOrds in this case:
            debug_assert!(!long_ords);
            packed_bytes_length + INT_BYTES as usize
        } else if long_ords {
            packed_bytes_length + (LONG_BYTES + INT_BYTES) as usize
        } else {
            packed_bytes_length + (INT_BYTES + INT_BYTES) as usize
        };

        // As we recurse, we compute temporary partitions of the data, halving the
        // number of points at each recursion.  Once there are few enough points,
        // we can switch to sorting in heap instead of offline (on disk).  At any
        // time in the recursion, we hold the number of points at that level, plus
        // all recursive halves (i.e. 16 + 8 + 4 + 2) so the memory usage is 2X
        // what that level would consume, so we multiply by 0.5 to convert from
        // bytes to points here.  Each dimension has its own sorted partition, so
        // we must divide by numDims as wel.
        let max_points_sort_in_heap = (0.5 * (max_mb_sort_in_heap * 1024f64 * 1024f64)
            / (bytes_per_doc as f64 * num_dims as f64))
            as i32;
        // Finally, we must be able to hold at least the leaf node in heap during build:
        if max_points_sort_in_heap < max_points_in_leaf_node {
            bail!(IllegalArgument(format!(
                "max_mb_sort_in_heap={} only allows for max_mb_sort_in_heap={}, but this is less \
                 than max_points_in_leaf_node={},; either increase max_points_sort_in_heap or \
                 decrease max_points_in_leaf_node",
                max_mb_sort_in_heap, max_points_sort_in_heap, max_points_in_leaf_node
            )));
        }

        // We write first maxPointsSortInHeap in heap, then cutover to offline for additional
        // points:
        let heap_point_writer = HeapPointWriter::new(
            16,
            max_points_sort_in_heap as usize,
            packed_bytes_length,
            long_ords,
            single_value_per_doc,
        );

        Ok(BKDWriter {
            _bytes_per_doc: bytes_per_doc,
            num_dims,
            bytes_per_dim,
            packed_bytes_length,
            temp_dir: TrackingDirectoryWrapper::new(temp_dir),
            temp_file_name_prefix: temp_file_name_prefix.to_string(),
            _max_mb_sort_in_heap: max_mb_sort_in_heap,
            scratch_diff: vec![0u8; bytes_per_dim],
            scratch1: vec![0u8; packed_bytes_length],
            scratch2: vec![0u8; packed_bytes_length],
            common_prefix_lengths: vec![0; num_dims],
            docs_seen: FixedBitSet::new(max_doc as usize),
            offline_point_writer: None,
            temp_input: None,
            heap_point_writer: Some(heap_point_writer),
            max_points_in_leaf_node,
            max_points_sort_in_heap,
            point_count: 0,
            total_point_count,
            long_ords,
            single_value_per_doc,
            max_doc,
            min_packed_value: vec![0u8; packed_bytes_length],
            max_packed_value: vec![0u8; packed_bytes_length],
            scratch_out: GrowableByteArrayDataOutput::new(32 * 1024),
        })
    }

    pub fn add(&mut self, packed_value: &[u8], doc_id: DocId) -> Result<()> {
        if packed_value.len() != self.packed_bytes_length {
            bail!(IllegalArgument(format!(
                "packedValue should be length={}",
                self.packed_bytes_length
            )));
        }

        if self.point_count > self.max_points_sort_in_heap as i64 {
            if self.offline_point_writer.is_none() {
                self.spill_to_offline()?;
            }

            self.offline_point_writer.as_mut().unwrap().append(
                packed_value,
                self.point_count,
                doc_id,
            )?;
        } else {
            // Not too many points added yet, continue using heap:
            self.heap_point_writer.as_mut().unwrap().append(
                packed_value,
                self.point_count,
                doc_id,
            )?;
        }

        // TODO: we could specialize for the 1D case:
        if self.point_count == 0 {
            self.min_packed_value[0..self.packed_bytes_length]
                .copy_from_slice(&packed_value[0..self.packed_bytes_length]);
            self.max_packed_value[0..self.packed_bytes_length]
                .copy_from_slice(&packed_value[0..self.packed_bytes_length]);
        } else {
            for dim in 0..self.num_dims {
                let offset = dim * self.bytes_per_dim;
                let end_offset = (dim + 1) * self.bytes_per_dim;
                if packed_value[offset..end_offset] < self.min_packed_value[offset..end_offset] {
                    self.min_packed_value[offset..end_offset]
                        .copy_from_slice(&packed_value[offset..end_offset]);
                }
                if packed_value[offset..end_offset] > self.max_packed_value[offset..end_offset] {
                    self.max_packed_value[offset..end_offset]
                        .copy_from_slice(&packed_value[offset..end_offset]);
                }
            }
        }

        self.point_count += 1;
        if self.point_count > self.total_point_count {
            bail!(IllegalState(format!(
                "totalPointCount={} was passed when we were created, but we just hit {} values",
                self.total_point_count, self.point_count
            )));
        }

        self.docs_seen.set(doc_id as usize);

        Ok(())
    }

    pub fn write_field(
        &mut self,
        out: &mut impl IndexOutput,
        field_name: &str,
        reader: impl MutablePointsReader,
    ) -> Result<i64> {
        if self.num_dims == 1 {
            self.write_field_1_dim(out, field_name, reader)
        } else {
            unimplemented!()
        }
    }

    pub fn verify_params(
        num_dims: usize,
        max_points_in_leaf_node: i32,
        max_mb_sort_in_heap: f64,
        total_point_count: i64,
    ) -> Result<()> {
        // We encode dim in a single byte in the splitPackedValues, but we only expose 4 bits for
        // it now, in case we want to use remaining 4 bits for another purpose later
        if num_dims < 1 || num_dims as i32 > MAX_DIMS {
            bail!(IllegalArgument(format!(
                "num_dims must be 1 .. {}",
                MAX_DIMS
            )));
        }
        if max_points_in_leaf_node <= 0 {
            bail!(IllegalArgument(
                "max_points_in_leaf_node must be > 0".into()
            ));
        }
        if max_mb_sort_in_heap < 0.0 {
            bail!(IllegalArgument("max_mb_sort_in_heap must be >= 0.0".into()));
        }
        if total_point_count < 0 {
            bail!(IllegalArgument("total_point_count must be >=0".into()));
        }

        Ok(())
    }

    pub fn finish(&mut self, out: &mut impl IndexOutput) -> Result<i64> {
        // TODO: specialize the 1D case?  it's much faster at indexing time (no partitioning on
        // recurse...)

        // Catch user silliness:
        if self.heap_point_writer.is_none() && self.temp_input.is_none() {
            bail!(IllegalState("already finished".into()));
        }

        if self.offline_point_writer.is_some() {
            self.offline_point_writer.as_mut().unwrap().close()?;
        }

        if self.point_count == 0 {
            bail!(IllegalState("must index at least one point".into()));
        }

        let mut ord_bit_set = if self.num_dims > 1 {
            if self.single_value_per_doc {
                Some(LongBitSet::new(self.max_doc as i64))
            } else {
                Some(LongBitSet::new(self.point_count as i64))
            }
        } else {
            None
        };

        let mut count_per_leaf = self.point_count;
        let mut inner_node_count = 1;
        while count_per_leaf > self.max_points_in_leaf_node as i64 {
            count_per_leaf = (count_per_leaf + 1) / 2;
            inner_node_count *= 2;
        }

        let num_leaves = inner_node_count;
        self.check_max_leaf_node_count(num_leaves)?;

        // NOTE: we could save the 1+ here, to use a bit less heap at search time, but then we'd
        // need a somewhat costly check at each step of the recursion to recompute the
        // split dim:

        // Indexed by nodeID, but first (root) nodeID is 1.  We do 1+ because the lead byte at each
        // recursion says which dim we split on.
        let mut split_packed_values = vec![0u8; num_leaves as usize * (1 + self.bytes_per_dim)];
        // +1 because leaf count is power of 2 (e.g. 8), and innerNodeCount is power of 2 minus 1
        // (e.g. 7)
        let mut leaf_block_fps = vec![0i64; num_leaves as usize];
        // Make sure the math above "worked":
        debug_assert!(self.point_count / num_leaves as i64 <= self.max_points_in_leaf_node as i64);

        // Sort all docs once by each dimension:
        let mut sorted_point_writers: Vec<PathSlice<PointWriterEnum<D>>> = vec![];
        // This is only used on exception; on normal code paths we close all files we opened:
        let mut _success = false;

        for dim in 0..self.num_dims {
            sorted_point_writers.push(PathSlice::new(self.sort(dim as i32)?, 0, self.point_count));
        }

        if self.temp_input.is_some() {
            self.temp_dir
                .delete_file(self.temp_input.as_ref().unwrap())?;
            self.temp_input = None;
        } else {
            debug_assert!(self.heap_point_writer.is_some());
            self.heap_point_writer = None;
        }

        let min_packed_value = (&mut self.min_packed_value) as *mut Vec<u8>;
        let max_packed_value = (&mut self.max_packed_value) as *mut Vec<u8>;
        let mut parent_splits: Vec<i32> = vec![0i32; self.num_dims as usize];
        self.build(
            1,
            num_leaves,
            &mut sorted_point_writers,
            &mut ord_bit_set,
            out,
            unsafe { &mut (*min_packed_value) },
            unsafe { &mut (*max_packed_value) },
            &mut parent_splits,
            &mut split_packed_values,
            &mut leaf_block_fps,
        )?;

        for slice in &mut sorted_point_writers {
            slice.writer.destory()?;
        }

        // If no exception, we should have cleaned everything up:
        debug_assert!(self.temp_dir.create_files().is_empty());

        _success = true;

        //        if (_success == false) {
        //            IOUtils.deleteFilesIgnoringExceptions(tempDir, tempDir.getCreatedFiles());
        //            IOUtils.closeWhileHandlingException(toCloseHeroically);
        //        }

        // Write index:
        let index_fp = out.file_pointer();
        self.write_index(out, &leaf_block_fps, &mut split_packed_values)?;

        Ok(index_fp)
    }

    /// More efficient bulk-add for incoming {@link BKDReader}s.  This does a merge sort
    /// of the already sorted values and currently only works when numDims==1.  This
    /// returns -1 if all documents containing dimensional values were deleted.
    pub fn merge(
        &mut self,
        output: &mut impl IndexOutput,
        doc_maps: Vec<&LiveDocsDocMap>,
        readers: Vec<&BKDReader>,
    ) -> Result<i64> {
        debug_assert!(doc_maps.is_empty() || readers.len() == doc_maps.len());

        let mut stub_visitors = vec![StubIntersectVisitor::default(); readers.len()];
        let mut sub_vps: Vec<*mut StubIntersectVisitor> = stub_visitors
            .iter_mut()
            .map(|v| v as *mut StubIntersectVisitor)
            .collect();
        let mut queue = BinaryHeap::with_capacity(readers.len());

        for i in 0..readers.len() {
            let doc_map = if doc_maps.is_empty() {
                None
            } else {
                Some(doc_maps[i])
            };
            let visitor = unsafe { &mut *sub_vps[i] };
            let mut reader = MergeReader::new(readers[i], doc_map, visitor)?;
            if reader.next()? {
                queue.push(reader);
            }
        }

        let mut one_dim_writer = OneDimensionBKDWriter::new(output, self)?;

        loop {
            let mut should_pop = false;
            {
                if let Some(mut reader) = queue.peek_mut() {
                    one_dim_writer.add(&reader.state.scratch_packed_value, reader.doc_id)?;

                    if !reader.next()? {
                        // This segment was exhausted
                        should_pop = true;
                    }
                } else {
                    break;
                }

                if should_pop {
                    queue.pop();
                }
            }
        }

        one_dim_writer.finish()
    }
}

impl<D: Directory> BKDWriter<D> {
    fn spill_to_offline(&mut self) -> Result<()> {
        // For each .add we just append to this input file, then in .finish we sort this input and
        // resursively build the tree:
        self.offline_point_writer = Some(OfflinePointWriter::prefix_new(
            Arc::clone(&self.temp_dir.directory),
            &self.temp_file_name_prefix,
            self.packed_bytes_length,
            self.long_ords,
            "spill",
            0,
            self.single_value_per_doc,
        ));

        let input = self
            .offline_point_writer
            .as_ref()
            .unwrap()
            .output()
            .name()
            .to_string();
        self.temp_input = Some(input);

        let mut reader = HeapPointWriter::point_reader(
            self.heap_point_writer.as_ref().unwrap(),
            0,
            self.point_count as usize,
        )?;
        for i in 0..self.point_count {
            let has_next = reader.next()?;
            debug_assert!(has_next);

            self.offline_point_writer.as_mut().unwrap().append(
                reader.packed_value(),
                i,
                self.heap_point_writer.as_ref().unwrap().doc_ids[i as usize],
            )?;
        }

        self.heap_point_writer = None;

        Ok(())
    }

    // In the 1D case, we can simply sort points in ascending order and use the
    // same writing logic as we use at merge time.
    fn write_field_1_dim<O: IndexOutput>(
        &mut self,
        out: &mut O,
        field_name: &str,
        mut reader: impl MutablePointsReader,
    ) -> Result<i64> {
        let size = reader.size(field_name)? as i32;

        MutablePointsReaderUtils::sort(
            self.max_doc,
            self.packed_bytes_length as i32,
            &mut reader,
            0,
            size,
        );

        let mut one_dim_writer = OneDimensionBKDWriter::new(out, self)?;
        let one_dim_writer_ptr = (&mut one_dim_writer) as *mut OneDimensionBKDWriter<D, O>;
        let mut visitor = OneDimIntersectVisitor::new(unsafe { &mut (*one_dim_writer_ptr) });
        reader.intersect(field_name, &mut visitor)?;

        one_dim_writer.finish()
    }

    fn rotate_to_tree(
        &self,
        node_id: i32,
        offset: i32,
        count: i32,
        index: &mut Vec<u8>,
        leaf_block_start_values: &[Vec<u8>],
    ) {
        let dest = node_id as usize * (1 + self.bytes_per_dim) + 1;

        if count == 1 {
            // Leaf index node
            index[dest..dest + self.bytes_per_dim]
                .copy_from_slice(&leaf_block_start_values[offset as usize][0..self.bytes_per_dim]);
        } else if count > 1 {
            // Internal index node: binary partition of count
            let mut count_at_level = 1;
            let mut total_count = 0;

            loop {
                let count_left = count - total_count;
                if count_left <= count_at_level {
                    // This is the last level, possibly partially filled:
                    let last_left_count = count_left.min(count_at_level / 2);
                    debug_assert!(last_left_count >= 0);

                    let left_half = (total_count - 1) / 2 + last_left_count;
                    let root_offset = offset + left_half;

                    index[dest..dest + self.bytes_per_dim].copy_from_slice(
                        &leaf_block_start_values[root_offset as usize][0..self.bytes_per_dim],
                    );

                    // TODO: we could optimize/specialize, when we know it's simply fully balanced
                    // binary tree under here, to save this while loop on each
                    // recursion

                    // Recurse left
                    self.rotate_to_tree(
                        2 * node_id,
                        offset,
                        left_half,
                        index,
                        leaf_block_start_values,
                    );
                    // Recurse right
                    self.rotate_to_tree(
                        2 * node_id + 1,
                        root_offset + 1,
                        count - left_half - 1,
                        index,
                        leaf_block_start_values,
                    );
                    return;
                }

                total_count += count_at_level;
                count_at_level *= 2;
            }
        } else {
            debug_assert!(count == 0);
        }
    }

    fn pack_index(
        &mut self,
        leaf_block_fps: &[i64],
        split_packed_values: &mut Vec<u8>,
    ) -> Result<Vec<u8>> {
        let mut leaf_block_fps = leaf_block_fps.to_vec();
        let num_leaves = leaf_block_fps.len();
        // Possibly rotate the leaf block FPs, if the index not fully balanced binary tree (only
        // happens if it was created by OneDimensionBKDWriter).  In this case the leaf
        // nodes may straddle the two bottom levels of the binary tree:
        if self.num_dims == 1 && num_leaves > 1 {
            let mut level_count = 2;
            loop {
                if num_leaves >= level_count && num_leaves <= 2 * level_count {
                    let last_level = 2 * (num_leaves - level_count);

                    if last_level != 0 {
                        // Last level is partially filled, so we must rotate the leaf FPs to match.
                        // We do this here, after loading at read-time, so
                        // that we can still delta code them on disk at write:
                        let mut new_leaf_block_fps = vec![0i64; num_leaves];
                        let length = leaf_block_fps.len() - last_level;
                        new_leaf_block_fps[0..length]
                            .copy_from_slice(&leaf_block_fps[last_level..last_level + length]);
                        new_leaf_block_fps[length..length + last_level]
                            .copy_from_slice(&leaf_block_fps[0..last_level]);
                        leaf_block_fps = new_leaf_block_fps;
                    }

                    break;
                }

                level_count *= 2;
            }
        }

        // Reused while packing the index
        let mut write_buffer = RAMOutputStream::new(false);

        // This is the "file" we append the bytes to:
        let mut blocks = vec![];
        let mut last_split_values = vec![0u8; self.bytes_per_dim * self.num_dims];
        let mut negative_deltas = vec![false; self.num_dims];

        let total_size = self.recurse_pack_index(
            &mut write_buffer,
            &leaf_block_fps,
            split_packed_values,
            0,
            &mut blocks,
            1,
            &mut last_split_values,
            &mut negative_deltas,
            false,
        )? as usize;

        let mut index: Vec<u8> = vec![0u8; total_size];
        let mut upto = 0usize;
        for block in &blocks {
            index[upto..upto + block.len()].copy_from_slice(block);
            upto += block.len();
        }

        debug_assert!(upto == total_size);
        Ok(index)
    }

    #[allow(clippy::too_many_arguments)]
    fn recurse_pack_index(
        &self,
        write_buffer: &mut RAMOutputStream,
        leaf_block_fps: &[i64],
        split_packed_values: &mut [u8],
        min_block_fp: i64,
        blocks: &mut Vec<Vec<u8>>,
        node_id: usize,
        last_split_values: &mut [u8],
        negative_deltas: &mut [bool],
        is_left: bool,
    ) -> Result<i32> {
        if node_id >= leaf_block_fps.len() {
            let leaf_id = node_id - leaf_block_fps.len();

            // In the unbalanced case it's possible the left most node only has one child:
            if leaf_id < leaf_block_fps.len() {
                let delta = leaf_block_fps[leaf_id] - min_block_fp;
                if is_left {
                    debug_assert!(delta == 0);
                    Ok(0)
                } else {
                    debug_assert!(node_id == 1 || delta > 0);
                    write_buffer.write_vlong(delta)?;
                    self.append_block(write_buffer, blocks)
                }
            } else {
                Ok(0)
            }
        } else {
            let left_block_fp: i64;
            if !is_left {
                left_block_fp = self.left_most_leaf_block_fp(leaf_block_fps, node_id);
                let delta = left_block_fp - min_block_fp;
                debug_assert!(node_id == 1 || delta > 0);
                write_buffer.write_vlong(delta)?;
            } else {
                // The left tree's left most leaf block FP is always the minimal FP:
                left_block_fp = min_block_fp;
            }

            let mut address = node_id * (1 + self.bytes_per_dim);
            let split_dim = split_packed_values[address] as usize;
            address += 1;

            // find common prefix with last split value in this dim:
            let mut prefix = 0;
            while prefix < self.bytes_per_dim {
                if split_packed_values[address + prefix]
                    != last_split_values[split_dim * self.bytes_per_dim + prefix]
                {
                    break;
                }
                prefix += 1;
            }

            let mut first_diff_byte_delta: i32;
            if prefix < self.bytes_per_dim {
                first_diff_byte_delta = (split_packed_values[address + prefix] as u32 as i32)
                    - (last_split_values[split_dim * self.bytes_per_dim + prefix] as u32 as i32);
                if negative_deltas[split_dim] {
                    first_diff_byte_delta *= -1;
                }

                debug_assert!(first_diff_byte_delta > 0);
            } else {
                first_diff_byte_delta = 0;
            }

            // pack the prefix, splitDim and delta first diff byte into a single vInt:
            let code = ((first_diff_byte_delta as usize * (1 + self.bytes_per_dim) + prefix)
                * self.num_dims
                + split_dim) as i32;
            write_buffer.write_vint(code)?;

            // write the split value, prefix coded vs. our parent's split value:
            let suffix = self.bytes_per_dim - prefix;
            if suffix > 1 {
                write_buffer.write_bytes(split_packed_values, address + prefix + 1, suffix - 1)?;
            }

            let cmp = last_split_values.to_vec();
            let src_offset = split_dim * self.bytes_per_dim + prefix;

            // copy our split value into lastSplitValues for our children to prefix-code against
            let sav_split_value = last_split_values[src_offset..src_offset + suffix].to_vec();
            let split_start = address + prefix;
            last_split_values[src_offset..src_offset + suffix]
                .copy_from_slice(&split_packed_values[split_start..split_start + suffix]);
            let num_bytes = self.append_block(write_buffer, blocks)?;

            // placeholder for left-tree numBytes; we need this so that at search time if we only
            // need to recurse into the right sub-tree we can quickly seek to its
            // starting point
            let idx_sav = blocks.len();
            blocks.push(vec![]);

            let sav_negative_delta = negative_deltas[split_dim];
            negative_deltas[split_dim] = true;

            let left_num_bytes = self.recurse_pack_index(
                write_buffer,
                leaf_block_fps,
                split_packed_values,
                left_block_fp,
                blocks,
                2 * node_id,
                last_split_values,
                negative_deltas,
                true,
            )?;
            if node_id * 2 < leaf_block_fps.len() {
                write_buffer.write_vint(left_num_bytes)?;
            } else {
                debug_assert!(left_num_bytes == 0);
            }

            let num_bytes2 = write_buffer.file_pointer();
            let mut bytes2: Vec<u8> = vec![0u8; num_bytes2 as usize];
            write_buffer.write_to_buf(&mut bytes2)?;
            write_buffer.reset();

            // replace our placeholder:
            blocks[idx_sav] = bytes2;
            negative_deltas[split_dim] = false;

            let right_num_bytes = self.recurse_pack_index(
                write_buffer,
                leaf_block_fps,
                split_packed_values,
                left_block_fp,
                blocks,
                2 * node_id + 1,
                last_split_values,
                negative_deltas,
                false,
            )?;
            negative_deltas[split_dim] = sav_negative_delta;

            // restore lastSplitValues to what caller originally passed us:
            let dest_offset = split_dim * self.bytes_per_dim + prefix;
            last_split_values[dest_offset..dest_offset + suffix].copy_from_slice(&sav_split_value);
            debug_assert!(last_split_values == cmp.as_slice());

            Ok(num_bytes + num_bytes2 as i32 + left_num_bytes + right_num_bytes)
        }
    }

    fn append_block(
        &self,
        write_buffer: &mut RAMOutputStream,
        blocks: &mut Vec<Vec<u8>>,
    ) -> Result<i32> {
        let pos = write_buffer.file_pointer();
        debug_assert_eq!(pos as i32 as i64, pos);
        let mut bytes = vec![0u8; pos as usize];

        write_buffer.write_to_buf(&mut bytes)?;
        write_buffer.reset();

        blocks.push(bytes);

        Ok(pos as i32)
    }

    fn left_most_leaf_block_fp(&self, leaf_block_fps: &[i64], mut node_id: usize) -> i64 {
        // TODO: can we do this cheaper, e.g. a closed form solution instead of while loop?  Or
        // change the recursion while packing the index to return this left-most leaf block FP
        // from each recursion instead?
        //
        // Still, the overall cost here is minor: this method's cost is O(log(N)), and while writing
        // we call it O(N) times (N = number of leaf blocks)
        while node_id < leaf_block_fps.len() {
            node_id *= 2;
        }

        let leaf_id = node_id - leaf_block_fps.len();
        let result = leaf_block_fps[leaf_id];
        debug_assert!(result >= 0);
        result
    }

    fn write_index(
        &mut self,
        out: &mut impl IndexOutput,
        leaf_block_fps: &[i64],
        split_packed_values: &mut Vec<u8>,
    ) -> Result<()> {
        let packed_index = self.pack_index(leaf_block_fps, split_packed_values)?;
        self.write_index_packed(out, leaf_block_fps.len() as i32, &packed_index)
    }

    fn write_index_packed(
        &mut self,
        out: &mut impl IndexOutput,
        num_leaves: i32,
        packed_index: &[u8],
    ) -> Result<()> {
        write_header(out, CODEC_NAME, VERSION_CURRENT)?;

        out.write_vint(self.num_dims as i32)?;
        out.write_vint(self.max_points_in_leaf_node)?;
        out.write_vint(self.bytes_per_dim as i32)?;

        debug_assert!(num_leaves > 0);
        out.write_vint(num_leaves)?;
        out.write_bytes(&self.min_packed_value, 0, self.packed_bytes_length)?;
        out.write_bytes(&self.max_packed_value, 0, self.packed_bytes_length)?;

        out.write_vlong(self.point_count)?;
        out.write_vint(self.docs_seen.cardinality() as i32)?;
        out.write_vint(packed_index.len() as i32)?;
        out.write_bytes(packed_index, 0, packed_index.len())?;

        Ok(())
    }

    fn write_leaf_block_docs(
        &self,
        out: &mut impl DataOutput,
        doc_ids: &[DocId],
        start: usize,
        count: usize,
    ) -> Result<()> {
        out.write_vint(count as i32)?;
        DocIdsWriter::write_doc_ids(out, doc_ids, start, count)
    }

    fn write_leaf_block_packed_values(
        &self,
        out: &mut impl DataOutput,
        common_prefix_lengths: &mut [usize],
        count: usize,
        sorted_dim: usize,
        values: &mut Vec<&[u8]>,
    ) -> Result<()> {
        let mut prefix_sum = 0;
        for i in 0..common_prefix_lengths.len() {
            prefix_sum += common_prefix_lengths[i];
        }

        if prefix_sum == self.packed_bytes_length {
            // all values in this block are equal
            out.write_byte(0xff)?;
        } else {
            debug_assert!(common_prefix_lengths[sorted_dim] < self.bytes_per_dim);
            out.write_byte(sorted_dim as u8)?;

            let compressed_byte_offset =
                sorted_dim * self.bytes_per_dim + common_prefix_lengths[sorted_dim] as usize;
            common_prefix_lengths[sorted_dim] += 1;

            let mut i = 0;
            while i < count {
                // do run-length compression on the byte at compressedByteOffset
                let run_len = self.run_len(values, i, count.min(i + 0xFF), compressed_byte_offset);
                debug_assert!(run_len <= 0xFF);
                let prefix_byte = values[i][compressed_byte_offset];
                out.write_byte(prefix_byte)?;
                out.write_byte(run_len as u8)?;

                self.write_leaf_block_packed_values_range(
                    out,
                    common_prefix_lengths,
                    i,
                    i + run_len,
                    values,
                )?;
                i += run_len;

                debug_assert!(i <= count)
            }
        }

        Ok(())
    }

    fn write_leaf_block_packed_values_range(
        &self,
        out: &mut impl DataOutput,
        common_prefix_lengths: &[usize],
        start: usize,
        end: usize,
        values: &[&[u8]],
    ) -> Result<()> {
        for i in start..end {
            debug_assert_eq!(values[i].len(), self.packed_bytes_length);

            for dim in 0..self.num_dims {
                let prefix = common_prefix_lengths[dim];
                out.write_bytes(
                    values[i],
                    dim * self.bytes_per_dim + prefix,
                    self.bytes_per_dim - prefix,
                )?;
            }
        }

        Ok(())
    }

    fn run_len(&self, values: &[&[u8]], start: usize, end: usize, byte_offset: usize) -> usize {
        let b = values[start][byte_offset];

        for i in start + 1..end {
            let b2 = values[i][byte_offset];
            debug_assert!(b2 >= b);
            if b != b2 {
                return i - start;
            }
        }

        end - start
    }

    fn write_common_prefixes(
        &self,
        out: &mut impl DataOutput,
        common_prefix_lengths: &[usize],
        packed_value: &mut Vec<u8>,
    ) -> Result<()> {
        for dim in 0..self.num_dims {
            out.write_vint(common_prefix_lengths[dim] as i32)?;
            out.write_bytes(
                packed_value,
                dim * self.bytes_per_dim,
                common_prefix_lengths[dim],
            )?;
        }

        Ok(())
    }

    fn check_max_leaf_node_count(&self, num_leaves: i32) -> Result<()> {
        if (1 + self.bytes_per_dim as i32) * num_leaves > i32::max_value() - 64 {
            bail!(IllegalState(format!(
                "too many nodes:increase max_points_in_leaf_node( currently {}) and reindex)",
                self.max_points_in_leaf_node
            )));
        }

        Ok(())
    }

    fn value_in_order(
        &self,
        ord: i64,
        sorted_dim: usize,
        last_packed_value: &[u8],
        packed_value: &[u8],
        doc: DocId,
        last_doc: DocId,
    ) -> Result<()> {
        let dim_offset = sorted_dim * self.bytes_per_dim;
        if ord > 0 {
            let cmp = last_packed_value[dim_offset..dim_offset + self.bytes_per_dim]
                .cmp(&packed_value[dim_offset..dim_offset + self.bytes_per_dim]);
            if cmp == Ordering::Greater {
                bail!(IllegalArgument(format!(
                    "values out of order. offset: {}, bytes_per_dim: {},  value: {:?}, last: {:?}",
                    dim_offset, self.bytes_per_dim, packed_value, last_packed_value
                )));
            } else if cmp == Ordering::Equal && doc < last_doc {
                bail!(IllegalArgument(format!(
                    "docs out of order. offset: {}, bytes_per_dim: {},  value: {:?}, last: {:?}, \
                     doc: {}, last: {}",
                    dim_offset, self.bytes_per_dim, packed_value, last_packed_value, doc, last_doc
                )));
            }
        }

        Ok(())
    }

    fn value_in_order_and_bounds(
        &self,
        count: usize,
        sorted_dim: usize,
        min_packed_value: &[u8],
        max_packed_value: &[u8],
        values: &[&[u8]],
        docs: &[DocId],
    ) -> bool {
        let mut last_packed_value: Vec<u8> = vec![0u8; self.packed_bytes_length];
        let mut last_doc = -1;

        for i in 0..count {
            debug_assert_eq!(values[i].len(), self.packed_bytes_length);
            debug_assert!(self
                .value_in_order(
                    i as i64,
                    sorted_dim,
                    &last_packed_value,
                    values[i],
                    docs[i],
                    last_doc,
                )
                .is_ok());

            last_packed_value[0..self.packed_bytes_length].copy_from_slice(values[i]);

            last_doc = docs[i];

            // Make sure this value does in fact fall within this leaf cell:
            debug_assert!(self.value_in_bounds(values[i], min_packed_value, max_packed_value));
        }

        true
    }

    fn value_in_bounds(
        &self,
        values: &[u8],
        min_packed_value: &[u8],
        max_packed_value: &[u8],
    ) -> bool {
        for dim in 0..self.num_dims {
            let dim_offset = self.bytes_per_dim * dim;

            if values[dim_offset..dim_offset + self.bytes_per_dim]
                < min_packed_value[dim_offset..dim_offset + self.bytes_per_dim]
            {
                return false;
            }
            if values[dim_offset..dim_offset + self.bytes_per_dim]
                > max_packed_value[dim_offset..dim_offset + self.bytes_per_dim]
            {
                return false;
            }
        }

        true
    }

    #[allow(clippy::too_many_arguments)]
    fn build(
        &mut self,
        node_id: i32,
        leaf_node_offset: i32,
        slices: &mut Vec<PathSlice<PointWriterEnum<D>>>,
        ord_bitset: &mut Option<LongBitSet>,
        out: &mut impl IndexOutput,
        min_packed_value: &mut Vec<u8>,
        max_packed_value: &mut Vec<u8>,
        parent_splits: &mut Vec<i32>,
        split_packed_values: &mut Vec<u8>,
        leaf_block_fps: &mut Vec<i64>,
    ) -> Result<()> {
        for slice in slices.iter() {
            debug_assert!(slice.count == slices[0].count);
        }

        if self.num_dims == 1
            && slices[0].writer.point_type() == PointType::Offline
            && slices[0].count <= self.max_points_sort_in_heap as i64
        {
            // Special case for 1D, to cutover to heap once we recurse deeply enough:
            let p = self.switch_to_heap(&mut slices[0])?;
            slices[0] = p;
        }

        if node_id >= leaf_node_offset {
            // Leaf node: write block
            // We can write the block in any order so by default we write it sorted by the
            // dimension that has the least number of unique bytes at
            // commonPrefixLengths[dim], which makes compression more efficient
            let mut sorted_dim = 0;
            let mut sorted_dim_cardinality = i32::max_value();

            for dim in 0..self.num_dims {
                if slices[dim].writer.point_type() != PointType::Heap {
                    // Adversarial cases can cause this, e.g. very lopsided data, all equal points,
                    // such that we started offline, but then kept splitting
                    // only in one dimension, and so never had to rewrite into heap writer
                    let p = self.switch_to_heap(&mut slices[dim])?;
                    slices[dim] = p;
                }

                // Find common prefix by comparing first and last values, already sorted in this
                // dimension:
                let source = &mut slices[dim];
                let heap_source = source.writer.try_as_heap_writer();
                heap_source.read_packed_value(source.start as usize, &mut self.scratch1);
                heap_source.read_packed_value(
                    (source.start + source.count - 1) as usize,
                    &mut self.scratch2,
                );

                let offset = dim * self.bytes_per_dim;
                self.common_prefix_lengths[dim] = self.bytes_per_dim;
                for j in 0..self.bytes_per_dim {
                    if self.scratch1[offset + j] != self.scratch2[offset + j] {
                        self.common_prefix_lengths[dim] = j;
                        break;
                    }
                }

                let prefix = self.common_prefix_lengths[dim];
                if prefix < self.bytes_per_dim {
                    let mut cardinality = 1;
                    let mut previous = self.scratch1[offset + prefix];

                    for _i in 1..source.count {
                        heap_source
                            .read_packed_value(source.start as usize + 1, &mut self.scratch2);
                        let b = self.scratch2[offset + prefix];
                        debug_assert!(previous <= b);
                        if b != previous {
                            cardinality += 1;
                            previous = b;
                        }
                    }

                    debug_assert!(cardinality <= 256);
                    if cardinality < sorted_dim_cardinality {
                        sorted_dim = dim;
                        sorted_dim_cardinality = cardinality;
                    }
                }
            }

            let source = &mut slices[sorted_dim];
            // We ensured that maxPointsSortInHeap was >= maxPointsInLeafNode, so we better be in
            // heap at this point:
            let heap_source = source.writer.try_as_heap_writer();
            // Save the block file pointer:
            leaf_block_fps[(node_id - leaf_node_offset) as usize] = out.file_pointer();
            // Write docIDs first, as their own chunk, so that at intersect time we can add all
            // docIDs w/o loading the values:
            let count = source.count;
            debug_assert!(count > 0);
            self.write_leaf_block_docs(
                out,
                &heap_source.doc_ids,
                source.start as usize,
                count as usize,
            )?;
            // TODO: minor opto: we don't really have to write the actual common prefixes, because
            // BKDReader on recursing can regenerate it for us from the index, much
            // like how terms dict does so from the FST:

            // Write the common prefixes:
            let scratch1 = (&mut self.scratch1) as *mut Vec<u8>;
            self.write_common_prefixes(out, &self.common_prefix_lengths, unsafe {
                &mut (*scratch1)
            })?;

            let mut packed_values_to_store: Vec<Vec<u8>> = vec![];
            let mut packed_values: Vec<&[u8]> = vec![];
            for i in 0..count as usize {
                packed_values_to_store.push(vec![0u8; self.packed_bytes_length]);
                heap_source.packed_value_slice(
                    source.start as usize + i,
                    &mut packed_values_to_store[i][0..self.packed_bytes_length],
                );
            }
            for i in 0..count as usize {
                packed_values.push(&packed_values_to_store[i][0..self.packed_bytes_length]);
            }

            debug_assert!(self.value_in_order_and_bounds(
                count as usize,
                sorted_dim,
                &self.min_packed_value,
                &self.max_packed_value,
                &packed_values,
                &heap_source.doc_ids[source.start as usize..],
            ));

            let common_prefix_lengths = self.common_prefix_lengths.as_mut() as *mut [usize];
            self.write_leaf_block_packed_values(
                out,
                unsafe { &mut (*common_prefix_lengths) },
                count as usize,
                sorted_dim,
                &mut packed_values,
            )?;
        } else {
            // Inner node: partition/recurse
            let split_dim = if self.num_dims > 1 {
                let min_packed_value = (&self.min_packed_value) as *const Vec<u8>;
                let max_packed_value = (&self.max_packed_value) as *const Vec<u8>;
                self.split(
                    unsafe { &(*min_packed_value) },
                    unsafe { &(*max_packed_value) },
                    parent_splits,
                )
            } else {
                0
            };

            let source = (&slices[split_dim]) as *const PathSlice<PointWriterEnum<D>>;
            let source = unsafe { &(*source) };
            debug_assert!((node_id as usize) < split_packed_values.len());

            // How many points will be in the left tree:
            let right_count = source.count / 2;
            let left_count = source.count - right_count;

            let split_value =
                self.mark_right_tree(right_count, split_dim as i32, source, ord_bitset)?;
            let address = node_id as usize * (1 + self.bytes_per_dim);
            split_packed_values[address] = split_dim as u8;

            // Partition all PathSlice that are not the split dim into sorted left and right sets,
            // so we can recurse:
            let mut left_slices = vec![];
            let mut right_slices = vec![];

            let mut min_split_packed_value: Vec<u8> = vec![0u8; self.packed_bytes_length];
            min_split_packed_value[0..self.packed_bytes_length]
                .copy_from_slice(&min_packed_value[0..self.packed_bytes_length]);

            let mut max_split_packed_value: Vec<u8> = vec![0u8; self.packed_bytes_length];
            max_split_packed_value[0..self.packed_bytes_length]
                .copy_from_slice(&max_packed_value[0..self.packed_bytes_length]);

            // When we are on this dim, below, we clear the ordBitSet:
            let dim_to_clear = if self.num_dims - 1 == split_dim {
                self.num_dims as i32 - 2
            } else {
                self.num_dims as i32 - 1
            };

            for dim in 0..self.num_dims as usize {
                if dim == split_dim as usize {
                    // No need to partition on this dim since it's a simple slice of the incoming
                    // already sorted slice, and we will re-use its shared
                    // reader when visiting it as we recurse:
                    left_slices.push(PathSlice::new(
                        source.writer.clone(),
                        source.start,
                        left_count,
                    ));
                    right_slices.push(PathSlice::new(
                        source.writer.clone(),
                        source.start + left_count,
                        right_count,
                    ));

                    let offset = dim * self.bytes_per_dim;
                    min_split_packed_value[offset..offset + self.bytes_per_dim]
                        .copy_from_slice(&split_value[0..self.bytes_per_dim]);
                    max_split_packed_value[offset..offset + self.bytes_per_dim]
                        .copy_from_slice(&split_value[0..self.bytes_per_dim]);

                    continue;
                }

                // Not inside the try because we don't want to close this one now, so that after
                // recursion is done, we will have done a singel full sweep of the
                // file:
                let dim_start = slices[dim].start as usize;
                let dim_count = slices[dim].count as usize;
                let reader = slices[dim]
                    .writer
                    .shared_point_reader(dim_start, dim_count)?;

                let mut left_point_writer = self.point_writer(left_count, &format!("left{}", dim));
                let mut right_point_writer =
                    self.point_writer(source.count - left_count, &format!("right{}", dim));
                let next_right_count = reader.split(
                    source.count,
                    ord_bitset.as_mut().unwrap(),
                    &mut left_point_writer,
                    &mut right_point_writer,
                    dim as i32 == dim_to_clear,
                )?;

                if right_count != next_right_count {
                    bail!(IllegalState(format!(
                        "wrong number of points in split: expected={} but actual={}",
                        right_count, next_right_count
                    )));
                }

                left_slices.push(PathSlice::new(left_point_writer, 0, left_count));
                right_slices.push(PathSlice::new(right_point_writer, 0, right_count));
            }

            parent_splits[split_dim as usize] += 1;

            self.build(
                2 * node_id,
                leaf_node_offset,
                &mut left_slices,
                ord_bitset,
                out,
                min_packed_value,
                &mut max_split_packed_value,
                parent_splits,
                split_packed_values,
                leaf_block_fps,
            )?;

            for dim in 0..self.num_dims as usize {
                // Don't destroy the dim we split on because we just re-used what our caller above
                // gave us for that dim:
                if dim != split_dim as usize {
                    left_slices[dim].writer.destory()?;
                }
            }

            // TODO: we could "tail recurse" here?  have our parent discard its refs as we recurse
            // right?
            self.build(
                2 * node_id + 1,
                leaf_node_offset,
                &mut right_slices,
                ord_bitset,
                out,
                &mut min_split_packed_value,
                max_packed_value,
                parent_splits,
                split_packed_values,
                leaf_block_fps,
            )?;

            for dim in 0..self.num_dims as usize {
                // Don't destroy the dim we split on because we just re-used what our caller above
                // gave us for that dim:
                if dim != split_dim as usize {
                    right_slices[dim].writer.destory()?;
                }
            }

            parent_splits[split_dim as usize] -= 1;
        }

        Ok(())
    }

    fn mark_right_tree(
        &self,
        right_count: i64,
        split_dim: i32,
        source: &PathSlice<PointWriterEnum<D>>,
        ord_bitset: &mut Option<LongBitSet>,
    ) -> Result<Vec<u8>> {
        // Now we mark ords that fall into the right half, so we can partition on all other dims
        // that are not the split dim: Read the split value, then mark all ords in the
        // right tree (larger than the split value): TODO: find a way to also checksum this
        // reader?  If we changed to markLeftTree, and scanned the final chunk, it could work?

        let mut reader = source.writer.point_reader(
            (source.start + source.count - right_count) as usize,
            right_count as usize,
        )?;
        let result = reader.next()?;
        debug_assert!(result);
        let mut scratch: Vec<u8> = vec![0u8; self.packed_bytes_length];
        let offset = split_dim as usize * self.bytes_per_dim;
        scratch[0..self.bytes_per_dim]
            .copy_from_slice(&reader.packed_value()[offset..offset + self.bytes_per_dim]);
        if self.num_dims > 1 {
            debug_assert!(ord_bitset.is_some());
            let ord_bitset = ord_bitset.as_mut().unwrap();
            debug_assert!(!ord_bitset.get(reader.ord()));
            ord_bitset.set(reader.ord());
            // Subtract 1 from rightCount because we already did the first value above (so we could
            // record the split value):
            reader.mark_ords(right_count - 1, ord_bitset)?;
        }

        Ok(scratch)
    }

    fn point_writer(&self, count: i64, desc: &str) -> PointWriterEnum<D> {
        if count < self.max_points_sort_in_heap as i64 {
            PointWriterEnum::Heap(HeapPointWriter::new(
                count as usize,
                count as usize,
                self.packed_bytes_length,
                self.long_ords,
                self.single_value_per_doc,
            ))
        } else {
            PointWriterEnum::Offline(OfflinePointWriter::prefix_new(
                Arc::clone(&self.temp_dir.directory),
                &self.temp_file_name_prefix,
                self.packed_bytes_length,
                self.long_ords,
                desc,
                count,
                self.single_value_per_doc,
            ))
        }
    }

    fn split(
        &mut self,
        min_packed_value: &[u8],
        max_packed_value: &[u8],
        parent_splits: &[i32],
    ) -> usize {
        // First look at whether there is a dimension that has split less than 2x less than
        // the dim that has most splits, and return it if there is such a dimension and it
        // does not only have equals values. This helps ensure all dimensions are indexed.
        let mut max_num_splits = 0;
        for num_splits in parent_splits {
            max_num_splits = max_num_splits.max(*num_splits);
        }

        for dim in 0..self.num_dims {
            let offset = (self.bytes_per_dim * dim) as usize;
            if parent_splits[dim as usize] < max_num_splits / 2
                && min_packed_value[offset..offset + self.bytes_per_dim]
                    != max_packed_value[offset..offset + self.bytes_per_dim]
            {
                return dim;
            }
        }

        // Find which dim has the largest span so we can split on it:
        let mut split_dim = None;
        for dim in 0..self.num_dims {
            bytes_subtract(
                self.bytes_per_dim,
                dim,
                max_packed_value,
                min_packed_value,
                self.scratch_diff.as_mut(),
            );

            if split_dim.is_none()
                || self.scratch_diff[0..self.bytes_per_dim] > self.scratch1[0..self.bytes_per_dim]
            {
                self.scratch1[0..self.bytes_per_dim]
                    .copy_from_slice(&self.scratch_diff[0..self.bytes_per_dim]);
                split_dim = Some(dim);
            }
        }

        split_dim.unwrap()
    }

    fn switch_to_heap<W: PointWriter>(
        &self,
        source: &mut PathSlice<W>,
    ) -> Result<PathSlice<PointWriterEnum<D>>> {
        let count = source.count;
        // Not inside the try because we don't want to close it here:
        let reader = source
            .writer
            .shared_point_reader(source.start as usize, source.count as usize)?;

        let mut writer = HeapPointWriter::new(
            count as usize,
            count as usize,
            self.packed_bytes_length,
            self.long_ords,
            self.single_value_per_doc,
        );
        for _i in 0..count {
            let has_next: bool = reader.next()?;
            debug_assert!(has_next);

            writer.append(reader.packed_value(), reader.ord(), reader.doc_id())?;
        }

        Ok(PathSlice::new(PointWriterEnum::Heap(writer), 0, count))
    }

    fn sort_heap_point_writer(&mut self, writer: &mut HeapPointWriter, dim: i32) {
        // Tie-break by docID:

        // No need to tie break on ord, for the case where the same doc has the same value in a
        // given dimension indexed more than once: it can't matter at search time since we
        // don't write ords into the index:
        let max_length = self.bytes_per_dim as i32 + INT_BYTES;
        let intro_sorter = BKDWriterMSBIntroSorter::new(0, max_length, self, writer, dim);

        let mut msb_sorter = MSBRadixSorter::new(max_length, intro_sorter);

        msb_sorter.sort(0, self.point_count as i32);
    }

    fn sort(&mut self, dim: i32) -> Result<PointWriterEnum<D>> {
        debug_assert!(dim >= 0 && dim < self.num_dims as i32);

        if self.heap_point_writer.is_some() {
            debug_assert!(self.temp_input.is_none());

            // We never spilled the incoming points to disk, so now we sort in heap:
            let mut sorted = HeapPointWriter::new(
                self.point_count as usize,
                self.point_count as usize,
                self.packed_bytes_length,
                self.long_ords,
                self.single_value_per_doc,
            );
            sorted.copy_from(&self.heap_point_writer.as_ref().unwrap())?;
            self.sort_heap_point_writer(&mut sorted, dim);
            sorted.close()?;

            Ok(PointWriterEnum::Heap(sorted))
        } else {
            // Offline sort:
            unimplemented!()
        }
    }
}

impl<D: Directory> Drop for BKDWriter<D> {
    fn drop(&mut self) {
        if self.temp_input.is_some() {
            // NOTE: this should only happen on exception, e.g. caller calls close w/o calling
            // finish:
            match self.temp_dir.delete_file(self.temp_input.as_ref().unwrap()) {
                _ => {}
            }
            self.temp_input = None;
        }
    }
}

pub struct BKDWriterMSBIntroSorter<D: Directory> {
    bkd_writer: *mut BKDWriter<D>,
    heap_writer: *mut HeapPointWriter,
    dim: i32,

    k: i32,
    max_length: i32,
    pivot: Vec<u8>,
    pivot_len: usize,
}

impl<D: Directory> BKDWriterMSBIntroSorter<D> {
    pub fn new(
        k: i32,
        max_length: i32,
        bkd_writer: &mut BKDWriter<D>,
        heap_writer: &mut HeapPointWriter,
        dim: i32,
    ) -> BKDWriterMSBIntroSorter<D> {
        BKDWriterMSBIntroSorter {
            bkd_writer,
            heap_writer,
            dim,
            k,
            max_length,
            pivot: vec![0u8; max_length as usize + 1],
            pivot_len: 0,
        }
    }
}

impl<D: Directory> MSBSorter for BKDWriterMSBIntroSorter<D> {
    type Fallback = BKDWriterMSBIntroSorter<D>;
    fn byte_at(&self, i: i32, k: i32) -> Option<u8> {
        debug_assert!(k >= 0);
        let bkd_writer = unsafe { &(*self.bkd_writer) };
        let heap_writer = unsafe { &(*self.heap_writer) };
        let res = if k < bkd_writer.bytes_per_dim as i32 {
            // dim bytes
            let block = i as usize / heap_writer.values_per_block;
            let index = i as usize % heap_writer.values_per_block;

            let offset = index * bkd_writer.packed_bytes_length
                + self.dim as usize * bkd_writer.bytes_per_dim
                + k as usize;
            heap_writer.blocks[block][offset]
        } else {
            // doc id
            let s = 3 - (k - bkd_writer.bytes_per_dim as i32);
            ((heap_writer.doc_ids[i as usize]).unsigned_shift((s * 8) as usize) & 0xFF) as u8
        };
        Some(res)
    }

    fn msb_swap(&mut self, i: i32, j: i32) {
        let i = i as usize;
        let j = j as usize;
        let bkd_writer = unsafe { &mut (*self.bkd_writer) };
        let heap_writer = unsafe { &mut (*self.heap_writer) };

        heap_writer.doc_ids.swap(i, j);

        if !bkd_writer.single_value_per_doc {
            if bkd_writer.long_ords {
                heap_writer.ords_long.swap(i, j);
            } else {
                heap_writer.ords.swap(i, j);
            }
        }

        let block_i = i / heap_writer.values_per_block;
        let block_j = j / heap_writer.values_per_block;
        let index_i = (i % heap_writer.values_per_block) * bkd_writer.packed_bytes_length;
        let index_j = (j % heap_writer.values_per_block) * bkd_writer.packed_bytes_length;

        bkd_writer.scratch1.copy_from_slice(
            &heap_writer.blocks[block_i][index_i..index_i + bkd_writer.packed_bytes_length],
        );
        bkd_writer.scratch2.copy_from_slice(
            &heap_writer.blocks[block_j][index_j..index_j + bkd_writer.packed_bytes_length],
        );

        heap_writer.blocks[block_i][index_i..index_i + bkd_writer.packed_bytes_length]
            .copy_from_slice(&bkd_writer.scratch2);
        heap_writer.blocks[block_j][index_j..index_j + bkd_writer.packed_bytes_length]
            .copy_from_slice(&bkd_writer.scratch1);
    }

    fn fallback_sorter(&mut self, k: i32) -> BKDWriterMSBIntroSorter<D> {
        BKDWriterMSBIntroSorter::new(
            k,
            self.max_length,
            unsafe { &mut (*self.bkd_writer) },
            unsafe { &mut (*self.heap_writer) },
            self.dim,
        )
    }
}

impl<D: Directory> Sorter for BKDWriterMSBIntroSorter<D> {
    fn compare(&mut self, i: i32, j: i32) -> Ordering {
        for o in self.k..self.max_length {
            let b1 = self.byte_at(i, o);
            let b2 = self.byte_at(j, o);
            if b1 != b2 {
                return b1.cmp(&b2);
            } else if b1.is_none() {
                break;
            }
        }

        Ordering::Equal
    }

    fn swap(&mut self, i: i32, j: i32) {
        self.msb_swap(i, j)
    }

    fn sort(&mut self, from: i32, to: i32) {
        check_range(from, to);
        self.quick_sort(from, to, 2 * ((((to - from) as f64).log2()) as i32));
    }

    fn set_pivot(&mut self, i: i32) {
        self.pivot_len = 0;
        for o in self.k..self.max_length {
            if let Some(b) = self.byte_at(i, o) {
                self.pivot[self.pivot_len] = b;
                self.pivot_len += 1;
            } else {
                break;
            }
        }
    }

    fn compare_pivot(&mut self, j: i32) -> Ordering {
        for o in 0..self.pivot_len {
            let b1 = self.pivot[o];
            if let Some(b2) = self.byte_at(j, self.k + o as i32) {
                if b1 != b2 {
                    return b1.cmp(&b2);
                }
            } else {
                return Ordering::Greater;
            }
        }

        if self.k + self.pivot_len as i32 == self.max_length {
            Ordering::Equal
        } else if self.byte_at(j, self.k + self.pivot_len as i32).is_some() {
            Ordering::Less
        } else {
            Ordering::Equal
        }
    }
}
