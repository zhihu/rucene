use core::codec::codec_util;
use core::index::point_values::{IntersectVisitor, Relation};
use core::store::{ByteArrayDataInput, ByteArrayRef, ByteSlicesDataInput, DataInput, IndexInput};
use core::util::bit_util::UnsignedShift;
use core::util::math;
use core::util::string_util::bytes_compare;
use core::util::DocId;
use error::*;
use std::sync::Arc;

/// Used to track all state for a single call to {@link #intersect}.
pub struct IntersectState<'a> {
    input: Box<IndexInput>,
    scratch_doc_ids: Vec<i32>,
    scratch_packed_value: Vec<u8>,
    common_prefix_lengths: Vec<i32>,
    visitor: &'a mut IntersectVisitor,
    index_tree: Box<IndexTree>,
}

impl<'a> IntersectState<'a> {
    pub fn new(
        input: Box<IndexInput>,
        num_dims: usize,
        packed_bytes_length: usize,
        max_points_in_leaf_node: usize,
        visitor: &'a mut IntersectVisitor,
        index_tree: Box<IndexTree>,
    ) -> IntersectState {
        let scratch_doc_ids = vec![0i32; max_points_in_leaf_node];
        let scratch_packed_value = vec![0u8; packed_bytes_length];
        let common_prefix_lengths = vec![0i32; num_dims];
        IntersectState {
            input,
            scratch_doc_ids,
            scratch_packed_value,
            common_prefix_lengths,
            visitor,
            index_tree,
        }
    }
}

/// Handles intersection of an multi-dimensional shape in byte[] space with a block KD-tree
/// previously written with `BKDWriter`.

pub struct BKDReader {
    /// Packed array of byte[] holding all split values in the full binary tree:
    leaf_node_offset: i32,
    pub num_dims: usize,
    pub bytes_per_dim: usize,
    num_leaves: usize,
    input: Arc<IndexInput>,
    max_points_in_leaf_node: usize,
    pub min_packed_value: Vec<u8>,
    pub max_packed_value: Vec<u8>,
    pub point_count: i64,
    pub doc_count: i32,
    version: i32,
    packed_bytes_length: usize,
    /// Used for 6.4.0+ index format
    packed_index: Arc<Vec<u8>>,
    /// Used for Legacy (pre-6.4.0) index format, to hold a compact form of the index:
    #[allow(dead_code)]
    split_packed_values: Vec<u8>,
    bytes_per_index_entry: usize,
    leaf_block_fps: Arc<Vec<i64>>,
}

pub const BKD_CODEC_NAME: &str = "BKD";
pub const BKD_VERSION_COMPRESSED_DOC_IDS: i32 = 1;
pub const BKD_VERSION_COMPRESSED_VALUES: i32 = 2;
pub const BKD_VERSION_IMPLICIT_SPLIT_DIM_1D: i32 = 3;
pub const BKD_VERSION_PACKED_INDEX: i32 = 4;
pub const BKD_VERSION_START: i32 = 0;
pub const BKD_VERSION_CURRENT: i32 = BKD_VERSION_PACKED_INDEX;

impl BKDReader {
    pub fn new(input: Arc<IndexInput>) -> Result<BKDReader> {
        let mut reader: Box<IndexInput> = input.as_ref().clone()?;
        let version = codec_util::check_header(
            reader.as_mut(),
            BKD_CODEC_NAME,
            BKD_VERSION_START,
            BKD_VERSION_CURRENT,
        )?;
        let num_dims = reader.read_vint()? as usize;
        let max_points_in_leaf_node = reader.read_vint()? as usize;
        let bytes_per_dim = reader.read_vint()? as usize;
        let bytes_per_index_entry = if num_dims == 1 && version >= BKD_VERSION_IMPLICIT_SPLIT_DIM_1D
        {
            bytes_per_dim
        } else {
            bytes_per_dim + 1usize
        };
        let packed_bytes_length = num_dims * bytes_per_dim;

        // read index
        let num_leaves = reader.read_vint()? as usize;
        debug_assert!(num_leaves as i32 > 0);
        let leaf_node_offset = num_leaves as i32;

        let mut min_packed_value = vec![0u8; packed_bytes_length];
        let mut max_packed_value = vec![0u8; packed_bytes_length];
        reader.read_bytes(&mut min_packed_value, 0, packed_bytes_length)?;
        reader.read_bytes(&mut max_packed_value, 0, packed_bytes_length)?;

        for dim in 0..num_dims {
            if bytes_compare(
                bytes_per_dim,
                &min_packed_value[dim * bytes_per_dim..],
                &max_packed_value[dim * bytes_per_dim..],
            ) > 0
            {
                bail!(ErrorKind::CorruptIndex(format!(
                    "min_packed_value > max_packed_value for dim: {}",
                    dim
                )));
            }
        }

        let point_count = reader.read_vlong()?;
        let doc_count = reader.read_vint()?;
        let mut packed_index = Vec::new();
        let mut leaf_block_fps = Vec::new();
        let mut split_packed_values = Vec::new();
        if version >= BKD_VERSION_PACKED_INDEX {
            let num_bytes = reader.read_vint()? as usize;
            packed_index.resize(num_bytes, 0u8);
            reader.read_bytes(&mut packed_index, 0, num_bytes)?;
        } else {
            // legacy un-packed index
            let length = bytes_per_index_entry * num_leaves;
            split_packed_values.resize(length, 0u8);
            reader.read_bytes(&mut split_packed_values, 0, length)?;

            // Read the file pointers to the start of each leaf block:
            leaf_block_fps.resize(num_leaves, 0i64);
            let mut last_fp = 0i64;
            for fp in leaf_block_fps.iter_mut().take(num_leaves) {
                let delta = reader.read_vlong()?;
                *fp = last_fp + delta;
                last_fp += delta;
            }

            // Possibly rotate the leaf block FPs, if the index not fully balanced binary tree
            // (only happens if it was created by BKDWriter.merge or OneDimWriter).  In this case
            // the leaf nodes may straddle the two bottom levels of the binary tree:
            if num_dims == 1 && num_leaves > 1 {
                let mut level_count = 2usize;
                loop {
                    if num_leaves >= level_count && num_leaves <= 2 * level_count {
                        let last_level = 2 * (num_leaves - level_count);
                        debug_assert!(last_level as isize >= 0);
                        if last_level > 0 {
                            // Last level is partially filled, so we must rotate the leaf FPs to
                            // match. We do this here, after loading at
                            // read-time, so that we can still
                            // delta code them on disk at write:
                            let mut new_leaf_block_fps = Vec::with_capacity(num_leaves);
                            new_leaf_block_fps.resize(num_leaves, 0i64);
                            let length = leaf_block_fps.len() - last_level;
                            new_leaf_block_fps[0..length]
                                .copy_from_slice(&leaf_block_fps[last_level..leaf_block_fps.len()]);
                            new_leaf_block_fps[length..leaf_block_fps.len()]
                                .copy_from_slice(&leaf_block_fps[0..last_level]);
                            leaf_block_fps = new_leaf_block_fps;
                        }
                        break;
                    }
                    level_count *= 2;
                }
            }
        }

        Ok(BKDReader {
            leaf_node_offset,
            num_dims,
            bytes_per_dim,
            num_leaves,
            input,
            max_points_in_leaf_node,
            min_packed_value,
            max_packed_value,
            point_count,
            doc_count,
            version,
            packed_bytes_length,
            packed_index: Arc::new(packed_index),
            split_packed_values,
            bytes_per_index_entry,
            leaf_block_fps: Arc::new(leaf_block_fps),
        })
    }

    #[allow(dead_code)]
    fn min_leaf_block_fp(&self) -> Result<i64> {
        if self.packed_index.is_empty() {
            ByteSlicesDataInput::new(self.packed_index.as_ref()).read_vlong()
        } else {
            let mut min_fp = i64::max_value();
            for fp in self.leaf_block_fps.as_ref() {
                min_fp = min_fp.min(*fp);
            }
            Ok(min_fp)
        }
    }

    fn tree_depth(&self) -> Result<usize> {
        // First +1 because all the non-leave nodes makes another power
        // of 2; e.g. to have a fully balanced tree with 4 leaves you
        // need a depth=3 tree:

        // Second +1 because MathUtil.log computes floor of the logarithm; e.g.
        // with 5 leaves you need a depth=4 tree:
        Ok((math::log(self.num_leaves as i64, 2)? + 2) as usize)
    }

    pub fn intersect(&self, visitor: &mut IntersectVisitor) -> Result<()> {
        let mut state = self.create_intersect_state(visitor)?;

        self.intersect_with_state(
            &mut state,
            self.min_packed_value.as_slice(),
            self.max_packed_value.as_slice(),
        )
    }

    fn intersect_with_state(
        &self,
        state: &mut IntersectState,
        cell_min_packed: &[u8],
        cell_max_packed: &[u8],
    ) -> Result<()> {
        let r = state.visitor.compare(cell_min_packed, cell_max_packed);

        let _s = match r {
            Relation::CellOutsideQuery => "CellOutsideQuery",
            Relation::CellInsideQuery => "CellInsideQuery",
            Relation::CellCrossesQuery => "CellCrossesQuery",
        };

        if r == Relation::CellOutsideQuery {
            // This cell is fully outside of the query shape: stop recursing
        } else if r == Relation::CellInsideQuery {
            // This cell is fully inside of the query shape: recursively add all points in this
            // cell without filtering
            self.add_all(state)?;
        // The cell crosses the shape boundary, or the cell fully contains the query, so we fall
        // through and do full filtering:
        } else if state.index_tree.is_leaf_node() {
            // TODO: we can assert that the first value here in fact matches what the index claimed?

            // In the unbalanced case it's possible the left most node only has one child:
            if state.index_tree.node_exists() {
                // Leaf node; scan and filter all points in this block:
                let count = self.read_doc_ids(
                    state.input.as_mut(),
                    state.index_tree.leaf_block_fp(),
                    state.scratch_doc_ids.as_mut(),
                )?;

                // Again, this time reading values and checking with the visitor
                self.visit_doc_values(
                    state.common_prefix_lengths.as_mut(),
                    state.scratch_packed_value.as_mut(),
                    state.input.as_mut(),
                    state.scratch_doc_ids.as_ref(),
                    count,
                    state.visitor,
                )?;
            }
        } else {
            // Non-leaf node: recurse on the split left and right nodes
            let split_dim = state.index_tree.split_dim() as usize;
            debug_assert!(split_dim as i32 >= 0, format!("split_dim={}", split_dim));
            debug_assert!(split_dim < self.num_dims);

            let split_packed_value_idx = state.index_tree.split_packed_value_index();
            let mut split_dim_value = state.index_tree.split_dim_value();
            debug_assert_eq!(split_dim_value.len(), self.bytes_per_dim);

            // make sure cellMin <= splitValue <= cellMax:
            debug_assert!(
                bytes_compare(
                    self.bytes_per_dim,
                    &cell_min_packed[split_dim * self.bytes_per_dim..],
                    split_dim_value.as_slice()
                ) <= 0
            );
            debug_assert!(
                bytes_compare(
                    self.bytes_per_dim,
                    &cell_max_packed[split_dim * self.bytes_per_dim..],
                    split_dim_value.as_slice()
                ) >= 0
            );

            // Recurse on left sub-tree:
            state.index_tree.set_split_packed_value(
                split_packed_value_idx,
                0,
                &cell_max_packed[0..self.packed_bytes_length],
            );
            state.index_tree.set_split_packed_value(
                split_packed_value_idx,
                split_dim * self.bytes_per_dim,
                &split_dim_value,
            );
            let mut split_packed_value = state.index_tree.split_packed_value();

            state.index_tree.push_left()?;
            self.intersect_with_state(state, cell_min_packed, &split_packed_value)?;
            state.index_tree.pop();

            // Restore the split dim value since it may have been overwritten while recursing:
            split_dim_value[0..self.bytes_per_dim].copy_from_slice(
                &split_packed_value[split_dim * self.bytes_per_dim
                                        ..split_dim * self.bytes_per_dim + self.bytes_per_dim],
            );

            // Recurse on right sub-tree:
            split_packed_value[0..self.packed_bytes_length]
                .copy_from_slice(&cell_min_packed[0..self.packed_bytes_length]);
            split_packed_value[split_dim * self.bytes_per_dim
                                   ..split_dim * self.bytes_per_dim + self.bytes_per_dim]
                .copy_from_slice(&split_dim_value[0..self.bytes_per_dim]);
            state
                .index_tree
                .set_split_packed_value(split_packed_value_idx, 0, &split_packed_value);
            state.index_tree.set_split_dim_value(&split_dim_value);

            state.index_tree.push_right()?;
            self.intersect_with_state(state, split_packed_value.as_slice(), cell_max_packed)?;
            state.index_tree.pop();
        }

        Ok(())
    }

    pub fn create_intersect_state<'a>(
        &self,
        visitor: &'a mut IntersectVisitor,
    ) -> Result<IntersectState<'a>> {
        let index_tree: Box<IndexTree> = if !self.packed_index.is_empty() {
            Box::new(PackedIndexTree::new(
                self.bytes_per_dim,
                self.num_dims,
                self.tree_depth()?,
                self.packed_bytes_length,
                self.leaf_node_offset,
                Arc::clone(&self.packed_index),
            )?)
        } else {
            Box::new(LegacyIndexTree::new(
                self.bytes_per_dim,
                self.bytes_per_index_entry as i32,
                self.num_dims as i32,
                Arc::clone(&self.leaf_block_fps),
                self.version,
                self.tree_depth()?,
                self.packed_bytes_length,
                self.leaf_node_offset,
            ))
        };

        Ok(IntersectState::new(
            self.input.as_ref().clone()?,
            self.num_dims,
            self.packed_bytes_length,
            self.max_points_in_leaf_node,
            visitor,
            index_tree,
        ))
    }

    /// Fast path: this is called when the query box fully encompasses all cells under this
    /// node.
    fn add_all(&self, state: &mut IntersectState) -> Result<()> {
        if state.index_tree.is_leaf_node() {
            if state.index_tree.node_exists() {
                self.visit_doc_ids(
                    state.input.as_mut(),
                    state.index_tree.leaf_block_fp(),
                    state.visitor,
                )?;
            }
        // TODO: we can assert that the first value here in fact matches what the index claimed?
        } else {
            state.index_tree.push_left()?;
            self.add_all(state)?;
            state.index_tree.pop();

            state.index_tree.push_right()?;
            self.add_all(state)?;
            state.index_tree.pop();
        }

        Ok(())
    }

    /// Visits all docIDs and packed values in a single leaf block
    pub fn visit_leaf_block_values(
        &self,
        index_tree: &SimpleIndexTree,
        state: &mut IntersectState,
    ) -> Result<()> {
        // Leaf node; scan and filter all points in this block:
        let count = self.read_doc_ids(
            state.input.as_mut(),
            index_tree.leaf_block_fp(),
            &mut state.scratch_doc_ids,
        )?;

        // Again, this time reading values and checking with the visitor
        self.visit_doc_values(
            state.common_prefix_lengths.as_mut(),
            state.scratch_packed_value.as_mut(),
            state.input.as_mut(),
            &state.scratch_doc_ids,
            count,
            state.visitor,
        )
    }

    fn visit_doc_ids(
        &self,
        input: &mut IndexInput,
        block_fp: i64,
        visitor: &mut IntersectVisitor,
    ) -> Result<()> {
        // Leaf node
        input.seek(block_fp)?;

        // How many points are stored in this leaf cell:
        let count = input.read_vint()? as usize;
        visitor.grow(count);

        if self.version < BKD_VERSION_COMPRESSED_DOC_IDS {
            DocIdsWriter::read_ints32_with_visitor(input, count, visitor)
        } else {
            DocIdsWriter::read_ints_with_visitor(input, count, visitor)
        }
    }

    fn read_doc_ids(
        &self,
        input: &mut IndexInput,
        block_fp: i64,
        doc_ids: &mut [DocId],
    ) -> Result<usize> {
        // Leaf node
        input.seek(block_fp as i64)?;

        // How many points are stored in this leaf cell:
        let count = input.read_vint()? as usize;

        if self.version < BKD_VERSION_COMPRESSED_DOC_IDS {
            DocIdsWriter::read_ints32(input, count, doc_ids)?;
        } else {
            DocIdsWriter::read_ints(input, count, doc_ids)?;
        }

        Ok(count)
    }

    fn visit_doc_values(
        &self,
        common_prefix_lengths: &mut [i32],
        scratch_packed_value: &mut [u8],
        input: &mut IndexInput,
        doc_ids: &[DocId],
        count: usize,
        visitor: &mut IntersectVisitor,
    ) -> Result<()> {
        visitor.grow(count);

        self.read_common_prefixes(common_prefix_lengths, scratch_packed_value, input)?;

        let compressed_dim = if self.version < BKD_VERSION_COMPRESSED_VALUES {
            -1
        } else {
            self.read_compressed_dim(input)?
        };

        if compressed_dim == -1 {
            self.visit_raw_doc_values(
                common_prefix_lengths,
                scratch_packed_value,
                input,
                doc_ids,
                count,
                visitor,
            )?;
        } else {
            self.visit_compressed_doc_values(
                common_prefix_lengths,
                scratch_packed_value,
                input,
                doc_ids,
                count,
                visitor,
                compressed_dim as usize,
            )?;
        }

        Ok(())
    }

    // Just read suffixes for every dimension
    fn visit_raw_doc_values(
        &self,
        common_prefix_lengths: &[i32],
        scratch_packed_value: &mut [u8],
        input: &mut IndexInput,
        doc_ids: &[DocId],
        count: usize,
        visitor: &mut IntersectVisitor,
    ) -> Result<()> {
        for doc in doc_ids.iter().take(count) {
            for (dim, length) in common_prefix_lengths.iter().enumerate().take(self.num_dims) {
                let prefix = *length as usize;

                input.read_bytes(
                    scratch_packed_value,
                    dim * self.bytes_per_dim + prefix,
                    self.bytes_per_dim - prefix,
                )?;
            }

            visitor.visit_by_packed_value(*doc, scratch_packed_value)?;
        }

        Ok(())
    }

    #[allow(too_many_arguments)]
    fn visit_compressed_doc_values(
        &self,
        common_prefix_lengths: &mut [i32],
        scratch_packed_value: &mut [u8],
        input: &mut IndexInput,
        doc_ids: &[DocId],
        count: usize,
        visitor: &mut IntersectVisitor,
        compressed_dim: usize,
    ) -> Result<()> {
        // the byte at `compressedByteOffset` is compressed using run-length compression,
        // other suffix bytes are stored verbatim
        let compressed_byte_offset =
            compressed_dim * self.bytes_per_dim + common_prefix_lengths[compressed_dim] as usize;
        common_prefix_lengths[compressed_dim] += 1;

        let mut i = 0usize;
        while i < count {
            scratch_packed_value[compressed_byte_offset] = input.read_byte()?;

            let run_len = input.read_byte()? as usize;
            for j in 0..run_len {
                for (dim, length) in common_prefix_lengths.iter().enumerate().take(self.num_dims) {
                    let prefix = *length as usize;

                    input.read_bytes(
                        scratch_packed_value,
                        dim * self.bytes_per_dim + prefix,
                        self.bytes_per_dim - prefix,
                    )?;
                }

                visitor.visit_by_packed_value(doc_ids[i + j], scratch_packed_value)?;
            }

            i += run_len;
        }

        if i != count {
            bail!(ErrorKind::CorruptIndex(format!(
                "Sub blocks do not add up to the expected count: {} != {}",
                count, i
            )));
        }

        Ok(())
    }

    fn read_compressed_dim(&self, input: &mut IndexInput) -> Result<i32> {
        let compressed_dim = i32::from(input.read_byte()? as i8);

        if compressed_dim < -1 || compressed_dim >= self.num_dims as i32 {
            bail!(ErrorKind::CorruptIndex(format!(
                "Got compressedDim={}",
                compressed_dim
            )));
        }

        Ok(compressed_dim)
    }

    fn read_common_prefixes(
        &self,
        common_prefix_lengths: &mut [i32],
        scratch_packed_value: &mut [u8],
        input: &mut IndexInput,
    ) -> Result<()> {
        for (dim, length) in common_prefix_lengths
            .iter_mut()
            .enumerate()
            .take(self.num_dims)
        {
            let prefix = input.read_vint()?;

            *length = prefix;
            if prefix > 0 {
                input.read_bytes(
                    scratch_packed_value,
                    dim * self.bytes_per_dim,
                    prefix as usize,
                )?;
            }
        }

        Ok(())
    }
}

pub trait IndexTree {
    fn push_left(&mut self) -> Result<()>;
    fn push_right(&mut self) -> Result<()>;
    fn pop(&mut self);

    fn is_leaf_node(&self) -> bool;
    fn node_exists(&self) -> bool;
    fn node_id(&self) -> i32;
    fn split_packed_value(&self) -> Vec<u8>;
    fn split_packed_value_index(&self) -> usize;
    fn set_split_packed_value(&mut self, index: usize, offset: usize, data: &[u8]);
    /// Only valid after pushLeft or pushRight, not pop!
    fn split_dim(&self) -> i32;
    /// Only valid after pushLeft or pushRight, not pop!
    fn split_dim_value(&mut self) -> Vec<u8>;
    fn set_split_dim_value(&mut self, data: &[u8]);
    /// Only valid after pushLeft or pushRight, not pop!
    fn leaf_block_fp(&self) -> i64;
}

/// Used to walk the in-heap index
// @lucene.internal
pub struct SimpleIndexTree {
    node_id: i32,
    // level is 1-based so that we can do level-1 w/o checking each time:
    level: usize,
    split_dim: i32,
    split_packed_value_stack: Vec<Vec<u8>>,

    packed_bytes_length: usize,
    leaf_node_offset: i32,
}

impl SimpleIndexTree {
    pub fn new(depth: usize, packed_bytes_length: usize, leaf_node_offset: i32) -> SimpleIndexTree {
        let node_id = 1;
        let level = 1;
        let split_dim = 0;
        let split_packed_value_stack = vec![vec![0u8; packed_bytes_length]; depth + 1];

        SimpleIndexTree {
            node_id,
            level,
            split_dim,
            split_packed_value_stack,
            packed_bytes_length,
            leaf_node_offset,
        }
    }
}

impl IndexTree for SimpleIndexTree {
    fn push_left(&mut self) -> Result<()> {
        self.node_id *= 2;
        self.level += 1;

        if self.split_packed_value_stack[self.level].is_empty() {
            self.split_packed_value_stack[self.level].resize(self.packed_bytes_length, 0u8);
        }
        Ok(())
    }

    fn push_right(&mut self) -> Result<()> {
        self.node_id = self.node_id * 2 + 1;
        self.level += 1;

        if self.split_packed_value_stack[self.level].is_empty() {
            self.split_packed_value_stack[self.level].resize(self.packed_bytes_length, 0u8);
        }
        Ok(())
    }

    fn pop(&mut self) {
        self.node_id /= 2;
        self.level -= 1;
        self.split_dim -= 1;
    }

    fn is_leaf_node(&self) -> bool {
        self.node_id >= self.leaf_node_offset
    }

    fn node_exists(&self) -> bool {
        self.node_id - self.leaf_node_offset < self.leaf_node_offset
    }

    fn node_id(&self) -> i32 {
        self.node_id
    }

    fn split_packed_value(&self) -> Vec<u8> {
        self.split_packed_value_stack[self.level].clone()
    }

    fn split_packed_value_index(&self) -> usize {
        debug_assert_eq!(self.is_leaf_node(), false);
        debug_assert!(!self.split_packed_value_stack[self.level].is_empty());

        self.level
    }

    fn set_split_packed_value(&mut self, _index: usize, offset: usize, data: &[u8]) {
        let len = data.len();
        self.split_packed_value_stack[self.level][offset..offset + len].copy_from_slice(data)
    }

    /// Only valid after pushLeft or pushRight, not pop!
    fn split_dim(&self) -> i32 {
        debug_assert_eq!(self.is_leaf_node(), false);

        self.split_dim
    }

    /// Only valid after pushLeft or pushRight, not pop!
    fn split_dim_value(&mut self) -> Vec<u8> {
        unimplemented!()
    }

    fn set_split_dim_value(&mut self, _data: &[u8]) {
        unimplemented!()
    }

    /// Only valid after pushLeft or pushRight, not pop!
    fn leaf_block_fp(&self) -> i64 {
        unimplemented!()
    }
}

/// Reads the original simple yet heap-heavy index format
pub struct LegacyIndexTree {
    leaf_block_fp: i64,
    split_dim_value: Vec<u8>,
    index_tree: SimpleIndexTree,

    bytes_per_dim: usize,
    bytes_per_index_entry: i32,
    num_dims: i32,
    leaf_block_fps: Arc<Vec<i64>>,
    version: i32,
}

impl LegacyIndexTree {
    #[allow(too_many_arguments)]
    pub fn new(
        bytes_per_dim: usize,
        bytes_per_index_entry: i32,
        num_dims: i32,
        leaf_block_fps: Arc<Vec<i64>>,
        version: i32,
        depth: usize,
        packed_bytes_length: usize,
        leaf_node_offset: i32,
    ) -> LegacyIndexTree {
        let index_tree = SimpleIndexTree::new(depth, packed_bytes_length, leaf_node_offset);
        let split_dim_value = vec![0u8; bytes_per_dim];
        let mut legacy_index_tree = LegacyIndexTree {
            leaf_block_fp: 0,
            split_dim_value,
            index_tree,

            bytes_per_dim,
            bytes_per_index_entry,
            num_dims,
            leaf_block_fps,
            version,
        };

        legacy_index_tree.set_node_data();

        legacy_index_tree
    }

    fn set_node_data(&mut self) {
        if self.index_tree.is_leaf_node() {
            self.leaf_block_fp = self.leaf_block_fps
                [(self.index_tree.node_id - self.index_tree.leaf_node_offset) as usize];
            self.index_tree.split_dim -= 1;
        } else {
            self.leaf_block_fp = -1;
            let mut address = (self.index_tree.node_id * self.bytes_per_index_entry) as usize;

            if self.num_dims == 1 {
                self.index_tree.split_dim = 0;
                if self.version < BKD_VERSION_IMPLICIT_SPLIT_DIM_1D {
                    // skip over wastefully encoded 0 splitDim:
                    debug_assert_eq!(self.index_tree.split_packed_value()[address], 0);
                    address += 1;
                }
            } else {
                self.index_tree.split_dim =
                    i32::from(self.index_tree.split_packed_value()[address]);
                address += 1;
            }

            self.split_dim_value[0..self.bytes_per_dim].copy_from_slice(
                &self.index_tree.split_packed_value()[address..address + self.bytes_per_dim],
            );
        }
    }
}

impl IndexTree for LegacyIndexTree {
    fn push_left(&mut self) -> Result<()> {
        self.index_tree.push_left()?;
        self.set_node_data();
        Ok(())
    }

    fn push_right(&mut self) -> Result<()> {
        self.index_tree.push_right()?;
        self.set_node_data();
        Ok(())
    }

    fn pop(&mut self) {
        self.index_tree.pop();
        self.leaf_block_fp = -1;
    }

    fn is_leaf_node(&self) -> bool {
        self.index_tree.is_leaf_node()
    }

    fn node_exists(&self) -> bool {
        self.index_tree.node_exists()
    }

    fn node_id(&self) -> i32 {
        self.index_tree.node_id()
    }

    fn split_packed_value(&self) -> Vec<u8> {
        self.index_tree.split_packed_value()
    }

    fn split_packed_value_index(&self) -> usize {
        self.index_tree.split_packed_value_index()
    }

    fn set_split_packed_value(&mut self, index: usize, offset: usize, data: &[u8]) {
        self.index_tree.set_split_packed_value(index, offset, data)
    }

    fn split_dim(&self) -> i32 {
        self.index_tree.split_dim()
    }

    fn split_dim_value(&mut self) -> Vec<u8> {
        debug_assert!(!self.is_leaf_node());
        self.split_dim_value.clone()
    }

    fn set_split_dim_value(&mut self, data: &[u8]) {
        debug_assert_eq!(data.len(), self.split_dim_value.len());
        self.split_dim_value.copy_from_slice(data);
    }

    fn leaf_block_fp(&self) -> i64 {
        debug_assert!(self.is_leaf_node());
        self.leaf_block_fp
    }
}

/// Reads the new packed byte[] index format which can be up to ~63% smaller than the legacy index
/// format on 20M NYC taxis tests.  This
/// format takes advantage of the limited access pattern to the BKD tree at search time, i.e.
/// starting at the root node and recursing  downwards one child at a time.
pub struct PackedIndexTree {
    // used to read the packed byte[]
    input: ByteArrayDataInput<ByteArrayRef>,
    // holds the minimum (left most) leaf block file pointer for each level we've recursed to:
    leaf_block_fp_stack: Vec<i64>,
    // holds the address, in the packed byte[] index, of the left-node of each level:
    left_node_positions: Vec<i32>,
    // holds the address, in the packed byte[] index, of the right-node of each level:
    right_node_positions: Vec<i32>,
    // holds the splitDim for each level:
    split_dims: Vec<i32>,
    // true if the per-dim delta we read for the node at this level is a negative offset vs. the
    // last split on this dim; this is a packed 2D array, i.e. to access array[level][dim] you
    // read from negativeDeltas[level*numDims+dim].  this will be true if the last time we
    // split on this dimension, we next pushed to the left sub-tree:
    negative_deltas: Vec<bool>,
    // holds the packed per-level split values; the intersect method uses this to save the cell
    // min/max as it recurses:
    split_values_stack: Vec<Vec<u8>>,
    // scratch value to return from getPackedValue:
    scratch: Vec<u8>,
    index_tree: SimpleIndexTree,

    bytes_per_dim: usize,
    num_dims: usize,
    packed_bytes_length: usize,
}

impl PackedIndexTree {
    pub fn new(
        bytes_per_dim: usize,
        num_dims: usize,
        depth: usize,
        packed_bytes_length: usize,
        leaf_node_offset: i32,
        packed_index: Arc<Vec<u8>>,
    ) -> Result<PackedIndexTree> {
        let index_tree = SimpleIndexTree::new(depth, packed_bytes_length, leaf_node_offset);

        let leaf_block_fp_stack = vec![0i64; depth + 1];
        let left_node_positions = vec![0i32; depth + 1];
        let right_node_positions = vec![0i32; depth + 1];
        let split_values_stack: Vec<Vec<u8>> = vec![vec![0u8; packed_bytes_length]; depth + 1];
        let split_dims = vec![0i32; depth + 1];
        let negative_deltas = vec![false; num_dims * (depth + 1)];
        let scratch = vec![0u8; bytes_per_dim];

        let mut packed_index_tree = PackedIndexTree {
            input: ByteArrayDataInput::new(ByteArrayRef::new(packed_index)),
            leaf_block_fp_stack,
            left_node_positions,
            right_node_positions,
            split_dims,
            negative_deltas,
            split_values_stack,
            scratch,
            index_tree,

            bytes_per_dim,
            num_dims,
            packed_bytes_length,
        };

        packed_index_tree.read_node_data(false)?;

        Ok(packed_index_tree)
    }

    fn read_node_data(&mut self, is_left: bool) -> Result<()> {
        let level = self.index_tree.level;
        self.leaf_block_fp_stack[level] = self.leaf_block_fp_stack[level - 1];

        // read leaf block FP delta
        if !is_left {
            self.leaf_block_fp_stack[level] += self.input.read_vlong()?;
        }

        if self.index_tree.is_leaf_node() {
            self.index_tree.split_dim = -1;
        } else {
            // read split dim, prefix, firstDiffByteDelta encoded as int:
            let mut code = self.input.read_vint()?;

            self.index_tree.split_dim = code % self.num_dims as i32;
            self.split_dims[level] = self.index_tree.split_dim;

            code /= self.num_dims as i32;

            let prefix = (code % (1 + self.bytes_per_dim as i32)) as usize;
            let suffix = self.bytes_per_dim - prefix;

            if self.split_values_stack[level].is_empty() {
                let len = self.packed_bytes_length;
                self.split_values_stack[level].resize(len, 0u8);
            }

            {
                let size = self.split_values_stack[level - 1].len();
                for i in 0..size {
                    let v = self.split_values_stack[level - 1][i];
                    self.split_values_stack[level][i] = v;
                }
            }

            if suffix > 0 {
                let mut first_diff_byte_delta = code / (1 + self.bytes_per_dim) as i32;
                if self.negative_deltas[level * self.num_dims + self.index_tree.split_dim as usize]
                {
                    first_diff_byte_delta = -first_diff_byte_delta;
                }

                let old_byte = i32::from(
                    self.split_values_stack[level]
                        [self.index_tree.split_dim as usize * self.bytes_per_dim + prefix],
                );
                self.split_values_stack[level]
                    [self.index_tree.split_dim as usize * self.bytes_per_dim + prefix] =
                    (old_byte + first_diff_byte_delta) as u8;
                self.input.read_bytes(
                    self.split_values_stack[level].as_mut(),
                    self.index_tree.split_dim as usize * self.bytes_per_dim + prefix + 1,
                    suffix - 1,
                )?;
            } else {
                // our split value is == last split value in this dim, which can happen when there
                // are many duplicate values
            }

            let left_num_bytes = if self.index_tree.node_id * 2 < self.index_tree.leaf_node_offset {
                self.input.read_vint()?
            } else {
                0
            };

            self.left_node_positions[level] = self.input.position() as i32;
            self.right_node_positions[level] = self.left_node_positions[level] + left_num_bytes;
        }
        Ok(())
    }
}

impl IndexTree for PackedIndexTree {
    fn push_left(&mut self) -> Result<()> {
        let node_position = self.left_node_positions[self.index_tree.level];
        self.index_tree.push_left()?;

        let to_copy: Vec<bool> = Vec::from(
            &self.negative_deltas[(self.index_tree.level - 1) * self.num_dims
                                      ..self.index_tree.level * self.num_dims],
        );

        self.negative_deltas[self.index_tree.level * self.num_dims
                                 ..self.index_tree.level * self.num_dims + self.num_dims]
            .copy_from_slice(to_copy.as_slice());

        debug_assert_ne!(self.index_tree.split_dim as i32, -1);
        self.negative_deltas
            [self.index_tree.level * self.num_dims + self.index_tree.split_dim as usize] = true;

        self.input.set_position(node_position as usize);

        self.read_node_data(true)
    }

    fn push_right(&mut self) -> Result<()> {
        let node_position = self.right_node_positions[self.index_tree.level];
        self.index_tree.push_right()?;

        let to_copy: Vec<bool> = Vec::from(
            &self.negative_deltas[(self.index_tree.level - 1) * self.num_dims
                                      ..self.index_tree.level * self.num_dims],
        );

        self.negative_deltas[self.index_tree.level * self.num_dims
                                 ..self.index_tree.level * self.num_dims + self.num_dims]
            .copy_from_slice(to_copy.as_slice());

        debug_assert_ne!(self.index_tree.split_dim as i32, -1);
        self.negative_deltas
            [self.index_tree.level * self.num_dims + self.index_tree.split_dim as usize] = false;

        self.input.set_position(node_position as usize);

        self.read_node_data(false)
    }

    fn pop(&mut self) {
        self.index_tree.pop();
        self.index_tree.split_dim = self.split_dims[self.index_tree.level];
    }

    fn is_leaf_node(&self) -> bool {
        self.index_tree.is_leaf_node()
    }

    fn node_exists(&self) -> bool {
        self.index_tree.node_exists()
    }

    fn node_id(&self) -> i32 {
        self.index_tree.node_id()
    }

    fn split_packed_value(&self) -> Vec<u8> {
        self.index_tree.split_packed_value()
    }

    fn split_packed_value_index(&self) -> usize {
        self.index_tree.split_packed_value_index()
    }

    fn set_split_packed_value(&mut self, index: usize, offset: usize, data: &[u8]) {
        self.index_tree.set_split_packed_value(index, offset, data)
    }

    fn split_dim(&self) -> i32 {
        self.index_tree.split_dim()
    }

    fn split_dim_value(&mut self) -> Vec<u8> {
        debug_assert!(!self.is_leaf_node());
        let split_dim = self.index_tree.split_dim as usize;
        self.scratch[0..self.bytes_per_dim].copy_from_slice(
            &self.split_values_stack[self.index_tree.level]
                [split_dim * self.bytes_per_dim..(split_dim + 1) * self.bytes_per_dim],
        );
        self.scratch.clone()
    }

    fn set_split_dim_value(&mut self, data: &[u8]) {
        debug_assert_eq!(data.len(), self.scratch.len());
        self.scratch.copy_from_slice(data);
    }

    fn leaf_block_fp(&self) -> i64 {
        debug_assert!(
            self.is_leaf_node(),
            format!("node_id={} is not a leaf", self.index_tree.node_id)
        );
        self.leaf_block_fp_stack[self.index_tree.level as usize]
    }
}

pub struct DocIdsWriter;

impl DocIdsWriter {
    /// Read {@code count} integers into {@code docIDs}.
    fn read_ints(input: &mut IndexInput, count: usize, doc_ids: &mut [DocId]) -> Result<()> {
        let bpv = input.read_byte()?;

        match bpv {
            0 => DocIdsWriter::read_delta_vints(input, count, doc_ids),
            32 => DocIdsWriter::read_ints32(input, count, doc_ids),
            24 => DocIdsWriter::read_ints24(input, count, doc_ids),
            _ => bail!("Unsupported number of bits per value: {}", bpv),
        }
    }

    fn read_delta_vints(input: &mut IndexInput, count: usize, doc_ids: &mut [DocId]) -> Result<()> {
        let mut doc = 0;
        for id in doc_ids.iter_mut().take(count) {
            doc += input.read_vint()?;
            *id = doc;
        }

        Ok(())
    }

    fn read_ints32(input: &mut IndexInput, count: usize, doc_ids: &mut [DocId]) -> Result<()> {
        for id in doc_ids.iter_mut().take(count) {
            *id = input.read_int()?;
        }

        Ok(())
    }

    fn read_ints24(input: &mut IndexInput, count: usize, doc_ids: &mut [DocId]) -> Result<()> {
        let mut i = 0usize;

        while i + 7 < count {
            let l1 = input.read_long()?;
            let l2 = input.read_long()?;
            let l3 = input.read_long()?;

            doc_ids[i] = l1.unsigned_shift(40) as i32;
            doc_ids[i + 1] = (l1.unsigned_shift(16) & 0xFF_FFFF) as i32;
            doc_ids[i + 2] = (((l1 & 0xFFFF) << 8) | l2.unsigned_shift(56)) as i32;
            doc_ids[i + 3] = (l2.unsigned_shift(32) & 0xFF_FFFF) as i32;
            doc_ids[i + 4] = (l2.unsigned_shift(8) & 0xFF_FFFF) as i32;
            doc_ids[i + 5] = (((l2 & 0xFF) << 16) | l3.unsigned_shift(48)) as i32;
            doc_ids[i + 6] = (l3.unsigned_shift(24) & 0xFF_FFFF) as i32;
            doc_ids[i + 7] = (l3 & 0xFF_FFFF) as i32;

            i += 8;
        }

        for id in doc_ids.iter_mut().take(count).skip(i) {
            let l1 = input.read_short()? as u32;
            let l2 = u32::from(input.read_byte()?);
            *id = ((l1 << 8) | l2) as i32;
        }

        Ok(())
    }

    /// Read {@code count} integers and feed the result directly to {@link
    /// IntersectVisitor#visit(int)}.
    fn read_ints_with_visitor(
        input: &mut IndexInput,
        count: usize,
        visitor: &mut IntersectVisitor,
    ) -> Result<()> {
        let bpv = input.read_byte()?;

        match bpv {
            0 => DocIdsWriter::read_delta_vints_with_visitor(input, count, visitor),
            32 => DocIdsWriter::read_ints32_with_visitor(input, count, visitor),
            24 => DocIdsWriter::read_ints24_with_visitor(input, count, visitor),
            _ => panic!("Unsupported number of bits per value: {}", bpv),
        }
    }

    fn read_delta_vints_with_visitor(
        input: &mut IndexInput,
        count: usize,
        visitor: &mut IntersectVisitor,
    ) -> Result<()> {
        let mut doc = 0;
        for _ in 0..count {
            doc += input.read_vint()?;
            visitor.visit(doc)?;
        }

        Ok(())
    }

    fn read_ints32_with_visitor(
        input: &mut IndexInput,
        count: usize,
        visitor: &mut IntersectVisitor,
    ) -> Result<()> {
        for _ in 0..count {
            visitor.visit(input.read_vint()?)?;
        }

        Ok(())
    }

    fn read_ints24_with_visitor(
        input: &mut IndexInput,
        count: usize,
        visitor: &mut IntersectVisitor,
    ) -> Result<()> {
        let mut i = 0;

        while i + 7 < count {
            let l1 = input.read_long()?;
            let l2 = input.read_long()?;
            let l3 = input.read_long()?;

            visitor.visit(l1.unsigned_shift(40) as i32)?;
            visitor.visit((l1.unsigned_shift(16) & 0xFF_FFFF) as i32)?;
            visitor.visit((((l1 & 0xFFFF) << 8) | l2.unsigned_shift(56)) as i32)?;
            visitor.visit((l2.unsigned_shift(32) & 0xFF_FFFF) as i32)?;
            visitor.visit((l2.unsigned_shift(8) & 0xFF_FFFF) as i32)?;
            visitor.visit((((l2 & 0xFF) << 16) | l3.unsigned_shift(48)) as i32)?;
            visitor.visit((l3.unsigned_shift(24) & 0xFF_FFFF) as i32)?;
            visitor.visit((l3 & 0xFF_FFFF) as i32)?;

            i += 8;
        }

        for _ in i..count {
            let l1 = input.read_short()? as u32;
            let l2 = u32::from(input.read_byte()?);
            visitor.visit(((l1 << 8) | l2) as i32)?;
        }

        Ok(())
    }
}
