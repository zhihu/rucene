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

use std::cmp::{max, min};
use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::mem;

use core::util::bit_util::BitsRequired;
use core::util::fst::bytes_store::StoreBytesReader;
use core::util::fst::fst_reader::{CompiledAddress, InputType};
use core::util::fst::{BytesReader, Output, OutputFactory, FST};
use core::util::ints_ref::{IntsRef, IntsRefBuilder};
use core::util::packed::COMPACT;
use core::util::packed::{PagedGrowableWriter, PagedMutableWriter};
use core::util::LongValues;

use error::Result;

/// Builds a minimal FST (maps an IntsRef term to an arbitrary
/// output) from pre-sorted terms with outputs.  The FST
/// becomes an FSA if you use NoOutputs.  The FST is written
/// on-the-fly into a compact serialized format byte array, which can
/// be saved to / loaded from a Directory or used directly
/// for traversal.  The FST is always finite (no cycles).
///
/// NOTE: The algorithm is described at
/// http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.24.3698</p>
///
/// The parameterized type T is the output type.  See the
/// subclasses of {@link Outputs}.
///
/// FSTs larger than 2.1GB are now possible (as of Lucene
/// 4.2).  FSTs containing more than 2.1B nodes are also now
/// possible, however they cannot be packed.
pub struct FstBuilder<F: OutputFactory> {
    dedup_hash: Option<NodeHash<F>>,
    pub fst: FST<F>,
    no_output: F::Value,
    // simplistic pruning: we prune node (and all following
    // nodes) if less than this number of terms go through it:
    min_suffix_count1: u32,
    // better pruning: we prune node (and all following
    // nodes) if the prior node has less than this number of
    // terms go through it:
    min_suffix_count2: u32,
    do_share_non_singleton_nodes: bool,
    share_max_tail_length: u32,
    last_input: IntsRefBuilder,
    // NOTE: cutting this over to ArrayList instead loses ~6%
    // in build performance on 9.8M Wikipedia terms; so we
    // left this as an array:
    // current "frontier"
    pub frontier: Vec<UnCompiledNode<F>>,
    // Used for the BIT_TARGET_NEXT optimization (whereby
    // instead of storing the address of the target node for
    // a given arc, we mark a single bit noting that the next
    // node in the bytes is the target node):
    pub last_frozen_node: i64,
    // Reused temporarily while building the FST:
    pub reused_bytes_per_arc: Vec<usize>,
    pub arc_count: u64,
    pub node_count: u64,
    pub allow_array_arcs: bool,
    do_share_suffix: bool,
    inited: bool,
    // bytes: BytesStore,    // this is fst.bytes_store
}

impl<F: OutputFactory> FstBuilder<F> {
    pub fn new(input_type: InputType, outputs: F) -> Self {
        Self::build(
            input_type,
            0,
            0,
            true,
            true,
            i32::max_value() as u32,
            outputs,
            true,
            15,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn build(
        input_type: InputType,
        min_suffix_count1: u32,
        min_suffix_count2: u32,
        do_share_suffix: bool,
        do_share_non_singleton_nodes: bool,
        share_max_tail_length: u32,
        outputs: F,
        allow_array_arcs: bool,
        bytes_page_bits: u32,
    ) -> Self {
        let no_output = outputs.empty();
        let fst = FST::new(input_type, outputs, bytes_page_bits as usize);

        FstBuilder {
            dedup_hash: None,
            fst,
            no_output,
            min_suffix_count1,
            min_suffix_count2,
            do_share_non_singleton_nodes,
            share_max_tail_length,
            last_input: IntsRefBuilder::new(),
            frontier: Vec::with_capacity(10),
            last_frozen_node: 0,
            reused_bytes_per_arc: Vec::with_capacity(4),
            arc_count: 0,
            node_count: 0,
            allow_array_arcs,
            do_share_suffix,
            inited: false,
        }
    }

    // this should be call after new FstBuilder
    pub fn init(&mut self) {
        if self.do_share_suffix {
            let reader = self.fst.bytes_store.get_reverse_reader();
            let dedup_hash = NodeHash::new(&mut self.fst, reader);
            self.dedup_hash = Some(dedup_hash);
        }
        for i in 0..10 {
            let node = UnCompiledNode::new(self, i);
            self.frontier.push(node);
        }
        self.inited = true;
    }

    pub fn term_count(&self) -> i64 {
        self.frontier[0].input_count
    }

    fn compile_node(&mut self, node_index: usize, tail_length: u32) -> Result<CompiledAddress> {
        debug_assert!(self.inited);
        let node: i64;
        let bytes_pos_start = self.fst.bytes_store.get_position();

        let builder = self as *mut FstBuilder<F>;
        unsafe {
            if let Some(ref mut dedup_hash) = self.dedup_hash {
                if (self.do_share_non_singleton_nodes || self.frontier[node_index].num_arcs <= 1)
                    && tail_length <= self.share_max_tail_length
                {
                    if self.frontier[node_index].num_arcs == 0 {
                        node = self.fst.add_node(&mut *builder, node_index)?;
                        self.last_frozen_node = node;
                    } else {
                        node = dedup_hash.add(&mut *builder, node_index)? as i64;
                    }
                } else {
                    node = self.fst.add_node(&mut *builder, node_index)?;
                }
            } else {
                node = self.fst.add_node(&mut *builder, node_index)?;
            }
        }
        assert_ne!(node, -2);

        let bytes_pos_end = self.fst.bytes_store.get_position();
        if bytes_pos_end != bytes_pos_start {
            // fst added a new node
            assert!(bytes_pos_end > bytes_pos_start);
            self.last_frozen_node = node;
        }

        self.frontier[node_index].clear();

        Ok(node)
    }

    #[allow(unused_assignments)]
    fn freeze_tail(&mut self, prefix_len_plus1: usize) -> Result<()> {
        debug_assert!(self.inited);
        let down_to = max(1, prefix_len_plus1);
        if self.last_input.length < down_to {
            return Ok(());
        }
        for i in 0..=self.last_input.length - down_to {
            let idx = self.last_input.length - i;
            let mut do_prune = false;
            let mut do_compile = false;

            let tmp = UnCompiledNode::new(self, 0);
            let mut parent = mem::replace(&mut self.frontier[idx - 1], tmp);

            if self.frontier[idx].input_count < self.min_suffix_count1 as i64 {
                do_prune = true;
                do_compile = true;
            } else if idx > prefix_len_plus1 {
                // prune if parent's input_count is less than suffix_min_count2
                if parent.input_count < self.min_suffix_count2 as i64
                    || (self.min_suffix_count2 == 1 && parent.input_count == 1 && idx > 1)
                {
                    // my parent, about to be compiled, doesn't make the cut, so
                    // I'm definitely pruned

                    // if minSuffixCount2 is 1, we keep only up
                    // until the 'distinguished edge', ie we keep only the
                    // 'divergent' part of the FST. if my parent, about to be
                    // compiled, has inputCount 1 then we are already past the
                    // distinguished edge.  NOTE: this only works if
                    // the FST outputs are not "compressible" (simple
                    // ords ARE compressible).
                    do_prune = true;
                } else {
                    // my parent, about to be compiled, does make the cut, so
                    // I'm definitely not pruned
                    do_prune = false;
                }
                do_compile = true;
            } else {
                // if pruning is disabled (count is 0) we can always
                // compile current node
                do_compile = self.min_suffix_count2 == 0;
            }

            if self.frontier[idx].input_count < self.min_suffix_count2 as i64
                || (self.min_suffix_count2 == 1 && self.frontier[idx].input_count == 1 && idx > 1)
            {
                // drop all arcs
                for arc_idx in 0..self.frontier[idx].num_arcs {
                    if let Node::UnCompiled(target) = self.frontier[idx].arcs[arc_idx].target {
                        self.frontier[target].clear();
                    }
                }
                self.frontier[idx].num_arcs = 0;
            }

            if do_prune {
                // this node doesn't make it -- deref it
                self.frontier[idx].clear();
                parent.delete_last(self.last_input.int_at(idx - 1), &Node::UnCompiled(idx));
            } else {
                if self.min_suffix_count2 != 0 {
                    let tail_len = self.last_input.length - idx;
                    self.compile_all_targets(idx, tail_len)?;
                }

                let next_final_output = self.frontier[idx].output.clone();
                // We "fake" the node as being final if it has no
                // outgoing arcs; in theory we could leave it
                // as non-final (the FST can represent this), but
                // FSTEnum, Util, etc., have trouble w/ non-final
                // dead-end states:
                let is_final = self.frontier[idx].is_final || self.frontier[idx].num_arcs == 0;

                if do_compile {
                    // this node makes it and we now compile it.  first,
                    // compile any targets that were previously
                    // undecided:
                    let tail_len = (1 + self.last_input.length - idx) as u32;
                    let n = self.compile_node(idx, tail_len)?;
                    parent.replace_last(
                        self.last_input.int_at(idx - 1),
                        Node::Compiled(n),
                        next_final_output,
                        is_final,
                    );
                } else {
                    // replaceLast just to install
                    // next_final_output/is_final onto the arc
                    parent.replace_last(
                        self.last_input.int_at(idx - 1),
                        Node::UnCompiled(0), // a stub node,
                        next_final_output,
                        is_final,
                    );
                    // this node will stay in play for now, since we are
                    // undecided on whether to prune it.  later, it
                    // will be either compiled or pruned, so we must
                    // allocate a new node:
                    self.frontier[idx] = UnCompiledNode::new(self, idx as i32);
                }
            }
            self.frontier[idx - 1] = parent;
        }

        Ok(())
    }

    /// Add the next input/output pair.  The provided input
    /// must be sorted after the previous one according to
    /// `IntsRef#compareTo`.  It's also OK to add the same
    /// input twice in a row with different outputs, as long
    /// as `OutputFactory` implements the `OutputFactory#merge`
    /// method. Note that input is fully consumed after this
    /// method is returned (so caller is free to reuse), but
    /// output is not.  So if your outputs are changeable (eg
    /// `ByteSequenceOutputs`) then you cannot reuse across
    /// calls.
    pub fn add(&mut self, input: IntsRef, output: F::Value) -> Result<()> {
        debug_assert!(self.inited);
        assert!(self.last_input.length == 0 || input > self.last_input.get());
        let mut output = output;

        if self.frontier.len() < input.length + 1 {
            for i in self.frontier.len()..input.length + 2 {
                let node = UnCompiledNode::new(self, i as i32);
                self.frontier.push(node);
            }
        }

        if input.length == 0 {
            // empty input: only allowed as first input.  we have
            // to special case this because the packed FST
            // format cannot represent the empty input since
            // 'finalness' is stored on the incoming arc, not on
            // the node
            self.frontier[0].input_count += 1;
            self.frontier[0].is_final = true;
            self.fst.set_empty_output(output);
            return Ok(());
        }

        // compare shared prefix length
        let mut pos1 = 0;
        let mut pos2 = input.offset;
        let pos1_stop = min(self.last_input.length, input.length);
        loop {
            self.frontier[pos1].input_count += 1;
            if pos1 >= pos1_stop || self.last_input.int_at(pos1) != input.ints()[pos2] {
                break;
            }
            pos1 += 1;
            pos2 += 1;
        }
        let prefix_len_plus1 = pos1 + 1;

        // minimize/compile states from previous input's
        // orphan'd suffix
        self.freeze_tail(prefix_len_plus1)?;

        // init tail states for current input
        for i in prefix_len_plus1..=input.length {
            let node = Node::UnCompiled(i);
            self.frontier[i - 1].add_arc(input.ints()[input.offset + i - 1], node);
            self.frontier[i].input_count += 1;
        }

        let last_idx = input.length;
        if self.last_input.length != input.length || prefix_len_plus1 != input.length + 1 {
            self.frontier[last_idx].is_final = true;
            self.frontier[last_idx].output = self.no_output.clone();
        }

        // push conflicting outputs forward, only as far as needed
        for i in 1..prefix_len_plus1 {
            let last_output = self.frontier[i - 1]
                .get_last_output(input.ints()[input.offset + i - 1])
                .clone();

            let common_output_prefix: F::Value;
            if last_output != self.no_output {
                common_output_prefix = self.fst.outputs().common(&output, &last_output);
                let word_suffix = self
                    .fst
                    .outputs()
                    .subtract(&last_output, &common_output_prefix);
                self.frontier[i].prepend_output(word_suffix);
            } else {
                common_output_prefix = self.no_output.clone();
            }
            output = self.fst.outputs().subtract(&output, &common_output_prefix);
            if last_output != self.no_output {
                self.frontier[i - 1]
                    .set_last_output(input.ints()[input.offset + i - 1], common_output_prefix);
            }
        }

        if self.last_input.length == input.length && prefix_len_plus1 == input.length + 1 {
            // same input more than 1 time in a row, mapping to
            // multiple outputs
            self.frontier[last_idx].output = self
                .fst
                .outputs()
                .merge(&self.frontier[last_idx].output, &output);
        } else {
            // this new arc is private to this new input; set its
            // arc output to the leftover output:
            self.frontier[prefix_len_plus1 - 1]
                .set_last_output(input.ints()[input.offset + prefix_len_plus1 - 1], output);
        }

        // save last input
        self.last_input.copy_ints_ref(&input);

        Ok(())
    }

    // Returns final FST. NOTE: this will return None if nothing is accepted by the fst
    pub fn finish(&mut self) -> Result<Option<FST<F>>> {
        debug_assert!(self.inited);
        // minimize nodes in the last word's suffix
        self.freeze_tail(0)?;

        if self.frontier[0].input_count < self.min_suffix_count1 as i64
            || self.frontier[0].input_count < self.min_suffix_count2 as i64
            || self.frontier[0].num_arcs == 0
        {
            if self.fst.empty_output.is_none()
                || (self.min_suffix_count1 > 0 || self.min_suffix_count2 > 0)
            {
                return Ok(None);
            }
        } else if self.min_suffix_count2 != 0 {
            let tail_len = self.last_input.length;
            self.compile_all_targets(0, tail_len)?;
        }

        let node = {
            let tail_len = self.last_input.length as u32;
            self.compile_node(0, tail_len)?
        };
        self.fst.finish(node)?;

        // create a tmp for mem replace
        let tmp_fst = FST::new(self.fst.input_type, self.fst.outputs().clone(), 1);
        let fst = mem::replace(&mut self.fst, tmp_fst);
        Ok(Some(fst))
    }

    fn compile_all_targets(&mut self, node_idx: usize, tail_length: usize) -> Result<()> {
        for i in 0..self.frontier[node_idx].num_arcs {
            if let Node::UnCompiled(index) = self.frontier[node_idx].arcs[i].target {
                // not yet compiled
                if self.frontier[index].num_arcs == 0 {
                    self.frontier[node_idx].arcs[i].is_final = true;
                    self.frontier[index].is_final = true;
                }
                self.frontier[node_idx].arcs[i].target =
                    Node::Compiled(self.compile_node(index, tail_length as u32 - 1)? as i64);
            }
        }

        Ok(())
    }
}

pub struct BuilderArc<F: OutputFactory> {
    pub label: i32,
    pub target: Node,
    pub is_final: bool,
    pub output: F::Value,
    pub next_final_output: F::Value,
}

impl<F: OutputFactory> fmt::Debug for BuilderArc<F> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let target = match self.target {
            Node::Compiled(c) => format!("Compiled({})", c),
            Node::UnCompiled(_) => "UnCompiled".to_string(),
        };
        write!(
            f,
            "BuilderArc(label: {}, is_final: {}, output: {:?}, next_final_output: {:?}, target: \
             {})",
            self.label, self.is_final, self.output, self.next_final_output, target
        )
    }
}

impl<F> Clone for BuilderArc<F>
where
    F: OutputFactory,
{
    fn clone(&self) -> Self {
        BuilderArc {
            label: self.label,
            target: self.target.clone(),
            is_final: self.is_final,
            output: self.output.clone(),
            next_final_output: self.next_final_output.clone(),
        }
    }
}

/// used to dedup states (lookup already-frozen states)
struct NodeHash<F: OutputFactory> {
    table: PagedGrowableWriter,
    count: usize,
    mask: usize,

    fst: *mut FST<F>,
    input: StoreBytesReader,
}

impl<F: OutputFactory> NodeHash<F> {
    pub fn new(fst: &mut FST<F>, input: StoreBytesReader) -> Self {
        let table = PagedGrowableWriter::new(16, 1 << 27, 8, COMPACT);
        NodeHash {
            table,
            count: 0,
            mask: 15,
            fst: fst as *mut FST<F>,
            input,
        }
    }

    #[allow(clippy::mut_from_ref)]
    fn fst(&self) -> &mut FST<F> {
        unsafe { &mut (*self.fst) }
    }

    fn nodes_equal(&mut self, node: &UnCompiledNode<F>, address: CompiledAddress) -> Result<bool> {
        let reader = &mut self.input as *mut StoreBytesReader;
        let mut scratch_arc = unsafe { self.fst().read_first_real_arc(address, &mut *reader)? };
        if scratch_arc.bytes_per_arc > 0 && node.num_arcs != scratch_arc.num_arcs {
            return Ok(false);
        }

        for idx in 0..node.num_arcs {
            let arc = &node.arcs[idx];
            if arc.label != scratch_arc.label || arc.is_final != scratch_arc.is_final() {
                return Ok(false);
            }

            if let Some(ref output) = scratch_arc.output {
                if output != &arc.output {
                    return Ok(false);
                }
            } else if !arc.output.is_empty() {
                return Ok(false);
            }

            if let Some(ref output) = scratch_arc.next_final_output {
                if output != &arc.next_final_output {
                    return Ok(false);
                }
            } else if !arc.next_final_output.is_empty() {
                return Ok(false);
            }

            if let Node::Compiled(ref node) = arc.target {
                if *node != scratch_arc.target {
                    return Ok(false);
                }
            }

            if scratch_arc.is_last() {
                return Ok(idx == node.num_arcs - 1);
            }
            unsafe {
                self.fst()
                    .read_next_real_arc(&mut scratch_arc, &mut *reader)?
            };
        }
        Ok(false)
    }

    fn hash_code<Y: Hash>(&self, v: &Y) -> u64 {
        let mut state = DefaultHasher::new();
        v.hash(&mut state);
        state.finish()
    }

    fn node_hash_uncompiled(&self, node: &UnCompiledNode<F>) -> u64 {
        let prime = 31u64;
        let mut h = 0u64;
        // TODO maybe if number of arcs is high we can safely subsample?
        let no_output = self.fst().outputs().empty();
        for arc in &node.arcs[0..node.num_arcs] {
            h = prime.wrapping_mul(h).wrapping_add(arc.label as u64);
            if let Node::Compiled(n) = arc.target {
                if n != 0 {
                    h = prime.wrapping_mul(h).wrapping_add((n ^ (n >> 32)) as u64);
                }
            }
            if arc.output != no_output {
                h = prime
                    .wrapping_mul(h)
                    .wrapping_add(self.hash_code(&arc.output));
            }
            if arc.next_final_output != no_output {
                h = prime
                    .wrapping_mul(h)
                    .wrapping_add(self.hash_code(&arc.next_final_output));
            }
            if arc.is_final {
                h = h.wrapping_add(17);
            }
        }
        h
    }

    fn node_hash_compiled(&self, n: CompiledAddress, input: &mut dyn BytesReader) -> Result<u64> {
        let prime = 31u64;
        let mut h = 0u64;
        let mut arc = self.fst().read_first_real_arc(n, input)?;
        loop {
            h = prime.wrapping_mul(h).wrapping_add(arc.label as u64);
            if arc.target != 0 {
                h = prime
                    .wrapping_mul(h)
                    .wrapping_add((arc.target ^ (arc.target >> 32)) as u64);
            }
            if let Some(ref output) = arc.output {
                h = prime.wrapping_mul(h).wrapping_add(self.hash_code(output));
            }
            if let Some(ref output) = arc.next_final_output {
                h = prime.wrapping_mul(h).wrapping_add(self.hash_code(output));
            }
            if arc.is_final() {
                h = h.wrapping_add(17);
            }
            if arc.is_last() {
                break;
            }
            self.fst().read_next_real_arc(&mut arc, input)?;
        }
        Ok(h)
    }

    pub fn add(&mut self, builder: &mut FstBuilder<F>, node_index: usize) -> Result<u64> {
        let h = self.node_hash_uncompiled(&builder.frontier[node_index]);
        let mut pos = h & self.mask as u64;
        let mut c = 0;
        let reader = &mut self.input as *mut StoreBytesReader;
        loop {
            let v = self.table.get64(pos as i64)?;
            if v == 0 {
                unsafe {
                    // freeze & add
                    let node = self.fst().add_node(builder, node_index)?;
                    let compiled_hash = self.node_hash_compiled(node, &mut *reader)?;
                    assert_eq!(compiled_hash, h);
                    self.count += 1;
                    self.table.set(pos as usize, node);
                    assert_eq!(self.table.get64(pos as i64).unwrap(), node);
                    // rehash at 2/3 occupancy:
                    if self.count > 2 * self.table.paged_mutable_base().size / 3 {
                        self.rehash(&mut *reader)?;
                    }
                    return Ok(node as u64);
                }
            } else if self.nodes_equal(&builder.frontier[node_index], v)? {
                // same node is already here
                return Ok(v as u64);
            }

            // quadratic probe
            c += 1;
            pos = (pos + c) & self.mask as u64;
        }
    }

    fn rehash(&mut self, input: &mut dyn BytesReader) -> Result<()> {
        let old_size = self.table.size();
        let new_table = PagedGrowableWriter::new(
            2 * old_size,
            1 << 30,
            self.count.bits_required() as i32,
            COMPACT,
        );
        self.mask = new_table.size() - 1;
        let old_table = mem::replace(&mut self.table, new_table);
        for i in 0..old_size {
            let address = old_table.get64(i as i64)?;
            if address != 0 {
                self.add_new(address, input)?;
            }
        }

        Ok(())
    }

    // called only by rehash
    fn add_new(&mut self, address: i64, input: &mut dyn BytesReader) -> Result<()> {
        let hash = self.node_hash_compiled(address, input)? as usize;
        let mut pos = hash & self.mask;
        let mut c = 0;
        loop {
            if self.table.get64(pos as i64)? == 0 {
                self.table.set(pos, address);
                break;
            }

            // quadratic probe
            c += 1;
            pos = (pos + c) & self.mask;
        }
        Ok(())
    }
}

// NOTE: not many instances of Node or CompiledNode are in
// memory while the FST is being built; it's only the
// current "frontier":
#[derive(Clone)]
pub enum Node {
    Compiled(CompiledAddress),
    UnCompiled(usize), // index in builder.frontier
}

/// Expert: holds a pending (seen but not yet serialized) Node.
pub struct UnCompiledNode<F: OutputFactory> {
    owner: *const FstBuilder<F>,
    pub num_arcs: usize,
    pub arcs: Vec<BuilderArc<F>>,
    // TODO: instead of recording is_final/output on the
    // node, maybe we should use -1 arc to mean "end" (like
    // we do when reading the FST).  Would simplify much
    // code here...
    pub output: F::Value,
    pub is_final: bool,
    pub input_count: i64,
    // This node's depth, starting from the automaton root
    pub depth: i32,
}

impl<F: OutputFactory> UnCompiledNode<F> {
    pub fn new(owner: &FstBuilder<F>, depth: i32) -> Self {
        let output = owner.no_output.clone();
        UnCompiledNode {
            owner: owner as *const FstBuilder<F>,
            num_arcs: 0,
            arcs: Vec::with_capacity(1),
            output,
            is_final: false,
            input_count: 0,
            depth,
        }
    }

    fn builder(&self) -> &FstBuilder<F> {
        unsafe { &*self.owner }
    }

    pub fn clear(&mut self) {
        self.num_arcs = 0;
        self.is_final = false;
        self.output = self.builder().no_output.clone();
        self.input_count = 0;

        // We don't clear the depth here because it never changes
        // for nodes on the frontier (even when reused).
    }

    fn get_last_output(&self, label_to_match: i32) -> &F::Value {
        assert!(self.num_arcs > 0);
        assert_eq!(self.arcs[self.num_arcs - 1].label, label_to_match);
        &self.arcs[self.num_arcs - 1].output
    }

    fn set_last_output(&mut self, label_to_match: i32, new_output: F::Value) {
        assert!(self.num_arcs > 0);
        assert_eq!(self.arcs[self.num_arcs - 1].label, label_to_match);
        self.arcs[self.num_arcs - 1].output = new_output;
    }

    fn add_arc(&mut self, label: i32, target: Node) {
        assert!(label > 0);
        assert!(self.num_arcs == 0 || label > self.arcs[self.num_arcs - 1].label);
        let new_arc = BuilderArc {
            label,
            target,
            is_final: false,
            output: self.builder().no_output.clone(),
            next_final_output: self.builder().no_output.clone(),
        };
        if self.num_arcs == self.arcs.len() {
            self.arcs.push(new_arc);
        } else {
            self.arcs[self.num_arcs] = new_arc;
        }
        self.num_arcs += 1;
    }

    fn replace_last(
        &mut self,
        label_to_match: i32,
        target: Node,
        next_final_output: F::Value,
        is_final: bool,
    ) {
        assert!(self.num_arcs > 0);
        let arc = &mut self.arcs[self.num_arcs - 1];
        assert_eq!(arc.label, label_to_match);
        arc.target = target;
        arc.next_final_output = next_final_output;
        arc.is_final = is_final;
    }

    fn delete_last(&mut self, label: i32, _target: &Node) {
        assert!(self.num_arcs > 0);
        assert_eq!(self.arcs[self.num_arcs - 1].label, label);

        self.num_arcs -= 1;
    }

    fn prepend_output(&mut self, output_prefix: F::Value) {
        for i in 0..self.num_arcs {
            self.arcs[i].output = self
                .builder()
                .fst
                .outputs()
                .add(&output_prefix, &self.arcs[i].output);
        }
        if self.is_final {
            self.output = self
                .builder()
                .fst
                .outputs()
                .add(&output_prefix, &self.output);
        }
    }
}
