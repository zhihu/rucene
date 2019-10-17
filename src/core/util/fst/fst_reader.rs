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

use std::cmp::max;
use std::io;

use core::codec::{check_header, write_header};
use core::store::io::{ByteArrayDataOutput, DataInput, DataOutput};
use core::util::fst::bytes_store::{BytesStore, StoreBytesReader};
use core::util::fst::fst_builder::{FstBuilder, Node};
use core::util::fst::DirectionalBytesReader;
use core::util::fst::{BytesReader, Output, OutputFactory};
use error::{ErrorKind, Result};

const BIT_FINAL_ARC: u8 = 1;
const BIT_LAST_ARC: u8 = 1 << 1;
const BIT_TARGET_NEXT: u8 = 1 << 2;
const BIT_STOP_NODE: u8 = 1 << 3;
const BIT_ARC_HAS_OUTPUT: u8 = 1 << 4;
const BIT_ARC_HAS_FINAL_OUTPUT: u8 = 1 << 5;

/// Arcs are stored as fixed-size (per entry) array, so
/// that we can find an arc using binary search.  We do
/// this when number of arcs is > NUM_ARCS_ARRAY:
/// If set, the target node is delta coded vs current
/// position:
// const BIT_TARGET_DELTA: u8 = 1 << 6;

/// We use this as a marker (because this one flag is
/// illegal by itself ...):
const ARCS_AS_FIXED_ARRAY: u8 = BIT_ARC_HAS_FINAL_OUTPUT;

const FIXED_ARRAY_SHALLOW_DISTANCE: u8 = 3;
const FIXED_ARRAY_NUM_ARCS_SHALLOW: u8 = 5;
const FIXED_ARRAY_NUM_ARCS_DEEP: u8 = 10;
const FILE_FORMAT_NAME: &str = "FST";

const VERSION_PACKED: i32 = 3;
const VERSION_VINT_TARGET: i32 = 4;
const VERSION_NO_NODE_ARC_COUNTS: i32 = 5;
// LUCENE-7531, donot support pack fst anymore
const VERSION_PACKED_REMOVED: i32 = 6;
const VERSION_CURRENT: i32 = VERSION_PACKED_REMOVED;
const FINAL_END_NODE: CompiledAddress = -1;
const NON_FINAL_END_NODE: CompiledAddress = 0;

/// Only works on 64bit os
const DEFAULT_MAX_BLOCK_BITS: usize = 30;

pub const END_LABEL: Label = -1;

fn flag(flags: u8, bit: u8) -> bool {
    (flags & bit) != 0
}

type Label = i32;
pub type CompiledAddress = i64;

#[derive(Copy, Clone)]
pub enum InputType {
    Byte1,
    Byte2,
    Byte4,
}

#[derive(Default, Clone, Eq, PartialEq, Debug)]
pub struct Arc<T: Output> {
    pub flags: u8,
    pub label: Label,
    pub output: Option<T>,
    pub next_final_output: Option<T>,
    pub next_arc: Option<CompiledAddress>,
    /// To node
    pub target: CompiledAddress,
    /// Where the first arc in the array starts; only valid if bytesPerArc != 0.
    pub arc_start_position: usize,

    /// Non-zero if this arc is part of an array, which means all
    /// arcs for the node are encoded with a fixed number of bytes so
    /// that we can random access by index.  We do when there are enough
    /// arcs leaving one node.  It wastes some bytes but gives faster lookups.
    pub bytes_per_arc: usize,

    /// Where we are in the array; only valid if bytesPerArc != 0.
    pub arc_index: usize,

    /// How many arcs in the array; only valid if bytesPerArc != 0.
    pub num_arcs: usize,
}

impl<T: Output> Arc<T> {
    pub fn empty() -> Arc<T> {
        Arc {
            flags: 0u8,
            label: 0i32,
            output: None,
            next_final_output: None,
            next_arc: None,
            target: 0,
            arc_start_position: 0,
            bytes_per_arc: 0,
            arc_index: 0,
            num_arcs: 0,
        }
    }

    pub fn is_last(&self) -> bool {
        flag(self.flags, BIT_LAST_ARC)
    }

    pub fn is_final(&self) -> bool {
        flag(self.flags, BIT_FINAL_ARC)
    }

    #[allow(dead_code)]
    fn copy_from(&mut self, other: &Arc<T>) {
        self.flags = other.flags;
        self.label = other.label;
        self.output = other.output.clone();
        self.next_final_output = other.next_final_output.clone();
        self.next_arc = other.next_arc;
        self.target = other.target;
        self.bytes_per_arc = other.bytes_per_arc;
        if self.bytes_per_arc > 0 {
            self.arc_start_position = other.arc_start_position;
            self.arc_index = other.arc_index;
            self.num_arcs = other.num_arcs;
        }
    }
}

struct BytesRefFSTEnum<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> BytesRefFSTEnum<'a> {
    fn new(bytes: &'a [u8]) -> BytesRefFSTEnum {
        BytesRefFSTEnum { bytes, offset: 0 }
    }
}

impl<'a> Iterator for BytesRefFSTEnum<'a> {
    type Item = Label;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.bytes.len() {
            None
        //        } else if self.offset == self.bytes.len() {
        //            self.offset += 1;
        //            Some(END_LABEL)
        } else {
            let label = self.bytes[self.offset];
            self.offset += 1;
            Some(Label::from(label))
        }
    }
}

pub struct FST<F: OutputFactory> {
    pub input_type: InputType,
    // if non-none, this FST accepts the empty string and
    // produces this output
    pub empty_output: Option<F::Value>,
    // used during building, or during reading when
    // the FST is very large (more than 1 GB).  If the FST is less than 1
    // GB then bytesArray is set instead.
    pub bytes_store: BytesStore,
    // Used at read time when the FST fits into a single Vec<u8>.
    bytes_array: Vec<u8>,
    // flag of whether use bytes_store or bytes_array
    use_bytes_array: bool,
    start_node: CompiledAddress,
    version: i32,
    output_factory: F,
    cached_root_arcs: Vec<Option<Arc<F::Value>>>,
}

impl<F: OutputFactory> FST<F> {
    pub fn new(input_type: InputType, output_factory: F, bytes_page_bits: usize) -> Self {
        let mut bytes_store = BytesStore::with_block_bits(bytes_page_bits);
        let _ = bytes_store.write_byte(0);
        FST {
            input_type,
            empty_output: None,
            bytes_store,
            bytes_array: Vec::with_capacity(0),
            use_bytes_array: false,
            start_node: -1,
            version: VERSION_CURRENT,
            output_factory,
            cached_root_arcs: Vec::with_capacity(0),
        }
    }

    pub fn from_input<I: DataInput + ?Sized>(data_in: &mut I, output_factory: F) -> Result<Self> {
        let output_factory = output_factory;
        let max_block_bits = DEFAULT_MAX_BLOCK_BITS;

        // Only reads most recent format; we don't have
        // back-compat promise for FSTs (they are experimental):
        let version = check_header(data_in, FILE_FORMAT_NAME, VERSION_PACKED, VERSION_CURRENT)?;

        if version < VERSION_PACKED_REMOVED && data_in.read_byte()? == 1 {
            bail!(ErrorKind::CorruptIndex(
                "Cannot read packed FSTs anymore".into()
            ));
        }

        let empty_output = if data_in.read_byte()? == 1 {
            // Accepts empty string
            // 1KB blocks:
            let num_bytes = data_in.read_vint()? as usize;
            let bytes_store = BytesStore::new(data_in, num_bytes, 30)?;
            let mut reader = bytes_store.get_reverse_reader();
            if num_bytes > 0 {
                reader.set_position(num_bytes - 1);
            }

            Some(output_factory.read_final_output(&mut reader)?)
        } else {
            None
        };

        let input_type = match data_in.read_byte()? {
            0 => InputType::Byte1,
            1 => InputType::Byte2,
            2 => InputType::Byte4,
            x => bail!(ErrorKind::IllegalState(format!(
                "Invalid input type: {}",
                x
            ),)),
        };
        let start_node = data_in.read_vlong()? as CompiledAddress;
        if version < VERSION_NO_NODE_ARC_COUNTS {
            data_in.read_vlong()?;
            data_in.read_vlong()?;
            data_in.read_vlong()?;
        }

        let num_bytes = data_in.read_vlong()?;
        let bytes_store: BytesStore;
        let mut bytes_array: Vec<u8>;
        let use_bytes_array: bool;

        if num_bytes > (1 << max_block_bits as i64) {
            // FST is big: we need multiple pages
            bytes_store = BytesStore::new(data_in, num_bytes as usize, 1 << max_block_bits)?;
            bytes_array = Vec::with_capacity(0);
            use_bytes_array = false;
        } else {
            let len = num_bytes as usize;
            bytes_array = vec![0u8; len];
            data_in.read_exact(&mut bytes_array)?;
            // a dummy struct
            bytes_store = BytesStore::with_block_bits(8);
            use_bytes_array = true;
        };

        Ok(FST {
            input_type,
            start_node,
            version,
            output_factory,
            bytes_store,
            use_bytes_array,
            empty_output,
            bytes_array,
            cached_root_arcs: Vec::with_capacity(0),
        })
    }

    pub fn outputs(&self) -> &F {
        &self.output_factory
    }

    pub fn set_empty_output(&mut self, v: F::Value) {
        let new_output = if let Some(ref output) = self.empty_output {
            self.output_factory.merge(&output, &v)
        } else {
            v
        };
        self.empty_output = Some(new_output);
    }

    pub fn get(&self, bytes: &[u8]) -> Result<Option<F::Value>> {
        let mut arc = self.root_arc();
        let mut output = self.output_factory.empty();
        let mut bytes_reader = self.bytes_reader();
        let bytes_ref = BytesRefFSTEnum::new(bytes);

        for label in bytes_ref {
            let next_arc = self.find_target_arc(label, &arc, &mut bytes_reader)?;
            match next_arc {
                Some(a) => {
                    arc = a;
                    if let Some(ref out) = arc.output {
                        if !out.is_empty() {
                            output = output.cat(&out);
                        }
                    }
                }
                None => return Ok(None),
            }
        }

        if arc.is_final() {
            if let Some(ref out) = arc.next_final_output {
                if !out.is_empty() {
                    output = output.cat(&out);
                }
            }
            Ok(Some(output))
        } else {
            Ok(None)
        }
    }

    pub fn floor(&self, bytes: &[u8]) -> Result<(F::Value, usize)> {
        let mut arc = self.root_arc();
        let mut output = self.output_factory.empty();
        let mut bytes_reader = self.bytes_reader();
        let mut length = 0usize;
        let mut last_final_length = 0usize;

        let bytes_ref = BytesRefFSTEnum::new(bytes);
        if let Some(ref out) = arc.output {
            if !out.is_empty() {
                output.concat(&out);
            }
        }
        let mut last_final_output = if arc.is_final() && arc.next_final_output.is_some() {
            Some(output.cat(arc.next_final_output.as_ref().unwrap()))
        } else {
            None
        };

        for label in bytes_ref {
            let next_arc = self.find_target_arc(label, &arc, &mut bytes_reader)?;
            match next_arc {
                Some(a) => {
                    if label != END_LABEL {
                        length += 1;
                    }
                    arc = a;
                    if let Some(ref out) = arc.output {
                        if !out.is_empty() {
                            output.concat(&out);
                        }
                    }
                    if arc.is_final() {
                        last_final_output = Some(match arc.next_final_output {
                            Some(ref out) => output.cat(out),
                            None => output.clone(),
                        });
                        last_final_length = length;
                    }
                }
                None => break,
            }
        }

        Ok((
            match last_final_output {
                Some(out) => out,
                None => output,
            },
            last_final_length,
        ))
    }

    pub fn bytes_reader(&self) -> FSTBytesReader {
        if self.use_bytes_array {
            FSTBytesReader::Directional(DirectionalBytesReader::new(&self.bytes_array, true))
        } else {
            FSTBytesReader::BytesStore(self.bytes_store.get_reverse_reader())
        }
    }

    pub fn root_arc(&self) -> Arc<F::Value> {
        let mut arc = Arc::empty();

        if let Some(ref default_output) = self.empty_output {
            arc.flags = BIT_FINAL_ARC | BIT_LAST_ARC;
            arc.next_final_output = Some(default_output.clone());
            if !default_output.is_empty() {
                arc.flags |= BIT_ARC_HAS_FINAL_OUTPUT;
            }
        } else {
            arc.flags = BIT_LAST_ARC;
            arc.next_final_output = Some(self.output_factory.empty());
        }
        arc.output = Some(self.output_factory.empty());

        // If there are no nodes, ie, the FST only accepts the
        // empty string, then startNode is 0.
        arc.target = self.start_node;

        arc
    }

    pub fn find_target_arc(
        &self,
        label: Label,
        incoming_arc: &Arc<F::Value>,
        bytes_reader: &mut dyn BytesReader,
    ) -> Result<Option<Arc<F::Value>>> {
        self.find_target_arc_with_cache(label, &incoming_arc, bytes_reader, true)
    }

    pub fn find_target_arc_with_cache(
        &self,
        label: Label,
        incoming_arc: &Arc<F::Value>,
        bytes_reader: &mut dyn BytesReader,
        use_root_arc_cache: bool,
    ) -> Result<Option<Arc<F::Value>>> {
        if label == END_LABEL {
            if incoming_arc.is_final() {
                let mut target_arc = incoming_arc.clone();
                if !self.target_has_arc(incoming_arc.target) {
                    target_arc.flags = BIT_LAST_ARC;
                } else {
                    // NOTE!!:
                    target_arc.flags = 0u8;
                    // next_arc is a node (not an address!) in this case:
                    target_arc.next_arc = Some(incoming_arc.target);
                }
                target_arc.output = incoming_arc.next_final_output.clone();
                target_arc.label = END_LABEL;
                return Ok(Some(target_arc));
            } else {
                return Ok(None);
            }
        }

        if use_root_arc_cache
            && !self.cached_root_arcs.is_empty()
            && incoming_arc.target == self.start_node
            && label < self.cached_root_arcs.len() as i32
        {
            let result = self.cached_root_arcs[label as usize].clone();
            debug_assert!(self.assert_root_cached_arc(label, &result)?);
            return Ok(result);
        }

        if !self.target_has_arc(incoming_arc.target) {
            return Ok(None);
        }

        bytes_reader.set_position(incoming_arc.target as usize);

        let mut arc = Arc::empty();
        if bytes_reader.read_byte()? == ARCS_AS_FIXED_ARRAY {
            // Arcs are full array, do binary search.

            arc.num_arcs = bytes_reader.read_vint()? as usize;
            arc.bytes_per_arc = if self.version >= VERSION_VINT_TARGET {
                bytes_reader.read_vint()? as usize
            } else {
                bytes_reader.read_int()? as usize
            };
            arc.arc_start_position = bytes_reader.position();
            let mut low = 0usize;
            let mut high = arc.num_arcs - 1;
            while low <= high {
                let mid = (low + high) >> 1;
                bytes_reader.set_position(arc.arc_start_position);
                bytes_reader.skip_bytes(arc.bytes_per_arc * mid + 1)?;
                let current_label = self.read_label(bytes_reader)?;
                let cmp = current_label - label;
                if cmp < 0 {
                    low = mid + 1;
                } else if cmp > 0 {
                    if mid == 0 {
                        break;
                    }
                    high = mid - 1;
                } else {
                    arc.arc_index = mid;
                    self.read_next_real_arc(&mut arc, bytes_reader)?;
                    return Ok(Some(arc));
                }
            }
            return Ok(None);
        }

        // Do linear scan
        let mut arc = self.read_first_real_arc(incoming_arc.target, bytes_reader)?;

        loop {
            if arc.label == label {
                return Ok(Some(arc));
            } else if arc.label > label || arc.is_last() {
                return Ok(None);
            } else {
                self.read_next_real_arc(&mut arc, bytes_reader)?;
            }
        }
    }

    // LUCENE-5152: called only from asserts, to validate that the
    // non-cached arc lookup would produce the same result, to
    // catch callers that illegally modify shared structures with
    // the result (we shallow-clone the Arc itself, but e.g. a BytesRef
    // output is still shared):
    fn assert_root_cached_arc(&self, label: Label, arc: &Option<Arc<F::Value>>) -> Result<bool> {
        let root = self.root_arc();
        let mut input = self.bytes_reader();
        let result = self.find_target_arc(label, &root, &mut input)?;
        if let Some(ref res) = result {
            if let Some(arc) = arc {
                assert_eq!(res, arc);
            } else {
                panic!();
            }
        } else {
            debug_assert!(arc.is_none());
        }
        Ok(true)
    }

    fn target_has_arc(&self, target: CompiledAddress) -> bool {
        target > 0
    }

    fn read_label(&self, reader: &mut dyn BytesReader) -> Result<Label> {
        match self.input_type {
            InputType::Byte1 => reader.read_byte().map(Label::from),
            InputType::Byte2 => reader.read_short().map(Label::from),
            InputType::Byte4 => reader.read_vint(),
        }
    }

    pub fn read_first_real_arc(
        &self,
        node: CompiledAddress,
        bytes_reader: &mut dyn BytesReader,
    ) -> Result<Arc<F::Value>> {
        bytes_reader.set_position(node as usize);

        let mut arc = Arc::empty();
        if bytes_reader.read_byte()? == ARCS_AS_FIXED_ARRAY {
            arc.num_arcs = bytes_reader.read_vint()? as usize;
            arc.bytes_per_arc = if self.version >= VERSION_VINT_TARGET {
                bytes_reader.read_vint()? as usize
            } else {
                bytes_reader.read_int()? as usize
            };
            arc.arc_start_position = bytes_reader.position();
            arc.arc_index = 0;
        } else {
            arc.next_arc = Some(node);
        }
        self.read_next_real_arc(&mut arc, bytes_reader)?;
        Ok(arc)
    }

    pub fn read_first_target_arc(
        &self,
        follow: &Arc<F::Value>,
        input: &mut dyn BytesReader,
    ) -> Result<Arc<F::Value>> {
        if follow.is_final() {
            let mut arc = Arc::empty();
            arc.flags = BIT_FINAL_ARC;
            arc.label = END_LABEL;
            arc.target = FINAL_END_NODE;
            arc.output = follow.next_final_output.clone();
            if !self.target_has_arc(follow.target) {
                arc.flags |= BIT_LAST_ARC;
            } else {
                arc.next_arc = Some(follow.target);
            };
            Ok(arc)
        } else {
            self.read_first_real_arc(follow.target, input)
        }
    }

    pub fn read_next_arc(
        &self,
        arc: &mut Arc<F::Value>,
        bytes_reader: &mut dyn BytesReader,
    ) -> Result<()> {
        if arc.label == END_LABEL {
            // This was a fake inserted "final" arc
            if arc.next_arc.unwrap() <= 0 {
                bail!(ErrorKind::IllegalArgument(
                    "cannot read_next_arc when arc.is_last()".into()
                ));
            }
            let new_arc = self.read_first_real_arc(arc.next_arc.unwrap(), bytes_reader)?;
            arc.copy_from(&new_arc);
        } else {
            self.read_next_real_arc(arc, bytes_reader)?;
        }
        Ok(())
    }

    pub fn read_next_real_arc(
        &self,
        arc: &mut Arc<F::Value>,
        bytes_reader: &mut dyn BytesReader,
    ) -> Result<()> {
        if arc.bytes_per_arc > 0 {
            debug_assert!(arc.arc_index < arc.num_arcs);
            bytes_reader.set_position(arc.arc_start_position);
            bytes_reader.skip_bytes(arc.arc_index * arc.bytes_per_arc)?;
            arc.arc_index += 1;
        } else {
            assert!(arc.next_arc.is_some());
            bytes_reader.set_position(arc.next_arc.unwrap() as usize);
        }

        arc.flags = bytes_reader.read_byte()?;
        arc.label = self.read_label(bytes_reader)?;
        arc.output = if flag(arc.flags, BIT_ARC_HAS_OUTPUT) {
            Some(self.output_factory.read(bytes_reader)?)
        } else {
            None
        };
        arc.next_final_output = if flag(arc.flags, BIT_ARC_HAS_FINAL_OUTPUT) {
            Some(self.output_factory.read_final_output(bytes_reader)?)
        } else {
            None
        };
        if flag(arc.flags, BIT_STOP_NODE) {
            arc.target = FINAL_END_NODE;
            arc.next_arc = Some(bytes_reader.position() as i64);
        } else if flag(arc.flags, BIT_TARGET_NEXT) {
            arc.next_arc = Some(bytes_reader.position() as i64);
            if !flag(arc.flags, BIT_LAST_ARC) {
                if arc.bytes_per_arc > 0 {
                    bytes_reader.set_position(arc.arc_start_position);
                    bytes_reader.skip_bytes(arc.bytes_per_arc * arc.num_arcs)?;
                } else {
                    self.seek_to_next_node(bytes_reader)?;
                }
            }
            arc.target = bytes_reader.position() as CompiledAddress;
        } else {
            arc.target = self.read_unpacked_node(bytes_reader)?;

            arc.next_arc = Some(bytes_reader.position() as i64);
        }
        Ok(())
    }

    fn seek_to_next_node(&self, bytes_reader: &mut dyn BytesReader) -> Result<()> {
        loop {
            let flags = bytes_reader.read_byte()?;
            self.read_label(bytes_reader)?;

            if flag(flags, BIT_ARC_HAS_OUTPUT) {
                self.output_factory.skip_output(bytes_reader)?;
            }
            if flag(flags, BIT_ARC_HAS_FINAL_OUTPUT) {
                self.output_factory.skip_final_output(bytes_reader)?;
            }
            if !flag(flags, BIT_STOP_NODE) && !flag(flags, BIT_TARGET_NEXT) {
                self.read_unpacked_node(bytes_reader)?;
            }

            if flag(flags, BIT_LAST_ARC) {
                return Ok(());
            }
        }
    }

    fn read_unpacked_node(&self, bytes_reader: &mut dyn BytesReader) -> Result<CompiledAddress> {
        if self.version < VERSION_VINT_TARGET {
            bytes_reader.read_int().map(|x| x as CompiledAddress)
        } else {
            bytes_reader.read_vlong().map(|x| x as CompiledAddress)
        }
    }

    // implements for build

    // serializes new node by appending its bytes to the end
    // of the current bytes
    pub fn add_node(
        &mut self,
        builder: &mut FstBuilder<F>,
        node_index: usize,
    ) -> Result<CompiledAddress> {
        let no_output = self.output_factory.empty();

        if builder.frontier[node_index].num_arcs == 0 {
            if builder.frontier[node_index].is_final {
                return Ok(FINAL_END_NODE as i64);
            } else {
                return Ok(NON_FINAL_END_NODE as i64);
            }
        }
        let start_address = self.bytes_store.get_position();

        let do_fixed_array = self.should_expand(builder, node_index);
        if do_fixed_array
            && builder.reused_bytes_per_arc.len() < builder.frontier[node_index].num_arcs
        {
            builder
                .reused_bytes_per_arc
                .resize(builder.frontier[node_index].num_arcs, 0);
        }
        builder.arc_count += builder.frontier[node_index].num_arcs as u64;

        let last_arc = builder.frontier[node_index].num_arcs - 1;
        let mut last_arc_start = self.bytes_store.get_position();
        let mut max_bytes_per_arc = 0;
        for idx in 0..builder.frontier[node_index].num_arcs {
            let arc = &builder.frontier[node_index].arcs[idx];

            let target = match arc.target {
                Node::Compiled(c) => c,
                Node::UnCompiled(_) => unreachable!(),
            };
            let mut flags = 0;
            if idx == last_arc {
                flags += BIT_LAST_ARC;
            }
            if builder.last_frozen_node == target && !do_fixed_array {
                // TODO: for better perf(but more RAM used) we could avoid this except when
                // arc is "near" the last arc:
                flags += BIT_TARGET_NEXT;
            }

            if arc.is_final {
                flags += BIT_FINAL_ARC;
                if arc.next_final_output != no_output {
                    flags += BIT_ARC_HAS_FINAL_OUTPUT;
                }
            } else {
                assert_eq!(arc.next_final_output, no_output);
            }

            let target_has_arcs = target > 0;
            if !target_has_arcs {
                flags += BIT_STOP_NODE;
            }

            if arc.output != no_output {
                flags += BIT_ARC_HAS_OUTPUT;
            }

            self.bytes_store.write_byte(flags)?;
            self.write_label_local(arc.label)?;

            if arc.output != no_output {
                self.output_factory
                    .write(&arc.output, &mut self.bytes_store)?;
            }

            if arc.next_final_output != no_output {
                self.output_factory
                    .write(&arc.next_final_output, &mut self.bytes_store)?;
            }

            if target_has_arcs && (flags & BIT_TARGET_NEXT) == 0 {
                assert!(target > 0);
                self.bytes_store.write_vlong(target)?;
            }

            // just write the arcs "like normal" on first pass,
            // but record how many bytes each one took, and max
            // byte size:
            if do_fixed_array {
                let length = self.bytes_store.get_position() - last_arc_start;
                builder.reused_bytes_per_arc[idx] = length;
                last_arc_start = self.bytes_store.get_position();
                max_bytes_per_arc = max(max_bytes_per_arc, length);
            }
        }

        if do_fixed_array {
            let max_header_size = 11; // header(byte) + numArcs(vint) + numBytes(vint)
            assert!(max_bytes_per_arc > 0);
            // 2nd pass just "expands" all arcs to take up a fixed
            // byte size

            // print!("write int @pos=" + (fixedArrayStart-4) + " numArcs=" + nodeIn.numArcs);
            // create the header
            // TODO: clean this up: or just rewind+reuse and deal with it
            let mut header = vec![0u8; max_header_size];
            let len = header.len();
            let fixed_array_start: usize;
            let header_len: usize;
            {
                let mut bad = ByteArrayDataOutput::new(&mut header, 0, len);
                // write a "false" first arc
                bad.write_byte(ARCS_AS_FIXED_ARRAY)?;
                bad.write_vint(builder.frontier[node_index].num_arcs as i32)?;
                bad.write_vint(max_bytes_per_arc as i32)?;
                header_len = bad.pos;
                fixed_array_start = start_address + header_len;
            }

            // expand the arcs in place, backwards
            let mut src_pos = self.bytes_store.get_position();
            let mut dest_pos =
                fixed_array_start + builder.frontier[node_index].num_arcs * max_bytes_per_arc;
            assert!(dest_pos >= src_pos);
            if dest_pos > src_pos {
                self.bytes_store.skip_bytes(dest_pos - src_pos);
                for i in 0..builder.frontier[node_index].num_arcs {
                    let arc_idx = builder.frontier[node_index].num_arcs - 1 - i;
                    dest_pos -= max_bytes_per_arc;
                    src_pos -= builder.reused_bytes_per_arc[arc_idx];
                    if src_pos != dest_pos {
                        assert!(dest_pos > src_pos);
                        self.bytes_store.copy_bytes_local(
                            src_pos,
                            dest_pos,
                            builder.reused_bytes_per_arc[arc_idx],
                        );
                    }
                }
            }

            // now write the header
            self.bytes_store
                .write_bytes_local(start_address, &header, 0, header_len);
        }

        let this_node_address = self.bytes_store.get_position() - 1;
        self.bytes_store.reverse(start_address, this_node_address);

        builder.node_count += 1;
        Ok(this_node_address as CompiledAddress)
    }

    #[allow(dead_code)]
    fn write_label(&self, out: &mut impl DataOutput, v: i32) -> Result<()> {
        assert!(v > 0);
        match self.input_type {
            InputType::Byte1 => {
                assert!(v <= 255);
                out.write_byte(v as u8)
            }
            InputType::Byte2 => {
                assert!(v <= 65535);
                out.write_short(v as i16)
            }
            InputType::Byte4 => out.write_vint(v),
        }
    }

    fn write_label_local(&mut self, v: i32) -> Result<()> {
        assert!(v > 0);
        match self.input_type {
            InputType::Byte1 => {
                assert!(v <= 255);
                self.bytes_store.write_byte(v as u8)
            }
            InputType::Byte2 => {
                assert!(v <= 65535);
                self.bytes_store.write_short(v as i16)
            }
            InputType::Byte4 => self.bytes_store.write_vint(v),
        }
    }

    /// Nodes will be expanded if their depth (distance from the root node) is
    /// <= this value and their number of arcs is >=
    /// `FIXED_ARRAY_NUM_ARCS_SHALLOW`.
    ///
    /// Fixed array consumes more RAM but enables binary search on the arcs
    /// (instead of a linear scan) on lookup by arc label.
    ///
    /// @return <code>true</code> if <code>node</code> should be stored in an
    ///         expanded (array) form.
    fn should_expand(&self, builder: &FstBuilder<F>, idx: usize) -> bool {
        let node = &builder.frontier[idx];
        builder.allow_array_arcs
            && ((node.depth <= FIXED_ARRAY_SHALLOW_DISTANCE as i32
                && node.num_arcs >= FIXED_ARRAY_NUM_ARCS_SHALLOW as usize)
                || node.num_arcs >= FIXED_ARRAY_NUM_ARCS_DEEP as usize)
    }

    pub fn finish(&mut self, new_start_node: CompiledAddress) -> Result<()> {
        assert!(new_start_node <= self.bytes_store.get_position() as i64);
        if self.start_node != -1 {
            bail!(ErrorKind::IllegalState("already finished".into()));
        }
        let new_start_node = if new_start_node == FINAL_END_NODE {
            0
        } else {
            new_start_node
        };
        self.start_node = if new_start_node == FINAL_END_NODE && self.empty_output.is_some() {
            0
        } else {
            new_start_node
        };
        self.bytes_store.finish();
        self.cache_root_arcs()
    }

    // optional caches first 128 labels
    fn cache_root_arcs(&mut self) -> Result<()> {
        // we should only be called once per fst
        let root = self.root_arc();
        let parent = root.target;
        if self.target_has_arc(root.target) {
            let mut count = 0;
            let mut arcs = vec![None; 128];
            {
                let mut input = self.bytes_reader();
                let mut arc = self.read_first_real_arc(parent, &mut input)?;

                loop {
                    assert_ne!(arc.label, END_LABEL);
                    let is_last = arc.is_last();
                    if arc.label < arcs.len() as i32 {
                        let idx = arc.label as usize;
                        let mut new_arc = Arc::empty();
                        new_arc.copy_from(&arc);
                        arcs[idx] = Some(new_arc);
                    } else {
                        break;
                    }
                    if is_last {
                        break;
                    }
                    self.read_next_real_arc(&mut arc, &mut input)?;
                    count += 1;
                }
            }

            if count >= FIXED_ARRAY_NUM_ARCS_SHALLOW {
                self.cached_root_arcs = arcs;
            }
        }
        Ok(())
    }

    pub fn save(&self, out: &mut impl DataOutput) -> Result<()> {
        if self.start_node == -1 {
            bail!(ErrorKind::IllegalState("call finish first!".into()));
        }
        write_header(out, FILE_FORMAT_NAME, VERSION_CURRENT)?;
        if VERSION_CURRENT < VERSION_PACKED_REMOVED {
            out.write_byte(0)?;
        }
        // TODO: really we should encode this as an arc, arriving
        // to the root node, instead of special casing here:
        if let Some(ref empty_output) = self.empty_output {
            // Accepts empty string
            out.write_byte(1)?;

            // Serialize empty-string output
            let mut empty_output_bytes: Vec<u8> = Vec::new();
            self.output_factory
                .write_final_output(empty_output, &mut empty_output_bytes)?;

            // reverse
            empty_output_bytes.reverse();
            out.write_vint(empty_output_bytes.len() as i32)?;
            out.write_bytes(&empty_output_bytes, 0, empty_output_bytes.len())?;
        } else {
            out.write_byte(0)?;
        }
        let t = match self.input_type {
            InputType::Byte1 => 0,
            InputType::Byte2 => 1,
            InputType::Byte4 => 2,
        };
        out.write_byte(t)?;
        out.write_vlong(self.start_node)?;
        if self.bytes_store.get_position() > 0 {
            debug_assert!(!self.use_bytes_array);
            let num_bytes = self.bytes_store.get_position();
            out.write_vlong(num_bytes as i64)?;
            self.bytes_store.write_to(out)?;
        } else {
            debug_assert!(self.use_bytes_array);
            debug_assert!(!self.bytes_array.is_empty());
            out.write_vlong(self.bytes_array.len() as i64)?;
            out.write_bytes(&self.bytes_array, 0, self.bytes_array.len())?;
        }
        Ok(())
    }
}

// this should only be used for place holder to avoid Option
impl<F: OutputFactory + Default> Default for FST<F> {
    fn default() -> Self {
        FST {
            input_type: InputType::Byte1,
            empty_output: None,
            bytes_store: BytesStore::with_block_bits(1),
            bytes_array: Vec::with_capacity(0),
            use_bytes_array: true,
            start_node: 0,
            version: VERSION_CURRENT,
            output_factory: F::default(),
            cached_root_arcs: Vec::with_capacity(0),
        }
    }
}

pub enum FSTBytesReader {
    Directional(DirectionalBytesReader),
    BytesStore(StoreBytesReader),
}

impl BytesReader for FSTBytesReader {
    fn position(&self) -> usize {
        match *self {
            FSTBytesReader::Directional(ref d) => d.position(),
            FSTBytesReader::BytesStore(ref b) => b.position(),
        }
    }

    fn set_position(&mut self, pos: usize) {
        match *self {
            FSTBytesReader::Directional(ref mut d) => d.set_position(pos),
            FSTBytesReader::BytesStore(ref mut b) => b.set_position(pos),
        }
    }

    fn reversed(&self) -> bool {
        match *self {
            FSTBytesReader::Directional(ref d) => d.reversed(),
            FSTBytesReader::BytesStore(ref b) => b.reversed(),
        }
    }
}

impl io::Read for FSTBytesReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match *self {
            FSTBytesReader::Directional(ref mut d) => d.read(buf),
            FSTBytesReader::BytesStore(ref mut b) => b.read(buf),
        }
    }
}

impl DataInput for FSTBytesReader {
    fn read_byte(&mut self) -> Result<u8> {
        match *self {
            FSTBytesReader::Directional(ref mut d) => d.read_byte(),
            FSTBytesReader::BytesStore(ref mut b) => b.read_byte(),
        }
    }

    fn read_bytes(&mut self, b: &mut [u8], offset: usize, length: usize) -> Result<()> {
        match *self {
            FSTBytesReader::Directional(ref mut d) => d.read_bytes(b, offset, length),
            FSTBytesReader::BytesStore(ref mut r) => r.read_bytes(b, offset, length),
        }
    }

    fn skip_bytes(&mut self, count: usize) -> Result<()> {
        match *self {
            FSTBytesReader::Directional(ref mut d) => d.skip_bytes(count),
            FSTBytesReader::BytesStore(ref mut b) => b.skip_bytes(count),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use core::util::fst::bytes_output::*;
    use core::util::ints_ref::IntsRefBuilder;

    #[test]
    fn test_fst() {
        let mut builder = FstBuilder::new(InputType::Byte1, ByteSequenceOutputFactory {});
        builder.init();
        let input_values = vec!["cat", "dag", "dbg", "dcg", "ddg", "deg", "dog", "dogs"];
        let output_values = vec![5, 7, 12, 13, 14, 15, 16, 17];

        let mut ints_ref_builder = IntsRefBuilder::new();
        for i in 0..input_values.len() {
            ints_ref_builder.clear();
            for j in input_values[i].as_bytes() {
                ints_ref_builder.append(*j as i32);
            }
            let output = ByteSequenceOutput::new(vec![output_values[i] as u8]);
            let res = builder.add(ints_ref_builder.get(), output);
            assert!(res.is_ok());
        }

        let fst: FST<ByteSequenceOutputFactory> = builder.finish().unwrap().unwrap();

        // test get
        for i in 0..input_values.len() {
            let res = fst.get(input_values[i].as_bytes());
            if let Ok(Some(value)) = res {
                let output = ByteSequenceOutput::new(vec![output_values[i] as u8]);
                assert_eq!(value, output);
            } else {
                assert!(false);
            }
        }
    }
}
