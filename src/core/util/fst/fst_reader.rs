use std::cmp::{max, min};
use std::cmp::{Ord, Ordering, PartialOrd};
use std::collections::{BinaryHeap, HashMap};
use std::io;

use core::codec::codec_util;
use core::store::{ByteArrayDataOutput, DataInput, DataOutput};
use core::util::fst::bytes_store::{BytesStore, StoreBytesReader};
use core::util::fst::fst_builder::{FstBuilder, Node, UnCompiledNode};
use core::util::fst::DirectionalBytesReader;
use core::util::fst::{BytesReader, Output, OutputFactory};
use core::util::packed_misc::{get_mutable_by_ratio_as_reader, unsigned_bits_required};
use core::util::packed_misc::{get_reader, GrowableWriter, Mutable, Reader};
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
///
const BIT_TARGET_DELTA: u8 = 1 << 6;

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
const VERSION_CURRENT: i32 = VERSION_NO_NODE_ARC_COUNTS;
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

#[derive(Default, Eq, PartialEq, Debug)]
pub struct Arc<T: Output> {
    pub flags: u8,
    pub label: Label,
    pub output: Option<T>,
    pub next_final_output: Option<T>,
    pub next_arc: Option<CompiledAddress>,

    /// From node, currently only used when
    // building an FST w/ packed=true
    pub from: Option<CompiledAddress>,

    /// To node
    pub target: Option<CompiledAddress>,
}

#[derive(Debug)]
pub enum ArcLayoutContext {
    FixedArray {
        /// Where the first arc in the array starts; only valid if bytesPerArc != 0.
        arc_start_position: usize,

        /// Non-zero if this arc is part of an array, which means all
        /// arcs for the node are encoded with a fixed number of bytes so
        /// that we can random access by index.  We do when there are enough
        /// arcs leaving one node.  It wastes some bytes but gives faster lookups.
        bytes_per_arc: usize,

        /// Where we are in the array; only valid if bytesPerArc != 0.
        arc_index: usize,

        /// How many arcs in the array; only valid if bytesPerArc != 0.
        num_arcs: usize,
    },
    Linear(usize),
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
            Some(Label::from(label as i8))
        }
    }
}

impl<T: Output> Arc<T> {
    fn empty() -> Arc<T> {
        Arc {
            flags: 0u8,
            label: 0i32,
            output: None,
            next_final_output: None,
            next_arc: None,
            from: None,
            target: None,
        }
    }

    pub fn is_last(&self) -> bool {
        flag(self.flags, BIT_LAST_ARC)
    }

    pub fn is_final(&self) -> bool {
        flag(self.flags, BIT_FINAL_ARC)
    }

    #[allow(dead_code)]
    fn copy_from(mut self, other: &Arc<T>) {
        self.flags = other.flags;
        self.label = other.label;
        self.output = other.output.clone();
        self.next_final_output = other.next_final_output.clone();
        self.next_arc = other.next_arc.clone();
        self.from = other.from.clone();
        self.target = other.target;
    }
}

impl<T: Output> Clone for Arc<T> {
    fn clone(&self) -> Self {
        Arc {
            flags: self.flags,
            label: self.label,
            output: self.output.clone(),
            next_final_output: self.next_final_output.clone(),
            next_arc: self.next_arc,
            from: self.from,
            target: self.target,
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
    // Used at read time when the FST fits into a single byte[].
    bytes_array: Vec<u8>,
    // flag of whether use bytes_store or bytes_array
    use_bytes_array: bool,
    start_node: CompiledAddress,
    packed: bool,
    version: i32,
    output_factory: F,
    node_ref_to_address: Option<Box<Reader>>,
    cached_root_arcs: Vec<Option<Arc<F::Value>>>,
    node_address_lookup: Option<GrowableWriter>,
    in_counts: Option<GrowableWriter>,
}

impl<F: OutputFactory> FST<F> {
    pub fn new(
        input_type: InputType,
        output_factory: F,
        will_pack_fst: bool,
        acceptable_overhead_ratio: f32,
        bytes_page_bits: usize,
    ) -> Self {
        let mut bytes_store = BytesStore::with_block_bits(bytes_page_bits);
        let _ = bytes_store.write_byte(0);
        let (node_address_lookup, in_counts) = if will_pack_fst {
            (
                Some(GrowableWriter::new(15, 8, acceptable_overhead_ratio)),
                Some(GrowableWriter::new(1, 8, acceptable_overhead_ratio)),
            )
        } else {
            (None, None)
        };
        FST {
            input_type,
            empty_output: None,
            bytes_store,
            bytes_array: Vec::with_capacity(0),
            use_bytes_array: false,
            start_node: -1,
            packed: false,
            version: VERSION_CURRENT,
            output_factory,
            node_ref_to_address: None,
            node_address_lookup,
            in_counts,
            cached_root_arcs: Vec::with_capacity(0),
        }
    }

    pub fn from_input<I: DataInput + ?Sized>(data_in: &mut I, output_factory: F) -> Result<Self> {
        let output_factory = output_factory;
        let max_block_bits = DEFAULT_MAX_BLOCK_BITS;

        // Only reads most recent format; we don't have
        // back-compat promise for FSTs (they are experimental):
        let version = codec_util::check_header(
            data_in,
            FILE_FORMAT_NAME,
            VERSION_PACKED,
            VERSION_NO_NODE_ARC_COUNTS,
        )?;
        let packed = data_in.read_byte()? == 1;
        let empty_output = if data_in.read_byte()? == 1 {
            // Accepts empty string
            // 1KB blocks:
            let num_bytes = data_in.read_vint()? as usize;
            let bytes_store = BytesStore::new(data_in, num_bytes, 30)?;
            let mut reader = if packed {
                bytes_store.get_forward_reader()
            } else {
                let mut r = bytes_store.get_reverse_reader();
                if num_bytes > 0 {
                    r.set_position(num_bytes - 1);
                }
                r
            };

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
        let node_ref_to_addr = if packed {
            Some(get_reader(data_in)?)
        } else {
            None
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
            bytes_array = vec![0u8; num_bytes as usize];
            let len = bytes_array.len();
            data_in.read_bytes(&mut bytes_array, 0, len)?;
            // a dummy struct
            bytes_store = BytesStore::with_block_bits(8);
            use_bytes_array = true;
        };

        Ok(FST {
            input_type,
            start_node,
            packed,
            version,
            output_factory,
            bytes_store,
            use_bytes_array,
            node_ref_to_address: node_ref_to_addr,
            node_address_lookup: None,
            empty_output,
            in_counts: None,
            bytes_array,
            cached_root_arcs: Vec::with_capacity(0),
        })
    }

    pub fn new_packed(input_type: InputType, output_factory: F, bytes_page_bits: usize) -> Self {
        FST {
            input_type,
            empty_output: None,
            bytes_store: BytesStore::with_block_bits(bytes_page_bits),
            bytes_array: Vec::with_capacity(0),
            use_bytes_array: false,
            start_node: 0,
            packed: true,
            version: VERSION_CURRENT,
            output_factory,
            node_ref_to_address: None,
            node_address_lookup: None,
            in_counts: None,
            cached_root_arcs: Vec::with_capacity(0),
        }
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

    fn bytes_reader(&self) -> FSTBytesReader {
        if self.packed {
            if self.use_bytes_array {
                FSTBytesReader::Directional(DirectionalBytesReader::new(&self.bytes_array, false))
            } else {
                FSTBytesReader::BytesStore(self.bytes_store.get_forward_reader())
            }
        } else {
            if self.use_bytes_array {
                FSTBytesReader::Directional(DirectionalBytesReader::new(&self.bytes_array, false))
            } else {
                FSTBytesReader::BytesStore(self.bytes_store.get_reverse_reader())
            }
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
        arc.target = Some(self.start_node);

        arc
    }

    pub fn find_target_arc(
        &self,
        label: Label,
        incoming_arc: &Arc<F::Value>,
        bytes_reader: &mut BytesReader,
    ) -> Result<Option<Arc<F::Value>>> {
        self.find_target_arc_with_cache(label, &incoming_arc, bytes_reader, true)
    }

    pub fn find_target_arc_with_cache(
        &self,
        label: Label,
        incoming_arc: &Arc<F::Value>,
        bytes_reader: &mut BytesReader,
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
                    target_arc.next_arc = incoming_arc.target;
                    target_arc.from = incoming_arc.target;
                }
                target_arc.output = incoming_arc.next_final_output.clone();
                target_arc.label = END_LABEL;
                return Ok(Some(target_arc));
            } else {
                return Ok(None);
            }
        }

        if use_root_arc_cache && self.cached_root_arcs.len() > 0
            && incoming_arc.target.unwrap() == self.start_node
            && label < self.cached_root_arcs.len() as i32
        {
            let result = self.cached_root_arcs[label as usize].clone();
            debug_assert!(self.assert_root_cached_arc(label, &result)?);
            return Ok(result);
        }

        if !self.target_has_arc(incoming_arc.target) {
            return Ok(None);
        }

        if let Some(target) = incoming_arc.target {
            bytes_reader.set_position(self.real_node_address(target) as usize);
        } else {
            debug_assert!(false, "real node address must be an offset");
        }

        if bytes_reader.read_byte()? == ARCS_AS_FIXED_ARRAY {
            // Arcs are full array, do binary search.

            let num_arcs = bytes_reader.read_vint()? as usize;
            let bytes_per_arc = if self.packed || self.version >= VERSION_VINT_TARGET {
                bytes_reader.read_vint()? as usize
            } else {
                bytes_reader.read_int()? as usize
            };
            let arc_start_position = bytes_reader.position();
            let mut low = 0usize;
            let mut high = num_arcs - 1;
            while low <= high {
                let mid = (low + high) >> 1;
                bytes_reader.set_position(arc_start_position);
                bytes_reader.skip_bytes(bytes_per_arc * mid + 1)?;
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
                    let mut arc_context = ArcLayoutContext::FixedArray {
                        arc_start_position,
                        bytes_per_arc,
                        arc_index: mid,
                        num_arcs,
                    };
                    return self.read_next_real_arc(
                        &mut arc_context,
                        bytes_reader,
                        incoming_arc.next_arc.unwrap(),
                    ).map(Some);
                }
            }
            return Ok(None);
        }

        // Do linear scan
        let mut arc = self.read_first_real_arc(incoming_arc.target.unwrap(), bytes_reader)?;

        loop {
            if arc.label == label {
                return Ok(Some(arc));
            } else if arc.label > label || arc.is_last() {
                return Ok(None);
            } else {
                let mut context = ArcLayoutContext::Linear(arc.next_arc.unwrap() as usize);
                arc = self.read_next_real_arc(
                    &mut context,
                    bytes_reader,
                    incoming_arc.target.unwrap(),
                )?;
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
                assert!(false);
            }
        } else {
            debug_assert!(arc.is_none());
        }
        Ok(true)
    }

    fn target_has_arc(&self, target: Option<CompiledAddress>) -> bool {
        target.map(|v| v > 0).unwrap_or(false)
    }

    fn real_node_address(&self, node: CompiledAddress) -> CompiledAddress {
        if let Some(ref node_address_lookup) = self.node_address_lookup {
            node_address_lookup.get(node as usize) as CompiledAddress
        } else {
            node
        }
    }

    fn read_label(&self, reader: &mut BytesReader) -> Result<Label> {
        match self.input_type {
            InputType::Byte1 => reader.read_byte().map(Label::from),
            InputType::Byte2 => reader.read_short().map(Label::from),
            InputType::Byte4 => reader.read_vint(),
        }
    }

    pub fn read_first_real_arc_with_layout(
        &self,
        node: CompiledAddress,
        bytes_reader: &mut BytesReader,
    ) -> Result<(Arc<F::Value>, ArcLayoutContext)> {
        let address = self.real_node_address(node);
        bytes_reader.set_position(address as usize);

        // let mut arc: Arc<T::Value> = Arc::empty();
        // arc.from = Some(node);

        if bytes_reader.read_byte()? == ARCS_AS_FIXED_ARRAY {
            let num_arcs = bytes_reader.read_vint()? as usize;
            let bytes_per_arc = if self.packed || self.version >= VERSION_VINT_TARGET {
                bytes_reader.read_vint()? as usize
            } else {
                bytes_reader.read_int()? as usize
            };
            let arc_start_position = bytes_reader.position();
            let mut layout_context = ArcLayoutContext::FixedArray {
                arc_start_position,
                bytes_per_arc,
                arc_index: 0,
                num_arcs,
            };
            let arc = self.read_next_real_arc(&mut layout_context, bytes_reader, node)?;
            Ok((arc, layout_context))
        } else {
            let mut layout_context = ArcLayoutContext::Linear(address as usize);
            let arc = self.read_next_real_arc(&mut layout_context, bytes_reader, node)?;
            Ok((arc, layout_context))
        }
    }

    pub fn read_first_real_arc(
        &self,
        node: CompiledAddress,
        bytes_reader: &mut BytesReader,
    ) -> Result<Arc<F::Value>> {
        let (arc, _) = self.read_first_real_arc_with_layout(node, bytes_reader)?;
        Ok(arc)
    }

    pub fn read_next_real_arc(
        &self,
        layout_context: &mut ArcLayoutContext,
        bytes_reader: &mut BytesReader,
        parent_node: CompiledAddress,
    ) -> Result<Arc<F::Value>> {
        match *layout_context {
            ArcLayoutContext::FixedArray {
                arc_start_position,
                bytes_per_arc,
                ref mut arc_index,
                num_arcs,
            } => {
                debug_assert!(*arc_index < num_arcs);
                bytes_reader.set_position(arc_start_position);
                bytes_reader.skip_bytes(*arc_index * bytes_per_arc)?;
                *arc_index += 1;
            }
            ArcLayoutContext::Linear(pos) => {
                bytes_reader.set_position(pos);
            }
        }

        let flags = bytes_reader.read_byte()?;
        let label = self.read_label(bytes_reader)?;
        let output = if flag(flags, BIT_ARC_HAS_OUTPUT) {
            Some(self.output_factory.read(bytes_reader)?)
        } else {
            None
        };
        let final_output = if flag(flags, BIT_ARC_HAS_FINAL_OUTPUT) {
            Some(self.output_factory.read_final_output(bytes_reader)?)
        } else {
            None
        };
        let mut arc = Arc::empty();
        arc.label = label;
        arc.flags = flags;
        arc.output = output;
        arc.next_final_output = final_output;
        if flag(flags, BIT_STOP_NODE) {
            arc.target = Some(FINAL_END_NODE);
            arc.next_arc = Some(bytes_reader.position() as i64);
        } else if flag(flags, BIT_TARGET_NEXT) {
            arc.next_arc = Some(bytes_reader.position() as i64);
            if self.node_address_lookup.is_some() {
                debug_assert!(parent_node > 1);

                arc.target = Some(parent_node - 1);
            } else {
                if !flag(flags, BIT_LAST_ARC) {
                    match *layout_context {
                        ArcLayoutContext::FixedArray {
                            arc_start_position,
                            bytes_per_arc,
                            num_arcs,
                            ..
                        } => {
                            bytes_reader.set_position(arc_start_position);
                            bytes_reader.skip_bytes(bytes_per_arc * num_arcs)?;
                        }
                        ArcLayoutContext::Linear(_) => {
                            self.seek_to_next_node(bytes_reader)?;
                        }
                    }
                }
                arc.target = Some(bytes_reader.position() as i64);
            }
        } else {
            arc.target = if self.packed {
                debug_assert!(self.node_ref_to_address.is_some());

                let pos = bytes_reader.position();
                let code = bytes_reader.read_vlong()? as usize;
                if flag(flags, BIT_TARGET_DELTA) {
                    Some((pos + code) as i64)
                } else if let Some(ref node_ref) = self.node_ref_to_address {
                    if code < node_ref.size() {
                        Some(node_ref.get(code))
                    } else {
                        Some(code as i64)
                    }
                } else {
                    Some(code as i64)
                }
            } else {
                Some(self.read_unpacked_node(bytes_reader)?)
            };
            arc.next_arc = Some(bytes_reader.position() as i64);
        }
        if let ArcLayoutContext::Linear(ref mut v) = layout_context {
            assert!(arc.next_arc.is_some());
            *v = arc.next_arc.unwrap() as usize;
        }
        Ok(arc)
    }

    fn seek_to_next_node(&self, bytes_reader: &mut BytesReader) -> Result<()> {
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
                if self.packed {
                    bytes_reader.read_vlong()?;
                } else {
                    self.read_unpacked_node(bytes_reader)?;
                }
            }

            if flag(flags, BIT_LAST_ARC) {
                return Ok(());
            }
        }
    }

    fn read_unpacked_node(&self, bytes_reader: &mut BytesReader) -> Result<CompiledAddress> {
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
        node_in: &UnCompiledNode<F>,
    ) -> Result<CompiledAddress> {
        let no_output = self.output_factory.empty();

        if node_in.num_arcs == 0 {
            if node_in.is_final {
                return Ok(FINAL_END_NODE as i64);
            } else {
                return Ok(NON_FINAL_END_NODE as i64);
            }
        }
        let start_address = self.bytes_store.get_position();
        let do_fixed_array = self.should_expand(builder, &node_in);
        if do_fixed_array {
            if builder.reused_bytes_per_arc.len() < node_in.num_arcs {
                builder.reused_bytes_per_arc.resize(node_in.num_arcs, 0);
            }
        }
        builder.arc_count += node_in.num_arcs as u64;

        let last_arc = node_in.num_arcs - 1;
        let mut last_arc_start = self.bytes_store.get_position();
        let mut max_bytes_per_arc = 0;
        for idx in 0..node_in.arcs.len() {
            let arc = &node_in.arcs[idx];

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
            } else if let Some(ref mut in_counts) = self.in_counts {
                let v = in_counts.get(target as usize) + 1;
                in_counts.set(target as usize, v);
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
                bad.write_vint(node_in.num_arcs as i32)?;
                bad.write_vint(max_bytes_per_arc as i32)?;
                header_len = bad.pos;
                fixed_array_start = start_address + header_len;
            }

            // expand the arcs in place, backwards
            let mut src_pos = self.bytes_store.get_position();
            let mut dest_pos = fixed_array_start + node_in.num_arcs * max_bytes_per_arc;
            assert!(dest_pos >= src_pos);
            if dest_pos > src_pos {
                self.bytes_store.skip_bytes(dest_pos - src_pos);
                for i in 0..node_in.num_arcs {
                    let arc_idx = node_in.num_arcs - 1 - i;
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

        // PackedInts uses int as the index, so we cannot handle
        // > 2.1B nodes when packing
        if self.node_address_lookup.is_some() && builder.node_count == i32::max_value() as u64 {
            bail!(ErrorKind::IllegalState(
                "cannot create a packed FST with more than 2.1 billion nodes".into()
            ));
        }

        builder.node_count += 1;
        let node: i64;
        if let Some(ref mut node_address) = self.node_address_lookup {
            // nodes are addressed by 1+ord:
            if builder.node_count == node_address.size() as u64 {
                node_address.resize(builder.node_count as usize + 1);
                if let Some(ref mut in_counts) = self.in_counts {
                    let new_size = in_counts.size() + 1;
                    in_counts.resize(new_size);
                }
            }
            node_address.set(builder.node_count as usize, this_node_address as i64);
            node = builder.node_count as i64;
        } else {
            node = this_node_address as i64;
        }

        Ok(node)
    }

    #[allow(dead_code)]
    fn write_label(&self, out: &mut DataOutput, v: i32) -> Result<()> {
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
    ///
    fn should_expand(&self, builder: &FstBuilder<F>, node: &UnCompiledNode<F>) -> bool {
        builder.allow_array_arcs
            && ((node.depth <= FIXED_ARRAY_SHALLOW_DISTANCE as i32
                && node.num_arcs >= FIXED_ARRAY_NUM_ARCS_SHALLOW as usize)
                || node.num_arcs >= FIXED_ARRAY_NUM_ARCS_DEEP as usize)
    }

    pub fn finish(&mut self, new_start_node: CompiledAddress) -> Result<()> {
        assert!(new_start_node as usize <= self.bytes_store.get_position());
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
        let parent = root.target.unwrap();
        if self.target_has_arc(root.target) {
            let mut count = 0;
            let mut arcs = vec![None; 128];
            {
                let mut input = self.bytes_reader();
                let (mut arc, mut layout) =
                    self.read_first_real_arc_with_layout(parent, &mut input)?;

                loop {
                    assert_ne!(arc.label, END_LABEL);
                    let is_last = arc.is_last();
                    if arc.label < arcs.len() as i32 {
                        let idx = arc.label as usize;
                        arcs[idx] = Some(arc);
                    } else {
                        break;
                    }
                    if is_last {
                        break;
                    }
                    arc = self.read_next_real_arc(&mut layout, &mut input, parent)?;
                    count += 1;
                }
            }

            if count >= FIXED_ARRAY_NUM_ARCS_SHALLOW {
                self.cached_root_arcs = arcs;
            }
        }
        Ok(())
    }

    pub fn save<T: DataOutput + ?Sized>(&self, out: &mut T) -> Result<()> {
        if self.start_node != -1 {
            bail!(ErrorKind::IllegalState("call finish first!".into()));
        }
        if self.node_address_lookup.is_some() {
            bail!(ErrorKind::IllegalState(
                "cannot save an FST pre-packed FST; it must first be packed".into()
            ));
        }
        if self.packed && self.node_ref_to_address.is_none() {
            bail!(ErrorKind::IllegalState(
                "cannot save a FST which has been loaded from disk".into()
            ));
        }
        codec_util::write_header(out, FILE_FORMAT_NAME, VERSION_CURRENT)?;
        if self.packed {
            out.write_byte(1)?;
        } else {
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

            if !self.packed {
                // reverse
                empty_output_bytes.reverse();
            }
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
        if self.packed {
            self.node_ref_to_address
                .as_ref()
                .unwrap()
                .as_mutable()
                .save(out.as_data_output_mut())?;
        }
        out.write_vlong(self.start_node)?;
        if self.bytes_store.get_position() > 0 {
            let num_bytes = self.bytes_store.get_position();
            out.write_vlong(num_bytes as i64)?;
            self.bytes_store.write_to(out)?;
        }
        Ok(())
    }

    /// Expert: creates an FST by packing this one.  This
    /// process requires substantial additional RAM (currently
    /// up to ~8 bytes per node depending on
    /// acceptable_overhead_ratio), but then should
    /// produce a smaller FST.
    ///
    /// The implementation of this method uses ideas from
    /// <a target="_blank" href="http://www.cs.put.poznan.pl/dweiss/site/publications/download/fsacomp.pdf">Smaller Representation of Finite State Automata</a>,
    /// which describes techniques to reduce the size of a FST.
    /// However, this is not a strict implementation of the
    /// algorithms described in this paper.
    ///
    pub fn pack(
        &mut self,
        builder: &FstBuilder<F>,
        min_in_count_deref: usize,
        max_deref_nodes: usize,
        acceptable_overhead_ratio: f32,
    ) -> Result<FST<F>> {
        // NOTE: maxDerefNodes is intentionally int: we cannot
        // support > 2.1B deref nodes

        // TODO: other things to try
        //   - renumber the nodes to get more next / better locality?
        //   - allow multiple input labels on an arc, so
        //     singular chain of inputs can take one arc (on
        //     wikipedia terms this could save another ~6%)
        //   - in the ord case, the output '1' is presumably
        //     very common (after NO_OUTPUT)... maybe use a bit
        //     for it..?
        //   - use spare bits in flags.... for top few labels /
        //     outputs / targets
        if self.node_address_lookup.is_none() {
            bail!(ErrorKind::IllegalArgument(
                "this FST was not built with willPackFST=true".into()
            ));
        }

        // let no_output = self.outputs().empty();

        let in_counts_count = self.in_counts.as_ref().unwrap().size();
        let top_n = min(max_deref_nodes, in_counts_count);
        let mut q = BinaryHeap::with_capacity(top_n);

        // TODO: we could use more RAM efficient selection algo here...
        for i in 0..in_counts_count {
            let count = self.in_counts.as_ref().unwrap().get(i);
            if count >= min_in_count_deref as i64 {
                if q.len() < top_n {
                    q.push(NodeAndInCount::new(i, count));
                } else {
                    let mut top = q.peek_mut().unwrap();
                    if top.count > count {
                        top.count = count;
                        top.node = i;
                    }
                }
            }
        }

        // free up ram
        self.in_counts = None;

        let mut top_node_map = HashMap::new();
        let len = q.len();
        for i in 0..len {
            let n = q.pop().unwrap();
            top_node_map.insert(n.node, len - 1 - i);
        }

        // +1 because node ords start at 1 (0 is reserved as stop node):
        let mut new_node_address = GrowableWriter::new(
            unsigned_bits_required(self.bytes_store.get_position() as i64),
            (builder.node_count + 1) as usize,
            acceptable_overhead_ratio,
        );

        // Fill initial coarse guess:
        for i in 1..builder.node_count as usize + 1 {
            new_node_address.set(
                i,
                1 + self.bytes_store.get_position() as i64
                    - self.node_address_lookup.as_ref().unwrap().get(i),
            );
        }

        let mut r = self.bytes_reader();

        let mut fst: FST<F>;
        let mut next_count = 0;
        let mut abs_count = 0;
        let mut top_count = 0;
        let mut delta_count = 0;
        loop {
            let mut changed = true;
            let mut neg_delta = false;

            fst = FST::new_packed(
                self.input_type,
                self.output_factory.clone(),
                self.bytes_store.block_bits,
            );

            // Skip 0 byte since 0 is reserved target:
            fst.bytes_store.write_byte(0)?;

            let mut address_error = 0;
            let mut changed_count = 0;
            // Since we re-reverse the bytes, we now write the
            // nodes backwards, so that BIT_TARGET_NEXT is
            // unchanged:
            for i in 0..builder.node_count {
                let node = (builder.node_count - i) as usize;

                let address = fst.bytes_store.get_position();
                if address as i64 != new_node_address.get(node) {
                    address_error = address as i64 - new_node_address.get(node);
                    changed = true;
                    new_node_address.set(node, address as i64);
                    changed_count += 1;
                }

                let mut node_arc_count = 0;
                let mut bytes_per_arc = 0;

                let mut retry = false;

                // for assert
                let mut any_neg_delta = false;

                // Retry loop: possibly iterate more than once, if
                // this is an array'd node and bytesPerArc changes:
                loop {
                    let (mut arc, mut layout) =
                        self.read_first_real_arc_with_layout(node as i64, &mut r)?;
                    let target = arc.target.unwrap() as usize;

                    let mut use_arc_array = false;
                    if let ArcLayoutContext::FixedArray {
                        bytes_per_arc: per_arc,
                        num_arcs,
                        ..
                    } = layout
                    {
                        // Write false first arc:
                        if bytes_per_arc == 0 {
                            bytes_per_arc = per_arc;
                        }
                        fst.bytes_store.write_byte(ARCS_AS_FIXED_ARRAY)?;
                        fst.bytes_store.write_vint(num_arcs as i32)?;
                        fst.bytes_store.write_vint(bytes_per_arc as i32)?;

                        use_arc_array = true;
                    }

                    let mut max_bytes_per_arc = 0;
                    loop {
                        let arc_start_pos = fst.bytes_store.get_position();
                        node_arc_count += 1;

                        let mut flags = 0;

                        if arc.is_last() {
                            flags += BIT_LAST_ARC;
                        }

                        if !use_arc_array && node != 1 && target == node - 1 {
                            flags += BIT_TARGET_NEXT;
                            if !retry {
                                next_count += 1;
                            }
                        }

                        if arc.is_final() {
                            flags += BIT_FINAL_ARC;
                            if arc.next_final_output.is_some() {
                                flags += BIT_ARC_HAS_FINAL_OUTPUT;
                            }
                        }

                        if !self.target_has_arc(arc.target) {
                            flags += BIT_STOP_NODE;
                        }

                        if arc.output.is_some() {
                            flags += BIT_ARC_HAS_OUTPUT;
                        }

                        let abs_ptr: i64;
                        let do_write_target =
                            self.target_has_arc(arc.target) && (flags & BIT_TARGET_NEXT) == 0;
                        if do_write_target {
                            if let Some(ptr) = top_node_map.get(&target) {
                                abs_ptr = *ptr as i64;
                            } else {
                                abs_ptr = top_node_map.len() as i64 + new_node_address.get(target)
                                    + address_error;
                            }

                            let mut delta = new_node_address.get(target) + address_error
                                - fst.bytes_store.get_position() as i64
                                - 2;
                            if delta < 0 {
                                any_neg_delta = true;
                                delta = 0;
                            }

                            if delta < abs_ptr {
                                flags |= BIT_TARGET_DELTA;
                            }
                        } else {
                            abs_ptr = 0;
                        }

                        assert_ne!(flags, ARCS_AS_FIXED_ARRAY);
                        fst.bytes_store.write_byte(flags)?;

                        fst.write_label_local(arc.label)?;

                        if let Some(ref output) = arc.output {
                            self.output_factory.write(output, &mut fst.bytes_store)?;
                        }

                        if let Some(ref output) = arc.next_final_output {
                            self.output_factory.write(output, &mut fst.bytes_store)?;
                        }

                        if do_write_target {
                            let mut delta = new_node_address.get(target) + address_error
                                - fst.bytes_store.get_position() as i64;
                            if delta < 0 {
                                any_neg_delta = true;
                                delta = 0;
                            }

                            if flag(flags, BIT_TARGET_DELTA) {
                                fst.bytes_store.write_vlong(delta)?;
                                if !retry {
                                    delta_count += 1;
                                }
                            } else {
                                fst.bytes_store.write_vlong(abs_ptr)?;
                                if !retry {
                                    if abs_ptr >= top_node_map.len() as i64 {
                                        abs_count += 1;
                                    } else {
                                        top_count += 1;
                                    }
                                }
                            }
                        }

                        if use_arc_array {
                            let arc_bytes = fst.bytes_store.get_position() - arc_start_pos;
                            max_bytes_per_arc = max(max_bytes_per_arc, arc_bytes);
                            // NOTE: this may in fact go "backwards", if
                            // somehow (rarely, possibly never) we use
                            // more bytesPerArc in this rewrite than the
                            // incoming FST did... but in this case we
                            // will retry (below) so it's OK to ovewrite
                            // bytes:
                            // wasted += bytesPerArc - arcBytes;
                            let pos = fst.bytes_store.get_position();
                            fst.bytes_store
                                .skip_bytes(arc_start_pos + bytes_per_arc - pos);
                        }

                        if arc.is_last() {
                            break;
                        }

                        arc = self.read_next_real_arc(&mut layout, &mut r, arc.from.unwrap())?;
                    }

                    if use_arc_array {
                        if max_bytes_per_arc == bytes_per_arc
                            || (retry && max_bytes_per_arc <= bytes_per_arc)
                        {
                            // converged
                            break;
                        }
                    } else {
                        break;
                    }

                    // Retry:
                    bytes_per_arc = max_bytes_per_arc;
                    fst.bytes_store.truncate(address);
                    node_arc_count = 0;
                    retry = true;
                    any_neg_delta = false;
                }

                debug!("node arc count: {}", node_arc_count);

                neg_delta |= any_neg_delta;
            }

            debug!("node change count: {}", changed_count);

            if !changed {
                // We don't renumber the nodes (just reverse their
                // order) so nodes should only point forward to
                // other nodes because we only produce acyclic FSTs
                // w/ nodes only pointing "forwards":
                assert!(!neg_delta);
                break;
            }
        }

        debug!(
            "next count: {}, abs count: {}, top count:{}, delta count: {}",
            next_count, abs_count, top_count, delta_count
        );

        let mut max_address = 0;
        for (k, _) in &top_node_map {
            max_address = max(max_address, new_node_address.get(*k));
        }

        let mut node_ref_to_address_in = get_mutable_by_ratio_as_reader(
            top_node_map.len(),
            unsigned_bits_required(max_address),
            acceptable_overhead_ratio,
        );
        for (k, v) in &top_node_map {
            node_ref_to_address_in
                .as_mutable_mut()
                .set(*v, new_node_address.get(*k));
        }
        fst.node_ref_to_address = Some(node_ref_to_address_in);
        fst.start_node = new_node_address.get(self.start_node as usize);

        if let Some(ref empty) = self.empty_output {
            fst.set_empty_output(empty.clone());
        }

        fst.bytes_store.finish();
        fst.cache_root_arcs()?;

        Ok(fst)
    }
}

#[derive(Eq, PartialEq)]
struct NodeAndInCount {
    node: usize,
    count: i64,
}

impl NodeAndInCount {
    fn new(node: usize, count: i64) -> Self {
        NodeAndInCount { node, count }
    }
}

impl PartialOrd for NodeAndInCount {
    fn partial_cmp(&self, other: &NodeAndInCount) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// reversed order for binary heap
impl Ord for NodeAndInCount {
    fn cmp(&self, other: &Self) -> Ordering {
        let ord = other.count.cmp(&self.count);
        if ord != Ordering::Equal {
            ord
        } else {
            self.node.cmp(&other.node)
        }
    }
}

enum FSTBytesReader<'a> {
    Directional(DirectionalBytesReader<'a>),
    BytesStore(StoreBytesReader),
}

impl<'a> BytesReader for FSTBytesReader<'a> {
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

impl<'a> io::Read for FSTBytesReader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match *self {
            FSTBytesReader::Directional(ref mut d) => d.read(buf),
            FSTBytesReader::BytesStore(ref mut b) => b.read(buf),
        }
    }
}

impl<'a> DataInput for FSTBytesReader<'a> {
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
    use core::util::ints_ref::{IntsRef, IntsRefBuilder};

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
