use core::codec::codec_util;
use core::store::DataInput;
use core::util::fst::bytes_store::BytesStore;
use core::util::fst::{BytesReader, Output, OutputFactory};
use core::util::packed_misc::{get_reader, GrowableWriter, Reader};
use error::*;

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

// const FIXED_ARRAY_SHALLOW_DISTANCE: u8 = 3;
// const FIXED_ARRAY_NUM_ARCS_SHALLOW: u8 = 5;
// const FIXED_ARRAY_NUM_ARCS_DEEP: u8 = 10;
const FILE_FORMAT_NAME: &str = "FST";

const VERSION_PACKED: u8 = 3;
const VERSION_VINT_TARGET: u8 = 4;
const VERSION_NO_NODE_ARC_COUNTS: u8 = 5;
// const VERSION_CURRENT: u8 = VERSION_NO_NODE_ARC_COUNTS;
// const FINAL_END_NODE: u8 = -1;
// const NON_FINAL_END_NODE: u8 = 0;

/// Only works on 64bit os
const DEFAULT_MAX_BLOCK_BITS: u8 = 30;

pub const END_LABEL: Label = -1;

fn flag(flags: u8, bit: u8) -> bool {
    (flags & bit) != 0
}

type Label = i32;
type CompiledAddress = usize;

#[derive(Clone)]
pub enum InputType {
    Byte1,
    Byte2,
    Byte4,
}

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

enum ArcLayoutContext {
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
        if self.offset > self.bytes.len() {
            None
        } else if self.offset == self.bytes.len() {
            self.offset += 1;
            Some(END_LABEL)
        } else {
            let label = self.bytes[self.offset];
            self.offset += 1;
            Some(Label::from(label))
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

    fn is_last(&self) -> bool {
        flag(self.flags, BIT_LAST_ARC)
    }

    fn is_final(&self) -> bool {
        flag(self.flags, BIT_FINAL_ARC)
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

pub struct FST<T: OutputFactory> {
    pub input_type: InputType,
    start_node: CompiledAddress,
    packed: bool,
    version: u8,
    output_factory: T,
    bytes_store: BytesStore,
    node_ref_to_address: Option<Box<Reader>>,
    node_address_lookup: Option<GrowableWriter>,
    default_output_if_no_input: Option<T::Value>,
}

impl<T: OutputFactory> FST<T> {
    pub fn from_input<I: DataInput + ?Sized>(data_in: &mut I, output_factory: T) -> Result<FST<T>> {
        let output_factory = output_factory;
        let max_block_bits = DEFAULT_MAX_BLOCK_BITS;

        // Only reads most recent format; we don't have
        // back-compat promise for FSTs (they are experimental):
        let version = codec_util::check_header(
            data_in,
            FILE_FORMAT_NAME,
            i32::from(VERSION_PACKED),
            i32::from(VERSION_NO_NODE_ARC_COUNTS),
        )?;
        let packed = data_in.read_byte()? == 1;
        let default_output_if_no_input = if data_in.read_byte()? == 1 {
            // Accepts empty string
            // 1KB blocks:
            let num_bytes = data_in.read_vint()? as usize;
            let bytes_store = BytesStore::new(data_in, num_bytes, 10)?;
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
        if version < i32::from(VERSION_NO_NODE_ARC_COUNTS) {
            data_in.read_vlong()?;
            data_in.read_vlong()?;
            data_in.read_vlong()?;
        }

        let num_bytes = data_in.read_vlong()?;
        let block_bits = if num_bytes > (1 << max_block_bits) {
            1 << max_block_bits
        } else {
            num_bytes
        };
        let bytes_store = BytesStore::new(data_in, num_bytes as usize, block_bits as usize)?;

        Ok(FST {
            input_type,
            start_node,
            packed,
            version: version as u8,
            output_factory,
            bytes_store,
            node_ref_to_address: node_ref_to_addr,
            node_address_lookup: None,
            default_output_if_no_input,
        })
    }

    pub fn get(&self, bytes: &[u8]) -> Result<Option<T::Value>> {
        let mut arc = self.root_arc();
        let mut output = self.output_factory.empty();
        let mut bytes_reader = self.bytes_reader();
        let bytes_ref = BytesRefFSTEnum::new(bytes);

        for label in bytes_ref {
            let next_arc = self.find_target_arc(label, arc, bytes_reader.as_mut())?;
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
            Ok(Some(output))
        } else {
            Ok(None)
        }
    }

    pub fn floor(&self, bytes: &[u8]) -> Result<(T::Value, usize)> {
        let mut arc = self.root_arc();
        let mut output = self.output_factory.empty();
        let mut bytes_reader = self.bytes_reader();
        let mut length = 0usize;
        let mut last_final_length = 0usize;
        let bytes_ref = BytesRefFSTEnum::new(bytes);
        if let Some(ref out) = arc.output {
            if !out.is_empty() {
                output = output.cat(&out);
            }
        }
        let mut last_final_output = if arc.is_final() && arc.next_final_output.is_some() {
            Some(output.cat(arc.next_final_output.as_ref().unwrap()))
        } else {
            None
        };

        for label in bytes_ref {
            let next_arc = self.find_target_arc(label, arc, bytes_reader.as_mut())?;
            match next_arc {
                Some(a) => {
                    if label != END_LABEL {
                        length += 1;
                    }
                    arc = a;
                    if let Some(ref out) = arc.output {
                        if !out.is_empty() {
                            output = output.cat(&out);
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

    fn bytes_reader(&self) -> Box<BytesReader> {
        let bytes_reader = if self.packed {
            self.bytes_store.get_forward_reader()
        } else {
            self.bytes_store.get_reverse_reader()
        };
        Box::new(bytes_reader)
    }

    pub fn root_arc(&self) -> Arc<T::Value> {
        let mut arc = Arc::empty();

        if let Some(ref default_output) = self.default_output_if_no_input {
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
        incoming_arc: Arc<T::Value>,
        bytes_reader: &mut BytesReader,
    ) -> Result<Option<Arc<T::Value>>> {
        if label == END_LABEL {
            let mut target_arc: Arc<T::Value> = incoming_arc.clone();

            if incoming_arc.is_final() {
                if !self.target_has_arc(incoming_arc.target) {
                    target_arc.flags = BIT_LAST_ARC;
                } else {
                    // NOTE!!:
                    // target_arc.flags = 0u8;
                    //
                    // next_arc is a node (not an address!) in this case:
                    target_arc.next_arc = incoming_arc.target;
                    target_arc.from = incoming_arc.target;
                }
                target_arc.output = incoming_arc.next_final_output;
                target_arc.label = END_LABEL;
                return Ok(Some(target_arc));
            } else {
                return Ok(None);
            }
        }

        // TODO: use arc cache

        if !self.target_has_arc(incoming_arc.target) {
            return Ok(None);
        }

        if let Some(target) = incoming_arc.target {
            bytes_reader.set_position(target);
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
                    let arc_context = ArcLayoutContext::FixedArray {
                        arc_start_position,
                        bytes_per_arc,
                        arc_index: mid,
                        num_arcs,
                    };
                    return self.read_real_arc(&arc_context, bytes_reader, incoming_arc.next_arc)
                        .map(Some);
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
                let context = ArcLayoutContext::Linear(arc.next_arc.unwrap());
                arc = self.read_real_arc(&context, bytes_reader, None)?;
            }
        }
    }

    fn target_has_arc(&self, target: Option<CompiledAddress>) -> bool {
        match target {
            Some(t) => t > 0,
            None => false,
        }
    }

    fn real_node_address(&self, node: CompiledAddress) -> Result<CompiledAddress> {
        match self.node_address_lookup {
            Some(ref lookup) => Ok(lookup.get(node) as CompiledAddress),
            None => Ok(node),
        }
    }

    fn read_label(&self, reader: &mut BytesReader) -> Result<Label> {
        match self.input_type {
            InputType::Byte1 => reader.read_byte().map(Label::from),
            InputType::Byte2 => reader.read_short().map(Label::from),
            InputType::Byte4 => reader.read_vint(),
        }
    }

    fn read_first_real_arc(
        &self,
        node: CompiledAddress,
        bytes_reader: &mut BytesReader,
    ) -> Result<Arc<T::Value>> {
        let address = self.real_node_address(node)?;
        bytes_reader.set_position(address);

        let mut arc: Arc<T::Value> = Arc::empty();
        arc.from = Some(node);

        if bytes_reader.read_byte()? == ARCS_AS_FIXED_ARRAY {
            let num_arcs = bytes_reader.read_vint()? as usize;
            let bytes_per_arc = if self.packed || self.version >= VERSION_VINT_TARGET {
                bytes_reader.read_vint()? as usize
            } else {
                bytes_reader.read_int()? as usize
            };
            let arc_start_position = bytes_reader.position();
            let layout_context = ArcLayoutContext::FixedArray {
                arc_start_position,
                bytes_per_arc,
                arc_index: 0,
                num_arcs,
            };
            self.read_real_arc(&layout_context, bytes_reader, Some(arc_start_position))
        } else {
            let layout_context = ArcLayoutContext::Linear(address);
            self.read_real_arc(&layout_context, bytes_reader, None)
        }
    }

    fn read_real_arc(
        &self,
        layout_context: &ArcLayoutContext,
        bytes_reader: &mut BytesReader,
        parent_node: Option<CompiledAddress>,
    ) -> Result<Arc<T::Value>> {
        match *layout_context {
            ArcLayoutContext::FixedArray {
                arc_start_position,
                bytes_per_arc,
                arc_index,
                num_arcs,
            } => {
                debug_assert!(arc_index < num_arcs);
                bytes_reader.set_position(arc_start_position);
                bytes_reader.skip_bytes(arc_index * bytes_per_arc)?;
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
        let mut arc: Arc<T::Value> = Arc::empty();
        arc.label = label;
        arc.flags = flags;
        arc.output = output;
        arc.next_final_output = final_output;
        if flag(flags, BIT_STOP_NODE) {
            arc.target = None;
            arc.next_arc = Some(bytes_reader.position());
        } else if flag(flags, BIT_TARGET_NEXT) {
            arc.next_arc = Some(bytes_reader.position());
            if self.node_address_lookup.is_some() {
                debug_assert!(parent_node.is_some());

                arc.target = parent_node;
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
                arc.target = Some(bytes_reader.position());
            }
        } else {
            arc.target = if self.packed {
                debug_assert!(self.node_ref_to_address.is_some());

                let pos = bytes_reader.position();
                let code = bytes_reader.read_vlong()? as usize;
                if flag(flags, BIT_TARGET_DELTA) {
                    Some(pos + code)
                } else if let Some(ref node_ref) = self.node_ref_to_address {
                    if code < node_ref.size() {
                        Some(node_ref.get(code) as usize)
                    } else {
                        Some(code)
                    }
                } else {
                    Some(code)
                }
            } else {
                Some(self.read_unpacked_node(bytes_reader)?)
            };
            arc.next_arc = Some(bytes_reader.position());
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
}
