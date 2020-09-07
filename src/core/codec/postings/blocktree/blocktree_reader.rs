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

use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::btree_map::Keys;
use std::collections::BTreeMap;
use std::fmt::Write;
use std::io::Read;
use std::ops::DerefMut;
use std::string::ToString;
use std::sync::Arc;

use core::codec::field_infos::FieldInfo;
use core::codec::postings::blocktree::{BlockTermState, SegmentTermsIterFrame, MAX_LONGS_SIZE};
use core::codec::postings::{
    FieldsProducer, Lucene50PostingIterator, Lucene50PostingsReader, Lucene50PostingsReaderRef,
};
use core::codec::segment_infos::{segment_file_name, SegmentReadState};
use core::codec::{codec_util, Codec};
use core::codec::{Fields, SeekStatus, TermIterator, Terms};
use core::doc::IndexOptions;
use core::store::directory::Directory;
use core::store::io::{ByteArrayDataInput, DataInput, IndexInput};
use core::util::fst::{
    Arc as FSTArc, ByteSequenceOutput, ByteSequenceOutputFactory, DirectionalBytesReader,
    FSTBytesReader, OutputFactory, FST,
};
use core::util::UnsignedShift;
use error::{
    ErrorKind::{CorruptIndex, IllegalState, UnsupportedOperation},
    Result,
};

const OUTPUT_FLAGS_NUM_BITS: usize = 2;
// const OUTPUT_FLAGS_MASK: i32 = 0x3;
pub const OUTPUT_FLAGS_IS_FLOOR: i64 = 0x1;
pub const OUTPUT_FLAGS_HAS_TERMS: i64 = 0x2;

/// Extension of terms file
pub const TERMS_EXTENSION: &str = "tim";
pub const TERMS_CODEC_NAME: &str = "BlockTreeTermsDict";

/// Initial terms format.
pub const VERSION_START: i32 = 0;

/// Auto-prefix terms.
pub const VERSION_AUTO_PREFIX_TERMS: i32 = 1;

/// Conditional auto-prefix terms: we record at write time whether
/// this field did write any auto-prefix terms.
/// const VERSION_AUTO_PREFIX_TERMS_COND: i32 = 2;

/// Auto-prefix terms have been superseded by points.
pub const VERSION_AUTO_PREFIX_TERMS_REMOVED: i32 = 3;

/// Current terms format.
pub const VERSION_CURRENT: i32 = VERSION_AUTO_PREFIX_TERMS_REMOVED;

/// Extension of terms index file
pub const TERMS_INDEX_EXTENSION: &str = "tip";
pub const TERMS_INDEX_CODEC_NAME: &str = "BlockTreeTermsIndex";

type IndexInputRef = Arc<dyn IndexInput>;

/// A block-based terms index and dictionary that assigns
/// terms to variable length blocks according to how they
/// share prefixes.
///
/// The terms index is a prefix trie
/// whose leaves are term blocks.  The advantage of this
/// approach is that seekExact is often able to
/// determine a term cannot exist without doing any IO, and
/// intersection with Automata is very fast.  Note that this
/// terms dictionary has its own fixed terms index (ie, it
/// does not support a pluggable terms index
/// implementation).
///
/// <p><b>NOTE</b>: this terms dictionary supports
/// min/maxItemsPerBlock during indexing to control how
/// much memory the terms index uses.</p>
///
/// <p>If auto-prefix terms were indexed (see
/// {@link BlockTreeTermsWriter}), then the {@link Terms#intersect}
/// implementation here will make use of these terms only if the
/// automaton has a binary sink state, i.e. an accept state
/// which has a transition to itself accepting all byte values.
/// For example, both {@link PrefixQuery} and {@link TermRangeQuery}
/// pass such automata to {@link Terms#intersect}.</p>
///
/// <p>The data structure used by this implementation is very
/// similar to a burst trie
/// (http://citeseer.ist.psu.edu/viewdoc/summary?doi=10.1.1.18.3499),
/// but with added logic to break up too-large blocks of all
/// terms sharing a given prefix into smaller ones.</p>
///
/// <p>Use {@link org.apache.lucene.index.CheckIndex} with the <code>-verbose</code>
/// option to see summary statistics on the blocks in the dictionary.
pub struct BlockTreeTermsReader {
    // Open input to the main terms dict file (_X.tib)
    terms_in: IndexInputRef,

    // Reads the terms dict entries, to gather state to
    // produce DocsEnum on demand
    postings_reader: Lucene50PostingsReaderRef,

    fields: BTreeMap<String, FieldReaderRef>,

    /// File offset where the directory starts in the terms file.
    dir_offset: i64,

    /// File offset where the directory starts in the index file.
    index_dir_offset: i64,

    segment: Arc<String>,

    version: i32,

    any_auto_prefix_terms: bool,
}

impl BlockTreeTermsReader {
    pub fn new<D: Directory, DW: Directory, C: Codec>(
        postings_reader: Lucene50PostingsReader,
        state: &SegmentReadState<'_, D, DW, C>,
    ) -> Result<BlockTreeTermsReader> {
        let segment = Arc::new(state.segment_info.name.clone());
        let terms_name = segment_file_name(&segment, &state.segment_suffix, TERMS_EXTENSION);
        let mut terms_in = state.directory.open_input(&terms_name, state.context)?;
        let version = codec_util::check_index_header(
            terms_in.as_mut(),
            TERMS_CODEC_NAME,
            VERSION_START,
            VERSION_CURRENT,
            &state.segment_info.id,
            &state.segment_suffix,
        )?;
        let any_auto_prefix_terms = if version < VERSION_AUTO_PREFIX_TERMS
            || version >= VERSION_AUTO_PREFIX_TERMS_REMOVED
        {
            // Old (pre-5.2.0) or recent (6.2.0+) index, no auto-prefix terms:
            false
        } else if version == VERSION_AUTO_PREFIX_TERMS {
            // 5.2.x index, might have auto-prefix terms:
            true
        } else {
            // 5.3.x index, we record up front if we may have written any auto-prefix terms:
            match terms_in.read_byte()? {
                0 => false,
                1 => true,
                b => bail!(CorruptIndex(format!(
                    "invalid any_auto_prefix_terms: expected 0 or 1 but got {}",
                    b
                ))),
            }
        };

        let index_name = segment_file_name(&segment, &state.segment_suffix, TERMS_INDEX_EXTENSION);
        let mut index_in = state.directory.open_input(&index_name, state.context)?;
        codec_util::check_index_header(
            index_in.as_mut(),
            TERMS_INDEX_CODEC_NAME,
            version,
            version,
            &state.segment_info.id,
            &state.segment_suffix,
        )?;
        // codec_util::checksum_entire_file(index_in.as_mut())?;

        // Have PostingsReader init itself
        postings_reader.init(terms_in.as_mut(), state)?;
        let postings_reader = Arc::new(postings_reader);

        // NOTE: data file is too costly to verify checksum against all the bytes on open,
        // but for now we at least verify proper structure of the checksum footer: which looks
        // for FOOTER_MAGIC + algorithmID. This is cheap and can detect some forms of corruption
        // such as file truncation.
        codec_util::retrieve_checksum(terms_in.as_mut())?;

        // Read per-field details
        Self::seek_dir(terms_in.as_mut(), 0)?;
        Self::seek_dir(index_in.as_mut(), 0)?;

        let num_fields = terms_in.read_vint()?;
        if num_fields < 0 {
            bail!(CorruptIndex(format!("invalid num_fields: {}", num_fields)));
        }

        let readers_terms_in = Arc::from(terms_in.clone()?);
        let mut terms_reader = BlockTreeTermsReader {
            terms_in: readers_terms_in,
            postings_reader: postings_reader.clone(),
            fields: BTreeMap::default(),
            segment: segment.clone(),
            version,
            any_auto_prefix_terms,
            dir_offset: 0,
            index_dir_offset: 0,
        };

        let fields = {
            let mut fields = BTreeMap::new();

            for _ in 0..num_fields as usize {
                let field = terms_in.read_vint()?;
                let num_terms = terms_in.read_vlong()?;
                if num_terms <= 0 {
                    bail!(CorruptIndex(format!(
                        "Illegal num_terms for field number: {}",
                        field
                    )));
                }
                let num_bytes = terms_in.read_vint()?;
                if num_bytes < 0 {
                    bail!(CorruptIndex(format!(
                        "invalid root_code for field number: {}, num_bytes={}",
                        field, num_bytes
                    )));
                }
                let mut root_code = vec![0 as u8; num_bytes as usize];
                terms_in.read_exact(&mut root_code)?;
                let field_info = state.field_infos.by_number.get(&(field as u32));
                if field_info.is_none() {
                    bail!(CorruptIndex(format!("invalid field number: {}", field)));
                }
                let field_info = field_info.unwrap();
                let sum_total_term_freq = match field_info.index_options {
                    IndexOptions::Docs => -1,
                    _ => terms_in.read_vlong()?,
                };
                let sum_doc_freq = terms_in.read_vlong()?;
                let doc_count = terms_in.read_vint()?;
                let longs_size = terms_in.read_vint()?;
                if longs_size < 0 {
                    bail!(CorruptIndex(format!(
                        "invalid longs_size for field: {}, longs_size={}",
                        field_info.name, longs_size
                    )));
                }
                let min_term = Self::read_bytes(terms_in.deref_mut())?;
                let max_term = Self::read_bytes(terms_in.deref_mut())?;
                if doc_count < 0 || doc_count > state.segment_info.max_doc {
                    // #docs with field must be <= #docs
                    bail!(CorruptIndex(format!(
                        "invalid doc_count: {} max_doc: {}",
                        doc_count, state.segment_info.max_doc
                    )));
                }
                if sum_doc_freq < i64::from(doc_count) {
                    // #postings must be >= #docs with field
                    bail!(CorruptIndex(format!(
                        "invalid sum_doc_freq: {} docCount: {}",
                        sum_doc_freq, doc_count
                    )));
                }
                if sum_total_term_freq != -1 && sum_total_term_freq < sum_doc_freq {
                    // #positions must be >= #postings
                    bail!(CorruptIndex(format!(
                        "invalid sum_total_term_freq: {} sum_doc_freq: {}",
                        sum_total_term_freq, sum_doc_freq
                    )));
                }
                let index_start_fp = index_in.read_vlong()?;
                if fields.contains_key(&field_info.name) {
                    bail!(CorruptIndex(format!(
                        "duplicated field: {}",
                        field_info.name
                    )));
                }
                let terms_in = Arc::from(terms_in.clone()?);
                let reader = Arc::new(FieldReader::new(
                    terms_reader.clone_without_fields(),
                    field_info.clone(),
                    num_terms,
                    root_code,
                    sum_total_term_freq,
                    sum_doc_freq,
                    doc_count,
                    index_start_fp,
                    longs_size as usize,
                    Some(index_in.as_mut()),
                    min_term,
                    max_term,
                    terms_in,
                    postings_reader.clone(),
                )?);
                fields.insert(field_info.name.clone(), reader);
            }
            fields
        };

        terms_reader.fields = fields;
        Ok(terms_reader)
    }

    fn clone_without_fields(&self) -> BlockTreeTermsReader {
        BlockTreeTermsReader {
            terms_in: Arc::clone(&self.terms_in),
            postings_reader: Arc::clone(&self.postings_reader),
            fields: BTreeMap::default(),
            segment: Arc::clone(&self.segment),
            version: self.version,
            any_auto_prefix_terms: self.any_auto_prefix_terms,
            dir_offset: self.dir_offset,
            index_dir_offset: self.index_dir_offset,
        }
    }

    fn read_bytes(input: &mut dyn IndexInput) -> Result<Vec<u8>> {
        let len = input.read_vint()? as usize;
        let mut vec = vec![0 as u8; len];
        input.read_exact(&mut vec)?;
        Ok(vec)
    }

    /// Seek {@code input} to the directory offset.
    fn seek_dir(input: &mut dyn IndexInput, _dir_offset: i64) -> Result<()> {
        // TODO double check this in lucene code
        let offset = input.len() as i64 - codec_util::footer_length() as i64 - 8;
        input.seek(offset)?;
        let dir_offset = input.read_long()?;
        input.seek(dir_offset)
    }

    pub fn postings_reader(&self) -> &Lucene50PostingsReaderRef {
        &self.postings_reader
    }

    pub fn dir_offset(&self) -> i64 {
        self.dir_offset
    }

    pub fn index_dir_offset(&self) -> i64 {
        self.index_dir_offset
    }

    pub fn segment(&self) -> &str {
        &self.segment
    }

    pub fn version(&self) -> i32 {
        self.version
    }

    pub fn is_any_auto_prefix_terms(&self) -> bool {
        self.any_auto_prefix_terms
    }

    pub fn keys(&self) -> Keys<String, FieldReaderRef> {
        self.fields.keys()
    }
}

impl FieldsProducer for BlockTreeTermsReader {
    fn check_integrity(&self) -> Result<()> {
        //        let input = (*self.terms_in).clone()?;
        //        codec_util::checksum_entire_file(input.as_mut())?;
        self.postings_reader.check_integrity()
    }
}

impl Fields for BlockTreeTermsReader {
    type Terms = FieldReaderRef;
    fn fields(&self) -> Vec<String> {
        self.fields.keys().cloned().collect()
    }

    fn terms(&self, field: &str) -> Result<Option<Self::Terms>> {
        Ok(self.fields.get(field).map(Arc::clone))
    }

    fn size(&self) -> usize {
        self.fields.len()
    }
}

type FSTRef = Arc<FST<ByteSequenceOutputFactory>>;

/// BlockTree's implementation of `Terms`
pub struct FieldReader {
    num_terms: i64,
    field_info: Arc<FieldInfo>,
    sum_total_term_freq: i64,
    sum_doc_freq: i64,
    doc_count: i32,
    _index_start_fp: i64,
    _root_block_fp: i64,
    root_code: Vec<u8>,
    min_term: Vec<u8>,
    max_term: Vec<u8>,
    longs_size: usize,
    index: Option<FSTRef>,
    terms_in: IndexInputRef,
    postings_reader: Lucene50PostingsReaderRef,
    parent: BlockTreeTermsReader,
}

pub type FieldReaderRef = Arc<FieldReader>;

impl FieldReader {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        parent: BlockTreeTermsReader,
        field_info: Arc<FieldInfo>,
        num_terms: i64,
        root_code: Vec<u8>,
        sum_total_term_freq: i64,
        sum_doc_freq: i64,
        doc_count: i32,
        index_start_fp: i64,
        longs_size: usize,
        index_in: Option<&mut dyn IndexInput>,
        min_term: Vec<u8>,
        max_term: Vec<u8>,
        terms_in: IndexInputRef,
        postings_reader: Lucene50PostingsReaderRef,
    ) -> Result<FieldReader> {
        debug_assert!(longs_size <= MAX_LONGS_SIZE);
        let mut root_block_fp = root_code.as_slice().read_vlong()? as usize;
        root_block_fp >>= OUTPUT_FLAGS_NUM_BITS;
        let root_block_fp = root_block_fp as i64;
        let index = if index_in.is_some() {
            let mut clone = index_in.unwrap().clone()?;
            clone.seek(index_start_fp)?;
            Some(Arc::new(FST::from_input(
                clone.as_mut(),
                ByteSequenceOutputFactory {},
            )?))
        } else {
            None
        };
        Ok(FieldReader {
            field_info,
            num_terms,
            root_code,
            sum_total_term_freq,
            sum_doc_freq,
            doc_count,
            _index_start_fp: index_start_fp,
            _root_block_fp: root_block_fp,
            min_term,
            max_term,
            longs_size,
            index,
            terms_in,
            postings_reader,
            parent,
        })
    }

    pub fn root_code(&self) -> &[u8] {
        &self.root_code
    }

    pub fn field_info(&self) -> &FieldInfo {
        self.field_info.as_ref()
    }

    #[inline]
    pub fn index(&self) -> &FSTRef {
        self.index.as_ref().unwrap()
    }

    #[inline]
    pub fn longs_size(&self) -> usize {
        self.longs_size
    }

    #[inline]
    pub fn parent(&self) -> &BlockTreeTermsReader {
        &self.parent
    }
}

impl<'a> Terms for FieldReader {
    type Iterator = SegmentTermIterator;

    fn iterator(&self) -> Result<Self::Iterator> {
        let field_info = self.field_info.clone();
        debug_assert!(self.index.is_some());
        let postings_reader = self.postings_reader.clone();
        let terms_in = self.terms_in.clone();

        Ok(SegmentTermIterator::new(
            self,
            terms_in,
            postings_reader,
            field_info,
        ))
    }

    fn size(&self) -> Result<i64> {
        Ok(self.num_terms)
    }

    fn sum_total_term_freq(&self) -> Result<i64> {
        Ok(self.sum_total_term_freq)
    }

    fn sum_doc_freq(&self) -> Result<i64> {
        Ok(self.sum_doc_freq)
    }

    fn doc_count(&self) -> Result<i32> {
        Ok(self.doc_count)
    }

    fn has_freqs(&self) -> Result<bool> {
        Ok(self.field_info.index_options.has_freqs())
    }

    fn has_offsets(&self) -> Result<bool> {
        Ok(self.field_info.index_options.has_offsets())
    }

    fn has_positions(&self) -> Result<bool> {
        Ok(self.field_info.index_options.has_positions())
    }

    fn has_payloads(&self) -> Result<bool> {
        Ok(self.field_info.has_store_payloads)
    }

    fn min(&self) -> Result<Option<Vec<u8>>> {
        Ok(Some(self.min_term.clone()))
    }

    fn max(&self) -> Result<Option<Vec<u8>>> {
        Ok(Some(self.max_term.clone()))
    }

    fn stats(&self) -> Result<String> {
        let field_info = self.field_info.clone();
        debug_assert!(self.index.is_some());
        let postings_reader = self.postings_reader.clone();
        let terms_in = self.terms_in.clone();
        let mut iter = SegmentTermIteratorInner::new(self, terms_in, postings_reader, field_info);
        let stats = iter.compute_block_stats()?;

        stats.to_string()
    }
}

/// BlockTree statistics for a single field
/// returned by {@link FieldReader#getStats()}.
pub struct Stats {
    /// Byte size of the index.
    index_num_bytes: i64,

    /// Total number of terms in the field.
    total_term_count: i64,

    /// Total number of bytes (sum of term lengths) across all terms in the field.
    total_term_bytes: i64,

    // TODO: add total auto-prefix term count
    /// The number of normal (non-floor) blocks in the terms file.
    non_floor_block_count: i32,

    /// The number of floor blocks (meta-blocks larger than the
    /// allowed {@code maxItemsPerBlock}) in the terms file.
    floor_block_count: i32,

    /// The number of sub-blocks within the floor blocks.
    floor_sub_block_count: i32,

    /// The number of "internal" blocks (that have both
    /// terms and sub-blocks).
    mixed_block_count: i32,

    /// The number of "leaf" blocks (blocks that have only
    /// terms).
    terms_only_block_count: i32,

    /// The number of "internal" blocks that do not contain
    /// terms (have only sub-blocks).
    sub_blocks_only_block_count: i32,

    /// Total number of blocks.
    total_block_count: i32,

    /// Number of blocks at each prefix depth.
    block_count_by_prefix_len: Vec<i32>,
    start_block_count: i32,
    end_block_count: i32,

    /// Total number of bytes used to store term suffixes.
    total_block_suffix_bytes: i64,

    /// Total number of bytes used to store term stats (not
    /// including what the {@link PostingsReaderBase}
    /// stores.
    total_block_stats_bytes: i64,

    /// Total bytes stored by the {@link PostingsReaderBase},
    /// plus the other few vInts stored in the frame.
    total_block_other_bytes: i64,

    /// Segment name.
    segment: String,

    /// Field name.
    field: String,
}

impl Stats {
    pub fn new(segment: &str, field: &str) -> Stats {
        Stats {
            index_num_bytes: 0,

            /// Total number of terms in the field.
            total_term_count: 0,

            /// Total number of bytes (sum of term lengths) across all terms in the
            /// field.
            total_term_bytes: 0,

            // TODO: add total auto-prefix term count
            /// The number of normal (non-floor) blocks in the terms file.
            non_floor_block_count: 0,

            /// The number of floor blocks (meta-blocks larger than the
            /// allowed {@code maxItemsPerBlock}) in the terms file.
            floor_block_count: 0,

            /// The number of sub-blocks within the floor blocks.
            floor_sub_block_count: 0,

            /// The number of "internal" blocks (that have both
            /// terms and sub-blocks).
            mixed_block_count: 0,

            /// The number of "leaf" blocks (blocks that have only
            /// terms).
            terms_only_block_count: 0,

            /// The number of "internal" blocks that do not contain
            /// terms (have only sub-blocks).
            sub_blocks_only_block_count: 0,

            /// Total number of blocks.
            total_block_count: 0,

            /// Number of blocks at each prefix depth.
            block_count_by_prefix_len: vec![0 as i32; 10],
            start_block_count: 0,
            end_block_count: 0,

            /// Total number of bytes used to store term suffixes.
            total_block_suffix_bytes: 0,

            /// Total number of bytes used to store term stats (not
            /// including what the {@link PostingsReaderBase}
            /// stores.
            total_block_stats_bytes: 0,

            /// Total bytes stored by the {@link PostingsReaderBase},
            /// plus the other few vInts stored in the frame.
            total_block_other_bytes: 0,

            /// Segment name.
            segment: String::from(segment),

            /// Field name.
            field: String::from(field),
        }
    }
    pub fn start_block(&mut self, frame: &SegmentTermsIterFrame, is_floor: bool) {
        self.total_block_count += 1;
        if is_floor {
            if frame.fp == frame.fp_orig {
                self.floor_block_count += 1;
            }
            self.floor_sub_block_count += 1;
        } else {
            self.non_floor_block_count += 1;
        }
        if self.block_count_by_prefix_len.len() <= frame.prefix {
            self.block_count_by_prefix_len.resize(frame.prefix + 1, 0);
        }
        self.block_count_by_prefix_len[frame.prefix] += 1;
        self.start_block_count += 1;
        self.total_block_suffix_bytes += frame.suffixes_reader.length() as i64;
        self.total_block_stats_bytes += frame.stats_reader.length() as i64;
    }

    pub fn end_block(&mut self, frame: &SegmentTermsIterFrame) -> Result<()> {
        let term_count = if frame.is_leaf_block {
            frame.ent_count
        } else {
            frame.state.term_block_ord
        };
        let sub_block_count = frame.ent_count - term_count;
        match (term_count, sub_block_count) {
            (0, x) if x > 0 => self.sub_blocks_only_block_count += 1,
            (x, 0) if x > 0 => self.terms_only_block_count += 1,
            (x, y) if x > 0 && y > 0 => self.mixed_block_count += 1,
            (_, _) => bail!(IllegalState(
                "term_count and sub_block_count both be 0".into()
            )),
        }
        self.end_block_count += 1;
        let other_bytes = frame.fp_end
            - frame.fp
            - frame.suffixes_reader.length() as i64
            - frame.stats_reader.length() as i64;
        debug_assert!(other_bytes > 0);
        self.total_block_other_bytes += other_bytes;
        Ok(())
    }

    pub fn index_num_bytes(&self) -> i64 {
        self.index_num_bytes
    }

    pub fn total_term_count(&self) -> i64 {
        self.total_term_count
    }

    pub fn total_term_bytes(&self) -> i64 {
        self.total_term_bytes
    }

    pub fn non_floor_block_count(&self) -> i32 {
        self.non_floor_block_count
    }

    pub fn floor_block_count(&self) -> i32 {
        self.floor_block_count
    }

    pub fn floor_sub_block_count(&self) -> i32 {
        self.floor_sub_block_count
    }

    pub fn mixed_block_count(&self) -> i32 {
        self.mixed_block_count
    }

    pub fn terms_only_block_count(&self) -> i32 {
        self.terms_only_block_count
    }

    pub fn sub_blocks_only_block_count(&self) -> i32 {
        self.sub_blocks_only_block_count
    }

    pub fn total_block_count(&self) -> i32 {
        self.total_block_count
    }

    pub fn block_count_by_prefix_len(&self) -> &[i32] {
        &self.block_count_by_prefix_len
    }
    pub fn start_block_count(&self) -> i32 {
        self.start_block_count
    }
    pub fn end_block_count(&self) -> i32 {
        self.end_block_count
    }

    pub fn total_block_suffix_bytes(&self) -> i64 {
        self.total_block_suffix_bytes
    }

    pub fn total_block_stats_bytes(&self) -> i64 {
        self.total_block_stats_bytes
    }

    pub fn total_block_other_bytes(&self) -> i64 {
        self.total_block_other_bytes
    }

    pub fn segment(&self) -> &str {
        &self.segment
    }

    pub fn field(&self) -> &str {
        &self.field
    }

    pub fn term(&mut self, term: &[u8]) {
        self.total_term_bytes += term.len() as i64
    }

    pub fn finish(&self) {
        debug_assert!(
            self.start_block_count == self.end_block_count,
            "self.start_block_count={} self.end_block_count={}",
            self.start_block_count,
            self.end_block_count
        );
        debug_assert!(
            self.total_block_count == self.floor_sub_block_count + self.non_floor_block_count,
            "self.floor_sub_block_count={} self.non_floor_block_count={} self.total_block_count={}",
            self.floor_sub_block_count,
            self.non_floor_block_count,
            self.total_block_count
        );
        debug_assert!(
            self.total_block_count
                == self.mixed_block_count
                    + self.terms_only_block_count
                    + self.sub_blocks_only_block_count,
            "self.total_block_count={} self.mixed_block_count={} \
             self.sub_blocks_only_block_count={} self.terms_only_block_count={}",
            self.total_block_count,
            self.mixed_block_count,
            self.sub_blocks_only_block_count,
            self.terms_only_block_count
        );
    }

    pub fn to_string(&self) -> Result<String> {
        let mut string = String::with_capacity(1024);
        string.write_fmt(format_args!("  index FST:"))?;
        string.write_fmt(format_args!("    {} bytes", self.index_num_bytes))?;
        string.write_fmt(format_args!("  terms:"))?;
        string.write_fmt(format_args!("    {} terms", self.total_term_count))?;
        let bps = if self.total_term_count != 0 {
            self.total_term_bytes as f64 / self.total_term_count as f64
        } else {
            0.0
        };
        string.write_fmt(format_args!(
            "    {} bytes {} (bytes/term)",
            self.total_term_bytes, bps
        ))?;
        string.write_fmt(format_args!("  blocks:"))?;
        string.write_fmt(format_args!("    {} blocks", self.total_block_count))?;
        string.write_fmt(format_args!(
            "    {} terms-only blocks",
            self.terms_only_block_count
        ))?;
        string.write_fmt(format_args!(
            "    {} sub-block-only blocks",
            self.sub_blocks_only_block_count,
        ))?;
        string.write_fmt(format_args!("    {} mixed blocks", self.mixed_block_count))?;
        string.write_fmt(format_args!("    {} floor blocks", self.floor_block_count))?;
        string.write_fmt(format_args!(
            "    {} non-floor blocks",
            (self.total_block_count - self.floor_sub_block_count),
        ))?;
        string.write_fmt(format_args!(
            "    {} floor sub-blocks",
            self.floor_sub_block_count
        ))?;
        let (bsubps, bstbps, bobps) = if self.total_block_count != 0 {
            let total_block_count = f64::from(self.total_block_count);
            (
                self.total_block_suffix_bytes as f64 / total_block_count,
                self.total_block_stats_bytes as f64 / total_block_count,
                self.total_block_other_bytes as f64 / total_block_count,
            )
        } else {
            (0.0, 0.0, 0.0)
        };
        string.write_fmt(format_args!(
            "    {} term suffix bytes {} (suffix-bytes/block)",
            self.total_block_suffix_bytes, bsubps
        ))?;
        string.write_fmt(format_args!(
            "    {} term stats bytes {} (stats-bytes/block)",
            self.total_block_stats_bytes, bstbps
        ))?;
        string.write_fmt(format_args!(
            "    {} other bytes {} (other-bytes/block)",
            self.total_block_other_bytes, bobps
        ))?;
        if self.total_block_count != 0 {
            string.write_fmt(format_args!("    by prefix length:"))?;
            let mut total = 0;
            for prefix in 0..self.block_count_by_prefix_len.len() {
                let block_count = self.block_count_by_prefix_len[prefix];
                total += block_count;
                if block_count != 0 {
                    string.write_fmt(format_args!("      {}: {}", prefix, block_count))?;
                }
            }
            debug_assert!(self.total_block_count == total);
        }

        Ok(string)
    }
}

impl ToString for Stats {
    fn to_string(&self) -> String {
        Stats::to_string(self).unwrap()
    }
}

/// Iterates through terms in this field.
///
/// This implementation skips any auto-prefix terms it encounters.
pub struct SegmentTermIterator {
    iter: Box<SegmentTermIteratorInner>,
}

impl SegmentTermIterator {
    pub fn new(
        field_reader: &FieldReader,
        terms_in: IndexInputRef,
        postings_reader: Lucene50PostingsReaderRef,
        field_info: Arc<FieldInfo>,
    ) -> Self {
        let iter = Box::new(SegmentTermIteratorInner::new(
            field_reader,
            terms_in,
            postings_reader,
            field_info,
        ));
        Self { iter }
    }
}

impl TermIterator for SegmentTermIterator {
    type Postings = Lucene50PostingIterator;
    type TermState = BlockTermState;

    #[inline]
    fn next(&mut self) -> Result<Option<Vec<u8>>> {
        self.iter.next()
    }

    #[inline]
    fn seek_exact(&mut self, text: &[u8]) -> Result<bool> {
        self.iter.seek_exact(text)
    }

    #[inline]
    fn seek_ceil(&mut self, text: &[u8]) -> Result<SeekStatus> {
        self.iter.seek_ceil(text)
    }

    #[inline]
    fn seek_exact_ord(&mut self, ord: i64) -> Result<()> {
        self.iter.seek_exact_ord(ord)
    }

    #[inline]
    fn seek_exact_state(&mut self, text: &[u8], state: &Self::TermState) -> Result<()> {
        self.iter.seek_exact_state(text, state)
    }

    #[inline]
    fn term(&self) -> Result<&[u8]> {
        Ok(self.iter.term())
    }

    #[inline]
    fn ord(&self) -> Result<i64> {
        self.iter.ord()
    }

    #[inline]
    fn doc_freq(&mut self) -> Result<i32> {
        self.iter.doc_freq()
    }

    #[inline]
    fn total_term_freq(&mut self) -> Result<i64> {
        self.iter.total_term_freq()
    }

    #[inline]
    fn postings(&mut self) -> Result<Self::Postings> {
        self.iter.postings()
    }

    #[inline]
    fn postings_with_flags(&mut self, flags: u16) -> Result<Self::Postings> {
        self.iter.postings_with_flags(flags)
    }

    #[inline]
    fn term_state(&mut self) -> Result<Self::TermState> {
        self.iter.term_state()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.iter.is_empty()
    }
}

pub struct SegmentTermIteratorInner {
    field_info: Arc<FieldInfo>,
    postings_reader: Lucene50PostingsReaderRef,
    pub input: Option<Box<dyn IndexInput>>,
    frame_inited: bool,
    pub stack: Vec<SegmentTermsIterFrame>,
    pub current_frame_ord: usize,
    // index in stack, -1 for static_frame
    terms_in: IndexInputRef,
    fr: *const FieldReader,
    // Lazy init:
    pub term_exists: bool,

    target_before_current_length: usize,
    valid_index_prefix: usize,

    // assert only:
    eof: bool,
    fst_reader: FSTBytesReader,
    arcs: Vec<FSTArc<ByteSequenceOutput>>,

    pub term: Vec<u8>,
    pub term_len: usize,
}

// used for empty fst reader
const EMPTY_BYTES: [u8; 0] = [];

impl SegmentTermIteratorInner {
    fn new(
        field_reader: &FieldReader,
        terms_in: IndexInputRef,
        postings_reader: Lucene50PostingsReaderRef,
        field_info: Arc<FieldInfo>,
    ) -> Self {
        let mut arcs = vec![];
        if let Some(ref index) = field_reader.index {
            arcs.push(index.root_arc());
        } else {
            arcs.push(FSTArc::empty());
        }
        let fst_reader = if let Some(ref index) = field_reader.index {
            index.bytes_reader()
        } else {
            // NOTE: fst reader is always used when self.fr.index is Some,
            // so this will be safe because it will never be used
            FSTBytesReader::Directional(DirectionalBytesReader::new(&EMPTY_BYTES, false))
        };

        // the first item is the static_frame, thus the ord is always plus 1
        let stack = vec![SegmentTermsIterFrame::default()];

        SegmentTermIteratorInner {
            field_info,
            terms_in,
            postings_reader,
            input: None,
            frame_inited: false,
            stack,
            current_frame_ord: 0,
            term: Vec::new(),
            term_len: 0,
            fr: field_reader,
            term_exists: false,
            target_before_current_length: 0,
            valid_index_prefix: 0,
            eof: false,
            fst_reader,
            arcs,
        }
    }

    fn init(&mut self) {
        let iter = self as *mut SegmentTermIteratorInner;
        self.stack[0].init(unsafe { &mut *iter }, 0);
        self.frame_inited = true;
    }

    #[inline]
    pub fn field_reader(&self) -> &FieldReader {
        unsafe { &*self.fr }
    }

    #[inline]
    pub fn term(&self) -> &[u8] {
        &self.term[..self.term_len]
    }

    pub fn init_index_input(&mut self) -> Result<()> {
        if self.input.is_none() {
            self.input = Some((*self.terms_in).clone()?);
        }
        Ok(())
    }

    fn compute_block_stats(&mut self) -> Result<Stats> {
        let mut stats = Stats::new(
            &self.field_reader().parent.segment,
            &self.field_reader().field_info.name,
        );
        if self.field_reader().index.is_some() {
            // stats.index_num_bytes = self.fr.index.unwrap().
        }
        self.current_frame_ord = 0;

        let arc = {
            if let Some(ref fst_reader) = self.field_reader().index {
                Some(fst_reader.root_arc())
            } else {
                None
            }
        };
        let root_code = self.field_reader().root_code().to_vec();
        self.current_frame_ord = self.push_frame_by_data(arc, &root_code, 0)?;
        self.stack[self.current_frame_ord].fp_orig = self.stack[self.current_frame_ord].fp;
        self.stack[self.current_frame_ord].load_block()?;
        self.valid_index_prefix = 0;

        {
            let frame = &mut self.stack[self.current_frame_ord];
            stats.start_block(frame, !frame.is_last_in_floor);
        }

        'all_term: loop {
            while self.stack[self.current_frame_ord].next_ent
                == self.stack[self.current_frame_ord].ent_count
            {
                stats.end_block(&self.stack[self.current_frame_ord])?;
                if !self.stack[self.current_frame_ord].is_last_in_floor {
                    self.stack[self.current_frame_ord].load_next_floor_block()?;
                    stats.start_block(&self.stack[self.current_frame_ord], true);
                    break;
                } else {
                    let ord = self.stack[self.current_frame_ord].ord;
                    if ord == 1 {
                        break 'all_term;
                    }
                    let last_fp = self.stack[self.current_frame_ord].fp_orig;
                    self.current_frame_ord = ord - 1;
                    debug_assert!(last_fp == self.stack[self.current_frame_ord].last_sub_fp);
                }
            }
            loop {
                if self.stack[self.current_frame_ord].next()? {
                    let last_sub_fp = self.stack[self.current_frame_ord].last_sub_fp;
                    let term_len = self.term_len;
                    self.current_frame_ord = self.push_frame_by_fp(None, last_sub_fp, term_len)?;
                    self.stack[self.current_frame_ord].load_block()?;
                    let frame = &mut self.stack[self.current_frame_ord];
                    stats.start_block(frame, !frame.is_last_in_floor);
                } else {
                    stats.term(self.term());
                    break;
                }
            }
        }
        stats.finish();

        self.current_frame_ord = 0;

        let arc = {
            if let Some(ref fst_reader) = self.field_reader().index {
                Some(fst_reader.root_arc())
            } else {
                None
            }
        };
        let root_code = self.field_reader().root_code().to_vec();
        self.current_frame_ord = self.push_frame_by_data(arc, &root_code, 0)?;
        self.stack[self.current_frame_ord].rewind();
        self.stack[self.current_frame_ord].load_block()?;
        self.valid_index_prefix = 0;
        self.term.clear();
        self.term_len = 0;
        Ok(stats)
    }

    fn clear_eof(&mut self) {
        self.eof = false;
    }

    #[allow(dead_code)]
    fn set_eof(&mut self) {
        self.eof = true;
    }

    fn push_frame_by_data(
        &mut self,
        arc: Option<FSTArc<ByteSequenceOutput>>,
        frame_data: &[u8],
        length: usize,
    ) -> Result<usize> {
        let mut scratch_reader = ByteArrayDataInput::new(frame_data);
        let code = scratch_reader.read_vlong()?;
        let fp_seek = code.unsigned_shift(OUTPUT_FLAGS_NUM_BITS);
        let idx = (1 + self.current_frame_ord) as usize;
        let ord = self.get_frame(idx);
        self.stack[ord].has_terms = (code & OUTPUT_FLAGS_HAS_TERMS) != 0;
        self.stack[ord].has_terms_orig = self.stack[ord].has_terms;
        self.stack[ord].is_floor = (code & OUTPUT_FLAGS_IS_FLOOR) != 0;
        if self.stack[ord].is_floor {
            self.stack[ord].set_floor_data(&mut scratch_reader, frame_data)?;
        }
        self.push_frame_by_fp(arc, fp_seek, length)?;
        Ok(ord)
    }

    // Pushes next'd frame or seek'd frame; we later
    // lazy-load the frame only when needed
    pub fn push_frame_by_fp(
        &mut self,
        arc: Option<FSTArc<ByteSequenceOutput>>,
        fp: i64,
        length: usize,
    ) -> Result<usize> {
        let idx = 1 + self.current_frame_ord;
        let ord = self.get_frame(idx);
        self.stack[ord].arc = arc;
        if self.stack[ord].fp_orig == fp && self.stack[ord].next_ent != -1 {
            if self.stack[ord].ord > self.target_before_current_length {
                self.stack[ord].rewind();
            }
            debug_assert_eq!(length, self.stack[ord].prefix);
        } else {
            let frame = &mut self.stack[ord];
            frame.next_ent = -1;
            frame.prefix = length;
            frame.state.term_block_ord = 0;
            frame.fp = fp;
            frame.fp_orig = fp;
            frame.last_sub_fp = -1;
        }

        Ok(ord)
    }

    fn get_frame(&mut self, ord: usize) -> usize {
        if ord >= self.stack.len() {
            for cur in self.stack.len()..=ord {
                let frame = SegmentTermsIterFrame::new(self, cur);
                self.stack.push(frame);
            }
        }
        debug_assert_eq!(self.stack[ord].ord, ord);
        ord
    }

    #[inline]
    pub fn current_frame(&self) -> &SegmentTermsIterFrame {
        &self.stack[self.current_frame_ord]
    }

    fn add_arc(&mut self, arc: FSTArc<ByteSequenceOutput>, index: usize) {
        if index < self.arcs.len() {
            self.arcs[index] = arc;
        } else {
            let cnt = index + 1 - self.arcs.len();
            self.arcs.reserve(cnt);
            for _i in self.arcs.len()..index {
                self.arcs.push(FSTArc::empty());
            }
            self.arcs.push(arc);
        }
    }

    pub fn resize_term(&mut self, len: usize) {
        self.term.resize(len, 0);
        self.term_len = len;
    }
}

impl TermIterator for SegmentTermIteratorInner {
    type Postings = Lucene50PostingIterator;
    type TermState = BlockTermState;
    // Decodes only the term bytes of the next term.  If caller then asks for
    // metadata, ie docFreq, totalTermFreq or pulls a D/&PEnum, we then (lazily)
    // decode all metadata up to the current term.
    fn next(&mut self) -> Result<Option<Vec<u8>>> {
        // fresh iterator, seek to first term
        if !self.frame_inited {
            self.init();
        }
        if self.input.is_none() {
            let arc = {
                if let Some(ref fst_reader) = self.field_reader().index {
                    Some(fst_reader.root_arc())
                } else {
                    None
                }
            };
            let root_code = self.field_reader().root_code().to_vec();
            self.current_frame_ord = self.push_frame_by_data(arc, &root_code, 0)?;
            self.stack[self.current_frame_ord].load_block()?;
        }

        self.target_before_current_length = self.current_frame_ord;
        assert!(!self.eof);

        if self.current_frame_ord == self.stack[0].ord {
            // If seek was previously called and the term was
            // cached, or seek(TermState) was called, usually
            // caller is just going to pull a D/&PEnum or get
            // docFreq, etc.  But, if they then call next(),
            // this method catches up all internal state so next()
            // works properly:
            let term = self.term[..self.term_len].to_vec();
            let result = self.seek_exact(&term)?;
            debug_assert!(result);
        }

        // Pop finished blocks:
        debug_assert!(self.current_frame_ord > 0);
        let mut current_idx = self.current_frame_ord;
        while self.stack[current_idx].next_ent == self.stack[current_idx].ent_count {
            if !self.stack[current_idx].is_last_in_floor {
                // Advance to next floor block
                self.stack[current_idx].load_next_floor_block()?;
                break;
            } else {
                if current_idx == 1 {
                    self.eof = true;
                    self.term.clear();
                    self.term_len = 0;
                    self.valid_index_prefix = 0;
                    self.stack[1].rewind();
                    self.term_exists = false;
                    return Ok(None);
                }

                let last_fp = self.stack[current_idx].fp_orig;
                self.current_frame_ord -= 1;
                current_idx -= 1;

                if self.stack[current_idx].next_ent == -1
                    || self.stack[current_idx].last_sub_fp != last_fp
                {
                    // We popped into a frame that's not loaded
                    // yet or not scan'd to the right entry
                    self.stack[current_idx].scan_to_floor_frame(&self.term[..self.term_len])?;
                    self.stack[current_idx].load_block()?;
                    self.stack[current_idx].scan_to_sub_block(last_fp)?;
                }

                // Note that the seek state (last seek) has been
                // invalidated beyond this depth
                self.valid_index_prefix =
                    self.valid_index_prefix.min(self.stack[current_idx].prefix);
            }
        }

        loop {
            if self.stack[self.current_frame_ord].next()? {
                // Push to new block:
                let fp = self.stack[self.current_frame_ord as usize].last_sub_fp;
                let term_len = self.term_len;
                self.current_frame_ord = self.push_frame_by_fp(None, fp, term_len)?;
                // This is a "next" frame -- even if it's
                // floor'd we must pretend it isn't so we don't
                // try to scan to the right floor frame:
                self.stack[self.current_frame_ord].load_block()?;
            } else {
                return Ok(Some(self.term().to_vec()));
            }
        }
    }

    fn seek_exact(&mut self, target: &[u8]) -> Result<bool> {
        if self.term.len() < target.len() {
            self.term.resize(target.len(), 0);
        }
        // self.term.copy_from_slice(target);
        let outputs = ByteSequenceOutputFactory::new();

        self.clear_eof();
        let mut arc_idx = 0;
        // debug_assert!(self.arcs[arc_idx].is_final());
        let mut output;
        let mut target_upto;
        self.target_before_current_length = self.current_frame_ord;
        if self.current_frame_ord != self.stack[0].ord {
            // We are already seek'd; find the common
            // prefix of new seek term vs current term and
            // re-use the corresponding seek state.  For
            // example, if app first seeks to foobar, then
            // seeks to foobaz, we can re-use the seek state
            // for the first 5 bytes.
            output = self.arcs[arc_idx]
                .output
                .clone()
                .unwrap_or_else(|| outputs.empty());
            target_upto = 0;
            let mut last_frame_idx = 1;
            debug_assert!(self.valid_index_prefix <= self.term_len);
            let target_limit = target.len().min(self.valid_index_prefix);

            let mut cmp = Ordering::Equal;
            // TODO: reverse vLong byte order for better FST
            // prefix output sharing

            // First compare up to valid seek frames:
            while target_upto < target_limit {
                cmp = self.term[target_upto].cmp(&target[target_upto]);
                if cmp != Ordering::Equal {
                    break;
                }
                arc_idx = target_upto + 1;
                debug_assert_eq!(self.arcs[arc_idx].label, target[target_upto] as u32 as i32);
                if let Some(ref out) = self.arcs[arc_idx].output {
                    output = outputs.add(&output, out);
                }
                if self.arcs[arc_idx].is_final() {
                    last_frame_idx += 1;
                }
                target_upto += 1;
            }

            if cmp == Ordering::Equal {
                let target_upto_mid = target_upto;

                // Second compare the rest of the term, but
                // don't save arc/output/frame; we only do this
                // to find out if the target term is before,
                // equal or after the current term
                let target_limit2 = target.len().min(self.term_len);
                while target_upto < target_limit2 {
                    cmp = self.term[target_upto].cmp(&target[target_upto]);
                    if cmp != Ordering::Equal {
                        break;
                    }
                    target_upto += 1;
                }

                if cmp == Ordering::Equal {
                    cmp = self.term_len.cmp(&target.len());
                }
                target_upto = target_upto_mid;
            }

            match cmp {
                Ordering::Less => {
                    // Common case: target term is after current
                    // term, ie, app is seeking multiple terms
                    // in sorted order
                    self.current_frame_ord = last_frame_idx;
                }
                Ordering::Greater => {
                    // Uncommon case: target term
                    // is before current term; this means we can
                    // keep the currentFrame but we must rewind it
                    // (so we scan from the start)
                    self.target_before_current_length = last_frame_idx;
                    self.current_frame_ord = last_frame_idx;
                    self.stack[self.current_frame_ord].rewind();
                }
                Ordering::Equal => {
                    // Target is exactly the same as current term
                    debug_assert_eq!(self.term_len, target.len());
                    if self.term_exists {
                        return Ok(true);
                    }
                }
            }
        } else {
            self.target_before_current_length = 0;
            let arc = self.field_reader().index.as_ref().unwrap().root_arc();
            self.arcs[0] = arc;
            arc_idx = 0;

            // Empty string prefix must have an output (block) in the index!
            debug_assert!(self.arcs[0].is_final());
            debug_assert!(self.arcs[0].output.is_some());

            output = self.arcs[0].output.clone().unwrap();
            self.current_frame_ord = self.stack[0].ord;
            target_upto = 0;
            let cur_output = if let Some(ref out) = self.arcs[0].next_final_output {
                outputs.add(&output, out)
            } else {
                output.clone()
            };
            let arc = Some(self.arcs[0].clone());
            let idx = self.push_frame_by_data(arc, cur_output.inner(), 0)?;
            self.current_frame_ord = idx;
        }

        // We are done sharing the common prefix with the incoming target and where we are
        // currently seek'd; now continue walking the index:
        while target_upto < target.len() {
            let target_label = target[target_upto] as u32 as i32;
            if let Some(next_arc) = unsafe {
                (*self.fr).index().find_target_arc(
                    target_label,
                    &self.arcs[arc_idx],
                    &mut self.fst_reader,
                )?
            } {
                self.term[target_upto] = target_label as u8;
                if let Some(ref out) = next_arc.output {
                    if !out.is_empty() {
                        output = outputs.add(&output, out);
                    }
                }
                target_upto += 1;
                if next_arc.is_final() {
                    let cur_output = if let Some(ref out) = next_arc.next_final_output {
                        outputs.add(&output, out)
                    } else {
                        output.clone()
                    };
                    let idx = self.push_frame_by_data(
                        Some(next_arc.clone()),
                        cur_output.inner(),
                        target_upto,
                    )?;
                    self.current_frame_ord = idx;
                }
                self.add_arc(next_arc, target_upto);
                arc_idx = target_upto;
            } else {
                // Index is exhausted
                debug_assert!(self.current_frame_ord > 0);
                self.valid_index_prefix = self.stack[self.current_frame_ord as usize].prefix;
                self.stack[self.current_frame_ord].scan_to_floor_frame(target)?;
                if !self.stack[self.current_frame_ord].has_terms {
                    self.term_exists = false;
                    self.term[target_upto] = target_label as u8;
                    self.term.truncate(target_upto + 1);
                    self.term_len = target_upto + 1;
                    return Ok(false);
                }
                self.stack[self.current_frame_ord].load_block()?;

                let result = self.stack[self.current_frame_ord].scan_to_term(target, true)?;
                return Ok(result == SeekStatus::Found);
            }
        }

        self.valid_index_prefix = self.stack[self.current_frame_ord].prefix;

        self.stack[self.current_frame_ord].scan_to_floor_frame(target)?;

        // Target term is entirely contained in the index:
        if !self.stack[self.current_frame_ord].has_terms {
            self.term_exists = false;
            self.term.truncate(target_upto);
            self.term_len = target_upto;
            return Ok(false);
        }

        self.stack[self.current_frame_ord].load_block()?;
        let result = self.stack[self.current_frame_ord].scan_to_term(target, true)?;
        Ok(result == SeekStatus::Found)
    }

    fn seek_ceil(&mut self, target: &[u8]) -> Result<SeekStatus> {
        if self.field_reader().index.is_none() {
            bail!(IllegalState("terms index was not loaded".into()));
        }

        if target.len() > self.term.len() {
            self.term.resize(target.len(), 0);
        }

        let outputs = ByteSequenceOutputFactory::new();

        self.clear_eof();
        let mut arc_idx = 0;
        // debug_assert!(self.arcs[arc_idx].is_final());
        let mut output;
        let mut target_upto;
        self.target_before_current_length = self.current_frame_ord;
        if self.current_frame_ord != self.stack[0].ord {
            // We are already seek'd; find the common
            // prefix of new seek term vs current term and
            // re-use the corresponding seek state.  For
            // example, if app first seeks to foobar, then
            // seeks to foobaz, we can re-use the seek state
            // for the first 5 bytes.
            output = self.arcs[arc_idx]
                .output
                .clone()
                .unwrap_or_else(|| outputs.empty());
            target_upto = 0;
            let mut last_frame_idx = 1;
            debug_assert!(self.valid_index_prefix <= self.term_len);
            let target_limit = target.len().min(self.valid_index_prefix);

            let mut cmp = Ordering::Equal;
            // TODO: reverse vLong byte order for better FST
            // prefix output sharing

            // First compare up to valid seek frames:
            while target_upto < target_limit {
                cmp = self.term[target_upto].cmp(&target[target_upto]);
                if cmp != Ordering::Equal {
                    break;
                }
                arc_idx = target_upto + 1;
                debug_assert_eq!(self.arcs[arc_idx].label, target[target_upto] as u32 as i32);
                if let Some(ref out) = self.arcs[arc_idx].output {
                    output = outputs.add(&output, out);
                }
                if self.arcs[arc_idx].is_final() {
                    last_frame_idx += 1;
                }
                target_upto += 1;
            }

            if cmp == Ordering::Equal {
                let target_upto_mid = target_upto;

                // Second compare the rest of the term, but
                // don't save arc/output/frame:
                let target_limit2 = target.len().min(self.term_len);
                while target_upto < target_limit2 {
                    cmp = self.term[target_upto].cmp(&target[target_upto]);
                    if cmp != Ordering::Equal {
                        break;
                    }
                    target_upto += 1;
                }

                if cmp == Ordering::Equal {
                    cmp = self.term_len.cmp(&target.len());
                }
                target_upto = target_upto_mid;
            }

            match cmp {
                Ordering::Less => {
                    // Common case: target term is after current
                    // term, ie, app is seeking multiple terms
                    // in sorted order
                    self.current_frame_ord = last_frame_idx;
                }
                Ordering::Greater => {
                    // Uncommon case: target term
                    // is before current term; this means we can
                    // keep the currentFrame but we must rewind it
                    // (so we scan from the start)
                    self.target_before_current_length = 1;
                    self.current_frame_ord = last_frame_idx;
                    self.stack[self.current_frame_ord].rewind();
                }
                Ordering::Equal => {
                    // Target is exactly the same as current term
                    debug_assert_eq!(self.term_len, target.len());
                    if self.term_exists {
                        return Ok(SeekStatus::Found);
                    }
                }
            }
        } else {
            self.target_before_current_length = 0;
            let arc = self.field_reader().index.as_ref().unwrap().root_arc();
            self.arcs[0] = arc;
            arc_idx = 0;

            // Empty string prefix must have an output (block) in the index!
            debug_assert!(self.arcs[0].is_final());
            debug_assert!(self.arcs[0].output.is_some());

            output = self.arcs[0].output.clone().unwrap();
            self.current_frame_ord = self.stack[0].ord;
            target_upto = 0;
            let cur_output = if let Some(ref out) = self.arcs[0].next_final_output {
                outputs.add(&output, out)
            } else {
                output.clone()
            };
            let arc = Some(self.arcs[0].clone());
            let idx = self.push_frame_by_data(arc, cur_output.inner(), 0)?;
            self.current_frame_ord = idx;
        }

        // We are done sharing the common prefix with the incoming target and where we are
        // currently seek'd; now continue walking the index:
        while target_upto < target.len() {
            let target_label = target[target_upto] as u32 as i32;
            if let Some(next_arc) = unsafe {
                (*self.fr).index().find_target_arc(
                    target_label,
                    &self.arcs[arc_idx],
                    &mut self.fst_reader,
                )?
            } {
                self.term[target_upto] = target_label as u8;
                if let Some(ref out) = next_arc.output {
                    if !out.is_empty() {
                        output = outputs.add(&output, out);
                    }
                }
                target_upto += 1;
                if next_arc.is_final() {
                    let cur_output = if let Some(ref out) = next_arc.next_final_output {
                        outputs.add(&output, out)
                    } else {
                        output.clone()
                    };
                    let idx = self.push_frame_by_data(
                        Some(next_arc.clone()),
                        cur_output.inner(),
                        target_upto,
                    )?;
                    self.current_frame_ord = idx;
                }
                self.add_arc(next_arc, target_upto);
                arc_idx = target_upto;
            } else {
                // Index is exhausted
                debug_assert!(self.current_frame_ord > 0);
                self.valid_index_prefix = self.stack[self.current_frame_ord as usize].prefix;
                self.stack[self.current_frame_ord as usize].scan_to_floor_frame(target)?;
                self.stack[self.current_frame_ord as usize].load_block()?;
                let result =
                    self.stack[self.current_frame_ord as usize].scan_to_term(target, false)?;
                if result == SeekStatus::End {
                    self.term.resize(target.len(), 0);
                    self.term.copy_from_slice(target);
                    self.term_len = target.len();
                    self.term_exists = false;
                    if self.next()?.is_some() {
                        return Ok(SeekStatus::NotFound);
                    } else {
                        return Ok(SeekStatus::End);
                    }
                } else {
                    return Ok(result);
                }
            }
        }

        self.valid_index_prefix = self.stack[self.current_frame_ord].prefix;

        self.stack[self.current_frame_ord].scan_to_floor_frame(target)?;

        self.stack[self.current_frame_ord].load_block()?;
        let result = self.stack[self.current_frame_ord].scan_to_term(target, false)?;

        if result == SeekStatus::End {
            self.term.resize(target.len(), 0);
            self.term.copy_from_slice(target);
            self.term_len = target.len();
            self.term_exists = false;
            if self.next()?.is_some() {
                Ok(SeekStatus::NotFound)
            } else {
                Ok(SeekStatus::End)
            }
        } else {
            Ok(result)
        }
    }

    fn seek_exact_ord(&mut self, _ord: i64) -> Result<()> {
        unimplemented!()
    }

    fn seek_exact_state(&mut self, text: &[u8], state: &Self::TermState) -> Result<()> {
        self.clear_eof();
        if text != self.term() || !self.term_exists {
            self.current_frame_ord = self.stack[0].ord;
            self.stack[0].state.copy_from(state);
            self.resize_term(text.len());
            self.term.copy_from_slice(text);
            self.stack[0].metadata_upto = self.stack[0].get_term_block_ord();
            debug_assert!(self.stack[0].metadata_upto > 0);
            self.valid_index_prefix = 0;
        }

        Ok(())
    }

    fn term(&self) -> Result<&[u8]> {
        Ok(&self.term[..self.term_len])
    }

    fn ord(&self) -> Result<i64> {
        bail!(UnsupportedOperation(Cow::Borrowed("")))
    }

    fn doc_freq(&mut self) -> Result<i32> {
        debug_assert!(!self.eof);
        self.stack[self.current_frame_ord].decode_metadata()?;
        Ok(self.stack[self.current_frame_ord].state.doc_freq)
    }

    fn total_term_freq(&mut self) -> Result<i64> {
        debug_assert!(!self.eof);
        self.stack[self.current_frame_ord].decode_metadata()?;
        Ok(self.stack[self.current_frame_ord].state.total_term_freq)
    }

    fn postings(&mut self) -> Result<Self::Postings> {
        self.postings_with_flags(0)
    }

    fn postings_with_flags(&mut self, flags: u16) -> Result<Self::Postings> {
        debug_assert!(!self.eof);
        self.stack[self.current_frame_ord].decode_metadata()?;
        self.postings_reader.postings(
            self.field_info.as_ref(),
            &self.stack[self.current_frame_ord].state,
            flags,
        )
    }

    fn term_state(&mut self) -> Result<Self::TermState> {
        self.stack[self.current_frame_ord].decode_metadata()?;
        Ok(self.stack[self.current_frame_ord].state.clone())
    }
}
