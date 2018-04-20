use std::collections::btree_map::Keys;
use std::collections::BTreeMap;
use std::fmt::Write;
use std::io::Read;
use std::ops::DerefMut;
use std::string::ToString;
use std::sync::Arc;

use core::codec::codec_util;
use core::codec::lucene50::{lucene50_decode_term, Lucene50PostingsReader,
                            Lucene50PostingsReaderRef};
use core::codec::BlockTermState;
use core::codec::{FieldsProducer, FieldsProducerRef};
use core::index::segment_file_name;
use core::index::term::TermState;
use core::index::term::{SeekStatus, TermIterator, Terms, TermsRef};
use core::index::{FieldInfo, FieldInfoRef, Fields};
use core::index::{IndexOptions, SegmentReadState};
use core::search::posting_iterator::PostingIterator;
use core::store::DataInput;
use core::store::{ByteBufferIndexInput, IndexInput};
use core::util::bit_util::UnsignedShift;
use core::util::fst::{ByteSequenceOutputFactory, FST};
use error::Result;

const OUTPUT_FLAGS_NUM_BITS: usize = 2;
// const OUTPUT_FLAGS_MASK: i32 = 0x3;
const OUTPUT_FLAGS_IS_FLOOR: i64 = 0x1;
const OUTPUT_FLAGS_HAS_TERMS: i64 = 0x2;

/// Extension of terms file
const TERMS_EXTENSION: &str = "tim";
const TERMS_CODEC_NAME: &str = "BlockTreeTermsDict";

/// Initial terms format.
const VERSION_START: i32 = 0;

/// Auto-prefix terms.
const VERSION_AUTO_PREFIX_TERMS: i32 = 1;

/// Conditional auto-prefix terms: we record at write time whether
/// this field did write any auto-prefix terms.
/// const VERSION_AUTO_PREFIX_TERMS_COND: i32 = 2;

/// Auto-prefix terms have been superseded by points.
const VERSION_AUTO_PREFIX_TERMS_REMOVED: i32 = 3;

/// Current terms format.
const VERSION_CURRENT: i32 = VERSION_AUTO_PREFIX_TERMS_REMOVED;

/// Extension of terms index file
const TERMS_INDEX_EXTENSION: &str = "tip";
const TERMS_INDEX_CODEC_NAME: &str = "BlockTreeTermsIndex";

type IndexInputRef = Arc<IndexInput>;

/// A block-based terms index and dictionary that assigns
/// terms to variable length blocks according to how they
/// share prefixes.  The terms index is a prefix trie
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
/// option to see summary statistics on the blocks in the
/// dictionary.
///
/// See {@link BlockTreeTermsWriter}.
///
/// @lucene.experimental
///
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
    pub fn new(
        postings_reader: Lucene50PostingsReader,
        state: &SegmentReadState,
    ) -> Result<BlockTreeTermsReader> {
        let segment = Arc::new(state.segment_info.name.clone());
        let terms_name = segment_file_name(&segment, &state.segment_suffix, TERMS_EXTENSION);
        let mut terms_in = state.directory.open_input(&terms_name, &state.context)?;
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
                b => bail!(
                    "invalid any_auto_prefix_terms: expected 0 or 1 but got {}",
                    b
                ),
            }
        };

        let index_name = segment_file_name(&segment, &state.segment_suffix, TERMS_INDEX_EXTENSION);
        let mut index_in = state.directory.open_input(&index_name, &state.context)?;
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
            bail!("invalid num_fields: {}", num_fields);
        }

        let fields = {
            let mut fields = BTreeMap::new();

            for _ in 0..num_fields as usize {
                let field = terms_in.read_vint()?;
                let num_terms = terms_in.read_vlong()?;
                if num_terms <= 0 {
                    bail!("Illegal numTerms for field number: {}", field);
                }
                let num_bytes = terms_in.read_vint()?;
                if num_bytes < 0 {
                    bail!(
                        "invalid rootCode for field number: {}, num_bytes={}",
                        field,
                        num_bytes
                    );
                }
                let mut root_code = vec![0 as u8; num_bytes as usize];
                terms_in.read_exact(&mut root_code)?;
                let field_info = state.field_infos.by_number.get(&field);
                if field_info.is_none() {
                    bail!("invalid field number: {}", field);
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
                    bail!(
                        "invalid longsSize for field: {}, longsSize={}",
                        field_info.name,
                        longs_size
                    );
                }
                let min_term = Self::read_bytes(terms_in.deref_mut())?;
                let max_term = Self::read_bytes(terms_in.deref_mut())?;
                if doc_count < 0 || doc_count > state.segment_info.max_doc {
                    // #docs with field must be <= #docs
                    bail!(
                        "invalid docCount: {} maxDoc: {}",
                        doc_count,
                        state.segment_info.max_doc
                    );
                }
                if sum_doc_freq < i64::from(doc_count) {
                    // #postings must be >= #docs with field
                    bail!(
                        "invalid sumDocFreq: {} docCount: {}",
                        sum_doc_freq,
                        doc_count
                    );
                }
                if sum_total_term_freq != -1 && sum_total_term_freq < sum_doc_freq {
                    // #positions must be >= #postings
                    bail!(
                        "invalid sumTotalTermFreq: {} sumDocFreq: {}",
                        sum_total_term_freq,
                        sum_doc_freq
                    );
                }
                let index_start_fp = index_in.read_vlong()?;
                if fields.contains_key(&field_info.name) {
                    bail!("duplicated field: {}", field_info.name);
                }
                let terms_in = Arc::from(terms_in.clone()?);
                let mut reader = Arc::new(FieldReader::new(
                    segment.clone(),
                    field_info.clone(),
                    num_terms,
                    root_code,
                    sum_total_term_freq,
                    sum_doc_freq,
                    doc_count,
                    index_start_fp,
                    longs_size,
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

        let terms_in = Arc::from(terms_in);
        Ok(BlockTreeTermsReader {
            terms_in,
            postings_reader: postings_reader.clone(),
            fields,
            segment,
            version,
            any_auto_prefix_terms,
            dir_offset: 0,
            index_dir_offset: 0,
        })
    }

    fn read_bytes(input: &mut IndexInput) -> Result<Vec<u8>> {
        let len = input.read_vint()? as usize;
        let mut vec = vec![0 as u8; len];
        input.read_exact(&mut vec)?;
        Ok(vec)
    }

    /// Seek {@code input} to the directory offset.
    fn seek_dir(input: &mut IndexInput, _dir_offset: i64) -> Result<()> {
        // TODO double check this in lucene code
        let offset = input.len() as i64 - codec_util::footer_length() as i64 - 8;
        input.seek(offset)?;
        let dir_offset = input.read_long()?;
        input.seek(dir_offset)
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

    fn check_integrity(&self) -> Result<()> {
        let mut input = (*self.terms_in).clone()?;
        codec_util::checksum_entire_file(input.as_mut())?;
        self.postings_reader.check_integrity()
    }

    fn get_merge_instance(&self) -> Result<FieldsProducerRef> {
        unimplemented!()
    }
}

impl FieldsProducer for BlockTreeTermsReader {
    fn check_integrity(&self) -> Result<()> {
        self.check_integrity()
    }

    fn get_merge_instance(&self) -> Result<FieldsProducerRef> {
        self.get_merge_instance()
    }
}

impl Fields for BlockTreeTermsReader {
    fn fields(&self) -> Vec<String> {
        self.fields.keys().cloned().collect()
    }

    fn terms(&self, field: &str) -> Result<Option<TermsRef>> {
        if let Some(reader) = self.fields.get(field) {
            Ok(Some(reader.clone()))
        } else {
            Ok(None)
        }
    }

    fn size(&self) -> usize {
        self.fields.len()
    }
}

type FSTRef = Arc<FST<ByteSequenceOutputFactory>>;

pub struct FieldReader {
    segment: Arc<String>,
    num_terms: i64,
    field_info: FieldInfoRef,
    sum_total_term_freq: i64,
    sum_doc_freq: i64,
    doc_count: i32,
    index_start_fp: i64,
    root_block_fp: i64,
    root_code: Vec<u8>,
    min_term: Vec<u8>,
    max_term: Vec<u8>,
    longs_size: i32,
    index: Option<FSTRef>,
    terms_in: IndexInputRef,
    postings_reader: Lucene50PostingsReaderRef,
}

type FieldReaderRef = Arc<FieldReader>;

impl FieldReader {
    #[allow(too_many_arguments)]
    pub fn new(
        segment: Arc<String>,
        field_info: FieldInfoRef,
        num_terms: i64,
        root_code: Vec<u8>,
        sum_total_term_freq: i64,
        sum_doc_freq: i64,
        doc_count: i32,
        index_start_fp: i64,
        longs_size: i32,
        index_in: Option<&mut IndexInput>,
        min_term: Vec<u8>,
        max_term: Vec<u8>,
        terms_in: IndexInputRef,
        postings_reader: Lucene50PostingsReaderRef,
    ) -> Result<FieldReader> {
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
            segment,
            field_info,
            num_terms,
            root_code,
            sum_total_term_freq,
            sum_doc_freq,
            doc_count,
            index_start_fp,
            root_block_fp,
            min_term,
            max_term,
            longs_size,
            index,
            terms_in,
            postings_reader,
        })
    }

    pub fn index_start_fp(&self) -> i64 {
        self.index_start_fp
    }

    pub fn root_block_fp(&self) -> i64 {
        self.root_block_fp
    }

    pub fn root_code(&self) -> &[u8] {
        &self.root_code
    }

    pub fn field_info(&self) -> &FieldInfo {
        self.field_info.as_ref()
    }
}

impl<'a> Terms for FieldReader {
    fn min(&self) -> Result<Vec<u8>> {
        Ok(self.min_term.clone())
    }

    fn max(&self) -> Result<Vec<u8>> {
        Ok(self.max_term.clone())
    }

    fn stats(&self) -> Result<String> {
        unimplemented!()
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

    fn iterator(&self) -> Result<Box<TermIterator>> {
        let field_info = self.field_info.clone();
        debug_assert!(self.index.is_some());
        let fst = self.index.as_ref().cloned().unwrap();
        let postings_reader = self.postings_reader.clone();
        let terms_in = self.terms_in.clone();

        Ok(Box::new(SegmentTermIterator::new(
            self,
            terms_in,
            postings_reader,
            fst,
            field_info,
        )))
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
                == self.mixed_block_count + self.terms_only_block_count
                    + self.sub_blocks_only_block_count,
            "self.total_block_count={} self.mixed_block_count={} \
             self.sub_blocks_only_block_count={} self.terms_only_block_count={}",
            self.total_block_count,
            self.mixed_block_count,
            self.sub_blocks_only_block_count,
            self.terms_only_block_count
        );
    }

    fn to_string(&self) -> Result<String> {
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

pub struct SegmentTermIterator {
    field_info: Arc<FieldInfo>,
    fst: FSTRef,
    postings_reader: Lucene50PostingsReaderRef,
    input: Option<Box<IndexInput>>,
    terms_in: IndexInputRef,
    // Lazy init:
    term_exists: bool,

    #[allow(dead_code)]
    ord: i32,
    #[allow(dead_code)]
    target_before_current_length: i32,
    #[allow(dead_code)]
    valid_index_prefix: i32,

    // assert only:
    eof: bool,

    term: Vec<u8>,

    // term state
    scratch_reader: ByteBufferIndexInput,
    suffix_reader: ByteBufferIndexInput,
    stats_reader: ByteBufferIndexInput,
    bytes_reader: ByteBufferIndexInput,

    floor_reader: Option<ByteBufferIndexInput>,

    has_terms: bool,
    #[allow(dead_code)]
    has_terms_orig: bool,
    is_floor: bool,

    version_auto_prefix: bool,

    // File pointer where this block was loaded from
    fp: i64,
    fp_orig: i64,
    fp_end: i64,

    // Length of prefix shared by all terms in this block
    prefix: i32,

    // Number of entries (term or sub-block) in this block
    ent_count: i32,

    // Which term we will next read, or -1 if the block
    // isn't loaded yet
    next_ent: i32,

    // True if this block is either not a floor block,
    // or, it's the last sub-block of a floor block
    is_last_in_floor: bool,

    // True if all entries are terms
    is_leaf_block: bool,

    last_sub_fp: i64,

    next_floor_label: i32,
    num_follow_floor_blocks: i32,

    // Next term to decode metaData, we decode metaData
    // lazily so that scanning to find the matching term is
    // fast and only if you find a match and app wants the
    // stats or docs/positions enums, will we decode the
    // metaData
    meta_data_upto: i32,

    // metadata buffer, holding monotonic values
    longs: Vec<i64>,

    start_byte_pos: i32,
    suffix: i32,
    sub_code: i64,

    state: BlockTermState,

    segment: Arc<String>,
}

impl<'a> SegmentTermIterator {
    pub fn new(
        field_reader: &FieldReader,
        terms_in: IndexInputRef,
        postings_reader: Lucene50PostingsReaderRef,
        fst: FSTRef,
        field_info: FieldInfoRef,
    ) -> SegmentTermIterator {
        // frame related
        let longs = vec![0 as i64; field_reader.longs_size as usize];
        // TODO
        let version_auto_prefix = false;
        let scratch_reader = ByteBufferIndexInput::with_capacity(0);
        let suffix_reader = ByteBufferIndexInput::with_capacity(128);
        let stats_reader = ByteBufferIndexInput::with_capacity(64);
        let bytes_reader = ByteBufferIndexInput::with_capacity(32);
        let state = BlockTermState::new();
        let segment = field_reader.segment.clone();

        SegmentTermIterator {
            field_info,
            terms_in,
            fst,
            postings_reader,
            input: None,
            term: Vec::new(),
            term_exists: false,
            ord: 0,
            target_before_current_length: 0,
            valid_index_prefix: 0,
            eof: false,

            // term state related
            scratch_reader,
            suffix_reader,
            stats_reader,
            bytes_reader,

            longs,
            version_auto_prefix,
            has_terms: false,
            has_terms_orig: false,
            is_floor: false,
            fp: 0,
            fp_orig: 0,
            fp_end: 0,
            prefix: 0,
            ent_count: 0,
            next_ent: 0,
            is_last_in_floor: false,
            is_leaf_block: false,
            last_sub_fp: 0,
            meta_data_upto: 0,
            floor_reader: None,
            next_floor_label: 0,
            num_follow_floor_blocks: 0,
            start_byte_pos: 0,
            suffix: 0,
            sub_code: 0,
            state,
            segment,
        }
    }

    pub fn is_term_exists(&self) -> bool {
        self.term_exists
    }

    pub fn term(&self) -> &[u8] {
        &self.term
    }

    pub fn term_mut(&mut self) -> &mut [u8] {
        &mut self.term
    }

    fn init_index_input(&mut self) -> Result<()> {
        if self.input.is_none() {
            self.input = Some((*self.terms_in).clone()?);
        }
        Ok(())
    }

    #[allow(dead_code)]
    fn compute_block_stats(&self) -> Result<Stats> {
        // let field_reader = self.field_reader.lock()?;
        // let parent = field_reader.parent();
        // let block_reader = parent.lock()?;
        // Ok(Stats::new(block_reader.segment(), self.field_info.name()))
        unimplemented!()
    }

    fn clear_eof(&mut self) {
        self.eof = false;
    }

    #[allow(dead_code)]
    fn set_eof(&mut self) {
        self.eof = true;
    }

    fn decode_meta_data(&mut self) -> Result<BlockTermState> {
        let limit = if self.is_leaf_block {
            self.next_ent
        } else {
            self.state.term_block_ord
        };
        let mut absolute = self.meta_data_upto == 0;
        debug_assert!(limit > 0);

        loop {
            if self.meta_data_upto >= limit {
                break;
            }
            // TODO: we could make "tiers" of metadata, ie,
            // decode docFreq/totalTF but don't decode postings
            // metadata; this way caller could get
            // docFreq/totalTF w/o paying decode cost for
            // postings

            // TODO: if docFreq were bulk decoded we could
            // just skipN here:

            // stats
            self.state.doc_freq = self.stats_reader.read_vint()?;
            self.state.total_term_freq = match self.field_info.index_options {
                IndexOptions::Docs => -1,
                _ => i64::from(self.state.doc_freq) + self.stats_reader.read_vlong()?,
            };
            unsafe {
                let ptr = self.longs.as_mut_ptr();
                for i in 0..self.longs.len() {
                    *ptr.offset(i as isize) = self.bytes_reader.read_vlong()?;
                }
            }
            lucene50_decode_term(
                &self.longs,
                &mut self.bytes_reader,
                self.field_info.as_ref(),
                &mut self.state,
                absolute,
            )?;

            self.meta_data_upto += 1;
            absolute = false;
        }
        self.state.term_block_ord = self.meta_data_upto;
        Ok(self.state.clone())
    }

    // pub fn set_floor_data(&mut self) -> Result<()> {
    // let field_reader = self.field_reader;
    // let source = field_reader.root_code();
    // let input = &mut self.scratch_reader;
    // let (_, mut source) = source.split_at(input.position());
    // self.floor_buffer.reload(&mut source);
    // self.num_follow_floor_blocks = self.floor_buffer.read_vint()?;
    // self.next_floor_label = self.floor_buffer.read_byte()? as i32;
    // Ok(())
    // }
    //
    // pub fn rewind(&mut self) -> Result<()> {
    // Force reload:
    // self.fp = self.fp_orig;
    // self.next_ent = -1;
    // self.has_terms = self.has_terms_orig;
    // if self.is_floor {
    // self.num_follow_floor_blocks = self.floor_buffer.read_vint()?;
    // self.next_floor_label = self.floor_buffer.read_byte()? as i32 & 0xff;
    // }
    // Ok(())
    // }
    //
    // fn load_frame(&mut self) -> Result<()> {
    // {
    // let field_reader = self.field_reader;
    // let frame_data = field_reader.root_code();
    // self.scratch_reader.reload_slice(frame_data)?;
    // }
    // let code = self.scratch_reader.read_vlong()?;
    // let fp_seek = (code as u64 >> OUTPUT_FLAGS_NUM_BITS) as i64;
    // self.has_terms = (code & OUTPUT_FLAGS_HAS_TERMS) != 0;
    // self.has_terms_orig = self.has_terms;
    // self.is_floor = (code & OUTPUT_FLAGS_IS_FLOOR) != 0;
    // if self.is_floor {
    // self.set_floor_data()?;
    // }
    // let length = 0;
    // if self.fp_orig == fp_seek && self.next_ent != -1 {
    // if self.ord > self.target_before_current_length {
    // self.rewind();
    // }
    // assert!(length == self.prefix);
    // } else {
    // self.next_ent = -1;
    // self.prefix = length;
    // self.state.term_block_ord = 0;
    // self.fp_orig = fp_seek;
    // self.fp = fp_seek;
    // self.last_sub_fp = -1;
    // }
    // Ok(())
    // }
    //

    fn scan_to_term(&mut self, target: &[u8], exact_only: bool) -> Result<SeekStatus> {
        if self.is_leaf_block {
            self.scan_to_term_leaf(target, exact_only)
        } else {
            self.scan_to_term_non_leaf(target, exact_only)
        }
    }

    // Target's prefix matches this block's prefix; we
    // scan the entries check if the suffix matches.
    fn scan_to_term_leaf(&mut self, target: &[u8], exact_only: bool) -> Result<SeekStatus> {
        self.term_exists = true;
        self.sub_code = 0;

        if self.next_ent == self.ent_count {
            if exact_only {
                self.fill_term()?;
            }
            return Ok(SeekStatus::End);
        }

        // Loop over each entry (term or sub-block) in this block:
        // nextTerm: while(nextEnt < entCount) {

        // TODO nextTerm: while (true) {
        loop {
            self.next_ent += 1;
            self.suffix = self.suffix_reader.read_vint()?;

            let term_len = (self.prefix + self.suffix) as usize;
            self.start_byte_pos = self.suffix_reader.position() as i32;
            self.suffix_reader.skip_bytes(self.suffix as usize)?;

            let target_limit = if target.len() < term_len {
                target.len()
            } else {
                term_len
            };
            let mut target_pos = self.prefix as usize;

            // Loop over bytes in the suffix, comparing to
            // the target
            let mut byte_pos = self.start_byte_pos as usize;
            let mut brk = false;
            loop {
                let cmp: i32;
                let stop: bool;
                {
                    let backing = self.suffix_reader.backing();
                    if target_pos < target_limit {
                        cmp = i32::from(backing[byte_pos]) - i32::from(target[target_pos]);
                        byte_pos += 1;
                        target_pos += 1;
                        stop = false;
                    } else {
                        debug_assert!(target_pos == target_limit);
                        cmp = term_len as i32 - target.len() as i32;
                        stop = true;
                    }
                }

                if cmp < 0 {
                    // Current entry is still before the target;
                    // keep scanning

                    if self.next_ent == self.ent_count {
                        // We are done scanning this block
                        // TODO break nextTerm;
                        brk = true;
                    }
                    break;
                } else if cmp > 0 {
                    // Done!  Current entry is after target --
                    // return NOT_FOUND:
                    self.fill_term()?;

                    // if (DEBUG) System.out.println("        not found");
                    return Ok(SeekStatus::NotFound);
                } else if stop {
                    // Exact match!

                    // This cannot be a sub-block because we
                    // would have followed the index to this
                    // sub-block from the start:

                    debug_assert!(self.term_exists);
                    self.fill_term()?;
                    // if (DEBUG) System.out.println("        found!");
                    return Ok(SeekStatus::Found);
                }
            }

            if brk {
                break;
            }
        }

        // It is possible (and OK) that terms index pointed us
        // at this block, but, we scanned the entire block and
        // did not find the term to position to.  This happens
        // when the target is after the last term in the block
        // (but, before the next term in the index).  EG
        // target could be foozzz, and terms index pointed us
        // to the foo* block, but the last term in this block
        // was fooz (and, eg, first term in the next block will
        // bee fop).
        // if self.exact_only nextTerm{
        if exact_only {
            self.fill_term()?;
        }

        // TODO: not consistent that in the
        // not-exact case we don't next() into the next
        // frame here
        Ok(SeekStatus::End)
    }

    // Target's prefix matches this block's prefix; we
    // scan the entries check if the suffix matches.
    fn scan_to_term_non_leaf(&mut self, target: &[u8], exact_only: bool) -> Result<SeekStatus> {
        if self.next_ent == self.ent_count {
            if exact_only {
                self.fill_term()?;
                self.term_exists = self.sub_code == 0;
            }
            return Ok(SeekStatus::End);
        }

        // assert!(self.prefix_matches(target));

        // Loop over each entry (term or sub-block) in this block:
        // TODO nextTerm:
        while self.next_ent < self.ent_count {
            self.next_ent += 1;

            let code = self.suffix_reader.read_vint()?;
            self.suffix = if !self.version_auto_prefix {
                code.unsigned_shift(1)
            } else {
                code.unsigned_shift(2)
            };

            let term_len = (self.prefix + self.suffix) as usize;
            self.start_byte_pos = self.suffix_reader.position() as i32;
            self.suffix_reader.skip_bytes(self.suffix as usize)?;
            {
                if !self.version_auto_prefix {
                    self.term_exists = (code & 1) == 0;
                    if self.term_exists {
                        self.state.term_block_ord += 1;
                        self.sub_code = 0;
                    } else {
                        self.sub_code = self.suffix_reader.read_vlong()?;
                        self.last_sub_fp = self.fp - self.sub_code;
                    }
                } else {
                    match code & 3 {
                        0 => {
                            // Normal term
                            self.term_exists = true;
                            self.state.term_block_ord += 1;
                            self.sub_code = 0;
                            break;
                        }
                        1 => {
                            // Sub-block
                            self.term_exists = false;
                            self.sub_code = self.suffix_reader.read_vlong()?;
                            self.last_sub_fp = self.fp - self.sub_code;
                            break;
                        }
                        _ => {
                            // Floor prefix term: skip it
                            self.suffix_reader.read_byte()?;
                            self.term_exists = false;
                            self.state.term_block_ord += 1;
                            continue;
                        }
                    }
                }
            }

            let target_limit = if target.len() < term_len {
                target.len()
            } else {
                term_len
            };
            let mut target_pos = self.prefix as usize;

            // Loop over bytes in the suffix, comparing to
            // the target
            let mut byte_pos = self.start_byte_pos as usize;
            loop {
                let cmp: i32;
                let stop: bool;
                {
                    let backing = self.suffix_reader.backing();
                    if target_pos < target_limit {
                        cmp = i32::from(backing[byte_pos]) - i32::from(target[target_pos]);
                        byte_pos += 1;
                        target_pos += 1;
                        stop = false;
                    } else {
                        debug_assert!(target_pos == target_limit);
                        cmp = term_len as i32 - target.len() as i32;
                        stop = true;
                    }
                }

                if cmp < 0 {
                    // Current entry is still before the target;
                    // keep scanning
                    // TODO continue next_term;
                    break;
                } else if cmp > 0 {
                    // Done!  Current entry is after target --
                    // return NOT_FOUND:
                    self.fill_term()?;

                    // if (DEBUG) System.out.println("        maybe done exact_only=" + exact_only
                    // + " ste.term_exists=" + ste.term_exists);

                    if !exact_only && !self.term_exists {
                        // TODO this
                        // We are on a sub-block, and caller wants
                        // us to position to the next term after
                        // the target, so we must recurse into the
                        // sub-frame(s):
                        // TODO
                        // ste.current_frame =
                        // ste.push_frame(null, ste.current_frame.last_sub_fp, term_len);
                        // ste.current_frame.load_block();
                        // while ste.current_frame.next() {
                        // ste.current_frame =
                        // ste.push_frame(null, ste.current_frame.last_sub_fp, ste.term.len());
                        // ste.current_frame.load_block();
                        // }
                        //
                    }
                    return Ok(SeekStatus::NotFound);
                } else if stop {
                    // Exact match!

                    // This cannot be a sub-block because we
                    // would have followed the index to this
                    // sub-block from the start:

                    debug_assert!(self.term_exists);
                    self.fill_term()?;
                    return Ok(SeekStatus::Found);
                }
            }
        }

        // It is possible (and OK) that terms index pointed us
        // at this block, but, we scanned the entire block and
        // did not find the term to position to.  This happens
        // when the target is after the last term in the block
        // (but, before the next term in the index).  EG
        // target could be foozzz, and terms index pointed us
        // to the foo* block, but the last term in this block
        // was fooz (and, eg, first term in the next block will
        // bee fop).
        // if (DEBUG) System.out.println("      block end");
        if exact_only {
            self.fill_term()?;
        }

        // TODO: not consistent that in the
        // not-exact case we don't next() into the next
        // frame here
        Ok(SeekStatus::End)
    }

    fn fill_term(&mut self) -> Result<()> {
        let term_length = (self.prefix + self.suffix) as usize;
        self.term.resize(term_length, 0);
        let prefix = self.prefix as usize;
        let suffix = self.suffix as usize;
        self.suffix_reader.seek(i64::from(self.start_byte_pos))?;
        self.suffix_reader
            .read_exact(&mut self.term[prefix..prefix + suffix])?;
        Ok(())
    }

    fn load_block(&mut self) -> Result<()> {
        if self.next_ent != -1 {
            return Ok(());
        }
        self.init_index_input()?;
        let terms_in = self.input.as_mut().unwrap();
        terms_in.seek(self.fp)?;
        let mut code = terms_in.read_vint()?;
        self.ent_count = code.unsigned_shift(1);
        debug_assert!(self.ent_count > 0);
        self.is_last_in_floor = (code & 1) != 0;
        debug_assert!(self.is_last_in_floor || self.is_floor);

        // TODO: if suffixes were stored in random-access
        // array structure, then we could do binary search
        // instead of linear scan to find target term; eg
        // we could have simple array of offsets

        // term suffixes:
        code = terms_in.read_vint()?;
        self.is_leaf_block = (code & 1) != 0;
        let num_bytes = code.unsigned_shift(1);
        self.suffix_reader
            .reload_with_len(terms_in.as_mut(), num_bytes as usize)?;

        // stats
        self.stats_reader.reload(terms_in.as_mut())?;
        self.bytes_reader.reload(terms_in.as_mut())?;

        self.meta_data_upto = 0;
        self.state.term_block_ord = 0;
        self.next_ent = 0;
        self.last_sub_fp = -1;

        // Sub-blocks of a single floor block are always
        // written one after another -- tail recurse:
        self.fp_end = terms_in.file_pointer();

        Ok(())
    }

    // TODO: make this array'd so we can do bin search?
    // likely not worth it?  need to measure how many
    // floor blocks we "typically" get
    pub fn scan_to_floor_frame(&mut self, target: &[u8]) -> Result<()> {
        if !self.is_floor || target.len() <= self.prefix as usize {
            return Ok(());
        }

        let target_label = i32::from(target[self.prefix as usize] as u8);

        if target_label < self.next_floor_label {
            return Ok(());
        }

        let mut new_fp;
        let floor_reader = self.floor_reader.as_mut().unwrap();
        loop {
            let code: i64 = floor_reader.read_vlong()?;
            new_fp = self.fp_orig + code.unsigned_shift(1);
            self.has_terms = (code & 1) != 0;

            self.is_last_in_floor = self.num_follow_floor_blocks == 1;
            self.num_follow_floor_blocks -= 1;

            if self.is_last_in_floor {
                self.next_floor_label = 256;
                break;
            } else {
                self.next_floor_label = i32::from(floor_reader.read_byte()? as u8);
                if target_label < self.next_floor_label {
                    break;
                }
            }
        }

        if new_fp != self.fp {
            // Force re-load of the block:
            self.next_ent = -1;
            self.fp = new_fp;
        }
        Ok(())
    }
}

impl<'a> TermIterator for SegmentTermIterator {
    fn next(&mut self) -> Result<Vec<u8>> {
        // fresh iterator, seek to first term
        //
        // if self.input.is_none() {
        // {
        // let mut field_reader = self.field_reader;
        // self.arc = if self.fst {
        // Some(field_reader.index.as_mut().unwrap().root_arc())
        // } else {
        // None
        // };
        // }
        // self.frame_ord += 1;
        // self.load_block()?;
        // }
        //
        // self.target_before_current_length = self.frame_ord;
        // assert!(!self.eof);
        //
        // {
        // let term = self.term().unwrap();
        // let result = self.seek_exact(&term).unwrap();
        //
        // assert!(result);
        // }

        unimplemented!()
    }

    fn seek_exact(&mut self, target: &[u8]) -> Result<bool> {
        self.term.clear();
        self.term.extend(target.iter());

        self.clear_eof();
        {
            let (output, prefix) = { self.fst.floor(target)? };

            let output: Vec<u8> = output.into();
            let output_len = output.len();
            self.scratch_reader.reset(output);
            let code = self.scratch_reader.read_vlong()?;
            self.fp = code.unsigned_shift(OUTPUT_FLAGS_NUM_BITS);
            self.fp_orig = self.fp;
            self.has_terms = (code & OUTPUT_FLAGS_HAS_TERMS) != 0;
            self.is_floor = (code & OUTPUT_FLAGS_IS_FLOOR) != 0;
            self.floor_reader = if self.is_floor {
                let position = self.scratch_reader.position();
                let mut floor_reader = ByteBufferIndexInput::from(self.scratch_reader
                    .get_slice_clone(position as i64, output_len - position)?);
                self.num_follow_floor_blocks = floor_reader.read_vint()?;
                self.next_floor_label = i32::from(floor_reader.read_byte()?);
                Some(floor_reader)
            } else {
                None
            };
            self.next_ent = -1;
            self.prefix = prefix as i32;
            self.state.term_block_ord = 0;
            self.last_sub_fp = -1;
        }
        self.scan_to_floor_frame(target)?;
        self.load_block()?;
        Ok(match self.scan_to_term(target, true)? {
            SeekStatus::Found => true,
            _ => false,
        })
    }

    fn seek_ceil(&mut self, target: &[u8]) -> Result<SeekStatus> {
        self.term.clear();
        self.term.extend(target.iter());

        self.clear_eof();
        // FST.Arc<BytesRef> arc;
        // let mut target_upto: usize;
        // let mut output: Vec<u8> = vec![];

        unimplemented!()
    }

    fn seek_exact_ord(&mut self, _ord: i64) -> Result<()> {
        unimplemented!()
    }

    fn seek_exact_state(&mut self, text: &[u8], state: &TermState) -> Result<()> {
        self.term.clear();
        self.term.extend(text.iter());
        self.state = BlockTermState::deserialize(&state.serialize())?;
        self.is_leaf_block = true;
        self.meta_data_upto = self.state.term_block_ord;
        Ok(())
    }

    fn term(&mut self) -> Result<Vec<u8>> {
        Ok(self.term.clone())
    }

    fn ord(&mut self) -> Result<i64> {
        bail!("Unsupported")
    }

    fn doc_freq(&mut self) -> Result<i32> {
        debug_assert!(!self.eof);
        let state = self.decode_meta_data()?;
        Ok(state.doc_freq)
    }

    fn total_term_freq(&mut self) -> Result<i64> {
        debug_assert!(!self.eof);
        let state = self.decode_meta_data()?;
        Ok(state.total_term_freq)
    }

    fn postings(&mut self) -> Result<Box<PostingIterator>> {
        self.postings_with_flags(0)
    }

    fn postings_with_flags(&mut self, flags: i16) -> Result<Box<PostingIterator>> {
        debug_assert!(!self.eof);
        let state = self.decode_meta_data()?;
        self.postings_reader.postings(
            self.field_info.as_ref(),
            &state,
            flags,
            self.segment.clone(),
            &self.term,
        )
    }

    fn term_state(&mut self) -> Result<Box<TermState>> {
        Ok(Box::new(self.decode_meta_data()?))
    }
}
