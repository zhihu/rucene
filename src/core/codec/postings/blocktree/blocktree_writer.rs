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

use core::codec::codec_util::{write_footer, write_index_header};
use core::codec::field_infos::{FieldInfo, FieldInfos};
use core::codec::postings::blocktree::*;
use core::codec::postings::{
    FieldsConsumer, PostingsWriterBase, DEFAULT_DOC_TERM_FREQ, DEFAULT_SEGMENT_DOC_FREQ,
};
use core::codec::segment_infos::{segment_file_name, SegmentWriteState};
use core::codec::Codec;
use core::codec::{Fields, TermIterator, Terms};
use core::doc::IndexOptions;
use core::store::directory::Directory;
use core::store::io::{DataOutput, IndexOutput, RAMOutputStream};
use core::util::fst::BytesRefFSTIterator;
use core::util::fst::FstBuilder;
use core::util::fst::{ByteSequenceOutput, ByteSequenceOutputFactory};
use core::util::fst::{InputType, FST};
use core::util::DocId;
use core::util::{to_ints_ref, IntsRefBuilder};
use core::util::{FixedBitSet, ImmutableBitSet};
use error::{ErrorKind, Result};

use std::cmp::min;
use std::mem;

/// Block-based terms index and dictionary writer.
///
/// Writes terms dict and index, block-encoding (column
/// stride) each term's metadata for each set of terms
/// between two index terms.
///
/// Files:
/// - .tim: Term Dictionary
/// - .tip: Term Index
///
/// ### Term Dictionary
///
/// <The .tim file contains the list of terms in each
/// field along with per-term statistics (such as docfreq)
/// and per-term metadata (typically pointers to the postings list
/// for that term in the inverted index).
///
/// The .tim is arranged in blocks: with blocks containing
/// a variable number of entries (by default 25-48), where
/// each entry is either a term or a reference to a
/// sub-block.
///
/// NOTE: The term dictionary can plug into different postings implementations:
/// the postings writer/reader are actually responsible for encoding
/// and decoding the Postings Metadata and Term Metadata sections.
///
/// - TermsDict (.tim) --> Header, <i>PostingsHeader</i>, NodeBlock<sup>NumBlocks</sup>,
///   FieldSummary, DirOffset, Footer
/// - NodeBlock --> (OuterNode | InnerNode)
/// - OuterNode --> EntryCount, SuffixLength, Byte<sup>SuffixLength</sup>, StatsLength, < TermStats
/// ><sup>EntryCount</sup>, MetaLength, <<i>TermMetadata</i>><sup>EntryCount</sup> - InnerNode -->
/// EntryCount, SuffixLength[,Sub?], Byte<sup>SuffixLength</sup>, StatsLength, < TermStats ?
/// ><sup>EntryCount</sup>, MetaLength, <<i>TermMetadata ? </i>><sup>EntryCount</sup> - TermStats
/// --> DocFreq, TotalTermFreq - FieldSummary --> NumFields, <FieldNumber, NumTerms,
/// RootCodeLength, Byte<sup>RootCodeLength</sup>, SumTotalTermFreq?,
/// SumDocFreq, DocCount, LongsSize, MinTerm, MaxTerm><sup>NumFields</sup> - Header -->
/// `CodecUtil#writeHeader CodecHeader} - DirOffset --> `DataOutput#write_long Uint64}
/// - MinTerm,MaxTerm --> `DataOutput#write_vint VInt} length followed by the bytes
/// - EntryCount,SuffixLength,StatsLength,DocFreq,MetaLength,NumFields,
///   FieldNumber,RootCodeLength,DocCount,LongsSize --> `DataOutput#write_vint VInt}
/// - TotalTermFreq,NumTerms,SumTotalTermFreq,SumDocFreq --> `DataOutput#write_vlong VLong}
/// - Footer --> `codec_util#writeFooter CodecFooter}
/// Notes:
///    - Header is a `codec_util#writeHeader CodecHeader} storing the version information for the
///      BlockTree implementation.
///    - DirOffset is a pointer to the FieldSummary section.
///    - DocFreq is the count of documents which contain the term.
///    - TotalTermFreq is the total number of occurrences of the term. This is encoded as the
///      difference between the total number of occurrences and the DocFreq.
///    - FieldNumber is the fields number from `FieldInfos}. (.fnm)
///    - NumTerms is the number of unique terms for the field.
///    - RootCode points to the root block for the field.
///    - SumDocFreq is the total number of postings, the number of term-document pairs across the
///      entire field.
///    - DocCount is the number of documents that have at least one posting for this field.
///    - LongsSize records how many long values the postings writer/reader record per term (e.g., to
///      hold freq/prox/doc file offsets).
///    - MinTerm, MaxTerm are the lowest and highest term in this field.
///    - PostingsHeader and TermMetadata are plugged into by the specific postings implementation:
///      these contain arbitrary per-file data (such as parameters or versioning information) and
///      per-term data (such as pointers to inverted files).
///    - For inner nodes of the tree, every entry will steal one bit to mark whether it points
/// to child nodes(sub-block). If so, the corresponding TermStats and TermMetaData are
/// omitted
///
/// Term Index
/// The .tip file contains an index into the term dictionary, so that it can be
/// accessed randomly.  The index is also used to determine
/// when a given term cannot exist on disk (in the .tim file), saving a disk seek.
///   - TermsIndex (.tip) --> Header, FSTIndex<sup>NumFields</sup>
///     <IndexStartFP><sup>NumFields</sup>, DirOffset, Footer
///   - Header --> `codec_util#writeHeader CodecHeader`
///   - DirOffset --> `DataOutput#writeLong Uint64`
///   - IndexStartFP --> `DataOutput#write_vlong VLong`
///   TODO: better describe FST output here
///   - FSTIndex --> `FST FST<Vec<u8>>`
///   - Footer --> `codec_util::writeFooter CodecFooter`
/// Notes:
///   - The .tip file contains a separate FST for each field.  The FST maps a term prefix to the
///     on-disk block that holds all terms starting with that prefix.  Each field's IndexStartFP
///     points to its FST.
///   - DirOffset is a pointer to the start of the IndexStartFPs for all fields
///   - It's possible that an on-disk block would contain too many terms (more than the allowed
///     maximum (default: 48)).  When this happens, the block is sub-divided into new blocks (called
///     "floor blocks"), and then the output in the FST for the block's prefix encodes the leading
///     byte of each sub-block, and its file pointer.
///
/// @see BlockTreeTermsReader
pub struct BlockTreeTermsWriter<T: PostingsWriterBase, O: IndexOutput> {
    terms_out: O,
    index_out: O,
    max_doc: DocId,
    min_items_in_block: usize,
    max_items_in_block: usize,
    postings_writer: T,
    field_infos: FieldInfos,
    fields: Vec<FieldMetaData>,
    scratch_bytes: RAMOutputStream,
    scratch_ints_ref: IntsRefBuilder,
    closed: bool,
}

impl<T: PostingsWriterBase, O: IndexOutput> BlockTreeTermsWriter<T, O> {
    pub fn new<D: Directory, DW: Directory<IndexOutput = O>, C: Codec>(
        state: &SegmentWriteState<D, DW, C>,
        postings_writer: T,
        min_items_in_block: usize,
        max_items_in_block: usize,
    ) -> Result<BlockTreeTermsWriter<T, O>> {
        Self::validate_settings(min_items_in_block, max_items_in_block)?;

        let max_doc = state.segment_info.max_doc;

        let terms_name = segment_file_name(
            &state.segment_info.name,
            &state.segment_suffix,
            TERMS_EXTENSION,
        );
        let mut terms_out = state.directory.create_output(&terms_name, &state.context)?;
        write_index_header(
            &mut terms_out,
            TERMS_CODEC_NAME,
            VERSION_CURRENT,
            state.segment_info.get_id(),
            &state.segment_suffix,
        )?;

        let index_name = segment_file_name(
            &state.segment_info.name,
            &state.segment_suffix,
            TERMS_INDEX_EXTENSION,
        );
        let mut index_out = state.directory.create_output(&index_name, &state.context)?;
        write_index_header(
            &mut index_out,
            TERMS_INDEX_CODEC_NAME,
            VERSION_CURRENT,
            state.segment_info.get_id(),
            &state.segment_suffix,
        )?;

        let mut postings_writer = postings_writer;
        postings_writer.init(&mut terms_out, &state)?;

        Ok(BlockTreeTermsWriter {
            terms_out,
            index_out,
            max_doc,
            min_items_in_block,
            max_items_in_block,
            postings_writer,
            field_infos: state.field_infos.clone(),
            fields: vec![],
            scratch_bytes: RAMOutputStream::new(false),
            scratch_ints_ref: IntsRefBuilder::new(),
            closed: false,
        })
    }

    fn write_trailer(out: &mut impl IndexOutput, dir_start: i64) -> Result<()> {
        out.write_long(dir_start)
    }

    fn write_index_trailer(out: &mut impl IndexOutput, dir_start: i64) -> Result<()> {
        out.write_long(dir_start)
    }

    fn validate_settings(min_items_in_block: usize, max_items_in_block: usize) -> Result<()> {
        if min_items_in_block <= 1 {
            bail!(ErrorKind::IllegalArgument(format!(
                "min_items_in_block must be >= 2; got {}",
                min_items_in_block
            )));
        }

        if min_items_in_block > max_items_in_block {
            bail!(ErrorKind::IllegalArgument(format!(
                "min_items_in_block '{}' >= max_items_in_block '{}'",
                min_items_in_block, max_items_in_block
            )));
        }
        if 2 * (min_items_in_block - 1) > max_items_in_block {
            bail!(ErrorKind::IllegalArgument(format!(
                "2 * (min_items_in_block '{}' - 1) >= max_items_in_block '{}'",
                min_items_in_block, max_items_in_block
            )));
        }
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }

        self.closed = true;

        let dir_start = self.terms_out.file_pointer();
        let index_dir_start = self.index_out.file_pointer();

        let field_cnt = self.fields.len() as i32;
        self.terms_out.write_vint(field_cnt)?;

        let fields = mem::replace(&mut self.fields, vec![]);
        for field in &fields {
            self.terms_out.write_vint(field.field_info.number as i32)?;
            debug_assert!(field.num_terms > 0);
            self.terms_out.write_vlong(field.num_terms as i64)?;
            self.terms_out.write_vint(field.root_code.len() as i32)?;
            self.terms_out
                .write_bytes(&field.root_code, 0, field.root_code.len())?;
            debug_assert_ne!(field.field_info.index_options, IndexOptions::Null);
            if field.field_info.index_options != IndexOptions::Docs {
                self.terms_out.write_vlong(field.sum_total_term_freq)?;
            }
            self.terms_out.write_vlong(field.sum_doc_freq)?;
            self.terms_out.write_vint(field.doc_count)?;
            self.terms_out.write_vint(field.longs_size as i32)?;
            self.index_out.write_vlong(field.index_start_fp)?;
            Self::write_bytes_ref(&mut self.terms_out, &field.min_term)?;
            Self::write_bytes_ref(&mut self.terms_out, &field.max_term)?;
        }
        Self::write_trailer(&mut self.terms_out, dir_start)?;
        write_footer(&mut self.terms_out)?;
        Self::write_index_trailer(&mut self.index_out, index_dir_start)?;
        write_footer(&mut self.index_out)?;

        self.postings_writer.close()
    }

    fn write_bytes_ref(out: &mut impl IndexOutput, bytes: &[u8]) -> Result<()> {
        out.write_vint(bytes.len() as i32)?;
        out.write_bytes(bytes, 0, bytes.len())
    }
}

impl<T: PostingsWriterBase, O: IndexOutput> FieldsConsumer for BlockTreeTermsWriter<T, O> {
    fn write(&mut self, fields: &impl Fields) -> Result<()> {
        let mut last_field = String::new();
        for field in fields.fields() {
            debug_assert!(last_field < field);

            if let Some(terms) = fields.terms(&field)? {
                let mut terms_iter = terms.iterator()?;
                let field_info = self.field_infos.field_info_by_name(&field).unwrap().clone();
                let mut terms_writer = TermsWriter::new(field_info, self);

                while let Some(term) = terms_iter.next()? {
                    terms_writer.write(
                        &term,
                        &mut terms_iter,
                        DEFAULT_SEGMENT_DOC_FREQ,
                        DEFAULT_DOC_TERM_FREQ,
                    )?;
                }
                terms_writer.finish()?;
            }
            last_field = field;
        }
        Ok(())
    }
}

impl<T: PostingsWriterBase, O: IndexOutput> Drop for BlockTreeTermsWriter<T, O> {
    fn drop(&mut self) {
        if let Err(e) = self.close() {
            error!("drop BlockTreeTermsWriter failed by '{:?}'", e);
        }
    }
}

struct FieldMetaData {
    field_info: FieldInfo,
    root_code: Vec<u8>,
    num_terms: usize,
    index_start_fp: i64,
    sum_total_term_freq: i64,
    sum_doc_freq: i64,
    doc_count: i32,
    longs_size: usize,
    min_term: Vec<u8>,
    max_term: Vec<u8>,
}

struct TermsWriter<'a, T: PostingsWriterBase, O: IndexOutput> {
    field_info: FieldInfo,
    num_terms: i64,
    docs_seen: FixedBitSet,
    sum_total_term_freq: i64,
    sum_doc_freq: i64,
    index_start_fp: i64,
    last_term: Vec<u8>,
    prefix_starts: Vec<usize>,
    longs: Vec<i64>,
    longs_size: usize,
    // Pending stack of terms and blocks.  As terms arrive (in sorted order)
    // we append to this stack, and once the top of the stack has enough
    // terms starting with a common prefix, we write a new block with
    // those terms and replace those terms in the stack with a new block:
    pending: Vec<PendingEntry>,
    // Reused in writeBlocks:
    new_blocks: Vec<PendingBlock>,
    first_pending_term: Option<Vec<u8>>,
    last_pending_term: Option<Vec<u8>>,

    suffix_writer: RAMOutputStream,
    stats_writer: RAMOutputStream,
    meta_writer: RAMOutputStream,
    bytes_writer: RAMOutputStream,

    block_tree_writer: &'a mut BlockTreeTermsWriter<T, O>,
}

impl<'a, T: PostingsWriterBase, O: IndexOutput> TermsWriter<'a, T, O> {
    fn new(field_info: FieldInfo, block_tree_writer: &'a mut BlockTreeTermsWriter<T, O>) -> Self {
        assert_ne!(field_info.index_options, IndexOptions::Null);
        let docs_seen = FixedBitSet::new(block_tree_writer.max_doc as usize);
        let longs_size = block_tree_writer.postings_writer.set_field(&field_info) as usize;
        let longs = vec![0i64; longs_size];
        TermsWriter {
            field_info,
            longs_size,
            longs,
            docs_seen,
            num_terms: 0,
            sum_total_term_freq: 0,
            sum_doc_freq: 0,
            index_start_fp: 0,
            last_term: vec![],
            prefix_starts: vec![0usize; 8],
            pending: vec![],
            new_blocks: vec![],
            first_pending_term: None,
            last_pending_term: None,
            suffix_writer: RAMOutputStream::new(false),
            stats_writer: RAMOutputStream::new(false),
            meta_writer: RAMOutputStream::new(false),
            bytes_writer: RAMOutputStream::new(false),
            block_tree_writer,
        }
    }

    fn write_blocks(&mut self, prefix_length: usize, count: usize) -> Result<()> {
        assert!(prefix_length > 0 || count == self.pending.len());

        let mut last_suffix_lead_label = -1;

        // True if we saw at least one term in this block (we record if a block
        // only points to sub-blocks in the terms index so we can avoid seeking
        // to it when we are looking for a term):
        let mut has_terms = false;
        let mut has_sub_blocks = false;

        let start = self.pending.len() - count;
        let end = self.pending.len();
        let mut next_block_start = start;
        let mut next_floor_lead_label = -1;

        for i in start..end {
            let is_term_entry: bool;
            let suffix_lead_label = match self.pending[i] {
                PendingEntry::Term(ref term) => {
                    is_term_entry = true;
                    if term.term_bytes.len() == prefix_length {
                        // Suffix is 0, i.e. prefix 'foo' and term is
                        // 'foo' so the term has empty string suffix
                        // in this block
                        debug_assert_eq!(last_suffix_lead_label, -1);
                        -1
                    } else {
                        term.term_bytes[prefix_length] as u32 as i32
                    }
                }
                PendingEntry::Block(ref block) => {
                    is_term_entry = false;
                    debug_assert!(block.prefix.len() > prefix_length);
                    block.prefix[prefix_length] as u32 as i32
                }
            };

            if suffix_lead_label != last_suffix_lead_label {
                let items_in_block = i - next_block_start;
                if items_in_block >= self.block_tree_writer.min_items_in_block
                    && end - next_block_start > self.block_tree_writer.max_items_in_block
                {
                    // The count is too large for one block, so we must break it into "floor"
                    // blocks, where we record the leading label of the suffix
                    // of the first term in each floor block, so at search time we can
                    // jump to the right floor block.  We just use a naive greedy segmenter here:
                    // make a new floor block as soon as we have at least
                    // minItemsInBlock.  This is not always best: it often produces
                    // a too-small block as the final block:
                    let is_floor = items_in_block < count;
                    let new_block = self.write_block(
                        prefix_length,
                        is_floor,
                        next_floor_lead_label,
                        next_block_start,
                        i,
                        has_terms,
                        has_sub_blocks,
                    )?;
                    self.new_blocks.push(new_block);

                    has_terms = false;
                    has_sub_blocks = false;
                    next_floor_lead_label = suffix_lead_label;
                    next_block_start = i;
                }

                last_suffix_lead_label = suffix_lead_label;
            }

            if is_term_entry {
                has_terms = true;
            } else {
                has_sub_blocks = true;
            }
        }

        // Write last block, if any:
        if next_block_start < end {
            let items_in_block = end - next_block_start;
            let is_floor = items_in_block < count;

            let new_block = self.write_block(
                prefix_length,
                is_floor,
                next_floor_lead_label,
                next_block_start,
                end,
                has_terms,
                has_sub_blocks,
            )?;
            self.new_blocks.push(new_block);
        }

        debug_assert!(!self.new_blocks.is_empty());
        debug_assert!(self.new_blocks[0].is_floor || self.new_blocks.len() == 1);
        let mut first_block = self.new_blocks.remove(0);
        first_block.compile_index(
            &mut self.new_blocks,
            &mut self.block_tree_writer.scratch_bytes,
            &mut self.block_tree_writer.scratch_ints_ref,
        )?;

        // Remove slice from the top of the pending stack, that we just wrote:
        let new_size = self.pending.len() - count;
        self.pending.truncate(new_size);
        // Append new block
        self.pending.push(PendingEntry::Block(first_block));

        self.new_blocks.clear();

        Ok(())
    }

    /// Writes the specified slice (start is inclusive, end is exclusive)
    /// from pending stack as a new block.  If isFloor is true, there
    /// were too many (more than maxItemsInBlock) entries sharing the
    /// same prefix, and so we broke it into multiple floor blocks where
    /// we record the starting label of the suffix of each floor block.
    #[allow(clippy::too_many_arguments)]
    fn write_block(
        &mut self,
        prefix_length: usize,
        is_floor: bool,
        floor_lead_label: i32,
        start: usize,
        end: usize,
        has_terms: bool,
        has_sub_blocks: bool,
    ) -> Result<PendingBlock> {
        debug_assert!(end > start);

        let start_fp = self.block_tree_writer.terms_out.file_pointer();
        let has_floor_lead_label = is_floor && floor_lead_label != -1;

        let prefix_bytes_len = prefix_length + if has_floor_lead_label { 1 } else { 0 };
        let mut prefix = Vec::with_capacity(prefix_bytes_len);
        prefix.resize(prefix_length, 0u8);
        prefix.copy_from_slice(&self.last_term[0..prefix_length]);

        let num_entries = end - start;
        let mut code = num_entries << 1;
        if end == self.pending.len() {
            // Last block:
            code |= 1;
        }
        self.block_tree_writer.terms_out.write_vint(code as i32)?;

        // 1st pass: pack term suffix bytes into bytes blob
        // TODO: cutover to bulk int codec... simple64?

        // We optimize the leaf block case (block has only terms), writing a more
        // compact format in this case:
        let is_leaf_block = !has_sub_blocks;
        let mut sub_indices = Vec::new();
        let mut absolute = true;

        if is_leaf_block {
            // Block contains only ordinary terms:
            for i in start..end {
                if let PendingEntry::Term(ref term) = self.pending[i] {
                    assert!(term.term_bytes.starts_with(&prefix));
                    let suffix = term.term_bytes.len() - prefix_length;
                    // For leaf block we write suffix straight
                    self.suffix_writer.write_vint(suffix as i32)?;
                    self.suffix_writer
                        .write_bytes(&term.term_bytes, prefix_length, suffix)?;
                    debug_assert!(
                        floor_lead_label == -1
                            || (term.term_bytes[prefix_length] as u32 as i32) >= floor_lead_label
                    );

                    // Write term stats, to separate bytes blob:
                    self.stats_writer.write_vint(term.state.doc_freq)?;
                    if self.field_info.index_options != IndexOptions::Docs {
                        assert!(term.state.total_term_freq >= term.state.doc_freq as i64);
                        self.stats_writer
                            .write_vlong(term.state.total_term_freq - term.state.doc_freq as i64)?;
                    }

                    // Write term meta data
                    self.block_tree_writer.postings_writer.encode_term(
                        &mut self.longs,
                        &mut self.bytes_writer,
                        &self.field_info,
                        &term.state,
                        absolute,
                    )?;
                    for pos in 0..self.longs_size {
                        assert!(self.longs[pos] >= 0);
                        self.meta_writer.write_vlong(self.longs[pos])?;
                    }
                    self.bytes_writer.write_to(&mut self.meta_writer)?;
                    self.bytes_writer.reset();
                    absolute = false;
                } else {
                    unreachable!();
                }
            }
        } else {
            // Block has at least one prefix term or a sub block:
            for i in start..end {
                // replace the entry with a stub entry, it won't be used, so this action is safe
                // the stub entry will be free in parent caller soon
                let entry = mem::replace(
                    &mut self.pending[i],
                    PendingEntry::Term(PendingTerm::new(
                        Vec::with_capacity(0),
                        BlockTermState::new(),
                    )),
                );
                match entry {
                    PendingEntry::Term(term) => {
                        debug_assert!(term.term_bytes.starts_with(&prefix));
                        let suffix = term.term_bytes.len() - prefix_length;

                        // For non-leaf block we borrow 1 bit to record
                        // if entry is term or sub-block, and 1 bit to record if
                        // it's a prefix term.  Terms cannot be larger than ~32 KB
                        // so we won't run out of bits:
                        self.suffix_writer.write_vint((suffix << 1) as i32)?;
                        self.suffix_writer
                            .write_bytes(&term.term_bytes, prefix_length, suffix)?;

                        // Write term stats, to separate bytes blob:
                        self.stats_writer.write_vint(term.state.doc_freq)?;
                        if self.field_info.index_options != IndexOptions::Docs {
                            assert!(term.state.total_term_freq >= term.state.doc_freq as i64);
                            self.stats_writer.write_vlong(
                                term.state.total_term_freq - term.state.doc_freq as i64,
                            )?;
                        }

                        // TODO: now that terms dict "sees" these longs,
                        // we can explore better column-stride encodings
                        // to encode all long[0]s for this block at
                        // once, all long[1]s, etc., e.g. using
                        // Simple64.  Alternatively, we could interleave
                        // stats + meta ... no reason to have them
                        // separate anymore:

                        // Write term meta data
                        self.block_tree_writer.postings_writer.encode_term(
                            &mut self.longs,
                            &mut self.bytes_writer,
                            &self.field_info,
                            &term.state,
                            absolute,
                        )?;
                        for pos in 0..self.longs_size {
                            assert!(self.longs[pos] >= 0);
                            self.meta_writer.write_vlong(self.longs[pos])?;
                        }
                        self.bytes_writer.write_to(&mut self.meta_writer)?;
                        self.bytes_writer.reset();
                        absolute = false;
                    }
                    PendingEntry::Block(block) => {
                        debug_assert!(block.prefix.starts_with(&prefix));
                        let suffix = block.prefix.len() - prefix_length;
                        debug_assert!(suffix > 0);

                        // For non-leaf block we borrow 1 bit to record
                        // if entry is term or sub-block:f
                        self.suffix_writer.write_vint(((suffix << 1) | 1) as i32)?;
                        self.suffix_writer
                            .write_bytes(&block.prefix, prefix_length, suffix)?;

                        debug_assert!(
                            floor_lead_label == -1
                                || (block.prefix[prefix_length] as u32 as i32) >= floor_lead_label
                        );
                        debug_assert!(block.fp < start_fp);

                        self.suffix_writer.write_vlong(start_fp - block.fp)?;
                        debug_assert!(block.index.is_some());
                        sub_indices.push(block.index.unwrap());
                    }
                }
            }
            assert!(!sub_indices.is_empty());
        }

        // TODO: we could block-write the term suffix pointers;
        // this would take more space but would enable binary
        // search on lookup

        // Write suffixes bytes blob to terms dict output:
        let term_addr =
            (self.suffix_writer.file_pointer() << 1) as i32 + if is_leaf_block { 1 } else { 0 };
        self.block_tree_writer.terms_out.write_vint(term_addr)?;
        self.suffix_writer
            .write_to(&mut self.block_tree_writer.terms_out)?;
        self.suffix_writer.reset();

        // Write term stats data bytes blob
        self.block_tree_writer
            .terms_out
            .write_vint(self.stats_writer.file_pointer() as i32)?;
        self.stats_writer
            .write_to(&mut self.block_tree_writer.terms_out)?;
        self.stats_writer.reset();

        // Write term meta data bytes blob
        self.block_tree_writer
            .terms_out
            .write_vint(self.meta_writer.file_pointer() as i32)?;
        self.meta_writer
            .write_to(&mut self.block_tree_writer.terms_out)?;
        self.meta_writer.reset();

        if has_floor_lead_label {
            prefix.push(floor_lead_label as u8);
        }

        Ok(PendingBlock::new(
            prefix,
            start_fp,
            has_terms,
            is_floor,
            floor_lead_label,
            sub_indices,
        ))
    }

    // Writes one term's worth of postings.
    pub fn write(
        &mut self,
        text: &[u8],
        terms_iter: &mut impl TermIterator,
        doc_freq_limit: i32,
        term_freq_limit: i32,
    ) -> Result<()> {
        if let Some(state) = self.block_tree_writer.postings_writer.write_term(
            text,
            terms_iter,
            &mut self.docs_seen,
            doc_freq_limit,
            term_freq_limit,
        )? {
            assert_ne!(state.doc_freq, 0);
            assert!(
                self.field_info.index_options == IndexOptions::Docs
                    || state.total_term_freq >= state.doc_freq as i64
            );
            self.push_term(text)?;

            self.sum_doc_freq += state.doc_freq as i64;
            self.sum_total_term_freq += state.total_term_freq;
            self.num_terms += 1;

            let term = PendingTerm::new(text.to_vec(), state);
            self.pending.push(PendingEntry::Term(term));

            if self.first_pending_term.is_none() {
                self.first_pending_term = Some(text.to_vec());
            }
            self.last_pending_term = Some(text.to_vec());
        }
        Ok(())
    }

    /// Pushes the new term to the top of the stack, and writes new blocks.
    fn push_term(&mut self, text: &[u8]) -> Result<()> {
        let limit = min(self.last_term.len(), text.len());

        // Find common prefix between last term and current term:
        let mut pos = 0;
        while pos < limit && self.last_term[pos] == text[pos] {
            pos += 1;
        }

        // Close the "abandoned" suffix now:
        let last_term_len = self.last_term.len();
        for i in 0..last_term_len - pos {
            // How many items on top of the stack share the current suffix
            // we are closing:
            let idx = last_term_len - 1 - i;
            let prefix_top_size = self.pending.len() - self.prefix_starts[idx];
            if prefix_top_size >= self.block_tree_writer.min_items_in_block {
                self.write_blocks(idx + 1, prefix_top_size)?;
                self.prefix_starts[idx] = self.prefix_starts[idx].wrapping_sub(prefix_top_size - 1);
            }
        }

        if self.prefix_starts.len() < text.len() {
            self.prefix_starts.resize(text.len(), 0usize);
        }

        // Init new tail:
        let pending_len = self.pending.len();
        for i in pos..text.len() {
            self.prefix_starts[i] = pending_len;
        }

        self.last_term.resize(text.len(), 0u8);
        self.last_term.copy_from_slice(text);
        Ok(())
    }

    /// Finishes all terms in this field
    pub fn finish(&mut self) -> Result<()> {
        if self.num_terms > 0 {
            // Add empty term to force closing of all final blocks
            self.push_term(&[])?;

            // TODO: if pending.len() is already 1 with a non-zero prefix length
            // we can save writing a "degenerate" root block, but we have to
            // fix all the places that assume the root block's prefix is the empty string:
            self.push_term(&[])?;
            let length = self.pending.len();
            self.write_blocks(0, length)?;

            // We better have one final "root" block:
            debug_assert!(self.pending.len() == 1 && !self.pending[0].is_term());
            let mut root = {
                let first = self.pending.remove(0);
                match first {
                    PendingEntry::Block(b) => b,
                    _ => unreachable!(),
                }
            };
            debug_assert_eq!(root.prefix.len(), 0);
            debug_assert!(root.index.as_ref().unwrap().empty_output.is_some());

            let empty_bytes = root
                .index
                .as_ref()
                .unwrap()
                .empty_output
                .as_ref()
                .unwrap()
                .inner()
                .to_vec();

            // write fst to index
            let start_fp = self.block_tree_writer.index_out.file_pointer();
            self.index_start_fp = start_fp;
            root.index
                .as_mut()
                .unwrap()
                .save(&mut self.block_tree_writer.index_out)?;

            debug_assert!(self.first_pending_term.is_some());
            let min_term = self.first_pending_term.take().unwrap();
            debug_assert!(self.last_pending_term.is_some());
            let max_term = self.last_pending_term.take().unwrap();

            let meta = FieldMetaData {
                field_info: self.field_info.clone(),
                root_code: empty_bytes,
                num_terms: self.num_terms as usize,
                index_start_fp: self.index_start_fp,
                sum_total_term_freq: self.sum_total_term_freq,
                sum_doc_freq: self.sum_doc_freq,
                doc_count: self.docs_seen.cardinality() as i32,
                longs_size: self.longs_size,
                min_term,
                max_term,
            };
            self.block_tree_writer.fields.push(meta);
        } else {
            debug_assert!(
                self.sum_total_term_freq == 0
                    || self.field_info.index_options == IndexOptions::Docs
                        && self.sum_total_term_freq == -1
            );
            debug_assert_eq!(self.sum_doc_freq, 0);
            debug_assert_eq!(self.docs_seen.cardinality(), 0);
        }
        Ok(())
    }
}

enum PendingEntry {
    Term(PendingTerm),
    Block(PendingBlock),
}

impl PendingEntry {
    fn is_term(&self) -> bool {
        match *self {
            PendingEntry::Term(_) => true,
            _ => false,
        }
    }
}

struct PendingTerm {
    term_bytes: Vec<u8>,
    state: BlockTermState,
}

impl PendingTerm {
    fn new(term: Vec<u8>, state: BlockTermState) -> Self {
        PendingTerm {
            term_bytes: term,
            state,
        }
    }
}

struct PendingBlock {
    prefix: Vec<u8>,
    fp: i64,
    has_terms: bool,
    is_floor: bool,
    floor_lead_byte: i32,
    index: Option<FST<ByteSequenceOutputFactory>>,
    sub_indices: Vec<FST<ByteSequenceOutputFactory>>,
}

impl PendingBlock {
    fn new(
        prefix: Vec<u8>,
        fp: i64,
        has_terms: bool,
        is_floor: bool,
        floor_lead_byte: i32,
        sub_indices: Vec<FST<ByteSequenceOutputFactory>>,
    ) -> Self {
        PendingBlock {
            prefix,
            fp,
            sub_indices,
            has_terms,
            is_floor,
            floor_lead_byte,
            index: None,
        }
    }

    // this method's logic is a little different for lucene implement
    // blocks not contain self as the first element, due to rust borrow-checker mechanism
    fn compile_index(
        &mut self,
        blocks: &mut [PendingBlock],
        scratch_bytes: &mut RAMOutputStream,
        scratch_ints_ref: &mut IntsRefBuilder,
    ) -> Result<()> {
        assert!((self.is_floor && !blocks.is_empty()) || (!self.is_floor && blocks.is_empty()));
        assert_eq!(scratch_bytes.file_pointer(), 0);

        // TODO: try writing the leading vLong in MSB order
        // (opposite of what Lucene does today), for better
        // outputs sharing in the FST
        scratch_bytes.write_vlong(encode_output(self.fp, self.has_terms, self.is_floor))?;
        if self.is_floor {
            scratch_bytes.write_vint(blocks.len() as i32)?;
            for i in 0..blocks.len() {
                assert_ne!(blocks[i].floor_lead_byte, -1);
                scratch_bytes.write_byte(blocks[i].floor_lead_byte as u8)?;
                assert!(blocks[i].fp > self.fp);
                let flag = if blocks[i].has_terms { 1 } else { 0 };
                scratch_bytes.write_vlong((blocks[i].fp - self.fp) << 1 | flag)?;
            }
        }

        let mut index_builder = FstBuilder::build(
            InputType::Byte1,
            0,
            0,
            true,
            false,
            i32::max_value() as u32,
            ByteSequenceOutputFactory {},
            true,
            15,
        );
        index_builder.init();
        let mut bytes = vec![0u8; scratch_bytes.file_pointer() as usize];
        debug_assert!(!bytes.is_empty());
        scratch_bytes.write_to_buf(&mut bytes)?;
        index_builder.add(
            to_ints_ref(self.prefix.as_ref(), scratch_ints_ref),
            ByteSequenceOutput::new(bytes),
        )?;
        scratch_bytes.reset();

        // Copy over index for self (self is the first sub-block)
        if !self.sub_indices.is_empty() {
            let sub_indices = mem::replace(&mut self.sub_indices, vec![]);
            for sub_index in sub_indices {
                self.append(&mut index_builder, sub_index, scratch_ints_ref)?;
            }
        }

        // Copy over index for all other sub-blocks
        for block in blocks {
            if !block.sub_indices.is_empty() {
                let sub_indices = mem::replace(&mut block.sub_indices, vec![]);
                for sub_index in sub_indices {
                    self.append(&mut index_builder, sub_index, scratch_ints_ref)?;
                }
            }
        }

        self.index = index_builder.finish()?;

        debug_assert!(self.sub_indices.is_empty());

        Ok(())
    }

    // TODO: maybe we could add bulk-add method to
    // Builder?  Takes FST and unions it w/ current FST.
    fn append(
        &self,
        builder: &mut FstBuilder<ByteSequenceOutputFactory>,
        sub_index: FST<ByteSequenceOutputFactory>,
        scratch_ints_ref: &mut IntsRefBuilder,
    ) -> Result<()> {
        let mut fst_iterator = BytesRefFSTIterator::new(sub_index);
        fst_iterator.init();
        while let Some((input, output)) = fst_iterator.next()? {
            let ints_ref = to_ints_ref(input, scratch_ints_ref);
            builder.add(ints_ref, output)?;
        }
        Ok(())
    }
}

fn encode_output(fp: i64, has_terms: bool, is_floor: bool) -> i64 {
    debug_assert!(fp < (1i64 << 62));
    let term_flag = if has_terms { OUTPUT_FLAGS_HAS_TERMS } else { 0 };
    let floor_flag = if is_floor { OUTPUT_FLAGS_IS_FLOOR } else { 0 };
    (fp << 2) | term_flag | floor_flag
}
