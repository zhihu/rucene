use core::codec::codec_util::index_header_length;
use core::codec::codec_util::*;
use core::codec::compressing::stored_fields::CompressingStoredFieldsIndexReader;
use core::codec::compressing::{CompressionMode, Decompress, Decompressor};
use core::codec::format::TermVectorsFormat;
use core::codec::reader::TermVectorsReader;
use core::codec::writer::TermVectorsWriter;
use core::index::field_info::FieldInfos;
use core::index::field_info::Fields;
use core::index::segment_file_name;
use core::index::term::{SeekStatus, TermIterator, TermState, Terms, TermsRef};
use core::index::SegmentInfo;
use core::search::posting_iterator::PostingIterator;
use core::search::{DocIterator, Payload, NO_MORE_DOCS};
use core::store::DirectoryRc;
use core::store::{IOContext, IndexInput};
use core::util::bit_util::UnsignedShift;
use core::util::packed_misc::get_mutable_by_ratio;
use core::util::packed_misc::get_reader_no_header;
use core::util::packed_misc::ReaderIterator;
use core::util::packed_misc::{get_reader_iterator_no_header, unsigned_bits_required};
use core::util::packed_misc::{BlockPackedReaderIterator, Format, Mutable, Reader};
use core::util::packed_misc::{OffsetAndLength, COMPACT};
use core::util::DocId;
use error::*;
use std::cmp::Ordering;
use std::sync::Arc;

#[derive(Clone)]
pub struct CompressingTermVectorsFormat {
    format_name: String,
    segment_suffix: String,
    compression_mode: CompressionMode,
    chunk_size: i32,
    block_size: i32,
}

impl CompressingTermVectorsFormat {
    pub fn new(
        format_name: String,
        segment_suffix: String,
        compression_mode: CompressionMode,
        chunk_size: i32,
        block_size: i32,
    ) -> CompressingTermVectorsFormat {
        debug_assert!(chunk_size >= 1 && block_size >= 1);
        CompressingTermVectorsFormat {
            format_name,
            segment_suffix,
            compression_mode,
            chunk_size,
            block_size,
        }
    }
}

impl Default for CompressingTermVectorsFormat {
    fn default() -> Self {
        CompressingTermVectorsFormat::new(
            String::from("Lucene50TermVectors"),
            String::new(),
            CompressionMode::FAST,
            1 << 12,
            1024,
        )
    }
}

impl TermVectorsFormat for CompressingTermVectorsFormat {
    fn vectors_reader(
        &self,
        directory: DirectoryRc,
        si: &SegmentInfo,
        field_info: Arc<FieldInfos>,
        ioctx: &IOContext,
    ) -> Result<Box<TermVectorsReader>> {
        CompressingTermVectorsReader::new(
            &directory,
            si,
            &self.segment_suffix,
            field_info,
            ioctx,
            &self.format_name,
            self.compression_mode.clone(),
        ).map(|tvf| -> Box<TermVectorsReader> { Box::new(tvf) })
    }

    fn vectors_writer(
        &self,
        _directory: DirectoryRc,
        _segment_info: &SegmentInfo,
        _context: &IOContext,
    ) -> Result<Box<TermVectorsWriter>> {
        unimplemented!()
    }
}

const VECTORS_EXTENSION: &str = "tvd";
const VECTORS_INDEX_EXTENSION: &str = "tvx";

const CODEC_SFX_IDX: &str = "Index";
const CODEC_SFX_DAT: &str = "Data";

const VERSION_START: i32 = 0;
const VERSION_CHUNK_STATS: i32 = 1;
const VERSION_CURRENT: i32 = VERSION_CHUNK_STATS;
const PACKED_BLOCK_SIZE: i32 = 64;

const POSITIONS: i32 = 0x01;
const OFFSETS: i32 = 0x02;
const PAYLOADS: i32 = 0x04;
const FLAGS_BITS: i32 = 3; // unsigned_bits_required((POSITIONS | OFFSETS | PAYLOADS) as i64)

pub struct CompressingTermVectorsReader {
    field_infos: Arc<FieldInfos>,
    index_reader: Arc<CompressingStoredFieldsIndexReader>,
    vectors_stream: Arc<IndexInput>,
    version: i32,
    packed_ints_version: i32,
    compression_mode: CompressionMode,
    decompressor: Decompressor,
    chunk_size: i32,
    num_docs: i32,
    reader: BlockPackedReaderIterator,
    num_chunks: i64,
    num_dirty_chunks: i64,
    max_pointer: i64,
}

impl CompressingTermVectorsReader {
    pub fn new(
        d: &DirectoryRc,
        si: &SegmentInfo,
        segment_suffix: &str,
        field_infos: Arc<FieldInfos>,
        context: &IOContext,
        format_name: &str,
        compression_mode: CompressionMode,
    ) -> Result<CompressingTermVectorsReader> {
        let num_docs = si.max_doc;
        // let mut index_reader: Option<CompressingStoredFieldsIndexReader> = None;
        // load the index into memory
        let index_name = segment_file_name(&si.name, segment_suffix, VECTORS_INDEX_EXTENSION);
        let mut input = d.as_ref().open_checksum_input(&index_name, context)?;
        let codec_name_idx = format!("{}{}", format_name, CODEC_SFX_IDX);
        let version = check_index_header(
            input.as_mut(),
            &codec_name_idx,
            VERSION_START,
            VERSION_CURRENT,
            &si.id,
            segment_suffix,
        )?;
        debug_assert_eq!(
            index_header_length(&codec_name_idx, segment_suffix),
            input.file_pointer() as usize
        );
        let index_reader = CompressingStoredFieldsIndexReader::new(input.as_mut(), si)?;
        let max_pointer = input.as_mut().read_vlong()?;
        // TODO once the input is a `ChecksumIndexInput`, we should check the footer
        check_footer(input.as_mut())?;
        // check_footer(input, exception);

        // open the data file and read metadata
        let vectors_stream_fn = segment_file_name(&si.name, segment_suffix, VECTORS_EXTENSION);
        let mut vectors_stream = d.open_input(&vectors_stream_fn, context)?;
        let codec_name_dat: String = String::from(format_name) + CODEC_SFX_DAT;
        let version2 = check_index_header(
            vectors_stream.as_mut(),
            &codec_name_dat,
            VERSION_START,
            VERSION_CURRENT,
            &si.id,
            segment_suffix,
        )?;
        if version != version2 {
            bail!("Corrupt: Version mismatch between stored fields and data!");
        }
        debug_assert_eq!(
            index_header_length(&codec_name_dat, segment_suffix),
            vectors_stream.file_pointer() as usize
        );

        let pos = vectors_stream.file_pointer();
        let mut num_chunks = -1i64;
        let mut num_dirty_chunks = -1i64;
        if version >= VERSION_CHUNK_STATS {
            vectors_stream.seek(max_pointer)?;
            num_chunks = vectors_stream.read_vlong()?;
            num_dirty_chunks = vectors_stream.read_vlong()?;
            if num_dirty_chunks > num_chunks {
                bail!(
                    "invalid chunk counts: dirty={}, total={}",
                    num_dirty_chunks,
                    num_chunks
                );
            }
        }
        retrieve_checksum(vectors_stream.as_mut())?;
        vectors_stream.seek(pos)?;
        let packed_ints_version = vectors_stream.read_vint()?;
        let chunk_size = vectors_stream.read_vint()?;
        let decompressor = compression_mode.new_decompressor();
        let reader = BlockPackedReaderIterator::new(
            vectors_stream.as_mut(),
            packed_ints_version,
            PACKED_BLOCK_SIZE as usize,
            0,
        );
        Ok(CompressingTermVectorsReader {
            field_infos,
            index_reader: Arc::new(index_reader),
            vectors_stream: Arc::from(vectors_stream),
            version,
            packed_ints_version,
            compression_mode,
            decompressor,
            chunk_size,
            num_docs,
            reader,
            num_chunks,
            num_dirty_chunks,
            max_pointer,
        })
    }

    // field -> term index -> position index
    fn position_index(
        &self,
        skip: usize,
        num_fields: usize,
        num_terms: &Reader,
        term_freqs: &[i32],
    ) -> Vec<Vec<i32>> {
        let mut position_index = Vec::with_capacity(num_fields);
        let mut term_index = 0usize;
        for i in 0..skip {
            let term_count = num_terms.get(i) as usize;
            term_index += term_count;
        }
        for i in 0..num_fields {
            let term_count = num_terms.get(skip + i) as usize;
            let mut local_indexes = Vec::with_capacity(term_count + 1usize);
            local_indexes.push(0i32);
            for j in 0..term_count {
                let freq: i32 = term_freqs[term_index + j];
                let prev = local_indexes[j];
                local_indexes.push(prev + freq);
            }
            position_index.push(local_indexes);
            term_index += term_count;
        }
        position_index
    }

    #[allow(too_many_arguments)]
    fn read_positions(
        &mut self,
        vectors_stream: &mut IndexInput,
        skip: usize,
        num_fields: usize,
        flags: &Reader,
        num_terms: &Reader,
        term_freqs: &[i32],
        flag: i32,
        total_positions: i32,
        position_index: &[Vec<i32>],
    ) -> Result<Vec<Vec<i32>>> {
        let mut positions = Vec::with_capacity(num_fields);
        self.reader.reset(i64::from(total_positions));
        // skip
        let mut to_skip = 0;
        let mut term_index = 0usize;
        for i in 0..skip {
            let f = flags.get(i) as i32;
            let term_count = num_terms.get(i) as usize;
            if (f & flag) != 0 {
                for j in 0..term_count {
                    let freq: i32 = term_freqs[term_index + j];
                    to_skip += freq;
                }
            }
            term_index += term_count;
        }

        self.reader.skip(vectors_stream, i64::from(to_skip))?;
        // read doc positions
        for (i, item) in position_index.iter().enumerate().take(num_fields) {
            let f = flags.get(skip + i) as i32;
            let term_count = num_terms.get(skip + i) as usize;
            if (f & flag) != 0 {
                let total_freq: i32 = item[term_count];
                let mut field_positions = Vec::with_capacity(total_freq as usize);
                let mut j = 0usize;
                while j < total_freq as usize {
                    let next_positions_off = self.reader
                        .next_longs_ref(vectors_stream, total_freq as usize - j)?;
                    for k in 0..next_positions_off.1 {
                        field_positions.push(self.reader.values[next_positions_off.0 + k] as i32);
                        j += 1;
                    }
                }
                positions.push(field_positions);
            } else {
                positions.push(Vec::with_capacity(0usize));
            }
            term_index += term_count;
        }
        let ord = self.reader.ord;
        self.reader
            .skip(vectors_stream, i64::from(total_positions) - ord)?;
        Ok(positions)
    }

    fn clone(&self) -> Result<Self> {
        Ok(CompressingTermVectorsReader {
            field_infos: self.field_infos.clone(),
            index_reader: self.index_reader.clone(),
            vectors_stream: Arc::from(IndexInput::clone(self.vectors_stream.as_ref())?),
            version: self.version,
            packed_ints_version: self.packed_ints_version,
            compression_mode: self.compression_mode.clone(),
            decompressor: self.decompressor.clone(),
            chunk_size: self.chunk_size,
            num_docs: self.num_docs,
            reader: self.reader.clone(),
            num_chunks: self.num_chunks,
            num_dirty_chunks: self.num_dirty_chunks,
            max_pointer: self.max_pointer,
        })
    }

    #[allow(cyclomatic_complexity)]
    fn get_mut(&mut self, doc: i32) -> Result<Box<Fields>> {
        let mut vectors_stream = self.vectors_stream.as_ref().clone()?;
        let vectors_stream = vectors_stream.as_mut();

        // seek to the right place
        {
            let start_pointer = self.index_reader.start_pointer(doc)?;
            vectors_stream.seek(start_pointer)?;
        }

        // decode
        // - doc_base: first doc ID of the chunk
        // - chunk_docs: number of docs of the chunk
        let doc_base = vectors_stream.read_vint()?;
        let chunk_docs = vectors_stream.read_vint()?;
        if doc < doc_base || doc >= doc_base + chunk_docs || doc_base + chunk_docs > self.num_docs {
            bail!(
                "Corrupt: doc_base={}, chunk_docs={}, doc={}",
                doc_base,
                chunk_docs,
                self.num_docs
            );
        }

        // number of fields to skip
        let skip;
        // number of fields of the document we're looking for
        let num_fields;
        // total number of fields of the chunk(sum for all docs)
        let total_fields = if chunk_docs == 1 {
            skip = 0usize;
            let value = vectors_stream.read_vint()? as usize;
            num_fields = value;
            value
        } else {
            self.reader.reset(i64::from(chunk_docs));
            let mut sum = 0i64;
            for _ in doc_base..doc {
                sum += self.reader.next(vectors_stream)?;
            }
            skip = sum as usize;
            num_fields = self.reader.next(vectors_stream)? as usize;
            sum += num_fields as i64;
            for _ in doc + 1..doc_base + chunk_docs {
                sum += self.reader.next(vectors_stream)?;
            }
            sum as usize
        };

        if num_fields == 0usize {
            // TODO maybe return None instead
            bail!("doc contains no fields!");
        }

        // read field numbers that have term vectors
        let token = i32::from(vectors_stream.read_byte()?) & 0xffi32;
        debug_assert_ne!(token, 0);
        let bits_per_field_num = token & 0x1f;
        let mut total_distinct_fields = token.unsigned_shift(5usize) as usize;
        if total_distinct_fields == 0x07 {
            total_distinct_fields += vectors_stream.read_vint()? as usize;
        }
        total_distinct_fields += 1;

        let mut it = get_reader_iterator_no_header(
            Format::Packed,
            self.packed_ints_version,
            total_distinct_fields,
            bits_per_field_num,
            1,
        )?;
        let mut field_nums = Vec::with_capacity(total_distinct_fields);
        for _ in 0..field_nums.capacity() {
            field_nums.push(it.next(vectors_stream)? as i32);
        }

        // read field numbers and flags
        let bits_per_off = unsigned_bits_required(field_nums.len() as i64 - 1);
        let all_field_num_off = get_reader_no_header(
            vectors_stream,
            Format::Packed,
            self.packed_ints_version,
            total_fields,
            bits_per_off,
        )?;
        // FIXME 下面定义的这两个 Option 实际是为了解决无法直接实现 Box<Mutable> -> Box<Reader> 的 work around
        let mut flags_reader: Option<Box<Reader>> = None;
        let mut flags_mutable: Option<Box<Mutable>> = None;
        match vectors_stream.read_vint()? {
            0 => {
                let field_flags = get_reader_no_header(
                    vectors_stream,
                    Format::Packed,
                    self.packed_ints_version,
                    field_nums.len(),
                    FLAGS_BITS,
                )?;
                let mut f = get_mutable_by_ratio(total_fields, FLAGS_BITS, COMPACT);
                for i in 0..total_fields {
                    let field_num_off = all_field_num_off.as_ref().get(i) as usize;
                    debug_assert!(field_num_off < field_nums.len());
                    let fgs = field_flags.as_ref().get(field_num_off);
                    f.as_mut().set(i, fgs);
                }
                flags_mutable = Some(f);
            }
            1 => {
                flags_reader = Some(get_reader_no_header(
                    vectors_stream,
                    Format::Packed,
                    self.packed_ints_version,
                    total_fields,
                    FLAGS_BITS,
                )?);
            }
            _ => {
                unreachable!();
            }
        }
        debug_assert!(flags_mutable.is_some() || flags_reader.is_some());
        let flags = if flags_mutable.is_some() {
            flags_mutable.as_ref().unwrap().as_ref().as_reader()
        } else {
            flags_reader.as_ref().unwrap().as_ref()
        };

        let field_num_offs: Vec<_> = (0..num_fields)
            .map(|i| all_field_num_off.get((skip + i) as usize) as i32)
            .collect();

        // number of terms per field for all fields
        let bits_required = vectors_stream.read_vint()?;
        let num_terms = get_reader_no_header(
            vectors_stream,
            Format::Packed,
            self.packed_ints_version,
            total_fields,
            bits_required,
        )?;

        let total_terms = (0..total_fields)
            .map(|i| num_terms.as_ref().get(i) as i32)
            .fold(0, |acc, x| acc + x) as usize;

        // term length
        let mut doc_off = 0;
        let mut doc_len = 0;
        let mut total_len;
        let mut field_lengths: Vec<i32> = Vec::with_capacity(num_fields);
        let mut prefix_lengths: Vec<Vec<i32>> = Vec::with_capacity(num_fields);
        let mut suffix_lengths: Vec<Vec<i32>> = Vec::with_capacity(num_fields);
        {
            self.reader.reset(total_terms as i64);
            // skip
            let mut to_skip = 0;
            for i in 0..skip {
                to_skip += num_terms.as_ref().get(i);
            }
            self.reader.skip(vectors_stream, to_skip)?;
            // read prefix lengths
            for i in 0..num_fields {
                let term_count = num_terms.as_ref().get(skip + i) as usize;
                let mut field_prefix_lengths = Vec::with_capacity(term_count as usize);
                let mut j = 0usize;
                while j < term_count {
                    let next = self.reader.next_longs_ref(vectors_stream, term_count - j)?;
                    for k in 0..next.1 {
                        field_prefix_lengths.push(self.reader.values[next.0 + k] as i32);
                        j += 1;
                    }
                }
                prefix_lengths.push(field_prefix_lengths);
            }
            let skip_size = total_terms as i64 - self.reader.ord;
            self.reader.skip(vectors_stream, skip_size)?;

            self.reader.reset(total_terms as i64);

            // skip
            for i in 0..skip {
                for _ in 0..num_terms.as_ref().get(i) {
                    doc_off += self.reader.next(vectors_stream)?;
                }
            }
            for i in 0..num_fields {
                let term_count = num_terms.get(skip + i) as usize;
                let mut field_suffix_lengths = Vec::with_capacity(term_count);
                let mut j = 0usize;
                while j < term_count {
                    let next = self.reader.next_longs_ref(vectors_stream, term_count - j)?;
                    for k in 0..next.1 {
                        field_suffix_lengths.push(self.reader.values[next.0 + k] as i32);
                        j += 1;
                    }
                }
                suffix_lengths.push(field_suffix_lengths);
                field_lengths.push(suffix_lengths[i].iter().sum());
                doc_len += field_lengths[i];
            }

            total_len = doc_off as i32 + doc_len;
            for i in skip + num_fields..total_fields {
                for _j in 0..num_terms.as_ref().get(i) {
                    total_len += self.reader.next(vectors_stream)? as i32;
                }
            }
        }

        // term freqs
        let mut term_freqs: Vec<i32> = Vec::with_capacity(total_terms);
        {
            self.reader.reset(total_terms as i64);
            let mut i = 0usize;
            while i < total_terms {
                let next = self.reader.next_longs_ref(vectors_stream, total_terms - i)?;
                for k in 0..next.1 {
                    term_freqs.push(1 + self.reader.values[next.0 + k] as i32);
                    i += 1;
                }
            }
        }

        // total number of positions, offsets and payloads
        let mut total_positions = 0;
        let mut total_offsets = 0;
        let mut total_payloads = 0;
        let mut term_index = 0usize;
        for i in 0..total_fields {
            let f = flags.get(i) as i32;
            let term_count = num_terms.get(i) as i32;
            for _j in 0..term_count {
                let freq = term_freqs[term_index];
                term_index += 1;
                if (f & POSITIONS) != 0 {
                    total_positions += freq;
                }
                if (f & OFFSETS) != 0 {
                    total_offsets += freq;
                }
                if (f & PAYLOADS) != 0 {
                    total_payloads += freq;
                }
            }
            debug_assert!(i != total_fields - 1usize || term_index == total_terms);
        }

        let position_index = self.position_index(skip, num_fields, num_terms.as_ref(), &term_freqs);
        let mut positions = if total_positions > 0 {
            self.read_positions(
                vectors_stream,
                skip,
                num_fields,
                flags,
                num_terms.as_ref(),
                &term_freqs,
                POSITIONS,
                total_positions,
                &position_index,
            )?
        } else {
            vec![Vec::with_capacity(0); num_fields]
        };

        let mut start_offsets: Vec<Vec<i32>> = Vec::with_capacity(num_fields);
        let mut lengths: Vec<Vec<i32>> = Vec::with_capacity(num_fields);
        if total_offsets > 0 {
            let mut chars_per_term = Vec::with_capacity(field_nums.len());
            for _ in 0..chars_per_term.capacity() {
                chars_per_term.push(f32::from_bits(vectors_stream.read_int()? as u32));
            }
            start_offsets = self.read_positions(
                vectors_stream,
                skip,
                num_fields,
                flags,
                num_terms.as_ref(),
                &term_freqs,
                OFFSETS,
                total_offsets,
                &position_index,
            )?;
            lengths = self.read_positions(
                vectors_stream,
                skip,
                num_fields,
                flags,
                num_terms.as_ref(),
                &term_freqs,
                OFFSETS,
                total_offsets,
                &position_index,
            )?;

            for i in 0..num_fields {
                let mut f_start_offsets = &mut start_offsets[i];
                let mut f_positions = &mut positions[i];
                if !f_start_offsets.is_empty() && !f_positions.is_empty() {
                    let field_chars_per_term: f32 = chars_per_term[field_num_offs[i] as usize];
                    for j in 0..f_start_offsets.len() {
                        f_start_offsets[j] += (field_chars_per_term * f_positions[j] as f32) as i32;
                    }
                }
                if !f_start_offsets.is_empty() {
                    let f_prefix_lengths = &prefix_lengths[i];
                    let f_suffix_lengths = &suffix_lengths[i];
                    let mut f_lengths = &mut lengths[i];
                    let end = num_terms.get(skip + i) as usize;
                    for j in 0..end {
                        let term_length = f_prefix_lengths[j] + f_suffix_lengths[j];
                        f_lengths[position_index[i][j] as usize] += term_length;
                        for k in
                            (position_index[i][j] + 1) as usize..(position_index[i][j + 1]) as usize
                        {
                            f_start_offsets[k] += f_start_offsets[k - 1];
                            f_lengths[k] += term_length;
                        }
                    }
                }
            }
        } else {
            start_offsets.resize(num_fields, Vec::with_capacity(0));
            lengths.resize(num_fields, Vec::with_capacity(0));
        }
        if total_positions > 0 {
            // delta-decode positions
            for i in 0..num_fields {
                let mut f_positions = &mut positions[i];
                let f_position_index = &position_index[i];
                if !f_positions.is_empty() {
                    let end = num_terms.get(skip + i) as usize;
                    for j in 0..end {
                        for k in (f_position_index[j] + 1) as usize
                            ..f_position_index[j + 1usize] as usize
                        {
                            f_positions[k] += f_positions[k - 1usize];
                        }
                    }
                }
            }
        }

        // payload lengths
        let payload_index = Vec::with_capacity(num_fields);
        let mut total_payload_length = 0;
        let mut payload_off = 0;
        let mut payload_len = 0;
        if total_payloads > 0 {
            self.reader.reset(i64::from(total_payloads));
            // skip
            let mut term_index = 0usize;
            for i in 0..skip {
                let f = flags.get(i) as i32;
                let term_count = num_terms.as_ref().get(i) as usize;
                if (f & PAYLOADS) != 0 {
                    for j in 0..term_count {
                        let freq = term_freqs[term_index + j];
                        for _k in 0..freq {
                            payload_off += self.reader.next(vectors_stream)? as i32;
                        }
                    }
                }
                term_index += term_count;
            }
            total_payload_length = payload_off;
            // read doc payload lengths
            for (i, item) in position_index.iter().enumerate().take(num_fields) {
                let f = flags.get(skip + i) as i32;
                let term_count = num_terms.as_ref().get(skip + i) as usize;
                if (f & PAYLOADS) != 0 {
                    let total_freq = item[term_count];
                    let mut cur_payload_index = Vec::with_capacity((total_freq + 1) as usize);
                    cur_payload_index.push(payload_len);
                    for j in 0..term_count {
                        let freq = term_freqs[term_index + j];
                        for _ in 0..freq {
                            let payload_length = self.reader.next(vectors_stream)? as i32;
                            payload_len += payload_length;
                            cur_payload_index.push(payload_len);
                        }
                    }
                    debug_assert_eq!(cur_payload_index.len(), cur_payload_index.capacity());
                }
                term_index += term_count;
            }
            total_payload_length += payload_len;
            for i in skip + num_fields..total_fields {
                let f = flags.get(i) as i32;
                let term_count = num_terms.as_ref().get(i) as usize;
                if (f & PAYLOADS) != 0 {
                    for j in 0..term_count {
                        let freq = term_freqs[term_index + j];
                        for _k in 0..freq {
                            total_payload_length += self.reader.next(vectors_stream)? as i32;
                        }
                    }
                }
                term_index += term_count;
            }
            debug_assert_eq!(term_index, total_terms);
        }

        // decompress data
        let mut suffix_bytes = vec![0u8; (total_len + total_payload_length) as usize * 2];
        let mut suffix_bytes_position = OffsetAndLength(0, 0);
        self.decompressor.decompress(
            vectors_stream,
            (total_len + total_payload_length) as usize,
            (doc_off + i64::from(payload_off)) as usize,
            (doc_len + payload_len) as usize,
            &mut suffix_bytes,
            &mut suffix_bytes_position,
        )?;
        suffix_bytes_position.1 = doc_len as usize;
        // payload bytes ref(offset and length) in suffix_bytes
        let payload_bytes_position = OffsetAndLength(
            suffix_bytes_position.0 + doc_len as usize,
            payload_len as usize,
        );

        let mut field_flags = Vec::with_capacity(num_fields);
        for i in 0..num_fields {
            field_flags.push(flags.get(skip + i) as i32);
        }
        let mut field_num_terms = Vec::with_capacity(num_fields);
        for i in 0..num_fields {
            field_num_terms.push(num_terms.as_ref().get(skip + i) as i32);
        }

        let mut field_term_freqs = Vec::with_capacity(num_fields);
        {
            let mut term_idx = 0usize;
            for i in 0..skip {
                term_idx += num_terms.as_ref().get(i) as usize;
            }
            for i in 0..num_fields {
                let term_count = num_terms.as_ref().get(skip + i) as usize;
                let mut cur_term_freqs = Vec::with_capacity(term_count);
                for _ in 0..term_count {
                    cur_term_freqs.push(term_freqs[term_idx]);
                    term_idx += 1;
                }
                field_term_freqs.push(cur_term_freqs);
            }
        }

        debug_assert_eq!(field_lengths.iter().sum::<i32>(), doc_len);
        Ok(Box::new(TVFields::new(
            self.field_infos.clone(),
            field_nums,
            field_flags,
            field_num_offs,
            field_num_terms,
            field_lengths,
            prefix_lengths,
            suffix_lengths,
            field_term_freqs,
            position_index,
            positions,
            start_offsets,
            lengths,
            payload_index,
            suffix_bytes,
            suffix_bytes_position,
            payload_bytes_position,
        )))
    }
}

impl TermVectorsReader for CompressingTermVectorsReader {
    fn get(&self, doc: i32) -> Result<Box<Fields>> {
        self.clone()?.get_mut(doc)
    }
}

struct TVFieldsData {
    pub prefix_lengths: Vec<Vec<i32>>,
    pub suffix_lengths: Vec<Vec<i32>>,
    pub term_freqs: Vec<Vec<i32>>,
    pub position_index: Vec<Vec<i32>>,
    pub positions: Vec<Vec<i32>>,
    pub start_offsets: Vec<Vec<i32>>,
    pub lengths: Vec<Vec<i32>>,
    pub payload_index: Vec<Vec<i32>>,
    pub suffix_bytes: Vec<u8>,
}

struct TVFields {
    pub field_infos: Arc<FieldInfos>,
    pub field_nums: Vec<i32>,
    pub field_flags: Vec<i32>,
    pub field_num_offs: Vec<i32>,
    pub num_terms: Vec<i32>,
    pub field_lengths: Vec<i32>,
    pub fields_data: Arc<TVFieldsData>,
    // position in field_data.suffix_bytes
    pub suffix_bytes_position: OffsetAndLength,
    pub payload_bytes_position: OffsetAndLength,
}

impl TVFields {
    #[allow(too_many_arguments)]
    pub fn new(
        field_infos: Arc<FieldInfos>,
        field_nums: Vec<i32>,
        field_flags: Vec<i32>,
        field_num_offs: Vec<i32>,
        num_terms: Vec<i32>,
        field_lengths: Vec<i32>,
        prefix_lengths: Vec<Vec<i32>>,
        suffix_lengths: Vec<Vec<i32>>,
        term_freqs: Vec<Vec<i32>>,
        position_index: Vec<Vec<i32>>,
        positions: Vec<Vec<i32>>,
        start_offsets: Vec<Vec<i32>>,
        lengths: Vec<Vec<i32>>,
        payload_index: Vec<Vec<i32>>,
        suffix_bytes: Vec<u8>,
        suffix_bytes_position: OffsetAndLength,
        // payload bytes offset and length in suffix_bytes
        payload_bytes_position: OffsetAndLength,
    ) -> TVFields {
        let fields_data = Arc::new(TVFieldsData {
            prefix_lengths,
            suffix_lengths,
            term_freqs,
            position_index,
            positions,
            start_offsets,
            lengths,
            payload_index,
            suffix_bytes,
        });
        TVFields {
            field_infos,
            field_nums,
            field_flags,
            field_num_offs,
            num_terms,
            field_lengths,
            fields_data,
            suffix_bytes_position,
            payload_bytes_position,
        }
    }
}

impl Fields for TVFields {
    fn fields(&self) -> Vec<String> {
        let mut field_names = Vec::with_capacity(self.field_num_offs.len());
        for i in 0..self.field_num_offs.len() {
            let field_num = self.field_nums[self.field_num_offs[i] as usize];
            field_names.push(self.field_infos.by_number[&field_num].name.clone());
        }
        field_names
    }

    fn terms(&self, field: &str) -> Result<Option<TermsRef>> {
        let field_info = self.field_infos.by_name.get(field);
        if field_info.is_none() {
            return Ok(None);
        }
        let mut idx = -1;
        let field_info = field_info.unwrap();
        for i in 0..self.field_num_offs.len() {
            if self.field_nums[self.field_num_offs[i] as usize] == field_info.number {
                idx = i as i32;
            }
        }

        if idx == -1 || self.num_terms[idx as usize] == 0 {
            // no term
            return Ok(None);
        }
        let mut field_off = 0;
        let mut field_len = -1;
        for i in 0..self.field_num_offs.len() {
            if (i as i32) < idx {
                field_off += self.field_lengths[i];
            } else {
                field_len = self.field_lengths[i];
                break;
            }
        }
        debug_assert!(field_len >= 0);
        let index = idx as usize;
        Ok(Some(Arc::new(TVTerms::new(
            self.num_terms[index],
            self.field_flags[index],
            self.fields_data.clone(),
            index,
            OffsetAndLength(
                self.suffix_bytes_position.0 + field_off as usize,
                field_len as usize,
            ),
            self.payload_bytes_position.clone(),
        ))))
    }

    fn size(&self) -> usize {
        self.field_num_offs.len()
    }
}

struct TVTerms {
    num_terms: i32,
    flags: i32,
    fields_data: Arc<TVFieldsData>,
    data_index: usize,
    term_bytes_position: OffsetAndLength,
    // for term input
    payload_bytes_position: OffsetAndLength,
}

impl TVTerms {
    pub fn new(
        num_terms: i32,
        flags: i32,
        fields_data: Arc<TVFieldsData>,
        data_index: usize,
        term_bytes_position: OffsetAndLength,
        payload_bytes_position: OffsetAndLength,
    ) -> TVTerms {
        TVTerms {
            num_terms,
            flags,
            fields_data,
            data_index,
            term_bytes_position,
            payload_bytes_position,
        }
    }
}

impl Terms for TVTerms {
    fn iterator(&self) -> Result<Box<TermIterator>> {
        Ok(Box::new(TVTermsIterator::new(
            self.num_terms,
            self.flags,
            self.fields_data.clone(),
            self.data_index,
            self.term_bytes_position.clone(),
            self.payload_bytes_position.clone(),
        )))
    }

    fn size(&self) -> Result<i64> {
        Ok(i64::from(self.num_terms))
    }

    fn sum_total_term_freq(&self) -> Result<i64> {
        Ok(-1i64)
    }

    fn sum_doc_freq(&self) -> Result<i64> {
        Ok(i64::from(self.num_terms))
    }

    fn doc_count(&self) -> Result<i32> {
        Ok(1)
    }

    fn has_freqs(&self) -> Result<bool> {
        Ok(true)
    }

    fn has_offsets(&self) -> Result<bool> {
        Ok((self.flags & OFFSETS) != 0)
    }

    fn has_positions(&self) -> Result<bool> {
        Ok((self.flags & POSITIONS) != 0)
    }

    fn has_payloads(&self) -> Result<bool> {
        Ok((self.flags & PAYLOADS) != 0)
    }
}

struct TVTermsIterator {
    num_terms: i32,
    start_pos: usize,
    ord: i32,
    fields_data: Arc<TVFieldsData>,
    data_index: usize,
    payload_bytes_position: OffsetAndLength,
    // offset and length in bytes_data for payload_bytes
    term_bytes_position: OffsetAndLength,
    // offset and length in bytes_data for term_bytes
    term: Vec<u8>,
}

impl TVTermsIterator {
    pub fn new(
        num_terms: i32,
        _flag: i32,
        fields_data: Arc<TVFieldsData>,
        data_index: usize,
        term_bytes_position: OffsetAndLength,
        payload_bytes_position: OffsetAndLength,
    ) -> TVTermsIterator {
        TVTermsIterator {
            num_terms,
            start_pos: term_bytes_position.0,
            ord: -1,
            fields_data,
            data_index,
            term_bytes_position,
            payload_bytes_position,
            term: Vec::with_capacity(16usize),
        }
    }

    fn reset(&mut self) {
        self.term.clear();
        self.term_bytes_position.0 = self.start_pos;
        self.ord = -1;
    }

    // TODO a copy of next() but just return a ref to avoid copy bytes vector
    fn next_local(&mut self) -> Result<&[u8]> {
        if self.ord == self.num_terms - 1 {
            return Ok(&self.term[0..0]);
        } else {
            debug_assert!(self.ord < self.num_terms);
            self.ord += 1;
        }

        // read term
        let ord = self.ord as usize;
        let prefix_len = self.fields_data.prefix_lengths[self.data_index][ord] as usize;
        let suffix_len = self.fields_data.suffix_lengths[self.data_index][ord] as usize;
        let term_len = prefix_len + suffix_len;
        if self.term.len() < term_len {
            for _ in 0..term_len - self.term.len() {
                self.term.push(0u8);
            }
        } else if self.term.len() > term_len {
            self.term.truncate(term_len);
        }

        if self.term_bytes_position.0 + suffix_len > self.fields_data.suffix_bytes.len() {
            bail!("not enough data to copy!");
        } else {
            self.term[prefix_len..term_len].copy_from_slice(
                &self.fields_data.suffix_bytes
                    [self.term_bytes_position.0..self.term_bytes_position.0 + suffix_len],
            );
            self.term_bytes_position.0 += suffix_len;
            self.term_bytes_position.1 -= suffix_len;
        }

        Ok(&self.term)
    }
}

impl TermIterator for TVTermsIterator {
    fn next(&mut self) -> Result<Vec<u8>> {
        Ok(Vec::from(self.next_local()?))
    }

    fn seek_ceil(&mut self, text: &[u8]) -> Result<SeekStatus> {
        if self.ord < self.num_terms && self.ord >= 0 {
            // let term_ref: &[u8] = self.term.as_ref();
            let cmp = text.cmp(self.term.as_ref());
            if cmp == Ordering::Equal {
                return Ok(SeekStatus::Found);
            } else if cmp == Ordering::Less {
                self.reset();
            }
        }

        // linear scan
        loop {
            let term = self.next_local()?;
            if term.is_empty() {
                return Ok(SeekStatus::End);
            }
            let cmp = term.cmp(text);
            if cmp == Ordering::Greater {
                return Ok(SeekStatus::NotFound);
            } else if cmp == Ordering::Equal {
                return Ok(SeekStatus::Found);
            }
        }
    }

    fn seek_exact_ord(&mut self, _ord: i64) -> Result<()> {
        // Not Supported Operation
        unimplemented!()
    }

    fn term(&mut self) -> Result<Vec<u8>> {
        Ok(self.term.clone())
    }

    fn ord(&mut self) -> Result<i64> {
        // Not Supported Operation
        unimplemented!()
    }

    fn doc_freq(&mut self) -> Result<i32> {
        Ok(1)
    }

    fn total_term_freq(&mut self) -> Result<i64> {
        Ok(i64::from(
            self.fields_data.term_freqs[self.data_index][self.ord as usize],
        ))
    }

    fn postings_with_flags(&mut self, _flags: i16) -> Result<Box<PostingIterator>> {
        Ok(Box::new(TVPostingsIterator::new(
            self.fields_data.term_freqs[self.data_index][self.ord as usize],
            self.fields_data.position_index[self.data_index][self.ord as usize],
            self.fields_data.clone(),
            self.data_index,
            self.payload_bytes_position.0,
        )))
    }

    fn term_state(&mut self) -> Result<Box<TermState>> {
        unimplemented!()
    }
}

struct TVPostingsIterator {
    doc: DocId,
    term_freq: i32,
    position_index: i32,
    fields_data: Arc<TVFieldsData>,
    data_index: usize,
    payload_position: OffsetAndLength,
    base_payload_offset: usize,
    i: i32,
}

impl TVPostingsIterator {
    pub fn new(
        freq: i32,
        position_index: i32,
        fields_data: Arc<TVFieldsData>,
        data_index: usize,
        base_payload_offset: usize,
    ) -> TVPostingsIterator {
        TVPostingsIterator {
            doc: -1,
            term_freq: freq,
            position_index,
            fields_data,
            data_index,
            payload_position: OffsetAndLength(0, 0),
            base_payload_offset,
            i: -1,
        }
    }

    fn check_doc(&self) -> Result<()> {
        if self.doc == NO_MORE_DOCS {
            bail!("DocsEnum exhausted");
        } else if self.doc == -1 {
            bail!("DocsEnum not started");
        }
        Ok(())
    }

    fn check_position(&self) -> Result<()> {
        self.check_doc()?;
        if self.i < 0 {
            bail!("Position enum not started");
        } else if self.i >= self.term_freq {
            bail!("Read past last position");
        }
        Ok(())
    }

    pub fn clone(&self) -> Self {
        TVPostingsIterator::new(
            self.term_freq,
            self.position_index,
            self.fields_data.clone(),
            self.data_index,
            self.base_payload_offset,
        )
    }
}

impl PostingIterator for TVPostingsIterator {
    fn clone_as_doc_iterator(&self) -> Result<Box<DocIterator>> {
        Ok(Box::new(self.clone()))
    }

    fn freq(&self) -> Result<i32> {
        self.check_doc()?;
        Ok(self.term_freq)
    }

    fn next_position(&mut self) -> Result<i32> {
        if self.doc != 0 {
            bail!("Illegal State!");
        } else if self.i > self.term_freq - 1 {
            bail!("Read past last position");
        }

        self.i += 1;

        if self.data_index < self.fields_data.payload_index.len() {
            let payload_index = &self.fields_data.payload_index[self.data_index];
            if !payload_index.is_empty() {
                self.payload_position.0 = self.base_payload_offset
                    + payload_index[(self.position_index + self.i) as usize] as usize;
                self.payload_position.1 = (payload_index
                    [(self.position_index + self.i + 1) as usize]
                    - payload_index[(self.position_index + self.i) as usize])
                    as usize;
            }
        }

        if self.data_index >= self.fields_data.positions.len()
            || self.fields_data.positions[self.data_index].is_empty()
        {
            Ok(-1)
        } else {
            Ok(
                self.fields_data.positions[self.data_index]
                    [(self.position_index + self.i) as usize],
            )
        }
    }

    fn start_offset(&self) -> Result<i32> {
        self.check_position()?;
        if !self.fields_data.start_offsets.is_empty() {
            Ok(self.fields_data.start_offsets[self.data_index]
                [(self.position_index + self.i) as usize])
        } else {
            Ok(-1)
        }
    }

    fn end_offset(&self) -> Result<i32> {
        self.check_position()?;
        if !self.fields_data.start_offsets[self.data_index].is_empty() {
            Ok(self.fields_data.start_offsets[self.data_index]
                [(self.position_index + self.i) as usize]
                + self.fields_data.lengths[self.data_index]
                    [(self.position_index + self.i) as usize])
        } else {
            Ok(-1)
        }
    }

    fn payload(&self) -> Result<Payload> {
        self.check_position()?;
        if self.fields_data.payload_index[self.data_index].is_empty()
            || self.payload_position.1 == 0
        {
            Ok(Vec::with_capacity(0))
        } else {
            Ok(Vec::from(
                &self.fields_data.suffix_bytes
                    [self.payload_position.0..self.payload_position.0 + self.payload_position.1],
            ))
        }
    }
}

impl DocIterator for TVPostingsIterator {
    fn doc_id(&self) -> DocId {
        self.doc
    }

    fn next(&mut self) -> Result<DocId> {
        if self.doc == -1 {
            self.doc = 0;
        } else {
            self.doc = NO_MORE_DOCS;
        }
        Ok(self.doc)
    }

    fn advance(&mut self, target: DocId) -> Result<DocId> {
        self.slow_advance(target)
    }

    fn cost(&self) -> usize {
        1usize
    }
}
