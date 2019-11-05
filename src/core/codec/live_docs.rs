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

use std::collections::HashSet;
use std::sync::Arc;

use core::codec::codec_util::{check_footer, check_index_header};
use core::codec::codec_util::{write_footer, write_index_header};
use core::codec::segment_infos::{file_name_from_generation, SegmentCommitInfo};
use core::codec::Codec;
use core::store::directory::Directory;
use core::store::io::{DataInput, DataOutput};
use core::store::IOContext;
use core::util::to_base36;
use core::util::{bits2words, BitSet, FixedBitSet, ImmutableBitSet};
use core::util::{Bits, BitsRef};
use error::ErrorKind::CorruptIndex;
use error::Result;

/// Format for live/deleted documents
pub trait LiveDocsFormat {
    /// Creates a new Bits, with all bits set, for the specified size.
    fn new_live_docs(&self, size: usize) -> Result<FixedBitSet>;
    /// Creates a new mutablebits of the same bits set and size of existing.
    fn new_live_docs_from_existing(&self, existing: &dyn Bits) -> Result<BitsRef>;
    /// Read live docs bits.
    fn read_live_docs<D: Directory, C: Codec>(
        &self,
        dir: Arc<D>,
        info: &SegmentCommitInfo<D, C>,
        context: &IOContext,
    ) -> Result<BitsRef>;

    /// Persist live docs bits.  Use {@link
    /// SegmentCommitInfo#getNextDelGen} to determine the
    /// generation of the deletes file you should write to. */
    fn write_live_docs<D: Directory, D1: Directory, C: Codec>(
        &self,
        bits: &dyn Bits,
        dir: &D1,
        info: &SegmentCommitInfo<D, C>,
        new_del_count: i32,
        context: &IOContext,
    ) -> Result<()>;

    /// Records all files in use by this {@link SegmentCommitInfo} into the files argument. */
    fn files<D: Directory, C: Codec>(
        &self,
        info: &SegmentCommitInfo<D, C>,
        files: &mut HashSet<String>,
    );
}

const EXTENSION: &str = "liv";
const CODEC_NAME: &str = "Lucene50LiveDocs";
const VERSION_START: i32 = 0;
const VERSION_CURRENT: i32 = VERSION_START;

/// Lucene 5.0 live docs format
#[derive(Copy, Clone)]
pub struct Lucene50LiveDocsFormat {}

impl LiveDocsFormat for Lucene50LiveDocsFormat {
    fn new_live_docs(&self, size: usize) -> Result<FixedBitSet> {
        let mut bits = FixedBitSet::new(size);
        bits.batch_set(0, size);
        Ok(bits)
    }

    fn new_live_docs_from_existing(&self, existing: &dyn Bits) -> Result<BitsRef> {
        // existing is type of FixedBitSet
        Ok(existing.clone_box())
    }

    fn read_live_docs<D: Directory, C: Codec>(
        &self,
        dir: Arc<D>,
        info: &SegmentCommitInfo<D, C>,
        context: &IOContext,
    ) -> Result<BitsRef> {
        let gen = info.del_gen();
        let name = file_name_from_generation(info.info.name.as_str(), EXTENSION, gen as u64);
        let length = info.info.max_doc as usize;

        let mut index_stream = dir.open_checksum_input(name.as_str(), context)?;
        check_index_header(
            &mut index_stream,
            CODEC_NAME,
            VERSION_START,
            VERSION_CURRENT,
            &info.info.id,
            to_base36(gen as u64).as_str(),
        )?;

        let num_words = bits2words(length);
        let mut bits: Vec<i64> = Vec::with_capacity(num_words);
        for _ in 0..num_words {
            bits.push(index_stream.read_long()?);
        }

        check_footer(&mut index_stream)?;

        let fix_bits = FixedBitSet::copy_from(bits, length)?;
        if fix_bits.len() - fix_bits.cardinality() == info.del_count() as usize {
            Ok(Arc::new(fix_bits))
        } else {
            bail!(CorruptIndex(format!(
                "bits.deleted= {} info.delcount= {}",
                fix_bits.len() - fix_bits.cardinality(),
                info.del_count()
            )))
        }
    }

    fn write_live_docs<D: Directory, D1: Directory, C: Codec>(
        &self,
        bits: &dyn Bits,
        dir: &D1,
        info: &SegmentCommitInfo<D, C>,
        new_del_count: i32,
        context: &IOContext,
    ) -> Result<()> {
        let gen = info.next_write_del_gen();
        let name = file_name_from_generation(&info.info.name, EXTENSION, gen as u64);
        let fbs = bits.as_bit_set().as_fixed_bit_set();

        if fbs.len() - fbs.cardinality() != (info.del_count() + new_del_count) as usize {
            bail!(CorruptIndex(format!(
                "index: {},  bits.deleted={}, info.del_count={}, new_del_count={}",
                &name,
                fbs.len() - fbs.cardinality(),
                info.del_count(),
                new_del_count
            )));
        }

        let mut output = dir.create_output(&name, context)?;
        write_index_header(
            &mut output,
            CODEC_NAME,
            VERSION_CURRENT,
            info.info.get_id(),
            &to_base36(gen as u64),
        )?;
        for v in &fbs.bits {
            output.write_long(*v)?;
        }
        write_footer(&mut output)
    }

    fn files<D: Directory, C: Codec>(
        &self,
        info: &SegmentCommitInfo<D, C>,
        files: &mut HashSet<String>,
    ) {
        if info.has_deletions() {
            let new_file =
                file_name_from_generation(&info.info.name, EXTENSION, info.del_gen() as u64);
            files.insert(new_file);
        }
    }
}
