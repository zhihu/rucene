use std::collections::HashSet;
use std::sync::Arc;

use core::codec::codec_util::{check_footer, check_index_header};
use core::codec::codec_util::{write_footer, write_index_header};
use core::codec::format::LiveDocsFormat;
use core::index::{file_name_from_generation, SegmentCommitInfo};
use core::store::{Directory, DirectoryRc, IOContext};
use core::util::bit_set::{bits2words, BitSet, FixedBitSet, ImmutableBitSet};
use core::util::numeric::to_base36;
use core::util::{Bits, BitsRef};
use error::ErrorKind::CorruptIndex;
use error::Result;

const EXTENSION: &str = "liv";
const CODEC_NAME: &str = "Lucene50LiveDocs";
const VERSION_START: i32 = 0;
const VERSION_CURRENT: i32 = VERSION_START;

pub struct Lucene50LiveDocsFormat {}

impl LiveDocsFormat for Lucene50LiveDocsFormat {
    fn new_live_docs(&self, size: usize) -> Result<Box<Bits>> {
        let mut bits = FixedBitSet::new(size);
        bits.batch_set(0, size);
        Ok(Box::new(bits))
    }

    fn new_live_docs_from_existing(&self, existing: &Bits) -> Result<BitsRef> {
        // existing is type of FixedBitSet
        Ok(existing.clone())
    }

    fn read_live_docs(
        &self,
        dir: DirectoryRc,
        info: &SegmentCommitInfo,
        context: &IOContext,
    ) -> Result<BitsRef> {
        let gen = info.del_gen();
        let name = file_name_from_generation(info.info.name.as_str(), EXTENSION, gen as u64);
        let length = info.info.max_doc as usize;

        let mut index_stream = dir.open_checksum_input(name.as_str(), context)?;
        check_index_header(
            index_stream.as_mut(),
            CODEC_NAME,
            VERSION_START,
            VERSION_CURRENT,
            &info.info.id,
            to_base36(gen as u64).as_str(),
        )?;

        let num_words = bits2words(length);
        let mut bits: Vec<i64> = Vec::with_capacity(num_words);
        for _ in 0..num_words {
            bits.push(index_stream.as_mut().read_long()?);
        }

        check_footer(index_stream.as_mut())?;

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

    fn write_live_docs(
        &self,
        bits: &Bits,
        dir: &Directory,
        info: &SegmentCommitInfo,
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
            output.as_mut(),
            CODEC_NAME,
            VERSION_CURRENT,
            info.info.get_id(),
            &to_base36(gen as u64),
        )?;
        for v in &fbs.bits {
            output.write_long(*v)?;
        }
        write_footer(output.as_mut())
    }

    fn files(&self, info: &SegmentCommitInfo, files: &mut HashSet<String>) {
        if info.has_deletions() {
            let new_file =
                file_name_from_generation(&info.info.name, EXTENSION, info.del_gen() as u64);
            files.insert(new_file);
        }
    }
}
