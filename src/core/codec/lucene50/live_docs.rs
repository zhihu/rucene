use std::sync::Arc;

use core::codec::codec_util::{check_footer, check_index_header};
use core::codec::format::LiveDocsFormat;
use core::index::{file_name_from_generation, SegmentCommitInfo};
use core::store::{DirectoryRc, IOContext};
use core::util::numeric::to_base36;
use core::util::{Bits, FixedBits};
use error::ErrorKind::CorruptIndex;
use error::*;

const EXTENSION: &str = "liv";
const CODEC_NAME: &str = "Lucene50LiveDocs";
const VERSION_START: i32 = 0;
const VERSION_CURRENT: i32 = VERSION_START;

pub struct Lucene50LiveDocsFormat {}

impl LiveDocsFormat for Lucene50LiveDocsFormat {
    fn new_live_docs(&self, _size: i32) -> Result<Bits> {
        unimplemented!()
    }

    fn new_live_docs_from_existing(&self, _existing: &Bits) -> Result<Bits> {
        unimplemented!()
    }

    fn read_live_docs(
        &self,
        dir: DirectoryRc,
        info: &SegmentCommitInfo,
        context: &IOContext,
    ) -> Result<Bits> {
        let gen = info.del_gen;
        let name = file_name_from_generation(info.info.name.as_str(), EXTENSION, gen);
        let length = info.info.max_doc as usize;

        let mut index_stream = dir.open_checksum_input(name.as_str(), context)?;
        check_index_header(
            index_stream.as_mut(),
            CODEC_NAME,
            VERSION_START,
            VERSION_CURRENT,
            info.info.id.as_slice(),
            to_base36(gen).as_str(),
        )?;

        let num_words = FixedBits::bits_2_words(length);
        let mut bits: Vec<i64> = Vec::with_capacity(num_words);
        for _ in 0..num_words {
            bits.push(index_stream.as_mut().read_long()?);
        }

        check_footer(index_stream.as_mut())?;

        let fix_bits = FixedBits::new(Arc::new(bits), length);
        if fix_bits.length() - fix_bits.cardinality() == info.del_count as usize {
            Ok(Bits::new(Box::new(fix_bits)))
        } else {
            bail!(CorruptIndex(format!(
                "bits.deleted= {} info.delcount= {}",
                fix_bits.length() - fix_bits.cardinality(),
                info.del_count
            )))
        }
    }

    fn write_live_docs(
        &self,
        _bits: &Bits,
        _dir: DirectoryRc,
        _info: &SegmentCommitInfo,
        _new_del_count: i32,
        _context: &IOContext,
    ) -> Result<()> {
        unimplemented!()
    }

    fn files(&self, _info: &SegmentCommitInfo, _files: &[String]) -> Result<()> {
        unimplemented!()
    }
}
