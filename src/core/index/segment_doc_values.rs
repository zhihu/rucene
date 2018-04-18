use core::codec::DocValuesProducer;
use core::index::FieldInfos;
use core::index::{SegmentCommitInfo, SegmentReadState};
use core::store::{DirectoryRc, IO_CONTEXT_READONCE};
use core::util::to_base36;
use error::ErrorKind::IllegalState;
use error::Result;

use std::sync::Arc;

/// Manage the `DocValuesProducer` held by `SegmentReader`.
pub struct SegmentDocValues;

impl SegmentDocValues {
    pub fn get_doc_values_producer(
        gen: i64,
        si: &SegmentCommitInfo,
        dir: DirectoryRc,
        infos: Arc<FieldInfos>,
    ) -> Result<Box<DocValuesProducer>> {
        let (dv_dir, segment_suffix) = if gen != -1 {
            (Arc::clone(&si.info.directory), to_base36(gen))
        } else {
            (dir, "".to_owned())
        };

        let srs =
            SegmentReadState::new(dv_dir, &si.info, infos, IO_CONTEXT_READONCE, segment_suffix);

        match si.info.codec {
            None => bail!(IllegalState("si.info.codec can't be None".to_owned())),
            Some(ref codec) => {
                let dv_format = codec.doc_values_format();
                let dv_producer = dv_format.fields_producer(&srs)?;
                Ok(dv_producer)
            }
        }
    }
}
