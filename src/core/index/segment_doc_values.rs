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

use core::codec::{Codec, DocValuesFormat, DocValuesProducer};
use core::index::FieldInfos;
use core::index::{SegmentCommitInfo, SegmentReadState};
use core::store::{Directory, IOContext};
use core::util::numeric::to_base36;
use error::ErrorKind::IllegalState;
use error::Result;

use std::sync::Arc;

/// Manage the `DocValuesProducer` held by `SegmentReader`.
pub struct SegmentDocValues;

impl SegmentDocValues {
    pub fn get_doc_values_producer<D: Directory, DW: Directory, C: Codec>(
        gen: i64,
        si: &SegmentCommitInfo<D, C>,
        dir: Arc<DW>,
        infos: Arc<FieldInfos>,
    ) -> Result<Box<dyn DocValuesProducer>> {
        if gen != -1 {
            Self::do_get_doc_values_producer(
                si,
                Arc::clone(&si.info.directory),
                infos,
                to_base36(gen as u64),
            )
        } else {
            Self::do_get_doc_values_producer(si, dir, infos, "".into())
        }
    }

    fn do_get_doc_values_producer<D: Directory, DW: Directory, C: Codec>(
        si: &SegmentCommitInfo<D, C>,
        dv_dir: Arc<DW>,
        infos: Arc<FieldInfos>,
        segment_suffix: String,
    ) -> Result<Box<dyn DocValuesProducer>> {
        let srs = SegmentReadState::new(
            dv_dir,
            &si.info,
            infos,
            &IOContext::READ_ONCE,
            segment_suffix,
        );

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
