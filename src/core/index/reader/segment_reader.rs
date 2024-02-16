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

use std::cell::RefCell;
use std::collections::{hash_map::Entry, HashMap, HashSet};
use std::fmt;
use std::mem;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};

use error::{ErrorKind::IllegalArgument, ErrorKind::IllegalState, Result};
use thread_local::{CachedThreadLocal, ThreadLocal};

use core::codec::doc_values::{
    BinaryDocValues, BinaryDocValuesProvider, DocValuesFormat, DocValuesProducer,
    DocValuesProviderEnum, NumericDocValues, NumericDocValuesProvider, SortedDocValues,
    SortedDocValuesProvider, SortedNumericDocValues, SortedNumericDocValuesProvider,
    SortedSetDocValues, SortedSetDocValuesProvider,
};

use core::codec::field_infos::{FieldInfo, FieldInfos, FieldInfosFormat};
use core::codec::norms::{NormsFormat, NormsProducer};
use core::codec::points::PointsFormat;
use core::codec::postings::PostingsFormat;
use core::codec::segment_infos::{SegmentCommitInfo, SegmentInfo, SegmentReadState};
use core::codec::stored_fields::{StoredFieldsFormat, StoredFieldsReader};
use core::codec::term_vectors::{TermVectorsFormat, TermVectorsReader};
use core::codec::{
    Codec, CodecFieldsProducer, CodecNormsProducer, CodecPointsReader, CodecStoredFieldsReader,
    CodecTVFields, CodecTVReader, CompoundFormat, LiveDocsFormat, Lucene50CompoundReader,
};
use core::doc::{DocValuesType, Document, DocumentStoredFieldVisitor, StoredFieldVisitor};
use core::index::reader::{IndexReader, LeafReader, LeafReaderContext};
use core::search::sort_field::Sort;
use core::store::directory::Directory;
use core::store::io::{BufferedChecksumIndexInput, IndexInput};
use core::store::IOContext;
use core::util::external::Deferred;
use core::util::{id2str, random_id, to_base36, BitsMut, BitsRef, DocId, MatchAllBits};

/// Holds core readers that are shared (unchanged) when
/// SegmentReader is cloned or reopened
pub struct SegmentCoreReaders<D: Directory, C: Codec> {
    _codec: Arc<C>,
    pub fields: <C::PostingFmt as PostingsFormat>::FieldsProducer,
    pub norms_producer: Option<Arc<CodecNormsProducer<C>>>,
    pub fields_reader: Arc<CodecStoredFieldsReader<C>>,
    pub term_vectors_reader: Option<Arc<CodecTVReader<C>>>,
    pub segment: String,
    pub cfs_reader: Option<Arc<CfsDirectory<D>>>,
    /// fieldinfos for this core: means gen=-1.
    /// this is the exact fieldinfos these codec components saw at write.
    /// in the case of DV updates, SR may hold a newer version.
    pub core_field_infos: Arc<FieldInfos>,
    pub points_reader: Option<Arc<CodecPointsReader<C>>>,
    pub core_dropped_listeners: Mutex<Vec<Deferred>>,
    pub core_cache_key: String,
}

impl<D: Directory, C: Codec> SegmentCoreReaders<D, C> {
    pub fn new(
        dir: &Arc<D>,
        si: &SegmentInfo<D, C>,
        ctx: &IOContext,
    ) -> Result<SegmentCoreReaders<D, C>> {
        let codec = si.codec();

        let cfs_dir = if si.is_compound_file() {
            Arc::new(CfsDirectory::Cfs(
                codec
                    .compound_format()
                    .get_compound_reader(dir.clone(), si, ctx)?,
            ))
        } else {
            Arc::new(CfsDirectory::Raw(Arc::clone(dir)))
        };

        let cfs_reader = if si.is_compound_file() {
            Some(Arc::clone(&cfs_dir))
        } else {
            None
        };

        let segment = si.name.clone();
        let core_field_infos = Arc::new(codec.field_infos_format().read(
            cfs_dir.as_ref(),
            si,
            "",
            &ctx,
        )?);
        let segment_read_state = SegmentReadState::new(
            cfs_dir.clone(),
            si,
            core_field_infos.clone(),
            ctx,
            String::new(),
        );

        let norms_producer = if core_field_infos.has_norms {
            Some(codec.norms_format().norms_producer(&segment_read_state)?)
        } else {
            None
        };

        let format = codec.postings_format();
        let fields = format.fields_producer(&segment_read_state)?;

        let fields_reader = codec.stored_fields_format().fields_reader(
            &*cfs_dir,
            si,
            core_field_infos.clone(),
            ctx,
        )?;
        let term_vectors_reader = if core_field_infos.has_vectors {
            let reader = codec.term_vectors_format().tv_reader(
                &*cfs_dir,
                si,
                core_field_infos.clone(),
                ctx,
            )?;
            Some(Arc::new(reader))
        } else {
            None
        };
        let points_reader = if core_field_infos.has_point_values {
            Some(Arc::new(
                codec.points_format().fields_reader(&segment_read_state)?,
            ))
        } else {
            None
        };
        // TODO process norms_producers/store_fields_reader/term vectors

        Ok(SegmentCoreReaders {
            _codec: Arc::clone(codec),
            fields,
            fields_reader: Arc::new(fields_reader),
            norms_producer: norms_producer.map(Arc::new),
            term_vectors_reader,
            segment,
            cfs_reader,
            core_field_infos,
            points_reader,
            core_dropped_listeners: Mutex::new(vec![]),
            core_cache_key: format!("{}@{}", si.name, id2str(&random_id())),
        })
    }

    pub fn fields(&self) -> &<C::PostingFmt as PostingsFormat>::FieldsProducer {
        &self.fields
    }

    pub fn add_core_drop_listener(&self, listener: Deferred) {
        let mut guard = self.core_dropped_listeners.lock().unwrap();
        guard.push(listener);
    }

    pub fn core_field_infos(&self) -> Arc<FieldInfos> {
        Arc::clone(&self.core_field_infos)
    }

    //    fn field_info(&self, field: &str) -> Option<&FieldInfo> {
    //        self.core_field_infos.field_info_by_name(field)
    //    }
    //
    //    fn sorted_doc_values(&self, _field: &str) -> Box<dyn SortedDocValues> {
    //        unimplemented!()
    //    }
    //
    //    fn sorted_set_doc_values(&self, _field: &str) -> Box<dyn SortedSetDocValues> {
    //        unimplemented!()
    //    }
}

impl<D: Directory, C: Codec> Drop for SegmentCoreReaders<D, C> {
    fn drop(&mut self) {
        let mut listeners_guard = self.core_dropped_listeners.lock().unwrap();
        let listeners = mem::replace(&mut *listeners_guard, Vec::with_capacity(0));
        for listener in listeners {
            listener.call();
        }
    }
}

pub enum CfsDirectory<D: Directory> {
    Raw(Arc<D>),
    Cfs(Lucene50CompoundReader<D>),
}

impl<D: Directory> Directory for CfsDirectory<D> {
    type IndexOutput = D::IndexOutput;
    type TempOutput = D::TempOutput;

    fn list_all(&self) -> Result<Vec<String>> {
        match self {
            CfsDirectory::Raw(d) => d.list_all(),
            CfsDirectory::Cfs(d) => d.list_all(),
        }
    }

    fn file_length(&self, name: &str) -> Result<i64> {
        match self {
            CfsDirectory::Raw(d) => d.file_length(name),
            CfsDirectory::Cfs(d) => d.file_length(name),
        }
    }

    fn create_output(&self, name: &str, context: &IOContext) -> Result<Self::IndexOutput> {
        match self {
            CfsDirectory::Raw(d) => d.create_output(name, context),
            CfsDirectory::Cfs(d) => d.create_output(name, context),
        }
    }

    fn open_input(&self, name: &str, ctx: &IOContext) -> Result<Box<dyn IndexInput>> {
        match self {
            CfsDirectory::Raw(d) => d.open_input(name, ctx),
            CfsDirectory::Cfs(d) => d.open_input(name, ctx),
        }
    }

    fn open_checksum_input(
        &self,
        name: &str,
        ctx: &IOContext,
    ) -> Result<BufferedChecksumIndexInput> {
        match self {
            CfsDirectory::Raw(d) => d.open_checksum_input(name, ctx),
            CfsDirectory::Cfs(d) => d.open_checksum_input(name, ctx),
        }
    }

    fn create_temp_output(
        &self,
        prefix: &str,
        suffix: &str,
        ctx: &IOContext,
    ) -> Result<Self::TempOutput> {
        match self {
            CfsDirectory::Raw(d) => d.create_temp_output(prefix, suffix, ctx),
            CfsDirectory::Cfs(d) => d.create_temp_output(prefix, suffix, ctx),
        }
    }

    fn delete_file(&self, name: &str) -> Result<()> {
        match self {
            CfsDirectory::Raw(d) => d.delete_file(name),
            CfsDirectory::Cfs(d) => d.delete_file(name),
        }
    }

    fn sync(&self, name: &HashSet<String>) -> Result<()> {
        match self {
            CfsDirectory::Raw(d) => d.sync(name),
            CfsDirectory::Cfs(d) => d.sync(name),
        }
    }

    fn sync_meta_data(&self) -> Result<()> {
        match self {
            CfsDirectory::Raw(d) => d.sync_meta_data(),
            CfsDirectory::Cfs(d) => d.sync_meta_data(),
        }
    }

    fn rename(&self, source: &str, dest: &str) -> Result<()> {
        match self {
            CfsDirectory::Raw(d) => d.rename(source, dest),
            CfsDirectory::Cfs(d) => d.rename(source, dest),
        }
    }

    fn copy_from<D1: Directory>(
        &self,
        from: Arc<D1>,
        src: &str,
        dest: &str,
        ctx: &IOContext,
    ) -> Result<()> {
        match self {
            CfsDirectory::Raw(d) => d.copy_from(from, src, dest, ctx),
            CfsDirectory::Cfs(d) => d.copy_from(from, src, dest, ctx),
        }
    }

    fn create_files(&self) -> HashSet<String> {
        match self {
            CfsDirectory::Raw(d) => d.create_files(),
            CfsDirectory::Cfs(d) => d.create_files(),
        }
    }

    fn resolve(&self, name: &str) -> PathBuf {
        match self {
            CfsDirectory::Raw(d) => d.resolve(name),
            CfsDirectory::Cfs(d) => d.resolve(name),
        }
    }
}

impl<D: Directory> fmt::Display for CfsDirectory<D> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CfsDirectory::Raw(d) => write!(f, "CfsDirectory({})", d.as_ref()),
            CfsDirectory::Cfs(d) => write!(f, "CfsDirectory({})", d),
        }
    }
}

/// Manage the `DocValuesProducer` held by `SegmentReader`.
#[derive(Clone)]
pub struct SegmentDocValues {
    dv_producers_by_field: HashMap<String, Arc<dyn DocValuesProducer>>,
}

impl SegmentDocValues {
    pub fn new<D: Directory, DW: Directory, C: Codec>(
        si: &SegmentCommitInfo<D, C>,
        dir: Arc<DW>,
        infos: Arc<FieldInfos>,
    ) -> Self {
        let mut dv_producers_by_field: HashMap<String, Arc<dyn DocValuesProducer>> = HashMap::new();

        if let Ok(default_dv_producer) = Self::get_dv_producer(-1, si, dir.clone(), infos.clone()) {
            let default_dv_producer: Arc<dyn DocValuesProducer> = Arc::from(default_dv_producer);

            for (field, fi) in &infos.by_name {
                if fi.doc_values_type == DocValuesType::Null {
                    continue;
                }

                if fi.dv_gen == -1 {
                    // not updated field
                    dv_producers_by_field.insert(field.clone(), default_dv_producer.clone());
                } else {
                    // updated field
                    if let Ok(field_infos) = FieldInfos::new(vec![fi.as_ref().clone()]) {
                        let field_infos = Arc::new(field_infos);
                        if let Ok(dvp) =
                            Self::get_dv_producer(fi.dv_gen, si, dir.clone(), field_infos)
                        {
                            dv_producers_by_field.insert(fi.name.clone(), Arc::from(dvp));
                        } else {
                            error!(
                                "segment {} get_dv_producer for {} error.",
                                si.info.name, fi.name
                            );
                        }
                    } else {
                        error!(
                            "segment {} new field_infos for {} error.",
                            si.info.name, fi.name
                        );
                    }
                }
            }
        } else {
            error!("segment {} get_dv_producer for -1 error.", si.info.name);
        }

        Self {
            dv_producers_by_field,
        }
    }

    pub fn get_dv_producer<D: Directory, DW: Directory, C: Codec>(
        gen: i64,
        si: &SegmentCommitInfo<D, C>,
        dir: Arc<DW>,
        infos: Arc<FieldInfos>,
    ) -> Result<Box<dyn DocValuesProducer>> {
        if gen != -1 {
            Self::do_get_dv_producer(
                si,
                Arc::clone(&si.info.directory),
                infos,
                to_base36(gen as u64),
            )
        } else {
            Self::do_get_dv_producer(si, dir, infos, "".into())
        }
    }

    fn do_get_dv_producer<D: Directory, DW: Directory, C: Codec>(
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

    pub fn get_dv_provider(
        dv_producer: Arc<dyn DocValuesProducer>,
        field_infos: Arc<FieldInfos>,
    ) -> HashMap<String, DocValuesProviderEnum> {
        let mut dvs = HashMap::new();

        for (field_name, field_info) in field_infos.by_name.iter() {
            match field_info.doc_values_type {
                DocValuesType::Binary => {
                    if let Ok(cell) = dv_producer.get_binary(field_info) {
                        dvs.insert(
                            field_name.to_string(),
                            DocValuesProviderEnum::Binary(Arc::clone(&cell)),
                        );
                    }
                }
                DocValuesType::Numeric => {
                    if let Ok(cell) = dv_producer.get_numeric(field_info) {
                        dvs.insert(
                            field_name.to_string(),
                            DocValuesProviderEnum::Numeric(Arc::clone(&cell)),
                        );
                    }
                }
                DocValuesType::SortedNumeric => {
                    if let Ok(cell) = dv_producer.get_sorted_numeric(field_info) {
                        dvs.insert(
                            field_name.to_string(),
                            DocValuesProviderEnum::SortedNumeric(Arc::clone(&cell)),
                        );
                    }
                }
                DocValuesType::Sorted => {
                    if let Ok(cell) = dv_producer.get_sorted(field_info) {
                        dvs.insert(
                            field_name.to_string(),
                            DocValuesProviderEnum::Sorted(Arc::clone(&cell)),
                        );
                    }
                }
                DocValuesType::SortedSet => {
                    if let Ok(cell) = dv_producer.get_sorted_set(field_info) {
                        dvs.insert(
                            field_name.to_string(),
                            DocValuesProviderEnum::SortedSet(Arc::clone(&cell)),
                        );
                    }
                }
                _ => {}
            }
        }

        dvs
    }
}

impl DocValuesProducer for SegmentDocValues {
    fn get_numeric(&self, field_info: &FieldInfo) -> Result<Arc<dyn NumericDocValuesProvider>> {
        let dv_producer = self.dv_producers_by_field.get(&field_info.name).unwrap();
        dv_producer.get_numeric(field_info)
    }

    fn get_binary(&self, field_info: &FieldInfo) -> Result<Arc<dyn BinaryDocValuesProvider>> {
        let dv_producer = self.dv_producers_by_field.get(&field_info.name).unwrap();
        dv_producer.get_binary(field_info)
    }

    fn get_sorted(&self, field_info: &FieldInfo) -> Result<Arc<dyn SortedDocValuesProvider>> {
        let dv_producer = self.dv_producers_by_field.get(&field_info.name).unwrap();
        dv_producer.get_sorted(field_info)
    }

    fn get_sorted_numeric(
        &self,
        field_info: &FieldInfo,
    ) -> Result<Arc<dyn SortedNumericDocValuesProvider>> {
        let dv_producer = self.dv_producers_by_field.get(&field_info.name).unwrap();
        dv_producer.get_sorted_numeric(field_info)
    }

    fn get_sorted_set(
        &self,
        field_info: &FieldInfo,
    ) -> Result<Arc<dyn SortedSetDocValuesProvider>> {
        let dv_producer = self.dv_producers_by_field.get(&field_info.name).unwrap();
        dv_producer.get_sorted_set(field_info)
    }

    fn get_docs_with_field(&self, field_info: &FieldInfo) -> Result<Box<dyn BitsMut>> {
        let dv_producer = self.dv_producers_by_field.get(&field_info.name).unwrap();
        dv_producer.get_docs_with_field(field_info)
    }

    fn check_integrity(&self) -> Result<()> {
        Ok(())
    }

    fn get_merge_instance(&self) -> Result<Box<dyn DocValuesProducer>> {
        Ok(Box::new(self.clone()))
    }
}

pub type ThreadLocalDocValueProducer = ThreadLocal<Arc<dyn DocValuesProducer>>;

pub struct SegmentReader<D: Directory, C: Codec> {
    pub si: Arc<SegmentCommitInfo<D, C>>,
    pub live_docs: BitsRef,
    num_docs: i32,
    pub core: Arc<SegmentCoreReaders<D, C>>,
    pub is_nrt: bool,
    pub field_infos: Arc<FieldInfos>,
    dv_producers_preload: Arc<RwLock<Vec<Arc<dyn DocValuesProducer>>>>,
    dv_producer_local: ThreadLocalDocValueProducer,
    dv_providers_preload: Arc<RwLock<Vec<RefCell<HashMap<String, DocValuesProviderEnum>>>>>,
    dv_provider_local: CachedThreadLocal<RefCell<HashMap<String, DocValuesProviderEnum>>>,
}

unsafe impl<D: Directory + Send + Sync + 'static, C: Codec> Sync for SegmentReader<D, C> {}

/// IndexReader implementation over a single segment.
/// Instances pointing to the same segment (but with different deletes, etc)
/// may share the same core data.
/// @lucene.experimental
impl<D: Directory + 'static, C: Codec> SegmentReader<D, C> {
    pub fn new(
        si: Arc<SegmentCommitInfo<D, C>>,
        live_docs: BitsRef,
        num_docs: i32,
        core: Arc<SegmentCoreReaders<D, C>>,
        is_nrt: bool,
        field_infos: Arc<FieldInfos>,
    ) -> SegmentReader<D, C> {
        let max_preload_num = num_cpus::get_physical() * 3;
        let mut dv_producers_preload: Vec<Arc<dyn DocValuesProducer>> =
            Vec::with_capacity(max_preload_num);
        let mut dv_providers_preload: Vec<RefCell<HashMap<String, DocValuesProviderEnum>>> =
            Vec::with_capacity(max_preload_num);

        for _ in 0..max_preload_num {
            if let Some(dv_producer) =
                SegmentReader::get_dv_producer(core.as_ref(), si.as_ref(), field_infos.clone())
            {
                dv_producers_preload.push(dv_producer);
            }
        }

        let dv_producer_local = ThreadLocal::new();
        if let Some(dv_producer) = dv_producers_preload.pop() {
            for _ in 0..max_preload_num {
                let dvs =
                    SegmentDocValues::get_dv_provider(dv_producer.clone(), field_infos.clone());
                dv_providers_preload.push(RefCell::new(dvs));
            }
            dv_producer_local.get_or(|| Box::new(dv_producer));
        }

        let dv_provider_local = CachedThreadLocal::new();
        if let Some(dv_preload) = dv_providers_preload.pop() {
            dv_provider_local.get_or(|| Box::new(dv_preload));
        }

        SegmentReader {
            si,
            live_docs,
            num_docs,
            core,
            is_nrt,
            field_infos,
            dv_producers_preload: Arc::new(RwLock::new(dv_producers_preload)),
            dv_producer_local,
            dv_providers_preload: Arc::new(RwLock::new(dv_providers_preload)),
            dv_provider_local,
        }
    }

    pub fn build(
        si: Arc<SegmentCommitInfo<D, C>>,
        live_docs: BitsRef,
        num_docs: i32,
        core: Arc<SegmentCoreReaders<D, C>>,
    ) -> Result<Self> {
        let field_infos = Self::init_field_infos(si.as_ref(), core.as_ref())?;
        Ok(Self::new(si, live_docs, num_docs, core, true, field_infos))
    }

    pub fn build_from_reader(
        si: Arc<SegmentCommitInfo<D, C>>,
        sr: &SegmentReader<D, C>,
    ) -> Result<Self> {
        let live_docs: BitsRef = if si.has_deletions() {
            si.info.codec().live_docs_format().read_live_docs(
                Arc::clone(&si.info.directory),
                si.as_ref(),
                &IOContext::READ_ONCE,
            )?
        } else {
            Arc::new(MatchAllBits::new(si.info.max_doc as usize))
        };
        let num_docs = si.info.max_doc() - si.del_count();
        Self::build_from(si, sr, live_docs, num_docs, false)
    }

    pub fn build_from(
        si: Arc<SegmentCommitInfo<D, C>>,
        sr: &SegmentReader<D, C>,
        live_docs: BitsRef,
        num_docs: i32,
        is_nrt: bool,
    ) -> Result<SegmentReader<D, C>> {
        if num_docs > si.info.max_doc {
            bail!(IllegalArgument(format!(
                "num_docs={}, but max_docs={}",
                num_docs, si.info.max_doc
            )));
        }
        if live_docs.len() != si.info.max_doc as usize {
            bail!(IllegalArgument(format!(
                "max_doc={}, but live_docs.len()={}",
                si.info.max_doc,
                live_docs.len()
            )));
        }

        let field_infos = Self::init_field_infos(si.as_ref(), sr.core.as_ref())?;

        Ok(SegmentReader::new(
            si,
            live_docs,
            num_docs,
            Arc::clone(&sr.core),
            is_nrt,
            field_infos,
        ))
    }

    pub fn max_docs(&self) -> i32 {
        self.si.info.max_doc()
    }

    pub fn num_docs(&self) -> i32 {
        self.num_docs
    }

    pub fn num_deleted_docs(&self) -> i32 {
        self.max_docs() - self.num_docs
    }

    pub fn check_bounds(&self, doc_id: DocId) {
        debug_assert!(
            doc_id >= 0 && doc_id < self.max_docs(),
            "doc_id={} max_docs={}", doc_id, self.max_docs()
        );
    }

    /// Constructs a new SegmentReader with a new core.
    /// @throws CorruptIndexException if the index is corrupt
    /// @throws IOException if there is a low-level IO error
    pub fn open(si: &Arc<SegmentCommitInfo<D, C>>, ctx: &IOContext) -> Result<SegmentReader<D, C>> {
        let core = Arc::new(SegmentCoreReaders::new(&si.info.directory, &si.info, ctx)?);
        let codec = si.info.codec();
        let num_docs = si.info.max_doc() - si.del_count();
        let field_infos = if !si.has_field_updates() {
            Arc::clone(&core.core_field_infos)
        } else {
            let fis_format = codec.field_infos_format();
            let segment_suffix = to_base36(si.field_infos_gen() as u64);
            let field_infos = fis_format.read(
                si.info.directory.as_ref(),
                &si.info,
                &segment_suffix,
                &IOContext::READ_ONCE,
            )?;
            Arc::new(field_infos)
        };

        let live_docs = if si.has_deletions() {
            codec.live_docs_format().read_live_docs(
                Arc::clone(&si.info.directory),
                si,
                &IOContext::READ,
            )?
        } else {
            assert_eq!(si.del_count(), 0);
            Arc::new(MatchAllBits::new(si.info.max_doc() as usize))
        };

        Ok(SegmentReader::new(
            Arc::clone(si),
            live_docs,
            num_docs,
            core,
            false,
            field_infos,
        ))
    }

    fn get_dv_field(&self, field: &str, dv_type: DocValuesType) -> Option<&FieldInfo> {
        match self.field_infos.field_info_by_name(field) {
            Some(fi) if fi.doc_values_type == dv_type => Some(fi),
            _ => None,
        }
    }

    pub fn leaf_context(&self) -> LeafReaderContext<C> {
        LeafReaderContext::new(self, self, 0, 0)
    }
}

impl<D: Directory + 'static, C: Codec> SegmentReader<D, C> {
    fn get_dv_producer(
        core: &SegmentCoreReaders<D, C>,
        si: &SegmentCommitInfo<D, C>,
        field_infos: Arc<FieldInfos>,
    ) -> Option<Arc<dyn DocValuesProducer>> {
        // initDocValuesProducer: init most recent DocValues for the current commit
        let dir = match core.cfs_reader {
            Some(ref d) => Arc::clone(d),
            None => Arc::new(CfsDirectory::Raw(Arc::clone(&si.info.directory))),
        };

        if !field_infos.has_doc_values {
            None
        } else if si.has_field_updates() {
            let dv_producer: Box<dyn DocValuesProducer> =
                Box::new(SegmentDocValues::new(&si, dir, field_infos));

            Some(Arc::from(dv_producer))
        } else {
            // simple case, no DocValues updates
            if let Ok(dv_producer) =
                SegmentDocValues::get_dv_producer(-1_i64, &si, dir, field_infos)
            {
                Some(Arc::from(dv_producer))
            } else {
                None
            }
        }
    }

    fn init_local_dv_producer(&self) -> Result<()> {
        if self.field_infos.has_doc_values {
            if self.dv_producer_local.get().is_some() {
                return Ok(());
            }

            if let Some(dv_producer) = self.dv_producers_preload.write()?.pop() {
                self.dv_producer_local.get_or(|| Box::new(dv_producer));

                return Ok(());
            }

            if let Some(dv_producer) = SegmentReader::get_dv_producer(
                self.core.as_ref(),
                self.si.as_ref(),
                self.field_infos.clone(),
            ) {
                self.dv_producer_local.get_or(|| Box::new(dv_producer));
            }
        }
        Ok(())
    }

    fn init_field_infos(
        si: &SegmentCommitInfo<D, C>,
        core: &SegmentCoreReaders<D, C>,
    ) -> Result<Arc<FieldInfos>> {
        if !si.has_field_updates() {
            Ok(Arc::clone(&core.core_field_infos))
        } else {
            // updates always outside of CFS
            let fis_format = si.info.codec().field_infos_format();
            let segment_suffix = to_base36(si.field_infos_gen() as u64);
            Ok(Arc::new(fis_format.read(
                si.info.directory.as_ref(),
                &si.info,
                &segment_suffix,
                &IOContext::READ_ONCE,
            )?))
        }
    }

    fn dv_provider_local(&self) -> Result<&RefCell<HashMap<String, DocValuesProviderEnum>>> {
        self.init_local_dv_producer()?;

        Ok(self.dv_provider_local.get_or(|| {
            if let Some(dv_preload) = self.dv_providers_preload.write().unwrap().pop() {
                Box::new(dv_preload)
            } else {
                Box::new(RefCell::new(HashMap::new()))
            }
        }))
    }
}

impl<D: Directory + 'static, C: Codec> IndexReader for SegmentReader<D, C> {
    type Codec = C;
    fn leaves(&self) -> Vec<LeafReaderContext<C>> {
        vec![self.leaf_context()]
    }

    fn term_vector(&self, doc_id: i32) -> Result<Option<CodecTVFields<C>>> {
        LeafReader::term_vector(self, doc_id)
    }

    fn document(&self, doc_id: i32, fields: &[String]) -> Result<Document> {
        let mut visitor = DocumentStoredFieldVisitor::new(&fields);
        LeafReader::document(self, doc_id, &mut visitor)?;
        Ok(visitor.document())
    }

    fn max_doc(&self) -> i32 {
        LeafReader::max_doc(self)
    }

    fn num_docs(&self) -> i32 {
        LeafReader::num_docs(self)
    }

    fn leaf_reader_for_doc(&self, doc: i32) -> LeafReaderContext<C> {
        debug_assert!(doc <= IndexReader::max_doc(self));
        self.leaf_context()
    }
}

impl<D, C> LeafReader for SegmentReader<D, C>
where
    D: Directory + 'static,
    C: Codec,
{
    type Codec = C;
    type FieldsProducer = CodecFieldsProducer<C>;
    type TVFields = CodecTVFields<C>;
    type TVReader = Arc<CodecTVReader<C>>;
    type StoredReader = Arc<CodecStoredFieldsReader<C>>;
    type NormsReader = Arc<CodecNormsProducer<C>>;
    type PointsReader = Arc<CodecPointsReader<C>>;

    fn codec(&self) -> &Self::Codec {
        self.si.info.codec().as_ref()
    }

    fn fields(&self) -> Result<Self::FieldsProducer> {
        Ok(self.core.fields().clone())
    }

    fn name(&self) -> &str {
        &self.si.info.name
    }

    fn term_vector(&self, doc_id: DocId) -> Result<Option<CodecTVFields<C>>> {
        self.check_bounds(doc_id);
        if let Some(ref reader) = self.core.term_vectors_reader {
            reader.get(doc_id)
        } else {
            Ok(None)
        }
    }

    fn document(&self, doc_id: DocId, visitor: &mut dyn StoredFieldVisitor) -> Result<()> {
        self.check_bounds(doc_id);
        self.core.fields_reader.visit_document(doc_id, visitor)
    }

    fn live_docs(&self) -> BitsRef {
        Arc::clone(&self.live_docs)
    }

    fn field_info(&self, field: &str) -> Option<&FieldInfo> {
        self.field_infos.field_info_by_name(field)
    }

    fn field_infos(&self) -> &FieldInfos {
        &self.field_infos
    }

    fn clone_field_infos(&self) -> Arc<FieldInfos> {
        Arc::clone(&self.field_infos)
    }

    fn max_doc(&self) -> DocId {
        self.si.info.max_doc
    }

    fn num_docs(&self) -> i32 {
        self.num_docs
    }

    fn get_numeric_doc_values(&self, field: &str) -> Result<Box<dyn NumericDocValues>> {
        match self
            .dv_provider_local()?
            .borrow_mut()
            .entry(String::from(field))
        {
            Entry::Occupied(o) => match *o.get() {
                DocValuesProviderEnum::Numeric(ref dv) => dv.get(),
                _ => bail!(IllegalArgument(format!(
                    "non-numeric dv found for field {}",
                    field
                ))),
            },
            Entry::Vacant(v) => match self.get_dv_field(field, DocValuesType::Numeric) {
                Some(fi) if self.dv_producer_local.get().is_some() => {
                    let dv_producer = self.dv_producer_local.get().unwrap();
                    let cell = dv_producer.get_numeric(fi)?;
                    v.insert(DocValuesProviderEnum::Numeric(Arc::clone(&cell)));
                    cell.get()
                }
                _ => bail!(IllegalArgument(format!(
                    "non-dv-segment or non-exist or non-numeric field: {}",
                    field
                ))),
            },
        }
    }

    fn get_binary_doc_values(&self, field: &str) -> Result<Box<dyn BinaryDocValues>> {
        match self
            .dv_provider_local()?
            .borrow_mut()
            .entry(String::from(field))
        {
            Entry::Occupied(o) => match *o.get() {
                DocValuesProviderEnum::Binary(ref dv) => dv.get(),
                _ => bail!(IllegalArgument(format!(
                    "non-binary dv found for field {}",
                    field
                ))),
            },
            Entry::Vacant(v) => match self.get_dv_field(field, DocValuesType::Binary) {
                Some(fi) if self.dv_producer_local.get().is_some() => {
                    let dv_producer = self.dv_producer_local.get().unwrap();
                    let dv = dv_producer.get_binary(fi)?;
                    v.insert(DocValuesProviderEnum::Binary(Arc::clone(&dv)));
                    dv.get()
                }
                _ => bail!(IllegalArgument(format!(
                    "non-dv-segment or non-exist or non-binary field: {}",
                    field
                ))),
            },
        }
    }

    fn get_sorted_doc_values(&self, field: &str) -> Result<Box<dyn SortedDocValues>> {
        match self
            .dv_provider_local()?
            .borrow_mut()
            .entry(String::from(field))
        {
            Entry::Occupied(o) => match *o.get() {
                DocValuesProviderEnum::Sorted(ref dv) => dv.get(),
                _ => bail!(IllegalArgument(format!(
                    "non-sorted dv found for field {}",
                    field
                ))),
            },
            Entry::Vacant(v) => match self.get_dv_field(field, DocValuesType::Sorted) {
                Some(fi) if self.dv_producer_local.get().is_some() => {
                    let dv_producer = self.dv_producer_local.get().unwrap();
                    let dv = dv_producer.get_sorted(fi)?;
                    v.insert(DocValuesProviderEnum::Sorted(Arc::clone(&dv)));
                    dv.get()
                }
                _ => bail!(IllegalArgument(format!(
                    "non-dv-segment or non-exist or non-sorted field: {}",
                    field
                ))),
            },
        }
    }

    fn get_sorted_numeric_doc_values(
        &self,
        field: &str,
    ) -> Result<Box<dyn SortedNumericDocValues>> {
        match self
            .dv_provider_local()?
            .borrow_mut()
            .entry(String::from(field))
        {
            Entry::Occupied(o) => match *o.get() {
                DocValuesProviderEnum::SortedNumeric(ref dv) => dv.get(),
                _ => bail!(IllegalArgument(format!(
                    "non-sorted_numeric dv found for field {}",
                    field
                ))),
            },
            Entry::Vacant(v) => match self.get_dv_field(field, DocValuesType::SortedNumeric) {
                Some(fi) if self.dv_producer_local.get().is_some() => {
                    let dv_producer = self.dv_producer_local.get().unwrap();
                    let dv = dv_producer.get_sorted_numeric(fi)?;
                    let cell = dv;
                    v.insert(DocValuesProviderEnum::SortedNumeric(Arc::clone(&cell)));
                    cell.get()
                }
                _ => bail!(IllegalArgument(format!(
                    "non-dv-segment or non-exist or non-sorted_numeric field: {}",
                    field
                ))),
            },
        }
    }

    fn get_sorted_set_doc_values(&self, field: &str) -> Result<Box<dyn SortedSetDocValues>> {
        match self
            .dv_provider_local()?
            .borrow_mut()
            .entry(String::from(field))
        {
            Entry::Occupied(o) => match *o.get() {
                DocValuesProviderEnum::SortedSet(ref dv) => dv.get(),
                _ => bail!(IllegalArgument(format!(
                    "non-sorted_set dv found for field {}",
                    field
                ))),
            },
            Entry::Vacant(v) => match self.get_dv_field(field, DocValuesType::SortedSet) {
                Some(fi) if self.dv_producer_local.get().is_some() => {
                    let dv_producer = self.dv_producer_local.get().unwrap();
                    let dv = dv_producer.get_sorted_set(fi)?;
                    let cell = dv;
                    v.insert(DocValuesProviderEnum::SortedSet(Arc::clone(&cell)));
                    cell.get()
                }

                _ => bail!(IllegalArgument(format!(
                    "non-dv-segment or non-exist or non-sorted_set field: {}",
                    field
                ))),
            },
        }
    }

    fn norm_values(&self, field: &str) -> Result<Option<Box<dyn NumericDocValues>>> {
        if let Some(field_info) = self.field_infos.field_info_by_name(field) {
            if field_info.has_norms() {
                assert!(self.core.norms_producer.is_some());
                let norms_producer = self.core.norms_producer.as_ref().unwrap();
                return Ok(Some(norms_producer.norms(&field_info)?));
            }
        }

        Ok(None)
    }

    fn get_docs_with_field(&self, field: &str) -> Result<Box<dyn BitsMut>> {
        match self.field_infos.field_info_by_name(field) {
            Some(fi)
                if fi.doc_values_type != DocValuesType::Null
                    && self.dv_producer_local.get().is_some() =>
            {
                let dv_producer = self.dv_producer_local.get().unwrap();
                dv_producer.get_docs_with_field(fi)
            }

            // FIXME: chain errors
            _ => bail!(IllegalArgument(format!(
                "non-exist or DocValuesType::Null field: {}, or non-dv segment",
                field
            ))),
        }
    }

    fn point_values(&self) -> Option<Self::PointsReader> {
        self.core.points_reader.clone()
    }

    fn core_cache_key(&self) -> &str {
        // use segment name as unique segment cache key
        &self.core.core_cache_key
    }

    fn index_sort(&self) -> Option<&Sort> {
        self.si.info.index_sort()
    }

    fn add_core_drop_listener(&self, listener: Deferred) {
        self.core.add_core_drop_listener(listener)
    }

    fn is_codec_reader(&self) -> bool {
        true
    }

    fn store_fields_reader(&self) -> Result<Self::StoredReader> {
        Ok(Arc::clone(&self.core.fields_reader))
    }

    fn term_vectors_reader(&self) -> Result<Option<Self::TVReader>> {
        Ok(self.core.term_vectors_reader.clone())
    }

    fn norms_reader(&self) -> Result<Option<Self::NormsReader>> {
        Ok(self.core.norms_producer.clone())
    }

    fn doc_values_reader(&self) -> Result<Option<Arc<dyn DocValuesProducer>>> {
        self.init_local_dv_producer()?;
        Ok(self.dv_producer_local.get().map(Arc::clone))
    }

    fn postings_reader(&self) -> Result<Self::FieldsProducer> {
        self.fields()
    }
}

impl<D: Directory + 'static, C: Codec> AsRef<dyn IndexReader<Codec = C>> for SegmentReader<D, C> {
    fn as_ref(&self) -> &(dyn IndexReader<Codec = C> + 'static) {
        self
    }
}
