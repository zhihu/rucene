use std::{
    cell::RefCell,
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
};
use thread_local::{CachedThreadLocal, ThreadLocal};

use core::store::Directory;
use core::{
    codec::{
        Codec, CodecFieldsProducer, CodecNormsProducer, CodecPointsReader, CodecStoredFieldsReader,
        CodecTVFields, CodecTVReader, DocValuesProducer, FieldInfosFormat, LiveDocsFormat,
        NormsProducer, StoredFieldsReader, TermVectorsReader,
    },
    doc::{Document, DocumentStoredFieldVisitor},
    index::{
        leaf_reader::LeafReaderContext, BinaryDocValuesRef, CfsDirectory, DocValuesType, FieldInfo,
        FieldInfos, IndexReader, LeafReader, NumericDocValues, NumericDocValuesRef,
        SegmentCommitInfo, SegmentCoreReaders, SegmentDocValues, SortedDocValuesRef,
        SortedNumericDocValuesRef, SortedSetDocValuesRef, StoredFieldVisitor,
    },
    search::sort::Sort,
    store::IOContext,
    util::{external::deferred::Deferred, numeric::to_base36, BitsRef, DocId, MatchAllBits},
};
use error::{ErrorKind::IllegalArgument, Result};

pub enum DocValuesRefEnum {
    Binary(BinaryDocValuesRef),
    Numeric(NumericDocValuesRef),
    Sorted(SortedDocValuesRef),
    SortedNumeric(SortedNumericDocValuesRef),
    SortedSet(SortedSetDocValuesRef),
}

pub type ThreadLocalDocValueProducer = ThreadLocal<Arc<dyn DocValuesProducer>>;

pub struct SegmentReader<D: Directory, C: Codec> {
    pub si: Arc<SegmentCommitInfo<D, C>>,
    pub live_docs: BitsRef,
    num_docs: i32,
    pub core: Arc<SegmentCoreReaders<D, C>>,
    pub is_nrt: bool,
    pub field_infos: Arc<FieldInfos>,
    // context: LeafReaderContext
    doc_values_producer: ThreadLocalDocValueProducer,
    docs_with_field_local: CachedThreadLocal<RefCell<HashMap<String, BitsRef>>>,
    doc_values_local: CachedThreadLocal<RefCell<HashMap<String, DocValuesRefEnum>>>,
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
        doc_values_producer: ThreadLocalDocValueProducer,
    ) -> SegmentReader<D, C> {
        let docs_with_field_local = CachedThreadLocal::new();
        docs_with_field_local.get_or(|| Box::new(RefCell::new(HashMap::new())));

        let doc_values_local = CachedThreadLocal::new();
        doc_values_local.get_or(|| Box::new(RefCell::new(HashMap::new())));

        SegmentReader {
            si,
            live_docs,
            num_docs,
            core,
            is_nrt,
            field_infos,
            doc_values_producer,
            docs_with_field_local,
            doc_values_local,
        }
    }

    pub fn build(
        si: Arc<SegmentCommitInfo<D, C>>,
        live_docs: BitsRef,
        num_docs: i32,
        core: Arc<SegmentCoreReaders<D, C>>,
    ) -> Result<Self> {
        let field_infos = Self::init_field_infos(si.as_ref(), core.as_ref())?;
        let doc_values =
            Self::init_doc_values_producer(core.as_ref(), si.as_ref(), Arc::clone(&field_infos))?;
        Ok(Self::new(
            si,
            live_docs,
            num_docs,
            core,
            true,
            field_infos,
            doc_values,
        ))
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
        let doc_values_producer = Self::init_doc_values_producer(
            sr.core.as_ref(),
            si.as_ref(),
            Arc::clone(&field_infos),
        )?;
        Ok(SegmentReader::new(
            si,
            live_docs,
            num_docs,
            Arc::clone(&sr.core),
            is_nrt,
            field_infos,
            doc_values_producer,
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
            format!("doc_id={} max_docs={}", doc_id, self.max_docs(),)
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
            let segment_suffix = format!("{}", si.field_infos_gen());
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

        let doc_values_producer =
            SegmentReader::init_doc_values_producer(&core, &si, Arc::clone(&field_infos))?;

        Ok(SegmentReader::new(
            Arc::clone(si),
            live_docs,
            num_docs,
            core,
            false,
            field_infos,
            doc_values_producer,
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
    fn init_doc_values_producer(
        core: &SegmentCoreReaders<D, C>,
        si: &SegmentCommitInfo<D, C>,
        field_infos: Arc<FieldInfos>,
    ) -> Result<ThreadLocalDocValueProducer> {
        // initDocValuesProducer: init most recent DocValues for the current commit
        let dir = match core.cfs_reader {
            Some(ref d) => Arc::clone(d),
            None => Arc::new(CfsDirectory::Raw(Arc::clone(&si.info.directory))),
        };

        let doc_values_producer = if !field_infos.has_doc_values {
            ThreadLocal::new()
        } else if si.has_field_updates() {
            unimplemented!()
        } else {
            // simple case, no DocValues updates
            let dv_producer =
                SegmentDocValues::get_doc_values_producer(-1_i64, &si, dir, field_infos)?;

            let doc_values_producer = ThreadLocal::new();
            doc_values_producer.get_or(|| Box::new(Arc::from(dv_producer)));
            doc_values_producer
        };
        Ok(doc_values_producer)
    }

    fn init_local_doc_values_producer(&self) -> Result<()> {
        if self.field_infos.has_doc_values {
            if self.si.has_field_updates() {
                unimplemented!()
            } else {
                if self.doc_values_producer.get().is_some() {
                    return Ok(());
                }

                let dv_producer = if let Some(ref cfs_dir) = self.core.cfs_reader {
                    SegmentDocValues::get_doc_values_producer(
                        -1_i64,
                        &self.si,
                        Arc::clone(cfs_dir),
                        Arc::clone(&self.field_infos),
                    )?
                } else {
                    SegmentDocValues::get_doc_values_producer(
                        -1_i64,
                        &self.si,
                        Arc::clone(&self.si.info.directory),
                        Arc::clone(&self.field_infos),
                    )?
                };

                self.doc_values_producer
                    .get_or(|| Box::new(Arc::from(dv_producer)));
            }
        }
        Ok(())
    }

    fn init_field_infos<C1: Codec>(
        si: &SegmentCommitInfo<D, C1>,
        core: &SegmentCoreReaders<D, C1>,
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

    fn document(&self, doc_id: DocId, visitor: &mut StoredFieldVisitor) -> Result<()> {
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

    fn get_numeric_doc_values(&self, field: &str) -> Result<NumericDocValuesRef> {
        self.init_local_doc_values_producer()?;

        match self
            .doc_values_local
            .get_or(|| Box::new(RefCell::new(HashMap::new())))
            .borrow_mut()
            .entry(String::from(field))
        {
            Entry::Occupied(o) => match *o.get() {
                DocValuesRefEnum::Numeric(ref dv) => Ok(Arc::clone(&dv)),
                _ => bail!(IllegalArgument(format!(
                    "non-numeric dv found for field {}",
                    field
                ))),
            },
            Entry::Vacant(v) => match self.get_dv_field(field, DocValuesType::Numeric) {
                Some(fi) if self.doc_values_producer.get().is_some() => {
                    let dv_producer = self.doc_values_producer.get().unwrap();
                    let cell = dv_producer.get_numeric(fi)?;
                    v.insert(DocValuesRefEnum::Numeric(Arc::clone(&cell)));
                    Ok(cell)
                }

                _ => bail!(IllegalArgument(format!(
                    "non-dv-segment or non-exist or non-numeric field: {}",
                    field
                ))),
            },
        }
    }

    fn get_binary_doc_values(&self, field: &str) -> Result<BinaryDocValuesRef> {
        self.init_local_doc_values_producer()?;
        match self
            .doc_values_local
            .get_or(|| Box::new(RefCell::new(HashMap::new())))
            .borrow_mut()
            .entry(String::from(field))
        {
            Entry::Occupied(o) => match *o.get() {
                DocValuesRefEnum::Binary(ref dv) => Ok(Arc::clone(&dv)),
                _ => bail!(IllegalArgument(format!(
                    "non-binary dv found for field {}",
                    field
                ))),
            },
            Entry::Vacant(v) => match self.get_dv_field(field, DocValuesType::Binary) {
                Some(fi) if self.doc_values_producer.get().is_some() => {
                    let dv_producer = self.doc_values_producer.get().unwrap();
                    let dv = dv_producer.get_binary(fi)?;
                    let cell = Arc::from(dv);
                    v.insert(DocValuesRefEnum::Binary(Arc::clone(&cell)));
                    Ok(cell)
                }

                _ => bail!(IllegalArgument(format!(
                    "non-dv-segment or non-exist or non-binary field: {}",
                    field
                ))),
            },
        }
    }

    fn get_sorted_doc_values(&self, field: &str) -> Result<SortedDocValuesRef> {
        self.init_local_doc_values_producer()?;

        match self
            .doc_values_local
            .get_or(|| Box::new(RefCell::new(HashMap::new())))
            .borrow_mut()
            .entry(String::from(field))
        {
            Entry::Occupied(o) => match *o.get() {
                DocValuesRefEnum::Sorted(ref dv) => Ok(Arc::clone(&dv)),
                _ => bail!(IllegalArgument(format!(
                    "non-binary dv found for field {}",
                    field
                ))),
            },
            Entry::Vacant(v) => match self.get_dv_field(field, DocValuesType::Sorted) {
                Some(fi) if self.doc_values_producer.get().is_some() => {
                    let dv_producer = self.doc_values_producer.get().unwrap();
                    let dv = dv_producer.get_sorted(fi)?;
                    v.insert(DocValuesRefEnum::Sorted(Arc::clone(&dv)));
                    Ok(dv)
                }
                _ => bail!(IllegalArgument(format!(
                    "non-dv-segment or non-exist or non-binary field: {}",
                    field
                ))),
            },
        }
    }

    fn get_sorted_numeric_doc_values(&self, field: &str) -> Result<SortedNumericDocValuesRef> {
        self.init_local_doc_values_producer()?;

        match self
            .doc_values_local
            .get_or(|| Box::new(RefCell::new(HashMap::new())))
            .borrow_mut()
            .entry(String::from(field))
        {
            Entry::Occupied(o) => match *o.get() {
                DocValuesRefEnum::SortedNumeric(ref dv) => Ok(Arc::clone(&dv)),
                _ => bail!(IllegalArgument(format!(
                    "non-binary dv found for field {}",
                    field
                ))),
            },
            Entry::Vacant(v) => match self.get_dv_field(field, DocValuesType::SortedNumeric) {
                Some(fi) if self.doc_values_producer.get().is_some() => {
                    let dv_producer = self.doc_values_producer.get().unwrap();
                    let dv = dv_producer.get_sorted_numeric(fi)?;
                    let cell = Arc::from(dv);
                    v.insert(DocValuesRefEnum::SortedNumeric(Arc::clone(&cell)));
                    Ok(cell)
                }
                _ => bail!(IllegalArgument(format!(
                    "non-dv-segment or non-exist or non-binary field: {}",
                    field
                ))),
            },
        }
    }

    fn get_sorted_set_doc_values(&self, field: &str) -> Result<SortedSetDocValuesRef> {
        self.init_local_doc_values_producer()?;

        match self
            .doc_values_local
            .get_or(|| Box::new(RefCell::new(HashMap::new())))
            .borrow_mut()
            .entry(String::from(field))
        {
            Entry::Occupied(o) => match *o.get() {
                DocValuesRefEnum::SortedSet(ref dv) => Ok(Arc::clone(&dv)),
                _ => bail!(IllegalArgument(format!(
                    "non-binary dv found for field {}",
                    field
                ))),
            },
            Entry::Vacant(v) => match self.get_dv_field(field, DocValuesType::SortedSet) {
                Some(fi) if self.doc_values_producer.get().is_some() => {
                    let dv_producer = self.doc_values_producer.get().unwrap();
                    let dv = dv_producer.get_sorted_set(fi)?;
                    let cell = Arc::from(dv);
                    v.insert(DocValuesRefEnum::SortedSet(Arc::clone(&cell)));
                    Ok(cell)
                }

                _ => bail!(IllegalArgument(format!(
                    "non-dv-segment or non-exist or non-binary field: {}",
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

    fn get_docs_with_field(&self, field: &str) -> Result<BitsRef> {
        if let Some(prev) = self
            .docs_with_field_local
            .get_or(|| Box::new(RefCell::new(HashMap::new())))
            .borrow_mut()
            .get(field)
        {
            return Ok(Arc::clone(prev));
        }

        self.init_local_doc_values_producer()?;

        match self.field_infos.field_info_by_name(field) {
            Some(fi)
                if fi.doc_values_type != DocValuesType::Null
                    && self.doc_values_producer.get().is_some() =>
            {
                let dv_producer = self.doc_values_producer.get().unwrap();
                let dv = dv_producer.get_docs_with_field(fi)?;
                self.docs_with_field_local
                    .get()
                    .unwrap()
                    .borrow_mut()
                    .insert(field.to_string(), Arc::clone(&dv));
                Ok(dv)
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
        Ok(self.doc_values_producer.get().map(Arc::clone))
    }

    fn postings_reader(&self) -> Result<Self::FieldsProducer> {
        self.fields()
    }
}

impl<D: Directory + 'static, C: Codec> AsRef<IndexReader<Codec = C>> for SegmentReader<D, C> {
    fn as_ref(&self) -> &(IndexReader<Codec = C> + 'static) {
        self
    }
}
