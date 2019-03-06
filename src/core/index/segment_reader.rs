use std::{any::Any,
          cell::{Cell, RefCell},
          collections::{hash_map::Entry, HashMap},
          sync::Arc};
use thread_local::{CachedThreadLocal, ThreadLocal};

use core::{codec::{DocValuesProducer, FieldsProducerRef, NormsProducer, StoredFieldsReader,
                   TermVectorsReader},
           doc::{Document, DocumentStoredFieldVisitor},
           index::{field_info::Fields, point_values::PointValuesRef,
                   stored_field_visitor::StoredFieldVisitor, BinaryDocValuesRef, DocValuesType,
                   FieldInfo, FieldInfos, IndexReader, LeafReader, NumericDocValues,
                   NumericDocValuesRef, SegmentCommitInfo, SegmentCoreReaders, SegmentDocValues,
                   SortedDocValuesRef, SortedNumericDocValuesRef, SortedSetDocValuesRef},
           search::sort::Sort,
           store::{IOContext, IO_CONTEXT_READ, IO_CONTEXT_READONCE},
           util::{external::deferred::Deferred, numeric::to_base36, BitsRef, DocId, MatchAllBits}};
use error::{ErrorKind::IllegalArgument, Result};

pub enum DocValuesRefEnum {
    Binary(BinaryDocValuesRef),
    Numeric(NumericDocValuesRef),
    Sorted(SortedDocValuesRef),
    SortedNumeric(SortedNumericDocValuesRef),
    SortedSet(SortedSetDocValuesRef),
}

pub type ThreadLocalDocValueProducer = ThreadLocal<Arc<DocValuesProducer>>;

pub struct SegmentReader {
    pub si: Arc<SegmentCommitInfo>,
    pub live_docs: BitsRef,
    num_docs: i32,
    pub core: Arc<SegmentCoreReaders>,
    pub is_nrt: bool,
    pub field_infos: Arc<FieldInfos>,
    // context: LeafReaderContext
    // TODO the doc base in parent, temporarily put here
    doc_base: Cell<DocId>,
    doc_values_producer: ThreadLocalDocValueProducer,
    docs_with_field_local: CachedThreadLocal<RefCell<HashMap<String, BitsRef>>>,
    doc_values_local: CachedThreadLocal<RefCell<HashMap<String, DocValuesRefEnum>>>,
}

unsafe impl Sync for SegmentReader {}

/// IndexReader implementation over a single segment.
/// Instances pointing to the same segment (but with different deletes, etc)
/// may share the same core data.
/// @lucene.experimental
///
impl SegmentReader {
    pub fn new(
        si: Arc<SegmentCommitInfo>,
        live_docs: BitsRef,
        num_docs: i32,
        core: Arc<SegmentCoreReaders>,
        is_nrt: bool,
        field_infos: Arc<FieldInfos>,
        doc_values_producer: ThreadLocalDocValueProducer,
    ) -> SegmentReader {
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
            doc_base: Cell::new(0),
            doc_values_producer,
            docs_with_field_local,
            doc_values_local,
        }
    }

    pub fn build(
        si: Arc<SegmentCommitInfo>,
        live_docs: BitsRef,
        num_docs: i32,
        core: Arc<SegmentCoreReaders>,
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

    pub fn build_from_reader(si: Arc<SegmentCommitInfo>, sr: &SegmentReader) -> Result<Self> {
        let live_docs: BitsRef = if si.has_deletions() {
            si.info.codec().live_docs_format().read_live_docs(
                Arc::clone(&si.info.directory),
                si.as_ref(),
                &IO_CONTEXT_READONCE,
            )?
        } else {
            Arc::new(MatchAllBits::new(si.info.max_doc as usize))
        };
        let num_docs = si.info.max_doc() - si.del_count();
        Self::build_from(si, sr, live_docs, num_docs, false)
    }

    pub fn build_from(
        si: Arc<SegmentCommitInfo>,
        sr: &SegmentReader,
        live_docs: BitsRef,
        num_docs: i32,
        is_nrt: bool,
    ) -> Result<SegmentReader> {
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
            doc_id >= 0 && doc_id < self.doc_base.get() + self.max_docs(),
            format!(
                "doc_id={} max_docs={} doc_base={}",
                doc_id,
                self.max_docs(),
                self.doc_base.get()
            )
        );
    }

    /// Constructs a new SegmentReader with a new core.
    /// @throws CorruptIndexException if the index is corrupt
    /// @throws IOException if there is a low-level IO error
    ///
    pub fn open(si: &Arc<SegmentCommitInfo>, ctx: &IOContext) -> Result<SegmentReader> {
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
                &IOContext::Read(true),
            )?;
            Arc::new(field_infos)
        };

        let live_docs = if si.has_deletions() {
            codec.live_docs_format().read_live_docs(
                Arc::clone(&si.info.directory),
                si,
                &IO_CONTEXT_READ,
            )?
        } else {
            assert_eq!(si.del_count(), 0);
            Arc::new(MatchAllBits::new(0))
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

    pub fn postings_reader(&self) -> FieldsProducerRef {
        self.core.fields()
    }

    pub fn set_doc_base(&self, doc_base: DocId) {
        self.doc_base.set(doc_base);
    }

    fn get_dv_field(&self, field: &str, dv_type: DocValuesType) -> Option<&FieldInfo> {
        match self.field_infos.field_info_by_name(field) {
            Some(fi) if fi.doc_values_type == dv_type => Some(fi),
            _ => None,
        }
    }
}

impl SegmentReader {
    fn init_doc_values_producer(
        core: &SegmentCoreReaders,
        si: &SegmentCommitInfo,
        field_infos: Arc<FieldInfos>,
    ) -> Result<ThreadLocalDocValueProducer> {
        // initDocValuesProducer: init most recent DocValues for the current commit
        let dir = match core.cfs_reader {
            Some(ref d) => Arc::clone(d),
            None => Arc::clone(&si.info.directory),
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

                let dir = self.core
                    .cfs_reader
                    .as_ref()
                    .map(|s| Arc::clone(&s))
                    .unwrap_or_else(|| Arc::clone(&self.si.info.directory));

                let dv_producer = SegmentDocValues::get_doc_values_producer(
                    -1_i64,
                    &self.si,
                    dir,
                    Arc::clone(&self.field_infos),
                )?;

                self.doc_values_producer
                    .get_or(|| Box::new(Arc::from(dv_producer)));
            }
        }
        Ok(())
    }

    fn init_field_infos(
        si: &SegmentCommitInfo,
        core: &SegmentCoreReaders,
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
                &IOContext::Read(true),
            )?))
        }
    }
}

impl IndexReader for SegmentReader {
    fn leaves(&self) -> Vec<&LeafReader> {
        vec![self]
    }

    fn term_vector(&self, doc_id: i32) -> Result<Option<Box<Fields>>> {
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

    fn leaf_reader_for_doc(&self, doc: i32) -> &LeafReader {
        debug_assert!(doc <= IndexReader::max_doc(self));
        self
    }

    fn as_any(&self) -> &Any {
        self
    }
}

impl LeafReader for SegmentReader {
    fn fields(&self) -> Result<FieldsProducerRef> {
        Ok(self.core.fields())
    }

    fn name(&self) -> &str {
        &self.si.info.name
    }

    fn doc_base(&self) -> DocId {
        self.doc_base.get()
    }

    fn term_vector(&self, doc_id: DocId) -> Result<Option<Box<Fields>>> {
        self.check_bounds(doc_id);
        if let Some(ref reader) = self.core.term_vectors_reader {
            reader.get(doc_id - self.doc_base.get())
        } else {
            bail!("the index does not support term vectors!");
        }
    }

    fn document(&self, doc_id: DocId, visitor: &mut StoredFieldVisitor) -> Result<()> {
        self.check_bounds(doc_id);
        self.core
            .fields_reader
            .as_ref()
            .visit_document(doc_id, visitor)
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

        match self.doc_values_local
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
        match self.doc_values_local
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

        match self.doc_values_local
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
                    let cell = Arc::from(dv);
                    v.insert(DocValuesRefEnum::Sorted(Arc::clone(&cell)));
                    Ok(cell)
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

        match self.doc_values_local
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

        match self.doc_values_local
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

    fn norm_values(&self, field: &str) -> Result<Option<Box<NumericDocValues>>> {
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
        if let Some(prev) = self.docs_with_field_local
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

    fn point_values(&self) -> Option<PointValuesRef> {
        match self.core.points_reader {
            Some(ref reader) => Some(Arc::clone(reader)),
            None => None,
        }
    }

    fn core_cache_key(&self) -> &str {
        // use segment name as unique segment cache key
        &self.si.info.name
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

    fn store_fields_reader(&self) -> Result<Arc<StoredFieldsReader>> {
        Ok(Arc::clone(&self.core.fields_reader))
    }

    fn term_vectors_reader(&self) -> Result<Option<Arc<TermVectorsReader>>> {
        Ok(self.core.term_vectors_reader.as_ref().map(Arc::clone))
    }

    fn norms_reader(&self) -> Result<Option<Arc<NormsProducer>>> {
        Ok(self.core.norms_producer.as_ref().map(Arc::clone))
    }

    fn doc_values_reader(&self) -> Result<Option<Arc<DocValuesProducer>>> {
        Ok(self.doc_values_producer.get().map(Arc::clone))
    }

    fn postings_reader(&self) -> Result<FieldsProducerRef> {
        Ok(self.core.fields())
    }
}

impl AsRef<IndexReader> for SegmentReader {
    fn as_ref(&self) -> &(IndexReader + 'static) {
        self
    }
}
