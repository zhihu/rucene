mod index_options;

pub use self::index_options::*;

mod doc_values_type;

pub use self::doc_values_type::*;

mod numeric_doc_values;

pub use self::numeric_doc_values::*;

pub use self::fieldable::Fieldable;

mod binary_doc_values;

pub use self::binary_doc_values::*;

mod sorted_numeric_doc_values;

pub use self::sorted_numeric_doc_values::*;

pub mod index_file_names;

mod sorted_doc_values_term_iterator;

pub use self::sorted_doc_values_term_iterator::*;

mod sorted_set_doc_values_term_iterator;

pub use self::sorted_set_doc_values_term_iterator::*;

mod doc_values;

pub use self::doc_values::*;

mod sorted_doc_values;

pub use self::sorted_doc_values::*;

mod singleton_sorted_numeric_doc_values;

pub use self::singleton_sorted_numeric_doc_values::*;

mod sorted_set_doc_values;

pub use self::sorted_set_doc_values::*;

mod singleton_sorted_set_doc_values;

pub use self::singleton_sorted_set_doc_values::*;

mod segment_doc_values;

pub use self::segment_doc_values::*;

mod segment_reader;

pub use self::segment_reader::*;

mod directory_reader;

pub use self::directory_reader::*;

mod segment;

pub use self::segment::*;

pub mod mapper_field_type;
pub mod point_values;

use serde::ser::SerializeStruct;
use serde::{Serialize, Serializer};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::result;
use std::sync::Arc;

use core::codec::Codec;
use core::doc::Document;
use core::index::stored_field_visitor::StoredFieldVisitor;
use core::index::term::TermState;
use core::store::{DirectoryRc, IOContext};
use core::util::{to_base36, Bits, DocId, Version};
use error::Result;

pub mod field_info;
pub use self::field_info::*;

mod leaf_reader;
pub use self::leaf_reader::*;

pub mod term;

pub mod fieldable;
pub mod index_lookup;
pub mod multi_fields;
pub mod multi_terms;
pub mod reader_slice;
pub mod stored_field_visitor;

// postings flags for postings enum
/// don't need per doc postings
pub const POSTINGS_NONE: i16 = 0;
/// require term frequencies
pub const POSTINGS_FREQS: i16 = 1 << 3;
/// require term positions
pub const POSTINGS_POSITIONS: i16 = POSTINGS_FREQS | 1 << 4;
/// require term offsets
pub const POSTINGS_OFFSETS: i16 = POSTINGS_POSITIONS | 1 << 5;
/// require term payloads
pub const POSTINGS_PAYLOADS: i16 = POSTINGS_POSITIONS | 1 << 6;
/// require positions, payloads and offsets
pub const POSTINGS_ALL: i16 = POSTINGS_OFFSETS | POSTINGS_PAYLOADS;

// index file names
pub const INDEX_FILE_SEGMENTS: &str = "segments";
pub const INDEX_FILE_PENDING_SEGMENTS: &str = "pending_segments";
pub const INDEX_FILE_OLD_SEGMENT_GEN: &str = "segments.gen";

fn index_of_segment_name(filename: &str) -> i32 {
    // If it is a .del file, there's an '_' after the first character
    let filename = &filename[1..];
    if let Some(i) = filename.find('_') {
        return i as i32 + 1;
    }
    // let it panic when input is invalid
    filename.find('.').unwrap() as i32 + 1
}

pub fn strip_segment_name(name: &str) -> String {
    let mut name = name;
    let idx = index_of_segment_name(name);
    if idx != -1 {
        name = &name[idx as usize..]
    }
    name.into()
}

pub fn segment_file_name(name: &str, suffix: &str, ext: &str) -> String {
    if !ext.is_empty() || !suffix.is_empty() {
        assert!(!ext.starts_with('.'));
        let mut filename = String::with_capacity(name.len() + 2 + suffix.len() + ext.len());
        filename.push_str(name);
        if !suffix.is_empty() {
            filename.push('_');
            filename.push_str(suffix);
        }
        if !ext.is_empty() {
            filename.push('.');
            filename.push_str(ext);
        }
        filename
    } else {
        String::from(name)
    }
}

pub fn file_name_from_generation(base: &str, ext: &str, gen: i64) -> String {
    assert!(gen >= 0);
    if gen == 0 {
        segment_file_name(base, "", ext)
    } else {
        let mut res = String::new();
        res.push_str(base);
        res.push('_');
        res += &to_base36(gen);
        if !ext.is_empty() {
            res.push('.');
            res.push_str(ext);
        }
        res
    }
}

pub trait IndexReader: Send + Sync {
    fn leaves(&self) -> Vec<&LeafReader>;
    fn term_vector(&self, doc_id: DocId) -> Result<Box<Fields>>;
    fn document(&self, doc_id: DocId, fields: &[String]) -> Result<Document>;
    fn max_doc(&self) -> i32;
    fn num_docs(&self) -> i32;
    fn leaf_reader_for_doc(&self, doc: DocId) -> &LeafReader {
        let leaves = self.leaves();
        let size = leaves.len();
        let mut lo = 0usize;
        let mut hi = size - 1;
        while hi >= lo {
            let mut mid = (lo + hi) >> 1;
            let mid_value = leaves[mid].doc_base();
            if doc < mid_value {
                hi = mid - 1;
            } else if doc > mid_value {
                lo = mid + 1;
            } else {
                while mid + 1 < size && leaves[mid + 1].doc_base() == mid_value {
                    mid += 1;
                }
                return leaves[mid];
            }
        }
        leaves[hi]
    }
}

pub type IndexReaderRef = Arc<IndexReader>;

pub const SEGMENT_INFO_YES: i32 = 1;
pub const SEGMENT_INFO_NO: i32 = -1;

pub struct SegmentInfo {
    pub name: String,
    pub max_doc: i32,
    pub directory: DirectoryRc,
    pub is_compound_file: bool,
    pub id: Vec<u8>,
    pub codec: Option<Arc<Codec>>,
    pub diagnostics: HashMap<String, String>,
    pub attributes: HashMap<String, String>,
    // index_sort: Sort,
    pub version: Version,
}

impl SegmentInfo {
    #[allow(too_many_arguments)]
    pub fn new(
        version: Version,
        name: &str,
        max_doc: i32,
        directory: DirectoryRc,
        is_compound_file: bool,
        diagnostics: HashMap<String, String>,
        id: &[u8],
        attributes: HashMap<String, String>,
    ) -> Result<SegmentInfo> {
        Ok(SegmentInfo {
            name: String::from(name),
            max_doc,
            directory,
            is_compound_file,
            id: Vec::from(id),
            version,
            codec: None,
            diagnostics,
            attributes,
        })
    }

    pub fn set_codec(&mut self, codec: Arc<Codec>) {
        self.codec = Some(codec);
    }

    pub fn codec(&self) -> Arc<Codec> {
        assert!(self.codec.is_some());
        Arc::clone(self.codec.as_ref().unwrap())
    }

    pub fn max_doc(&self) -> i32 {
        self.max_doc
    }

    pub fn is_compound_file(&self) -> bool {
        self.is_compound_file
    }

    pub fn get_id(&self) -> &[u8] {
        &self.id[..]
    }
}

impl Clone for SegmentInfo {
    fn clone(&self) -> Self {
        SegmentInfo {
            name: self.name.clone(),
            max_doc: self.max_doc,
            is_compound_file: self.is_compound_file,
            directory: Arc::clone(&self.directory),
            id: self.id.clone(),
            codec: self.codec.as_ref().map(|c| Arc::clone(c)),
            diagnostics: self.diagnostics.clone(),
            attributes: self.attributes.clone(),
            version: self.version.clone(),
        }
    }
}

impl Serialize for SegmentInfo {
    fn serialize<S>(&self, serializer: S) -> result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_struct("SegmentInfo", 8)?;
        s.serialize_field("name", &self.name)?;
        s.serialize_field("max_doc", &self.max_doc)?;
        s.serialize_field("is_compound_file", &self.is_compound_file)?;
        s.serialize_field("id", &self.id)?;
        // TODO: directory?
        if self.codec.is_some() {
            let codec = self.codec.as_ref().unwrap();
            s.serialize_field("codec", codec.name())?;
        } else {
            s.serialize_field("codec", "uninitialized")?;
        };
        s.serialize_field("diagnostics", &self.diagnostics)?;
        s.serialize_field("attributes", &self.attributes)?;
        s.serialize_field("version", &self.version)?;
        s.end()
    }
}

/// Holds buffered deletes and updates, by docID, term or query for a
/// single segment. This is used to hold buffered pending
/// deletes and updates against the to-be-flushed segment.  Once the
/// deletes and updates are pushed (on flush in DocumentsWriter), they
/// are converted to a FrozenBufferedUpdates instance. */
///
/// NOTE: instances of this class are accessed either via a private
/// instance on DocumentWriterPerThread, or via sync'd code by
/// DocumentsWriterDeleteQueue
pub struct BufferedUpdates {
    // num_term_deletes: AtomicIsize,
// num_numeric_updates: AtomicIsize,
// num_binary_updates: AtomicIsize,
//
// TODO: rename thes three: put "deleted" prefix in front:
// terms: HashMap<Term, i32>,
// queries: HashMap<Query, i32>,
// doc_ids: Vec<i32>,
//
// Map<dvField,Map<updateTerm,NumericUpdate>>
// For each field we keep an ordered list of NumericUpdates, key'd by the
// update Term. LinkedHashMap guarantees we will later traverse the map in
// insertion order (so that if two terms affect the same document, the last
// one that came in wins), and helps us detect faster if the same Term is
// used to update the same field multiple times (so we later traverse it
// only once).
// numeric_updates: HashMap<String, HashMap<Term, NumericDocValuesUpdate>>,
//
// Map<dvField,Map<updateTerm,BinaryUpdate>>
// For each field we keep an ordered list of BinaryUpdates, key'd by the
// update Term. LinkedHashMap guarantees we will later traverse the map in
// insertion order (so that if two terms affect the same document, the last
// one that came in wins), and helps us detect faster if the same Term is
// used to update the same field multiple times (so we later traverse it
// only once).
// binary_update: HashMap<String, HashMap<Term, BinaryDocValuesUpdate>>,
//
// bytes_used: AtomicI64,
// gen: i64,
// segmentName: String,
//
}

/// A Term represents a word from text.  This is the unit of search.  It is
/// composed of two elements, the text of the word, as a string, and the name of
/// the field that the text occurred in.
///
/// Note that terms may represent more than words from text fields, but also
/// things like dates, email addresses, urls, etc.
#[derive(Debug, PartialEq, Hash, Eq)]
pub struct Term {
    pub field: String,
    pub bytes: Vec<u8>,
}

impl Term {
    /// Constructs a Term with the given field and bytes.
    /// <p>Note that a null field or null bytes value results in undefined
    /// behavior for most Lucene APIs that accept a Term parameter.
    ///
    /// <p>The provided BytesRef is copied when it is non null.
    pub fn new(field: String, bytes: Vec<u8>) -> Term {
        Term { field, bytes }
    }

    /// Returns the field of this term.   The field indicates
    /// the part of a document which this term came from.
    pub fn field(&self) -> &str {
        &self.field
    }

    /// Returns the text of this term.  In the case of words, this is simply the
    /// text of the word.  In the case of dates and other types, this is an
    /// encoding of the object as a string.
    pub fn text(&self) -> Result<String> {
        Ok(String::from_utf8(self.bytes.clone())?)
    }
}

impl Clone for Term {
    fn clone(&self) -> Self {
        Self::new(self.field.clone(), self.bytes.clone())
    }
}

pub struct TermContext {
    pub doc_freq: i32,
    pub total_term_freq: i64,
    pub states: Vec<(DocId, Box<TermState>)>,
}

impl TermContext {
    pub fn new(reader: &IndexReader) -> TermContext {
        let doc_freq = 0;
        let total_term_freq = 0;
        let states = Vec::with_capacity(reader.leaves().len());
        TermContext {
            doc_freq,
            total_term_freq,
            states,
        }
    }

    pub fn build(&mut self, reader: &IndexReader, term: &Term) -> Result<()> {
        for reader in &reader.leaves() {
            if let Some(terms) = reader.terms(&term.field)? {
                let mut terms_enum = terms.iterator()?;
                if terms_enum.seek_exact(&term.bytes)? {
                    // TODO add TermStates if someone need it
                    let doc_freq = terms_enum.doc_freq()?;
                    let total_term_freq = terms_enum.total_term_freq()?;
                    self.accumulate_statistics(doc_freq, total_term_freq as i64);
                    self.states
                        .push((reader.doc_base(), terms_enum.term_state()?));
                }
            }
        }

        Ok(())
    }

    fn accumulate_statistics(&mut self, doc_freq: i32, total_term_freq: i64) {
        self.doc_freq += doc_freq;
        if self.total_term_freq >= 0 && total_term_freq >= 0 {
            self.total_term_freq += total_term_freq
        } else {
            self.total_term_freq = -1
        }
    }
}

/// Embeds a [read-only] SegmentInfo and adds per-commit
/// fields.
/// @lucene.experimental */
pub struct SegmentCommitInfo {
    /// The {@link SegmentInfo} that we wrap.
    pub info: SegmentInfo,
    /// How many deleted docs in the segment:
    pub del_count: i32,
    /// Generation number of the live docs file (-1 if there
    /// are no deletes yet):
    pub del_gen: i64,
    /// Normally 1+delGen, unless an exception was hit on last
    /// attempt to write:
    pub next_write_del_gen: i64,
    /// Generation number of the FieldInfos (-1 if there are no updates)
    field_infos_gen: i64,
    /// Normally 1+fieldInfosGen, unless an exception was hit on last attempt to
    /// write
    pub next_write_field_infos_gen: i64,
    /// Generation number of the DocValues (-1 if there are no updates)
    pub doc_values_gen: i64,
    /// Normally 1+dvGen, unless an exception was hit on last attempt to
    /// write
    pub next_write_doc_values_gen: i64,
    /// Track the per-field DocValues update files
    pub dv_updates_files: HashMap<i32, HashSet<String>>,
    /// TODO should we add .files() to FieldInfosFormat, like we have on
    /// LiveDocsFormat?
    /// track the fieldInfos update files
    pub field_infos_files: HashSet<String>,

    pub size_in_bytes: i64,
}

impl Serialize for SegmentCommitInfo {
    fn serialize<S>(&self, serializer: S) -> result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_struct("SegmentCommitInfo", 11)?;
        s.serialize_field("info", &self.info)?;
        s.serialize_field("del_count", &self.del_count)?;
        s.serialize_field("del_gen", &self.del_gen)?;
        s.serialize_field("next_write_del_gen", &self.next_write_del_gen)?;
        s.serialize_field("field_infos_gen", &self.field_infos_gen)?;
        s.serialize_field(
            "next_write_field_infos_gen",
            &self.next_write_field_infos_gen,
        )?;
        s.serialize_field("doc_values_gen", &self.doc_values_gen)?;
        s.serialize_field("next_write_doc_values_gen", &self.next_write_doc_values_gen)?;
        s.serialize_field("dv_updates_files", &self.dv_updates_files)?;
        s.serialize_field("field_infos_files", &self.field_infos_files)?;
        s.serialize_field("size_in_bytes", &self.size_in_bytes)?;
        s.end()
    }
}

impl fmt::Display for SegmentCommitInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Ok(s) = ::serde_json::to_string_pretty(self) {
            write!(f, "{}", s)?;
        }

        Ok(())
    }
}

impl SegmentCommitInfo {
    pub fn new(
        info: SegmentInfo,
        del_count: i32,
        del_gen: i64,
        field_infos_gen: i64,
        doc_values_gen: i64,
        dv_updates_files: HashMap<i32, HashSet<String>>,
        field_infos_files: HashSet<String>,
    ) -> SegmentCommitInfo {
        SegmentCommitInfo {
            info,
            del_count,
            del_gen,
            next_write_del_gen: if del_gen == -1 { 1i64 } else { del_gen + 1 },
            field_infos_gen,
            next_write_field_infos_gen: if field_infos_gen == -1 {
                1
            } else {
                field_infos_gen + 1
            },
            doc_values_gen,
            next_write_doc_values_gen: if doc_values_gen == -1 {
                1
            } else {
                doc_values_gen + 1
            },
            dv_updates_files,
            field_infos_files,
            size_in_bytes: -1,
        }
    }

    // pub fn info(&self) -> &SegmentInfo {
    // &self.info
    // }

    pub fn has_deletions(&self) -> bool {
        self.del_gen != -1
    }

    pub fn del_count(&self) -> i32 {
        self.del_count
    }

    pub fn has_field_updates(&self) -> bool {
        self.field_infos_gen != -1
    }

    pub fn field_infos_gen(&self) -> i64 {
        self.field_infos_gen
    }
}

/// Holder class for common parameters used during write.
/// @lucene.experimental
pub struct SegmentWriteState {
    /// {@link InfoStream} used for debugging messages. */
    // info_stream: InfoStream,
    /// {@link Directory} where this segment will be written
    /// to.
    pub directory: DirectoryRc,

    /// {@link SegmentInfo} describing this segment. */
    pub segment_info: SegmentInfo,

    /// {@link FieldInfos} describing all fields in this
    /// segment. */
    pub field_infos: FieldInfos,

    /// Number of deleted documents set while flushing the
    /// segment. */
    pub del_count_on_flush: i64,

    /// Deletes and updates to apply while we are flushing the segment. A Term is
    /// enrolled in here if it was deleted/updated at one point, and it's mapped to
    /// the docIDUpto, meaning any docID &lt; docIDUpto containing this term should
    /// be deleted/updated.
    pub seg_updates: BufferedUpdates,

    /// {@link MutableBits} recording live documents; this is
    /// only set if there is one or more deleted documents. */
    live_docs: Box<Bits>,

    /// Unique suffix for any postings files written for this
    /// segment.  {@link PerFieldPostingsFormat} sets this for
    /// each of the postings formats it wraps.  If you create
    /// a new {@link PostingsFormat} then any files you
    /// write/read must be derived using this suffix (use
    /// {@link IndexFileNames#segmentFileName(String,String,String)}).
    ///
    /// Note: the suffix must be either empty, or be a textual suffix contain exactly two parts
    /// (separated by underscore), or be a base36 generation. */
    pub segment_suffix: String,

    /// {@link IOContext} for all writes; you should pass this
    /// to {@link Directory#createOutput(String,IOContext)}. */
    pub context: IOContext,
}

/// Holder class for common parameters used during read.
/// @lucene.experimental
pub struct SegmentReadState<'a> {
    /// {@link Directory} where this segment is read from.
    pub directory: DirectoryRc,

    /// {@link SegmentInfo} describing this segment.
    pub segment_info: &'a SegmentInfo,

    /// {@link FieldInfos} describing all fields in this
    /// segment. */
    pub field_infos: Arc<FieldInfos>,

    /// {@link IOContext} to pass to {@link
    /// Directory#openInput(String,IOContext)}.
    pub context: IOContext,

    /// Unique suffix for any postings files read for this
    /// segment.  {@link PerFieldPostingsFormat} sets this for
    /// each of the postings formats it wraps.  If you create
    /// a new {@link PostingsFormat} then any files you
    /// write/read must be derived using this suffix (use
    /// {@link IndexFileNames#segmentFileName(String,String,String)}).
    pub segment_suffix: String,
}

impl<'a> SegmentReadState<'a> {
    pub fn new(
        directory: DirectoryRc,
        segment_info: &SegmentInfo,
        field_infos: Arc<FieldInfos>,
        context: IOContext,
        segment_suffix: String,
    ) -> SegmentReadState {
        SegmentReadState {
            directory,
            segment_info,
            field_infos,
            context,
            segment_suffix,
        }
    }

    pub fn with_suffix<'b>(state: &'b SegmentReadState, suffix: &str) -> SegmentReadState<'b> {
        Self::new(
            state.directory.clone(),
            state.segment_info,
            state.field_infos.clone(),
            state.context,
            String::from(suffix),
        )
    }
}

pub struct MergeState {}

impl MergeState {}

#[cfg(test)]
pub mod tests {
    use std::collections::HashMap;

    use super::*;
    use core::index::point_values::PointValuesRef;
    use core::search::bm25_similarity::BM25Similarity;
    use core::util::*;

    use core::codec::FieldsProducerRef;

    pub struct MockNumericValues {
        num: HashMap<i32, u8>,
    }

    impl Default for MockNumericValues {
        fn default() -> MockNumericValues {
            let mut num = HashMap::<i32, u8>::new();

            let norm_value = BM25Similarity::encode_norm_value(1f32, 120);
            num.insert(1, norm_value);
            let norm_value = BM25Similarity::encode_norm_value(1f32, 1000);
            num.insert(2, norm_value);
            MockNumericValues { num }
        }
    }

    impl NumericDocValues for MockNumericValues {
        fn get_with_ctx(
            &self,
            ctx: NumericDocValuesContext,
            doc_id: DocId,
        ) -> Result<(i64, NumericDocValuesContext)> {
            Ok((i64::from(self.num[&doc_id]), ctx))
        }
    }

    #[derive(Default)]
    pub struct MockBits {}

    impl Bits for MockBits {
        fn get_with_ctx(&self, ctx: BitsContext, _index: usize) -> Result<(bool, BitsContext)> {
            Ok((true, ctx))
        }

        fn len(&self) -> usize {
            unimplemented!()
        }
    }

    pub struct MockLeafReader {
        doc_base: DocId,
        live_docs: BitsRef,
        field_infos: FieldInfos,
    }

    impl MockLeafReader {
        pub fn new(doc_base: DocId) -> MockLeafReader {
            let mut infos = Vec::new();
            let field_info_one = FieldInfo::new(
                "test".to_string(),
                1,
                true,
                true,
                false,
                IndexOptions::Docs,
                DocValuesType::Numeric,
                1,
                HashMap::new(),
                1,
                1,
            );
            let field_info_two = FieldInfo::new(
                "test_2".to_string(),
                2,
                true,
                true,
                false,
                IndexOptions::Docs,
                DocValuesType::SortedNumeric,
                2,
                HashMap::new(),
                2,
                2,
            );
            infos.push(field_info_one.unwrap());
            infos.push(field_info_two.unwrap());

            MockLeafReader {
                doc_base,
                live_docs: Arc::new(MatchAllBits::new(0usize)),
                field_infos: FieldInfos::new(infos).unwrap(),
            }
        }
    }

    impl LeafReader for MockLeafReader {
        fn doc_base(&self) -> DocId {
            self.doc_base
        }

        fn name(&self) -> &str {
            "test"
        }

        fn fields(&self) -> Result<FieldsProducerRef> {
            unimplemented!()
        }

        fn term_vector(&self, _doc_id: DocId) -> Result<Box<Fields>> {
            unimplemented!()
        }

        fn point_values(&self) -> Option<PointValuesRef> {
            unimplemented!()
        }

        fn document(&self, _doc_id: DocId, _visitor: &mut StoredFieldVisitor) -> Result<()> {
            unimplemented!()
        }

        fn norm_values(&self, _field: &str) -> Result<Option<Box<NumericDocValues>>> {
            Ok(Some(Box::new(MockNumericValues::default())))
        }

        fn live_docs(&self) -> BitsRef {
            Arc::clone(&self.live_docs)
        }

        fn field_infos(&self) -> &FieldInfos {
            &self.field_infos
        }

        fn max_doc(&self) -> DocId {
            0
        }

        fn get_docs_with_field(&self, _field: &str) -> Result<BitsRef> {
            Ok(Arc::new(MockBits::default()))
        }

        fn get_numeric_doc_values(&self, _field: &str) -> Result<NumericDocValuesRef> {
            Ok(Arc::new(MockNumericValues::default()))
        }

        fn get_binary_doc_values(&self, _field: &str) -> Result<BinaryDocValuesRef> {
            unimplemented!()
        }

        fn get_sorted_doc_values(&self, _field: &str) -> Result<SortedDocValuesRef> {
            unimplemented!()
        }

        fn get_sorted_numeric_doc_values(&self, _field: &str) -> Result<SortedNumericDocValuesRef> {
            // TODO fix this
            // let boxed = Box::new(MockSortedNumericDocValues::new());
            // Ok(Arc::new(Mutex::new(boxed)))
            //
            unimplemented!()
        }

        fn get_sorted_set_doc_values(&self, _field: &str) -> Result<SortedSetDocValuesRef> {
            unimplemented!()
        }

        fn field_info(&self, _field: &str) -> Option<&FieldInfo> {
            unimplemented!()
        }

        fn num_docs(&self) -> i32 {
            0
        }

        fn core_cache_key(&self) -> &str {
            unimplemented!()
        }
    }

    pub struct MockIndexReader {
        leaves: Vec<MockLeafReader>,
    }

    impl MockIndexReader {
        pub fn new(leaves: Vec<MockLeafReader>) -> MockIndexReader {
            MockIndexReader { leaves }
        }
    }

    impl IndexReader for MockIndexReader {
        fn leaves(&self) -> Vec<&LeafReader> {
            let mut leaves: Vec<&LeafReader> = vec![];
            for leaf in &self.leaves {
                leaves.push(leaf);
            }
            leaves
        }

        fn term_vector(&self, _doc_id: DocId) -> Result<Box<Fields>> {
            unimplemented!()
        }

        fn document(&self, _doc_id: DocId, _fields_load: &[String]) -> Result<Document> {
            unimplemented!()
        }

        fn max_doc(&self) -> i32 {
            1
        }

        fn num_docs(&self) -> i32 {
            1
        }
    }
}
