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

mod segment_infos;

pub use self::segment_infos::*;

mod segment_infos_format;

pub use self::segment_infos_format::*;

use serde::ser::SerializeStruct;
use serde::{Serialize, Serializer};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::result;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, Ordering as AtomicOrdering};
use std::sync::Arc;

use regex::Regex;

use core::codec::field_infos::FieldInfos;
use core::codec::{Codec, LiveDocsFormat};
use core::index::writer::BufferedUpdates;
use core::search::sort_field::Sort;
use core::store::directory::Directory;
use core::store::IOContext;
use core::util::to_base36;
use core::util::FixedBitSet;
use core::util::Version;
use core::util::ID_LENGTH;

use error::{
    ErrorKind::{IllegalArgument, IllegalState},
    Result,
};

// index file names
pub const INDEX_FILE_SEGMENTS: &str = "segments";
pub const INDEX_FILE_PENDING_SEGMENTS: &str = "pending_segments";
pub const INDEX_FILE_OLD_SEGMENT_GEN: &str = "segments.gen";

pub const CODEC_FILE_PATTERN: &str = r"_[a-z0-9]+(_.*)?\..*";
pub const CODEC_UPDATE_FNM_PATTERN: &str = r"(_[a-z0-9]+){2}\.fnm";
pub const CODEC_UPDATE_DV_PATTERN: &str = r"(_[a-z0-9]+){2}(_.*)*\.dv[md]";

#[allow(dead_code)]
fn matches_extension(filename: &str, ext: &str) -> bool {
    filename.ends_with(ext)
}

// locates the boundary of the segment name, or None
fn index_of_segment_name(filename: &str) -> Option<usize> {
    // If it is a .del file, there's an '_' after the first character
    let filename = &filename[1..];
    if let Some(i) = filename.find('_') {
        return Some(i + 1);
    }
    filename.find('.').map(|i| i + 1)
}

pub fn strip_segment_name(name: &str) -> &str {
    if let Some(idx) = index_of_segment_name(name) {
        &name[idx..]
    } else {
        name
    }
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

pub fn file_name_from_generation(base: &str, ext: &str, gen: u64) -> String {
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

/// Returns the generation from this file name,
/// or 0 if there is no generation
pub fn parse_generation(filename: &str) -> Result<i64> {
    debug_assert!(filename.starts_with('_'));
    let parts: Vec<&str> = strip_extension(filename)[1..].split('_').collect();
    // 4 cases:
    // segment.ext
    // segment_gen.ext
    // segment_codec_suffix.ext
    // segment_gen_codec_suffix.ext
    if parts.len() == 2 || parts.len() == 4 {
        Ok(parts[1].parse()?)
    } else {
        Ok(0)
    }
}

/// Parses the segment name out of the given file name.
/// @return the segment name only, or filename if it
/// does not contain a '.' and '_'.
pub fn parse_segment_name(filename: &str) -> &str {
    if let Some(idx) = index_of_segment_name(filename) {
        &filename[..idx]
    } else {
        filename
    }
}

/// Removes the extension (anything after the first '.'),
/// otherwise returns the original filename.
fn strip_extension(filename: &str) -> &str {
    if let Some(idx) = filename.find('.') {
        &filename[..idx]
    } else {
        filename
    }
}

pub const SEGMENT_USE_COMPOUND_YES: u8 = 0x01;
pub const SEGMENT_USE_COMPOUND_NO: u8 = 0xff;

pub struct SegmentInfo<D: Directory, C: Codec> {
    pub name: String,
    pub max_doc: i32,
    pub directory: Arc<D>,
    pub is_compound_file: AtomicBool,
    pub id: [u8; ID_LENGTH],
    pub codec: Option<Arc<C>>,
    pub diagnostics: HashMap<String, String>,
    pub attributes: HashMap<String, String>,
    pub index_sort: Option<Sort>,
    pub version: Version,
    pub set_files: HashSet<String>,
}

impl<D: Directory, C: Codec> SegmentInfo<D, C> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        version: Version,
        name: &str,
        max_doc: i32,
        directory: Arc<D>,
        is_compound_file: bool,
        codec: Option<Arc<C>>,
        diagnostics: HashMap<String, String>,
        id: [u8; ID_LENGTH],
        attributes: HashMap<String, String>,
        index_sort: Option<Sort>,
    ) -> Result<SegmentInfo<D, C>> {
        Ok(SegmentInfo {
            name: String::from(name),
            max_doc,
            directory,
            is_compound_file: AtomicBool::new(is_compound_file),
            id,
            version,
            codec,
            diagnostics,
            attributes,
            set_files: HashSet::new(),
            index_sort,
        })
    }

    pub fn set_codec(&mut self, codec: Arc<C>) {
        self.codec = Some(codec);
    }

    pub fn codec(&self) -> &Arc<C> {
        assert!(self.codec.is_some());
        &self.codec.as_ref().unwrap()
    }

    pub fn max_doc(&self) -> i32 {
        debug_assert!(self.max_doc >= 0);
        self.max_doc
    }

    pub fn is_compound_file(&self) -> bool {
        self.is_compound_file.load(AtomicOrdering::Acquire)
    }

    pub fn set_use_compound_file(&self) {
        self.is_compound_file.store(true, AtomicOrdering::Release)
    }

    pub fn get_id(&self) -> &[u8] {
        &self.id
    }

    /// Return all files referenced by this SegmentInfo.
    pub fn files(&self) -> &HashSet<String> {
        // debug_assert!(!self.set_files.is_empty());
        &self.set_files
    }

    pub fn set_files(&mut self, files: &HashSet<String>) -> Result<()> {
        self.set_files = HashSet::with_capacity(files.len());
        self.add_files(files)
    }

    pub fn add_file(&mut self, file: &str) -> Result<()> {
        self.check_file_name(file)?;
        let file = self.named_for_this_segment(file);
        self.set_files.insert(file);
        Ok(())
    }

    pub fn add_files(&mut self, files: &HashSet<String>) -> Result<()> {
        for f in files {
            self.check_file_name(f)?;
        }
        for f in files {
            let file = self.named_for_this_segment(&f);
            self.set_files.insert(file);
        }
        Ok(())
    }

    fn check_file_name(&self, file: &str) -> Result<()> {
        let pattern = Regex::new(CODEC_FILE_PATTERN).unwrap();
        if !pattern.is_match(file) {
            bail!(IllegalArgument("invalid code file_name.".into()));
        }
        if file.to_lowercase().ends_with(".tmp") {
            bail!(IllegalArgument(
                "invalid code file_name, can't end with .tmp extension".into()
            ));
        }
        Ok(())
    }

    fn named_for_this_segment(&self, file: &str) -> String {
        let mut name = self.name.clone();
        name.push_str(strip_segment_name(file));
        name
    }

    pub fn index_sort(&self) -> Option<&Sort> {
        self.index_sort.as_ref()
    }

    pub fn set_diagnostics(&mut self, diags: HashMap<String, String>) {
        self.diagnostics = diags;
    }

    pub fn set_max_doc(&mut self, max_doc: i32) -> Result<()> {
        if self.max_doc != -1 {
            bail!(IllegalState("max_doc was already set".into()));
        }
        self.max_doc = max_doc;
        Ok(())
    }
}

impl<D: Directory, C: Codec> Clone for SegmentInfo<D, C> {
    fn clone(&self) -> Self {
        SegmentInfo {
            name: self.name.clone(),
            max_doc: self.max_doc,
            is_compound_file: AtomicBool::new(self.is_compound_file()),
            directory: Arc::clone(&self.directory),
            id: self.id,
            codec: self.codec.as_ref().map(|c| Arc::clone(c)),
            diagnostics: self.diagnostics.clone(),
            attributes: self.attributes.clone(),
            version: self.version,
            set_files: self.set_files.clone(),
            index_sort: self.index_sort.clone(),
        }
    }
}

impl<D: Directory, C: Codec> Hash for SegmentInfo<D, C> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(self.name.as_bytes());
    }
}

impl<D: Directory, C: Codec> Serialize for SegmentInfo<D, C> {
    fn serialize<S>(&self, serializer: S) -> result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_struct("SegmentInfo", 8)?;
        s.serialize_field("name", &self.name)?;
        s.serialize_field("max_doc", &self.max_doc)?;
        s.serialize_field("is_compound_file", &self.is_compound_file())?;
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

impl<D: Directory, C: Codec> fmt::Debug for SegmentInfo<D, C> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Ok(s) = ::serde_json::to_string_pretty(self) {
            write!(f, "{}", s)?;
        }

        Ok(())
    }
}

/// Embeds a [read-only] SegmentInfo and adds per-commit
/// fields.
/// @lucene.experimental */
pub struct SegmentCommitInfo<D: Directory, C: Codec> {
    /// The {@link SegmentInfo} that we wrap.
    pub info: SegmentInfo<D, C>,
    /// How many deleted docs in the segment:
    pub del_count: AtomicI32,
    /// Generation number of the live docs file (-1 if there
    /// are no deletes yet):
    pub del_gen: AtomicI64,
    /// Normally 1+delGen, unless an exception was hit on last
    /// attempt to write:
    pub next_write_del_gen: AtomicI64,
    /// Generation number of the FieldInfos (-1 if there are no updates)
    field_infos_gen: AtomicI64,
    /// Normally 1+fieldInfosGen, unless an exception was hit on last attempt to
    /// write
    pub next_write_field_infos_gen: AtomicI64,
    /// Generation number of the DocValues (-1 if there are no updates)
    pub doc_values_gen: AtomicI64,
    /// Normally 1+dvGen, unless an exception was hit on last attempt to
    /// write
    pub next_write_doc_values_gen: AtomicI64,
    /// Track the per-field DocValues update files
    pub dv_updates_files: HashMap<i32, HashSet<String>>,
    /// TODO should we add .files() to FieldInfosFormat, like we have on
    /// LiveDocsFormat?
    /// track the fieldInfos update files
    pub field_infos_files: HashSet<String>,

    pub size_in_bytes: AtomicI64,
    // NOTE: only used in-RAM by IW to track buffered deletes;
    // this is never written to/read from the Directory
    pub buffered_deletes_gen: AtomicI64,
}

impl<D: Directory, C: Codec> Hash for SegmentCommitInfo<D, C> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.info.hash(state);
    }
}

impl<D: Directory, C: Codec> SegmentCommitInfo<D, C> {
    pub fn new(
        info: SegmentInfo<D, C>,
        del_count: i32,
        del_gen: i64,
        field_infos_gen: i64,
        doc_values_gen: i64,
        dv_updates_files: HashMap<i32, HashSet<String>>,
        field_infos_files: HashSet<String>,
    ) -> SegmentCommitInfo<D, C> {
        let field_info_gen = if field_infos_gen == -1 {
            1
        } else {
            field_infos_gen + 1
        };
        SegmentCommitInfo {
            info,
            del_count: AtomicI32::new(del_count),
            del_gen: AtomicI64::new(del_gen),
            next_write_del_gen: AtomicI64::new(if del_gen == -1 { 1i64 } else { del_gen + 1 }),
            field_infos_gen: AtomicI64::new(field_infos_gen),
            next_write_field_infos_gen: AtomicI64::new(field_info_gen),
            doc_values_gen: AtomicI64::new(doc_values_gen),
            next_write_doc_values_gen: AtomicI64::new(if doc_values_gen == -1 {
                1
            } else {
                doc_values_gen + 1
            }),
            dv_updates_files,
            field_infos_files,
            size_in_bytes: AtomicI64::new(-1),
            buffered_deletes_gen: AtomicI64::new(0),
        }
    }

    pub fn files(&self) -> HashSet<String> {
        let mut files = HashSet::new();
        // Start from the wrapped info's files:
        for f in self.info.files() {
            files.insert(f.clone());
        }
        // TODO we could rely on TrackingDir.getCreatedFiles() (like we do for
        // updates) and then maybe even be able to remove LiveDocsFormat.files().

        // Must separately add any live docs files:
        self.info.codec().live_docs_format().files(self, &mut files);

        // must separately add any field updates files
        for fs in self.dv_updates_files.values() {
            for f in fs {
                files.insert(f.clone());
            }
        }

        // must separately add field_infos files
        for f in &self.field_infos_files {
            files.insert(f.clone());
        }

        files
    }

    pub fn has_deletions(&self) -> bool {
        self.del_gen() != -1
    }

    pub fn del_count(&self) -> i32 {
        self.del_count.load(AtomicOrdering::Acquire)
    }

    pub fn set_del_count(&self, del_count: i32) -> Result<()> {
        if del_count < 0 || del_count > self.info.max_doc() {
            bail!(IllegalArgument("invalid del_count".into()));
        }
        self.del_count.store(del_count, AtomicOrdering::Release);
        Ok(())
    }

    pub fn has_field_updates(&self) -> bool {
        self.field_infos_gen() != -1
    }

    pub fn set_field_infos_files(&mut self, field_infos_files: HashSet<String>) {
        self.field_infos_files = field_infos_files;
    }

    pub fn get_doc_values_updates_files(&self) -> &HashMap<i32, HashSet<String>> {
        &self.dv_updates_files
    }

    pub fn set_doc_values_updates_files(
        &mut self,
        dv_updates_files: HashMap<i32, HashSet<String>>,
    ) {
        self.dv_updates_files = dv_updates_files;
    }

    pub fn field_infos_gen(&self) -> i64 {
        self.field_infos_gen.load(AtomicOrdering::Acquire)
    }

    pub fn next_write_field_infos_gen(&self) -> i64 {
        self.next_write_field_infos_gen
            .load(AtomicOrdering::Acquire)
    }

    pub fn set_next_write_field_infos_gen(&self, gen: i64) {
        self.next_write_field_infos_gen
            .store(gen, AtomicOrdering::Release)
    }

    pub fn next_write_doc_values_gen(&self) -> i64 {
        self.next_write_doc_values_gen.load(AtomicOrdering::Acquire)
    }

    pub fn doc_values_gen(&self) -> i64 {
        self.doc_values_gen.load(AtomicOrdering::Acquire)
    }

    pub fn set_next_write_doc_values_gen(&self, gen: i64) {
        self.next_write_doc_values_gen
            .store(gen, AtomicOrdering::Release);
    }

    pub fn advance_field_infos_gen(&self) {
        self.field_infos_gen
            .store(self.next_field_infos_gen(), AtomicOrdering::Release);
        self.next_write_field_infos_gen
            .store(self.field_infos_gen() + 1, AtomicOrdering::Release);
        self.size_in_bytes.store(-1, AtomicOrdering::Release);
    }

    pub fn advance_doc_values_gen(&self) {
        self.doc_values_gen
            .store(self.next_write_doc_values_gen(), AtomicOrdering::Release);
        self.next_write_doc_values_gen
            .store(self.doc_values_gen() + 1, AtomicOrdering::Release);
        self.size_in_bytes.store(-1, AtomicOrdering::Release);
    }

    pub fn next_write_del_gen(&self) -> i64 {
        self.next_write_del_gen.load(AtomicOrdering::Acquire)
    }

    pub fn set_next_write_del_gen(&self, gen: i64) {
        self.next_write_del_gen.store(gen, AtomicOrdering::Release)
    }

    pub fn next_field_infos_gen(&self) -> i64 {
        self.next_write_field_infos_gen
            .load(AtomicOrdering::Acquire)
    }

    pub fn advance_next_write_del_gen(&self) {
        self.next_write_del_gen
            .fetch_add(1, AtomicOrdering::Acquire);
    }

    pub fn del_gen(&self) -> i64 {
        self.del_gen.load(AtomicOrdering::Acquire)
    }

    pub fn advance_del_gen(&self) {
        self.del_gen.store(
            self.next_write_del_gen.load(AtomicOrdering::Acquire),
            AtomicOrdering::Release,
        );
        self.next_write_del_gen.fetch_add(1, AtomicOrdering::AcqRel);
        self.size_in_bytes.store(-1, AtomicOrdering::Release);
    }

    pub fn size_in_bytes(&self) -> i64 {
        let mut size = self.size_in_bytes.load(AtomicOrdering::Acquire);
        if size == -1 {
            let mut sum = 0;
            for name in self.files() {
                match self.info.directory.file_length(&name) {
                    Ok(l) => {
                        sum += l;
                    }
                    Err(e) => {
                        warn!("get file '{}' length failed by '{:?}'", name, e);
                    }
                }
            }
            size = sum;
            self.size_in_bytes.store(size, AtomicOrdering::Release);
        }
        size
    }

    pub fn buffered_deletes_gen(&self) -> i64 {
        self.buffered_deletes_gen.load(AtomicOrdering::Acquire)
    }

    pub fn set_buffered_deletes_gen(&self, v: i64) {
        self.buffered_deletes_gen.store(v, AtomicOrdering::Release);
        self.size_in_bytes.store(-1, AtomicOrdering::Release);
    }
}

impl<D: Directory, C: Codec> Clone for SegmentCommitInfo<D, C> {
    fn clone(&self) -> Self {
        let infos = SegmentCommitInfo::new(
            self.info.clone(),
            self.del_count(),
            self.del_gen(),
            self.field_infos_gen(),
            self.doc_values_gen(),
            self.dv_updates_files.clone(),
            self.field_infos_files.clone(),
        );
        // Not clear that we need to carry over nextWriteDelGen
        // (i.e. do we ever clone after a failed write and
        // before the next successful write?), but just do it to
        // be safe:
        infos
            .next_write_del_gen
            .store(self.next_write_del_gen(), AtomicOrdering::Release);
        infos
            .next_write_field_infos_gen
            .store(self.next_write_field_infos_gen(), AtomicOrdering::Release);
        infos.set_next_write_doc_values_gen(self.next_write_doc_values_gen());
        infos
    }
}

impl<D: Directory, C: Codec> Eq for SegmentCommitInfo<D, C> {}

// WARN: only compare the segment name, maybe we should compare the raw pointer or the full struct?
impl<D: Directory, C: Codec> PartialEq for SegmentCommitInfo<D, C> {
    fn eq(&self, other: &SegmentCommitInfo<D, C>) -> bool {
        self.info.name.eq(&other.info.name)
    }
}

impl<D: Directory, C: Codec> Serialize for SegmentCommitInfo<D, C> {
    fn serialize<S>(&self, serializer: S) -> result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_struct("SegmentCommitInfo", 11)?;
        s.serialize_field("info", &self.info)?;
        s.serialize_field("del_count", &self.del_count())?;
        s.serialize_field("del_gen", &self.del_gen())?;
        s.serialize_field("next_write_del_gen", &self.next_write_del_gen())?;
        s.serialize_field("field_infos_gen", &self.field_infos_gen())?;
        s.serialize_field(
            "next_write_field_infos_gen",
            &self
                .next_write_field_infos_gen
                .load(AtomicOrdering::Acquire),
        )?;
        s.serialize_field("doc_values_gen", &self.doc_values_gen())?;
        s.serialize_field(
            "next_write_doc_values_gen",
            &self.next_write_doc_values_gen(),
        )?;
        s.serialize_field("dv_updates_files", &self.dv_updates_files)?;
        s.serialize_field("field_infos_files", &self.field_infos_files)?;
        s.serialize_field("size_in_bytes", &self.size_in_bytes())?;
        s.end()
    }
}

impl<D: Directory, C: Codec> fmt::Display for SegmentCommitInfo<D, C> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Ok(s) = ::serde_json::to_string_pretty(self) {
            write!(f, "{}", s)?;
        }

        Ok(())
    }
}

impl<D: Directory, C: Codec> fmt::Debug for SegmentCommitInfo<D, C> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Ok(s) = ::serde_json::to_string_pretty(self) {
            write!(f, "{}", s)?;
        }

        Ok(())
    }
}

/// Holder class for common parameters used during write.
/// @lucene.experimental
pub struct SegmentWriteState<D: Directory, DW: Directory, C: Codec> {
    /// {@link InfoStream} used for debugging messages. */
    // info_stream: InfoStream,
    /// {@link Directory} where this segment will be written
    /// to.
    pub directory: Arc<DW>,

    /// {@link SegmentInfo} describing this segment. */
    pub segment_info: SegmentInfo<D, C>,

    /// {@link FieldInfos} describing all fields in this
    /// segment. */
    pub field_infos: FieldInfos,

    /// Number of deleted documents set while flushing the
    /// segment. */
    pub del_count_on_flush: u32,

    /// Deletes and updates to apply while we are flushing the segment. A Term is
    /// enrolled in here if it was deleted/updated at one point, and it's mapped to
    /// the docIDUpto, meaning any docID &lt; docIDUpto containing this term should
    /// be deleted/updated.
    pub seg_updates: Option<*const BufferedUpdates<C>>,

    /// {@link MutableBits} recording live documents; this is
    /// only set if there is one or more deleted documents. */
    pub live_docs: FixedBitSet,

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

impl<D: Directory, DW: Directory, C: Codec> SegmentWriteState<D, DW, C> {
    pub fn new(
        directory: Arc<DW>,
        segment_info: SegmentInfo<D, C>,
        field_infos: FieldInfos,
        seg_updates: Option<*const BufferedUpdates<C>>,
        context: IOContext,
        segment_suffix: String,
    ) -> Self {
        debug_assert!(Self::assert_segment_suffix(&segment_suffix));
        SegmentWriteState {
            directory,
            segment_info,
            field_infos,
            del_count_on_flush: 0,
            seg_updates,
            live_docs: FixedBitSet::default(),
            segment_suffix,
            context,
        }
    }

    pub fn seg_updates(&self) -> &BufferedUpdates<C> {
        debug_assert!(self.seg_updates.is_some());
        unsafe { &*self.seg_updates.unwrap() }
    }

    // currently only used by assert? clean up and make real check?
    // either it's a segment suffix (_X_Y) or it's a parseable generation
    // TODO: this is very confusing how ReadersAndUpdates passes generations via
    // this mechanism, maybe add 'generation' explicitly to ctor create the 'actual suffix' here?
    fn assert_segment_suffix(segment_suffix: &str) -> bool {
        if !segment_suffix.is_empty() {
            let parts: Vec<&str> = segment_suffix.split('_').collect();
            if parts.len() == 2 {
                true
            } else if parts.len() == 1 {
                i64::from_str_radix(segment_suffix, 36).is_ok()
            } else {
                false // invalid
            }
        } else {
            true
        }
    }
}

impl<D: Directory, DW: Directory, C: Codec> Clone for SegmentWriteState<D, DW, C> {
    fn clone(&self) -> Self {
        SegmentWriteState {
            directory: Arc::clone(&self.directory),
            segment_info: self.segment_info.clone(),
            field_infos: self.field_infos.clone(),
            del_count_on_flush: self.del_count_on_flush,
            seg_updates: None,
            // no used
            live_docs: FixedBitSet::default(),
            // TODO, fake clone
            segment_suffix: self.segment_suffix.clone(),
            context: self.context,
        }
    }
}

/// Holder class for common parameters used during read.
/// @lucene.experimental
pub struct SegmentReadState<'a, D: Directory, DW: Directory, C: Codec> {
    /// {@link Directory} where this segment is read from.
    pub directory: Arc<DW>,

    /// {@link SegmentInfo} describing this segment.
    pub segment_info: &'a SegmentInfo<D, C>,

    /// {@link FieldInfos} describing all fields in this
    /// segment. */
    pub field_infos: Arc<FieldInfos>,

    /// {@link IOContext} to pass to {@link
    /// Directory#openInput(String,IOContext)}.
    pub context: &'a IOContext,

    /// Unique suffix for any postings files read for this
    /// segment.  {@link PerFieldPostingsFormat} sets this for
    /// each of the postings formats it wraps.  If you create
    /// a new {@link PostingsFormat} then any files you
    /// write/read must be derived using this suffix (use
    /// {@link IndexFileNames#segmentFileName(String,String,String)}).
    pub segment_suffix: String,
}

impl<'a, D: Directory, DW: Directory, C: Codec> SegmentReadState<'a, D, DW, C> {
    pub fn new(
        directory: Arc<DW>,
        segment_info: &'a SegmentInfo<D, C>,
        field_infos: Arc<FieldInfos>,
        context: &'a IOContext,
        segment_suffix: String,
    ) -> SegmentReadState<'a, D, DW, C> {
        SegmentReadState {
            directory,
            segment_info,
            field_infos,
            context,
            segment_suffix,
        }
    }

    pub fn with_suffix(
        state: &'a SegmentReadState<D, DW, C>,
        suffix: &str,
    ) -> SegmentReadState<'a, D, DW, C> {
        Self::new(
            state.directory.clone(),
            state.segment_info,
            state.field_infos.clone(),
            state.context,
            String::from(suffix),
        )
    }
}
