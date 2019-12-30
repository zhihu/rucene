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

use core::codec::segment_infos::INDEX_FILE_SEGMENTS;
use core::codec::segment_infos::{get_segment_file_name, run_with_find_segment_file, SegmentInfos};
use core::codec::{Codec, CodecTVFields};
use core::doc::{Document, DocumentStoredFieldVisitor};
use core::index::merge::MergePolicy;
use core::index::merge::MergeScheduler;
use core::index::reader::{
    IndexReader, LeafReader, LeafReaderContext, SearchLeafReader, SegmentReader,
};
use core::index::writer::{CommitPoint, IndexWriter};
use core::store::directory::Directory;
use core::store::IOContext;
use core::util::DocId;

use error::{
    ErrorKind::{IllegalArgument, IllegalState},
    Result,
};

use std::{collections::HashMap, fmt, sync::Arc};

///
// Returns <code>true</code> if an index likely exists at
// the specified directory.  Note that if a corrupt index
// exists, or if an index in the process of committing
// @param  directory the directory to check for an index
// @return <code>true</code> if an index exists; <code>false</code> otherwise
//
pub fn index_exist<D: Directory>(directory: &D) -> Result<bool> {
    // LUCENE-2812, LUCENE-2727, LUCENE-4738: this logic will
    // return true in cases that should arguably be false,
    // such as only IW.prepareCommit has been called, or a
    // corrupt first commit, but it's too deadly to make
    // this logic "smarter" and risk accidentally returning
    // false due to various cases like file description
    // exhaustion, access denied, etc., because in that
    // case IndexWriter may delete the entire index.  It's
    // safer to err towards "index exists" than try to be
    // smart about detecting not-yet-fully-committed or
    // corrupt indices.  This means that IndexWriter will
    // throw an exception on such indices and the app must
    // resolve the situation manually:
    let prefix = format!("{}_", INDEX_FILE_SEGMENTS);
    Ok((&directory.list_all()?)
        .iter()
        .any(|f| f.starts_with(&prefix)))
}

pub struct StandardDirectoryReader<
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
> {
    directory: Arc<D>,
    segment_infos: SegmentInfos<D, C>,
    max_doc: i32,
    pub num_docs: i32,
    starts: Vec<i32>,
    readers: Vec<Arc<SegmentReader<D, C>>>,
    apply_all_deletes: bool,
    write_all_deletes: bool,
    writer: Option<IndexWriter<D, C, MS, MP>>,
}

impl<D, C, MS, MP> StandardDirectoryReader<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    pub fn open(directory: Arc<D>) -> Result<Self> {
        let segment_file_name = get_segment_file_name(directory.as_ref())?;
        let segment_infos = SegmentInfos::read_commit(&directory, &segment_file_name)?;
        let mut readers = Vec::with_capacity(segment_infos.segments.len());
        for seg_info in &segment_infos.segments {
            let s = SegmentReader::open(seg_info, &IOContext::READ)?;
            readers.push(Arc::new(s));
        }
        Ok(Self::new(
            directory,
            readers,
            segment_infos,
            None,
            false,
            false,
        ))
    }

    /// Used by near real-time searcher
    pub fn open_by_writer(
        writer: IndexWriter<D, C, MS, MP>,
        infos: &SegmentInfos<D, C>,
        apply_all_deletes: bool,
        write_all_deletes: bool,
    ) -> Result<Self> {
        // IndexWriter synchronizes externally before calling
        // us, which ensures infos will not change; so there's
        // no need to process segments in reverse order
        let num_segments = infos.len();
        let mut readers = Vec::with_capacity(num_segments);

        let mut segment_infos = infos.clone();
        let mut infos_upto = 0;
        for i in 0..infos.segments.len() {
            // NOTE: important that we use infos not
            // segmentInfos here, so that we are passing the
            // actual instance of SegmentInfoPerCommit in
            // IndexWriter's segmentInfos:
            let rld = writer.reader_pool().get_or_create(&infos.segments[i])?;
            let reader = rld.get_readonly_clone(&IOContext::READ)?;
            if reader.num_docs() > 0 {
                // Steal the ref:
                readers.push(Arc::new(reader));
                infos_upto += 1;
            } else {
                segment_infos.segments.remove(infos_upto);
            }
            writer.reader_pool().release(&rld)?;
        }
        writer.inc_ref_deleter(&segment_infos)?;
        let dir = Arc::clone(writer.directory());
        Ok(StandardDirectoryReader::new(
            dir,
            readers,
            segment_infos,
            Some(writer),
            apply_all_deletes,
            write_all_deletes,
        ))
    }

    pub fn open_by_readers(
        directory: Arc<D>,
        infos: SegmentInfos<D, C>,
        old_readers: &[Arc<SegmentReader<D, C>>],
    ) -> Result<Self> {
        let mut reader_indexes: HashMap<&str, usize> = HashMap::with_capacity(old_readers.len());
        for (i, r) in old_readers.iter().enumerate() {
            reader_indexes.insert(r.name(), i);
        }

        let mut new_readers = Vec::with_capacity(old_readers.len());
        for commit_info in &infos.segments {
            // find SegmentReader for this segment
            let old_reader = reader_indexes
                .get(&commit_info.info.name.as_ref())
                .map(|idx| &old_readers[*idx]);
            if let Some(reader) = old_reader {
                if commit_info.info.get_id() != reader.si.info.get_id() {
                    bail!(IllegalState(format!(
                        "same segment {} has invalid doc count change",
                        commit_info.info.name
                    )));
                }

                if commit_info.info.is_compound_file() == reader.si.info.is_compound_file() {
                    if reader.si.del_gen() == commit_info.del_gen()
                        && reader.si.field_infos_gen() == commit_info.field_infos_gen()
                    {
                        // No change; this reader will be shard between the old and new one
                        new_readers.push(Arc::clone(&reader));
                    } else {
                        let new_reader = if reader.si.del_gen() == commit_info.del_gen() {
                            // only DV updates
                            SegmentReader::build_from(
                                Arc::clone(commit_info),
                                reader.as_ref(),
                                reader.live_docs(),
                                reader.num_docs(),
                                true,
                            )?
                        } else {
                            // both DV and liveDocs have changed
                            SegmentReader::build_from_reader(
                                Arc::clone(commit_info),
                                reader.as_ref(),
                            )?
                        };
                        new_readers.push(Arc::new(new_reader));
                    }
                    continue;
                }
            }
            let new_reader = SegmentReader::open(commit_info, &IOContext::READ)?;
            new_readers.push(Arc::new(new_reader));
        }
        Ok(StandardDirectoryReader::new(
            directory,
            new_readers,
            infos,
            None,
            false,
            false,
        ))
    }

    fn new(
        directory: Arc<D>,
        mut readers: Vec<Arc<SegmentReader<D, C>>>,
        segment_infos: SegmentInfos<D, C>,
        writer: Option<IndexWriter<D, C, MS, MP>>,
        apply_all_deletes: bool,
        write_all_deletes: bool,
    ) -> Self {
        let mut starts = Vec::with_capacity(readers.len() + 1);
        let mut max_doc = 0;
        let mut num_docs = 0;
        for reader in &mut readers {
            starts.push(max_doc);
            max_doc += reader.max_docs();
            num_docs += reader.num_docs();
        }

        starts.push(max_doc);

        StandardDirectoryReader {
            directory,
            segment_infos,
            max_doc,
            num_docs,
            starts,
            readers,
            writer,
            apply_all_deletes,
            write_all_deletes,
        }
    }

    pub fn set_writer(&mut self, writer: Option<IndexWriter<D, C, MS, MP>>) {
        self.writer = writer;
    }

    pub fn set_associate_writer(
        &mut self,
        writer: Option<IndexWriter<D, C, MS, MP>>,
    ) -> Result<()> {
        if self.writer.is_none() && self.readers.len() > 0 && writer.is_some() {
            writer
                .as_ref()
                .unwrap()
                .inc_ref_deleter(&self.segment_infos)?;
            self.writer = writer;
        }
        Ok(())
    }

    pub fn get_writer(&self) -> Option<IndexWriter<D, C, MS, MP>> {
        self.writer.clone()
    }

    pub fn version(&self) -> i64 {
        self.segment_infos.version
    }

    pub fn open_if_changed(&self, commit: Option<&CommitPoint>) -> Result<Option<Self>> {
        // If we were obtained by writer.getReader(), re-ask the
        // writer to get a new reader.
        if self.writer.is_some() {
            self.open_from_writer(commit)
        } else {
            self.do_open_no_writer(commit)
        }
    }

    fn open_from_writer(&self, commit: Option<&CommitPoint>) -> Result<Option<Self>> {
        if commit.is_some() {
            Ok(Some(self.open_from_commit(commit)?))
        } else {
            let writer = self.writer.as_ref().unwrap();
            if writer.nrt_is_current(&self.segment_infos) {
                return Ok(None);
            }

            let mut reader = writer.get_reader(self.apply_all_deletes, self.write_all_deletes)?;
            reader.writer = self.writer.clone();
            if reader.version() == self.segment_infos.version {
                return Ok(None);
            }
            Ok(Some(reader))
        }
    }

    fn open_from_commit(&self, commit: Option<&CommitPoint>) -> Result<Self> {
        run_with_find_segment_file(&self.directory, commit, |(dir, file_name)| {
            let infos = SegmentInfos::read_commit(dir, file_name)?;
            Self::open_by_readers(Arc::clone(dir), infos, &self.readers)
        })
    }

    fn do_open_no_writer(&self, commit: Option<&CommitPoint>) -> Result<Option<Self>> {
        if let Some(commit) = commit {
            if let Some(name) = self.segment_infos.segment_file_name() {
                if commit.segments_file_name() == name {
                    return Ok(None);
                }
            }
        } else if self.is_current()? {
            return Ok(None);
        }
        Ok(Some(self.open_from_commit(commit)?))
    }

    pub fn is_current(&self) -> Result<bool> {
        match &self.writer {
            Some(writer) if !writer.is_closed() => Ok(writer.nrt_is_current(&self.segment_infos)),
            _ => {
                // Fully read the segments file: this ensures that it's
                // completely written so that if
                // IndexWriter.prepareCommit has been called (but not
                // yet commit), then the reader will still see itself as
                // current:
                let sis = SegmentInfos::<D, C>::read_latest_commit(&self.directory)?;
                // we loaded SegmentInfos from the directory
                Ok(sis.version == self.segment_infos.version)
            }
        }
    }
}

impl<D, C, MS, MP> IndexReader for StandardDirectoryReader<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    type Codec = C;
    fn leaves(&self) -> Vec<LeafReaderContext<'_, C>> {
        self.readers
            .iter()
            .enumerate()
            .map(|(i, r)| {
                LeafReaderContext::new(self, r.as_ref() as &SearchLeafReader<C>, i, self.starts[i])
            })
            .collect()
    }

    fn term_vector(&self, doc_id: DocId) -> Result<Option<CodecTVFields<C>>> {
        if doc_id < 0 || doc_id > self.max_doc {
            bail!(IllegalArgument(format!("invalid doc id: {}", doc_id)));
        }
        let i = match self.starts.binary_search_by(|&probe| probe.cmp(&doc_id)) {
            Ok(i) => i,
            Err(i) => i - 1,
        };
        debug_assert!(i < self.readers.len());
        LeafReader::term_vector(self.readers[i].as_ref(), doc_id - self.starts[i])
    }

    fn document(&self, doc_id: DocId, fields_load: &[String]) -> Result<Document> {
        if doc_id < 0 || doc_id > self.max_doc {
            bail!(IllegalArgument(format!(
                "doc_id {} invalid: [max_doc={}]",
                doc_id, self.max_doc
            )));
        }

        let pos = match self.starts.binary_search_by(|&probe| probe.cmp(&doc_id)) {
            Ok(i) => i,
            Err(i) => i - 1,
        };
        let mut visitor = DocumentStoredFieldVisitor::new(&fields_load);
        LeafReader::document(
            self.readers[pos].as_ref(),
            doc_id - self.starts[pos],
            &mut visitor,
        )?;
        Ok(visitor.document())
    }

    fn max_doc(&self) -> i32 {
        self.max_doc
    }

    fn num_docs(&self) -> i32 {
        self.num_docs
    }

    fn refresh(&self) -> Result<Option<Box<dyn IndexReader<Codec = C>>>> {
        if let Some(reader) = self.open_if_changed(None)? {
            Ok(Some(Box::new(reader)))
        } else {
            Ok(None)
        }
    }
}

impl<D, C, MS, MP> fmt::Debug for StandardDirectoryReader<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let seg_infos = if let Some(name) = self.segment_infos.segment_file_name() {
            format!("{}:{}", name, self.segment_infos.version)
        } else {
            String::new()
        };
        let leaf_readers: Vec<&str> = self.leaves().iter().map(|l| l.reader.name()).collect();
        write!(
            f,
            "StandardDirectoryReader({},  leaves: {:?})",
            seg_infos, leaf_readers,
        )
    }
}

impl<D, C, MS, MP> AsRef<dyn IndexReader<Codec = C>> for StandardDirectoryReader<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    fn as_ref(&self) -> &(dyn IndexReader<Codec = C> + 'static) {
        self
    }
}

impl<D, C, MS, MP> Drop for StandardDirectoryReader<D, C, MS, MP>
where
    D: Directory + Send + Sync + 'static,
    C: Codec,
    MS: MergeScheduler,
    MP: MergePolicy,
{
    fn drop(&mut self) {
        if let Some(ref writer) = self.writer {
            if let Err(e) = writer.dec_ref_deleter(&self.segment_infos) {
                error!(
                    "StandardDirectoryReader drop failed by dec_ref_deleter: {:?}",
                    e
                );
            }
        }
    }
}
