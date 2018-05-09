use core::doc::Document;
use core::doc::DocumentStoredFieldVisitor;
use core::index::field_info::Fields;
use core::index::IndexReader;
use core::index::LeafReader;
use core::index::SegmentInfos;
use core::index::{get_segment_file_name, SegmentReader};
use core::store::{DirectoryRc, IOContext};
use core::util::DocId;
use error::Result;

pub struct StandardDirectoryReader {
    #[allow(dead_code)]
    directory: DirectoryRc,
    #[allow(dead_code)]
    segment_infos: SegmentInfos,
    max_doc: i32,
    num_docs: i32,
    starts: Vec<i32>,
    readers: Vec<SegmentReader>,
}

impl StandardDirectoryReader {
    pub fn open(directory: DirectoryRc) -> Result<StandardDirectoryReader> {
        let segment_file_name = get_segment_file_name(&directory)?;
        let segment_infos = SegmentInfos::read_commit(&directory, &segment_file_name)?;
        let mut readers = Vec::with_capacity(segment_infos.size());
        for seg_info in &segment_infos.segments {
            let s = SegmentReader::open(seg_info, IOContext::Read(false))?;
            readers.push(s);
        }
        let mut starts = Vec::with_capacity(readers.len() + 1);
        let mut max_doc = 0;
        let mut num_docs = 0;
        for reader in &mut readers {
            reader.set_doc_base(max_doc);
            starts.push(max_doc);
            max_doc += reader.max_docs();
            num_docs += reader.num_docs();
            // reader.set_context(LeafReaderContext::new(max_doc.clone(), i))
        }

        starts.push(max_doc);

        Ok(StandardDirectoryReader {
            directory,
            segment_infos,
            max_doc,
            num_docs,
            starts,
            readers,
        })
    }
}

impl IndexReader for StandardDirectoryReader {
    fn leaves(&self) -> Vec<&LeafReader> {
        self.readers.iter().map(|r| r as &LeafReader).collect()
    }

    fn term_vector(&self, doc_id: DocId) -> Result<Box<Fields>> {
        if doc_id < 0 || doc_id > self.max_doc {
            bail!("invalid doc id: {}", doc_id);
        }
        let i = match self.starts.binary_search_by(|&probe| probe.cmp(&doc_id)) {
            Ok(i) => i,
            Err(i) => i - 1,
        };
        assert!(i < self.readers.len());
        self.readers[i].term_vector(doc_id - self.starts[i])
    }

    fn document(&self, doc_id: DocId, fields_load: &[String]) -> Result<Document> {
        if doc_id < 0 || doc_id > self.max_doc {
            bail!("doc_id {} invalid: [max_doc={}]", doc_id, self.max_doc);
        }

        let pos = match self.starts.binary_search_by(|&probe| probe.cmp(&doc_id)) {
            Ok(i) => i,
            Err(i) => i - 1,
        };
        let mut visitor = DocumentStoredFieldVisitor::new(&fields_load);
        self.readers[pos].document(doc_id - self.starts[pos], &mut visitor)?;
        Ok(visitor.document())
    }

    fn max_doc(&self) -> i32 {
        self.max_doc
    }

    fn num_docs(&self) -> i32 {
        self.num_docs
    }
}
