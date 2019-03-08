#![recursion_limit = "1024"]
#![cfg_attr(feature = "clippy", feature(plugin))]
#![cfg_attr(feature = "clippy", plugin(clippy))]
#![cfg_attr(not(feature = "clippy"), allow(unknown_lints))]
//#![feature(const_max_value, option_filter, exact_size_is_empty)]
#![feature(option_filter, exact_size_is_empty)]
#![feature(drain_filter)]
#![feature(duration_extras)]
#![feature(hash_map_remove_entry)]
#![feature(hashmap_internals)]
//#![feature(vec_remove_item)]
#![feature(fnbox)]
#![feature(integer_atomics)]
#![feature(vec_remove_item)]
//#![feature(io)]
//#![feature(repr_transparent)]

#[macro_use]
extern crate error_chain;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate crossbeam;
extern crate serde_json;

extern crate byteorder;
extern crate bytes;
extern crate crc;
extern crate fasthash;
extern crate flate2;
extern crate memmap;
extern crate num_traits;
extern crate rand;
extern crate regex;
extern crate thread_local;
extern crate unicode_reader;

pub mod core;
pub mod error;

use core::analysis::TokenStream;
use core::codec::codec_for_name;
use core::codec::Lucene62Codec;
use core::doc::Field;
use core::doc::FieldType;
use core::doc::NumericDocValuesField;
use core::doc::Word;
use core::doc::WordTokenStream;
use core::doc::{BINARY_DOC_VALUES_FIELD_TYPE, NUMERIC_DOC_VALUES_FIELD_TYPE,
                SORTED_DOC_VALUES_FIELD_TYPE, SORTED_SET_DOC_VALUES_FIELD_TYPE};
use core::index::index_writer_config::{IndexWriterConfig, OpenMode};
use core::index::merge_policy::TieredMergePolicy;
use core::index::merge_policy::{MergePolicy, MergeSpecification, MergerTrigger};
use core::index::DocValuesType;
use core::index::Fieldable;
use core::index::IndexOptions;
use core::index::IndexWriter;
use core::index::Term;
use core::index::{SegmentCommitInfo, SegmentInfo, SegmentInfos};
use core::search::sort::Sort;
use core::search::sort_field::{SimpleSortField, SortField, SortFieldType};
use core::store::MmapDirectory;
use core::store::{Directory, IOContext, IndexInput, IndexOutput};
use core::store::{DirectoryRc, TrackingDirectoryWrapper};
use core::store::{LockFactory, NativeFSLockFactory};
use core::util::{Counter, VariantValue, Version};

use error::Result;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread;

struct ConcurrentMergeScheduler {}

impl MergePolicy for ConcurrentMergeScheduler {
    fn max_cfs_segment_size(&self) -> u64 {
        unimplemented!()
    }

    fn no_cfs_ratio(&self) -> f64 {
        unimplemented!()
    }

    //
    fn find_merges(
        &self,
        merge_trigger: MergerTrigger,
        segment_infos: &SegmentInfos,
        writer: &IndexWriter,
    ) -> Result<Option<MergeSpecification>> {
        unimplemented!()
    }

    fn find_forced_merges(
        &self,
        segment_infos: &SegmentInfos,
        max_segment_count: u32,
        segments_to_merge: &HashMap<Arc<SegmentCommitInfo>, bool>,
        writer: &IndexWriter,
    ) -> Result<Option<MergeSpecification>> {
        unimplemented!()
    }

    fn find_forced_deletes_mergers(
        &self,
        segments_infos: &SegmentInfos,
        writer: &IndexWriter,
    ) -> Result<Option<MergeSpecification>> {
        unimplemented!()
    }
}

fn mock_doc_stored() -> Vec<Box<Fieldable>> {
    let mut fields: Vec<Box<Fieldable>> = vec![];
    let field_type_stored = FieldType::new(
        true,
        false,
        false,
        false,
        false,
        false,
        false,
        IndexOptions::Null,
        DocValuesType::Null,
        0,
        0,
    );

    fields.push(Box::new(Field::new(
        "title.raw".into(),
        field_type_stored.clone(),
        Some(VariantValue::VString(
            "《三体·死神永生》开篇为什么描写那个想当圣女的妓女？"
                .to_string(),
        )),
        None,
    )));

    fields.push(Box::new(Field::new(
        "content.raw".into(),
        field_type_stored.clone(),
        Some(VariantValue::VString(
            "第一章《魔法师之死》，是对《死神永生》整本书故事的隐喻"
                .to_string(),
        )),
        None,
    )));

    fields
}

fn mock_doc_token() -> Vec<Box<Fieldable>> {
    let mut fields: Vec<Box<Fieldable>> = vec![];
    let field_type_index = FieldType::new(
        false,
        true,
        true,
        true,
        true,
        false,
        true,
        IndexOptions::DocsAndFreqsAndPositionsAndOffsets,
        DocValuesType::Null,
        0,
        0,
    );

    let field_type_stored = FieldType::new(
        true,
        false,
        false,
        false,
        false,
        false,
        false,
        IndexOptions::Null,
        DocValuesType::Null,
        0,
        0,
    );

    fields.push(Box::new(Field::new(
        "id".into(),
        field_type_index.clone(),
        Some(VariantValue::VString("11".to_string())),
        Some(Box::new(WordTokenStream::new(vec![Word::new("11", 0, 2)]))),
    )));

    fields.push(Box::new(Field::new(
        "title".into(),
        field_type_index.clone(),
        Some(VariantValue::VString(
            "《三体·死神永生》开篇为什么描写那个想当圣女的妓女？"
                .to_string(),
        )),
        Some(Box::new(WordTokenStream::new(vec![
            // Word::new("《", 0, 1),
            Word::new("三体", 1, 2),
            // Word::new("·", 3, 1),
            Word::new("死神永生", 4, 4),
            // Word::new("》", 8, 1),
            Word::new("开篇", 9, 2),
            Word::new("为什么", 11, 3),
            Word::new("描写", 14, 2),
            Word::new("那个", 16, 2),
            Word::new("想当", 18, 2),
            Word::new("圣女", 20, 2),
            Word::new("的", 22, 1),
            Word::new("妓女", 23, 2),
            // Word::new("？", 25, 1),
        ]))),
    )));

    fields.push(Box::new(Field::new(
        "content".into(),
        field_type_index.clone(),
        Some(VariantValue::VString(
            "第一章《魔法师之死》，是对《死神永生》整本书故事的隐喻"
                .to_string(),
        )),
        Some(Box::new(WordTokenStream::new(vec![
            Word::new("第", 0, 1),
            Word::new("一", 1, 1),
            Word::new("章", 2, 1),
            // Word::new("《", 3, 1),
            Word::new("魔法师", 4, 3),
            Word::new("之", 7, 1),
            Word::new("死", 8, 1),
            // Word::new("》", 9, 1),
            // Word::new("，", 10, 1),
            Word::new("是", 11, 1),
            Word::new("对", 12, 1),
            // Word::new("《", 13, 1),
            Word::new("死神永生", 14, 4),
            // Word::new("》", 18, 1),
            Word::new("整本", 19, 2),
            Word::new("书", 21, 1),
            Word::new("故事", 22, 2),
            Word::new("的", 24, 1),
            Word::new("隐喻", 25, 2),
        ]))),
    )));

    fields.push(Box::new(Field::new(
        "up_vote".into(),
        NUMERIC_DOC_VALUES_FIELD_TYPE.clone(),
        Some(VariantValue::Int(11)),
        None,
    )));

    fields.push(Box::new(Field::new(
        "wilson_score".into(),
        NUMERIC_DOC_VALUES_FIELD_TYPE.clone(),
        Some(VariantValue::Double(1111.95)),
        None,
    )));

    fields.push(Box::new(Field::new(
        "speaker_name".into(),
        SORTED_SET_DOC_VALUES_FIELD_TYPE.clone(),
        Some(VariantValue::Binary("1".to_string().into())),
        None,
    )));

    fields.push(Box::new(Field::new(
        "speaker_name".into(),
        SORTED_SET_DOC_VALUES_FIELD_TYPE.clone(),
        Some(VariantValue::Binary("2".to_string().into())),
        None,
    )));

    fields.push(Box::new(Field::new(
        "title.raw".into(),
        field_type_stored.clone(),
        Some(VariantValue::VString(
            "《三体·死神永生》开篇为什么描写那个想当圣女的妓女？"
                .to_string(),
        )),
        None,
    )));

    fields.push(Box::new(Field::new(
        "content.raw".into(),
        field_type_stored.clone(),
        Some(VariantValue::VString(
            "第一章《魔法师之死》，是对《死神永生》整本书故事的隐喻"
                .to_string(),
        )),
        None,
    )));

    fields
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct JWord {
    begin: i32,
    length: i32,
    value: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SegmentedField {
    raw: String,
    words: Vec<JWord>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ContentDoc {
    pub author_name: SegmentedField,
    pub author_name_pinyin: SegmentedField,
    pub author_name_prefix: SegmentedField,
    pub comment_count: i32,
    pub content: SegmentedField,
    pub content_length: i32,
    pub content_type: i32,
    pub created_time: i64,
    pub data_version: String,
    pub doc_id: String,
    pub original_id: String,
    pub segmenter_version: String,
    pub status: i32,
    pub title: SegmentedField,
    pub title_length: i32,
    pub unique_id: String,
    pub updated_time: i64,
    pub upvote_count: i32,
    pub url: String,
    pub url_token: String,
    pub wilson_score: f64,

    pub is_pu: Option<i32>,
    pub downvote_count: Option<i32>,
    pub follower_count: Option<i32>,
    pub is_pu_goodat_topic: Option<i32>,
    pub q_answer_count: Option<i32>,
    pub q_url_token: Option<String>,
    pub question_id: Option<i32>,
    pub member_id: Option<i32>,
    pub thank_count: Option<i32>,
    pub topic_ids: Option<Vec<String>>,
    pub topic_url_tokens: Option<Vec<String>>,
    pub qanchor: Option<SegmentedField>,
    pub collect_count: Option<i32>,
}

fn to_word_token(words: Vec<JWord>) -> Box<TokenStream> {
    let ws = words
        .iter()
        .map(|w| Word::new(w.value.as_str(), w.begin as usize, w.length as usize))
        .collect();
    Box::new(WordTokenStream::new(ws))
}

fn from_json(doc: ContentDoc) -> Vec<Box<Fieldable>> {
    let field_type_index = FieldType::new(
        false,
        true,
        true,
        true,
        true,
        false,
        true,
        IndexOptions::DocsAndFreqsAndPositionsAndOffsets,
        DocValuesType::Null,
        0,
        0,
    );

    let field_type_stored = FieldType::new(
        true,
        false,
        false,
        false,
        false,
        false,
        true,
        IndexOptions::Null,
        DocValuesType::Null,
        0,
        0,
    );
    let mut fields: Vec<Box<Fieldable>> = vec![];

    if true {
        // index fields
        fields.push(Box::new(Field::new(
            "author_name".into(),
            field_type_index.clone(),
            None,
            Some(to_word_token(doc.author_name.words)),
        )));
        fields.push(Box::new(Field::new(
            "author_name_pinyin".into(),
            field_type_index.clone(),
            None,
            Some(to_word_token(doc.author_name_pinyin.words)),
        )));
        fields.push(Box::new(Field::new(
            "author_name_prefix".into(),
            field_type_index.clone(),
            None,
            Some(to_word_token(doc.author_name_prefix.words)),
        )));
        fields.push(Box::new(Field::new(
            "content".into(),
            field_type_index.clone(),
            None,
            Some(to_word_token(doc.content.words)),
        )));
        fields.push(Box::new(Field::new(
            "title".into(),
            field_type_index.clone(),
            None,
            Some(to_word_token(doc.title.words)),
        )));
        if let Some(qanchor) = doc.qanchor {
            fields.push(Box::new(Field::new(
                "qanchor".into(),
                field_type_index.clone(),
                Some(VariantValue::VString("".to_string())),
                Some(to_word_token(qanchor.words)),
            )));
        }
    }

    if true {
        // doc_value fields
        fields.push(Box::new(Field::new(
            "data_version".into(),
            BINARY_DOC_VALUES_FIELD_TYPE,
            Some(VariantValue::Binary(doc.data_version.into_bytes())),
            None,
        )));
        fields.push(Box::new(Field::new(
            "doc_id".into(),
            BINARY_DOC_VALUES_FIELD_TYPE,
            Some(VariantValue::Binary(doc.doc_id.clone().into_bytes())),
            None,
        )));
        fields.push(Box::new(Field::new(
            "doc_id_index".into(),
            field_type_index.clone(),
            Some(VariantValue::VString(doc.doc_id.clone())),
            Some(to_word_token(vec![JWord {
                begin: 0,
                length: doc.doc_id.len() as i32,
                value: doc.doc_id.clone(),
            }])),
        )));
        fields.push(Box::new(Field::new(
            "segmenter_version".into(),
            BINARY_DOC_VALUES_FIELD_TYPE,
            Some(VariantValue::Binary(doc.segmenter_version.into_bytes())),
            None,
        )));
        fields.push(Box::new(Field::new(
            "original_id".into(),
            BINARY_DOC_VALUES_FIELD_TYPE,
            Some(VariantValue::Binary(doc.original_id.clone().into_bytes())),
            None,
        )));
        fields.push(Box::new(Field::new(
            "original_id_index".into(),
            field_type_index.clone(),
            Some(VariantValue::VString("".to_string())),
            Some(to_word_token(vec![JWord {
                begin: 0,
                length: doc.original_id.len() as i32,
                value: doc.original_id.clone(),
            }])),
        )));
        fields.push(Box::new(Field::new(
            "url".into(),
            BINARY_DOC_VALUES_FIELD_TYPE,
            Some(VariantValue::Binary(doc.url.into_bytes())),
            None,
        )));
        fields.push(Box::new(Field::new(
            "url_token".into(),
            BINARY_DOC_VALUES_FIELD_TYPE,
            Some(VariantValue::Binary(doc.url_token.clone().into_bytes())),
            None,
        )));
        fields.push(Box::new(Field::new(
            "url_token_index".into(),
            field_type_index.clone(),
            Some(VariantValue::VString("".to_string())),
            Some(to_word_token(vec![JWord {
                begin: 0,
                length: doc.url_token.len() as i32,
                value: doc.url_token.clone(),
            }])),
        )));
        fields.push(Box::new(Field::new(
            "unique_id".into(),
            BINARY_DOC_VALUES_FIELD_TYPE,
            Some(VariantValue::Binary(doc.unique_id.into_bytes())),
            None,
        )));
    }

    if true {
        fields.push(Box::new(Field::new(
            "upvote_count".into(),
            NUMERIC_DOC_VALUES_FIELD_TYPE.clone(),
            Some(VariantValue::Int(doc.upvote_count)),
            None,
        )));
        fields.push(Box::new(Field::new(
            "comment_count".into(),
            NUMERIC_DOC_VALUES_FIELD_TYPE.clone(),
            Some(VariantValue::Int(doc.comment_count)),
            None,
        )));
        fields.push(Box::new(Field::new(
            "content_length".into(),
            NUMERIC_DOC_VALUES_FIELD_TYPE.clone(),
            Some(VariantValue::Int(doc.content_length)),
            None,
        )));
        fields.push(Box::new(Field::new(
            "content_type".into(),
            NUMERIC_DOC_VALUES_FIELD_TYPE.clone(),
            Some(VariantValue::Int(doc.content_type)),
            None,
        )));
        fields.push(Box::new(Field::new(
            "created_time".into(),
            NUMERIC_DOC_VALUES_FIELD_TYPE.clone(),
            Some(VariantValue::Long(doc.created_time)),
            None,
        )));
        fields.push(Box::new(Field::new(
            "status".into(),
            NUMERIC_DOC_VALUES_FIELD_TYPE.clone(),
            Some(VariantValue::Int(doc.status)),
            None,
        )));
        fields.push(Box::new(Field::new(
            "title_length".into(),
            NUMERIC_DOC_VALUES_FIELD_TYPE.clone(),
            Some(VariantValue::Int(doc.title_length)),
            None,
        )));
        fields.push(Box::new(Field::new(
            "updated_time".into(),
            NUMERIC_DOC_VALUES_FIELD_TYPE.clone(),
            Some(VariantValue::Long(doc.updated_time)),
            None,
        )));
        fields.push(Box::new(Field::new(
            "wilson_score".into(),
            NUMERIC_DOC_VALUES_FIELD_TYPE.clone(),
            Some(VariantValue::Double(doc.wilson_score)),
            None,
        )));
    }

    if true {
        if let Some(downvote_count) = doc.downvote_count {
            fields.push(Box::new(Field::new(
                "downvote_count".into(),
                NUMERIC_DOC_VALUES_FIELD_TYPE.clone(),
                Some(VariantValue::Int(downvote_count)),
                None,
            )));
        }
        if let Some(follower_count) = doc.follower_count {
            fields.push(Box::new(Field::new(
                "follower_count".into(),
                NUMERIC_DOC_VALUES_FIELD_TYPE.clone(),
                Some(VariantValue::Int(follower_count)),
                None,
            )));
        }
        if let Some(is_pu) = doc.is_pu {
            fields.push(Box::new(Field::new(
                "is_pu".into(),
                NUMERIC_DOC_VALUES_FIELD_TYPE.clone(),
                Some(VariantValue::Int(is_pu)),
                None,
            )));
        }
        if let Some(is_pu_goodat_topic) = doc.is_pu_goodat_topic {
            fields.push(Box::new(Field::new(
                "is_pu_goodat_topic".into(),
                NUMERIC_DOC_VALUES_FIELD_TYPE.clone(),
                Some(VariantValue::Int(is_pu_goodat_topic)),
                None,
            )));
        }
        if let Some(member_id) = doc.member_id {
            fields.push(Box::new(Field::new(
                "member_id".into(),
                NUMERIC_DOC_VALUES_FIELD_TYPE.clone(),
                Some(VariantValue::Int(member_id)),
                None,
            )));
        }
        if let Some(q_answer_count) = doc.q_answer_count {
            fields.push(Box::new(Field::new(
                "q_answer_count".into(),
                NUMERIC_DOC_VALUES_FIELD_TYPE.clone(),
                Some(VariantValue::Int(q_answer_count)),
                None,
            )));
        }
        if let Some(question_id) = doc.question_id {
            fields.push(Box::new(Field::new(
                "question_id".into(),
                NUMERIC_DOC_VALUES_FIELD_TYPE.clone(),
                Some(VariantValue::Int(question_id)),
                None,
            )));
        }
        if let Some(thank_count) = doc.thank_count {
            fields.push(Box::new(Field::new(
                "thank_count".into(),
                NUMERIC_DOC_VALUES_FIELD_TYPE.clone(),
                Some(VariantValue::Int(thank_count)),
                None,
            )));
        }
        if let Some(q_url_token) = doc.q_url_token {
            fields.push(Box::new(Field::new(
                "q_url_token".into(),
                BINARY_DOC_VALUES_FIELD_TYPE,
                Some(VariantValue::Binary(q_url_token.into_bytes())),
                None,
            )));
        }
        if let Some(collect_count) = doc.collect_count {
            fields.push(Box::new(Field::new(
                "collect_count".into(),
                NUMERIC_DOC_VALUES_FIELD_TYPE.clone(),
                Some(VariantValue::Int(collect_count)),
                None,
            )));
        }
    }

    if true {
        if let Some(topic_ids) = doc.topic_ids {
            for topic_id in topic_ids {
                fields.push(Box::new(Field::new(
                    "topic_ids".into(),
                    SORTED_SET_DOC_VALUES_FIELD_TYPE.clone(),
                    Some(VariantValue::Binary(topic_id.into())),
                    None,
                )));
            }
        }
        if let Some(topic_url_tokens) = doc.topic_url_tokens {
            for topic_url_token in topic_url_tokens {
                fields.push(Box::new(Field::new(
                    "topic_url_tokens".into(),
                    SORTED_SET_DOC_VALUES_FIELD_TYPE.clone(),
                    Some(VariantValue::Binary(topic_url_token.into())),
                    None,
                )));
            }
        }
        if doc.title.raw.len() > 0 {
            fields.push(Box::new(Field::new(
                "title_raw".into(),
                BINARY_DOC_VALUES_FIELD_TYPE,
                Some(VariantValue::Binary(doc.title.raw.into_bytes())),
                None,
            )));
        }
    }

    fields
}

fn from_file<P: AsRef<Path>>(path: P) -> Vec<Vec<Box<Fieldable>>> {
    let file = File::open(path).unwrap();
    let buf = io::BufReader::new(file);
    let result: Vec<_> = buf.lines()
        .map(|l| {
            let line = l.unwrap();
            let r: ContentDoc = serde_json::from_str(&line).unwrap();
            from_json(r)
        })
        .collect();
    result
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let mut max_buffered_docs = if args.len() > 1 {
        args[1].parse::<u32>().unwrap()
    } else {
        1024
    };

    if max_buffered_docs < 2 {
        max_buffered_docs = 2;
    }

    let parallel_num = if args.len() > 2 {
        args[2].parse::<usize>().unwrap()
    } else {
        10
    };

    let p = Path::new("/tmp/test_index");
    let lock = Box::new(NativeFSLockFactory::default());
    let directory = Arc::new(MmapDirectory::new(&p, lock, 1024 * 1024).unwrap());
    let sort_fields: Vec<SortField> = vec![SortField::Simple(SimpleSortField::new(
        String::from("wilson_score"),
        SortFieldType::Double,
        true,
    ))];
    let sort = Sort::new(sort_fields);
    let index_conf = IndexWriterConfig {
        ram_buffer_size_mb: Some(4096.0),
        use_compound_file: false,
        max_buffered_delete_terms: Some(4096),
        max_buffered_docs: Some(max_buffered_docs),
        merge_policy: Box::new(TieredMergePolicy::default()),
        index_sort: Some(sort),
        reader_pooling: false,
        open_mode: OpenMode::CreateOrAppend,
        per_thread_hard_limit_mb: 4096,
        codec: Arc::from(codec_for_name("Lucene62").unwrap()),
        commit_on_close: true,
    };
    let index_conf = Arc::new(index_conf);
    let index_writer: Arc<IndexWriter> = Arc::new(IndexWriter::new(directory, index_conf).unwrap());
    let writer = index_writer.as_ref() as *const IndexWriter as *mut IndexWriter;

    unsafe { (*writer).init() };

    let file = File::open("./docs").unwrap();
    let buf = io::BufReader::new(file);
    let mut lines: Vec<Vec<String>> = vec![];
    for _ in 0..parallel_num {
        lines.push(vec![]);
    }

    let mut i = 0;
    for line in buf.lines() {
        let line = line.unwrap();
        lines[i % parallel_num].push(line);
        i += 1;
    }

    let mut thread_ids = vec![];
    for i in 0..parallel_num {
        let writer = Arc::clone(&index_writer);
        let raw_lines = lines.pop().unwrap();
        let thread_id = thread::spawn(move || -> Result<()> {
            let mut curr = 0;
            let mut terms = vec![];
            for line in raw_lines {
                let r: ContentDoc = serde_json::from_str(&line).unwrap();
                let doc_id = r.doc_id.clone();
                let term = Term::new("doc_id_index".to_string(), doc_id.clone().into_bytes());
                terms.push(term);

                let doc = from_json(r);

                if i < parallel_num {
                    let w = writer.as_ref() as *const IndexWriter as *mut IndexWriter;
                    unsafe {
                        (*w).add_document(doc).unwrap();
                    }
                    println!("debug add_document {} {} {:?}", i, curr, doc_id);

                    unsafe {
                        let tick = ::std::time::SystemTime::now()
                            .duration_since(::std::time::UNIX_EPOCH)?
                            .subsec_nanos() as usize;
                        // let to_delete = &terms[tick % terms.len()];
                        let to_delete = terms.pop().unwrap();
                        let deleted = (*w).delete_documents_by_terms(vec![to_delete.clone()]);
                        println!(
                            "debug delete_document {} {} {} {:?}",
                            i,
                            curr,
                            String::from_utf8(to_delete.bytes.clone()).unwrap(),
                            deleted
                        );
                    }
                }

                curr += 1;
            }

            Ok(())
        });

        thread_ids.push(thread_id);
    }

    for _ in 0..parallel_num {
        let id = thread_ids.pop().unwrap();
        let _ = id.join().unwrap();
    }

    unsafe {
        println!("debug to close");
        let _ = (*writer).close().unwrap();
    }

    thread::sleep_ms(1_000);
    show_docs()
}

use core::index::{IndexReader, StandardDirectoryReader};
use core::search::collector::TopDocsCollector;
use core::search::match_all::MatchAllDocsQuery;
use core::search::searcher::{DefaultIndexSearcher, IndexSearcher};
use core::store::FSDirectory;

fn show_docs() -> Result<()> {
    let dir = FSDirectory::new("/tmp/test_index", Box::new(NativeFSLockFactory::default()))?;
    let reader: Arc<IndexReader> = Arc::new(StandardDirectoryReader::open(Arc::new(dir))?);
    let searcher = DefaultIndexSearcher::new(reader);

    let query = MatchAllDocsQuery {};

    let mut collector = TopDocsCollector::new(100);
    searcher.search(&query, &mut collector)?;

    let docs = collector.top_docs();
    println!("\n ===== totoal: {}", docs.total_hits());
    for doc in docs.score_docs() {
        let id = doc.doc_id();
        let leaf = searcher.reader().leaf_reader_for_doc(id);
        let binary_doc_values = leaf.get_binary_doc_values("doc_id")?;
        let res = binary_doc_values.get(id - leaf.doc_base())?;
        println!(" --- doc id: {}, raw id: {}", id, String::from_utf8(res)?);
    }

    Ok(())
}

fn main1() -> Result<()> {
    let p = Path::new("/tmp/test_index");
    let lock = Box::new(NativeFSLockFactory::default());
    let directory = Arc::new(MmapDirectory::new(&p, lock, 1024 * 1024).unwrap());
    let sort_fields: Vec<SortField> = vec![SortField::Simple(SimpleSortField::new(
        String::from("wilson_score"),
        SortFieldType::Double,
        false,
    ))];
    let sort = Sort::new(sort_fields);
    let index_conf = IndexWriterConfig {
        ram_buffer_size_mb: Some(1024.0),
        use_compound_file: false,
        max_buffered_delete_terms: Some(1024),
        max_buffered_docs: Some(1024),
        merge_policy: Box::new(TieredMergePolicy::default()),
        index_sort: Some(sort),
        reader_pooling: false,
        open_mode: OpenMode::Append,
        per_thread_hard_limit_mb: 2048,
        codec: Arc::from(codec_for_name("Lucene62").unwrap()),
        commit_on_close: true,
    };
    let index_conf = Arc::new(index_conf);
    let mut index_writer: IndexWriter = IndexWriter::new(directory, index_conf).unwrap();
    index_writer.init();

    index_writer.force_merge(1, true)?;
    index_writer.close()?;

    Ok(())
}
