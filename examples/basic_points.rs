extern crate rucene;

use rucene::core::analysis::WhitespaceTokenizer;
use rucene::core::codec::doc_values::lucene54::DocValues;
use rucene::core::codec::Lucene62Codec;
use rucene::core::doc::{DocValuesType, Field, FieldType, Fieldable, IndexOptions, NumericDocValuesField, Term};
use rucene::core::index::reader::IndexReader;
use rucene::core::index::writer::{IndexWriter, IndexWriterConfig};
use rucene::core::search::collector::TopDocsCollector;
use rucene::core::search::query::{IntPoint, LongPoint, PointRangeQuery, Query, TermQuery};
use rucene::core::search::{DefaultIndexSearcher, IndexSearcher};
use rucene::core::store::directory::FSDirectory;

use std::fs;
use std::io;
use std::path::Path;
use std::sync::Arc;

use rucene::core::highlight::FastVectorHighlighter;
use rucene::core::highlight::FieldQuery;
use rucene::core::util::VariantValue;
use rucene::error::Result;

fn indexed_numeric_field_type() -> FieldType {
    let mut field_type = FieldType::default();
    field_type.tokenized = false;
    field_type.doc_values_type = DocValuesType::Binary;
    field_type.dimension_count = 1;
    field_type.dimension_num_bytes = 8;
    field_type
}

fn new_index_numeric_field(field_name: String, data: i64) -> Field {
    Field::new_bytes(field_name, LongPoint::pack(&[data]), indexed_numeric_field_type())
}
fn main() -> Result<()> {
    // create index directory
    let path = "/tmp/test_rucene";
    let dir_path = Path::new(path);
    if dir_path.exists() {
        fs::remove_dir_all(&dir_path)?;
        fs::create_dir(&dir_path)?;
    }

    // create index writer
    let config = Arc::new(IndexWriterConfig::default());
    let directory = Arc::new(FSDirectory::with_path(&dir_path)?);
    let writer = IndexWriter::new(directory, config)?;

    let mut doc: Vec<Box<dyn Fieldable>> = vec![];

    let timestamp: i64 = 1707782905540;

    let numeric_field = new_index_numeric_field("timestamp".into(), timestamp);

    doc.push(Box::new(numeric_field));

    writer.add_document(doc)?;

    // flush to disk
    writer.commit()?;

    // new index search
    let reader = writer.get_reader(true, false)?;
    let index_searcher = DefaultIndexSearcher::new(Arc::new(reader), None);

    // search
    let query= LongPoint::new_range_query(
        "timestamp".into(),
        1707782905539,
        1707782905541,
    )?;


    let mut collector: TopDocsCollector = TopDocsCollector::new(10);
    index_searcher.search(&*query, &mut collector)?;

    let top_docs = collector.top_docs();
    println!("total hits: {}", top_docs.total_hits());
    for d in top_docs.score_docs() {
        let doc_id = d.doc_id();
        println!("  doc: {}", doc_id);
        // fetch stored fields
        let stored_fields = vec!["timestamp".into()];
        let stored_doc = index_searcher.reader().document(doc_id, &stored_fields)?;
        if stored_doc.fields.len() > 0 {
            println!("    stroed fields: ");
            for s in &stored_doc.fields {
                println!(
                    "      field: {}, value: {}",
                    s.field.name(),
                    s.field.field_data().unwrap()
                );
            }
        }
    }

    Ok(())
}
