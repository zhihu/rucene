extern crate rucene;

use rucene::core::analysis::WhitespaceTokenizer;
use rucene::core::codec::doc_values::lucene54::DocValues;
use rucene::core::codec::Lucene62Codec;
use rucene::core::doc::{
    DocValuesType, Field, FieldType, Fieldable, IndexOptions, NumericDocValuesField, Term,
};
use rucene::core::index::reader::IndexReader;
use rucene::core::index::writer::{IndexWriter, IndexWriterConfig};
use rucene::core::search::collector::TopDocsCollector;
use rucene::core::search::query::{self, IntPoint, LongPoint, PointRangeQuery, Query, TermQuery};
use rucene::core::search::{DefaultIndexSearcher, IndexSearcher};
use rucene::core::store::directory::FSDirectory;

use std::borrow::{Borrow, BorrowMut};
use std::fs::{self, File};
use std::io::{self, BufRead};
use std::iter::Sum;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use std::{cmp, u128};

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
    Field::new_bytes(
        field_name,
        LongPoint::pack(&[data]),
        indexed_numeric_field_type(),
    )
}

fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
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

    let mut queries = vec![];

    const BASE_TIMESTAMP: i64 = 1672531200000;

    let mut sum: u128 = 0;
    let min: i64 = i64::MAX;
    let max: i64 = 0;

    if let Ok(mut lines) = read_lines("../range_datapoints") {
        let numDocs: &i32 = &lines.next().unwrap().unwrap().parse().unwrap();
        // Consumes the iterator, returns an (Optional) String

        for n in 0..*numDocs {
            let timestamp: &i64 = &lines.next().unwrap().unwrap().parse().unwrap();
            let numeric_field = new_index_numeric_field("timestamp".into(), *timestamp);
            let mut doc: Vec<Box<dyn Fieldable>> = vec![];
            doc.push(Box::new(numeric_field));

            writer.add_document(doc)?;

            if (n > 0 && n % 1000000 == 0) {
                writer.commit()?;
            }
        }
        let numQueries: &i32 = &lines.next().unwrap().unwrap().parse().unwrap();

        for n in 0..*numQueries {
            let l = lines.next().unwrap().unwrap();

            let mut range = l.split(',');

            let lower = range.next().unwrap();

            let lowerBound: i64 = lower.parse::<i64>().unwrap();

            // println!("lower: {}", lowerBound);

            let upper = range.next().unwrap();

            let upperBound: i64 = upper.parse::<i64>().unwrap();

            // println!("upper: {}", upperBound);

            queries.push(LongPoint::new_range_query(
                "timestamp".into(),
                lowerBound,
                upperBound,
            ));
        }

        let warmupCount = cmp::min(10000, queries.len());

        // for i in 0..warmupCount {
        //     let query = queries.get(i).unwrap().as_ref().unwrap();
        //     index_searcher.search(&**query, &mut collector);
        // }

        let mut hits: u64 = 0;

        let reader = writer.get_reader(true, false)?;
        let index_searcher = DefaultIndexSearcher::new(Arc::new(reader), None);
        let overall_start = Instant::now();
        for iter in queries.iter() {
            let mut collector = TopDocsCollector::new(10);
            let query = iter.as_ref().unwrap();
            let start_time: Instant = Instant::now();
            index_searcher.search(&**query, &mut collector);
            let time: Duration = Instant::now().duration_since(start_time);
            hits += collector.top_docs().total_hits() as u64;
            sum += time.as_nanos();
        }
        println!("Total hits: {}", hits);
        println!(
            "Searching time: {}",
            Instant::now().duration_since(overall_start).as_secs_f64()
        );
        println!("Queries len: {}", queries.len());
        println!("Avg. time: {}", sum / (queries.len() as u128));
    }

    Ok(())
}
