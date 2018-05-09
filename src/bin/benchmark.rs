extern crate rucene;

use std::env;
use std::fs::File;
use std::io::*;
use std::path::Path;
use std::sync::Arc;
use std::time;
use std::usize;

use rucene::core::index::StandardDirectoryReader;
use rucene::core::search::collector::{EarlyTerminatingSortingCollector, TopDocsCollector};
use rucene::core::search::query_string::*;
use rucene::core::search::searcher::*;
use rucene::core::store::MmapDirectory;

type QueryResult = (String, usize, time::Duration);

fn benchmark_queries(
    searcher: &IndexSearcher,
    field: &str,
    queries: &[String],
    limit: usize,
) -> Vec<QueryResult> {
    let mut results = Vec::new();
    for query in queries {
        let start = time::Instant::now();
        let mut top_collector = TopDocsCollector::new(100);
        let real_query =
            QueryStringQueryBuilder::new(query.clone(), vec![(field.into(), 1.0)], 1, 1.0)
                .build()
                .unwrap();
        if limit != usize::MAX {
            let mut early_collector =
                EarlyTerminatingSortingCollector::new(&mut top_collector, limit);
            searcher
                .search(real_query.as_ref(), &mut early_collector)
                .unwrap();
        } else {
            searcher
                .search(real_query.as_ref(), &mut top_collector)
                .unwrap();
        }
        results.push((
            query.clone(),
            top_collector.top_docs().score_docs().len(),
            start.elapsed(),
        ));
    }

    results.sort_unstable_by_key(|r| r.2);

    return results;
}

fn format_elapsed(duration: &time::Duration) -> String {
    let secs = duration.as_secs();
    let nanos = duration.subsec_nanos();
    let ms = nanos / 1000000;
    let us = (nanos % 1000000) / 1000;
    let ns = nanos % 1000;
    if secs > 0 {
        format!("{}.{} secs", secs, ms)
    } else if ms > 0 {
        format!("{}.{} ms", ms, us)
    } else {
        format!("{}.{} us", us, ns)
    }
}

fn format_result(result: &QueryResult) -> String {
    format!(
        "'{}' took {} returned {} docs",
        result.0,
        format_elapsed(&result.2),
        result.1
    )
}

fn benchmark_with_limit(searcher: &IndexSearcher, field: &str, queries: &[String], limit: usize) {
    if limit == usize::MAX {
        println!("======== Benchmark without early terminating ========");
    } else {
        println!(
            "======== Benchmark with early terminating of {} ========",
            limit
        );
    }
    let start = time::Instant::now();
    let results = benchmark_queries(searcher, field, queries, limit);
    let len = results.len();
    println!(
        "{} queries executed in {}, {} per query on average",
        queries.len(),
        format_elapsed(&start.elapsed()),
        format_elapsed(&(start.elapsed() / queries.len() as u32))
    );
    println!("p50: {}", format_result(&results[len / 2]));
    println!("p75: {}", format_result(&results[len * 75 / 100]));
    println!("p90: {}", format_result(&results[len * 90 / 100]));
    println!("p95: {}", format_result(&results[len * 95 / 100]));
    println!("p99: {}", format_result(&results[len * 99 / 100]));
    println!("p999: {}", format_result(&results[len * 999 / 1000]));
    println!("max: {}", format_result(&results[len - 1]));
}

fn benchmark(index: String, field: String, input: String) {
    let directory = Arc::new(MmapDirectory::new(&Path::new(&index), 1024 * 1024).unwrap());
    let queries: Vec<String> = File::open(&input)
        .map(BufReader::new)
        .map(BufReader::lines)
        .map(|i| i.filter_map(Result::ok))
        .map(Iterator::collect)
        .expect("No query to run");

    let reader = Arc::new(StandardDirectoryReader::open(directory).unwrap());
    let searcher = Arc::new(IndexSearcher::new(reader.clone()));
    benchmark_with_limit(searcher.as_ref(), &field, &queries, 100);
    benchmark_with_limit(searcher.as_ref(), &field, &queries, 50);
    benchmark_with_limit(searcher.as_ref(), &field, &queries, 100);
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 4 {
        panic!("Missing required arguments");
    }
    benchmark(args[1].clone(), args[2].clone(), args[3].clone());
}
