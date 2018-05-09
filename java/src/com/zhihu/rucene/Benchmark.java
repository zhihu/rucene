package com.zhihu.rucene;

import java.nio.file.Paths;
import java.nio.file.Files;
import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.nio.charset.StandardCharsets;
import java.nio.charset.*;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.*;
import org.apache.lucene.util.*;

public class Benchmark {
  public static class BenchmarkResult {
    long count;
    String query;
    long elapsed;

    BenchmarkResult(long count, long elapsed, String query) {
      this.elapsed = elapsed;
      this.count = count;
      this.query = query;
    }
  }

  public static void run(IndexSearcher searcher, String field, String queriesInput, int limit) throws IOException {
    Analyzer analyzer = new WhitespaceAnalyzer();
    BufferedReader queries = Files.newBufferedReader(Paths.get(queriesInput), StandardCharsets.UTF_8);
    QueryParser parser = new QueryParser(field, analyzer);

    /* execute queries */
    String query;
    ArrayList<BenchmarkResult> results = new ArrayList();
    long start = System.currentTimeMillis();
    while ((query = queries.readLine()) != null) {
      query = query.trim();
      if (!query.isEmpty()) {
        try {
          query = query.trim();
          Query realQuery = parser.parse(query);
          long queryStart = System.currentTimeMillis();
          TopDocs docs = searcher.search(realQuery, limit);
          results.add(new BenchmarkResult(docs.scoreDocs.length, System.currentTimeMillis() - queryStart, query));
        } catch (Exception ignored) {
        }
      }
    }
    long elapsed = System.currentTimeMillis() - start;
    Collections.sort(results, (o1, o2) -> {
      if (o1.elapsed < o2.elapsed) {
        return -1;
      } else if (o1.elapsed > o2.elapsed) {
        return 1;
      } else {
        return 0;
      }
    });

    int len = results.size();
    System.out.println("======== Benchmark with early terminating of " + limit + "========");
    System.out.println(results.size() + " queries executed in " + elapsed + " ms, " + (elapsed / len) + " ms per query on average");
    BenchmarkResult result = results.get(len / 2);
    System.out.println("p50: '" + result.query + "' returned " + result.count + " results in " + result.elapsed + " ms");
    result = results.get(len * 75 / 100);
    System.out.println("p75: '" + result.query + "' returned " + result.count + " results in " + result.elapsed + " ms");
    result = results.get(len * 90 / 100);
    System.out.println("p90: '" + result.query + "' returned " + result.count + " results in " + result.elapsed + " ms");
    result = results.get(len * 95 / 100);
    System.out.println("p95: '" + result.query + "' returned " + result.count + " results in " + result.elapsed + " ms");
    result = results.get(len * 99 / 100);
    System.out.println("p99: '" + result.query + "' returned " + result.count + " results in " + result.elapsed + " ms");
    result = results.get(len * 999 / 1000);
    System.out.println("p999: '" + result.query + "' returned " + result.count + " results in " + result.elapsed + " ms");
    result = results.get(len - 1);
    System.out.println("max: '" + result.query + "' returned " + result.count + " results in " + result.elapsed + " ms");
  }

  public static void main(String[] args) throws Exception {
    /* index path, field, search file, rucene path */
    if (args.length != 3) {
      throw new RuntimeException("missing required arguments");
    }
    IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(args[0])));

    IndexSearcher searcher = new IndexSearcher(reader);
    run(searcher, args[1], args[2], 100);
    run(searcher, args[1], args[2], 50);
    run(searcher, args[1], args[2], 100);
  }
}
