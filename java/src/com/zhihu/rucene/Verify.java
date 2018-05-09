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

public class Verify {
  private static class OutputDumper extends Thread {
    private BufferedReader input;
    private PrintStream output;
    public OutputDumper(InputStream input, PrintStream output) {
      this.input = new BufferedReader(new InputStreamReader(input));
      this.output = output;
      start();
    }
    public void run() {
      try {
        String line;
        while ((line = input.readLine()) != null) {
          output.println(line);
        }
      } catch (Exception ingored) {
      }
    }
  }

  private static Charset utf8 = StandardCharsets.UTF_8;
  private static byte[] EMPTY_BYTES = new byte[0];
  protected static void writeBytes(DataOutputStream output, byte[] bytes) throws IOException {
    output.writeInt(bytes.length);
    output.write(bytes);
  }
  protected static void writeBytes(DataOutputStream output, byte[] bytes, int offset, int length) throws IOException {
    output.writeInt(length);
    output.write(bytes, offset, length);
  }
  protected static void writeString(DataOutputStream output, String str) throws IOException {
    writeBytes(output, str.getBytes(utf8));
  }

  private static void writeDocValues(DataOutputStream output, BinaryDocValues docValues, int maxDoc) throws IOException {
    for (int doc = 0; doc < maxDoc; ++doc) {
      BytesRef ref = docValues.get(doc);
      writeBytes(output, ref.bytes, ref.offset, ref.length);
    }
  }

  private static void writeDocValues(DataOutputStream output, NumericDocValues docValues, int maxDoc) throws IOException {
    for (int doc = 0; doc < maxDoc; ++doc) {
      long value = docValues.get(doc);
      output.writeLong(value);
    }
  }

  private static void writeDocValues(DataOutputStream output, SortedDocValues docValues, int maxDoc) throws IOException {
    for (int doc = 0; doc < maxDoc; ++doc) {
      BytesRef ref = docValues.get(doc);
      writeBytes(output, ref.bytes, ref.offset, ref.length);
    }
  }

  private static void writeDocValues(DataOutputStream output, SortedNumericDocValues docValues, int maxDoc) throws IOException {
    for (int doc = 0; doc < maxDoc; ++doc) {
      docValues.setDocument(doc);
      int count = docValues.count();
      output.writeInt(count);
      for (int idx = 0; idx < count; ++idx) {
        output.writeLong(docValues.valueAt(idx));
      }
    }
  }

  private static void writeDocValues(DataOutputStream output, SortedSetDocValues docValues, int maxDoc) throws IOException {
    for (int doc = 0; doc < maxDoc; ++doc) {
      docValues.setDocument(doc);
      while (true) {
        long ord = docValues.nextOrd();
        output.writeLong(ord);
        if (ord != SortedSetDocValues.NO_MORE_ORDS) {
          BytesRef ref = docValues.lookupOrd(ord);
          writeBytes(output, ref.bytes, ref.offset, ref.length);
        } else {
          break;
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    /* index path, field, search file, rucene path */
    if (args.length != 4) {
      throw new RuntimeException("missing required arguments");
    }
    String index = args[0];
    String field = args[1];
    String rucenePath = args[3];
    IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(index)));

    IndexSearcher searcher = new IndexSearcher(reader);
    Analyzer analyzer = new WhitespaceAnalyzer();
    BufferedReader queries = Files.newBufferedReader(Paths.get(args[2]), StandardCharsets.UTF_8);
    QueryParser parser = new QueryParser(field, analyzer);

    Process rucene = new ProcessBuilder(rucenePath, index, field).start();
    DataOutputStream output = new DataOutputStream(new BufferedOutputStream(rucene.getOutputStream()));
    AtomicBoolean terminated = new AtomicBoolean(false);
    OutputDumper stderrDumper = new OutputDumper(rucene.getErrorStream(), System.err);
    OutputDumper stdoutDumper = new OutputDumper(rucene.getInputStream(), System.out);
    Thread watchdog = new Thread() {
      public void run() {
        try {
          rucene.waitFor();
        } catch (Exception ignored) {
        } finally {
          terminated.set(true);
        }
      }
    };
    watchdog.setName("Watchdog Thread");
    watchdog.start();

    /* verify doc values */
    List<LeafReaderContext> leaves = reader.leaves();
    output.writeInt(leaves.size());
    Map<String, FieldInfo> pointFields = new HashMap();
    for (LeafReaderContext ctx : leaves) {
      LeafReader leafReader = ctx.reader();
      int maxDoc = leafReader.maxDoc();
      FieldInfos fieldInfos = leafReader.getFieldInfos();
      output.writeInt(maxDoc);
      output.writeInt(fieldInfos.size());
      Iterator<FieldInfo> infoIter = fieldInfos.iterator();
      while (infoIter.hasNext()) {
        FieldInfo fieldInfo = infoIter.next();
        String fieldName = fieldInfo.name;
        if (fieldInfo.getPointDimensionCount() > 0) {
          pointFields.put(fieldName, fieldInfo);
        }
        output.writeInt(fieldInfo.number);
        writeString(output, fieldName);
        switch (fieldInfo.getDocValuesType()) {
        case BINARY:
          output.write(0);
          writeDocValues(output, leafReader.getBinaryDocValues(fieldName), maxDoc);
          break;
        case NONE:
          output.write(1);
          continue;
        case NUMERIC:
          output.write(2);
          writeDocValues(output, leafReader.getNumericDocValues(fieldName), maxDoc);
          break;
        case SORTED:
          output.write(3);
          writeDocValues(output, leafReader.getSortedDocValues(fieldName), maxDoc);
          break;
        case SORTED_NUMERIC:
          output.write(4);
          writeDocValues(output, leafReader.getSortedNumericDocValues(fieldName), maxDoc);
          break;
        case SORTED_SET:
          output.write(5);
          writeDocValues(output, leafReader.getSortedSetDocValues(fieldName), maxDoc);
          break;
        default:
          throw new RuntimeException("Unknown doc values type");
        }
      }
    }

    output.writeInt(pointFields.size());
    if (!pointFields.isEmpty()) {
      /* verify point queries if there is any */
      output.writeInt(leaves.size());
      for (FieldInfo fieldInfo : pointFields.values()) {
        String fieldName = fieldInfo.name;
        writeString(output, fieldName);
        for (LeafReaderContext ctx : leaves) {
          LeafReader leafReader = ctx.reader();
          PointValues pointValues = leafReader.getPointValues();
          if (leafReader.getFieldInfos().fieldInfo(fieldName) != null) {
            pointValues.intersect(fieldName, new PointValues.IntersectVisitor() {
              @Override
              public void visit(int docID) throws IOException {
              }
              @Override
              public void visit(int docID, byte[] packedValue) throws IOException {
                output.writeInt(docID);
                writeBytes(output, packedValue);
              }
              @Override
              public PointValues.Relation compare(byte[] min, byte[] max) {
                return PointValues.Relation.CELL_CROSSES_QUERY;
              }
            });
          }
          output.writeInt(-1);
          writeBytes(output, EMPTY_BYTES);
        }
      }
    }

    /* verify queries */
    String query;
    while ((query = queries.readLine()) != null) {
      query = query.trim();
      if (!query.isEmpty()) {
        try {
          query = query.trim();
          Query realQuery = parser.parse(query);
          VerifyCollector collector = new VerifyCollector(reader, field, query, output);
          searcher.search(realQuery, collector);
          collector.flush();
        } catch (Exception ex) {
          ex.printStackTrace();
        }
      }
      if (terminated.get()) {
        System.out.println("Exit lucene process as rucene process is already terminated");
        break;
      }
    }
    output.close();
    stdoutDumper.join();
    stderrDumper.join();
  }
}
