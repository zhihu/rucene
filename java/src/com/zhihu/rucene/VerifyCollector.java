package com.zhihu.rucene;

import java.util.*;
import java.io.*;

import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.search.*;
import org.apache.lucene.index.*;
import org.apache.lucene.util.*;

public class VerifyCollector implements Collector {
  private final static byte[] EMPTY_BYTES = new byte[0];
  private IndexReader reader;
  private DataOutputStream output;
  private VerifyStoredFieldVisitor visitor;
  private String field;
  private void writeBytes(byte[] bytes) throws IOException {
    Verify.writeBytes(output, bytes);
  }
  private void writeBytesRef(BytesRef bytes) throws IOException {
    Verify.writeBytes(output, bytes.bytes, bytes.offset, bytes.length);
  }
  private void writeString(String str) throws IOException {
    Verify.writeString(output, str);
  }

  class VerifyStoredFieldVisitor extends StoredFieldVisitor {
    @Override
    public void binaryField(FieldInfo fieldInfo, byte[] value) throws IOException {
      output.write(1);
      writeString(fieldInfo.name);
      writeBytes(value);
    }
    @Override
    public void doubleField(FieldInfo fieldInfo, double value) throws IOException {
      output.write(2);
      writeString(fieldInfo.name);
      output.writeDouble(value);
    }
    @Override
    public void floatField(FieldInfo fieldInfo, float value) throws IOException {
      output.write(3);
      writeString(fieldInfo.name);
      output.writeFloat(value);
    }
    @Override
    public void intField(FieldInfo fieldInfo, int value) throws IOException {
      output.write(4);
      writeString(fieldInfo.name);
      output.writeInt(value);
    }
    @Override
    public void longField(FieldInfo fieldInfo, long value) throws IOException {
      output.write(5);
      writeString(fieldInfo.name);
      output.writeLong(value);
    }
    @Override
    public void stringField(FieldInfo fieldInfo, byte[] value) throws IOException {
      output.write(6);
      writeString(fieldInfo.name);
      writeBytes(value);
    }
    @Override
    public StoredFieldVisitor.Status needsField(FieldInfo fieldInfo) {
      return StoredFieldVisitor.Status.YES;
    }

    public void flush() throws IOException {
      output.write(0);
      writeBytes(EMPTY_BYTES);
      writeBytes(EMPTY_BYTES);
    }
  }

  public VerifyCollector(IndexReader reader, String field, String query, DataOutputStream output) throws IOException {
    this.reader = reader;
    this.output = output;
    this.visitor = new VerifyStoredFieldVisitor();
    this.field = field;
    writeString(query);
  }
  @Override
  public boolean needsScores() {
    return false;
  }

  private void writePositions(Terms vector) throws IOException {
    TermsEnum iterator = vector.iterator();
    while (true) {
      BytesRef term = iterator.next();
      if (term == null) {
        output.write(0);
        break;
      }
      output.write(1);
      writeBytesRef(term);
      PostingsEnum positions = iterator.postings(null, PostingsEnum.POSITIONS);
      if (positions.nextDoc() == DocIdSetIterator.NO_MORE_DOCS) {
        throw new RuntimeException("Can't seek to first doc");
      }
      int freq = positions.freq();
      output.writeInt(freq);
      for (int idx = 0; idx < freq; ++idx) {
        output.writeInt(positions.nextPosition());
        output.writeInt(positions.startOffset());
        output.writeInt(positions.endOffset());
      }
    }
  }

  @Override
  public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
    final int docBase = context.docBase;
    return new LeafCollector() {
      @Override
      public void collect(int doc) throws IOException {
        output.writeInt(docBase);
        output.writeInt(doc);
        Terms vector = reader.getTermVector(docBase + doc, field);
        if (vector.hasPositions()) {
          output.write(1);
          writePositions(vector);
        } else {
          output.write(0);
        }
        reader.document(docBase + doc, visitor);
        visitor.flush();
      }
      @Override
      public void setScorer(Scorer scorer) throws IOException {}
    };
  }

  public void flush() throws IOException {
    output.writeInt(-1);
    output.writeInt(-1);
    output.flush();
  }
}
