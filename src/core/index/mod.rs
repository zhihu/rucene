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

pub mod merge;
pub mod reader;
pub mod writer;

error_chain! {
    types {
        Error, ErrorKind, ResultExt;
    }

    errors {
        MergeAborted(desc: String) {
            description(desc)
            display("merge is aborted: {}", desc)
        }
    }
}

#[cfg(test)]
pub mod tests {
    use std::collections::HashMap;

    use core::codec::doc_values::{
        BinaryDocValues, DocValuesProducerRef, NumericDocValues, SortedDocValues,
        SortedNumericDocValues, SortedSetDocValues,
    };
    use core::codec::field_infos::{FieldInfo, FieldInfos};
    use core::codec::tests::TestCodec;
    use core::codec::*;
    use core::doc::{DocValuesType, Document, IndexOptions, StoredFieldVisitor};
    use core::index::reader::*;
    use core::search::similarity::BM25Similarity;
    use core::search::sort_field::Sort;
    use core::util::external::Deferred;
    use core::util::*;
    use error::Result;
    use std::sync::Arc;

    pub struct MockNumericValues {
        num: HashMap<i32, u8>,
    }

    impl Default for MockNumericValues {
        fn default() -> MockNumericValues {
            let mut num = HashMap::<i32, u8>::new();

            let norm_value = BM25Similarity::encode_norm_value(1f32, 120);
            num.insert(1, norm_value);
            let norm_value = BM25Similarity::encode_norm_value(1f32, 1000);
            num.insert(2, norm_value);
            MockNumericValues { num }
        }
    }

    impl NumericDocValues for MockNumericValues {
        fn get(&self, doc_id: DocId) -> Result<i64> {
            Ok(i64::from(self.num[&doc_id]))
        }
    }

    #[derive(Default)]
    pub struct MockBits;

    impl Bits for MockBits {
        fn get(&self, _index: usize) -> Result<bool> {
            Ok(true)
        }

        fn len(&self) -> usize {
            unimplemented!()
        }
    }

    impl BitsMut for MockBits {
        fn get(&mut self, _index: usize) -> Result<bool> {
            Ok(true)
        }

        fn len(&self) -> usize {
            unimplemented!()
        }
    }

    pub struct MockLeafReader {
        codec: TestCodec,
        max_doc: DocId,
        live_docs: BitsRef,
        field_infos: FieldInfos,
    }

    impl MockLeafReader {
        pub fn new(max_doc: DocId) -> MockLeafReader {
            let mut infos = Vec::new();
            let field_info_one = FieldInfo::new(
                "test".to_string(),
                1,
                true,
                true,
                false,
                IndexOptions::Docs,
                DocValuesType::Numeric,
                1,
                HashMap::new(),
                1,
                1,
            )
            .unwrap();
            let field_info_two = FieldInfo::new(
                "test_2".to_string(),
                2,
                true,
                true,
                false,
                IndexOptions::Docs,
                DocValuesType::SortedNumeric,
                2,
                HashMap::new(),
                2,
                2,
            )
            .unwrap();
            infos.push(field_info_one);
            infos.push(field_info_two);

            MockLeafReader {
                codec: TestCodec::default(),
                max_doc,
                live_docs: Arc::new(MatchAllBits::new(0usize)),
                field_infos: FieldInfos::new(infos).unwrap(),
            }
        }
    }

    impl LeafReader for MockLeafReader {
        type Codec = TestCodec;
        type FieldsProducer = CodecFieldsProducer<TestCodec>;
        type TVFields = CodecTVFields<TestCodec>;
        type TVReader = Arc<CodecTVReader<TestCodec>>;
        type StoredReader = Arc<CodecStoredFieldsReader<TestCodec>>;
        type NormsReader = Arc<CodecNormsProducer<TestCodec>>;
        type PointsReader = Arc<CodecPointsReader<TestCodec>>;

        fn codec(&self) -> &Self::Codec {
            &self.codec
        }

        fn fields(&self) -> Result<Self::FieldsProducer> {
            bail!("unimplemented")
        }

        fn name(&self) -> &str {
            "test"
        }

        fn term_vector(&self, _leaf_doc_id: DocId) -> Result<Option<Self::TVFields>> {
            unimplemented!()
        }

        fn document(&self, _doc_id: DocId, _visitor: &mut dyn StoredFieldVisitor) -> Result<()> {
            unimplemented!()
        }

        fn live_docs(&self) -> BitsRef {
            Arc::clone(&self.live_docs)
        }

        fn field_info(&self, _field: &str) -> Option<&FieldInfo> {
            unimplemented!()
        }

        fn field_infos(&self) -> &FieldInfos {
            &self.field_infos
        }

        fn clone_field_infos(&self) -> Arc<FieldInfos> {
            unimplemented!()
        }

        fn max_doc(&self) -> DocId {
            self.max_doc
        }

        fn num_docs(&self) -> i32 {
            self.max_doc
        }

        fn get_numeric_doc_values(&self, _field: &str) -> Result<Box<dyn NumericDocValues>> {
            Ok(Box::new(MockNumericValues::default()))
        }

        fn get_binary_doc_values(&self, _field: &str) -> Result<Box<dyn BinaryDocValues>> {
            unimplemented!()
        }

        fn get_sorted_doc_values(&self, _field: &str) -> Result<Box<dyn SortedDocValues>> {
            unimplemented!()
        }

        fn get_sorted_numeric_doc_values(
            &self,
            _field: &str,
        ) -> Result<Box<dyn SortedNumericDocValues>> {
            unimplemented!()
        }

        fn get_sorted_set_doc_values(&self, _field: &str) -> Result<Box<dyn SortedSetDocValues>> {
            unimplemented!()
        }

        fn norm_values(&self, _field: &str) -> Result<Option<Box<dyn NumericDocValues>>> {
            Ok(Some(Box::new(MockNumericValues::default())))
        }

        fn get_docs_with_field(&self, _field: &str) -> Result<Box<dyn BitsMut>> {
            Ok(Box::new(MockBits::default()))
        }

        /// Returns the `PointValues` used for numeric or
        /// spatial searches, or None if there are no point fields.
        fn point_values(&self) -> Option<Self::PointsReader> {
            unimplemented!()
        }

        /// Expert: Returns a key for this IndexReader, so CachingWrapperFilter can find
        // it again.
        // This key must not have equals()/hashCode() methods, so &quot;equals&quot; means
        // &quot;identical&quot;.
        fn core_cache_key(&self) -> &str {
            unimplemented!()
        }

        /// Returns null if this leaf is unsorted, or the `Sort` that it was sorted by
        fn index_sort(&self) -> Option<&Sort> {
            None
        }

        /// Expert: adds a CoreClosedListener to this reader's shared core
        fn add_core_drop_listener(&self, _listener: Deferred) {
            unimplemented!()
        }

        // TODO, currently we don't provide remove listener method

        // following methods are from `CodecReader`
        // if this return false, then the following methods must not be called
        fn is_codec_reader(&self) -> bool {
            false
        }

        fn store_fields_reader(&self) -> Result<Self::StoredReader> {
            unimplemented!()
        }

        fn term_vectors_reader(&self) -> Result<Option<Self::TVReader>> {
            unimplemented!()
        }

        fn norms_reader(&self) -> Result<Option<Self::NormsReader>> {
            unimplemented!()
        }

        fn doc_values_reader(&self) -> Result<Option<DocValuesProducerRef>> {
            unimplemented!()
        }

        fn postings_reader(&self) -> Result<Self::FieldsProducer> {
            unimplemented!()
        }
    }

    impl AsRef<SearchLeafReader<TestCodec>> for MockLeafReader {
        fn as_ref(&self) -> &SearchLeafReader<TestCodec> {
            self
        }
    }

    pub struct MockIndexReader {
        leaves: Vec<MockLeafReader>,
        starts: Vec<i32>,
    }

    impl MockIndexReader {
        pub fn new(leaves: Vec<MockLeafReader>) -> MockIndexReader {
            let mut starts = Vec::with_capacity(leaves.len() + 1);
            let mut max_doc = 0;
            let mut _num_docs = 0;
            for reader in &leaves {
                starts.push(max_doc);
                max_doc += reader.max_doc();
                _num_docs += reader.num_docs();
            }

            starts.push(max_doc);

            MockIndexReader { leaves, starts }
        }
    }

    impl IndexReader for MockIndexReader {
        type Codec = TestCodec;

        fn leaves(&self) -> Vec<LeafReaderContext<'_, Self::Codec>> {
            self.leaves
                .iter()
                .enumerate()
                .map(|(i, r)| {
                    LeafReaderContext::new(
                        self,
                        r.as_ref() as &SearchLeafReader<TestCodec>,
                        i,
                        self.starts[i],
                    )
                })
                .collect()
        }

        fn term_vector(&self, _doc_id: DocId) -> Result<Option<CodecTVFields<Self::Codec>>> {
            unimplemented!()
        }

        fn document(&self, _doc_id: DocId, _fields_load: &[String]) -> Result<Document> {
            unimplemented!()
        }

        fn max_doc(&self) -> i32 {
            1
        }

        fn num_docs(&self) -> i32 {
            1
        }
    }
}
