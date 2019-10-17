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

use core::codec::doc_values::{
    BinaryDocValuesProvider, NumericDocValuesProvider, SortedDocValuesProvider,
    SortedNumericDocValuesProvider, SortedSetDocValuesProvider,
};
use core::codec::field_infos::FieldInfo;
use core::util::BitsMut;

use error::Result;
use std::sync::Arc;

/// Abstract API that produces numeric, binary, sorted, sortedset,
/// and sortednumeric docvalues.
///
/// NOTE: the returned instance must always be thread-safe, this is different from
/// the Lucene restraint
pub trait DocValuesProducer: Send + Sync {
    /// Returns `NumericDocValues` for this field.
    fn get_numeric(&self, field_info: &FieldInfo) -> Result<Arc<dyn NumericDocValuesProvider>>;

    ///  Returns `BinaryDocValues` for this field.
    fn get_binary(&self, field_info: &FieldInfo) -> Result<Arc<dyn BinaryDocValuesProvider>>;

    ///  Returns `SortedDocValues` for this field.
    fn get_sorted(&self, field: &FieldInfo) -> Result<Arc<dyn SortedDocValuesProvider>>;

    ///  Returns `SortedNumericDocValues` for this field.
    fn get_sorted_numeric(
        &self,
        field: &FieldInfo,
    ) -> Result<Arc<dyn SortedNumericDocValuesProvider>>;

    ///  Returns `SortedSetDocValues` for this field.
    fn get_sorted_set(&self, field: &FieldInfo) -> Result<Arc<dyn SortedSetDocValuesProvider>>;
    /// Returns a `bits` at the size of `reader.max_doc()`, with turned on bits for each doc_id
    /// that does have a value for this field.
    /// The returned instance need not be thread-safe: it will only be used by a single thread.
    fn get_docs_with_field(&self, field: &FieldInfo) -> Result<Box<dyn BitsMut>>;
    /// Checks consistency of this producer
    /// Note that this may be costly in terms of I/O, e.g.
    /// may involve computing a checksum value against large data files.
    fn check_integrity(&self) -> Result<()>;

    /// Returns an instance optimized for merging.
    fn get_merge_instance(&self) -> Result<Box<dyn DocValuesProducer>>;
}

pub type DocValuesProducerRef = Arc<dyn DocValuesProducer>;
