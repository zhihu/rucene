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

use core::index::NumericDocValues;
use core::index::{
    BinaryDocValuesProvider, NumericDocValuesProvider, SortedDocValuesProvider,
    SortedNumericDocValuesProvider, SortedSetDocValuesProvider,
};
use core::index::{FieldInfo, Fields};
use core::util::BitsMut;
use error::Result;

use core::codec::blocktree::BlockTreeTermsReader;
use core::codec::per_field::PerFieldFieldsReader;
use core::codec::FieldReaderRef;
use std::sync::Arc;

///  Abstract API that produces terms, doc, freq, prox, offset and payloads postings
pub trait FieldsProducer: Fields {
    /// Checks consistency of this reader.
    /// Note that this may be costly in terms of I/O, e.g.
    /// may involve computing a checksum value against large data files.
    fn check_integrity(&self) -> Result<()>;

    // Returns an instance optimized for merging.
    // fn get_merge_instance(&self) -> Result<FieldsProducerRef>;
}

// this type should be Arc<Codec::PostingsFormat::FieldsProducer>
pub type FieldsProducerRef = Arc<PerFieldFieldsReader>;

impl<T: FieldsProducer> FieldsProducer for Arc<T> {
    fn check_integrity(&self) -> Result<()> {
        (**self).check_integrity()
    }
}

impl<T: FieldsProducer> Fields for Arc<T> {
    type Terms = T::Terms;
    fn fields(&self) -> Vec<String> {
        (**self).fields()
    }

    fn terms(&self, field: &str) -> Result<Option<Self::Terms>> {
        (**self).terms(field)
    }

    fn size(&self) -> usize {
        (**self).size()
    }

    fn terms_freq(&self, field: &str) -> usize {
        (**self).terms_freq(field)
    }
}

/// `FieldsProducer` impl for `PostingsFormatEnum`
pub enum FieldsProducerEnum {
    Lucene50(BlockTreeTermsReader),
}

impl FieldsProducer for FieldsProducerEnum {
    fn check_integrity(&self) -> Result<()> {
        match self {
            FieldsProducerEnum::Lucene50(f) => f.check_integrity(),
        }
    }
}

impl Fields for FieldsProducerEnum {
    type Terms = FieldReaderRef;
    fn fields(&self) -> Vec<String> {
        match self {
            FieldsProducerEnum::Lucene50(f) => f.fields(),
        }
    }

    fn terms(&self, field: &str) -> Result<Option<Self::Terms>> {
        match self {
            FieldsProducerEnum::Lucene50(f) => f.terms(field),
        }
    }

    fn size(&self) -> usize {
        match self {
            FieldsProducerEnum::Lucene50(f) => f.size(),
        }
    }

    fn terms_freq(&self, field: &str) -> usize {
        match self {
            FieldsProducerEnum::Lucene50(f) => f.terms_freq(field),
        }
    }
}

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

/// Abstract API that produces field normalization values
pub trait NormsProducer {
    fn norms(&self, field: &FieldInfo) -> Result<Box<dyn NumericDocValues>>;
    fn check_integrity(&self) -> Result<()> {
        // codec_util::checksum_entire_file(input)?;
        Ok(())
    }
}

impl<T: NormsProducer> NormsProducer for Arc<T> {
    fn norms(&self, field: &FieldInfo) -> Result<Box<dyn NumericDocValues>> {
        (**self).norms(field)
    }
    fn check_integrity(&self) -> Result<()> {
        (**self).check_integrity()
    }
}
