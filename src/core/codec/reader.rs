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

use core::index::Fields;
use core::index::PointValues;
use core::index::StoredFieldVisitor;
use core::util::DocId;

use error::Result;

use std::any::Any;
use std::sync::Arc;

pub trait StoredFieldsReader: Sized {
    // NOTE: we can't use generic for `StoredFieldVisitor` because of IndexReader#docment can't use
    // generic
    fn visit_document(&self, doc_id: DocId, visitor: &mut dyn StoredFieldVisitor) -> Result<()>;

    // a mutable version for `Self::visit_document` that maybe more fast
    fn visit_document_mut(
        &mut self,
        doc_id: DocId,
        visitor: &mut dyn StoredFieldVisitor,
    ) -> Result<()>;

    fn get_merge_instance(&self) -> Result<Self>;

    // used for type Downcast
    fn as_any(&self) -> &dyn Any;
}

impl<T: StoredFieldsReader + 'static> StoredFieldsReader for Arc<T> {
    fn visit_document(&self, doc_id: DocId, visitor: &mut dyn StoredFieldVisitor) -> Result<()> {
        (**self).visit_document(doc_id, visitor)
    }

    fn visit_document_mut(
        &mut self,
        doc_id: DocId,
        visitor: &mut dyn StoredFieldVisitor,
    ) -> Result<()> {
        debug_assert_eq!(Arc::strong_count(self), 1);
        Arc::get_mut(self)
            .unwrap()
            .visit_document_mut(doc_id, visitor)
    }

    fn get_merge_instance(&self) -> Result<Self> {
        Ok(Arc::new((**self).get_merge_instance()?))
    }

    fn as_any(&self) -> &dyn Any {
        &**self
    }
}

pub trait TermVectorsReader {
    type Fields: Fields;
    fn get(&self, doc: DocId) -> Result<Option<Self::Fields>>;
    fn as_any(&self) -> &dyn Any;
}

impl<T: TermVectorsReader + 'static> TermVectorsReader for Arc<T> {
    type Fields = T::Fields;

    fn get(&self, doc: DocId) -> Result<Option<Self::Fields>> {
        (**self).get(doc)
    }

    fn as_any(&self) -> &dyn Any {
        (**self).as_any()
    }
}

/// trait for visit point values.
pub trait PointsReader: PointValues {
    fn check_integrity(&self) -> Result<()>;
    fn as_any(&self) -> &dyn Any;
}

/// `PointsReader` whose order of points can be changed.
///
/// This trait is useful for codecs to optimize flush.
pub trait MutablePointsReader: PointsReader {
    fn value(&self, i: i32, packed_value: &mut Vec<u8>);
    fn byte_at(&self, i: i32, k: i32) -> u8;
    fn doc_id(&self, i: i32) -> DocId;
    fn swap(&mut self, i: i32, j: i32);
    fn clone(&self) -> Self;
}
