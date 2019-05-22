use core::index::BoxedBinaryDocValuesEnum;
use core::index::SortedDocValuesTermIterator;
use core::index::{
    BinaryDocValues, CompressedBinaryDocValues, DocValuesTermIterator, LongBinaryDocValues,
};
use core::util::bit_util;
use core::util::DocId;
use core::util::LongValues;
use error::Result;

use std::sync::Arc;

pub trait SortedDocValues: BinaryDocValues {
    fn get_ord(&self, doc_id: DocId) -> Result<i32>;

    fn lookup_ord(&self, ord: i32) -> Result<Vec<u8>>;

    fn get_value_count(&self) -> usize;

    /// if key exists, return its ordinal, else return
    /// - insertion_point - 1.
    fn lookup_term(&self, key: &[u8]) -> Result<i32> {
        let mut low = 0;
        let mut high = self.get_value_count() as i32 - 1;
        while low <= high {
            let mid = low + (high - low) / 2;
            let term = self.lookup_ord(mid)?;
            let cmp = bit_util::bcompare(&term, key);
            if cmp < 0 {
                low = mid + 1;
            } else if cmp > 0 {
                high = mid - 1;
            } else {
                return Ok(mid); // key found
            }
        }
        Ok(-(low + 1)) // key not found
    }

    fn term_iterator(&self) -> Result<DocValuesTermIterator>;
}

pub type SortedDocValuesRef = Arc<dyn SortedDocValues>;

pub struct EmptySortedDocValues;

impl SortedDocValues for EmptySortedDocValues {
    fn get_ord(&self, _doc_id: DocId) -> Result<i32> {
        Ok(-1)
    }

    fn lookup_ord(&self, _ord: i32) -> Result<Vec<u8>> {
        Ok(Vec::with_capacity(0))
    }

    fn get_value_count(&self) -> usize {
        0
    }

    fn term_iterator(&self) -> Result<DocValuesTermIterator> {
        Ok(DocValuesTermIterator::empty())
    }
}

impl BinaryDocValues for EmptySortedDocValues {
    fn get(&self, _doc_id: DocId) -> Result<Vec<u8>> {
        Ok(Vec::with_capacity(0))
    }
}

impl<T: SortedDocValues + ?Sized> SortedDocValues for Arc<T> {
    fn get_ord(&self, doc_id: DocId) -> Result<i32> {
        (**self).get_ord(doc_id)
    }

    fn lookup_ord(&self, ord: i32) -> Result<Vec<u8>> {
        (**self).lookup_ord(ord)
    }

    fn get_value_count(&self) -> usize {
        (**self).get_value_count()
    }

    fn lookup_term(&self, key: &[u8]) -> Result<i32> {
        (**self).lookup_term(key)
    }

    fn term_iterator(&self) -> Result<DocValuesTermIterator> {
        (**self).term_iterator()
    }
}

impl<T: SortedDocValues + ?Sized> BinaryDocValues for Arc<T> {
    fn get(&self, doc_id: DocId) -> Result<Vec<u8>> {
        (**self).get(doc_id)
    }
}

#[derive(Clone)]
pub struct TailoredSortedDocValues {
    inner: Arc<TailoredSortedDocValuesInner>,
}

impl TailoredSortedDocValues {
    pub fn new(
        ordinals: Box<dyn LongValues>,
        binary: Box<dyn LongBinaryDocValues>,
        value_count: usize,
    ) -> Self {
        let inner = TailoredSortedDocValuesInner::new(ordinals, binary, value_count);
        TailoredSortedDocValues {
            inner: Arc::new(inner),
        }
    }

    pub fn with_compression(
        ordinals: Box<dyn LongValues>,
        binary: CompressedBinaryDocValues,
        value_count: usize,
    ) -> Self {
        let inner = TailoredSortedDocValuesInner::with_compression(ordinals, binary, value_count);
        TailoredSortedDocValues {
            inner: Arc::new(inner),
        }
    }
}

impl SortedDocValues for TailoredSortedDocValues {
    fn get_ord(&self, doc_id: DocId) -> Result<i32> {
        self.inner.get_ord(doc_id)
    }

    fn lookup_ord(&self, ord: i32) -> Result<Vec<u8>> {
        self.inner.lookup_ord(ord)
    }

    fn get_value_count(&self) -> usize {
        self.inner.value_count
    }

    fn lookup_term(&self, key: &[u8]) -> Result<i32> {
        self.inner.lookup_term(key)
    }
    fn term_iterator(&self) -> Result<DocValuesTermIterator> {
        match self.inner.binary {
            BoxedBinaryDocValuesEnum::Compressed(ref bin) => {
                let boxed = bin.get_term_iterator()?;
                Ok(DocValuesTermIterator::comp_bin(boxed))
            }
            _ => {
                let ti = SortedDocValuesTermIterator::new(self.clone());
                Ok(DocValuesTermIterator::sorted(ti))
            }
        }
    }
}

impl BinaryDocValues for TailoredSortedDocValues {
    fn get(&self, doc_id: DocId) -> Result<Vec<u8>> {
        let ord = self.get_ord(doc_id)?;
        if ord == -1 {
            Ok(Vec::with_capacity(0))
        } else {
            self.lookup_ord(ord)
        }
    }
}

pub struct TailoredSortedDocValuesInner {
    ordinals: Box<dyn LongValues>,
    binary: BoxedBinaryDocValuesEnum,
    value_count: usize,
}

impl TailoredSortedDocValuesInner {
    fn new(
        ordinals: Box<dyn LongValues>,
        binary: Box<dyn LongBinaryDocValues>,
        value_count: usize,
    ) -> Self {
        TailoredSortedDocValuesInner {
            ordinals,
            binary: BoxedBinaryDocValuesEnum::General(binary),
            value_count,
        }
    }

    fn with_compression(
        ordinals: Box<dyn LongValues>,
        binary: CompressedBinaryDocValues,
        value_count: usize,
    ) -> Self {
        TailoredSortedDocValuesInner {
            ordinals,
            binary: BoxedBinaryDocValuesEnum::Compressed(binary),
            value_count,
        }
    }

    fn get_ord(&self, doc_id: DocId) -> Result<i32> {
        let value = self.ordinals.get(doc_id)?;
        Ok(value as i32)
    }

    fn lookup_ord(&self, ord: i32) -> Result<Vec<u8>> {
        match self.binary {
            BoxedBinaryDocValuesEnum::General(ref binary) => binary.get(ord),
            BoxedBinaryDocValuesEnum::Compressed(ref binary) => binary.get(ord),
        }
    }

    fn lookup_term(&self, key: &[u8]) -> Result<i32> {
        match self.binary {
            BoxedBinaryDocValuesEnum::Compressed(ref binary) => {
                let val = binary.lookup_term(key)? as i32;
                Ok(val)
            }
            _ => {
                // TODO: Copy from SortedDocValues#lookup_term
                let mut low = 0;
                let mut high = self.value_count as i32 - 1;
                while low <= high {
                    let mid = low + (high - low) / 2;
                    let term = self.lookup_ord(mid)?;
                    let cmp = bit_util::bcompare(&term, key);
                    if cmp < 0 {
                        low = mid + 1;
                    } else if cmp > 0 {
                        high = mid - 1;
                    } else {
                        return Ok(mid); // key found
                    }
                }
                Ok(-(low + 1)) // key not found
            }
        }
    }
}
