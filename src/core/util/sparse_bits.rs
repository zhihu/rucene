use core::index::NumericDocValues;
use core::util::DocId;
use core::util::LongValues;
use core::util::MutableBits;
use error::ErrorKind::{IllegalArgument, IllegalState};
use error::Result;

use std::sync::Arc;

#[derive(Clone)]
pub struct SparseBits {
    max_doc: i64,
    doc_ids_length: i64,
    first_doc_id: i64,
    // index of doc_id in doc_ids
    index: i64, // mutable
    // doc_id at index
    doc_id: i64, // mutable
    // doc_id at (index + 1)
    next_doc_id: i64, // mutable
    doc_ids: Arc<Box<LongValues>>,
}

impl SparseBits {
    pub fn new(
        max_doc: i64,
        doc_ids_length: i64,
        mut doc_ids: Box<LongValues>,
    ) -> Result<SparseBits> {
        if doc_ids_length > 0 && max_doc <= doc_ids.as_mut().get64(doc_ids_length - 1)? {
            bail!(IllegalArgument(
                "max_doc must be > the last element of doc_ids".to_owned()
            ));
        };
        let first_doc_id = if doc_ids_length == 0 {
            max_doc
        } else {
            doc_ids.as_mut().get64(0)?
        };
        Ok(SparseBits {
            max_doc,
            doc_ids_length,
            first_doc_id,
            index: -1,
            doc_id: -1,
            next_doc_id: first_doc_id,
            doc_ids: Arc::new(doc_ids),
        })
    }

    fn reset(&mut self) {
        self.index = -1;
        self.doc_id = -1;
        self.next_doc_id = self.first_doc_id;
    }

    /// Gallop forward and stop as soon as an index is found that is greater than
    ///  the given docId. *index* will store an index that stores a value
    /// that is <= *docId* while the return value will give an index
    /// that stores a value that is > *doc_id*. These indices can then be
    /// used to binary search.
    fn gallop(&mut self, doc_id: i64) -> Result<i64> {
        self.index += 1;
        self.doc_id = self.next_doc_id;
        let mut hi_index = self.index + 1;
        loop {
            if hi_index >= self.doc_ids_length {
                hi_index = self.doc_ids_length;
                self.next_doc_id = self.max_doc;
                return Ok(hi_index);
            }

            let hi_doc_id = self.doc_ids.get64(hi_index)?;
            if hi_doc_id > doc_id {
                self.next_doc_id = hi_doc_id;
                return Ok(hi_index);
            }

            let delta = hi_index - self.index;
            self.index = hi_index;
            self.doc_id = hi_doc_id;
            hi_index += delta << 1; // double the step each time
        }
    }

    fn binary_search(&mut self, mut hi_index: i64, doc_id: i64) -> Result<()> {
        while self.index + 1 < hi_index {
            let mid_index = self.index + (hi_index - self.index) / 2;
            let mid_doc_id = self.doc_ids.get64(mid_index)?;
            if mid_doc_id > doc_id {
                hi_index = mid_index;
                self.next_doc_id = mid_doc_id;
            } else {
                self.index = mid_index;
                self.doc_id = mid_doc_id;
            }
        }
        Ok(())
    }

    fn check_invariants(&mut self, next_index: i64, doc_id: i64) -> Result<()> {
        if self.doc_id > doc_id || self.next_doc_id <= doc_id {
            bail!(IllegalState("internal error".to_owned()));
        }
        if !((self.index == -1 && self.doc_id == -1)
            || self.doc_id == self.doc_ids.get64(self.index)?)
        {
            bail!(IllegalState("internal error".to_owned()));
        }
        if !((next_index == self.doc_ids_length && self.next_doc_id == self.max_doc)
            || self.next_doc_id == self.doc_ids.get64(next_index)?)
        {
            bail!(IllegalState("internal error".to_owned()));
        }
        Ok(())
    }

    fn exponential_search(&mut self, doc_id: i64) -> Result<()> {
        // seek forward by doubling the interval on each iteration
        let hi_index = self.gallop(doc_id)?;
        self.check_invariants(hi_index, doc_id)?;
        // now perform the actual binary search
        self.binary_search(hi_index, doc_id)
    }

    fn get64(&mut self, doc_id: i64) -> Result<bool> {
        if doc_id < self.doc_id {
            // reading doc ids backward, go back to the start
            self.reset();
        }

        if doc_id >= self.next_doc_id {
            self.exponential_search(doc_id)?;
        }
        let next_index = self.index + 1;
        self.check_invariants(next_index, doc_id)?;
        Ok(doc_id == self.doc_id)
    }
}

impl MutableBits for SparseBits {
    // ugly workaround
    fn get(&mut self, index: usize) -> Result<bool> {
        self.get64(index as i64)
    }

    // again, workaround
    fn len(&self) -> usize {
        use core::util::math;

        if let Err(ref e) = math::long_to_int_exact(self.max_doc) {
            panic!("max_doc too big{}", e);
        }

        self.max_doc as usize
    }
}

pub struct SparseLongValues {
    docs_with_field: SparseBits,
    values: Box<LongValues>,
    missing_value: i64,
}

impl SparseLongValues {
    pub fn new(docs_with_field: SparseBits, values: Box<LongValues>, missing_value: i64) -> Self {
        SparseLongValues {
            docs_with_field,
            values,
            missing_value,
        }
    }

    pub fn docs_with_field_clone(&self) -> SparseBits {
        self.docs_with_field.clone()
    }
}

impl LongValues for SparseLongValues {
    fn get64(&self, doc_id: i64) -> Result<i64> {
        let mut docs_with_field = self.docs_with_field_clone();
        if docs_with_field.get64(doc_id)? {
            let r = self.values.get64(docs_with_field.index)?;
            Ok(r)
        } else {
            Ok(self.missing_value)
        }
    }
}

impl NumericDocValues for SparseLongValues {
    fn get(&self, doc_id: DocId) -> Result<i64> {
        LongValues::get64(self, i64::from(doc_id))
    }
}
