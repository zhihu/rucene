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

use core::index::{CloneableNumericDocValues, NumericDocValues, NumericDocValuesProvider};
use core::util::{Bits, BitsMut, CloneableLongValues, DocId, LongValues};
use error::ErrorKind::{IllegalArgument, IllegalState};
use error::Result;

#[derive(Clone)]
struct SparseBitsContext {
    // index of doc_id in doc_ids
    index: i64,
    // mutable
    // doc_id at index
    doc_id: i64,
    // mutable
    // doc_id at (index + 1)
    next_doc_id: i64, // mutable
}

impl SparseBitsContext {
    fn new(first_doc_id: i64) -> Self {
        SparseBitsContext {
            index: -1,
            doc_id: -1,
            next_doc_id: first_doc_id,
        }
    }

    fn reset(&mut self, first_doc_id: i64) {
        self.index = -1;
        self.doc_id = -1;
        self.next_doc_id = first_doc_id;
    }
}

#[derive(Clone)]
pub struct SparseBits<T: LongValues> {
    max_doc: i64,
    doc_ids_length: i64,
    first_doc_id: i64,
    doc_ids: T,
    ctx: SparseBitsContext,
}

impl<T: LongValues> SparseBits<T> {
    pub fn new(max_doc: i64, doc_ids_length: i64, doc_ids: T) -> Result<Self> {
        if doc_ids_length > 0 && max_doc <= doc_ids.get64(doc_ids_length - 1)? {
            bail!(IllegalArgument(
                "max_doc must be > the last element of doc_ids".to_owned()
            ));
        };
        let first_doc_id = if doc_ids_length == 0 {
            max_doc
        } else {
            doc_ids.get64(0)?
        };
        Ok(SparseBits {
            max_doc,
            doc_ids_length,
            first_doc_id,
            doc_ids,
            ctx: SparseBitsContext::new(first_doc_id),
        })
    }

    /// Gallop forward and stop as soon as an index is found that is greater than
    ///  the given docId. *index* will store an index that stores a value
    /// that is <= *docId* while the return value will give an index
    /// that stores a value that is > *doc_id*. These indices can then be
    /// used to binary search.
    fn gallop(&self, ctx: &mut SparseBitsContext, doc_id: i64) -> Result<i64> {
        ctx.index += 1;
        ctx.doc_id = ctx.next_doc_id;
        let mut hi_index = ctx.index + 1;
        loop {
            if hi_index >= self.doc_ids_length {
                hi_index = self.doc_ids_length;
                ctx.next_doc_id = self.max_doc;
                return Ok(hi_index);
            }

            let hi_doc_id = self.doc_ids.get64(hi_index)?;
            if hi_doc_id > doc_id {
                ctx.next_doc_id = hi_doc_id;
                return Ok(hi_index);
            }

            let delta = hi_index - ctx.index;
            ctx.index = hi_index;
            ctx.doc_id = hi_doc_id;
            hi_index += delta << 1; // double the step each time
        }
    }

    fn binary_search(
        &self,
        ctx: &mut SparseBitsContext,
        mut hi_index: i64,
        doc_id: i64,
    ) -> Result<()> {
        while ctx.index + 1 < hi_index {
            let mid_index = ctx.index + (hi_index - ctx.index) / 2;
            let mid_doc_id = self.doc_ids.get64(mid_index)?;
            if mid_doc_id > doc_id {
                hi_index = mid_index;
                ctx.next_doc_id = mid_doc_id;
            } else {
                ctx.index = mid_index;
                ctx.doc_id = mid_doc_id;
            }
        }
        Ok(())
    }

    fn check_invariants(
        &self,
        ctx: &SparseBitsContext,
        next_index: i64,
        doc_id: i64,
    ) -> Result<()> {
        if ctx.doc_id > doc_id || ctx.next_doc_id <= doc_id {
            bail!(IllegalState(format!(
                "internal error a {} {} {}",
                doc_id, ctx.doc_id, ctx.next_doc_id
            )));
        }
        if !((ctx.index == -1 && ctx.doc_id == -1)
            || ctx.doc_id == self.doc_ids.get64(ctx.index)?)
        {
            bail!(IllegalState(format!(
                "internal error b {} {} {}",
                ctx.index,
                ctx.doc_id,
                self.doc_ids.get64(ctx.index)?
            )));
        }
        if !((next_index == self.doc_ids_length && ctx.next_doc_id == self.max_doc)
            || ctx.next_doc_id == self.doc_ids.get64(next_index)?)
        {
            bail!(IllegalState(format!(
                "internal error c {} {} {} {} {}",
                next_index,
                self.doc_ids_length,
                ctx.next_doc_id,
                self.max_doc,
                self.doc_ids.get64(next_index)?
            )));
        }
        Ok(())
    }

    fn exponential_search(&self, ctx: &mut SparseBitsContext, doc_id: i64) -> Result<()> {
        // seek forward by doubling the interval on each iteration
        let hi_index = self.gallop(ctx, doc_id)?;
        self.check_invariants(ctx, hi_index, doc_id)?;
        // now perform the actual binary search
        self.binary_search(ctx, hi_index, doc_id)
    }

    fn get64(&self, ctx: &mut SparseBitsContext, doc_id: i64) -> Result<bool> {
        if doc_id < ctx.doc_id {
            // reading doc ids backward, go back to the start
            ctx.reset(self.first_doc_id)
        }

        if doc_id >= ctx.next_doc_id {
            self.exponential_search(ctx, doc_id)?;
        }
        let next_index = ctx.index + 1;
        self.check_invariants(ctx, next_index, doc_id)?;
        Ok(doc_id == ctx.doc_id)
    }

    fn len(&self) -> usize {
        use core::util::math;

        if let Err(ref e) = math::long_to_int_exact(self.max_doc) {
            panic!("max_doc too big{}", e);
        }

        self.max_doc as usize
    }

    fn context(&self) -> SparseBitsContext {
        SparseBitsContext::new(self.first_doc_id)
    }
}

impl<T: LongValues> Bits for SparseBits<T> {
    fn get(&self, index: usize) -> Result<bool> {
        self.get64(&mut self.context(), index as i64)
    }

    fn len(&self) -> usize {
        SparseBits::len(self)
    }
}

impl<T: LongValues> BitsMut for SparseBits<T> {
    fn get(&mut self, index: usize) -> Result<bool> {
        unsafe {
            let ctx = &self.ctx as *const _ as *mut _;
            self.get64(&mut *ctx, index as i64)
        }
    }

    fn len(&self) -> usize {
        SparseBits::len(self)
    }
}

pub struct SparseLongValues<T: LongValues + Clone> {
    docs_with_field: SparseBits<T>,
    values: Box<dyn CloneableLongValues>,
    missing_value: i64,
}

impl<T: LongValues + Clone> Clone for SparseLongValues<T> {
    fn clone(&self) -> Self {
        Self {
            docs_with_field: self.docs_with_field.clone(),
            values: self.values.cloned(),
            missing_value: self.missing_value,
        }
    }
}

impl<T: LongValues + Clone> SparseLongValues<T> {
    pub fn new(
        docs_with_field: SparseBits<T>,
        values: Box<dyn CloneableLongValues>,
        missing_value: i64,
    ) -> Self {
        SparseLongValues {
            docs_with_field,
            values,
            missing_value,
        }
    }

    pub fn docs_with_field_clone(&self) -> SparseBits<T> {
        self.docs_with_field.clone()
    }
}

impl<T: LongValues + Clone + 'static> LongValues for SparseLongValues<T> {
    fn get64(&self, index: i64) -> Result<i64> {
        let mut ctx = self.docs_with_field.context();
        let exists = self.docs_with_field.get64(&mut ctx, index)?;
        if exists {
            self.values.get64(ctx.index)
        } else {
            Ok(self.missing_value)
        }
    }

    fn get64_mut(&mut self, index: i64) -> Result<i64> {
        let exists = BitsMut::get(&mut self.docs_with_field, index as usize)?;
        if exists {
            self.values.get64_mut(self.docs_with_field.ctx.index)
        } else {
            Ok(self.missing_value)
        }
    }
}

impl<T: LongValues + Clone + 'static> NumericDocValues for SparseLongValues<T> {
    fn get(&self, doc_id: DocId) -> Result<i64> {
        self.get64(i64::from(doc_id))
    }

    fn get_mut(&mut self, doc_id: DocId) -> Result<i64> {
        self.get64_mut(i64::from(doc_id))
    }
}

impl<T: LongValues + Clone + 'static> CloneableNumericDocValues for SparseLongValues<T> {
    fn clone_box(&self) -> Box<dyn NumericDocValues> {
        Box::new(self.clone())
    }
}

impl<T: LongValues + Clone + 'static> NumericDocValuesProvider for SparseLongValues<T> {
    fn get(&self) -> Result<Box<dyn NumericDocValues>> {
        Ok(Box::new(self.clone()))
    }
}
