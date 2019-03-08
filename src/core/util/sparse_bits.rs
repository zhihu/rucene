use core::index::{NumericDocValues, NumericDocValuesContext};
use core::util::DocId;
use core::util::{Bits, BitsContext};
use core::util::{LongValues, LongValuesContext};
use error::ErrorKind::{IllegalArgument, IllegalState};
use error::Result;

use std::sync::Arc;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

#[derive(Clone)]
struct SparseBitsContext {
    // index of doc_id in doc_ids
    index: i64, // mutable
    // doc_id at index
    doc_id: i64, // mutable
    // doc_id at (index + 1)
    next_doc_id: i64, // mutable
}

const SPARSE_BITS_CONTEXT_SERIALIZED_SIZE: usize = 24;

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

    fn serialize(&self) -> BitsContext {
        let mut data = [0u8; 64];
        {
            let mut buffer = &mut data as &mut [u8];
            buffer.write_i64::<LittleEndian>(self.index).unwrap();
            buffer.write_i64::<LittleEndian>(self.doc_id).unwrap();
            buffer.write_i64::<LittleEndian>(self.next_doc_id).unwrap();
        }
        Some(data)
    }

    fn deserialize(mut from: &[u8]) -> Result<Self> {
        if from.len() < SPARSE_BITS_CONTEXT_SERIALIZED_SIZE {
            bail!(IllegalArgument(
                "Serialized bytes is not for SparseBitsContext".into()
            ))
        }
        let index = from.read_i64::<LittleEndian>()?;
        let doc_id = from.read_i64::<LittleEndian>()?;
        let next_doc_id = from.read_i64::<LittleEndian>()?;
        Ok(SparseBitsContext {
            index,
            doc_id,
            next_doc_id,
        })
    }
}

pub struct SparseBits {
    max_doc: i64,
    doc_ids_length: i64,
    first_doc_id: i64,
    doc_ids: Arc<LongValues>,
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
            doc_ids: Arc::from(doc_ids),
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
            bail!(IllegalState("internal error".to_owned()));
        }
        if !((ctx.index == -1 && ctx.doc_id == -1)
            || ctx.doc_id == self.doc_ids.get64(ctx.index)?)
        {
            bail!(IllegalState("internal error".to_owned()));
        }
        if !((next_index == self.doc_ids_length && ctx.next_doc_id == self.max_doc)
            || ctx.next_doc_id == self.doc_ids.get64(next_index)?)
        {
            bail!(IllegalState("internal error".to_owned()));
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

    fn get64(&self, ctx: &mut SparseBitsContext, doc_id: i64) -> Result<(bool, BitsContext)> {
        if doc_id < ctx.doc_id {
            // reading doc ids backward, go back to the start
            ctx.reset(self.first_doc_id)
        }

        if doc_id >= ctx.next_doc_id {
            self.exponential_search(ctx, doc_id)?;
        }
        let next_index = ctx.index + 1;
        self.check_invariants(ctx, next_index, doc_id)?;
        Ok((doc_id == ctx.doc_id, ctx.serialize()))
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

impl Bits for SparseBits {
    fn get_with_ctx(&self, ctx: BitsContext, index: usize) -> Result<(bool, BitsContext)> {
        let mut ctx = match ctx {
            Some(c) => SparseBitsContext::deserialize(&c)?,
            None => self.context(),
        };
        self.get64(&mut ctx, index as i64)
    }

    fn len(&self) -> usize {
        SparseBits::len(self)
    }
}

pub struct SparseLongValues {
    docs_with_field: Arc<SparseBits>,
    values: Box<LongValues>,
    missing_value: i64,
}

impl SparseLongValues {
    pub fn new(
        docs_with_field: Arc<SparseBits>,
        values: Box<LongValues>,
        missing_value: i64,
    ) -> Self {
        SparseLongValues {
            docs_with_field,
            values,
            missing_value,
        }
    }

    pub fn docs_with_field_clone(&self) -> Arc<SparseBits> {
        Arc::clone(&self.docs_with_field)
    }
}

impl LongValues for SparseLongValues {
    fn get64_with_ctx(
        &self,
        ctx: LongValuesContext,
        index: i64,
    ) -> Result<(i64, LongValuesContext)> {
        let mut ctx = match ctx {
            Some(c) => SparseBitsContext::deserialize(&c)?,
            None => self.docs_with_field.context(),
        };
        let (exists, new_ctx) = self.docs_with_field.get64(&mut ctx, index)?;
        if exists {
            let r = self.values.get64(ctx.index)?;
            Ok((r, new_ctx))
        } else {
            Ok((self.missing_value, new_ctx))
        }
    }
}

impl NumericDocValues for SparseLongValues {
    fn get_with_ctx(
        &self,
        ctx: NumericDocValuesContext,
        doc_id: DocId,
    ) -> Result<(i64, NumericDocValuesContext)> {
        LongValues::get64_with_ctx(self, ctx, i64::from(doc_id))
    }
}
