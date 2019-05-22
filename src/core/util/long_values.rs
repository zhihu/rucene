use core::codec::LiveBitsEnum;
use core::index::{NumericDocValues, NumericDocValuesContext};
use core::util::packed::DirectPackedReader;
use core::util::Bits;
use core::util::DocId;
use error::Result;

/// Abstraction over an array of longs.
pub type LongValuesContext = NumericDocValuesContext;

pub trait LongValues: NumericDocValues {
    fn get64(&self, index: i64) -> Result<i64> {
        self.get64_with_ctx(None, index).map(|x| x.0)
    }
    fn get64_with_ctx(
        &self,
        ctx: LongValuesContext,
        index: i64,
    ) -> Result<(i64, LongValuesContext)>;
}

pub struct EmptyLongValues;

impl LongValues for EmptyLongValues {
    fn get64_with_ctx(
        &self,
        ctx: LongValuesContext,
        _index: i64,
    ) -> Result<(i64, LongValuesContext)> {
        Ok((0, ctx))
    }
}

impl NumericDocValues for EmptyLongValues {
    fn get_with_ctx(
        &self,
        ctx: NumericDocValuesContext,
        _doc_id: DocId,
    ) -> Result<(i64, NumericDocValuesContext)> {
        Ok((0, ctx))
    }
}

pub struct IdentityLongValues;

impl LongValues for IdentityLongValues {
    fn get64_with_ctx(
        &self,
        ctx: LongValuesContext,
        index: i64,
    ) -> Result<(i64, LongValuesContext)> {
        Ok((index, ctx))
    }
}

impl NumericDocValues for IdentityLongValues {
    fn get_with_ctx(
        &self,
        ctx: NumericDocValuesContext,
        doc_id: DocId,
    ) -> Result<(i64, NumericDocValuesContext)> {
        Ok((doc_id as i64, ctx))
    }
}

pub struct LiveLongValues {
    live: LiveBitsEnum,
    constant: i64,
}

impl LiveLongValues {
    pub fn new(live: LiveBitsEnum, constant: i64) -> Self {
        LiveLongValues { live, constant }
    }
}

impl LongValues for LiveLongValues {
    fn get64_with_ctx(
        &self,
        ctx: LongValuesContext,
        index: i64,
    ) -> Result<(i64, LongValuesContext)> {
        let (bitwise, ctx) = self.live.get_with_ctx(ctx, index as usize)?;
        Ok((if bitwise { self.constant } else { 0 }, ctx))
    }
}

impl NumericDocValues for LiveLongValues {
    fn get_with_ctx(
        &self,
        ctx: NumericDocValuesContext,
        doc_id: DocId,
    ) -> Result<(i64, NumericDocValuesContext)> {
        LongValues::get64_with_ctx(self, ctx, i64::from(doc_id))
    }
}

pub struct DeltaLongValues {
    values: DirectPackedReader,
    delta: i64,
}

impl DeltaLongValues {
    pub fn new(values: DirectPackedReader, delta: i64) -> Self {
        DeltaLongValues { values, delta }
    }
}

impl LongValues for DeltaLongValues {
    fn get64_with_ctx(
        &self,
        ctx: LongValuesContext,
        index: i64,
    ) -> Result<(i64, LongValuesContext)> {
        let (packed, ctx) = self.values.get64_with_ctx(ctx, index)?;
        Ok((self.delta + packed, ctx))
    }
}

impl NumericDocValues for DeltaLongValues {
    fn get_with_ctx(
        &self,
        ctx: NumericDocValuesContext,
        doc_id: DocId,
    ) -> Result<(i64, NumericDocValuesContext)> {
        LongValues::get64_with_ctx(self, ctx, i64::from(doc_id))
    }
}

pub struct GcdLongValues {
    quotient_reader: DirectPackedReader,
    base: i64,
    mult: i64,
}

impl GcdLongValues {
    pub fn new(quotient_reader: DirectPackedReader, base: i64, mult: i64) -> Self {
        GcdLongValues {
            quotient_reader,
            base,
            mult,
        }
    }
}

impl LongValues for GcdLongValues {
    fn get64_with_ctx(
        &self,
        ctx: LongValuesContext,
        index: i64,
    ) -> Result<(i64, LongValuesContext)> {
        let (val, ctx) = self.quotient_reader.get64_with_ctx(ctx, index)?;
        Ok((self.base + self.mult * val, ctx))
    }
}

impl NumericDocValues for GcdLongValues {
    fn get_with_ctx(
        &self,
        ctx: NumericDocValuesContext,
        doc_id: DocId,
    ) -> Result<(i64, NumericDocValuesContext)> {
        LongValues::get64_with_ctx(self, ctx, i64::from(doc_id))
    }
}

pub struct TableLongValues {
    ords: DirectPackedReader,
    table: Vec<i64>,
}

impl TableLongValues {
    pub fn new(ords: DirectPackedReader, table: Vec<i64>) -> TableLongValues {
        TableLongValues { ords, table }
    }
}

impl LongValues for TableLongValues {
    fn get64_with_ctx(
        &self,
        ctx: LongValuesContext,
        index: i64,
    ) -> Result<(i64, LongValuesContext)> {
        let (val, ctx) = self.ords.get64_with_ctx(ctx, index)?;
        Ok((self.table[val as usize], ctx))
    }
}

impl NumericDocValues for TableLongValues {
    fn get_with_ctx(
        &self,
        ctx: NumericDocValuesContext,
        doc_id: DocId,
    ) -> Result<(i64, NumericDocValuesContext)> {
        LongValues::get64_with_ctx(self, ctx, i64::from(doc_id))
    }
}
