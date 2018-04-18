use core::index::NumericDocValues;
use core::util::Bits;
use core::util::DocId;
use error::Result;

/// Abstraction over an array of longs.
pub trait LongValues: NumericDocValues {
    fn get64(&mut self, index: i64) -> Result<i64>;
}

pub struct EmptyLongValues;

impl LongValues for EmptyLongValues {
    fn get64(&mut self, _index: i64) -> Result<i64> {
        Ok(0)
    }
}

impl NumericDocValues for EmptyLongValues {
    fn get(&mut self, _doc_id: DocId) -> Result<i64> {
        Ok(0)
    }
}

pub struct LiveLongValues {
    live: Bits,
    constant: i64,
}

impl LiveLongValues {
    pub fn new(live: Bits, constant: i64) -> Self {
        LiveLongValues { live, constant }
    }
}

impl LongValues for LiveLongValues {
    fn get64(&mut self, index: i64) -> Result<i64> {
        let bitwise = self.live.get(index as usize)?;
        Ok(if bitwise { self.constant } else { 0 })
    }
}

impl NumericDocValues for LiveLongValues {
    fn get(&mut self, doc_id: DocId) -> Result<i64> {
        LongValues::get64(self, i64::from(doc_id))
    }
}

pub struct DeltaLongValues {
    values: Box<LongValues>,
    delta: i64,
}

impl DeltaLongValues {
    pub fn new(values: Box<LongValues>, delta: i64) -> Self {
        DeltaLongValues { values, delta }
    }
}

impl LongValues for DeltaLongValues {
    fn get64(&mut self, index: i64) -> Result<i64> {
        let packed = self.values.get64(index)?;
        Ok(self.delta + packed)
    }
}

impl NumericDocValues for DeltaLongValues {
    fn get(&mut self, doc_id: DocId) -> Result<i64> {
        LongValues::get64(self, i64::from(doc_id))
    }
}

pub struct GcdLongValues {
    quotient_reader: Box<LongValues>,
    base: i64,
    mult: i64,
}

impl GcdLongValues {
    pub fn new(quotient_reader: Box<LongValues>, base: i64, mult: i64) -> Self {
        GcdLongValues {
            quotient_reader,
            base,
            mult,
        }
    }
}

impl LongValues for GcdLongValues {
    fn get64(&mut self, index: i64) -> Result<i64> {
        let val = self.quotient_reader.get64(index)?;
        Ok(self.base + self.mult * val)
    }
}

impl NumericDocValues for GcdLongValues {
    fn get(&mut self, doc_id: DocId) -> Result<i64> {
        LongValues::get64(self, i64::from(doc_id))
    }
}

pub struct TableLongValues {
    ords: Box<LongValues>,
    table: Vec<i64>,
}

impl TableLongValues {
    pub fn new(ords: Box<LongValues>, table: Vec<i64>) -> TableLongValues {
        TableLongValues { ords, table }
    }
}

impl LongValues for TableLongValues {
    fn get64(&mut self, index: i64) -> Result<i64> {
        let val = self.ords.get64(index)?;
        Ok(self.table[val as usize])
    }
}

impl NumericDocValues for TableLongValues {
    fn get(&mut self, doc_id: DocId) -> Result<i64> {
        LongValues::get64(self, i64::from(doc_id))
    }
}
