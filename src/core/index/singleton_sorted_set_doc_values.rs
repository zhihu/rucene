use core::index::term::TermIterator;
use core::index::{RandomAccessOrds, SortedDocValues, SortedSetDocValues,
                  SortedSetDocValuesContext, NO_MORE_ORDS};
use core::util::DocId;
use error::Result;

pub struct SingletonSortedSetDocValues {
    dv_in: Box<SortedDocValues>,
}

impl SingletonSortedSetDocValues {
    pub fn new(dv_in: Box<SortedDocValues>) -> Self {
        SingletonSortedSetDocValues { dv_in }
    }
    pub fn get_sorted_doc_values(&self) -> &SortedDocValues {
        self.dv_in.as_ref()
    }
}

impl RandomAccessOrds for SingletonSortedSetDocValues {
    fn ord_at(&self, ctx: &SortedSetDocValuesContext, _index: i32) -> Result<i64> {
        Ok(ctx.0)
    }

    fn cardinality(&self, ctx: &SortedSetDocValuesContext) -> i32 {
        ((ctx.0 as u64 >> 63) as i32) ^ 1
    }
}

impl SortedSetDocValues for SingletonSortedSetDocValues {
    fn set_document(&self, doc: DocId) -> Result<SortedSetDocValuesContext> {
        let v = i64::from(self.dv_in.get_ord(doc)?);
        Ok((v, v, v))
    }

    fn next_ord(&self, ctx: &mut SortedSetDocValuesContext) -> Result<i64> {
        let v = ctx.0;
        ctx.0 = NO_MORE_ORDS;
        Ok(v)
    }

    fn lookup_ord(&self, ord: i64) -> Result<Vec<u8>> {
        // cast is ok: single-valued cannot exceed i32::MAX
        self.dv_in.lookup_ord(ord as i32)
    }

    fn get_value_count(&self) -> usize {
        self.dv_in.get_value_count()
    }

    fn lookup_term(&self, key: &[u8]) -> Result<i64> {
        let val = self.dv_in.lookup_term(key)?;
        Ok(i64::from(val))
    }

    fn term_iterator<'a, 'b: 'a>(&'b self) -> Result<Box<TermIterator + 'a>> {
        self.dv_in.term_iterator()
    }
}
