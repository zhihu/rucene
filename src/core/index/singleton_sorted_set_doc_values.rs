use core::index::term::TermIterator;
use core::index::RandomAccessOrds;
use core::index::SortedDocValues;
use core::index::SortedSetDocValues;
use core::index::NO_MORE_ORDS;
use core::util::DocId;
use error::Result;

pub struct SingletonSortedSetDocValues {
    dv_in: Box<SortedDocValues>,
    current_ord: i64,
    ord: i64,
}

impl SingletonSortedSetDocValues {
    pub fn new(dv_in: Box<SortedDocValues>) -> Self {
        SingletonSortedSetDocValues {
            dv_in,
            current_ord: 0,
            ord: 0,
        }
    }
    pub fn get_sorted_doc_values(&self) -> &SortedDocValues {
        self.dv_in.as_ref()
    }
}

impl RandomAccessOrds for SingletonSortedSetDocValues {
    fn ord_at(&mut self, _index: i32) -> Result<i64> {
        Ok(self.ord)
    }

    fn cardinality(&self) -> i32 {
        ((self.ord as u64 >> 63) as i32) ^ 1
    }
}

impl SortedSetDocValues for SingletonSortedSetDocValues {
    fn set_document(&mut self, doc: DocId) -> Result<()> {
        let v = i64::from(self.dv_in.get_ord(doc)?);
        self.current_ord = v;
        self.ord = v;
        Ok(())
    }

    fn next_ord(&mut self) -> Result<i64> {
        let v = self.current_ord;
        self.current_ord = NO_MORE_ORDS;
        Ok(v)
    }

    fn lookup_ord(&mut self, ord: i64) -> Result<Vec<u8>> {
        // cast is ok: single-valued cannot exceed i32::MAX
        self.dv_in.lookup_ord(ord as i32)
    }

    fn get_value_count(&self) -> usize {
        self.dv_in.get_value_count()
    }

    fn lookup_term(&mut self, key: &[u8]) -> Result<i64> {
        let val = self.dv_in.lookup_term(key)?;
        Ok(i64::from(val))
    }

    fn term_iterator<'a, 'b: 'a>(&'b mut self) -> Result<Box<TermIterator + 'a>> {
        self.dv_in.term_iterator()
    }
}
