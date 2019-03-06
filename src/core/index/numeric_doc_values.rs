use core::util::{BitsContext, DocId};
use error::Result;

use std::sync::Arc;

pub type NumericDocValuesContext = BitsContext;

pub trait NumericDocValues: Send + Sync {
    fn get_with_ctx(
        &self,
        ctx: NumericDocValuesContext,
        doc_id: DocId,
    ) -> Result<(i64, NumericDocValuesContext)>;

    fn get(&self, doc_id: DocId) -> Result<i64> {
        self.get_with_ctx(None, doc_id).map(|x| x.0)
    }
}

pub type NumericDocValuesRef = Arc<NumericDocValues>;
