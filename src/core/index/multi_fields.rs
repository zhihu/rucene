use std::collections::HashMap;

use core::codec::FieldsProducer;
use core::codec::FieldsProducerRef;
use core::index::field_info::Fields;
use core::index::multi_terms::MultiTerms;
use core::index::reader_slice::ReaderSlice;
use core::index::term::*;
use core::index::IndexReader;
use error::Result;
use std::sync::Arc;

pub struct MultiFields {
    subs: Vec<FieldsProducerRef>,
    #[allow(dead_code)]
    sub_slices: Vec<ReaderSlice>,
    // TODO use ConcurrentHashMap instead
    // because Rc<T> is not Thread safety
    // could we change Rc<Terms> to Arc<Terms>
    #[allow(dead_code)]
    terms: HashMap<String, TermsRef>,
}

fn fields(reader: &IndexReader) -> Result<FieldsProducerRef> {
    let leaves = reader.leaves();

    if leaves.len() == 1 {
        Ok(leaves[0].fields()?)
    } else {
        let mut fields: Vec<FieldsProducerRef> = Vec::with_capacity(leaves.len());
        let mut slices: Vec<ReaderSlice> = Vec::with_capacity(leaves.len());
        for leaf in leaves {
            fields.push(leaf.fields()?);
            slices.push(ReaderSlice::new(
                leaf.doc_base(),
                reader.max_doc(),
                (fields.len() - 1) as i32,
            ));
        }
        if fields.len() == 1 {
            Ok(fields[0].clone())
        } else {
            Ok(Arc::new(MultiFields::new(fields, slices)))
        }
    }
}

impl MultiFields {
    fn new(subs: Vec<FieldsProducerRef>, sub_slices: Vec<ReaderSlice>) -> MultiFields {
        MultiFields {
            subs,
            sub_slices,
            terms: HashMap::<String, TermsRef>::new(),
        }
    }

    pub fn get_terms(reader: &IndexReader, field: &str) -> Result<Option<TermsRef>> {
        fields(reader)?.terms(field)
    }
}

impl Fields for MultiFields {
    fn fields(&self) -> Vec<String> {
        unimplemented!();
    }

    fn terms(&self, field: &str) -> Result<Option<TermsRef>> {
        // TODO cache terms by field
        let mut subs2 = Vec::new();
        let mut slices2 = Vec::new();

        for i in 0..self.subs.len() {
            if let Some(terms) = self.subs[i].terms(field)? {
                subs2.push(terms.to_owned());
                slices2.push(self.sub_slices[i]);
            }
        }
        Ok(Some(Arc::new(MultiTerms::new(subs2, slices2)?)))
    }

    fn size(&self) -> usize {
        1 as usize
    }
}

impl FieldsProducer for MultiFields {
    fn check_integrity(&self) -> Result<()> {
        unimplemented!();
    }

    fn get_merge_instance(&self) -> Result<FieldsProducerRef> {
        unimplemented!();
    }
}
