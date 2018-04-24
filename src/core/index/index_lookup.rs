use std::collections::hash_map::HashMap;
use std::sync::Arc;

use core::codec::FieldsProducerRef;
use core::index::Term;
use core::search::posting_iterator::*;
use core::search::{DocIterator, Payload, NO_MORE_DOCS};
use core::util::DocId;
use error::*;

#[derive(Debug)]
pub struct TermPosition {
    pub position: i32,
    pub start_offset: i32,
    pub end_offset: i32,
    pub payload: Payload,
}

impl Default for TermPosition {
    fn default() -> Self {
        TermPosition::new()
    }
}

impl TermPosition {
    pub fn new() -> TermPosition {
        TermPosition {
            position: -1,
            start_offset: -1,
            end_offset: -1,
            payload: Payload::with_capacity(0),
        }
    }

    pub fn payload_as_string(&mut self) -> String {
        if self.payload.is_empty() {
            unimplemented!()
        } else {
            unimplemented!()
        }
    }

    pub fn payload_as_float(&mut self, default: f32) -> f32 {
        if self.payload.is_empty() {
            default
        } else {
            unimplemented!()
        }
    }

    pub fn payload_as_int(&mut self, default: i32) -> i32 {
        if self.payload.is_empty() {
            default
        } else {
            unimplemented!()
        }
    }
}

#[derive(Clone)]
pub struct EmptyPostingIterator {
    doc_id: DocId,
}

impl Default for EmptyPostingIterator {
    fn default() -> Self {
        EmptyPostingIterator::new()
    }
}

impl EmptyPostingIterator {
    pub fn new() -> EmptyPostingIterator {
        EmptyPostingIterator { doc_id: -1 }
    }
}

impl DocIterator for EmptyPostingIterator {
    fn doc_id(&self) -> DocId {
        self.doc_id
    }

    fn next(&mut self) -> Result<DocId> {
        self.doc_id = NO_MORE_DOCS;
        Ok(NO_MORE_DOCS)
    }

    fn advance(&mut self, _target: DocId) -> Result<DocId> {
        self.doc_id = NO_MORE_DOCS;
        Ok(NO_MORE_DOCS)
    }

    fn cost(&self) -> usize {
        0usize
    }
}

impl PostingIterator for EmptyPostingIterator {
    fn clone_as_doc_iterator(&self) -> Result<Box<DocIterator>> {
        unimplemented!()
    }

    fn freq(&self) -> Result<i32> {
        Ok(1)
    }

    fn next_position(&mut self) -> Result<i32> {
        Ok(-1)
    }

    fn start_offset(&self) -> Result<i32> {
        Ok(-1)
    }

    fn end_offset(&self) -> Result<i32> {
        Ok(-1)
    }

    fn payload(&self) -> Result<Payload> {
        Ok(Payload::new())
    }
}

pub const FLAG_OFFSETS: i32 = 2;
pub const FLAG_PAYLOADS: i32 = 4;
pub const FLAG_FREQUENCIES: i32 = 8;
pub const FLAG_POSITIONS: i32 = 16;
pub const FLAG_CACHE: i32 = 32;

pub type LeafIndexFieldRef = LeafIndexField;
pub type LeafIndexFieldTermRef = LeafIndexFieldTerm;

///
// Holds all information on a particular term in a field.
//
pub struct LeafIndexFieldTerm {
    postings: Box<PostingIterator>,
    flags: i32,
    iterator: LeafPositionIterator,
    #[allow(dead_code)]
    identifier: Term,
    freq: i32,
}

impl LeafIndexFieldTerm {
    pub fn new(
        term: &str,
        field_name: &str,
        flags: i32,
        doc_id: DocId,
        fields: &FieldsProducerRef,
    ) -> Result<LeafIndexFieldTerm> {
        let identifier = Term::new(field_name.to_string(), term.as_bytes().to_vec());

        if let Some(terms) = fields.terms(identifier.field())? {
            let mut terms_iterator = terms.iterator()?;
            let lucene_flags = LeafIndexFieldTerm::convert_to_lucene_flags(flags);

            let mut postings = if terms_iterator
                .as_mut()
                .seek_exact(identifier.bytes.as_slice())?
            {
                terms_iterator.postings_with_flags(lucene_flags)?
            } else {
                Box::new(EmptyPostingIterator::new())
            };

            let mut current_doc_pos = postings.doc_id();
            if current_doc_pos < doc_id {
                current_doc_pos = postings.advance(doc_id)?;
            }
            let freq = if current_doc_pos == doc_id {
                postings.freq()?
            } else {
                0
            };

            let mut iterator = LeafPositionIterator::new();
            iterator.resetted = false;
            iterator.current_pos = 0;
            iterator.freq = freq;

            Ok(LeafIndexFieldTerm {
                postings,
                flags,
                iterator,
                identifier,
                freq,
            })
        } else {
            bail!("Terms for field '{}' must not be none!", identifier.field());
        }
    }

    pub fn convert_to_lucene_flags(flags: i32) -> i16 {
        let mut lucene_pos_flags = POSTING_ITERATOR_FLAG_NONE;
        if (flags & FLAG_FREQUENCIES) > 0 {
            lucene_pos_flags |= POSTING_ITERATOR_FLAG_FREQS;
        }
        if (flags & FLAG_POSITIONS) > 0 {
            lucene_pos_flags |= POSTING_ITERATOR_FLAG_POSITIONS;
        }
        if (flags & FLAG_PAYLOADS) > 0 {
            lucene_pos_flags |= POSTING_ITERATOR_FLAG_PAYLOADS;
        }
        if (flags & FLAG_OFFSETS) > 0 {
            lucene_pos_flags |= POSTING_ITERATOR_FLAG_OFFSETS;
        }

        lucene_pos_flags
    }

    pub fn tf(&self) -> i32 {
        self.freq
    }

    pub fn set_document(&mut self, doc_id: i32) -> Result<()> {
        let mut current_doc_pos = self.postings.doc_id();
        if current_doc_pos < doc_id {
            current_doc_pos = self.postings.advance(doc_id)?;
        }
        if current_doc_pos == doc_id {
            self.freq = self.postings.freq()?;
        } else {
            self.freq = 0;
        }
        self.next_doc();
        Ok(())
    }

    pub fn validate_flags(&self, flags2: i32) -> Result<()> {
        if (self.flags & flags2) < flags2 {
            panic!(
                "You must call get with all required flags! Instead of {} call {} once",
                flags2,
                flags2 | self.flags
            )
        } else {
            Ok(())
        }
    }

    pub fn has_next(&self) -> bool {
        self.iterator.current_pos < self.iterator.freq
    }

    pub fn next_pos(&mut self) -> Result<TermPosition> {
        let term_pos = TermPosition {
            position: self.postings.next_position()?,
            start_offset: self.postings.start_offset()?,
            end_offset: self.postings.end_offset()?,
            payload: self.postings.payload()?,
        };

        self.iterator.current_pos += 1;

        Ok(term_pos)
    }

    pub fn next_doc(&mut self) {
        self.iterator.resetted = false;
        self.iterator.current_pos = 0;
        self.iterator.freq = self.tf();
    }

    pub fn reset(&mut self) -> Result<()> {
        if self.iterator.resetted {
            panic!(
                "Cannot iterate twice! If you want to iterate more that once, add _CACHE \
                 explicitly."
            )
        }

        self.iterator.resetted = true;
        Ok(())
    }
}

pub struct LeafPositionIterator {
    resetted: bool,
    freq: i32,
    current_pos: i32,
}

impl Default for LeafPositionIterator {
    fn default() -> Self {
        LeafPositionIterator::new()
    }
}

impl LeafPositionIterator {
    pub fn new() -> LeafPositionIterator {
        LeafPositionIterator {
            resetted: false,
            freq: -1,
            current_pos: 0,
        }
    }
}

pub struct LeafIndexField {
    terms: HashMap<String, LeafIndexFieldTermRef>,
    field_name: String,
    doc_id: DocId,
    fields: FieldsProducerRef,
}

///
// Script interface to all information regarding a field.
//
impl LeafIndexField {
    pub fn new(field_name: &str, doc_id: DocId, fields: FieldsProducerRef) -> LeafIndexField {
        LeafIndexField {
            terms: HashMap::new(),
            field_name: String::from(field_name),
            doc_id,
            fields,
        }
    }

    pub fn get(&mut self, key: &str) -> Result<&mut LeafIndexFieldTermRef> {
        self.get_with_flags(key, FLAG_FREQUENCIES)
    }

    // TODO: might be good to get the field lengths here somewhere?

    // Returns a TermInfo object that can be used to access information on
    // specific terms. flags can be set as described in TermInfo.
    // TODO: here might be potential for running time improvement? If we knew in
    // advance which terms are requested, we could provide an array which the
    // user could then iterate over.
    //
    pub fn get_with_flags(&mut self, key: &str, flags: i32) -> Result<&mut LeafIndexFieldTermRef> {
        if !self.terms.contains_key(key) {
            let index_field_term =
                LeafIndexFieldTerm::new(key, &self.field_name, flags, self.doc_id, &self.fields)?;
            index_field_term.validate_flags(flags)?;
            let index_field_term_ref = index_field_term;
            self.terms
                .insert(String::from(key), index_field_term_ref);
        }
        let index_field_term_ref = self.terms.get_mut(key).unwrap();
        index_field_term_ref.validate_flags(flags)?;
        Ok(index_field_term_ref)
    }

    pub fn set_doc_id_in_terms(&mut self, doc_id: DocId) -> Result<()> {
        for ti in self.terms.values_mut() {
            ti.set_document(doc_id)?;
        }
        Ok(())
    }
}

pub struct LeafIndexLookup {
    pub fields: FieldsProducerRef,
    pub doc_id: DocId,
    index_fields: HashMap<String, LeafIndexFieldRef>,
    #[allow(dead_code)]
    num_docs: i32,
    #[allow(dead_code)]
    max_doc: i32,
    #[allow(dead_code)]
    num_deleted_docs: i32,
}

impl LeafIndexLookup {
    pub fn new(fields: FieldsProducerRef) -> LeafIndexLookup {
        LeafIndexLookup {
            fields,
            doc_id: -1,
            index_fields: HashMap::new(),
            num_docs: -1,
            max_doc: -1,
            num_deleted_docs: -1,
        }
    }

    pub fn set_document(&mut self, doc_id: DocId) -> Result<()> {
        if self.doc_id == doc_id {
            return Ok(());
        }

        // We assume that docs are processed in ascending order of id. If this
        // is not the case, we would have to re initialize all posting lists in
        // IndexFieldTerm. TODO: Instead of assert we could also call
        // setReaderInFields(); here?
        if self.doc_id > doc_id {
            // This might happen if the same SearchLookup is used in different
            // phases, such as score and fetch phase.
            // In this case we do not want to re initialize posting list etc.
            // because we do not even know if term and field statistics will be
            // needed in this new phase.
            // Therefore we just remove all IndexFieldTerms.
            self.index_fields.clear();
        }

        self.doc_id = doc_id;
        self.set_next_doc_id_in_fields()
    }

    fn set_next_doc_id_in_fields(&mut self) -> Result<()> {
        for stat in self.index_fields.values_mut() {
            stat.set_doc_id_in_terms(self.doc_id)?;
        }
        Ok(())
    }
    // TODO: here might be potential for running time improvement? If we knew in
    // advance which terms are requested, we could provide an array which the
    // user could then iterate over.
    //
    pub fn get(&mut self, key: &str) -> &mut LeafIndexFieldRef {
        if !self.index_fields.contains_key(key) {
            let index_field = LeafIndexField::new(key, self.doc_id, Arc::clone(&self.fields));
            let index_field_ref = index_field;
            self.index_fields
                .insert(String::from(key), index_field_ref);
        }
        self.index_fields.get_mut(key).unwrap()
    }
}
