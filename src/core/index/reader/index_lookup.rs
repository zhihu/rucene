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

use core::codec::{Fields, TermIterator, Terms};
use core::codec::{PostingIterator, PostingIteratorFlags};
use core::doc::Term;
use core::search::{Payload, NO_MORE_DOCS};
use core::util::DocId;

use error::{ErrorKind::IllegalState, Result};

use std::collections::hash_map::HashMap;

#[derive(Debug, Serialize, Deserialize)]
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

/// Holds all information on a particular term in a field.
pub struct LeafIndexFieldTerm<T: PostingIterator> {
    postings: Option<T>,
    flags: u16,
    iterator: LeafPositionIterator,
    #[allow(dead_code)]
    identifier: Term,
    freq: i32,
}

impl<T: PostingIterator> LeafIndexFieldTerm<T> {
    pub fn new<TI: TermIterator<Postings = T>, Tm: Terms<Iterator = TI>, F: Fields<Terms = Tm>>(
        term: &str,
        field_name: &str,
        flags: u16,
        doc_id: DocId,
        fields: &F,
    ) -> Result<Self> {
        let identifier = Term::new(field_name.to_string(), term.as_bytes().to_vec());

        if let Some(terms) = fields.terms(identifier.field())? {
            let mut terms_iterator = terms.iterator()?;

            let (postings, freq) = if terms_iterator.seek_exact(identifier.bytes.as_slice())? {
                let mut posting = terms_iterator.postings_with_flags(flags)?;
                let mut current_doc_pos = posting.doc_id();
                if current_doc_pos < doc_id {
                    current_doc_pos = posting.advance(doc_id)?;
                }
                let freq = if current_doc_pos == doc_id {
                    posting.freq()?
                } else {
                    0
                };
                (Some(posting), freq)
            } else {
                (None, 0)
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
            bail!(IllegalState(format!(
                "Terms {} for doc {} - field '{}' must not be none!",
                term, doc_id, field_name
            )));
        }
    }

    pub fn tf(&self) -> i32 {
        self.freq
    }

    fn current_doc(&self) -> DocId {
        if let Some(ref postings) = self.postings {
            postings.doc_id()
        } else {
            NO_MORE_DOCS
        }
    }

    pub fn set_document(&mut self, doc_id: i32) -> Result<()> {
        let mut current_doc_pos = self.current_doc();
        if current_doc_pos < doc_id {
            current_doc_pos = self.postings.as_mut().unwrap().advance(doc_id)?;
        }
        if current_doc_pos == doc_id && doc_id < NO_MORE_DOCS {
            self.freq = self.postings.as_ref().unwrap().freq()?;
        } else {
            self.freq = 0;
        }
        self.next_doc();
        Ok(())
    }

    pub fn validate_flags(&self, flags2: u16) -> Result<()> {
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
        let term_pos = if let Some(ref mut postings) = self.postings {
            TermPosition {
                position: postings.next_position()?,
                start_offset: postings.start_offset()?,
                end_offset: postings.end_offset()?,
                payload: postings.payload()?,
            }
        } else {
            TermPosition {
                position: -1,
                start_offset: -1,
                end_offset: -1,
                payload: Vec::with_capacity(0),
            }
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

pub struct LeafIndexField<T: Fields> {
    terms: HashMap<
        String,
        LeafIndexFieldTerm<<<T::Terms as Terms>::Iterator as TermIterator>::Postings>,
    >,
    field_name: String,
    doc_id: DocId,
    fields: T,
}

///
// Script interface to all information regarding a field.
//
impl<T: Fields + Clone> LeafIndexField<T> {
    pub fn new(field_name: &str, doc_id: DocId, fields: T) -> Self {
        LeafIndexField {
            terms: HashMap::new(),
            field_name: String::from(field_name),
            doc_id,
            fields,
        }
    }

    pub fn get(
        &mut self,
        key: &str,
    ) -> Result<&mut LeafIndexFieldTerm<<<T::Terms as Terms>::Iterator as TermIterator>::Postings>>
    {
        self.get_with_flags(key, PostingIteratorFlags::FREQS)
    }

    // TODO: might be good to get the field lengths here somewhere?

    // Returns a TermInfo object that can be used to access information on
    // specific terms. flags can be set as described in TermInfo.
    // TODO: here might be potential for running time improvement? If we knew in
    // advance which terms are requested, we could provide an array which the
    // user could then iterate over.
    //
    pub fn get_with_flags(
        &mut self,
        key: &str,
        flags: u16,
    ) -> Result<&mut LeafIndexFieldTerm<<<T::Terms as Terms>::Iterator as TermIterator>::Postings>>
    {
        if !self.terms.contains_key(key) {
            let index_field_term =
                LeafIndexFieldTerm::new(key, &self.field_name, flags, self.doc_id, &self.fields)?;
            index_field_term.validate_flags(flags)?;
            self.terms.insert(String::from(key), index_field_term);
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

pub struct LeafIndexLookup<T: Fields> {
    pub fields: T,
    pub doc_id: DocId,
    index_fields: HashMap<String, LeafIndexField<T>>,
    #[allow(dead_code)]
    num_docs: i32,
    #[allow(dead_code)]
    max_doc: i32,
    #[allow(dead_code)]
    num_deleted_docs: i32,
}

impl<T: Fields + Clone> LeafIndexLookup<T> {
    pub fn new(fields: T) -> LeafIndexLookup<T> {
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
    pub fn get(&mut self, key: &str) -> &mut LeafIndexField<T> {
        if !self.index_fields.contains_key(key) {
            let index_field = LeafIndexField::new(key, self.doc_id, self.fields.clone());
            self.index_fields.insert(String::from(key), index_field);
        }
        self.index_fields.get_mut(key).unwrap()
    }
}
