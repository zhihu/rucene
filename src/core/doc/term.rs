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

use error::Result;
use std::cmp::Ordering;

/// A Term represents a word from text.  This is the unit of search.  It is
/// composed of two elements, the text of the word, as a string, and the name of
/// the field that the text occurred in.
///
/// Note that terms may represent more than words from text fields, but also
/// things like dates, email addresses, urls, etc.
#[derive(Clone, Debug, PartialEq, Hash, Eq)]
pub struct Term {
    pub field: String,
    pub bytes: Vec<u8>,
}

impl Term {
    /// Constructs a Term with the given field and bytes.
    /// <p>Note that a null field or null bytes value results in undefined
    /// behavior for most Lucene APIs that accept a Term parameter.
    ///
    /// <p>The provided BytesRef is copied when it is non null.
    pub fn new(field: String, bytes: Vec<u8>) -> Term {
        Term { field, bytes }
    }

    /// Returns the field of this term.   The field indicates
    /// the part of a document which this term came from.
    pub fn field(&self) -> &str {
        &self.field
    }

    /// Returns the text of this term.  In the case of words, this is simply the
    /// text of the word.  In the case of dates and other types, this is an
    /// encoding of the object as a string.
    pub fn text(&self) -> Result<String> {
        Ok(String::from_utf8(self.bytes.clone())?)
    }

    pub fn is_empty(&self) -> bool {
        self.field.is_empty() && self.bytes.is_empty()
    }

    pub fn copy_bytes(&mut self, bytes: &[u8]) {
        if self.bytes.len() != bytes.len() {
            self.bytes.resize(bytes.len(), 0);
        }
        self.bytes.copy_from_slice(bytes);
    }
}

impl PartialOrd for Term {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Term {
    fn cmp(&self, other: &Self) -> Ordering {
        let res = self.field.cmp(&other.field);
        if res == Ordering::Equal {
            self.bytes.cmp(&other.bytes)
        } else {
            res
        }
    }
}
