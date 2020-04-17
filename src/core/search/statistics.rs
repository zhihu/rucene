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

use core::util::DocId;

/// Contains statistics for a collection (field)
#[derive(Clone)]
pub struct CollectionStatistics {
    pub field: String,
    pub doc_base: DocId,
    pub max_doc: i64,
    pub doc_count: i64,
    pub sum_total_term_freq: i64,
    pub sum_doc_freq: i64,
}

impl CollectionStatistics {
    pub fn new(
        field: String,
        doc_base: DocId,
        max_doc: i64,
        doc_count: i64,
        sum_total_term_freq: i64,
        sum_doc_freq: i64,
    ) -> CollectionStatistics {
        debug_assert!(max_doc >= 0);
        debug_assert!(doc_count >= -1 && doc_count <= max_doc); // #docs with field must be <= #docs
        debug_assert!(sum_doc_freq == -1 || sum_doc_freq >= doc_count); // #postings must be >= #docs with field
        debug_assert!(sum_total_term_freq == -1 || sum_total_term_freq >= sum_doc_freq); // #positions must be >= #postings
        CollectionStatistics {
            field,
            doc_base,
            max_doc,
            doc_count,
            sum_total_term_freq,
            sum_doc_freq,
        }
    }
}

/// Contains statistics for a specific term
pub struct TermStatistics {
    pub term: Vec<u8>,
    pub doc_freq: i64,
    pub total_term_freq: i64,
}

impl TermStatistics {
    pub fn new(term: Vec<u8>, doc_freq: i64, total_term_freq: i64) -> TermStatistics {
        debug_assert!(doc_freq >= 0);
        debug_assert!(total_term_freq == -1 || total_term_freq >= doc_freq);

        TermStatistics {
            term,
            doc_freq,
            total_term_freq,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::string::String;

    #[test]
    fn test_collection_statistics() {
        let collection_statistics =
            CollectionStatistics::new(String::from("hello"), 0, 25, 10, 14, 13);
        assert_eq!(collection_statistics.field, "hello");
        assert_eq!(collection_statistics.max_doc, 25);
        assert_eq!(collection_statistics.doc_count, 10);
        assert_eq!(collection_statistics.sum_total_term_freq, 14);
        assert_eq!(collection_statistics.sum_doc_freq, 13);
    }

    #[test]
    fn test_term_statistics() {
        let mut v: Vec<u8> = Vec::new();
        v.push(1);
        let term_statistics = TermStatistics::new(v, 1, 1);
        assert_eq!(term_statistics.term[0], 1);
        assert_eq!(term_statistics.doc_freq, 1);
        assert_eq!(term_statistics.total_term_freq, 1);
    }
}
