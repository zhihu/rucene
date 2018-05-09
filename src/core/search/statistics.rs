#[derive(Clone)]
pub struct CollectionStatistics {
    pub field: String,
    pub max_doc: i64,
    pub doc_count: i64,
    pub sum_total_term_freq: i64,
    pub sum_doc_freq: i64,
}

impl CollectionStatistics {
    pub fn new(
        field: String,
        max_doc: i64,
        doc_count: i64,
        sum_total_term_freq: i64,
        sum_doc_freq: i64,
    ) -> CollectionStatistics {
        CollectionStatistics {
            field,
            max_doc,
            doc_count,
            sum_total_term_freq,
            sum_doc_freq,
        }
    }
}

pub struct TermStatistics {
    pub term: Vec<u8>,
    pub doc_freq: i64,
    pub total_term_freq: i64,
}

impl TermStatistics {
    pub fn new(term: Vec<u8>, doc_freq: i64, total_term_freq: i64) -> TermStatistics {
        assert!(doc_freq >= 0);
        assert!(total_term_freq == -1 || total_term_freq >= doc_freq);
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
        let collection_statistics = CollectionStatistics::new(String::from("hello"), 5, 10, 4, 3);
        assert_eq!(collection_statistics.field, "hello");
        assert_eq!(collection_statistics.max_doc, 5);
        assert_eq!(collection_statistics.doc_count, 10);
        assert_eq!(collection_statistics.sum_total_term_freq, 4);
        assert_eq!(collection_statistics.sum_doc_freq, 3);
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
