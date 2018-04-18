use core::index::{DocValuesType, IndexOptions};
use std::fmt;

#[derive(Clone, Default, PartialEq, Hash, Serialize, Debug)]
pub struct FieldType {
    pub stored: bool,
    pub tokenized: bool,
    pub store_term_vectors: bool,
    pub store_term_vector_offsets: bool,
    pub store_term_vector_positions: bool,
    pub store_term_vector_payloads: bool,
    pub omit_norms: bool,
    pub index_options: IndexOptions,
    pub doc_values_type: DocValuesType,
}

impl FieldType {
    #[allow(too_many_arguments)]
    pub fn new(
        stored: bool,
        tokenized: bool,
        store_term_vectors: bool,
        store_term_vector_offsets: bool,
        store_term_vector_positions: bool,
        store_term_vector_payloads: bool,
        omit_norms: bool,
        index_options: IndexOptions,
        doc_values_type: DocValuesType,
    ) -> FieldType {
        FieldType {
            stored,
            tokenized,
            store_term_vectors,
            store_term_vector_offsets,
            store_term_vector_positions,
            store_term_vector_payloads,
            omit_norms,
            index_options,
            doc_values_type,
        }
    }

    pub fn stored(&self) -> bool {
        self.stored
    }

    pub fn store_term_vectors(&self) -> bool {
        self.store_term_vectors
    }

    pub fn store_term_vector_offsets(&self) -> bool {
        self.store_term_vector_offsets
    }

    pub fn store_term_vector_positions(&self) -> bool {
        self.store_term_vector_positions
    }

    pub fn store_term_vector_payloads(&self) -> bool {
        self.store_term_vector_payloads
    }

    pub fn omit_norms(&self) -> bool {
        self.omit_norms
    }

    pub fn index_options(&self) -> &IndexOptions {
        &self.index_options
    }

    pub fn doc_values_type(&self) -> &DocValuesType {
        &self.doc_values_type
    }

    pub fn set_index_options(&mut self, value: IndexOptions) {
        self.index_options = value;
    }

    pub fn set_doc_values_type(&mut self, value: DocValuesType) {
        self.doc_values_type = value;
    }

    pub fn tokenized(&self) -> bool {
        self.tokenized
    }
}

impl fmt::Display for FieldType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Ok(s) = ::serde_json::to_string_pretty(self) {
            write!(f, "{}", s)?;
        }

        Ok(())
    }
}

pub const NUMERIC_DOC_VALUES_FIELD_TYPE: FieldType = FieldType {
    stored: false,
    tokenized: false,
    store_term_vectors: false,
    store_term_vector_offsets: false,
    store_term_vector_positions: false,
    store_term_vector_payloads: false,
    omit_norms: false,
    index_options: IndexOptions::Null,
    doc_values_type: DocValuesType::Numeric,
};

pub const SORTED_NUMERIC_DOC_VALUES_FIELD_TYPE: FieldType = FieldType {
    stored: false,
    tokenized: false,
    store_term_vectors: false,
    store_term_vector_offsets: false,
    store_term_vector_positions: false,
    store_term_vector_payloads: false,
    omit_norms: false,
    index_options: IndexOptions::Null,
    doc_values_type: DocValuesType::SortedNumeric,
};

pub const BINARY_DOC_VALUES_FIELD_TYPE: FieldType = FieldType {
    stored: false,
    tokenized: false,
    store_term_vectors: false,
    store_term_vector_offsets: false,
    store_term_vector_positions: false,
    store_term_vector_payloads: false,
    omit_norms: false,
    index_options: IndexOptions::Null,
    doc_values_type: DocValuesType::Binary,
};

pub const SORTED_SET_DOC_VALUES_FIELD_TYPE: FieldType = FieldType {
    stored: false,
    tokenized: false,
    store_term_vectors: false,
    store_term_vector_offsets: false,
    store_term_vector_positions: false,
    store_term_vector_payloads: false,
    omit_norms: false,
    index_options: IndexOptions::Null,
    doc_values_type: DocValuesType::SortedSet,
};
