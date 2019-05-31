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

use std::fmt;

use core::index::{DocValuesType, IndexOptions, MAX_DIMENSIONS, MAX_NUM_BYTES};

use error::{ErrorKind::IllegalArgument, Result};

#[derive(Clone, PartialEq, Hash, Serialize, Debug)]
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
    pub dimension_count: u32,
    pub dimension_num_bytes: u32,
}

impl Default for FieldType {
    fn default() -> Self {
        FieldType {
            stored: false,
            tokenized: true,
            store_term_vectors: false,
            store_term_vector_offsets: false,
            store_term_vector_positions: false,
            store_term_vector_payloads: false,
            omit_norms: false,
            index_options: IndexOptions::Null,
            doc_values_type: DocValuesType::Null,
            dimension_count: 0,
            dimension_num_bytes: 0,
        }
    }
}

impl FieldType {
    #[allow(clippy::too_many_arguments)]
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
        dimension_count: u32,
        dimension_num_bytes: u32,
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
            dimension_count,
            dimension_num_bytes,
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

    pub fn set_store_term_vector_offsets(&mut self, v: bool) {
        if v {
            self.store_term_vectors = true;
        }
        self.store_term_vector_offsets = v;
    }

    pub fn store_term_vector_positions(&self) -> bool {
        self.store_term_vector_positions
    }

    pub fn set_store_term_vector_positions(&mut self, v: bool) {
        if v {
            self.store_term_vectors = true;
        }
        self.store_term_vector_positions = v;
    }

    pub fn store_term_vector_payloads(&self) -> bool {
        self.store_term_vector_payloads
    }

    pub fn set_store_term_vector_payloads(&mut self, v: bool) {
        if v {
            self.store_term_vectors = true;
        }
        self.store_term_vector_payloads = v;
    }

    pub fn omit_norms(&self) -> bool {
        self.omit_norms
    }

    pub fn index_options(&self) -> IndexOptions {
        self.index_options
    }

    pub fn doc_values_type(&self) -> DocValuesType {
        self.doc_values_type
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

    pub fn set_dimensions(&mut self, dimension_count: u32, dimension_num_bytes: u32) -> Result<()> {
        if dimension_count > MAX_DIMENSIONS {
            bail!(IllegalArgument(format!(
                "dimension_count must be <={}",
                MAX_DIMENSIONS
            )));
        }

        if dimension_num_bytes > MAX_NUM_BYTES {
            bail!(IllegalArgument(format!(
                "dimension_num_bytes must be <={}",
                MAX_NUM_BYTES
            )));
        }

        if dimension_count == 0 && dimension_num_bytes != 0 {
            bail!(IllegalArgument(format!(
                "when dimension_count is 0, dimension_num_bytes must be 0, got {}",
                dimension_num_bytes
            )));
        } else if dimension_num_bytes == 0 && dimension_count != 0 {
            bail!(IllegalArgument(format!(
                "when dimension_num_bytes is 0, dimension_count must be 0, got {}",
                dimension_count
            )));
        }

        self.dimension_count = dimension_count;
        self.dimension_num_bytes = dimension_num_bytes;

        Ok(())
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
    dimension_count: 0,
    dimension_num_bytes: 0,
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
    dimension_count: 0,
    dimension_num_bytes: 0,
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
    dimension_count: 0,
    dimension_num_bytes: 0,
};

pub const SORTED_DOC_VALUES_FIELD_TYPE: FieldType = FieldType {
    stored: false,
    tokenized: false,
    store_term_vectors: false,
    store_term_vector_offsets: false,
    store_term_vector_positions: false,
    store_term_vector_payloads: false,
    omit_norms: false,
    index_options: IndexOptions::Null,
    doc_values_type: DocValuesType::Sorted,
    dimension_count: 0,
    dimension_num_bytes: 0,
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
    dimension_count: 0,
    dimension_num_bytes: 0,
};
