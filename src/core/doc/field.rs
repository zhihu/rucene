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

use core::analysis::{BinaryTokenStream, StringTokenStream, TokenStream};
use core::doc::{DocValuesType, IndexOptions};
use core::util::{ByteBlockPool, BytesRef, Numeric, VariantValue};

use error::ErrorKind::IllegalArgument;
use error::{ErrorKind, Result};
use std::fmt;

#[derive(Debug)]
pub struct Field {
    field_name: String,
    field_type: FieldType,
    field_data: Option<VariantValue>,
    boost: f32,
    token_stream: Option<Box<dyn TokenStream>>,
}

impl Field {
    pub fn new(
        field_name: String,
        field_type: FieldType,
        field_data: Option<VariantValue>,
        token_stream: Option<Box<dyn TokenStream>>,
    ) -> Field {
        let field_data = if let Some(data) = field_data {
            match data {
                VariantValue::Binary(b) => {
                    let bytes = b[..(ByteBlockPool::BYTE_BLOCK_SIZE - 2).min(b.len())].to_vec();
                    Some(VariantValue::Binary(bytes))
                }
                VariantValue::VString(vs) => {
                    let mut index = (ByteBlockPool::BYTE_BLOCK_SIZE - 2).min(vs.len());
                    while !vs.is_char_boundary(index) {
                        index -= 1;
                    }
                    let s = (&vs[..index]).to_string();
                    Some(VariantValue::VString(s))
                }
                _ => Some(data),
            }
        } else {
            None
        };

        Field {
            field_type,
            field_name,
            field_data,
            boost: 1.0_f32,
            token_stream,
        }
    }

    pub fn new_bytes(field_name: String, bytes: Vec<u8>, field_type: FieldType) -> Self {
        let bytes = bytes[..(ByteBlockPool::BYTE_BLOCK_SIZE - 2).min(bytes.len())].to_vec();
        Field {
            field_name,
            field_data: Some(VariantValue::Binary(bytes)),
            field_type,
            token_stream: None,
            boost: 1.0,
        }
    }

    pub fn set_boost(&mut self, boost: f32) {
        self.boost = boost;
    }

    pub fn set_field_data(&mut self, data: Option<VariantValue>) {
        self.field_data = data;
    }
}

impl Fieldable for Field {
    fn name(&self) -> &str {
        &self.field_name
    }

    fn field_type(&self) -> &FieldType {
        &self.field_type
    }

    fn boost(&self) -> f32 {
        self.boost
    }

    fn field_data(&self) -> Option<&VariantValue> {
        self.field_data.as_ref()
    }

    // TODO currently this function should only be called once per doc field
    fn token_stream(&mut self) -> Result<Box<dyn TokenStream>> {
        if self.token_stream.is_some() {
            return Ok(self.token_stream.take().unwrap());
        }

        debug_assert_ne!(self.field_type.index_options, IndexOptions::Null);

        if !self.field_type.tokenized {
            if let Some(ref field_data) = self.field_data {
                match field_data {
                    VariantValue::VString(s) => {
                        return Ok(Box::new(StringTokenStream::new(s.clone())));
                    }
                    VariantValue::Binary(b) => {
                        return Ok(Box::new(BinaryTokenStream::new(BytesRef::new(b.as_ref()))));
                    }
                    _ => bail!(ErrorKind::IllegalArgument(
                        "Non-Tokenized Fields must have a String value".into()
                    )),
                }
            }
        }

        bail!(ErrorKind::IllegalArgument(
            "Tokenized field's token_stream should not be None ".into()
        ))
    }

    fn binary_value(&self) -> Option<&[u8]> {
        self.field_data.as_ref().and_then(|f| f.get_binary())
    }

    fn string_value(&self) -> Option<&str> {
        self.field_data.as_ref().and_then(|f| f.get_string())
    }

    fn numeric_value(&self) -> Option<Numeric> {
        self.field_data.as_ref().and_then(|f| f.get_numeric())
    }
}

impl Clone for Field {
    fn clone(&self) -> Self {
        assert!(self.token_stream.is_none());
        Field {
            field_name: self.field_name.clone(),
            field_type: self.field_type.clone(),
            field_data: self.field_data.clone(),
            boost: self.boost,
            token_stream: None,
            // TODO, no used
        }
    }
}

/// Represents a single field for indexing.
///
/// `IndexWriter` consumes Vec<Fieldable> as a document.
pub trait Fieldable {
    fn name(&self) -> &str;
    fn field_type(&self) -> &FieldType;
    fn boost(&self) -> f32;
    fn field_data(&self) -> Option<&VariantValue>;
    fn token_stream(&mut self) -> Result<Box<dyn TokenStream>>;
    fn binary_value(&self) -> Option<&[u8]>;
    fn string_value(&self) -> Option<&str>;
    fn numeric_value(&self) -> Option<Numeric>;
}

impl<T: Fieldable + ?Sized> Fieldable for Box<T> {
    fn name(&self) -> &str {
        (**self).name()
    }
    fn field_type(&self) -> &FieldType {
        (**self).field_type()
    }
    fn boost(&self) -> f32 {
        (**self).boost()
    }
    fn field_data(&self) -> Option<&VariantValue> {
        (**self).field_data()
    }
    fn token_stream(&mut self) -> Result<Box<dyn TokenStream>> {
        (**self).token_stream()
    }
    fn binary_value(&self) -> Option<&[u8]> {
        (**self).binary_value()
    }
    fn string_value(&self) -> Option<&str> {
        (**self).string_value()
    }
    fn numeric_value(&self) -> Option<Numeric> {
        (**self).numeric_value()
    }
}

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

/// Maximum number of bytes for each dimension
pub const POINT_MAX_NUM_BYTES: u32 = 16;
/// Maximum number of dimensions
pub const POINT_MAX_DIMENSIONS: u32 = 8; // TODO should be replaced by BKDWriter.MAX_DIMS

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
        if dimension_count > POINT_MAX_DIMENSIONS {
            bail!(IllegalArgument(format!(
                "dimension_count must be <={}",
                POINT_MAX_DIMENSIONS
            )));
        }

        if dimension_num_bytes > POINT_MAX_NUM_BYTES {
            bail!(IllegalArgument(format!(
                "dimension_num_bytes must be <={}",
                POINT_MAX_NUM_BYTES
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

pub const STORE_FIELD_TYPE: FieldType = FieldType {
    stored: true,
    tokenized: false,
    store_term_vectors: false,
    store_term_vector_offsets: false,
    store_term_vector_positions: false,
    store_term_vector_payloads: false,
    omit_norms: false,
    index_options: IndexOptions::Null,
    doc_values_type: DocValuesType::Null,
    dimension_count: 0,
    dimension_num_bytes: 0,
};
