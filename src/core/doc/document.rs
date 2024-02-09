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

use core::codec::field_infos::FieldInfo;
use core::doc::{DocValuesType, Field, FieldType, Fieldable, IndexOptions};
use core::util::VariantValue;

use error::Result;

pub enum Status {
    Yes,
    No,
    Stop,
}

/// Expert: provides a low-level means of accessing the stored field
/// values in an index.
///
/// NOTE: a `StoredFieldVisitor` implementation should not try to load or visit other
/// stored documents in the same reader because the implementation of stored fields for
/// most codecs is not reentrant and you will see strange exceptions as a result.
pub trait StoredFieldVisitor {
    fn add_binary_field(&mut self, field_info: &FieldInfo, value: Vec<u8>) -> Result<()>;
    fn add_string_field(&mut self, field_info: &FieldInfo, value: Vec<u8>) -> Result<()>;
    fn add_int_field(&mut self, field_info: &FieldInfo, value: i32) -> Result<()>;
    fn add_long_field(&mut self, field_info: &FieldInfo, value: i64) -> Result<()>;
    fn add_float_field(&mut self, field_info: &FieldInfo, value: f32) -> Result<()>;
    fn add_double_field(&mut self, field_info: &FieldInfo, value: f64) -> Result<()>;

    fn needs_field(&self, field_info: &FieldInfo) -> Status;
}

#[derive(Clone, Debug)]
pub struct StoredField {
    pub field: Field,
}

impl StoredField {
    pub fn new(
        name: &str,
        field_type: Option<FieldType>,
        fields_data: VariantValue,
    ) -> StoredField {
        if let Some(f) = field_type {
            StoredField {
                field: Field::new(name.to_string(), f, Some(fields_data), None),
            }
        } else {
            StoredField {
                field: Field::new(
                    name.to_string(),
                    FieldType::new(
                        true,
                        false,
                        false,
                        false,
                        false,
                        false,
                        false,
                        IndexOptions::Null,
                        DocValuesType::Null,
                        0,
                        0,
                    ),
                    Some(fields_data),
                    None,
                ),
            }
        }
    }
}

#[derive(Debug)]
pub struct Document {
    pub fields: Vec<StoredField>,
}

impl Document {
    pub fn new(fields: Vec<StoredField>) -> Document {
        Document { fields }
    }
}

impl Document {
    pub fn add(&mut self, field: StoredField) {
        self.fields.push(field);
    }
    pub fn remove_field(&mut self, name: &str) {
        self.fields.retain(|ref v| v.field.name() != name);
    }
}

pub struct DocumentStoredFieldVisitor {
    pub fields: Vec<StoredField>,
    pub fields_to_add: Vec<String>,
}

impl DocumentStoredFieldVisitor {
    pub fn new(fields_to_add: &[String]) -> DocumentStoredFieldVisitor {
        DocumentStoredFieldVisitor {
            fields: vec![],
            fields_to_add: fields_to_add.to_owned(),
        }
    }

    pub fn document(self) -> Document {
        Document::new(self.fields)
    }
}

impl StoredFieldVisitor for DocumentStoredFieldVisitor {
    fn add_binary_field(&mut self, field_info: &FieldInfo, value: Vec<u8>) -> Result<()> {
        self.fields.push(StoredField::new(
            &field_info.name,
            None,
            VariantValue::Binary(value),
        ));
        Ok(())
    }

    fn add_string_field(&mut self, field_info: &FieldInfo, value: Vec<u8>) -> Result<()> {
        let field_type = FieldType::new(
            true,
            true,
            field_info.has_store_term_vector,
            false,
            false,
            false,
            field_info.has_norms(),
            field_info.index_options,
            DocValuesType::Null,
            0,
            0,
        );

        match String::from_utf8(value) {
            Ok(s) => {
                self.fields.push(StoredField::new(
                    &field_info.name,
                    Some(field_type),
                    VariantValue::VString(s),
                ));
            }
            Err(e) => {
                panic!("string_field failed: {:?}", e);
            }
        }
        Ok(())
    }

    fn add_int_field(&mut self, field_info: &FieldInfo, value: i32) -> Result<()> {
        self.fields.push(StoredField::new(
            &field_info.name,
            None,
            VariantValue::Int(value),
        ));
        Ok(())
    }

    fn add_long_field(&mut self, field_info: &FieldInfo, value: i64) -> Result<()> {
        self.fields.push(StoredField::new(
            &field_info.name,
            None,
            VariantValue::Long(value),
        ));
        Ok(())
    }

    fn add_float_field(&mut self, field_info: &FieldInfo, value: f32) -> Result<()> {
        self.fields.push(StoredField::new(
            &field_info.name,
            None,
            VariantValue::Float(value),
        ));
        Ok(())
    }

    fn add_double_field(&mut self, field_info: &FieldInfo, value: f64) -> Result<()> {
        self.fields.push(StoredField::new(
            &field_info.name,
            None,
            VariantValue::Double(value),
        ));
        Ok(())
    }

    fn needs_field(&self, field_info: &FieldInfo) -> Status {
        if self.fields_to_add.is_empty() || self.fields_to_add.contains(&field_info.name) {
            Status::Yes
        } else {
            Status::No
        }
    }
}
