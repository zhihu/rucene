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

use core::doc::{Document, FieldType, StoredField};
use core::index::DocValuesType;
use core::index::FieldInfo;
use core::index::{Status, StoredFieldVisitor};
use core::util::VariantValue;

use error::Result;

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
    fn binary_field(&mut self, field_info: &FieldInfo, value: Vec<u8>) -> Result<()> {
        self.fields.push(StoredField::new(
            &field_info.name,
            None,
            VariantValue::Binary(value),
        ));
        Ok(())
    }

    fn string_field(&mut self, field_info: &FieldInfo, value: Vec<u8>) -> Result<()> {
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
                panic!(format!("string_field failed: {:?}", e));
            }
        }
        Ok(())
    }

    fn int_field(&mut self, field_info: &FieldInfo, value: i32) -> Result<()> {
        self.fields.push(StoredField::new(
            &field_info.name,
            None,
            VariantValue::Int(value),
        ));
        Ok(())
    }

    fn long_field(&mut self, field_info: &FieldInfo, value: i64) -> Result<()> {
        self.fields.push(StoredField::new(
            &field_info.name,
            None,
            VariantValue::Long(value),
        ));
        Ok(())
    }

    fn float_field(&mut self, field_info: &FieldInfo, value: f32) -> Result<()> {
        self.fields.push(StoredField::new(
            &field_info.name,
            None,
            VariantValue::Float(value),
        ));
        Ok(())
    }

    fn double_field(&mut self, field_info: &FieldInfo, value: f64) -> Result<()> {
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
