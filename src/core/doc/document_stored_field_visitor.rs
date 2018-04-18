use core::index::field_info::FieldInfo;
use core::index::stored_field_visitor::{Status, StoredFieldVisitor};
use core::index::DocValuesType;
use core::util::VariantValue;

use super::document::Document;
use super::field_type::FieldType;
use super::stored_field::StoredField;

pub struct DocumentStoredFieldVisitor {
    pub fields: Vec<StoredField>,
    pub fields_load: Vec<String>,
}

impl DocumentStoredFieldVisitor {
    pub fn new(fields_load: &[String]) -> DocumentStoredFieldVisitor {
        DocumentStoredFieldVisitor {
            fields: vec![],
            fields_load: fields_load.to_owned(),
        }
    }

    pub fn document(self) -> Document {
        Document::new(self.fields)
    }
}

impl StoredFieldVisitor for DocumentStoredFieldVisitor {
    fn binary_field(&mut self, field_info: &FieldInfo, value: &[u8]) {
        self.fields.push(StoredField::new(
            &field_info.name,
            None,
            VariantValue::Binary(value.to_vec()),
        ));
    }

    fn string_field(&mut self, field_info: &FieldInfo, value: &[u8]) {
        let field_type = FieldType::new(
            true,
            true,
            field_info.has_store_term_vector,
            false,
            false,
            false,
            field_info.has_norms(),
            field_info.index_options.clone(),
            DocValuesType::Null,
        );

        if let Ok(str) = String::from_utf8(value.to_vec()) {
            self.fields.push(StoredField::new(
                &field_info.name,
                Some(field_type),
                VariantValue::VString(str),
            ));
        } else {
            assert!(false, format!("from_utf8({:?}) failed.", value));
        }
    }

    fn int_field(&mut self, field_info: &FieldInfo, value: i32) {
        self.fields.push(StoredField::new(
            &field_info.name,
            None,
            VariantValue::Int(value),
        ))
    }

    fn long_field(&mut self, field_info: &FieldInfo, value: i64) {
        self.fields.push(StoredField::new(
            &field_info.name,
            None,
            VariantValue::Long(value),
        ))
    }

    fn float_field(&mut self, field_info: &FieldInfo, value: f32) {
        self.fields.push(StoredField::new(
            &field_info.name,
            None,
            VariantValue::Float(value),
        ))
    }

    fn double_field(&mut self, field_info: &FieldInfo, value: f64) {
        self.fields.push(StoredField::new(
            &field_info.name,
            None,
            VariantValue::Double(value),
        ))
    }

    fn needs_field(&self, field_info: &FieldInfo) -> Status {
        if self.fields_load.is_empty() || self.fields_load.contains(&field_info.name) {
            Status::YES
        } else {
            Status::NO
        }
    }
}
