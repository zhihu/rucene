use core::index::field_info::FieldInfo;
use core::index::stored_field_visitor::{Status, StoredFieldVisitor};
use core::index::DocValuesType;
use core::util::VariantValue;

use core::doc::{Document, FieldType, StoredField};

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
    fn binary_field(&mut self, field_info: &FieldInfo, value: Vec<u8>) {
        self.fields.push(StoredField::new(
            &field_info.name,
            None,
            VariantValue::Binary(value),
        ));
    }

    fn string_field(&mut self, field_info: &FieldInfo, value: Vec<u8>) {
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
                assert!(false, format!("string_field failed: {:?}", e));
            }
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
        if self.fields_to_add.is_empty() || self.fields_to_add.contains(&field_info.name) {
            Status::YES
        } else {
            Status::NO
        }
    }
}
