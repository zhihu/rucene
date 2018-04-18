use core::index::{DocValuesType, IndexOptions};
use core::util::VariantValue;

use core::doc::Field;
use core::doc::FieldType;

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
                field: Field::new(name, f, fields_data),
            }
        } else {
            StoredField {
                field: Field::new(
                    name,
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
                    ),
                    fields_data,
                ),
            }
        }
    }
}
