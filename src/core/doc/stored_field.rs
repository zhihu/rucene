use core::index::{DocValuesType, IndexOptions};
use core::util::VariantValue;

use core::doc::Field;
use core::doc::FieldType;

lazy_static! {
    pub static ref STORE_FIELD_TYPE: FieldType = {
        let mut field_type = FieldType::default();
        field_type.stored = true;
        field_type
    };
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
