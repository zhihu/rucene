use std::ops::Deref;

use core::doc::{Field, FieldType, BINARY_DOC_VALUES_FIELD_TYPE};
use core::index::fieldable::Fieldable;
use core::util::VariantValue;

pub struct BinaryDocValuesField {
    field: Field,
}

impl BinaryDocValuesField {
    pub fn new(name: &str, value: &[u8]) -> BinaryDocValuesField {
        BinaryDocValuesField {
            field: Field::new(
                name,
                BINARY_DOC_VALUES_FIELD_TYPE,
                VariantValue::from(value),
            ),
        }
    }
}

impl Fieldable for BinaryDocValuesField {
    fn name(&self) -> &str {
        self.field.name()
    }

    fn field_type(&self) -> &FieldType {
        self.field.field_type()
    }

    fn boost(&self) -> f32 {
        self.field.boost()
    }

    fn fields_data(&self) -> &VariantValue {
        self.field.fields_data()
    }
}

impl Deref for BinaryDocValuesField {
    type Target = Field;
    fn deref(&self) -> &Field {
        &self.field
    }
}
