use std::ops::Deref;

use core::doc::{Field, NUMERIC_DOC_VALUES_FIELD_TYPE};
use core::index::Fieldable;
use core::util::VariantValue;

pub struct FloatDocValuesField {
    field: Field,
}

impl FloatDocValuesField {
    pub fn new(name: &str, value: f32) -> FloatDocValuesField {
        FloatDocValuesField {
            field: Field::new(
                String::from(name),
                NUMERIC_DOC_VALUES_FIELD_TYPE,
                Some(VariantValue::Float(value)),
                None,
            ),
        }
    }

    pub fn float_value(&self) -> f32 {
        self.field.fields_data().unwrap().get_float().unwrap()
    }
}

impl Deref for FloatDocValuesField {
    type Target = Field;
    fn deref(&self) -> &Field {
        &self.field
    }
}
