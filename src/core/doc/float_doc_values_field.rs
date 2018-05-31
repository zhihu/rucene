use std::ops::Deref;

use core::doc::{Field, NUMERIC_DOC_VALUES_FIELD_TYPE};
use core::index::fieldable::Fieldable;
use core::util::VariantValue;

pub struct FloatDocValuesField {
    field: Field,
}

impl FloatDocValuesField {
    pub fn new(name: &str, value: f32) -> FloatDocValuesField {
        FloatDocValuesField {
            field: Field::new(
                name,
                NUMERIC_DOC_VALUES_FIELD_TYPE,
                VariantValue::Float(value),
            ),
        }
    }

    pub fn float_value(&self) -> f32 {
        let data = self.field.fields_data();
        match *data {
            VariantValue::Float(val) => val,
            _ => unreachable!(),
        }
    }
}

impl Deref for FloatDocValuesField {
    type Target = Field;
    fn deref(&self) -> &Field {
        &self.field
    }
}
