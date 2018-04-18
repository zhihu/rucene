use core::doc::Field;
use core::doc::NUMERIC_DOC_VALUES_FIELD_TYPE;
use core::index::fieldable::Fieldable;
use core::util::VariantValue;
use std::ops::Deref;

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
        let val = self.field.fields_data();
        match *val {
            VariantValue::Float(fval) => fval,
            _ => panic!("Oops, NEVER reach here."),
        }
    }
}

impl Deref for FloatDocValuesField {
    type Target = Field;
    fn deref(&self) -> &Field {
        &self.field
    }
}
