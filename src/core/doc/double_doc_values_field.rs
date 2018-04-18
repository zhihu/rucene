use core::doc::Field;
use core::doc::NUMERIC_DOC_VALUES_FIELD_TYPE;
use core::index::fieldable::Fieldable;
use core::util::VariantValue;
use std::ops::Deref;

pub struct DoubleDocValuesField {
    field: Field,
}

impl DoubleDocValuesField {
    pub fn new(name: &str, value: f64) -> DoubleDocValuesField {
        DoubleDocValuesField {
            field: Field::new(
                name,
                NUMERIC_DOC_VALUES_FIELD_TYPE,
                VariantValue::Double(value),
            ),
        }
    }

    pub fn double_value(&self) -> f64 {
        let val = self.field.fields_data();
        match *val {
            VariantValue::Double(d) => d,
            _ => panic!("Oops, NEVER reach here."),
        }
    }
}

impl Deref for DoubleDocValuesField {
    type Target = Field;
    fn deref(&self) -> &Field {
        &self.field
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::f64::consts::PI;

    #[test]
    fn double_doc_values_field_new_test() {
        let numeric_field = DoubleDocValuesField::new("pi", PI);
        assert_eq!(numeric_field.name(), "pi");
        assert!((numeric_field.double_value() - PI) < ::std::f64::EPSILON);
    }
}
