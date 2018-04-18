use core::doc::NUMERIC_DOC_VALUES_FIELD_TYPE;
use core::doc::{Field, FieldType};
use core::index::fieldable::Fieldable;
use core::util::VariantValue;

pub struct NumericDocValuesField {
    field: Field,
}

impl NumericDocValuesField {
    pub fn new(name: &str, value: i64) -> NumericDocValuesField {
        NumericDocValuesField {
            field: Field::new(
                name,
                NUMERIC_DOC_VALUES_FIELD_TYPE,
                VariantValue::Long(value),
            ),
        }
    }

    pub fn numeric_value(&self) -> i64 {
        match *self.field.fields_data() {
            VariantValue::Long(v) => v,
            _ => panic!("Oops, SHOULD NOT REACH HERE!"),
        }
    }
}

impl Fieldable for NumericDocValuesField {
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
