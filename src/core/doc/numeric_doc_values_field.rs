use core::analysis::TokenStream;
use core::doc::NUMERIC_DOC_VALUES_FIELD_TYPE;
use core::doc::{Field, FieldType};
use core::index::Fieldable;
use core::util::{Numeric, VariantValue};

use error::Result;

pub struct NumericDocValuesField {
    field: Field,
}

impl NumericDocValuesField {
    pub fn new(name: &str, value: i64) -> NumericDocValuesField {
        NumericDocValuesField {
            field: Field::new(
                String::from(name),
                NUMERIC_DOC_VALUES_FIELD_TYPE,
                Some(VariantValue::Long(value)),
                None,
            ),
        }
    }

    pub fn numeric_value(&self) -> i64 {
        self.field.fields_data().unwrap().get_long().unwrap()
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

    fn fields_data(&self) -> Option<&VariantValue> {
        self.field.fields_data()
    }

    fn token_stream(&mut self) -> Result<Box<dyn TokenStream>> {
        unreachable!()
    }

    fn binary_value(&self) -> Option<&[u8]> {
        None
    }

    fn string_value(&self) -> Option<&str> {
        None
    }

    fn numeric_value(&self) -> Option<Numeric> {
        self.fields_data()
            .map(|v| Numeric::Long(v.get_long().unwrap()))
    }
}
