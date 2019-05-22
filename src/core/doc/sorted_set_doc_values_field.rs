use core::analysis::TokenStream;
use core::doc::{Field, FieldType, SORTED_SET_DOC_VALUES_FIELD_TYPE};
use core::index::Fieldable;
use core::util::{Numeric, VariantValue};

use error::Result;

pub struct SortedSetDocValuesField {
    field: Field,
}

impl SortedSetDocValuesField {
    pub fn new(name: &str, value: &[u8]) -> SortedSetDocValuesField {
        SortedSetDocValuesField {
            field: Field::new(
                String::from(name),
                SORTED_SET_DOC_VALUES_FIELD_TYPE,
                Some(VariantValue::from(value)),
                None,
            ),
        }
    }

    pub fn binary_value(&self) -> &[u8] {
        match self.field.fields_data().unwrap() {
            VariantValue::Binary(ref v) => v,
            _ => unreachable!(),
        }
    }
}

impl Fieldable for SortedSetDocValuesField {
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
        self.field.token_stream()
    }

    fn binary_value(&self) -> Option<&[u8]> {
        self.field.binary_value()
    }

    fn string_value(&self) -> Option<&str> {
        self.field.string_value()
    }

    fn numeric_value(&self) -> Option<Numeric> {
        self.field.numeric_value()
    }
}
