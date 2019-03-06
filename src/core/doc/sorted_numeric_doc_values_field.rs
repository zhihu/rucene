use std::ops::Deref;

use core::analysis::TokenStream;
use core::doc::SORTED_NUMERIC_DOC_VALUES_FIELD_TYPE;
use core::doc::{Field, FieldType};
use core::index::Fieldable;
use core::util::{Numeric, VariantValue};

use error::Result;

pub struct SortedNumericDocValuesField {
    field: Field,
}

impl SortedNumericDocValuesField {
    pub fn new(name: &str, value: i64) -> SortedNumericDocValuesField {
        SortedNumericDocValuesField {
            field: Field::new(
                String::from(name),
                SORTED_NUMERIC_DOC_VALUES_FIELD_TYPE,
                Some(VariantValue::Long(value)),
                None,
            ),
        }
    }

    pub fn numeric_value(&self) -> i64 {
        match self.field.fields_data().unwrap() {
            VariantValue::Long(v) => *v,
            _ => unreachable!(),
        }
    }
}

impl Fieldable for SortedNumericDocValuesField {
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

    fn token_stream(&mut self) -> Result<Box<TokenStream>> {
        self.field.token_stream()
    }

    fn binary_value(&self) -> Option<&[u8]> {
        None
    }

    fn string_value(&self) -> Option<&str> {
        None
    }

    fn numeric_value(&self) -> Option<Numeric> {
        self.field.numeric_value()
    }
}

impl Deref for SortedNumericDocValuesField {
    type Target = Field;

    fn deref(&self) -> &Field {
        &self.field
    }
}
