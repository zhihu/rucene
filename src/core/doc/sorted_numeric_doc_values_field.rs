use std::ops::Deref;

use core::doc::Field;
use core::doc::SORTED_NUMERIC_DOC_VALUES_FIELD_TYPE;
use core::index::Fieldable;
use core::util::VariantValue;


pub struct SortedNumericDocValuesField {
    field: Field,
}

impl SortedNumericDocValuesField {
    pub fn new(name: &str, value: i64) -> SortedNumericDocValuesField {
        SortedNumericDocValuesField {
            field: Field::new(
                name,
                SORTED_NUMERIC_DOC_VALUES_FIELD_TYPE,
                VariantValue::Long(value),
            ),
        }
    }

    pub fn numeric_value(&self) -> i64 {
        match *self.field.fields_data() {
            VariantValue::Long(v) => v,
            _ => unreachable!(),
        }
    }
}

impl Deref for SortedNumericDocValuesField {
    type Target = Field;

    fn deref(&self) -> &Field {
        &self.field
    }
}
