use core::doc::Field;
use core::doc::SORTED_SET_DOC_VALUES_FIELD_TYPE;
use core::index::fieldable::Fieldable;
use core::util::VariantValue;

use std::ops::Deref;

pub struct SortedSetDocValuesField {
    field: Field,
}

impl SortedSetDocValuesField {
    pub fn new(name: &str, value: &[u8]) -> SortedSetDocValuesField {
        SortedSetDocValuesField {
            field: Field::new(
                name,
                SORTED_SET_DOC_VALUES_FIELD_TYPE,
                VariantValue::from(value),
            ),
        }
    }

    pub fn binary_value(&self) -> &[u8] {
        match *self.field.fields_data() {
            VariantValue::Binary(ref v) => v,
            _ => panic!("Oops, SHOULD NOT REACH HERE!"),
        }
    }
}

impl Deref for SortedSetDocValuesField {
    type Target = Field;

    fn deref(&self) -> &Field {
        &self.field
    }
}
