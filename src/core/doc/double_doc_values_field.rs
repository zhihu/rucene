// Copyright 2019 Zhizhesihai (Beijing) Technology Limited.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::ops::Deref;

use core::doc::{Field, NUMERIC_DOC_VALUES_FIELD_TYPE};
use core::index::Fieldable;
use core::util::VariantValue;

pub struct DoubleDocValuesField {
    field: Field,
}

impl DoubleDocValuesField {
    pub fn new(name: &str, value: f64) -> DoubleDocValuesField {
        DoubleDocValuesField {
            field: Field::new(
                name.to_string(),
                NUMERIC_DOC_VALUES_FIELD_TYPE,
                Some(VariantValue::Double(value)),
                None,
            ),
        }
    }

    pub fn double_value(&self) -> f64 {
        self.field.fields_data().unwrap().get_double().unwrap()
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
