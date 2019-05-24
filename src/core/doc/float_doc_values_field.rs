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

pub struct FloatDocValuesField {
    field: Field,
}

impl FloatDocValuesField {
    pub fn new(name: &str, value: f32) -> FloatDocValuesField {
        FloatDocValuesField {
            field: Field::new(
                String::from(name),
                NUMERIC_DOC_VALUES_FIELD_TYPE,
                Some(VariantValue::Float(value)),
                None,
            ),
        }
    }

    pub fn float_value(&self) -> f32 {
        self.field.fields_data().unwrap().get_float().unwrap()
    }
}

impl Deref for FloatDocValuesField {
    type Target = Field;
    fn deref(&self) -> &Field {
        &self.field
    }
}
