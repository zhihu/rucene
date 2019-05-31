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

use core::analysis::TokenStream;
use core::doc::{BinaryTokenStream, Field, FieldType, BINARY_DOC_VALUES_FIELD_TYPE};
use core::index::Fieldable;
use core::util::{numeric::Numeric, BytesRef, VariantValue};

use error::Result;

pub struct BinaryDocValuesField {
    field: Field,
}

impl BinaryDocValuesField {
    pub fn new(name: &str, value: &[u8]) -> BinaryDocValuesField {
        BinaryDocValuesField {
            field: Field::new(
                String::from(name),
                BINARY_DOC_VALUES_FIELD_TYPE,
                Some(VariantValue::from(value)),
                None,
            ),
        }
    }
}

impl Fieldable for BinaryDocValuesField {
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
        if let VariantValue::Binary(ref v) = self.fields_data().unwrap() {
            Ok(Box::new(BinaryTokenStream::new(BytesRef::new(v.as_ref()))))
        } else {
            unreachable!();
        }
    }

    fn binary_value(&self) -> Option<&[u8]> {
        self.field.binary_value()
    }

    fn string_value(&self) -> Option<&str> {
        None
    }

    fn numeric_value(&self) -> Option<Numeric> {
        None
    }
}

impl Deref for BinaryDocValuesField {
    type Target = Field;
    fn deref(&self) -> &Field {
        &self.field
    }
}
