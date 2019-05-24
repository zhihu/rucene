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

use core::index::{DocValuesType, IndexOptions};
use core::util::VariantValue;

use core::doc::Field;
use core::doc::FieldType;

lazy_static! {
    pub static ref STORE_FIELD_TYPE: FieldType = {
        let mut field_type = FieldType::default();
        field_type.stored = true;
        field_type
    };
}

#[derive(Clone, Debug)]
pub struct StoredField {
    pub field: Field,
}

impl StoredField {
    pub fn new(
        name: &str,
        field_type: Option<FieldType>,
        fields_data: VariantValue,
    ) -> StoredField {
        if let Some(f) = field_type {
            StoredField {
                field: Field::new(name.to_string(), f, Some(fields_data), None),
            }
        } else {
            StoredField {
                field: Field::new(
                    name.to_string(),
                    FieldType::new(
                        true,
                        false,
                        false,
                        false,
                        false,
                        false,
                        false,
                        IndexOptions::Null,
                        DocValuesType::Null,
                        0,
                        0,
                    ),
                    Some(fields_data),
                    None,
                ),
            }
        }
    }
}
