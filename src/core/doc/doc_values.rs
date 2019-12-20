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

use core::analysis::{BinaryTokenStream, TokenStream};
use core::doc::{
    Field, FieldType, Fieldable, BINARY_DOC_VALUES_FIELD_TYPE, NUMERIC_DOC_VALUES_FIELD_TYPE,
    SORTED_NUMERIC_DOC_VALUES_FIELD_TYPE, SORTED_SET_DOC_VALUES_FIELD_TYPE,
};
use core::util::{BytesRef, Numeric, VariantValue};

use error::Result;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize)]
pub enum DocValuesType {
    /// No doc values for this field.
    Null,
    /// A per-document [u8].  Values may be larger than
    /// 32766 bytes, but different codecs may enforce their own limits.
    Binary,
    /// A per-document Number
    Numeric,
    /// A pre-sorted \[Numeric\]. Fields with this type store numeric values in sorted
    /// order according to `i64::cmp`.
    SortedNumeric,
    /// A pre-sorted [u8]. Fields with this type only store distinct byte values
    /// and store an additional offset pointer per document to dereference the shared
    /// [u8]. The stored [u8] is presorted and allows access via document id,
    /// ordinal and by-value.  Values must be `<= 32766` bytes.
    Sorted,
    /// A pre-sorted Set<[u8]>. Fields with this type only store distinct byte values
    /// and store additional offset pointers per document to dereference the shared
    /// bytes. The stored bytes is presorted and allows access via document id,
    /// ordinal and by-value.  Values must be `<= 32766` bytes.
    SortedSet,
}

impl DocValuesType {
    pub fn null(self) -> bool {
        match self {
            DocValuesType::Null => true,
            _ => false,
        }
    }

    pub fn is_numeric(self) -> bool {
        match self {
            DocValuesType::Numeric | DocValuesType::SortedNumeric => true,
            _ => false,
        }
    }

    // whether the doc values type can store string
    pub fn support_string(self) -> bool {
        match self {
            DocValuesType::Binary | DocValuesType::Sorted | DocValuesType::SortedSet => true,
            _ => false,
        }
    }
}

impl Default for DocValuesType {
    fn default() -> DocValuesType {
        DocValuesType::Null
    }
}

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

    fn field_data(&self) -> Option<&VariantValue> {
        self.field.field_data()
    }

    fn token_stream(&mut self) -> Result<Box<dyn TokenStream>> {
        if let VariantValue::Binary(ref v) = self.field_data().unwrap() {
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
        self.field.field_data().unwrap().get_float().unwrap()
    }
}

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
        self.field.field_data().unwrap().get_double().unwrap()
    }
}

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
        self.field.field_data().unwrap().get_long().unwrap()
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

    fn field_data(&self) -> Option<&VariantValue> {
        self.field.field_data()
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
        self.field_data()
            .map(|v| Numeric::Long(v.get_long().unwrap()))
    }
}

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
        match self.field.field_data().unwrap() {
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

    fn field_data(&self) -> Option<&VariantValue> {
        self.field.field_data()
    }

    fn token_stream(&mut self) -> Result<Box<dyn TokenStream>> {
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
        match self.field.field_data().unwrap() {
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

    fn field_data(&self) -> Option<&VariantValue> {
        self.field.field_data()
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
