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

use core::analysis::TokenStream;
use core::doc::FieldType;
use core::util::{numeric::Numeric, VariantValue};

use error::Result;

/// Represents a single field for indexing.
///
/// `IndexWriter` consumes Vec<Fieldable> as a document.
pub trait Fieldable {
    fn name(&self) -> &str;
    fn field_type(&self) -> &FieldType;
    fn boost(&self) -> f32;
    fn fields_data(&self) -> Option<&VariantValue>;
    fn token_stream(&mut self) -> Result<Box<dyn TokenStream>>;
    fn binary_value(&self) -> Option<&[u8]>;
    fn string_value(&self) -> Option<&str>;
    fn numeric_value(&self) -> Option<Numeric>;
}

impl<T: Fieldable + ?Sized> Fieldable for Box<T> {
    fn name(&self) -> &str {
        (**self).name()
    }
    fn field_type(&self) -> &FieldType {
        (**self).field_type()
    }
    fn boost(&self) -> f32 {
        (**self).boost()
    }
    fn fields_data(&self) -> Option<&VariantValue> {
        (**self).fields_data()
    }
    fn token_stream(&mut self) -> Result<Box<dyn TokenStream>> {
        (**self).token_stream()
    }
    fn binary_value(&self) -> Option<&[u8]> {
        (**self).binary_value()
    }
    fn string_value(&self) -> Option<&str> {
        (**self).string_value()
    }
    fn numeric_value(&self) -> Option<Numeric> {
        (**self).numeric_value()
    }
}
