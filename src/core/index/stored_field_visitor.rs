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

use core::index::FieldInfo;

use error::Result;

pub enum Status {
    Yes,
    No,
    Stop,
}

/// Expert: provides a low-level means of accessing the stored field
/// values in an index.
///
/// NOTE: a `StoredFieldVisitor` implementation should not try to load or visit other
/// stored documents in the same reader because the implementation of stored fields for
/// most codecs is not reentrant and you will see strange exceptions as a result.
pub trait StoredFieldVisitor {
    fn binary_field(&mut self, field_info: &FieldInfo, value: Vec<u8>) -> Result<()>;
    fn string_field(&mut self, field_info: &FieldInfo, value: Vec<u8>) -> Result<()>;
    fn int_field(&mut self, field_info: &FieldInfo, value: i32) -> Result<()>;
    fn long_field(&mut self, field_info: &FieldInfo, value: i64) -> Result<()>;
    fn float_field(&mut self, field_info: &FieldInfo, value: f32) -> Result<()>;
    fn double_field(&mut self, field_info: &FieldInfo, value: f64) -> Result<()>;

    fn needs_field(&self, field_info: &FieldInfo) -> Status;
}
