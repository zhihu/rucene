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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize)]
pub enum DocValuesType {
    /// No doc values for this field.
    Null,
    /// A per-document Number
    Numeric,
    /// A per-document [u8].  Values may be larger than
    /// 32766 bytes, but different codecs may enforce their own limits.
    Binary,
    /// A pre-sorted [u8]. Fields with this type only store distinct byte values
    /// and store an additional offset pointer per document to dereference the shared
    /// [u8]. The stored [u8] is presorted and allows access via document id,
    /// ordinal and by-value.  Values must be `<= 32766` bytes.
    Sorted,
    /// A pre-sorted [Number]. Fields with this type store numeric values in sorted
    /// order according to `i64::cmp`.
    SortedNumeric,
    /// A pre-sorted Set<[u8]>. Fields with this type only store distinct byte values
    /// and store additional offset pointers per document to dereference the shared
    /// byte[]s. The stored byte[] is presorted and allows access via document id,
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
