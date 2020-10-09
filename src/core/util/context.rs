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

use std::collections::HashMap;
use std::mem;

use core::util::VariantValue;

macro_rules! define_typed_getter {
    ($type:ty) => {
        pub fn get_bool(&self, key: $type) -> Option<bool> {
            self.get(key).and_then(VariantValue::get_bool)
        }
        pub fn get_char(&self, key: $type) -> Option<char> {
            self.get(key).and_then(VariantValue::get_char)
        }
        pub fn get_short(&self, key: $type) -> Option<i16> {
            self.get(key).and_then(VariantValue::get_short)
        }
        pub fn get_int(&self, key: $type) -> Option<i32> {
            self.get(key).and_then(VariantValue::get_int)
        }
        pub fn get_long(&self, key: $type) -> Option<i64> {
            self.get(key).and_then(VariantValue::get_long)
        }
        pub fn get_float(&self, key: $type) -> Option<f32> {
            self.get(key).and_then(VariantValue::get_float)
        }
        pub fn get_double(&self, key: $type) -> Option<f64> {
            self.get(key).and_then(VariantValue::get_double)
        }
        pub fn get_string(&self, key: $type) -> Option<&str> {
            self.get(key).and_then(VariantValue::get_string)
        }
        pub fn get_binary(&self, key: $type) -> Option<&[u8]> {
            self.get(key).and_then(VariantValue::get_binary)
        }

        pub fn set_bool(&mut self, key: $type, value: bool) -> Option<VariantValue> {
            self.set(key, VariantValue::Bool(value))
        }
        pub fn set_char(&mut self, key: $type, value: char) -> Option<VariantValue> {
            self.set(key, VariantValue::Char(value))
        }
        pub fn set_short(&mut self, key: $type, value: i16) -> Option<VariantValue> {
            self.set(key, VariantValue::Short(value))
        }
        pub fn set_int(&mut self, key: $type, value: i32) -> Option<VariantValue> {
            self.set(key, VariantValue::Int(value))
        }
        pub fn set_long(&mut self, key: $type, value: i64) -> Option<VariantValue> {
            self.set(key, VariantValue::Long(value))
        }
        pub fn set_float(&mut self, key: $type, value: f32) -> Option<VariantValue> {
            self.set(key, VariantValue::Float(value))
        }
        pub fn set_double(&mut self, key: $type, value: f64) -> Option<VariantValue> {
            self.set(key, VariantValue::Double(value))
        }
        pub fn set_string(&mut self, key: $type, value: String) -> Option<VariantValue> {
            self.set(key, VariantValue::VString(value))
        }
        pub fn set_binary(&mut self, key: $type, value: Vec<u8>) -> Option<VariantValue> {
            self.set(key, VariantValue::Binary(value))
        }
    };
}

#[derive(Clone, Debug, PartialEq)]
pub struct KeyedContext {
    ctx: HashMap<&'static str, VariantValue>,
}

impl Default for KeyedContext {
    fn default() -> Self {
        KeyedContext {
            ctx: HashMap::new(),
        }
    }
}

impl KeyedContext {
    pub fn get(&self, key: &'static str) -> Option<&VariantValue> {
        self.ctx.get(key)
    }

    pub fn set(&mut self, key: &'static str, value: VariantValue) -> Option<VariantValue> {
        self.ctx.insert(key, value)
    }

    define_typed_getter!(&'static str);
}

#[derive(Clone, Debug, PartialEq)]
pub struct IndexedContext {
    ctx: Vec<Option<VariantValue>>,
}

impl Default for IndexedContext {
    fn default() -> Self {
        IndexedContext { ctx: Vec::new() }
    }
}

impl IndexedContext {
    pub fn get(&self, key: usize) -> Option<&VariantValue> {
        if key + 1 > self.ctx.len() {
            None
        } else {
            self.ctx[key].as_ref()
        }
    }

    pub fn set(&mut self, key: usize, value: VariantValue) -> Option<VariantValue> {
        if key < self.ctx.len() {
            mem::replace(&mut self.ctx[key], Some(value))
        } else {
            self.ctx.resize(key + 1, None);
            self.ctx[key] = Some(value);
            None
        }
    }

    define_typed_getter!(usize);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keyed_context() {
        let mut ctx = KeyedContext::default();
        assert!(ctx.get("abc").is_none());
        assert!(ctx.set("abc", VariantValue::Bool(true)).is_none());
        assert_eq!(
            ctx.set("abc", VariantValue::Int(1)),
            Some(VariantValue::Bool(true))
        );
        assert_eq!(ctx.get("abc"), Some(&VariantValue::Int(1)));
        assert_eq!(ctx.get_int("abc"), Some(1));
        assert!(ctx.get_bool("abc").is_none());
    }

    #[test]
    fn test_indexed_context() {
        let mut ctx = IndexedContext::default();
        assert!(ctx.get(0).is_none());
        assert!(ctx.set(1, VariantValue::Bool(true)).is_none());
        assert_eq!(
            ctx.set(1, VariantValue::Int(1)),
            Some(VariantValue::Bool(true))
        );
        assert_eq!(ctx.get(1), Some(&VariantValue::Int(1)));
        assert_eq!(ctx.get_int(1), Some(1));
        assert!(ctx.get(0).is_none());
        assert!(ctx.get_bool(0).is_none());
    }
}
