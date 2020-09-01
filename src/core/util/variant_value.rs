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

use serde;
use serde::ser::{SerializeMap, SerializeSeq};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};

use core::util::numeric::Numeric;

#[derive(Debug, Clone, Deserialize)]
pub enum VariantValue {
    Bool(bool),
    Char(char),
    Short(i16),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    VString(String),
    Binary(Vec<u8>),
    Vec(Vec<VariantValue>),
    Map(HashMap<String, VariantValue>),
}

impl VariantValue {
    pub fn get_bool(&self) -> Option<bool> {
        match self {
            VariantValue::Bool(b) => Some(*b),
            _ => None,
        }
    }
    pub fn get_char(&self) -> Option<char> {
        match self {
            VariantValue::Char(c) => Some(*c),
            _ => None,
        }
    }
    pub fn get_short(&self) -> Option<i16> {
        match self {
            VariantValue::Short(s) => Some(*s),
            _ => None,
        }
    }
    pub fn get_int(&self) -> Option<i32> {
        match self {
            VariantValue::Int(i) => Some(*i),
            _ => None,
        }
    }
    pub fn get_long(&self) -> Option<i64> {
        match self {
            VariantValue::Long(l) => Some(*l),
            _ => None,
        }
    }
    pub fn get_numeric(&self) -> Option<Numeric> {
        match *self {
            VariantValue::Short(s) => Some(Numeric::Short(s)),
            VariantValue::Int(i) => Some(Numeric::Int(i)),
            VariantValue::Long(l) => Some(Numeric::Long(l)),
            VariantValue::Float(f) => Some(Numeric::Float(f)),
            VariantValue::Double(d) => Some(Numeric::Double(d)),
            _ => None,
        }
    }
    pub fn get_float(&self) -> Option<f32> {
        match self {
            VariantValue::Float(f) => Some(*f),
            _ => None,
        }
    }
    pub fn get_double(&self) -> Option<f64> {
        match self {
            VariantValue::Double(d) => Some(*d),
            _ => None,
        }
    }
    pub fn get_string(&self) -> Option<&str> {
        match self {
            VariantValue::VString(s) => Some(s.as_str()),
            _ => None,
        }
    }
    pub fn get_binary(&self) -> Option<&[u8]> {
        match self {
            VariantValue::Binary(b) => Some(b.as_slice()),
            _ => None,
        }
    }

    pub fn get_utf8_string(&self) -> Option<String> {
        match self {
            VariantValue::VString(s) => Some(s.clone()),
            VariantValue::Binary(b) => {
                if let Ok(s) = String::from_utf8(b.clone()) {
                    Some(s)
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    // used for index sort check
    pub fn is_zero(&self) -> bool {
        match self {
            VariantValue::Int(i) => *i == 0,
            VariantValue::Long(i) => *i == 0,
            VariantValue::Float(i) => *i == 0.0,
            VariantValue::Double(i) => *i == 0.0,
            _ => {
                unreachable!();
            }
        }
    }

    pub fn get_vec(&self) -> Option<&Vec<VariantValue>> {
        match self {
            VariantValue::Vec(v) => Some(v),
            _ => None,
        }
    }

    pub fn get_map(&self) -> Option<&HashMap<String, VariantValue>> {
        match self {
            VariantValue::Map(m) => Some(m),
            _ => None,
        }
    }
}

impl Eq for VariantValue {}

impl fmt::Display for VariantValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            VariantValue::Bool(b) => write!(f, "{}", b),
            VariantValue::Char(c) => write!(f, "{}", c),
            VariantValue::Short(s) => write!(f, "{}s", s),
            VariantValue::Int(ival) => write!(f, "{}", ival),
            VariantValue::Long(lval) => write!(f, "{}", lval),
            VariantValue::Float(fval) => write!(f, "{:.3}", fval),
            VariantValue::Double(d) => write!(f, "{:.6}", d),
            VariantValue::VString(ref s) => write!(f, "{}", s),
            VariantValue::Binary(ref _b) => write!(f, "Binary(unprintable)"),
            VariantValue::Vec(ref v) => write!(f, "{:?}", v),
            VariantValue::Map(ref m) => write!(f, "{:?}", m),
        }
    }
}

impl serde::Serialize for VariantValue {
    fn serialize<S>(&self, serializer: S) -> ::std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match *self {
            VariantValue::Bool(b) => serializer.serialize_bool(b),
            VariantValue::Char(c) => serializer.serialize_char(c),
            VariantValue::Short(s) => serializer.serialize_i16(s),
            VariantValue::Int(ival) => serializer.serialize_i32(ival),
            VariantValue::Long(lval) => serializer.serialize_i64(lval),
            VariantValue::Float(fval) => serializer.serialize_f32(fval),
            VariantValue::Double(d) => serializer.serialize_f64(d),
            VariantValue::VString(ref s) => serializer.serialize_str(s.as_str()),
            VariantValue::Binary(ref b) => serializer.serialize_bytes(b),
            VariantValue::Vec(ref vec) => {
                let mut seq = serializer.serialize_seq(Some(vec.len())).unwrap();
                for v in vec {
                    seq.serialize_element(v)?;
                }

                seq.end()
            }
            VariantValue::Map(ref m) => {
                let mut map = serializer.serialize_map(Some(m.len())).unwrap();
                for (k, v) in m {
                    map.serialize_entry(&k.to_string(), &v)?;
                }
                map.end()
            }
        }
    }
}

impl Hash for VariantValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match *self {
            VariantValue::Bool(ref b) => b.hash(state),
            VariantValue::Char(ref c) => c.hash(state),
            VariantValue::Short(ref s) => s.hash(state),
            VariantValue::Int(ref i) => i.hash(state),
            VariantValue::Long(ref l) => l.hash(state),
            VariantValue::Float(ref f) => f.to_bits().hash(state),
            VariantValue::Double(ref d) => d.to_bits().hash(state),
            VariantValue::VString(ref s) => s.hash(state),
            VariantValue::Binary(ref v) => v.hash(state),
            _ => (),
        }
    }
}

impl PartialEq for VariantValue {
    fn eq(&self, other: &VariantValue) -> bool {
        match *self {
            VariantValue::Bool(ref b) => {
                if let VariantValue::Bool(ref o) = *other {
                    b.eq(o)
                } else {
                    false
                }
            }
            VariantValue::Char(ref c) => {
                if let VariantValue::Char(ref o) = *other {
                    c.eq(o)
                } else {
                    false
                }
            }
            VariantValue::Short(ref s) => {
                if let VariantValue::Short(ref o) = *other {
                    s.eq(o)
                } else {
                    false
                }
            }
            VariantValue::Int(ref i) => {
                if let VariantValue::Int(ref o) = *other {
                    i.eq(o)
                } else {
                    false
                }
            }
            VariantValue::Long(ref l) => {
                if let VariantValue::Long(ref o) = *other {
                    l.eq(o)
                } else {
                    false
                }
            }
            VariantValue::Float(ref f) => {
                if let VariantValue::Float(ref o) = *other {
                    f.eq(o)
                } else {
                    false
                }
            }
            VariantValue::Double(ref d) => {
                if let VariantValue::Double(ref o) = *other {
                    d.eq(o)
                } else {
                    false
                }
            }
            VariantValue::VString(ref s) => {
                if let VariantValue::VString(ref o) = *other {
                    s.eq(o)
                } else {
                    false
                }
            }
            VariantValue::Binary(ref v) => {
                if let VariantValue::Binary(ref o) = *other {
                    v.eq(o)
                } else {
                    false
                }
            }
            _ => unreachable!(),
        }
    }
}

impl Ord for VariantValue {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (&VariantValue::Bool(b1), &VariantValue::Bool(b2)) => b1.cmp(&b2),
            (&VariantValue::Char(c1), &VariantValue::Char(c2)) => c1.cmp(&c2),
            (&VariantValue::Short(v1), &VariantValue::Short(v2)) => v1.cmp(&v2),
            (&VariantValue::Int(v1), &VariantValue::Int(v2)) => v1.cmp(&v2),
            (&VariantValue::Long(v1), &VariantValue::Long(v2)) => v1.cmp(&v2),
            (&VariantValue::Float(v1), &VariantValue::Float(v2)) => v1.partial_cmp(&v2).unwrap(),
            (&VariantValue::Double(v1), &VariantValue::Double(v2)) => v1.partial_cmp(&v2).unwrap(),
            (&VariantValue::VString(ref s1), &VariantValue::VString(ref s2)) => s1.cmp(&s2),
            (&VariantValue::Binary(ref b1), &VariantValue::Binary(ref b2)) => b1.cmp(&b2),
            (_, _) => panic!("Non-comparable"),
        }
    }
}

impl PartialOrd for VariantValue {
    fn partial_cmp(&self, other: &VariantValue) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl From<bool> for VariantValue {
    fn from(val: bool) -> Self {
        VariantValue::Bool(val)
    }
}

/// Implement the From<char> trait for VariantValue
impl From<char> for VariantValue {
    fn from(val: char) -> Self {
        VariantValue::Char(val)
    }
}

/// Implement the From<i16> trait for VariantValue
impl From<i16> for VariantValue {
    fn from(val: i16) -> Self {
        VariantValue::Short(val)
    }
}

impl From<i32> for VariantValue {
    fn from(val: i32) -> Self {
        VariantValue::Int(val)
    }
}

impl From<i64> for VariantValue {
    fn from(val: i64) -> Self {
        VariantValue::Long(val)
    }
}

impl From<f32> for VariantValue {
    fn from(val: f32) -> Self {
        VariantValue::Float(val)
    }
}

impl From<f64> for VariantValue {
    fn from(val: f64) -> Self {
        VariantValue::Double(val)
    }
}

impl<'a> From<&'a str> for VariantValue {
    fn from(val: &'a str) -> Self {
        VariantValue::VString(String::from(val))
    }
}

impl<'a> From<&'a [u8]> for VariantValue {
    fn from(val: &'a [u8]) -> Self {
        VariantValue::Binary(val.to_vec())
    }
}

impl From<Numeric> for VariantValue {
    fn from(val: Numeric) -> Self {
        debug_assert!(!val.is_null());
        match val {
            Numeric::Byte(b) => VariantValue::Char(b as u8 as char),
            Numeric::Short(s) => VariantValue::Short(s),
            Numeric::Int(i) => VariantValue::Int(i),
            Numeric::Long(v) => VariantValue::Long(v),
            Numeric::Float(v) => VariantValue::Float(v),
            Numeric::Double(v) => VariantValue::Double(v),
            Numeric::Null => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn variant_bool_test() {
        let b = VariantValue::Bool(true);
        let expr = format!("{}", b);
        assert_eq!(expr, "true");
    }

    #[test]
    fn variant_char_test() {
        let c = VariantValue::Char('Z');
        let expr = format!("{}", c);
        assert_eq!(expr, "Z");
    }

    #[test]
    fn variant_short_test() {
        let s = VariantValue::Short(30);
        let expr = format!("{}", s);
        assert_eq!(expr, "30s");
    }

    #[test]
    fn variant_int_test() {
        let ival = VariantValue::Int(287);
        let expr = format!("{}", ival);
        assert_eq!(expr, "287");
    }

    #[test]
    fn variant_long_test() {
        let ival = VariantValue::Long(28_754_383);
        let expr = format!("{}", ival);
        assert_eq!(expr, "28754383");
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn variant_float_test() {
        let fval = VariantValue::Float(3.141_593);
        let expr = format!("{}f", fval);
        assert_eq!(expr, "3.142f");

        {
            let fval2 = VariantValue::Float(3.141_593);
            assert_eq!(fval.cmp(&fval2), Ordering::Equal);
        }

        {
            let fval2 = VariantValue::Float(2.141_593);
            assert_eq!(fval.cmp(&fval2), Ordering::Greater);
        }

        {
            let fval2 = VariantValue::Float(4.141_593);
            assert_eq!(fval.cmp(&fval2), Ordering::Less);
        }

        {
            let fval2 = VariantValue::Float(4.141_593);
            assert_eq!(fval.partial_cmp(&fval2), Some(Ordering::Less));
        }
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn variant_double_test() {
        let dval = VariantValue::Double(3.141_592_653_5);
        let expr = format!("{}", dval);
        assert_eq!(expr, "3.141593");
    }

    #[test]
    fn variant_string_test() {
        let strval = VariantValue::VString(String::from("hello world"));
        let expr = format!("{}", strval);
        assert_eq!(expr, "hello world");
    }

    #[test]
    fn variant_binary_test() {
        let bval = VariantValue::Binary(vec![65u8, 66u8, 67u8]);
        let expr = format!("{}", bval);
        assert_eq!(expr, "Binary(unprintable)");

        if let VariantValue::Binary(ref bvec) = bval {
            for (i, val) in bvec.iter().enumerate() {
                assert_eq!(*val, b'A' + i as u8);
            }
        }
    }
}
