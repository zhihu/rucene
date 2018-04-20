use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone, Serialize)]
pub enum VariantValue {
    Bool(bool),
    Char(char),
    Short(i16),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    VString(String), // SHOULD BORROW ?
    Binary(Vec<u8>), // SHOULD BORROW ?
}

impl Eq for VariantValue {}

impl fmt::Display for VariantValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            VariantValue::Bool(b) => write!(f, "{}", b),
            VariantValue::Char(c) => write!(f, "{}", c),
            VariantValue::Short(s) => write!(f, "{}s", s),
            VariantValue::Int(ival) => write!(f, "{}", ival),
            VariantValue::Long(lval) => write!(f, "{}L", lval),
            VariantValue::Float(fval) => write!(f, "{:.3}", fval),
            VariantValue::Double(d) => write!(f, "{:.6}", d),
            VariantValue::VString(ref s) => write!(f, "{}", s),
            VariantValue::Binary(ref _b) => write!(f, "Binary(unprintable)"),
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
        assert_eq!(expr, "28754383L");
    }

    #[test]
    #[allow(approx_constant)]
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
    #[allow(approx_constant)]
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
        assert_eq!(expr, "Non-printable");

        if let VariantValue::Binary(ref bvec) = bval {
            for (i, val) in bvec.iter().enumerate() {
                assert_eq!(*val, b'A' + i as u8);
            }
        }
    }
}
