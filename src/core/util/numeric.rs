#[derive(Debug, Clone, Serialize)]
pub enum Numeric {
    Short(i16),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
}

impl Numeric {
    pub fn byte_value(&self) -> i8 {
        match *self {
            Numeric::Short(v) => v as i8,
            Numeric::Int(v) => v as i8,
            Numeric::Long(v) => v as i8,
            Numeric::Float(v) => v as i8,
            Numeric::Double(v) => v as i8,
        }
    }

    pub fn short_value(&self) -> i16 {
        match *self {
            Numeric::Short(v) => v,
            Numeric::Int(v) => v as i16,
            Numeric::Long(v) => v as i16,
            Numeric::Float(v) => v as i16,
            Numeric::Double(v) => v as i16,
        }
    }
}

impl From<bool> for Numeric {
    fn from(val: bool) -> Self {
        Numeric::Short(val as i16)
    }
}

impl From<char> for Numeric {
    fn from(val: char) -> Self {
        Numeric::Short(val as i16)
    }
}

impl From<i16> for Numeric {
    fn from(val: i16) -> Self {
        Numeric::Short(val)
    }
}

impl From<i32> for Numeric {
    fn from(val: i32) -> Self {
        Numeric::Int(val)
    }
}

impl From<i64> for Numeric {
    fn from(val: i64) -> Self {
        Numeric::Long(val)
    }
}

impl From<f32> for Numeric {
    fn from(val: f32) -> Self {
        Numeric::Float(val)
    }
}

impl From<f64> for Numeric {
    fn from(val: f64) -> Self {
        Numeric::Double(val)
    }
}

pub fn to_base36(val: i64) -> String {
    let base36 = "0123456789abcdefghijklmnopqrstuvwxyz";
    let mut val = val as usize;
    let mut result = String::with_capacity(14);
    loop {
        let idx = val % 36;
        result += &base36[idx..idx + 1];
        val /= 36;
        if val == 0 {
            return result.chars().rev().collect();
        }
    }
}

pub fn double2sortable_long(value: f64) -> i64 {
    sortable_double_bits(value.to_bits() as i64)
}

pub fn sortable_long2double(value: i64) -> f64 {
    f64::from_bits(sortable_double_bits(value) as u64)
}

pub fn float2sortable_int(value: f32) -> i32 {
    sortable_float_bits(f32::to_bits(value) as i32)
}

pub fn sortable_int2float(value: i32) -> f32 {
    f32::from_bits(sortable_float_bits(value) as u32)
}

/// Converts IEEE 754 representation of a double to sortable order (or back to the original)
pub fn sortable_double_bits(bits: i64) -> i64 {
    bits ^ (bits >> 63i64) & 0x7fff_ffff_ffff_ffffi64
}

pub fn sortable_float_bits(bits: i32) -> i32 {
    bits ^ (bits >> 31) & 0x7fff_ffff
}

/// Encodes an integer {@code value} such that unsigned byte order comparison
/// is consistent with {@link i32#compare(int, int)}
pub fn int2sortable_bytes(value: i32, result: &mut [u8]) {
    // Flip the sign bit, so negative ints sort before positive ints correctly:
    let value = ((value as u32) ^ 0x8000_0000) as i32;
    result[0] = (value >> 24) as u8;
    result[1] = (value >> 16) as u8;
    result[2] = (value >> 8) as u8;
    result[3] = value as u8;
}

/// Decodes an integer value previously written with {@link #int2sortable_bytes}
pub fn sortable_bytes2int(encoded: &[u8]) -> i32 {
    let x = ((i32::from(encoded[0]) & 0xff) << 24) | ((i32::from(encoded[1]) & 0xff) << 16)
        | ((i32::from(encoded[2]) & 0xff) << 8) | (i32::from(encoded[3]) & 0xff);
    (x as u32 ^ 0x8000_0000) as i32
}

pub fn long2sortable_bytes(value: i64, result: &mut [u8]) {
    let value = (value as u64 ^ 0x8000_0000_0000_0000) as i64;
    result[0] = (value >> 56) as u8;
    result[1] = (value >> 48) as u8;
    result[2] = (value >> 40) as u8;
    result[3] = (value >> 32) as u8;
    result[4] = (value >> 24) as u8;
    result[5] = (value >> 16) as u8;
    result[6] = (value >> 8) as u8;
    result[7] = value as u8;
}

pub fn sortable_bytes2long(encoded: &[u8]) -> i64 {
    let v = ((i64::from(encoded[0]) & 0xffi64) << 56) | ((i64::from(encoded[1]) & 0xffi64) << 48)
        | ((i64::from(encoded[2]) & 0xffi64) << 40)
        | ((i64::from(encoded[3]) & 0xffi64) << 32)
        | ((i64::from(encoded[4]) & 0xffi64) << 24)
        | ((i64::from(encoded[5]) & 0xffi64) << 16)
        | ((i64::from(encoded[6]) & 0xffi64) << 8) | (i64::from(encoded[7]) & 0xffi64);
    (v as u64 ^ 0x8000_0000_0000_0000) as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn numeric_i32_to_i8_test() {
        let v = Numeric::Int(0x1235);
        assert_eq!(v.byte_value(), 0x35);
    }

    #[test]
    fn numeric_i64_to_i16_test() {
        use std;
        let v = Numeric::Long(std::i64::MAX);
        assert_eq!(v.short_value(), -1);
    }

    #[test]
    fn numeric_f32_to_i16_test() {
        let v = Numeric::Float(17.59f32);
        assert_eq!(v.short_value(), 17);
    }

    #[test]
    fn numeric_f64_to_i8_test() {
        let v = Numeric::Double(-2.891_452_34);
        assert_eq!(v.byte_value(), -2);
    }
}
