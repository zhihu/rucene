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

pub struct SmallFloat;
impl SmallFloat {
    pub fn float_to_byte315(f: f32) -> u8 {
        let bits = f.to_bits() as i32;
        let small_float = (bits >> (24 - 3)) as i32;
        if small_float <= ((63 - 15) << 3) as i32 {
            return if bits <= 0 { 0u8 } else { 1u8 };
        }
        if small_float >= ((63 - 15) << 3) as i32 + 0x100 {
            return 255u8;
        }
        (small_float - ((63 - 15) << 3) as i32) as u8
    }

    pub fn byte315_to_float(b: u8) -> f32 {
        if b == 0 {
            0f32
        } else {
            let mut bits = u32::from(b) << (24 - 3);
            bits += (63 - 15) << 24;
            f32::from_bits(bits)
        }
    }
}

#[cfg(test)]
pub mod tests {
    extern crate rand;

    use super::*;

    fn origin_byte_to_float(b: u8) -> f32 {
        if b == 0 {
            return 0f32;
        }
        let mantissa = b & 7;
        let exponent = (b >> 3) & 31;
        let bits = ((u32::from(exponent) + (63 - 15)) << 24) | ((u32::from(mantissa)) << 21) as u32;
        f32::from_bits(bits as u32)
    }

    fn origin_float_to_byte(f: f32) -> u8 {
        if f < 0.0f32 {
            return 0u8;
        }

        let bits = f.to_bits() as i32;
        let mut mantissa = (bits & 0xff_ffff) >> 21 as i32;
        let mut exponent = (((bits >> 24) & 0x7f) - 63) + 15;

        if exponent > 31 {
            exponent = 31;
            mantissa = 7;
        }

        if exponent < 0 || (exponent == 0 && mantissa == 0) {
            exponent = 0;
            mantissa = 1;
        }
        ((exponent << 3) | mantissa) as u8
    }

    #[test]
    fn test_float_to_byte315() {
        let min_value = 1.4e-45f32;
        let positive_infinity = 1.0f32 / 0.0f32;
        let negative_infinity = -1.0f32 / 0.0f32;
        let max_value = 3.402_823_5e+38f32;

        assert_eq!(1, origin_float_to_byte(5.812_381_7E-10f32));
        assert_eq!(1, SmallFloat::float_to_byte315(5.812_381_7E-10f32));

        assert_eq!(0, SmallFloat::float_to_byte315(0f32));
        assert_eq!(1, SmallFloat::float_to_byte315(min_value));
        assert_eq!(255, SmallFloat::float_to_byte315(max_value));
        assert_eq!(255, SmallFloat::float_to_byte315(positive_infinity));

        assert_eq!(0, SmallFloat::float_to_byte315(-min_value));
        assert_eq!(0, SmallFloat::float_to_byte315(-max_value));
        assert_eq!(0, SmallFloat::float_to_byte315(negative_infinity));

        let num = 100_000;
        for _ in 0..num {
            let m: u32 = rand::random::<u32>();
            let f = f32::from_bits(m);
            if f.is_nan() {
                continue;
            }
            let b1 = origin_float_to_byte(f);
            let b2 = SmallFloat::float_to_byte315(f);
            assert_eq!(b1, b2);
        }
    }

    #[test]
    fn test_byte315_to_float() {
        for i in 0..256 {
            let f1 = origin_byte_to_float(i as u8);
            let f2 = SmallFloat::byte315_to_float(i as u8);
            assert!((f1 - f2) < ::std::f32::EPSILON);
        }
    }
}
