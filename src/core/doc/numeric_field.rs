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

use core::codec::Codec;
use core::search::point_range::{PointRangeQuery, PointValueType};
use core::search::Query;
use core::util::numeric;

use error::Result;

use num_traits::float::Float;

/// An indexed `f32` field for fast range filters.
///
/// If you also need to store the value, you should add a separate `StoredField` instance.
///
/// Finding all documents within an N-dimensional at search time is efficient.
/// Multiple values for the same field in one document is allowed.
///
/// This field defines static factory methods for creating common queries
pub struct FloatPoint;

impl FloatPoint {
    pub fn next_up(f: f32) -> f32 {
        let mut int_value = f32::to_bits(f);
        if int_value == 0x8000_0000u32 {
            // -0f32
            return 0f32;
        }
        // following code is copy from jdk 8 std lib `Math#nextUp`
        if f.is_nan() || (f.is_infinite() && f.is_sign_positive()) {
            return f;
        }
        let f = f + 0.0f32;
        if f >= 0.0f32 {
            int_value += 1;
        } else {
            int_value -= 1;
        }
        f32::from_bits(int_value)
    }

    pub fn next_down(f: f32) -> f32 {
        let mut int_value = f32::to_bits(f);
        if int_value == 0 {
            // 0f
            return -0f32;
        }
        // following code is copy from jdk 8 std lib `Math#nextDown`
        let neg_infinity: f32 = Float::neg_infinity();
        if f.is_nan() || (f - neg_infinity).abs() < ::std::f32::EPSILON {
            return f;
        }
        if f == 0.0f32 {
            -f32::min_positive_value()
        } else {
            if f <= 0.0f32 {
                int_value += 1;
            } else {
                int_value -= 1;
            }
            f32::from_bits(int_value)
        }
    }

    /// Create a query for matching an exact float value.
    pub fn new_exact_query<C: Codec>(field: String, value: f32) -> Result<Box<dyn Query<C>>> {
        FloatPoint::new_range_query(field, value, value)
    }

    /// Create a range query for float values.
    /// Ranges are inclusive. For exclusive ranges, pass {@link #next_up(f32) nextUp(lower)}
    /// or {@link #next_down(f32) next_down(upper)}.
    /// Range comparisons are consistent with {@link f32#compareTo(Float)}.
    pub fn new_range_query<C: Codec>(
        field: String,
        lower: f32,
        upper: f32,
    ) -> Result<Box<dyn Query<C>>> {
        FloatPoint::new_multi_range_query(field, &[lower], &[upper])
    }

    /// Create a range query for n-dimensional float values.
    pub fn new_multi_range_query<C: Codec>(
        field: String,
        lower: &[f32],
        upper: &[f32],
    ) -> Result<Box<dyn Query<C>>> {
        Ok(Box::new(PointRangeQuery::new(
            field,
            FloatPoint::pack(lower),
            FloatPoint::pack(upper),
            lower.len(),
            PointValueType::Float,
        )?))
    }

    pub fn encode_dimension(value: f32, dest: &mut [u8]) {
        numeric::int2sortable_bytes(numeric::float2sortable_int(value), dest)
    }

    pub fn decode_dimension(value: &[u8]) -> f32 {
        numeric::sortable_int2float(numeric::sortable_bytes2int(value))
    }

    fn pack(point: &[f32]) -> Vec<u8> {
        assert!(!point.is_empty());
        let mut packed = vec![0u8; point.len() * 4];
        for dim in 0..point.len() {
            FloatPoint::encode_dimension(point[dim], &mut packed[dim * 4..]);
        }
        packed
    }
}

pub struct DoublePoint;

impl DoublePoint {
    pub fn next_up(d: f64) -> f64 {
        let mut bits = f64::to_bits(d);
        if bits == 0x8000_0000_0000_0000u64 {
            // -0f64
            return 0f64;
        }

        let infinity: f64 = Float::infinity();
        if d.is_nan() || (d - infinity).abs() < ::std::f64::EPSILON {
            d
        } else {
            if d >= 0.0f64 {
                bits += 1;
            } else {
                bits -= 1;
            }
            f64::from_bits(bits)
        }
    }

    pub fn next_down(d: f64) -> f64 {
        let mut bits = f64::to_bits(d);
        if bits == 0u64 {
            return -0f64;
        }

        let neg_infinity: f64 = Float::neg_infinity();
        if d.is_nan() || (d - neg_infinity).abs() < ::std::f64::EPSILON {
            d
        } else if d == 0.0f64 {
            -f64::min_positive_value()
        } else {
            if d > 0.0f64 {
                bits -= 1;
            } else {
                bits += 1;
            }
            f64::from_bits(bits)
        }
    }

    pub fn pack(point: &[f64]) -> Vec<u8> {
        assert!(!point.is_empty());
        let mut packed = vec![0u8; point.len() * 8];
        for dim in 0..point.len() {
            DoublePoint::encode_dimension(point[dim], &mut packed[dim * 8..]);
        }
        packed
    }

    pub fn encode_dimension(value: f64, dest: &mut [u8]) {
        numeric::long2sortable_bytes(numeric::double2sortable_long(value), dest)
    }

    pub fn decode_dimension(value: &[u8]) -> f64 {
        numeric::sortable_long2double(numeric::sortable_bytes2long(value))
    }

    /// Create a query for matching an exact double value.
    pub fn new_exact_query<C: Codec>(field: String, value: f64) -> Result<Box<dyn Query<C>>> {
        DoublePoint::new_range_query(field, value, value)
    }

    /// Create a range query for double values.
    pub fn new_range_query<C: Codec>(
        field: String,
        lower: f64,
        upper: f64,
    ) -> Result<Box<dyn Query<C>>> {
        DoublePoint::new_multi_range_query(field, &[lower], &[upper])
    }

    /// Create a range query for n-dimensional double values.
    pub fn new_multi_range_query<C: Codec>(
        field: String,
        lower: &[f64],
        upper: &[f64],
    ) -> Result<Box<dyn Query<C>>> {
        Ok(Box::new(PointRangeQuery::new(
            field,
            DoublePoint::pack(lower),
            DoublePoint::pack(upper),
            lower.len(),
            PointValueType::Double,
        )?))
    }
}

pub struct IntPoint;

impl IntPoint {
    pub fn pack(point: &[i32]) -> Vec<u8> {
        assert!(!point.is_empty());
        let mut packed = vec![0u8; point.len() * 4];
        for dim in 0..point.len() {
            IntPoint::encode_dimension(point[dim], &mut packed[dim * 4..]);
        }
        packed
    }

    pub fn encode_dimension(value: i32, dest: &mut [u8]) {
        numeric::int2sortable_bytes(value, dest)
    }

    pub fn decode_dimension(value: &[u8]) -> i32 {
        numeric::sortable_bytes2int(value)
    }

    pub fn new_exact_query<C: Codec>(field: String, value: i32) -> Result<Box<dyn Query<C>>> {
        IntPoint::new_range_query(field, value, value)
    }

    pub fn new_range_query<C: Codec>(
        field: String,
        lower: i32,
        upper: i32,
    ) -> Result<Box<dyn Query<C>>> {
        IntPoint::new_multi_range_query(field, &[lower], &[upper])
    }

    pub fn new_multi_range_query<C: Codec>(
        field: String,
        lower: &[i32],
        upper: &[i32],
    ) -> Result<Box<dyn Query<C>>> {
        Ok(Box::new(PointRangeQuery::new(
            field,
            IntPoint::pack(lower),
            IntPoint::pack(upper),
            lower.len(),
            PointValueType::Integer,
        )?))
    }
}

pub struct LongPoint;

impl LongPoint {
    pub fn pack(point: &[i64]) -> Vec<u8> {
        assert!(!point.is_empty());
        let mut packed = vec![0u8; point.len() * 8];
        for dim in 0..point.len() {
            LongPoint::encode_dimension(point[dim], &mut packed[dim * 8..]);
        }
        packed
    }

    pub fn encode_dimension(value: i64, dest: &mut [u8]) {
        numeric::long2sortable_bytes(value, dest)
    }

    pub fn decode_dimension(value: &[u8]) -> i64 {
        numeric::sortable_bytes2long(value)
    }

    pub fn new_exact_query<C: Codec>(field: String, value: i64) -> Result<Box<dyn Query<C>>> {
        LongPoint::new_range_query(field, value, value)
    }

    pub fn new_range_query<C: Codec>(
        field: String,
        lower: i64,
        upper: i64,
    ) -> Result<Box<dyn Query<C>>> {
        LongPoint::new_multi_range_query(field, &[lower], &[upper])
    }

    pub fn new_multi_range_query<C: Codec>(
        field: String,
        lower: &[i64],
        upper: &[i64],
    ) -> Result<Box<dyn Query<C>>> {
        Ok(Box::new(PointRangeQuery::new(
            field,
            LongPoint::pack(lower),
            LongPoint::pack(upper),
            lower.len(),
            PointValueType::Long,
        )?))
    }
}
