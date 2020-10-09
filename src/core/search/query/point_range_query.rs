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

use error::{ErrorKind, Result};
use std::fmt;

use core::codec::points::{IntersectVisitor, PointValues, Relation};
use core::codec::Codec;
use core::index::reader::{LeafReader, LeafReaderContext};
use core::search::explanation::Explanation;
use core::search::query::{AllDocsIterator, Query, TermQuery, Weight};
use core::search::scorer::{ConstantScoreScorer, Scorer};
use core::search::searcher::SearchPlanBuilder;
use core::search::{DocIdSet, DocIterator, EmptyDocIterator};
use core::util::*;

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
        int2sortable_bytes(float2sortable_int(value), dest)
    }

    pub fn decode_dimension(value: &[u8]) -> f32 {
        sortable_int2float(sortable_bytes2int(value))
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
        long2sortable_bytes(double2sortable_long(value), dest)
    }

    pub fn decode_dimension(value: &[u8]) -> f64 {
        sortable_long2double(sortable_bytes2long(value))
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
        int2sortable_bytes(value, dest)
    }

    pub fn decode_dimension(value: &[u8]) -> i32 {
        sortable_bytes2int(value)
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
        long2sortable_bytes(value, dest)
    }

    pub fn decode_dimension(value: &[u8]) -> i64 {
        sortable_bytes2long(value)
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

#[derive(Copy, Clone)]
pub enum PointValueType {
    Integer,
    Float,
    Double,
    Long,
    /* Byte,
     * SmallFloat,
     * Short */
}

impl PointValueType {
    pub fn format_single_value(self, bytes: &[u8]) -> String {
        match self {
            PointValueType::Float => FloatPoint::decode_dimension(bytes).to_string(),
            PointValueType::Double => DoublePoint::decode_dimension(bytes).to_string(),
            PointValueType::Integer => IntPoint::decode_dimension(bytes).to_string(),
            PointValueType::Long => LongPoint::decode_dimension(bytes).to_string(),
        }
    }

    pub fn format_bytes(self, bytes: &[u8], bytes_per_dim: usize) -> String {
        assert_eq!(bytes.len() % bytes_per_dim, 0);
        let num = bytes.len() / bytes_per_dim;
        let mut values = Vec::with_capacity(num);
        for i in 0..num {
            values
                .push(self.format_single_value(&bytes[i * bytes_per_dim..(i + 1) * bytes_per_dim]));
        }
        format!("[{}]", values.join(", "))
    }
}

impl fmt::Display for PointValueType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let value = match *self {
            PointValueType::Integer => "int",
            PointValueType::Float => "float",
            PointValueType::Double => "double",
            PointValueType::Long => "long",
        };
        write!(f, "{}", value)
    }
}

/// range queries against single or multidimensional points such as `IntPoint`.
///
/// For a single-dimensional field this query is a simple range query;
/// in a multi-dimensional field it's a box shape.
pub struct PointRangeQuery {
    field: String,
    num_dims: usize,
    bytes_per_dim: usize,
    lower_point: Vec<u8>,
    upper_point: Vec<u8>,
    value_type: PointValueType,
}

impl PointRangeQuery {
    pub fn new(
        field: String,
        lower_point: Vec<u8>,
        upper_point: Vec<u8>,
        num_dims: usize,
        value_type: PointValueType,
    ) -> Result<PointRangeQuery> {
        assert!(!field.is_empty() && !lower_point.is_empty() && !upper_point.is_empty());
        assert!(num_dims > 0);

        if lower_point.len() % num_dims != 0 {
            bail!(ErrorKind::IllegalArgument(
                "lowerPoint is not a fixed multiple of numDims".into()
            ));
        }
        if lower_point.len() != upper_point.len() {
            bail!(ErrorKind::IllegalArgument(format!(
                "lowerPoint has length={} but upperPoint has different length={}",
                lower_point.len(),
                upper_point.len()
            )));
        }
        let bytes_per_dim = lower_point.len() / num_dims;
        Ok(PointRangeQuery {
            field,
            num_dims,
            bytes_per_dim,
            lower_point,
            upper_point,
            value_type,
        })
    }
}

pub const POINT_RANGE: &str = "point_range";

impl<C: Codec> Query<C> for PointRangeQuery {
    fn create_weight(
        &self,
        _searcher: &dyn SearchPlanBuilder<C>,
        _needs_scores: bool,
    ) -> Result<Box<dyn Weight<C>>> {
        Ok(Box::new(PointRangeWeight::new(
            self.field.clone(),
            self.num_dims,
            self.bytes_per_dim,
            self.lower_point.clone(),
            self.upper_point.clone(),
            self.value_type,
        )))
    }

    fn extract_terms(&self) -> Vec<TermQuery> {
        vec![]
    }

    fn as_any(&self) -> &dyn (::std::any::Any) {
        self
    }
}

impl fmt::Display for PointRangeQuery {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "PointRangeQuery(field: {}, type:{}, num_dims: {}, bytes_per_dim: {}, lower: {}, \
             upper: {})",
            &self.field,
            &self.value_type,
            self.num_dims,
            self.bytes_per_dim,
            self.value_type
                .format_bytes(&self.lower_point, self.bytes_per_dim),
            self.value_type
                .format_bytes(&self.upper_point, self.bytes_per_dim),
        )
    }
}

struct PointRangeWeight {
    field: String,
    num_dims: usize,
    bytes_per_dim: usize,
    lower_point: Vec<u8>,
    upper_point: Vec<u8>,
    value_type: PointValueType,
    weight: f32,
    norm: f32,
}

impl PointRangeWeight {
    pub fn new(
        field: String,
        num_dims: usize,
        bytes_per_dim: usize,
        lower_point: Vec<u8>,
        upper_point: Vec<u8>,
        value_type: PointValueType,
    ) -> PointRangeWeight {
        PointRangeWeight {
            field,
            num_dims,
            bytes_per_dim,
            lower_point,
            upper_point,
            value_type,
            weight: 0f32,
            norm: 1f32,
        }
    }

    fn build_matching_doc_set<R: LeafReader + ?Sized>(
        &self,
        reader: &R,
        values: &impl PointValues,
    ) -> Result<DocIdSetEnum> {
        let mut result = DocIdSetBuilder::from_values(reader.max_doc(), values, &self.field)?;
        {
            let mut visitor = PointRangeIntersectVisitor::new(&mut result, self);
            values.intersect(&self.field, &mut visitor)?;
        }

        Ok(result.build())
    }
}

impl<C: Codec> Weight<C> for PointRangeWeight {
    fn create_scorer(
        &self,
        leaf_reader_ctx: &LeafReaderContext<'_, C>,
    ) -> Result<Option<Box<dyn Scorer>>> {
        let leaf_reader = leaf_reader_ctx.reader;
        if let Some(ref values) = leaf_reader.point_values() {
            if let Some(field_info) = leaf_reader.field_info(&self.field) {
                if field_info.point_dimension_count != self.num_dims as u32 {
                    bail!(ErrorKind::IllegalArgument(format!(
                        "field '{}' was indexed with num_dims={} but this query has num_dims={}",
                        &self.field, field_info.point_dimension_count, self.num_dims
                    )));
                }
                if self.bytes_per_dim as u32 != field_info.point_num_bytes {
                    bail!(ErrorKind::IllegalArgument(format!(
                        "field '{}' was indexed with bytes_per_dim={} but this query has \
                         bytes_per_dim={}",
                        &self.field, field_info.point_num_bytes, self.bytes_per_dim
                    )));
                }

                let mut all_docs_match = false;
                if values.doc_count(&self.field)? == leaf_reader.max_doc() {
                    let field_packed_lower = values.min_packed_value(&self.field)?;
                    let field_packed_upper = values.max_packed_value(&self.field)?;

                    all_docs_match = true;
                    for i in 0..self.num_dims {
                        let offset = i * self.bytes_per_dim;
                        let end = offset + self.bytes_per_dim;
                        if self.lower_point[offset..end] > field_packed_lower[offset..end]
                            || self.upper_point[offset..end] < field_packed_upper[offset..end]
                        {
                            all_docs_match = false;
                            break;
                        }
                    }
                }

                let iterator = if all_docs_match {
                    PointDocIterEnum::All(AllDocsIterator::new(leaf_reader.max_doc()))
                } else if let Some(iter) = self
                    .build_matching_doc_set(leaf_reader, values)?
                    .iterator()?
                {
                    PointDocIterEnum::DocSet(iter)
                } else {
                    PointDocIterEnum::None(EmptyDocIterator::default())
                };
                let cost = iterator.cost();
                return Ok(Some(Box::new(ConstantScoreScorer::new(
                    self.weight,
                    iterator,
                    cost,
                ))));
            }
        }
        Ok(None)
    }

    fn query_type(&self) -> &'static str {
        POINT_RANGE
    }

    fn normalize(&mut self, norm: f32, boost: f32) {
        self.weight = norm * boost;
        self.norm = norm;
    }

    fn value_for_normalization(&self) -> f32 {
        self.weight * self.weight
    }

    fn needs_scores(&self) -> bool {
        false
    }

    fn explain(&self, _reader: &LeafReaderContext<'_, C>, _doc: DocId) -> Result<Explanation> {
        unimplemented!()
    }
}

impl fmt::Display for PointRangeWeight {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "PointRangeWeight(field: {}, type:{}, num_dims: {}, bytes_per_dim: {}, lower: {}, \
             upper: {})",
            &self.field,
            &self.value_type,
            self.num_dims,
            self.bytes_per_dim,
            self.value_type
                .format_bytes(&self.lower_point, self.bytes_per_dim),
            self.value_type
                .format_bytes(&self.upper_point, self.bytes_per_dim),
        )
    }
}

struct PointRangeIntersectVisitor<'a> {
    doc_id_set_builder: &'a mut DocIdSetBuilder,
    weight: &'a PointRangeWeight,
}

impl<'a> PointRangeIntersectVisitor<'a> {
    pub fn new(
        doc_id_set_builder: &'a mut DocIdSetBuilder,
        weight: &'a PointRangeWeight,
    ) -> PointRangeIntersectVisitor<'a> {
        PointRangeIntersectVisitor {
            doc_id_set_builder,
            weight,
        }
    }
}

impl<'a> IntersectVisitor for PointRangeIntersectVisitor<'a> {
    fn visit(&mut self, doc_id: DocId) -> Result<()> {
        self.doc_id_set_builder.add_doc(doc_id);
        Ok(())
    }

    fn visit_by_packed_value(&mut self, doc_id: DocId, packed_value: &[u8]) -> Result<()> {
        let bytes = self.weight.bytes_per_dim;
        for dim in 0..self.weight.num_dims {
            let offset = dim * bytes;
            let end = offset + bytes;
            if packed_value[offset..end] < self.weight.lower_point[offset..end] {
                return Ok(());
            }
            if packed_value[offset..end] > self.weight.upper_point[offset..end] {
                return Ok(());
            }
        }
        self.doc_id_set_builder.add_doc(doc_id);
        Ok(())
    }

    fn compare(&self, min_packed_value: &[u8], max_packed_value: &[u8]) -> Relation {
        let mut crosses = false;
        let bytes = self.weight.bytes_per_dim;
        for dim in 0..self.weight.num_dims {
            let offset = dim * bytes;
            let end = offset + bytes;
            if min_packed_value[offset..end] > self.weight.upper_point[offset..end]
                || max_packed_value[offset..end] < self.weight.lower_point[offset..]
            {
                return Relation::CellOutsideQuery;
            }

            let end = offset + bytes;
            crosses |= min_packed_value[offset..end] < self.weight.lower_point[offset..end]
                || max_packed_value[offset..end] > self.weight.upper_point[offset..end];
        }

        if crosses {
            Relation::CellCrossesQuery
        } else {
            Relation::CellInsideQuery
        }
    }

    fn grow(&mut self, count: usize) {
        self.doc_id_set_builder.grow(count)
    }
}

enum PointDocIterEnum {
    DocSet(DocIdSetDocIterEnum),
    All(AllDocsIterator),
    None(EmptyDocIterator),
}

impl DocIterator for PointDocIterEnum {
    fn doc_id(&self) -> DocId {
        match self {
            PointDocIterEnum::DocSet(i) => i.doc_id(),
            PointDocIterEnum::All(i) => i.doc_id(),
            PointDocIterEnum::None(i) => i.doc_id(),
        }
    }

    fn next(&mut self) -> Result<DocId> {
        match self {
            PointDocIterEnum::DocSet(i) => i.next(),
            PointDocIterEnum::All(i) => i.next(),
            PointDocIterEnum::None(i) => i.next(),
        }
    }

    fn advance(&mut self, target: i32) -> Result<DocId> {
        match self {
            PointDocIterEnum::DocSet(i) => i.advance(target),
            PointDocIterEnum::All(i) => i.advance(target),
            PointDocIterEnum::None(i) => i.advance(target),
        }
    }

    fn slow_advance(&mut self, target: i32) -> Result<DocId> {
        match self {
            PointDocIterEnum::DocSet(i) => i.slow_advance(target),
            PointDocIterEnum::All(i) => i.slow_advance(target),
            PointDocIterEnum::None(i) => i.slow_advance(target),
        }
    }

    fn cost(&self) -> usize {
        match self {
            PointDocIterEnum::DocSet(i) => i.cost(),
            PointDocIterEnum::All(i) => i.cost(),
            PointDocIterEnum::None(i) => i.cost(),
        }
    }

    fn matches(&mut self) -> Result<bool> {
        match self {
            PointDocIterEnum::DocSet(i) => i.matches(),
            PointDocIterEnum::All(i) => i.matches(),
            PointDocIterEnum::None(i) => i.matches(),
        }
    }

    fn match_cost(&self) -> f32 {
        match self {
            PointDocIterEnum::DocSet(i) => i.match_cost(),
            PointDocIterEnum::All(i) => i.match_cost(),
            PointDocIterEnum::None(i) => i.match_cost(),
        }
    }

    fn approximate_next(&mut self) -> Result<DocId> {
        match self {
            PointDocIterEnum::DocSet(i) => i.approximate_next(),
            PointDocIterEnum::All(i) => i.approximate_next(),
            PointDocIterEnum::None(i) => i.approximate_next(),
        }
    }

    fn approximate_advance(&mut self, target: i32) -> Result<DocId> {
        match self {
            PointDocIterEnum::DocSet(i) => i.approximate_advance(target),
            PointDocIterEnum::All(i) => i.approximate_advance(target),
            PointDocIterEnum::None(i) => i.approximate_advance(target),
        }
    }
}
