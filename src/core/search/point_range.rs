use error::*;
use std::fmt;

use core::doc::{DoublePoint, FloatPoint, IntPoint, LongPoint};
use core::index::LeafReader;
use core::index::{IntersectVisitor, PointValues, Relation};
use core::search::explanation::Explanation;
use core::search::match_all::{AllDocsIterator, ConstantScoreScorer};
use core::search::searcher::IndexSearcher;
use core::search::term_query::TermQuery;
use core::search::{DocIdSet, Query, Scorer, Weight};
use core::search::{EmptyDocIterator, MatchNoDocScorer};
use core::util::{DocId, DocIdSetBuilder};

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
    pub fn format_single_value(&self, bytes: &[u8]) -> String {
        match *self {
            PointValueType::Float => FloatPoint::decode_dimension(bytes).to_string(),
            PointValueType::Double => DoublePoint::decode_dimension(bytes).to_string(),
            PointValueType::Integer => IntPoint::decode_dimension(bytes).to_string(),
            PointValueType::Long => LongPoint::decode_dimension(bytes).to_string(),
        }
    }

    pub fn format_bytes(&self, bytes: &[u8], bytes_per_dim: usize) -> String {
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

impl Query for PointRangeQuery {
    fn create_weight(&self, _searcher: &IndexSearcher, _needs_scores: bool) -> Result<Box<Weight>> {
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
        unimplemented!()
    }

    fn query_type(&self) -> &'static str {
        POINT_RANGE
    }

    fn as_any(&self) -> &::std::any::Any {
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

    fn build_matching_doc_set(
        &self,
        reader: &LeafReader,
        values: &PointValues,
    ) -> Result<Box<DocIdSet>> {
        let mut result = DocIdSetBuilder::from_values(reader.max_doc(), values, &self.field)?;
        {
            let mut visitor = PointRangeIntersectVisitor::new(&mut result, self);
            values.intersect(&self.field, &mut visitor)?;
        }

        Ok(result.build())
    }
}

impl Weight for PointRangeWeight {
    fn create_scorer(&self, leaf_reader: &LeafReader) -> Result<Box<Scorer>> {
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
                    Box::new(AllDocsIterator::new(leaf_reader.max_doc()))
                } else {
                    let iterator = self.build_matching_doc_set(leaf_reader, values.as_ref())?
                        .iterator()?;
                    if iterator.is_some() {
                        iterator.unwrap()
                    } else {
                        Box::new(EmptyDocIterator::default())
                    }
                };
                let cost = iterator.cost();
                return Ok(Box::new(ConstantScoreScorer::new(
                    self.weight,
                    iterator,
                    cost,
                )));
            }
        }
        Ok(Box::new(MatchNoDocScorer::default()))
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

    fn explain(&self, _reader: &LeafReader, _doc: DocId) -> Result<Explanation> {
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
                || min_packed_value[offset..end] < self.weight.lower_point[offset..]
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
