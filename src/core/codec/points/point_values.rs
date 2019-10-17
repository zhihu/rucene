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

use error::Result;

use core::codec::Codec;
use core::index::reader::IndexReader;
use core::util::DocId;

use std::any::Any;
use std::sync::Arc;

/// Access to indexed numeric values.
///
/// Points represent numeric values and are indexed differently than ordinary text. Instead of an
/// inverted index, points are indexed with data structures such as [KD-trees](https://en.wikipedia.org/wiki/K-d_tree).
/// These structures are optimized for operations such as `range`, `distance`, `nearest-neighbor`,
/// and *point-in-polygon* queries.
/// Basic Point Types:
/// | data type | Rucene struct |
/// | --------- | ------------- |
/// | `i32` | `IntPoint` |
/// | `i64` | `LongPoint` |
/// | `f32` | `FloatPoint` |
/// | `f64` | `DoublePoint` |
/// | `Vec<u8>` | `BinaryPoint` |
///
/// Basic Rucene point types behave like their rust peers: for example `IntPoint` represents
/// a signed `i32`, supporting values ranging from `i32::min_value()` to `i32::max_value()`,
/// ordered consistent with `i32` ord. In addition to indexing support, point structs also
/// contain static methods (such as {@link IntPoint#newRangeQuery(String, int, int)}) for
/// creating common queries. For example:
/// ```rust, ignore
/// use rucene::core::search::query::IntPoint;
/// use rucene::core::search::collector::TopDocsCollector;
/// // issue range query of 1960-1980
/// let query = IntPoint::new_range_query("year".into(), 1960, 1980);
/// let mut collector = TopDocsCollector::new(10);
/// let docs = searcher.search(query, &mut collector);
/// ```
///
/// Custom structures can be created on top of single- or multi- dimensional basic types, on top of
/// `BinaryPoint` for more flexibility, or via custom `Field` subclasses.
pub trait PointValues {
    /// Finds all documents and points matching the provided visitor.
    /// This method does not enforce live documents, so it's up to the caller
    /// to test whether each document is deleted, if necessary.
    fn intersect(&self, field_name: &str, visitor: &mut impl IntersectVisitor) -> Result<()>;

    /// Returns minimum value for each dimension, packed, or null if `size` is 0
    fn min_packed_value(&self, field_name: &str) -> Result<Vec<u8>>;

    /// Returns maximum value for each dimension, packed, or null if `size` is 0
    fn max_packed_value(&self, field_name: &str) -> Result<Vec<u8>>;

    /// Returns how many dimensions were indexed
    fn num_dimensions(&self, field_name: &str) -> Result<usize>;

    /// Returns the number of bytes per dimension
    fn bytes_per_dimension(&self, field_name: &str) -> Result<usize>;

    /// Returns the total number of indexed points across all documents in this field.
    fn size(&self, field_name: &str) -> Result<i64>;

    /// Returns the total number of documents that have indexed at least one point for this
    /// field.
    fn doc_count(&self, field_name: &str) -> Result<i32>;

    fn as_any(&self) -> &dyn Any;
}

impl<T: PointValues + 'static> PointValues for Arc<T> {
    fn intersect(&self, field_name: &str, visitor: &mut impl IntersectVisitor) -> Result<()> {
        (**self).intersect(field_name, visitor)
    }

    fn min_packed_value(&self, field_name: &str) -> Result<Vec<u8>> {
        (**self).min_packed_value(field_name)
    }

    fn max_packed_value(&self, field_name: &str) -> Result<Vec<u8>> {
        (**self).max_packed_value(field_name)
    }

    fn num_dimensions(&self, field_name: &str) -> Result<usize> {
        (**self).num_dimensions(field_name)
    }

    fn bytes_per_dimension(&self, field_name: &str) -> Result<usize> {
        (**self).bytes_per_dimension(field_name)
    }

    fn size(&self, field_name: &str) -> Result<i64> {
        (**self).size(field_name)
    }

    fn doc_count(&self, field_name: &str) -> Result<i32> {
        (**self).doc_count(field_name)
    }

    fn as_any(&self) -> &dyn Any {
        (**self).as_any()
    }
}

/// Return the cumulated number of points across all leaves of the given
/// `IndexReader`. Leaves that do not have points for the given field
/// are ignored.
/// @see PointValues#size(String)
pub fn point_values_size<C: Codec>(
    reader: &dyn IndexReader<Codec = C>,
    field: &str,
) -> Result<i64> {
    let mut size = 0i64;
    for leaf_reader in reader.leaves() {
        if let Some(info) = leaf_reader.reader.field_info(field) {
            if info.point_dimension_count != 0 {
                if let Some(ref values) = leaf_reader.reader.point_values() {
                    size += values.size(field)?;
                }
            }
        }
    }
    Ok(size)
}

/// Return the cumulated number of docs that have points across all leaves
/// of the given `IndexReader`. Leaves that do not have points for the
/// given field are ignored.
/// @see PointValues#getDocCount(String)
pub fn point_values_doc_count<C: Codec>(
    reader: &dyn IndexReader<Codec = C>,
    field: &str,
) -> Result<i32> {
    let mut count = 0i32;
    for leaf_reader in reader.leaves() {
        if let Some(info) = leaf_reader.reader.field_info(field) {
            if info.point_dimension_count != 0 {
                if let Some(ref values) = leaf_reader.reader.point_values() {
                    count += values.doc_count(field)?;
                }
            }
        }
    }
    Ok(count)
}

/// Return the minimum packed values across all leaves of the given
/// `IndexReader`. Leaves that do not have points for the given field
/// are ignored.
/// @see PointValues#getMinPackedValue(String)
pub fn point_values_min_packed_value<C: Codec>(
    reader: &dyn IndexReader<Codec = C>,
    field: &str,
) -> Result<Vec<u8>> {
    let mut min_value = Vec::new();
    for leaf_reader in reader.leaves() {
        if let Some(info) = leaf_reader.reader.field_info(field) {
            if info.point_dimension_count == 0 {
                continue;
            }
            if let Some(ref values) = leaf_reader.reader.point_values() {
                let leaf_min_value = values.min_packed_value(field)?;
                if leaf_min_value.is_empty() {
                    continue;
                }
                if min_value.is_empty() {
                    min_value = leaf_min_value.clone();
                } else {
                    let num_dimensions = values.num_dimensions(field)?;
                    let num_bytes_per_dimension = values.bytes_per_dimension(field)?;
                    for i in 0..num_dimensions {
                        let offset: usize = i * num_bytes_per_dimension;
                        if leaf_min_value[offset..offset + num_dimensions]
                            < min_value[offset..offset + num_dimensions]
                        {
                            min_value[offset..offset + num_bytes_per_dimension].copy_from_slice(
                                &leaf_min_value[offset..offset + num_bytes_per_dimension],
                            );
                        }
                    }
                }
            }
        }
    }
    Ok(min_value)
}

/// Return the maximum packed values across all leaves of the given
/// `IndexReader`. Leaves that do not have points for the given field
/// are ignored.
///  @see PointValues#getMaxPackedValue(String)
pub fn point_values_max_packed_value<C: Codec>(
    reader: &dyn IndexReader<Codec = C>,
    field: &str,
) -> Result<Vec<u8>> {
    let mut max_value = Vec::new();
    for leaf_reader in reader.leaves() {
        if let Some(info) = leaf_reader.reader.field_info(field) {
            if info.point_dimension_count == 0 {
                continue;
            }
            let values = leaf_reader.reader.point_values();
            if let Some(ref values) = values {
                let leaf_max_value = values.max_packed_value(field)?;
                if leaf_max_value.is_empty() {
                    continue;
                }
                if max_value.is_empty() {
                    max_value = leaf_max_value.clone();
                } else {
                    let num_dimensions = values.num_dimensions(field)?;
                    let num_bytes_per_dimension = values.bytes_per_dimension(field)?;
                    for i in 0..num_dimensions {
                        let offset: usize = i * num_bytes_per_dimension;
                        if leaf_max_value[offset..offset + num_bytes_per_dimension]
                            > max_value[offset..offset + num_bytes_per_dimension]
                        {
                            max_value[offset..offset + num_bytes_per_dimension].copy_from_slice(
                                &leaf_max_value[offset..offset + num_bytes_per_dimension],
                            );
                        }
                    }
                }
            }
        }
    }
    Ok(max_value)
}

/// Used by {@link #intersect} to check how each recursive cell corresponds to the query.
#[derive(Eq, PartialEq)]
pub enum Relation {
    /// Return this if the cell is fully contained by the query
    CellInsideQuery,
    /// Return this if the cell and query do not overlap
    CellOutsideQuery,
    /// Return this if the cell partially overlaps the query
    CellCrossesQuery,
}

/// We recurse the BKD tree, using a provided instance of this to guide the recursion.
pub trait IntersectVisitor {
    /// Called for all documents in a leaf cell that's fully contained by the query.
    /// The consumer should blindly accept the docID.
    fn visit(&mut self, doc_id: DocId) -> Result<()>;

    /// Called for all documents in a leaf cell that crosses the query.  The consumer
    /// should scrutinize the packedValue to decide whether to accept it.  In the 1D case,
    /// values are visited in increasing order, and in the case of ties, in increasing
    /// docID order.
    fn visit_by_packed_value(&mut self, doc_id: DocId, packed_value: &[u8]) -> Result<()>;

    /// Called for non-leaf cells to test how the cell relates to the query, to
    /// determine how to further recurse down the tree.
    fn compare(&self, min_packed_value: &[u8], max_packed_value: &[u8]) -> Relation;

    /// Notifies the caller that this many documents (from one block) are about
    /// to be visited
    fn grow(&mut self, _count: usize) {}
}

/// Maximum number of bytes for each dimension
pub const MAX_NUM_BYTES: u32 = 16;

/// Maximum number of dimensions
pub const MAX_DIMENSIONS: u32 = 8; // TODO should be replaced by BKDWriter.MAX_DIMS
