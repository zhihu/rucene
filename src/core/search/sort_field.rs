use core::index::LeafReader;
use core::index::SortedNumericDocValuesRef;
use core::index::{NumericDocValues, NumericDocValuesContext, NumericDocValuesRef};
use core::search::field_comparator::*;
use core::util::numeric::{sortable_double_bits, sortable_float_bits};
use core::util::BitsRef;
use core::util::VariantValue;

use error::ErrorKind::IllegalArgument;
use error::Result;

use std::sync::Arc;

#[derive(PartialEq, Debug, Clone, Copy, Eq)]
pub enum SortFieldType {
    String,
    Score,
    Doc,
    Long,
    Int,
    Double,
    Float,
    /// Sort using a custom Comparator.  Sort values are any Comparable and
    /// sorting is done according to natural order.
    Custom,
}

#[derive(PartialEq, Debug, Clone, Copy, Eq)]
pub enum SortedSetSelectorType {
    Min,
    Max,
    MiddleMin,
    MiddleMax,
}

#[derive(PartialEq, Debug, Clone, Copy, Eq)]
pub enum SortedNumericSelectorType {
    Min,
    Max,
}

#[derive(PartialEq, Debug, Clone, Copy, Eq)]
pub enum SortFieldMissingValue {
    StringLast,
    StringFirst,
}

#[derive(Clone, Eq, PartialEq)]
pub enum SortField {
    Simple(SimpleSortField),
    SortedNumeric(SortedNumericSortField),
    // SortedSet(SortedSetSortField),
}

impl SortField {
    pub fn new_score() -> Self {
        SortField::Simple(SimpleSortField::new_score())
    }

    pub fn field(&self) -> &str {
        match self {
            SortField::Simple(s) => &s.field,
            SortField::SortedNumeric(s) => &s.raw_field.field,
        }
    }

    pub fn field_type(&self) -> SortFieldType {
        match self {
            SortField::Simple(s) => s.field_type,
            SortField::SortedNumeric(s) => s.raw_field.field_type,
        }
    }

    pub fn is_reverse(&self) -> bool {
        match self {
            SortField::Simple(s) => s.is_reverse,
            SortField::SortedNumeric(s) => s.raw_field.is_reverse,
        }
    }

    pub fn missing_value(&self) -> Option<&VariantValue> {
        match self {
            SortField::Simple(s) => s.missing_value.as_ref(),
            SortField::SortedNumeric(s) => s.raw_field.missing_value.as_ref(),
        }
    }

    pub fn needs_scores(&self) -> bool {
        match self {
            SortField::Simple(s) => s.needs_scores(),
            SortField::SortedNumeric(s) => s.raw_field.needs_scores(),
        }
    }

    pub fn set_missing_value(&mut self, value: Option<VariantValue>) {
        match self {
            SortField::Simple(s) => {
                s.missing_value = value;
            }
            SortField::SortedNumeric(s) => {
                s.raw_field.missing_value = value;
            }
        }
    }

    pub fn get_comparator(
        &self,
        num_hits: usize,
        missing_value: Option<&VariantValue>,
    ) -> Box<FieldComparator> {
        match self {
            SortField::Simple(s) => s.get_comparator(num_hits, missing_value),
            SortField::SortedNumeric(s) => s.get_comparator(num_hits, missing_value),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SimpleSortField {
    field: String,
    field_type: SortFieldType,
    is_reverse: bool,
    missing_value: Option<VariantValue>,
}

impl SimpleSortField {
    pub fn new(field: String, field_type: SortFieldType, is_reverse: bool) -> SimpleSortField {
        SimpleSortField {
            field,
            field_type,
            is_reverse,
            missing_value: None,
        }
    }

    pub fn new_score() -> SimpleSortField {
        SimpleSortField {
            field: String::new(),
            field_type: SortFieldType::Score,
            is_reverse: false,
            missing_value: None,
        }
    }

    pub fn field(&self) -> &String {
        &self.field
    }

    pub fn field_type(&self) -> SortFieldType {
        self.field_type
    }

    pub fn is_reverse(&self) -> bool {
        self.is_reverse
    }

    pub fn reverse(&self) -> i32 {
        if self.is_reverse {
            -1
        } else {
            1
        }
    }

    pub fn get_comparator(
        &self,
        num_hits: usize,
        missing_value: Option<&VariantValue>,
    ) -> Box<FieldComparator> {
        match self.field_type {
            SortFieldType::Score => Box::new(RelevanceComparator::new(num_hits)),
            SortFieldType::Doc => Box::new(DocComparator::new(num_hits)),
            SortFieldType::String => {
                unimplemented!();
            }
            _ => {
                // debug_assert!(missing_value.is_some());
                let missing_value = match self.field_type {
                    SortFieldType::Double => {
                        missing_value.unwrap_or(&VariantValue::Double(0.0)).clone()
                    }
                    SortFieldType::Int => missing_value.unwrap_or(&VariantValue::Int(0)).clone(),
                    SortFieldType::Long => missing_value.unwrap_or(&VariantValue::Long(0)).clone(),
                    SortFieldType::Float => {
                        missing_value.unwrap_or(&VariantValue::Float(0.0)).clone()
                    }
                    _ => missing_value.unwrap().clone(),
                };
                Box::new(NumericDocValuesComparator::new(
                    num_hits,
                    self.field.clone(),
                    self.field_type,
                    missing_value,
                    DefaultDocValuesSource::default(),
                ))
            }
        }
    }

    pub fn needs_scores(&self) -> bool {
        self.field_type == SortFieldType::Score
    }
}

/// SortField for `SortedNumericDocValues`
/// A SortedNumericDocValues contains multiple values for a field, so sorting with
/// this technique "selects" a value as the representative sort value for the document.
///
/// By default, the minimum value in the list is selected as the sort value, but
/// this can be customized.
///
/// Like sorting by string, this also supports sorting missing values as first or last,
/// via {@link #setMissingValue(Object)}.
#[derive(Clone, Eq, PartialEq)]
pub struct SortedNumericSortField {
    selector: SortedNumericSelectorType,
    real_type: SortFieldType,
    raw_field: SimpleSortField,
}

impl SortedNumericSortField {
    pub fn with_field(field: String, real_type: SortFieldType) -> Self {
        Self::with_default_selector(field, real_type, false)
    }

    pub fn with_default_selector(field: String, real_type: SortFieldType, reverse: bool) -> Self {
        Self::new(field, real_type, reverse, SortedNumericSelectorType::Min)
    }

    pub fn new(
        field: String,
        real_type: SortFieldType,
        reverse: bool,
        selector: SortedNumericSelectorType,
    ) -> Self {
        let raw_field = SimpleSortField::new(field, SortFieldType::Custom, reverse);
        SortedNumericSortField {
            selector,
            raw_field,
            real_type,
        }
    }

    /// Returns the numeric type in use for this sort
    pub fn numeric_type(&self) -> SortFieldType {
        self.real_type
    }

    pub fn selector(&self) -> SortedNumericSelectorType {
        self.selector
    }

    pub fn get_comparator(
        &self,
        num_hits: usize,
        missing_value: Option<&VariantValue>,
    ) -> Box<FieldComparator> {
        debug_assert!(missing_value.is_some());
        Box::new(NumericDocValuesComparator::new(
            num_hits,
            self.raw_field.field.clone(),
            self.raw_field.field_type,
            missing_value.unwrap().clone(),
            SortedWrapperDocValuesSource::new(self.selector, self.real_type),
        ))
    }
}

struct SortedWrapperDocValuesSource {
    selector: SortedNumericSelectorType,
    field_type: SortFieldType,
}

impl SortedWrapperDocValuesSource {
    fn new(selector: SortedNumericSelectorType, field_type: SortFieldType) -> Self {
        SortedWrapperDocValuesSource {
            selector,
            field_type,
        }
    }
}

impl DocValuesSource for SortedWrapperDocValuesSource {
    fn numeric_doc_values(&self, reader: &LeafReader, field: &str) -> Result<NumericDocValuesRef> {
        SortedNumericSelector::wrap(
            reader.get_sorted_numeric_doc_values(field)?,
            self.selector,
            self.field_type,
        )
    }

    fn docs_with_fields(&self, reader: &LeafReader, field: &str) -> Result<BitsRef> {
        reader.get_docs_with_field(field)
    }
}

/// Selects a value from the document's list to use as the representative value
///
/// This provides a NumericDocValues view over the SortedNumeric, for use with sorting,
/// expressions, function queries, etc.
pub struct SortedNumericSelector;

impl SortedNumericSelector {
    pub fn wrap(
        sorted_numeric: SortedNumericDocValuesRef,
        selector: SortedNumericSelectorType,
        numeric_type: SortFieldType,
    ) -> Result<NumericDocValuesRef> {
        if numeric_type != SortFieldType::Int
            && numeric_type != SortFieldType::Long
            && numeric_type != SortFieldType::Float
            && numeric_type != SortFieldType::Double
        {
            bail!(IllegalArgument(
                "numeric_type must be a numeric type".into()
            ));
        }
        let view = match selector {
            SortedNumericSelectorType::Min => {
                SortedNumAsNumDocValuesEnum::Min(SortedNumAsNumDocValuesMin::new(sorted_numeric))
            }
            SortedNumericSelectorType::Max => {
                SortedNumAsNumDocValuesEnum::Max(SortedNumAsNumDocValuesMax::new(sorted_numeric))
            }
        };
        let res: NumericDocValuesRef = match numeric_type {
            SortFieldType::Float => Arc::new(SortableFloatNumericDocValues::new(view)),
            SortFieldType::Double => Arc::new(SortableDoubleNumericDocValues::new(view)),
            _ => Arc::new(view),
        };
        Ok(res)
    }
}

struct SortableFloatNumericDocValues {
    doc_values: SortedNumAsNumDocValuesEnum,
}

impl SortableFloatNumericDocValues {
    fn new(doc_values: SortedNumAsNumDocValuesEnum) -> Self {
        SortableFloatNumericDocValues { doc_values }
    }
}

impl NumericDocValues for SortableFloatNumericDocValues {
    fn get_with_ctx(
        &self,
        ctx: NumericDocValuesContext,
        doc_id: i32,
    ) -> Result<(i64, NumericDocValuesContext)> {
        let (value, ctx) = self.doc_values.get_with_ctx(ctx, doc_id)?;
        let res = sortable_float_bits(value as i32) as i64;
        Ok((res, ctx))
    }
}

struct SortableDoubleNumericDocValues {
    doc_values: SortedNumAsNumDocValuesEnum,
}

impl SortableDoubleNumericDocValues {
    fn new(doc_values: SortedNumAsNumDocValuesEnum) -> Self {
        SortableDoubleNumericDocValues { doc_values }
    }
}

impl NumericDocValues for SortableDoubleNumericDocValues {
    fn get_with_ctx(
        &self,
        ctx: NumericDocValuesContext,
        doc_id: i32,
    ) -> Result<(i64, NumericDocValuesContext)> {
        let (value, ctx) = self.doc_values.get_with_ctx(ctx, doc_id)?;
        Ok((sortable_double_bits(value), ctx))
    }
}

enum SortedNumAsNumDocValuesEnum {
    Min(SortedNumAsNumDocValuesMin),
    Max(SortedNumAsNumDocValuesMax),
}

impl NumericDocValues for SortedNumAsNumDocValuesEnum {
    fn get_with_ctx(
        &self,
        ctx: NumericDocValuesContext,
        doc_id: i32,
    ) -> Result<(i64, NumericDocValuesContext)> {
        match self {
            SortedNumAsNumDocValuesEnum::Min(m) => m.get_with_ctx(ctx, doc_id),
            SortedNumAsNumDocValuesEnum::Max(m) => m.get_with_ctx(ctx, doc_id),
        }
    }
}

struct SortedNumAsNumDocValuesMin {
    doc_values: SortedNumericDocValuesRef,
}

impl SortedNumAsNumDocValuesMin {
    fn new(doc_values: SortedNumericDocValuesRef) -> Self {
        SortedNumAsNumDocValuesMin { doc_values }
    }
}

impl NumericDocValues for SortedNumAsNumDocValuesMin {
    fn get_with_ctx(
        &self,
        _ctx: NumericDocValuesContext,
        doc_id: i32,
    ) -> Result<(i64, NumericDocValuesContext)> {
        let ctx = self.doc_values.set_document(None, doc_id)?;
        if self.doc_values.count(&ctx) == 0 {
            Ok((0, None))
        } else {
            Ok((self.doc_values.value_at(&ctx, 0)?, None))
        }
    }
}

struct SortedNumAsNumDocValuesMax {
    doc_values: SortedNumericDocValuesRef,
}

impl SortedNumAsNumDocValuesMax {
    fn new(doc_values: SortedNumericDocValuesRef) -> Self {
        SortedNumAsNumDocValuesMax { doc_values }
    }
}

impl NumericDocValues for SortedNumAsNumDocValuesMax {
    fn get_with_ctx(
        &self,
        _ctx: NumericDocValuesContext,
        doc_id: i32,
    ) -> Result<(i64, NumericDocValuesContext)> {
        let ctx = self.doc_values.set_document(None, doc_id)?;
        let count = self.doc_values.count(&ctx);
        if count == 0 {
            Ok((0, None))
        } else {
            Ok((self.doc_values.value_at(&ctx, count - 1)?, None))
        }
    }
}

/// SortField for {@link SortedSetDocValues}.
///
/// A SortedSetDocValues contains multiple values for a field, so sorting with
/// this technique "selects" a value as the representative sort value for the document.
///
/// By default, the minimum value in the set is selected as the sort value, but
/// this can be customized. Selectors other than the default do have some limitations
/// to ensure that all selections happen in constant-time for performance.
///
/// Like sorting by string, this also supports sorting missing values as first or last,
/// via {@link #setMissingValue(Object)}.
/// @see SortedSetSelector
/// TODO, may implement later
struct SortedSetSortField {
    selector: SortedSetSelectorType,
    raw_field: SimpleSortField,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sort_field_with_score_type() {
        let sort_field = SortField::Simple(SimpleSortField::new(
            String::from("test"),
            SortFieldType::Score,
            true,
        ));

        assert_eq!("test", sort_field.field());
        assert_eq!(SortFieldType::Score, sort_field.field_type());
        assert_eq!(true, sort_field.is_reverse());
    }

    #[test]
    fn test_sort_field_with_doc_type() {
        let sort_field = SortField::Simple(SimpleSortField::new(
            String::from("test"),
            SortFieldType::Doc,
            true,
        ));

        assert_eq!("test", sort_field.field());
        assert_eq!(SortFieldType::Doc, sort_field.field_type());
        assert_eq!(true, sort_field.is_reverse());
    }
}
