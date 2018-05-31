use core::doc::FieldType;
use core::index::{DocValuesType, IndexOptions, LeafReader};
use core::search::boolean_query::BooleanQuery;
use core::search::match_all::ConstantScoreQuery;
use core::search::Query;
use core::util::VariantValue;
use error::*;

pub struct MappedFieldType {
    pub name: String,
    #[allow(dead_code)]
    boost: f32,
    pub field_type: FieldType,
    pub none_value: Option<VariantValue>,
    pub none_value_string: String, // for sending null value to _all field
}

impl MappedFieldType {
    pub fn new() -> MappedFieldType {
        let field_type = FieldType::new(
            false,
            true,
            false,
            false,
            false,
            false,
            false,
            IndexOptions::DocsAndFreqsAndPositions,
            DocValuesType::Null,
        );
        MappedFieldType {
            name: String::new(),
            boost: 1f32,
            field_type,
            none_value: None,
            none_value_string: String::new(),
        }
    }

    pub fn support_doc_values(&self) -> bool {
        self.field_type.doc_values_type() == DocValuesType::Null
    }
}

impl Default for MappedFieldType {
    fn default() -> Self {
        MappedFieldType::new()
    }
}

/// This defines the core properties and functions to operate on a field.
pub trait FieldMapper {
    fn mapped_field_type(&self) -> &MappedFieldType;

    /// Returns the name of this type, as would be specified in mapping properties
    fn type_name(&self) -> &str;

    fn is_searchable(&self) -> bool {
        *(self.mapped_field_type().field_type.index_options()) != IndexOptions::Null
    }

    fn term_query(&self, _value: &VariantValue, _ctx: &LeafReader) -> Result<Box<Query>> {
        unimplemented!();
    }

    fn terms_query(&self, values: &[VariantValue], ctx: &LeafReader) -> Result<Box<Query>> {
        if values.is_empty() {
            bail!(ErrorKind::IllegalArgument("values can't be empty!".into()));
        }
        if values.len() == 1 {
            self.term_query(&values[0], ctx)
        } else {
            let mut sub_queries = Vec::with_capacity(values.len());
            for value in values {
                sub_queries.push(self.term_query(value, ctx)?);
            }
            BooleanQuery::build(Vec::with_capacity(0), sub_queries, Vec::with_capacity(0))
        }
    }

    fn range_query(
        &self,
        _lower_term: VariantValue,
        _upper_term: VariantValue,
        _include_lower: bool,
        _include_upper: bool,
    ) -> Result<Box<Query>> {
        bail!(ErrorKind::IllegalArgument("values can't be empty!".into()))
    }

    fn prefix_query(&self, _value: &str, _ctx: &LeafReader) -> Result<Box<Query>> {
        bail!(ErrorKind::IllegalArgument("values can't be empty!".into()))
    }

    fn none_value_query(&self, ctx: &LeafReader) -> Result<Box<Query>> {
        if let Some(ref none_value) = self.mapped_field_type().none_value {
            Ok(Box::new(ConstantScoreQuery::new(
                self.term_query(&none_value, ctx)?,
            )))
        } else {
            bail!(ErrorKind::IllegalArgument(
                "none value is empty!".to_owned()
            ))
        }
    }

    fn fail_if_no_doc_values(&self) -> Result<()> {
        if !self.mapped_field_type().support_doc_values() {
            bail!(ErrorKind::IllegalArgument(format!(
                "fielddata is unsupported on fields of type [{}]",
                self.type_name()
            )));
        }
        Ok(())
    }
}

// const FIELD_NAME_INDEX: &'static str = "_index";
//
//// field type for field `_index`
// pub const FIELD_TYPE_INDEX: MappedFieldType = MappedFieldType {
//    field_type: FieldType::new(false, false, false, false, false, false, true, IndexOptions::Null,
//        DocValuesType::Null),
//    name: String::from(FIELD_NAME_INDEX),
//    boost: 1f32,
//    none_value: None,
//    none_value_string: String::new()
//};
//
//// field type for `_type`
// const FIELD_NAME_TYPE: &'static str = "_type";
// pub const FIELD_TYPE_TYPE: MappedFieldType = MappedFieldType {
//    field_type: FieldType::new(false, false, false, false, false, false, true, IndexOptions::Docs,
//        DocValuesType::Null),
//    name: String::from(FIELD_NAME_TYPE),
//    boost: 1f32,
//    none_value: None,
//    none_value_string: String::from("")
//};
//
//// field type for `_version`
// const FIELD_NAME_VERSION: &'static str = "_version";
// pub const FIELD_TYPE_VERSION: MappedFieldType = MappedFieldType {
//    field_type: FieldType::new(false, false, false, false, false, false, true, IndexOptions::Null,
//                               DocValuesType::Numeric),
//    name: String::from(FIELD_NAME_VERSION),
//    boost: 1f32,
//    none_value: None,
//    none_value_string: "".to_owned()
//};
