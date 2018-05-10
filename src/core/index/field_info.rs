use error::ErrorKind::{IllegalArgument, IllegalState};
use error::*;
use std::cmp::max;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::mem::discriminant;
use std::result;
use std::sync::Arc;

use serde::ser::SerializeStruct;
use serde::{Serialize, Serializer};

use core::attribute::{OffsetAttribute, PayloadAttribute, PositionIncrementAttribute};
use core::index::term::*;
use core::index::{DocValuesType, IndexOptions};

fn variant_eq<T>(a: &T, b: &T) -> bool {
    discriminant(a) == discriminant(b)
}

#[derive(Clone, Serialize, Debug)]
pub struct FieldInfo {
    pub name: String,
    pub number: i32,
    pub doc_values_type: DocValuesType,
    pub has_store_term_vector: bool,
    pub omit_norms: bool,
    pub index_options: IndexOptions,
    pub has_store_payloads: bool,
    pub attributes: HashMap<String, String>,
    pub dv_gen: i64,
    pub point_dimension_count: i32,
    pub point_num_bytes: i32,
}

impl FieldInfo {
    #[allow(too_many_arguments)]
    pub fn new(
        name: String,
        number: i32,
        store_term_vector: bool,
        omit_norms: bool,
        store_payloads: bool,
        index_options: IndexOptions,
        doc_values_type: DocValuesType,
        dv_gen: i64,
        attributes: HashMap<String, String>,
        point_dimension_count: i32,
        point_num_bytes: i32,
    ) -> Result<FieldInfo> {
        let info = FieldInfo {
            name,
            number,
            doc_values_type,
            has_store_term_vector: store_term_vector,
            omit_norms,
            index_options,
            has_store_payloads: store_payloads,
            attributes,
            dv_gen,
            point_dimension_count,
            point_num_bytes,
        };

        assert!(info.check_consistency()?);
        Ok(info)
    }

    fn check_consistency(&self) -> Result<bool> {
        if let IndexOptions::Null = self.index_options {
            if self.has_store_term_vector {
                bail!(IllegalState(format!(
                    "Illegal State: non-indexed field '{}' cannot store term vectors",
                    &self.name
                )));
            }
            if self.has_store_payloads {
                bail!(IllegalState(format!(
                    "Illegal State: non-indexed field '{}' cannot store payloads",
                    &self.name
                )));
            }
            if self.omit_norms {
                bail!(IllegalState(format!(
                    "Illegal State: non-indexed field '{}' cannot omit norms",
                    &self.name
                )));
            }
        } else {
            let opt = match self.index_options {
                IndexOptions::Null => true,
                IndexOptions::Docs => true,
                IndexOptions::DocsAndFreqs => true,
                _ => false,
            };

            if opt && self.has_store_payloads {
                bail!(IllegalState(format!(
                    "Illegal State: indexed field '{}' cannot have payloads without positions",
                    &self.name
                )));
            }
        }

        if self.point_dimension_count < 0 {
            bail!(IllegalState(format!(
                "Illegal State pointDimensionCount must be >= 0; got {}",
                self.point_dimension_count
            )));
        }

        if self.point_num_bytes < 0 {
            bail!(IllegalState(format!(
                "Illegal State pointNumBytes must be >= 0; got {}",
                self.point_num_bytes
            )));
        }

        if self.point_dimension_count != 0 && self.point_num_bytes == 0 {
            bail!(IllegalState(format!(
                "Illegal State: pointNumBytes must be > 0 when pointDimensionCount={}",
                self.point_dimension_count
            )));
        }

        if self.point_num_bytes != 0 && self.point_dimension_count == 0 {
            bail!(IllegalState(format!(
                "Illegal State: pointDimensionCount must be > 0 when pointNumBytes={}",
                self.point_num_bytes
            )));
        }

        if self.dv_gen != -1 && match self.doc_values_type {
            DocValuesType::Null => true,
            _ => false,
        } {
            bail!(IllegalState(format!(
                "Illegal State: field '{}' cannot have a docvalues update generation without \
                 having docvalues",
                &self.name
            )));
        }

        Ok(true)
    }

    pub fn set_doc_values_type(&mut self, dtype: DocValuesType) -> Result<()> {
        if !self.doc_values_type.null() && !dtype.null()
            && variant_eq(&self.doc_values_type, &dtype)
        {
            bail!(IllegalArgument(format!(
                "Illegal Argument: cannot change DocValues type from {:?} to {:?} for field \"{}\"",
                self.doc_values_type, dtype, self.name
            )));
        }
        assert!(self.check_consistency()?);
        Ok(())
    }

    pub fn has_norms(&self) -> bool {
        match self.index_options {
            IndexOptions::Null => false,
            _ => !self.omit_norms,
        }
    }
}

impl fmt::Display for FieldInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Ok(s) = ::serde_json::to_string_pretty(self) {
            write!(f, "{}", s)?;
        }

        Ok(())
    }
}

pub type FieldInfoRef = Arc<FieldInfo>;

/// This class tracks the number and position / offset parameters of terms
/// being added to the index. The information collected in this class is
/// also used to calculate the normalization factor for a field.
pub struct FieldInvertState {
    pub name: String,
    pub position: i32,
    pub length: i32,
    pub num_overlap: i32,
    pub offset: i32,
    pub max_term_frequency: i32,
    pub unique_term_count: i32,
    pub boost: f32,

    // we must track these across field instances (multi-valued case)
    pub last_start_offset: i32,
    pub last_position: i32,
    // attribute_source: AttributeSource,
    pub offset_attribute: OffsetAttribute,
    pub pos_incr_attribute: PositionIncrementAttribute,
    pub payload_attribute: PayloadAttribute,
    // term_attribute: TermToBytesRefAttribute,
}

#[derive(Clone)]
pub struct FieldInfos {
    pub has_freq: bool,
    pub has_prox: bool,
    pub has_payloads: bool,
    pub has_offsets: bool,
    pub has_vectors: bool,
    pub has_norms: bool,
    pub has_doc_values: bool,
    pub has_point_values: bool,

    pub by_number: BTreeMap<i32, FieldInfoRef>,
    pub by_name: HashMap<String, FieldInfoRef>,
}

impl Serialize for FieldInfos {
    fn serialize<S>(&self, serializer: S) -> result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_struct("FieldInfos", 9)?;
        s.serialize_field("has_freq", &self.has_freq)?;
        s.serialize_field("has_prox", &self.has_prox)?;
        s.serialize_field("has_payloads", &self.has_payloads)?;
        s.serialize_field("has_offsets", &self.has_offsets)?;
        s.serialize_field("has_vectors", &self.has_vectors)?;
        s.serialize_field("has_norms", &self.has_norms)?;
        s.serialize_field("has_doc_values", &self.has_doc_values)?;
        s.serialize_field("has_point_values", &self.has_point_values)?;

        let fields: HashMap<&String, &FieldInfo> = self.by_name
            .iter()
            .map(|pair| (pair.0, pair.1.as_ref()))
            .collect();
        s.serialize_field("fields", &fields)?;
        s.end()
    }
}

impl fmt::Display for FieldInfos {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Ok(s) = ::serde_json::to_string_pretty(self) {
            write!(f, "{}", s)?;
        }

        Ok(())
    }
}

impl FieldInfos {
    pub fn new(infos: Vec<FieldInfo>) -> Result<FieldInfos> {
        let mut has_vectors = false;
        let mut has_prox = false;
        let mut has_payloads = false;
        let mut has_offsets = false;
        let mut has_freq = false;
        let mut has_norms = false;
        let mut has_doc_values = false;
        let mut has_point_values = false;

        let mut by_number: BTreeMap<i32, FieldInfoRef> = BTreeMap::new();
        let mut by_name: HashMap<String, FieldInfoRef> = HashMap::new();
        let mut infos = infos;
        let mut max_number = 0;
        for info in infos.drain(..) {
            let info = Arc::new(info);
            let number = info.number;
            if number < 0 {
                bail!(
                    "Illegal Argument: illegal field number: {} for field {}",
                    number,
                    &info.name
                );
            }

            max_number = max(max_number, number);

            {
                let index_options = &info.index_options;
                has_vectors |= info.has_store_term_vector;
                has_prox |= index_options.has_positions();
                has_freq |= index_options.has_freqs();
                has_offsets |= index_options.has_offsets();
                has_norms |= info.has_norms();
                has_doc_values |= !info.doc_values_type.null();
                has_payloads |= info.has_store_payloads;
                has_point_values |= info.point_dimension_count != 0;
            }

            if let Some(previous) = by_number.insert(number, info.clone()) {
                let info = &by_number[&number];
                bail!(IllegalArgument(format!(
                    "Illegal Argument: duplicated field numbers: {} and {} have: {}",
                    previous.name, &info.name, number
                )));
            }

            let name = info.name.clone();
            if let Some(previous) = by_name.insert(name.clone(), info) {
                bail!(IllegalArgument(format!(
                    "Illegal Argument: duplicated field names: {} and {} have: {}",
                    previous.number, number, &name
                )));
            }
        }
        Ok(FieldInfos {
            has_freq,
            has_prox,
            has_payloads,
            has_offsets,
            has_vectors,
            has_norms,
            has_doc_values,
            has_point_values,
            by_number,
            by_name,
        })
    }

    pub fn field_info_by_number(&self, field_number: i32) -> Option<&FieldInfo> {
        self.by_number.get(&field_number).map(Arc::as_ref)
    }

    pub fn field_info_by_name(&self, field_name: &str) -> Option<&FieldInfo> {
        self.by_name.get(field_name).map(Arc::as_ref)
    }
}

pub trait Fields: Send + Sync {
    fn fields(&self) -> Vec<String>;
    fn terms(&self, field: &str) -> Result<Option<TermsRef>>;
    fn size(&self) -> usize;
}
