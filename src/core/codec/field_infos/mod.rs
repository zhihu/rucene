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

mod field_infos_format;

pub use self::field_infos_format::*;

use error::ErrorKind::{IllegalArgument, IllegalState};
use error::Result;

use std::cmp::max;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::iter::Iterator;
use std::result;
use std::sync::{Arc, Mutex, RwLock};

use serde::ser::SerializeStruct;
use serde::{Serialize, Serializer};

// use core::attribute::{OffsetAttribute, PayloadAttribute, PositionIncrementAttribute};
use core::codec::points::{MAX_DIMENSIONS, MAX_NUM_BYTES};
use core::codec::postings::{PER_FIELD_POSTING_FORMAT_KEY, PER_FIELD_POSTING_SUFFIX_KEY};
use core::doc::{DocValuesType, IndexOptions};

/// Access to the Field Info file that describes document fields and whether or
/// not they are indexed. Each segment has a separate Field Info file. Objects
/// of this class are thread-safe for multiple readers, but only one thread can
/// be adding documents at a time, with no other reader or writer threads
/// accessing this object.
#[derive(Clone, Debug)]
pub struct FieldInfo {
    pub name: String,
    pub number: u32,
    pub doc_values_type: DocValuesType,
    pub has_store_term_vector: bool,
    pub omit_norms: bool,
    pub index_options: IndexOptions,
    pub has_store_payloads: bool,
    pub attributes: Arc<RwLock<HashMap<String, String>>>,
    pub dv_gen: i64,
    pub point_dimension_count: u32,
    pub point_num_bytes: u32,
}

impl Serialize for FieldInfo {
    fn serialize<S>(&self, serializer: S) -> result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_struct("FieldInfo", 11)?;
        s.serialize_field("name", &self.name)?;
        s.serialize_field("number", &self.number)?;
        s.serialize_field("doc_values_type", &self.doc_values_type)?;
        s.serialize_field("has_store_term_vector", &self.has_store_term_vector)?;
        s.serialize_field("omit_norms", &self.omit_norms)?;
        s.serialize_field("index_options", &self.index_options)?;
        s.serialize_field("has_store_payloads", &self.has_store_payloads)?;
        s.serialize_field("attributes", &*self.attributes.read().unwrap())?;
        s.serialize_field("dv_gen", &self.dv_gen)?;
        s.serialize_field("point_dimension_count", &self.point_dimension_count)?;
        s.serialize_field("point_num_bytes", &self.point_num_bytes)?;
        s.end()
    }
}

impl FieldInfo {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: String,
        number: u32,
        store_term_vector: bool,
        omit_norms: bool,
        store_payloads: bool,
        index_options: IndexOptions,
        doc_values_type: DocValuesType,
        dv_gen: i64,
        attributes: HashMap<String, String>,
        point_dimension_count: u32,
        point_num_bytes: u32,
    ) -> Result<FieldInfo> {
        let info = FieldInfo {
            name,
            number,
            doc_values_type,
            has_store_term_vector: store_term_vector,
            omit_norms,
            index_options,
            has_store_payloads: store_payloads,
            attributes: Arc::new(RwLock::new(attributes)),
            dv_gen,
            point_dimension_count,
            point_num_bytes,
        };

        info.check_consistency()?;
        Ok(info)
    }

    pub fn check_consistency(&self) -> Result<()> {
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
        // if self.omit_norms {
        //     bail!(IllegalState(format!(
        //         "Illegal State: non-indexed field '{}' cannotannot omit norms",
        //         &self.name
        //     )));
        // }
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

        if self.dv_gen != -1
            && match self.doc_values_type {
                DocValuesType::Null => true,
                _ => false,
            }
        {
            bail!(IllegalState(format!(
                "Illegal State: field '{}' cannot have a docvalues update generation without \
                 having docvalues",
                &self.name
            )));
        }

        Ok(())
    }

    pub fn set_doc_values_type(&mut self, dv_type: DocValuesType) -> Result<()> {
        if !self.doc_values_type.null() && !dv_type.null() && self.doc_values_type != dv_type {
            bail!(IllegalArgument(format!(
                "Illegal Argument: cannot change DocValues type from {:?} to {:?} for field \"{}\"",
                self.doc_values_type, dv_type, self.name
            )));
        }
        self.doc_values_type = dv_type;
        assert!(self.check_consistency().is_ok());
        Ok(())
    }

    pub fn set_doc_values_gen(&mut self, gen: i64) -> i64 {
        let old_gen = self.dv_gen;
        self.dv_gen = gen;
        old_gen
    }

    pub fn set_store_payloads(&mut self) {
        if self.index_options != IndexOptions::Null
            && self.index_options >= IndexOptions::DocsAndFreqsAndPositions
        {
            self.has_store_payloads = true;
        }
        debug_assert!(self.check_consistency().is_ok());
    }

    pub fn set_index_options(&mut self, new_options: IndexOptions) {
        if self.index_options != new_options {
            if self.index_options == IndexOptions::Null {
                self.index_options = new_options;
            } else if new_options != IndexOptions::Null {
                // downgrade
                if self.index_options > new_options {
                    self.index_options = new_options;
                }
            }
        }

        if self.index_options < IndexOptions::DocsAndFreqsAndPositions {
            // cannot store payloads if we don't store positions:
            self.has_store_payloads = false;
        }
    }

    pub fn set_dimensions(&mut self, dimension_count: u32, dimension_num_bytes: u32) -> Result<()> {
        if self.point_dimension_count == 0 && dimension_count > 0 {
            self.point_dimension_count = dimension_count;
            self.point_num_bytes = dimension_num_bytes;
        } else if dimension_count != 0
            && (self.point_dimension_count != dimension_count
                || self.point_num_bytes != dimension_num_bytes)
        {
            bail!(IllegalArgument(format!(
                "cannot change field '{}' dimension count or dimension_num_bytes",
                self.name
            )));
        }
        Ok(())
    }

    pub fn has_norms(&self) -> bool {
        match self.index_options {
            IndexOptions::Null => false,
            _ => !self.omit_norms,
        }
    }

    fn update(
        &mut self,
        store_term_vector: bool,
        omit_norms: bool,
        store_payloads: bool,
        index_options: IndexOptions,
        dimension_count: u32,
        dimension_num_bytes: u32,
    ) -> Result<()> {
        if self.index_options != index_options {
            if self.index_options == IndexOptions::Null {
                self.index_options = index_options;
            } else if index_options != IndexOptions::Null {
                // downgrade
                if self.index_options > index_options {
                    self.index_options = index_options;
                }
            }
        }

        self.set_dimensions(dimension_count, dimension_num_bytes)?;

        // if updated field data is not for indexing, leave the updates out
        // once vector, always vector
        if self.index_options != IndexOptions::Null {
            self.has_store_term_vector |= store_term_vector;
            self.has_store_payloads |= store_payloads;

            // Awkward: only drop norms if incoming update is indexed:
            if index_options != IndexOptions::Null && self.omit_norms != omit_norms {
                // if one require omitNorms at least once, it remains off for life
                self.omit_norms = true;
            }
        }

        if self.index_options == IndexOptions::Null
            || self.index_options < IndexOptions::DocsAndFreqsAndPositions
        {
            // cannot store payloads if we don't store positions:
            self.has_store_payloads = false;
        }

        debug_assert!(self.check_consistency().is_ok());
        Ok(())
    }

    pub fn attribute(&self, key: &str) -> Option<String> {
        self.attributes.read().unwrap().get(key).cloned()
    }

    pub fn put_attribute(&self, key: String, value: String) -> Option<String> {
        self.attributes.write().unwrap().insert(key, value)
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

/// This class tracks the number and position / offset parameters of terms
/// being added to the index. The information collected in this class is
/// also used to calculate the normalization factor for a field.
#[derive(Debug)]
pub struct FieldInvertState {
    pub name: String,
    pub position: i32,
    pub length: i32,
    pub num_overlap: i32,
    pub offset: usize,
    pub max_term_frequency: u32,
    pub unique_term_count: u32,
    pub boost: f32,

    // we must track these across field instances (multi-valued case)
    pub last_start_offset: i32,
    pub last_position: i32,
    /*    pub offset_attribute: OffsetAttribute,
     *    pub pos_incr_attribute: PositionIncrementAttribute,
     *    pub payload_attribute: PayloadAttribute,
     *    term_attribute: TermToBytesRefAttribute, */
}

impl FieldInvertState {
    pub fn with_name(name: String) -> Self {
        Self::new(name, 0, 0, 0, 0, 0.0f32)
    }

    pub fn new(
        name: String,
        position: i32,
        length: i32,
        num_overlap: i32,
        offset: usize,
        boost: f32,
    ) -> Self {
        FieldInvertState {
            name,
            position,
            length,
            num_overlap,
            offset,
            max_term_frequency: 0,
            unique_term_count: 0,
            boost,
            last_start_offset: 0,
            last_position: 0,
        }
    }

    pub fn reset(&mut self) {
        self.position = -1;
        self.length = 0;
        self.num_overlap = 0;
        self.offset = 0;
        self.max_term_frequency = 0;
        self.unique_term_count = 0;
        self.boost = 1.0f32;
        self.last_position = 0;
        self.last_start_offset = 0;
    }
}

/// Collection of `FieldInfo`s (accessible by number or by name).
#[derive(Clone, Debug)]
pub struct FieldInfos {
    pub has_freq: bool,
    pub has_prox: bool,
    pub has_payloads: bool,
    pub has_offsets: bool,
    pub has_vectors: bool,
    pub has_norms: bool,
    pub has_doc_values: bool,
    pub has_point_values: bool,

    pub by_number: BTreeMap<u32, Arc<FieldInfo>>,
    pub by_name: HashMap<String, Arc<FieldInfo>>,
}

impl Serialize for FieldInfos {
    fn serialize<S>(&self, serializer: S) -> ::std::result::Result<S::Ok, S::Error>
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

        let fields: HashMap<&String, &FieldInfo> = self
            .by_name
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

        let mut by_number: BTreeMap<u32, Arc<FieldInfo>> = BTreeMap::new();
        let mut by_name: HashMap<String, Arc<FieldInfo>> = HashMap::new();
        let mut max_number = 0;

        let mut infos = infos;
        for info in &mut infos {
            if info.index_options != IndexOptions::Null {
                // force put attr...
                info.put_attribute(
                    PER_FIELD_POSTING_FORMAT_KEY.to_string(),
                    "Lucene50".to_string(),
                );
                info.put_attribute(PER_FIELD_POSTING_SUFFIX_KEY.to_string(), "0".to_string());
            }
        }

        for info in infos {
            let info = Arc::new(info);
            let number = info.number;

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

    pub fn field_info_by_number(&self, field_number: u32) -> Option<&FieldInfo> {
        self.by_number.get(&field_number).map(Arc::as_ref)
    }

    pub fn field_info_by_name(&self, field_name: &str) -> Option<&FieldInfo> {
        self.by_name.get(field_name).map(Arc::as_ref)
    }

    pub fn len(&self) -> usize {
        self.by_name.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub struct FieldInfosBuilder<T: AsRef<FieldNumbers>> {
    pub by_name: HashMap<String, FieldInfo>,
    pub global_field_numbers: T,
}

impl Default for FieldInfosBuilder<FieldNumbers> {
    fn default() -> Self {
        Self::new(FieldNumbers::new())
    }
}

impl<T: AsRef<FieldNumbers>> FieldInfosBuilder<T> {
    pub fn new(global_field_numbers: T) -> Self {
        FieldInfosBuilder {
            by_name: HashMap::new(),
            global_field_numbers,
        }
    }

    #[allow(dead_code)]
    // TODO: used for doc values update
    pub fn add_infos(&mut self, other: &FieldInfos) -> Result<()> {
        for v in other.by_number.values() {
            self.add(v.as_ref())?;
        }
        Ok(())
    }

    /// Create a new field, or return existing one.
    pub fn get_or_add(&mut self, name: &str) -> Result<&mut FieldInfo> {
        if !self.by_name.contains_key(name) {
            // This field wasn't yet added to this in-RAM
            // segment's FieldInfo, so now we get a global
            // number for this field.  If the field was seen
            // before then we'll get the same name and number,
            // else we'll allocate a new one:
            let field_number = self.global_field_numbers.as_ref().add_or_get(
                name,
                0,
                DocValuesType::Null,
                0,
                0,
            )?;
            let fi = FieldInfo::new(
                name.to_string(),
                field_number,
                false,
                false,
                false,
                IndexOptions::Null,
                DocValuesType::Null,
                -1,
                HashMap::new(),
                0,
                0,
            )?;
            self.global_field_numbers.as_ref().verify_consistent(
                field_number,
                name,
                DocValuesType::Null,
            )?;
            self.by_name.insert(name.to_string(), fi);
        }
        Ok(self.by_name.get_mut(name).unwrap())
    }

    pub fn add(&mut self, fi: &FieldInfo) -> Result<()> {
        self.add_or_update_internal(
            &fi.name,
            fi.number,
            fi.has_store_term_vector,
            fi.omit_norms,
            fi.has_store_payloads,
            fi.index_options,
            fi.doc_values_type,
            fi.point_dimension_count,
            fi.point_num_bytes,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn add_or_update_internal(
        &mut self,
        name: &str,
        preferred_field_number: u32,
        store_term_vector: bool,
        omit_norms: bool,
        store_payloads: bool,
        index_options: IndexOptions,
        doc_values: DocValuesType,
        dimension_count: u32,
        dimension_num_bytes: u32,
    ) -> Result<()> {
        if self.by_name.contains_key(name) {
            let field_info = self.by_name.get_mut(name).unwrap();
            field_info.update(
                store_term_vector,
                omit_norms,
                store_payloads,
                index_options,
                dimension_count,
                dimension_num_bytes,
            )?;
            if doc_values != DocValuesType::Null {
                // Only pay the synchronization cost if fi does not already have a DVType
                if field_info.doc_values_type == DocValuesType::Null {
                    // Must also update docValuesType map so it's
                    // aware of this field's DocValuesType.  This will throw
                    // IllegalArgumentException if an illegal type change was
                    // attempted.
                    self.global_field_numbers.as_ref().set_doc_values_type(
                        field_info.number,
                        name,
                        doc_values,
                    )?;
                }
                field_info.set_doc_values_type(doc_values)?;
            }
        } else {
            // This field wasn't yet added to this in-RAM
            // segment's FieldInfo, so now we get a global
            // number for this field.  If the field was seen
            // before then we'll get the same name and number,
            // else we'll allocate a new one:
            let field_number = self.global_field_numbers.as_ref().add_or_get(
                name,
                preferred_field_number,
                doc_values,
                dimension_count,
                dimension_num_bytes,
            )?;
            let fi = FieldInfo::new(
                name.to_string(),
                field_number,
                store_term_vector,
                omit_norms,
                store_payloads,
                index_options,
                doc_values,
                -1,
                HashMap::new(),
                dimension_count,
                dimension_num_bytes,
            )?;
            debug_assert!(!self.by_name.contains_key(name));
            self.global_field_numbers.as_ref().verify_consistent(
                fi.number,
                name,
                fi.doc_values_type,
            )?;
            self.by_name.insert(name.to_string(), fi);
        }

        Ok(())
    }

    pub fn finish(&self) -> Result<FieldInfos> {
        let infos: Vec<FieldInfo> = self.by_name.values().cloned().collect();
        FieldInfos::new(infos)
    }
}

pub struct FieldNumbers {
    inner: Mutex<FieldNumbersInner>,
}

impl FieldNumbers {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn add_or_get(
        &self,
        field_name: &str,
        preferred_field_number: u32,
        dv_type: DocValuesType,
        dimension_count: u32,
        dimension_num_bytes: u32,
    ) -> Result<u32> {
        self.inner.lock().unwrap().add_or_get(
            field_name,
            preferred_field_number,
            dv_type,
            dimension_count,
            dimension_num_bytes,
        )
    }

    pub fn clear(&self) {
        self.inner.lock().unwrap().clear();
    }

    pub fn set_doc_values_type(
        &self,
        number: u32,
        name: &str,
        dv_type: DocValuesType,
    ) -> Result<()> {
        self.inner
            .lock()
            .unwrap()
            .set_doc_values_type(number, name, dv_type)
    }

    pub fn set_dimensions(
        &self,
        number: u32,
        name: &str,
        dimension_count: u32,
        num_bytes: u32,
    ) -> Result<()> {
        self.inner
            .lock()
            .unwrap()
            .set_dimensions(number, name, dimension_count, num_bytes)
    }

    fn verify_consistent(&self, number: u32, name: &str, dv_type: DocValuesType) -> Result<()> {
        self.inner.lock()?.verify_consistent(number, name, dv_type)
    }

    pub fn contains(&self, field: &str, dv_type: DocValuesType) -> Result<bool> {
        Ok(self.inner.lock()?.contains(field, dv_type))
    }

    pub fn get_doc_values_type(&self, field: &str) -> Result<Option<DocValuesType>> {
        Ok(self.inner.lock()?.get_doc_values_type(field))
    }
}

impl Default for FieldNumbers {
    fn default() -> Self {
        let inner = Mutex::new(FieldNumbersInner::new());
        FieldNumbers { inner }
    }
}

struct FieldNumbersInner {
    pub number_to_name: HashMap<u32, String>,
    pub name_to_number: HashMap<String, u32>,
    // We use this to enforce that a given field never
    // changes DV type, even across segments / IndexWriter
    // sessions:
    doc_values_type: HashMap<String, DocValuesType>,
    dimensions: HashMap<String, FieldDimensions>,
    // TODO: we should similarly catch an attempt to turn
    // norms back on after they were already ommitted; today
    // we silently discard the norm but this is badly trappy
    lowest_unassigned_field_number: u32,
}

impl FieldNumbersInner {
    pub fn new() -> Self {
        FieldNumbersInner {
            number_to_name: HashMap::new(),
            name_to_number: HashMap::new(),
            doc_values_type: HashMap::new(),
            dimensions: HashMap::new(),
            lowest_unassigned_field_number: 0,
        }
    }

    /// Returns the global field number for the given field name. If the name
    /// does not exist yet it tries to add it with the given preferred field
    /// number assigned if possible otherwise the first unassigned field number
    /// is used as the field number.
    pub fn add_or_get(
        &mut self,
        field_name: &str,
        preferred_field_number: u32,
        dv_type: DocValuesType,
        dimension_count: u32,
        dimension_num_bytes: u32,
    ) -> Result<u32> {
        if dv_type != DocValuesType::Null {
            match self.doc_values_type.entry(field_name.to_string()) {
                Entry::Occupied(entry) => {
                    if *entry.get() != DocValuesType::Null && *entry.get() != dv_type {
                        bail!(IllegalArgument(format!(
                            "cannot change DocValues type from {:?} to {:?} for field '{}'",
                            *entry.get(),
                            dv_type,
                            field_name
                        )));
                    }
                }
                Entry::Vacant(entry) => {
                    entry.insert(dv_type);
                }
            }
        }

        if dimension_count != 0 {
            match self.dimensions.entry(field_name.to_string()) {
                Entry::Occupied(entry) => {
                    if entry.get().dimension_count != dimension_count {
                        bail!(IllegalArgument(format!(
                            "cannot change point dimension count from {} to {} for field '{}'",
                            entry.get().dimension_count,
                            dimension_count,
                            field_name
                        )));
                    }
                    if entry.get().dimension_num_bytes != dimension_num_bytes {
                        bail!(IllegalArgument(format!(
                            "cannot change point num_bytes count from {} to {} for field '{}'",
                            entry.get().dimension_num_bytes,
                            dimension_num_bytes,
                            field_name
                        )));
                    }
                }
                Entry::Vacant(entry) => {
                    entry.insert(FieldDimensions::new(dimension_count, dimension_num_bytes));
                }
            }
        }

        if !self.name_to_number.contains_key(field_name) {
            let field_number = if !self.number_to_name.contains_key(&preferred_field_number) {
                preferred_field_number
            } else {
                loop {
                    if !self
                        .number_to_name
                        .contains_key(&self.lowest_unassigned_field_number)
                    {
                        break;
                    }
                    self.lowest_unassigned_field_number += 1;
                }
                self.lowest_unassigned_field_number
            };
            self.name_to_number
                .insert(field_name.to_string(), field_number);
            self.number_to_name
                .insert(field_number, field_name.to_string());
        }
        Ok(self.name_to_number[field_name])
    }

    fn verify_consistent(&self, number: u32, name: &str, dv_type: DocValuesType) -> Result<()> {
        if self.number_to_name.contains_key(&number) && self.number_to_name[&number] != name {
            bail!(IllegalArgument(format!(
                "field number {} is already mapped to field name '{}' not '{}'",
                number, self.number_to_name[&number], name
            )));
        }

        if self.name_to_number.contains_key(name) && self.name_to_number[name] != number {
            bail!(IllegalArgument(format!(
                "field name {} is already mapped to field number '{}' not '{}'",
                name, self.name_to_number[name], number
            )));
        }

        if dv_type != DocValuesType::Null
            && self.doc_values_type.contains_key(name)
            && self.doc_values_type[name] != DocValuesType::Null
            && self.doc_values_type[name] != dv_type
        {
            bail!(IllegalArgument(format!(
                "cannot change doc_values type from '{:?}' to {:?} for field {}",
                self.doc_values_type[name], dv_type, name
            )));
        }
        Ok(())
    }

    fn verify_consistent_dimensions(
        &self,
        number: u32,
        name: &str,
        dimension_count: u32,
        dimension_num_bytes: u32,
    ) -> Result<()> {
        if self.number_to_name.contains_key(&number) && self.number_to_name[&number] != name {
            bail!(IllegalArgument(format!(
                "field number {} is already mapped to field name '{}' not '{}'",
                number, self.number_to_name[&number], name
            )));
        }

        if self.name_to_number.contains_key(name) && self.name_to_number[name] != number {
            bail!(IllegalArgument(format!(
                "field name {} is already mapped to field number '{}' not '{}'",
                name, self.name_to_number[name], number
            )));
        }

        if self.dimensions.contains_key(name) {
            if self.dimensions[name].dimension_count != dimension_count {
                bail!(IllegalArgument(format!(
                    "cannot change point dimension count from {} to {} for field '{}'",
                    self.dimensions[name].dimension_count, dimension_count, name
                )));
            }

            if self.dimensions[name].dimension_num_bytes != dimension_num_bytes {
                bail!(IllegalArgument(format!(
                    "cannot change point dimension num_bytes from {} to {} for field '{}'",
                    self.dimensions[name].dimension_num_bytes, dimension_num_bytes, name
                )));
            }
        }
        Ok(())
    }

    // TODO used for doc values update
    #[allow(dead_code)]
    /// return true if the field_name exists in the map and is of the type of dv_type
    fn contains(&self, field_name: &str, dv_type: DocValuesType) -> bool {
        // used by IndexWriter.updateNumericDocValue
        if !self.name_to_number.contains_key(field_name) {
            false
        } else {
            self.doc_values_type[field_name] == dv_type
        }
    }

    pub fn clear(&mut self) {
        self.number_to_name.clear();
        self.name_to_number.clear();
        self.doc_values_type.clear();
        self.dimensions.clear();
    }

    pub fn set_doc_values_type(
        &mut self,
        number: u32,
        name: &str,
        dv_type: DocValuesType,
    ) -> Result<()> {
        self.verify_consistent(number, name, dv_type)?;
        self.doc_values_type.insert(name.to_string(), dv_type);
        Ok(())
    }

    pub fn get_doc_values_type(&self, field_name: &str) -> Option<DocValuesType> {
        if self.name_to_number.contains_key(field_name) {
            Some(self.doc_values_type[field_name])
        } else {
            None
        }
    }

    pub fn set_dimensions(
        &mut self,
        number: u32,
        name: &str,
        dimension_count: u32,
        num_bytes: u32,
    ) -> Result<()> {
        if num_bytes > MAX_NUM_BYTES {
            bail!(IllegalArgument(
                "dimension num_bytes must be <= point_values::MAX_NUM_BYTES".into()
            ));
        }
        if dimension_count > MAX_DIMENSIONS {
            bail!(IllegalArgument(
                "dimension dimensions count must be <= point_values::MAX_DIMENSIONS".into()
            ));
        }
        self.verify_consistent_dimensions(number, &name, dimension_count, num_bytes)?;
        self.dimensions.insert(
            name.to_string(),
            FieldDimensions::new(dimension_count, num_bytes),
        );
        Ok(())
    }
}

impl AsRef<FieldNumbers> for FieldNumbers {
    fn as_ref(&self) -> &FieldNumbers {
        self
    }
}

impl AsMut<FieldNumbers> for FieldNumbers {
    fn as_mut(&mut self) -> &mut FieldNumbers {
        self
    }
}

#[derive(Clone)]
pub struct FieldNumbersRef {
    field_numbers: Arc<FieldNumbers>,
}

impl Default for FieldNumbersRef {
    fn default() -> Self {
        FieldNumbersRef {
            field_numbers: Arc::new(FieldNumbers::new()),
        }
    }
}

impl FieldNumbersRef {
    pub fn new(field_numbers: Arc<FieldNumbers>) -> Self {
        FieldNumbersRef { field_numbers }
    }
}

impl AsRef<FieldNumbers> for FieldNumbersRef {
    fn as_ref(&self) -> &FieldNumbers {
        self.field_numbers.as_ref()
    }
}

// NOTE: this struct should always be used under mutex
struct FieldDimensions {
    pub dimension_count: u32,
    pub dimension_num_bytes: u32,
}

impl FieldDimensions {
    fn new(dimension_count: u32, dimension_num_bytes: u32) -> Self {
        FieldDimensions {
            dimension_count,
            dimension_num_bytes,
        }
    }
}
