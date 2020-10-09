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

use core::codec::{Codec, PackedLongDocMap, PostingIteratorFlags};
use core::codec::{Fields, SorterDocMap, TermIterator, Terms};
use core::doc::{DocValuesType, Term};
use core::index::reader::{LeafReader, SegmentReader};
use core::search::DocIterator;
use core::search::NO_MORE_DOCS;
use core::store::directory::Directory;
use error::Result;
use std::cmp::Ordering;
use std::collections::binary_heap::BinaryHeap;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct NumericDocValuesUpdate {
    dv_type: DocValuesType,
    term: Term,
    field: String,
    value: i64,
    docid_up_to: i32,
}

impl NumericDocValuesUpdate {
    pub fn new(
        term: Term,
        field: String,
        dv_type: DocValuesType,
        value: i64,
        docid_up_to: Option<i32>,
    ) -> Self {
        let docid_up_to = if let Some(v) = docid_up_to {
            v
        } else {
            NO_MORE_DOCS
        };
        return NumericDocValuesUpdate {
            dv_type,
            term,
            field,
            value,
            docid_up_to,
        };
    }
}

pub trait DocValuesUpdate {
    fn term(&self) -> Term;
    fn field(&self) -> String;
    fn dv_type(&self) -> DocValuesType;
    fn numeric(&self) -> i64;
    fn binary(&self) -> String;
    fn docid_up_to(&self) -> i32;
    fn set_docid_up_to(&mut self, docid: i32);
}

impl DocValuesUpdate for NumericDocValuesUpdate {
    fn term(&self) -> Term {
        self.term.clone()
    }

    fn field(&self) -> String {
        self.field.clone()
    }

    fn dv_type(&self) -> DocValuesType {
        self.dv_type.clone()
    }

    fn numeric(&self) -> i64 {
        self.value
    }

    fn binary(&self) -> String {
        unimplemented!()
    }

    fn docid_up_to(&self) -> i32 {
        self.docid_up_to
    }

    fn set_docid_up_to(&mut self, docid: i32) {
        self.docid_up_to = docid;
    }
}

pub struct MergedDocValuesUpdatesIterator {
    dv_update: Option<Arc<dyn DocValuesUpdate>>,
    del_gen: u64,
    min_del_gen: u64,
    max_del_gen: u64,
    heap: BinaryHeap<DocValuesUpdatesWrapper>,
    subs: Vec<(u64, Vec<Arc<dyn DocValuesUpdate>>)>,
    // only for merge, tricky!!
    merged_updates: Option<Arc<DVUpdates>>,
    field: String,
    index: usize,
}

impl MergedDocValuesUpdatesIterator {
    pub fn new(subs: Vec<(u64, Vec<Arc<dyn DocValuesUpdate>>)>) -> Self {
        let mut heap = BinaryHeap::with_capacity(subs.len());
        let mut min_del_gen = std::u64::MAX;
        let mut max_del_gen = 0u64;
        for (del_gen, updates) in &subs {
            if updates.len() > 0 {
                let del_gen = *del_gen;
                if del_gen < min_del_gen {
                    min_del_gen = del_gen;
                }
                if del_gen > max_del_gen {
                    max_del_gen = del_gen;
                }
                heap.push(DocValuesUpdatesWrapper::new(updates, del_gen));
            }
        }
        Self {
            dv_update: None,
            del_gen: 0,
            min_del_gen,
            max_del_gen,
            heap,
            subs,
            merged_updates: None,
            field: "".into(),
            index: 0,
        }
    }

    pub fn join(iterators: Vec<Self>) -> Self {
        let mut subs = vec![];
        for v in iterators {
            subs.extend(v.subs);
        }
        Self::new(subs)
    }

    fn dv_update(&self) -> Option<Arc<dyn DocValuesUpdate>> {
        if let Some(dvu) = &self.dv_update {
            Some(dvu.clone())
        } else {
            None
        }
    }

    pub fn min_del_gen(&self) -> u64 {
        debug_assert!(!self.subs.is_empty());
        self.min_del_gen
    }

    pub fn max_del_gen(&self) -> u64 {
        debug_assert!(!self.subs.is_empty());
        self.max_del_gen
    }

    pub fn next(&mut self) -> Option<Arc<dyn DocValuesUpdate>> {
        let mut find = false;
        while !self.heap.is_empty() {
            {
                let mut updates = self.heap.peek_mut().unwrap();
                if let Some(up) = updates.current() {
                    updates.next();
                    if self.dv_update.is_none()
                        || self.dv_update.as_ref().unwrap().term() < up.term()
                    {
                        self.dv_update = Some(up);
                        self.del_gen = updates.del_gen;
                        find = true;
                        break;
                    }
                    continue;
                }
            }
            self.heap.pop();
        }
        if !find {
            self.dv_update = None;
        }
        self.dv_update()
    }

    pub fn from_updates(field: String, updates: Vec<(i32, i64)>) -> Self {
        Self {
            dv_update: None,
            del_gen: 0,
            min_del_gen: 0,
            max_del_gen: 0,
            subs: Vec::new(),
            heap: BinaryHeap::new(),
            merged_updates: Some(Arc::new(DVUpdates::Numeric(updates))),
            field,
            index: 0,
        }
    }

    pub fn is_merged_updates(&self) -> bool {
        self.merged_updates.is_some()
    }

    pub fn next_dv_update(&mut self) -> (i32, i64) {
        if let Some(updates) = &self.merged_updates {
            match updates.as_ref() {
                DVUpdates::Numeric(upds) => {
                    if self.index < upds.len() {
                        let update = upds[self.index];
                        self.index += 1;
                        return update;
                    }
                }
                _ => unimplemented!(),
            }
        }
        (NO_MORE_DOCS, 0)
    }

    pub fn current_dv_update(&self) -> (i32, i64) {
        if let Some(updates) = &self.merged_updates {
            match updates.as_ref() {
                DVUpdates::Numeric(upds) => {
                    if self.index < upds.len() {
                        let update = upds[self.index];
                        return update;
                    }
                }
                _ => unimplemented!(),
            }
        }
        (NO_MORE_DOCS, 0)
    }
}

impl Clone for MergedDocValuesUpdatesIterator {
    fn clone(&self) -> Self {
        if self.is_merged_updates() {
            Self {
                dv_update: None,
                del_gen: 0,
                min_del_gen: 0,
                max_del_gen: 0,
                subs: Vec::new(),
                heap: BinaryHeap::new(),
                merged_updates: Some(self.merged_updates.as_ref().unwrap().clone()),
                index: self.index,
                field: self.field.clone(),
            }
        } else {
            Self::new(self.subs.clone())
        }
    }
}

// updates iterator for binary heap merge
struct DocValuesUpdatesWrapper {
    data: *const Vec<Arc<dyn DocValuesUpdate>>,
    del_gen: u64,
    index: usize,
    end: bool,
}

impl DocValuesUpdatesWrapper {
    fn new(data: &Vec<Arc<dyn DocValuesUpdate>>, del_gen: u64) -> Self {
        Self {
            data: data as *const Vec<Arc<dyn DocValuesUpdate>>,
            del_gen,
            index: 0,
            end: false,
        }
    }

    fn current(&self) -> Option<Arc<dyn DocValuesUpdate>> {
        unsafe {
            if !self.end {
                Some((*self.data)[self.index].clone())
            } else {
                None
            }
        }
    }

    fn next(&mut self) {
        unsafe {
            if self.index + 1 < (*self.data).len() {
                self.index += 1;
            } else {
                self.end = true;
            }
        }
    }
}

impl Ord for DocValuesUpdatesWrapper {
    fn cmp(&self, other: &Self) -> Ordering {
        unsafe {
            let a = &(*self.data)[self.index].term().bytes;
            let b = &(*other.data)[other.index].term().bytes;
            let res = b.cmp(a);
            if res == Ordering::Equal {
                return self.del_gen.cmp(&other.del_gen);
            }
            res
        }
    }
}

impl PartialOrd for DocValuesUpdatesWrapper {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for DocValuesUpdatesWrapper {
    fn eq(&self, other: &Self) -> bool {
        self == other
    }
}

impl Eq for DocValuesUpdatesWrapper {}

enum DVUpdates {
    Numeric(Vec<(i32, i64)>),
    _Binary(Vec<(i32, String)>),
    Iterator(MergedDocValuesUpdatesIterator),
}

// Iterator for merge old & new doc values
pub struct NewDocValuesIterator<D, C>
where
    D: Directory,
    C: Codec,
{
    doc_id: i32,
    _numeric: i64,
    _binary: String,
    index: i32,
    reader: Arc<SegmentReader<D, C>>,
    updates: DVUpdates,
}

impl<'a, D, C> NewDocValuesIterator<D, C>
where
    D: Directory + 'static,
    C: Codec,
{
    pub fn new(
        reader: Arc<SegmentReader<D, C>>,
        iterator: MergedDocValuesUpdatesIterator,
        mut sort_map: Option<Arc<PackedLongDocMap>>,
        prepare_data: bool,
        include_old: bool,
    ) -> Result<Self> {
        let updates: DVUpdates;
        if prepare_data {
            updates = Self::prepare_data(reader.clone(), iterator, sort_map.take(), include_old)?;
        } else {
            updates = DVUpdates::Iterator(iterator);
        }
        Ok(Self {
            doc_id: -1,
            _numeric: 0,
            _binary: "".into(),
            index: -1,
            reader,
            updates,
        })
    }

    pub fn next_numeric(&mut self) -> (i32, i64) {
        match &mut self.updates {
            DVUpdates::Numeric(ndvs) => {
                self.index += 1;
                if self.index < ndvs.len() as i32 {
                    return ndvs[self.index as usize];
                }
                (NO_MORE_DOCS, 0)
            }
            DVUpdates::_Binary(_bdvs) => unimplemented!(),
            DVUpdates::Iterator(iterator) => {
                self.doc_id += 1;
                if self.doc_id >= LeafReader::max_doc(self.reader.as_ref()) {
                    return (NO_MORE_DOCS, 0);
                }
                let (doc_id, value) = iterator.current_dv_update();
                if self.doc_id == doc_id {
                    iterator.next_dv_update();
                    return (doc_id, value);
                }
                let mut dvs = self
                    .reader
                    .get_sorted_numeric_doc_values(&iterator.field)
                    .unwrap();
                dvs.set_document(self.doc_id).unwrap_or(());
                let value = dvs.value_at(0).unwrap_or(0);
                (self.doc_id, value)
            }
        }
    }

    pub fn next_binary(&mut self) -> (i32, String) {
        unimplemented!()
    }

    fn prepare_data(
        reader: Arc<SegmentReader<D, C>>,
        mut iterator: MergedDocValuesUpdatesIterator,
        sort_map: Option<Arc<PackedLongDocMap>>,
        include_old: bool,
    ) -> Result<DVUpdates> {
        let mut up = iterator.next();
        let dv_type = up.as_ref().unwrap().dv_type();
        match dv_type {
            DocValuesType::Numeric | DocValuesType::SortedNumeric => {
                let mut new_ndv = vec![];
                let mut updates = vec![];
                let mut field: Option<String> = None;
                let mut field_terms = None;
                loop {
                    let term = up.as_ref().unwrap().term();
                    if field_terms.is_none() {
                        field_terms = reader.fields()?.terms(&term.field)?;
                    }
                    if field.is_none() {
                        field = Some(up.as_ref().unwrap().field());
                    }

                    if let Some(terms) = &field_terms {
                        let mut it = terms.iterator()?;
                        if let Ok(found) = it.seek_exact(&term.bytes) {
                            if found {
                                let mut doc_ids =
                                    it.postings_with_flags(PostingIteratorFlags::NONE)?;
                                loop {
                                    let doc_id = doc_ids.next()?;
                                    if doc_id == NO_MORE_DOCS {
                                        break;
                                    }
                                    let mut old_id = doc_id;
                                    if sort_map.is_some() {
                                        // had been sorted when flush
                                        old_id = sort_map.as_ref().unwrap().new_to_old(doc_id);
                                    }
                                    if old_id < up.as_ref().unwrap().docid_up_to() {
                                        updates.push((
                                            doc_id,
                                            up.as_ref().unwrap().numeric(),
                                            iterator.del_gen,
                                        ));
                                    }
                                }
                            }
                        }
                    }

                    up = iterator.next();
                    if up.is_none() {
                        // sort doc_id & del_gen
                        updates.sort_by(|a, b| {
                            let res = a.0.cmp(&b.0);
                            if res == Ordering::Equal {
                                return b.2.cmp(&a.2);
                            }
                            res
                        });
                        // unique by doc_id
                        updates.dedup_by(|a, b| a.0.eq(&b.0));
                        if !include_old {
                            return Ok(DVUpdates::Numeric(
                                updates.iter().map(|(x, y, _)| (*x, *y)).collect(),
                            ));
                        }
                        break;
                    }
                }
                if !updates.is_empty() {
                    // merge old & new doc values
                    if let Some(field) = field {
                        let mut old_ndv = reader.get_sorted_numeric_doc_values(&field)?;
                        let mut i = 0;
                        let mut it = updates.iter();
                        loop {
                            if let Some((doc_id, value, _)) = it.next() {
                                let doc_id = *doc_id;
                                let value = *value;
                                if i < doc_id {
                                    // old values
                                    for id in i..doc_id {
                                        // let old_value = old_ndv.get(id).unwrap();
                                        old_ndv.set_document(id)?;
                                        let old_value = old_ndv.value_at(0).unwrap_or(0);
                                        new_ndv.push((id, old_value));
                                        i += 1;
                                    }
                                }
                                // new value
                                new_ndv.push((doc_id, value));
                                i += 1;
                            } else if i > 0 {
                                // old values
                                while i < reader.max_docs() {
                                    old_ndv.set_document(i)?;
                                    let old_value = old_ndv.value_at(0).unwrap_or(0);
                                    // let old_value = old_ndv.get(i).unwrap();
                                    new_ndv.push((i, old_value));
                                    i += 1;
                                }
                                break;
                            } else {
                                break;
                            }
                        }
                    }
                }
                Ok(DVUpdates::Numeric(new_ndv))
            }
            _ => unimplemented!(),
        }
    }

    pub fn get_numeric_updates(&mut self) -> Vec<(i32, i64)> {
        match &self.updates {
            DVUpdates::Numeric(updates) => updates.to_vec(),
            _ => unimplemented!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::doc::Term;
    use std::sync::Arc;

    #[test]
    fn merged_dvu_iterator_from_updates() {
        let updates = vec![(1, 100), (4, 200), (9, 1)];
        let mut mit = MergedDocValuesUpdatesIterator::from_updates("".into(), updates);
        assert_eq!(mit.next_dv_update(), (1, 100));
        assert_ne!(mit.next_dv_update(), (4, 201));
        assert_eq!(mit.next_dv_update(), (9, 1));
        assert_eq!(mit.next_dv_update(), (NO_MORE_DOCS, 0));
    }

    #[test]
    fn merged_dvu_iterator() {
        let (v1, v2, v3) = prepare_data();

        let it;
        {
            let v = vec![(1, v1), (3, v3), (2, v2)];

            it = MergedDocValuesUpdatesIterator::new(v);
        }
        run_data(it);
    }

    #[test]
    fn join_dvu_iterator() {
        let (v1, v2, v3) = prepare_data();

        let ita;
        let itb;
        let it;
        {
            let va = vec![(1, v1), (3, v3)];
            let vb = vec![(2, v2)];

            ita = MergedDocValuesUpdatesIterator::new(va);
            itb = MergedDocValuesUpdatesIterator::new(vb);

            let multi_iters = vec![ita, itb];
            it = MergedDocValuesUpdatesIterator::join(multi_iters);
        }
        run_data(it);
    }

    type V = Vec<Arc<dyn DocValuesUpdate>>;
    fn prepare_data() -> (V, V, V) {
        let id_field: String = "id".into();
        let weight_field: String = "weight".into();

        let mut v1: Vec<Arc<dyn DocValuesUpdate>> = vec![];
        let mut v2: Vec<Arc<dyn DocValuesUpdate>> = vec![];
        let mut v3: Vec<Arc<dyn DocValuesUpdate>> = vec![];

        for i in 1..10 {
            let j = format!("{}", i);
            v1.push(Arc::new(NumericDocValuesUpdate::new(
                Term::new(id_field.clone(), j.as_bytes().to_vec()),
                weight_field.clone(),
                DocValuesType::Numeric,
                111 * i,
                Some(0),
            )));
            if i == 2 || i == 7 {
                v3.push(Arc::new(NumericDocValuesUpdate::new(
                    Term::new(id_field.clone(), j.as_bytes().to_vec()),
                    weight_field.clone(),
                    DocValuesType::Numeric,
                    10 * i,
                    Some(0),
                )));
            }
            if i % 3 == 0 || i == 5 {
                continue;
            }
            v2.push(Arc::new(NumericDocValuesUpdate::new(
                Term::new(id_field.clone(), j.as_bytes().to_vec()),
                weight_field.clone(),
                DocValuesType::Numeric,
                100 * i,
                Some(0),
            )));
        }
        (v1, v2, v3)
    }

    fn run_data(mut it: MergedDocValuesUpdatesIterator) {
        let mut i = 1;
        while let Some(update) = it.next() {
            println!(
                "field: {}, id: {}, weight: {}",
                update.field(),
                std::str::from_utf8(&update.term().bytes).unwrap(),
                update.numeric()
            );
            assert_eq!(update.field().as_str(), "weight");
            assert_eq!(
                format!("{}", i).as_str(),
                std::str::from_utf8(&update.term().bytes).unwrap()
            );
            if i == 2 || i == 7 {
                assert_eq!(update.numeric(), i * 10);
            } else if i % 3 == 0 || i == 5 {
                assert_eq!(update.numeric(), i * 111);
            } else {
                assert_eq!(update.numeric(), i * 100);
            }
            i += 1;
        }
    }
}
