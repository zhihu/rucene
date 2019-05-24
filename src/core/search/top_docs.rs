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

use core::search::sort_field::SortField;
use core::util::DocId;
use core::util::VariantValue;
use std::cmp::{Ord, Ordering};
use std::f32;

/// Holds one hit in `TopDocs`
#[derive(Clone, Debug)]
pub struct ScoreDoc {
    pub doc: DocId,
    pub score: f32,
}

impl ScoreDoc {
    pub fn new(doc: DocId, score: f32) -> ScoreDoc {
        ScoreDoc { doc, score }
    }

    pub fn reset(&mut self, doc: DocId, score: f32) {
        self.doc = doc;
        self.score = score;
    }

    pub fn order_by_doc(d1: &ScoreDoc, d2: &ScoreDoc) -> Ordering {
        if d1.doc < d2.doc {
            Ordering::Less
        } else if d1.doc > d2.doc {
            Ordering::Greater
        } else {
            Ordering::Equal
        }
    }
}

impl Ord for ScoreDoc {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score.partial_cmp(&other.score).unwrap()
    }
}

impl PartialOrd for ScoreDoc {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.score
            .partial_cmp(&other.score)
            .map(|ord| ord.reverse())
    }
}

impl PartialEq for ScoreDoc {
    fn eq(&self, other: &Self) -> bool {
        self == other
    }
}

impl Eq for ScoreDoc {}

/// Expert: A ScoreDoc which also contains information about
/// how to sort the referenced document.  In addition to the
/// document number and score, this object contains an array
/// of values for the document from the field(s) used to sort.
/// For example, if the sort criteria was to sort by fields
/// "a", "b" then "c", the <code>fields</code> object array
/// will have three elements, corresponding respectively to
/// the term values for the document in fields "a", "b" and "c".
/// The class of each element in the array will be either
/// Integer, Float or String depending on the type of values
/// in the terms of each field.
///
/// @see ScoreDoc
/// @see TopFieldDocs
#[derive(Debug, Clone)]
pub struct FieldDoc {
    pub doc: DocId,
    pub score: f32,
    pub shard_index: usize,
    /// Expert: The values which are used to sort the referenced document.
    /// The order of these will match the original sort criteria given by a
    /// Sort object.  Each Object will have been returned from
    /// the `value` method corresponding
    /// FieldComparator used to sort this field.
    /// @see Sort
    /// @see IndexSearcher#search(Query,int,Sort)
    pub fields: Vec<VariantValue>,
}

impl FieldDoc {
    pub fn new(doc: DocId, score: f32, fields: Vec<VariantValue>) -> FieldDoc {
        FieldDoc {
            doc,
            score,
            shard_index: 0,

            fields,
        }
    }
}

impl Eq for FieldDoc {}

impl PartialEq for FieldDoc {
    fn eq(&self, other: &Self) -> bool {
        self == other
    }
}

impl Ord for FieldDoc {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score.partial_cmp(&other.score).unwrap()
    }
}

impl PartialOrd for FieldDoc {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.score
            .partial_cmp(&other.score)
            .map(|ord| ord.reverse())
    }
}

#[derive(Clone, Debug)]
pub enum ScoreDocHit {
    Score(ScoreDoc),
    Field(FieldDoc),
}

impl ScoreDocHit {
    pub fn score(&self) -> f32 {
        match *self {
            ScoreDocHit::Score(ref s) => s.score,
            ScoreDocHit::Field(ref f) => f.score,
        }
    }

    pub fn set_score(&mut self, score: f32) {
        match *self {
            ScoreDocHit::Score(ref mut s) => s.score = score,
            ScoreDocHit::Field(ref mut f) => f.score = score,
        }
    }

    pub fn doc_id(&self) -> DocId {
        match *self {
            ScoreDocHit::Score(ref s) => s.doc,
            ScoreDocHit::Field(ref f) => f.doc,
        }
    }

    pub fn order_by_doc(d1: &ScoreDocHit, d2: &ScoreDocHit) -> Ordering {
        if d1.doc_id() < d2.doc_id() {
            Ordering::Less
        } else if d1.doc_id() > d2.doc_id() {
            Ordering::Greater
        } else {
            Ordering::Equal
        }
    }
}

impl Eq for ScoreDocHit {}

impl PartialEq for ScoreDocHit {
    fn eq(&self, other: &Self) -> bool {
        self == other
    }
}

impl Ord for ScoreDocHit {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(&other).unwrap()
    }
}

impl PartialOrd for ScoreDocHit {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if let Some(ord) = self
            .score()
            .partial_cmp(&other.score())
            .map(|ord| ord.reverse())
        {
            if ord != Ordering::Equal {
                Some(ord)
            } else {
                self.doc_id().partial_cmp(&other.doc_id())
            }
        } else {
            None
        }
    }
}

/// Represents hits returned by `IndexSearcher::search`
#[derive(Clone)]
pub struct TopScoreDocs {
    /// The total number of hits for the query.
    pub total_hits: usize,

    /// The top hits for the query.
    pub score_docs: Vec<ScoreDocHit>,

    /// Stores the maximum score value encountered, needed for normalizing.
    max_score: f32,
}

impl TopScoreDocs {
    pub fn new(total_hits: usize, score_docs: Vec<ScoreDocHit>) -> TopScoreDocs {
        TopScoreDocs {
            total_hits,
            score_docs,
            max_score: f32::NAN,
        }
    }

    pub fn score_docs(&self) -> &[ScoreDocHit] {
        &self.score_docs
    }
}

#[derive(Clone)]
pub struct TopFieldDocs {
    pub total_hits: usize,
    pub score_docs: Vec<ScoreDocHit>,
    pub max_score: f32,
    pub fields: Vec<SortField>,
}

pub struct CollapseTopFieldDocs {
    /// The total number of hits for the query.
    pub total_hits: usize,

    /// The total group number of hits for the query.
    pub total_groups: usize,

    /// The top hits for the query.
    pub score_docs: Vec<ScoreDocHit>,

    /// Stores the maximum score value encountered, needed for normalizing.
    max_score: f32,

    /// The fields which were used to sort results by.
    pub fields: Vec<SortField>,

    /// The field used for collapsing
    pub field: String,

    /// The collapse value for each top doc
    pub collapse_values: Vec<VariantValue>,
}

impl CollapseTopFieldDocs {
    pub fn new(
        field: String,
        total_hits: usize,
        total_groups: usize,
        score_docs: Vec<ScoreDocHit>,
        sort_fields: Vec<SortField>,
        collapse_values: Vec<VariantValue>,
        max_score: f32,
    ) -> CollapseTopFieldDocs {
        CollapseTopFieldDocs {
            total_hits,
            total_groups,
            score_docs,
            max_score,
            fields: sort_fields,
            field,
            collapse_values,
        }
    }

    pub fn max_score(&self) -> f32 {
        self.max_score
    }
}

pub enum TopDocs {
    Score(TopScoreDocs),
    Field(TopFieldDocs),
    Collapse(CollapseTopFieldDocs),
}

impl TopDocs {
    pub fn total_hits(&self) -> usize {
        match *self {
            TopDocs::Score(ref s) => s.total_hits,
            TopDocs::Field(ref f) => f.total_hits,
            TopDocs::Collapse(ref c) => c.total_hits,
        }
    }

    pub fn total_groups(&self) -> usize {
        match *self {
            TopDocs::Score(ref s) => s.total_hits,
            TopDocs::Field(ref f) => f.total_hits,
            TopDocs::Collapse(ref c) => c.total_groups,
        }
    }

    pub fn score_docs(&self) -> &[ScoreDocHit] {
        match *self {
            TopDocs::Score(ref s) => &s.score_docs,
            TopDocs::Field(ref f) => &f.score_docs,
            TopDocs::Collapse(ref c) => &c.score_docs,
        }
    }

    pub fn score_docs_mut(&mut self) -> &mut Vec<ScoreDocHit> {
        match *self {
            TopDocs::Score(ref mut s) => &mut s.score_docs,
            TopDocs::Field(ref mut f) => &mut f.score_docs,
            TopDocs::Collapse(ref mut c) => &mut c.score_docs,
        }
    }
}
