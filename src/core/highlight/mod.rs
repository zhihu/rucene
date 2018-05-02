use core::index::POSTINGS_POSITIONS;
use core::index::{Fields, IndexReader, LeafReader, Term};
use core::search::term_query::TermQuery;
use core::search::Query;
use core::util::DocId;
use error::*;

use std::borrow::Cow;
use std::cmp::{max, Ordering};
use std::collections::HashMap;
use std::f32::EPSILON;

pub mod frag_list_builder;
pub mod fragments_builder;
pub mod fvh_highlighter;

///
// Encodes original text. The Encoder works with the {@link Formatter} to generate output.
//
//
pub trait Encoder {
    ///
    // @param originalText The section of text being output
    //
    fn encode_text<'a>(&self, original: &'a str) -> Cow<'a, str>;
}

pub struct DefaultEncoder {}

impl Default for DefaultEncoder {
    fn default() -> DefaultEncoder {
        DefaultEncoder {}
    }
}

impl Encoder for DefaultEncoder {
    fn encode_text<'a>(&self, original: &'a str) -> Cow<'a, str> {
        Cow::Borrowed(original)
    }
}

pub struct SimpleHtmlEncoder {}

impl Default for SimpleHtmlEncoder {
    fn default() -> SimpleHtmlEncoder {
        SimpleHtmlEncoder {}
    }
}

impl Encoder for SimpleHtmlEncoder {
    fn encode_text<'a>(&self, original: &'a str) -> Cow<'a, str> {
        if original.is_empty() {
            Cow::Borrowed(original)
        } else {
            let mut result = String::from("");
            for c in original.chars() {
                match c {
                    '"' => result.push_str("&quot;"),
                    '&' => result.push_str("&amp;"),
                    '<' => result.push_str("&lt;"),
                    '>' => result.push_str("&gt;"),
                    '\'' => result.push_str("&#x27;"),
                    '/' => result.push_str("&#x2F;"),
                    _ => result.push(c),
                }
            }

            Cow::Owned(result)
        }
    }
}

///
// Term offsets (start + end)
//
#[derive(Clone, PartialEq)]
pub struct Toffs {
    pub start_offset: i32,
    pub end_offset: i32,
}

impl Toffs {
    pub fn new(start_offset: i32, end_offset: i32) -> Toffs {
        Toffs {
            start_offset,
            end_offset,
        }
    }
}

///
// Represents the list of term offsets for some text
//
#[derive(Clone, PartialEq)]
pub struct SubInfo {
    pub text: String,
    pub terms_offsets: Vec<Toffs>,
    pub seqnum: i32,
    pub boost: f32,
}

impl SubInfo {
    pub fn new(text: String, terms_offsets: Vec<Toffs>, seqnum: i32, boost: f32) -> SubInfo {
        SubInfo {
            text,
            terms_offsets,
            seqnum,
            boost,
        }
    }
}

///
// List of term offsets + weight for a frag info
//
#[derive(Clone)]
pub struct WeightedFragInfo {
    pub sub_infos: Vec<SubInfo>,
    pub total_boost: f32,
    pub start_offset: i32,
    pub end_offset: i32,
}

impl WeightedFragInfo {
    pub fn new(
        sub_infos: Vec<SubInfo>,
        total_boost: f32,
        start_offset: i32,
        end_offset: i32,
    ) -> WeightedFragInfo {
        WeightedFragInfo {
            sub_infos,
            total_boost,
            start_offset,
            end_offset,
        }
    }

    pub fn order_by_boost_and_offset(o1: &WeightedFragInfo, o2: &WeightedFragInfo) -> Ordering {
        if o1.total_boost > o2.total_boost {
            Ordering::Less
        } else if o1.total_boost < o2.total_boost {
            Ordering::Greater
        } else if o1.start_offset < o2.start_offset {
            Ordering::Less
        } else if o1.start_offset == o2.start_offset {
            Ordering::Greater
        } else {
            Ordering::Equal
        }
    }
}

// FieldFragList has a list of "frag info" that is used by FragmentsBuilder class
// to create fragments (snippets).
//
pub trait FieldFragList {
    fn add(&mut self, start_offset: i32, end_offset: i32, phrase_info_list: &[WeightedPhraseInfo]);

    fn frag_infos(&mut self) -> &mut Vec<WeightedFragInfo>;
}

pub struct SimpleFieldFragList {
    frag_infos: Vec<WeightedFragInfo>,
}

impl Default for SimpleFieldFragList {
    fn default() -> SimpleFieldFragList {
        SimpleFieldFragList { frag_infos: vec![] }
    }
}

impl SimpleFieldFragList {
    pub fn new() -> SimpleFieldFragList {
        SimpleFieldFragList { frag_infos: vec![] }
    }
}

impl FieldFragList for SimpleFieldFragList {
    fn add(&mut self, start_offset: i32, end_offset: i32, phrase_info_list: &[WeightedPhraseInfo]) {
        let mut total_boost = 0f32;
        let mut sub_infos: Vec<SubInfo> = Vec::with_capacity(phrase_info_list.len());

        for phrase_info in phrase_info_list {
            let info = SubInfo::new(
                phrase_info.text(),
                phrase_info.terms_offsets.clone(),
                phrase_info.seqnum,
                phrase_info.boost,
            );
            sub_infos.push(info);

            total_boost += phrase_info.boost;
        }

        self.frag_infos.push(WeightedFragInfo::new(
            sub_infos,
            total_boost,
            start_offset,
            end_offset,
        ));
    }

    fn frag_infos(&mut self) -> &mut Vec<WeightedFragInfo> {
        &mut self.frag_infos
    }
}

pub struct WeightedFieldFragList {
    frag_infos: Vec<WeightedFragInfo>,
}

impl FieldFragList for WeightedFieldFragList {
    fn add(
        &mut self,
        _start_offset: i32,
        _end_offset: i32,
        _phrase_info_list: &[WeightedPhraseInfo],
    ) {
        unimplemented!()
    }

    fn frag_infos(&mut self) -> &mut Vec<WeightedFragInfo> {
        &mut self.frag_infos
    }
}

///
// Single term with its position/offsets in the document and IDF weight.
// It is Comparable but considers only position.
//
#[derive(Clone)]
pub struct TermInfo {
    pub text: String,
    pub start_offset: i32,
    pub end_offset: i32,
    pub position: i32,
    pub weight: f32,
    pub next: Vec<TermInfo>,
}

impl TermInfo {
    pub fn new(
        text: String,
        start_offset: i32,
        end_offset: i32,
        position: i32,
        weight: f32,
    ) -> TermInfo {
        TermInfo {
            text,
            start_offset,
            end_offset,
            position,
            weight,
            next: vec![],
        }
    }
}

#[derive(Clone)]
pub struct WeightedPhraseInfo {
    pub terms_offsets: Vec<Toffs>,
    pub boost: f32,
    pub seqnum: i32,
    pub terms_infos: Vec<TermInfo>,
}

impl WeightedPhraseInfo {
    pub fn new(terms_infos: Vec<TermInfo>, boost: f32, seqnum: Option<i32>) -> WeightedPhraseInfo {
        let seqnum = match seqnum {
            Some(x) => x,
            None => 0,
        };

        assert!(!terms_infos.is_empty());
        let mut terms_offsets: Vec<Toffs> = Vec::with_capacity(terms_infos.len());
        {
            let ti = &terms_infos[0];
            terms_offsets.push(Toffs::new(ti.start_offset, ti.end_offset));
            if terms_infos.len() > 1 {
                let mut pos = ti.position;
                for ti in (&terms_infos).iter().skip(1) {
                    if ti.position - pos == 1 {
                        let pos = terms_offsets.len() - 1;
                        terms_offsets[pos].end_offset = ti.end_offset;
                    } else {
                        terms_offsets.push(Toffs::new(ti.start_offset, ti.end_offset));
                    }

                    pos = ti.position;
                }
            }
        }

        WeightedPhraseInfo {
            terms_offsets,
            boost,
            seqnum,
            terms_infos,
        }
    }

    pub fn merge_new(to_merge: Vec<WeightedPhraseInfo>) -> WeightedPhraseInfo {
        // Pretty much the same idea as merging FieldPhraseLists:
        // Step 1.  Sort by startOffset, endOffset
        //          While we are here merge the boosts and termInfos
        assert!(
            !to_merge.is_empty(),
            "toMerge must contain at least one WeightedPhraseInfo."
        );

        let mut sequence: Vec<usize> = vec![];
        {
            let mut current_index: Vec<usize> = vec![0; to_merge.len()];
            let mut all_toffs: Vec<&Vec<Toffs>> = Vec::with_capacity(to_merge.len());

            for item in &to_merge {
                all_toffs.push(&item.terms_offsets);
            }

            loop {
                let mut min_offset = to_merge.len();
                for i in 0..to_merge.len() {
                    if current_index[i] >= all_toffs[i].len() {
                        continue;
                    }

                    if min_offset == to_merge.len() {
                        min_offset = i;
                    }

                    if all_toffs[i][current_index[i]].start_offset
                        < all_toffs[min_offset][current_index[min_offset]].start_offset
                        || (all_toffs[i][current_index[i]].start_offset
                            == all_toffs[min_offset][current_index[min_offset]].start_offset
                            && all_toffs[i][current_index[i]].end_offset
                                < all_toffs[min_offset][current_index[min_offset]].end_offset)
                    {
                        min_offset = i;
                    }
                }

                if min_offset != to_merge.len() {
                    sequence.push(min_offset);
                    current_index[min_offset] += 1;
                } else {
                    break;
                }
            }
        }

        let seqnum = to_merge[0].seqnum;
        let mut boost = 0f32;
        let mut terms_infos: Vec<TermInfo> = vec![];
        let mut toffs_iter_list = Vec::with_capacity(to_merge.len());
        let mut to_merge_iter = to_merge.into_iter();

        while !to_merge_iter.is_empty() {
            let info = to_merge_iter.next().unwrap();

            boost += info.boost;

            let mut term_info_iter = info.terms_infos.into_iter();
            while !term_info_iter.is_empty() {
                terms_infos.push(term_info_iter.next().unwrap());
            }

            toffs_iter_list.push(info.terms_offsets.into_iter());
        }

        let mut toffs: Vec<Toffs> = vec![];
        for index in sequence {
            if let Some(t) = toffs_iter_list[index].next() {
                toffs.push(t)
            }
        }

        // Step 2.  Walk the sorted list merging overlaps
        let mut terms_offsets: Vec<Toffs> = vec![];
        let mut iter_list = toffs.into_iter();

        if !iter_list.is_empty() {
            let mut work = iter_list.next().unwrap();

            while !iter_list.is_empty() {
                let curr = iter_list.next().unwrap();
                if curr.start_offset <= work.end_offset {
                    work.end_offset = max(work.end_offset, curr.end_offset);
                } else {
                    let merge_work = work;
                    terms_offsets.push(merge_work);
                    work = curr;
                }
            }
            terms_offsets.push(work);
        }

        WeightedPhraseInfo {
            terms_offsets,
            boost,
            seqnum,
            terms_infos,
        }
    }

    pub fn text(&self) -> String {
        let mut text = String::from("");

        for ti in &self.terms_infos {
            text.push_str(ti.text.as_str());
        }

        text
    }

    pub fn start_offset(&self) -> i32 {
        self.terms_offsets[0].start_offset
    }

    pub fn end_offset(&self) -> i32 {
        self.terms_offsets[self.terms_offsets.len() - 1].end_offset
    }

    pub fn is_offset_overlap(&self, other: &WeightedPhraseInfo) -> bool {
        let so = self.start_offset();
        let eo = self.end_offset();
        let oso = other.start_offset();
        let oeo = other.end_offset();

        (so <= oso && oso < eo) || (so < oeo && oeo <= eo) || (oso <= so && so < oeo)
            || (oso < eo && eo <= oeo)
    }
}

///
// Internal structure of a query for highlighting: represents
// a nested query structure
//
#[derive(Debug)]
pub struct QueryPhraseMap {
    pub terminal: bool,
    // valid if terminal == true and phraseHighlight == true
    pub slop: i32,
    // valid if terminal == true
    pub boost: f32,
    // valid if terminal == true
    pub term_or_phrase_number: i32,

    pub sub_map: HashMap<String, QueryPhraseMap>,
}

impl Default for QueryPhraseMap {
    fn default() -> QueryPhraseMap {
        QueryPhraseMap {
            terminal: false,
            slop: 0,
            boost: 0.0,
            term_or_phrase_number: 0,
            sub_map: HashMap::new(),
        }
    }
}

impl QueryPhraseMap {
    pub fn new() -> QueryPhraseMap {
        QueryPhraseMap {
            terminal: false,
            slop: 0,
            boost: 0.0,
            term_or_phrase_number: 0,
            sub_map: HashMap::new(),
        }
    }

    fn mark_terminal(&mut self, slop: Option<i32>, boost: f32, term_or_phrase_number: i32) {
        self.terminal = true;
        self.slop = match slop {
            Some(s) => s,
            None => 0,
        };
        self.boost = boost;
        self.term_or_phrase_number = term_or_phrase_number;
    }

    pub fn add_term(&mut self, term: &Term, boost: f32, term_or_phrase_number: i32) -> Result<()> {
        let text = term.text()?;
        if !self.sub_map.contains_key(&text) {
            self.sub_map.insert(text.clone(), QueryPhraseMap::default());
        }

        assert!(self.sub_map.contains_key(&text));
        self.sub_map
            .get_mut(&text)
            .unwrap()
            .mark_terminal(None, boost, term_or_phrase_number);

        Ok(())
    }

    pub fn add(
        &mut self,
        query: &TermQuery,
        _reader: Option<&IndexReader>,
        term_or_phrase_number: i32,
    ) -> Result<()> {
        let boost = 1f32;
        self.add_term(&query.term.clone(), boost, term_or_phrase_number)?;
        Ok(())
    }

    pub fn is_valid_term_or_phrase(&self, phrase_candidate: &[TermInfo]) -> bool {
        if !self.terminal {
            return false;
        }

        if phrase_candidate.len() == 1 {
            return true;
        }

        let mut pos = phrase_candidate[0].position;
        for term_info in phrase_candidate.iter().skip(1) {
            let next_pos = term_info.position;
            if (f64::from(next_pos - pos - 1)).abs() > f64::from(self.slop) {
                return false;
            }

            pos = next_pos;
        }
        true
    }

    pub fn search_phrase(&self, phrase_candidate: &[TermInfo]) -> Option<&QueryPhraseMap> {
        let mut curr_map = self;
        for ti in phrase_candidate {
            match curr_map.sub_map.get(&ti.text) {
                Some(map) => curr_map = map,
                None => return None,
            };
        }

        if curr_map.is_valid_term_or_phrase(phrase_candidate) {
            Some(curr_map)
        } else {
            None
        }
    }
}

///
// FieldQuery breaks down query object into terms/phrases and keeps
// them in a QueryPhraseMap structure.
//
// The maximum number of different matching terms accumulated from any one MultiTermQuery
pub const MAX_MTQ_TERMS: i32 = 1024;

#[derive(Debug)]
pub struct FieldQuery {
    field_match: bool,
    // fieldMatch==true,  Map<fieldName,QueryPhraseMap>
    // fieldMatch==false, Map<null,QueryPhraseMap>
    root_maps: HashMap<String, QueryPhraseMap>,
    // fieldMatch==true,  Map<fieldName,setOfTermsInQueries>
    // fieldMatch==false, Map<null,setOfTermsInQueries>
    term_set_map: HashMap<String, Vec<String>>,
    // used for colored tag support
    term_or_phrase_number: i32,
}

impl FieldQuery {
    pub fn new(
        query: &Query,
        reader: Option<&IndexReader>,
        _phrase_highlight: bool,
        field_match: bool,
    ) -> Result<FieldQuery> {
        let mut flat_queries: Vec<TermQuery> = vec![];
        let mut field_query = FieldQuery {
            field_match,
            root_maps: HashMap::new(),
            term_set_map: HashMap::new(),
            term_or_phrase_number: 0,
        };

        field_query.flatten(query, reader, &mut flat_queries, 1f32)?;
        field_query.save_terms(reader, &flat_queries)?;

        let expand_queries = field_query.expand(flat_queries);

        for flat_query in &expand_queries {
            let term_or_phrase_number = field_query.next_term_or_phrase_number();
            field_query.add_root_map_by_query(flat_query, reader, term_or_phrase_number)?;
        }

        Ok(field_query)
    }

    fn flatten(
        &self,
        source_query: &Query,
        _reader: Option<&IndexReader>,
        flat_queries: &mut Vec<TermQuery>,
        boost: f32,
    ) -> Result<()> {
        if (boost - 1f32).abs() > EPSILON {
            unimplemented!()
        }

        for term_query in source_query.extract_terms() {
            if !flat_queries.contains(&term_query) {
                flat_queries.push(term_query);
            }
        }

        Ok(())
    }

    // Save the set of terms in the queries to termSetMap.
    // ex1) q=name:john
    //      - fieldMatch==true
    //          termSetMap=Map<"name",Set<"john">>
    //      - fieldMatch==false
    //          termSetMap=Map<null,Set<"john">>
    //
    // ex2) q=name:john title:manager
    //      - fieldMatch==true
    //          termSetMap=Map<"name",Set<"john">,
    //                         "title",Set<"manager">>
    //      - fieldMatch==false
    //          termSetMap=Map<null,Set<"john","manager">>
    //
    // ex3) q=name:"john lennon"
    //      - fieldMatch==true
    //          termSetMap=Map<"name",Set<"john","lennon">>
    //      - fieldMatch==false
    //          termSetMap=Map<null,Set<"john","lennon">>
    //
    fn save_terms(
        &mut self,
        _reader: Option<&IndexReader>,
        flat_queries: &[TermQuery],
    ) -> Result<()> {
        for query in flat_queries {
            self.add_term_set_by_query(query, query.term.text()?);
        }

        Ok(())
    }

    // Return 'key' string. 'key' is the field name of the Query.
    // If not fieldMatch, 'key' will be null.
    //
    fn get_key(&self, query: &TermQuery) -> String {
        if !self.field_match {
            return String::from("");
        }

        String::from(query.term.field())
    }

    fn add_term_set_by_query(&mut self, query: &TermQuery, value: String) {
        let key = self.get_key(query);

        if !self.term_set_map.contains_key(&key) {
            let set: Vec<String> = vec![];
            self.term_set_map.insert(key.clone(), set);
        }

        assert!(self.term_set_map.contains_key(&key));
        self.term_set_map.get_mut(&key).unwrap().push(value)
    }

    pub fn get_term_set_by_field(&self, field: &str) -> Option<&Vec<String>> {
        self.term_set_map.get(field)
    }

    fn expand(&self, flat_queries: Vec<TermQuery>) -> Vec<TermQuery> {
        flat_queries
    }

    pub fn next_term_or_phrase_number(&mut self) -> i32 {
        self.term_or_phrase_number += 1;
        self.term_or_phrase_number
    }

    fn add_root_map_by_query(
        &mut self,
        query: &TermQuery,
        reader: Option<&IndexReader>,
        term_or_phrase_number: i32,
    ) -> Result<()> {
        let key = self.get_key(query);

        if !self.root_maps.contains_key(&key) {
            let map = QueryPhraseMap::default();
            self.root_maps.insert(key.clone(), map);
        }

        assert!(self.root_maps.contains_key(&key));
        self.root_maps
            .get_mut(&key)
            .unwrap()
            .add(query, reader, term_or_phrase_number)
    }

    pub fn get_root_map_by_field(&self, field_name: &str) -> Option<&QueryPhraseMap> {
        let key = if self.field_match { field_name } else { "" };

        self.root_maps.get(key)
    }

    pub fn search_phrase(
        &self,
        field_name: &str,
        phrase_candidate: &[TermInfo],
    ) -> Option<&QueryPhraseMap> {
        match self.get_root_map_by_field(field_name) {
            Some(root) => root.search_phrase(phrase_candidate),
            None => None,
        }
    }

    pub fn get_field_term_map(&self, field_name: &str, term: &str) -> Option<&QueryPhraseMap> {
        match self.get_root_map_by_field(field_name) {
            Some(root) => root.sub_map.get(term),
            None => None,
        }
    }
}

///
// <code>FieldTermStack</code> is a stack that keeps query terms in the specified field
// of the document to be highlighted.
//
pub struct FieldTermStack {
    pub field_name: String,
    pub term_list: Vec<TermInfo>,
}

impl FieldTermStack {
    pub fn new(
        reader: &LeafReader,
        doc_id: DocId,
        field_name: &str,
        field_query: &FieldQuery,
    ) -> Result<FieldTermStack> {
        // just return to make null snippet if un-matched fieldName specified when fieldMatch ==
        let term_set = match field_query.get_term_set_by_field(field_name) {
            Some(t) => t,
            None => {
                return Ok(FieldTermStack {
                    field_name: field_name.to_string(),
                    term_list: vec![],
                });
            }
        };

        let vectors: Box<Fields> = reader.term_vector(doc_id)?;
        if let Some(vector) = vectors.terms(field_name)? {
            // true null snippet
            if vectors.fields().is_empty() || !vector.has_positions()? {
                return Ok(FieldTermStack {
                    field_name: field_name.to_string(),
                    term_list: vec![],
                });
            }

            let mut terms_iter = vector.iterator()?;
            let max_docs = reader.max_doc();
            let mut term_list: Vec<TermInfo> = vec![];

            let mut text = terms_iter.as_mut().next()?;
            while !text.is_empty() {
                let term = String::from_utf8(text)?;
                if !term_set.contains(&term) {
                    text = terms_iter.as_mut().next()?;
                    continue;
                }

                let mut postings = terms_iter.postings_with_flags(POSTINGS_POSITIONS)?;
                postings.next()?;

                // For weight look here: http://lucene.apache.org/core/3_6_0/api/core/org/apache/lucene/search/DefaultSimilarity.html
                let weight = (f64::from(max_docs)
                    / (terms_iter.as_mut().total_term_freq()? + 1) as f64
                    + 1.0)
                    .log(10.0f64) as f32;
                let freq = postings.freq()?;

                for _ in 0..freq {
                    let pos = postings.next_position()?;

                    if postings.start_offset()? < 0 {
                        // no offsets, null snippet
                        return Ok(FieldTermStack {
                            field_name: field_name.to_string(),
                            term_list: vec![],
                        });
                    }

                    term_list.push(TermInfo::new(
                        term.clone(),
                        postings.start_offset()?,
                        postings.end_offset()?,
                        pos,
                        weight,
                    ));
                }

                text = terms_iter.as_mut().next()?;
            }

            // now look for dups at the same position, linking them together
            term_list.sort_by(|o1, o2| o1.position.cmp(&o2.position));

            let mut i = 0usize;
            while i < term_list.len() {
                let mut j = i + 1;
                while j < term_list.len() {
                    if term_list[j].position == term_list[i].position {
                        let term_info = term_list[j].clone();
                        term_list[i].next.push(term_info);
                        term_list[j].position = -1;
                    } else {
                        break;
                    }

                    j += 1;
                }

                i += term_list[i].next.len() + 1;
            }

            i = term_list.len();
            while i > 0 {
                if term_list[i - 1].position == -1 {
                    term_list.remove(i - 1);
                }

                i -= 1;
            }

            Ok(FieldTermStack {
                field_name: field_name.to_string(),
                term_list,
            })
        } else {
            Ok(FieldTermStack {
                field_name: field_name.to_string(),
                term_list: vec![],
            })
        }
    }

    pub fn pop(&mut self) -> Option<TermInfo> {
        if !self.term_list.is_empty() {
            let term_info = self.term_list[0].clone();
            self.term_list.remove(0);

            Some(term_info)
        } else {
            None
        }
    }

    pub fn push(&mut self, term_info: TermInfo) {
        self.term_list.insert(0, term_info);
    }

    pub fn is_empty(&self) -> bool {
        self.term_list.len() == 0
    }
}

pub struct FieldPhraseList {
    phrase_list: Vec<WeightedPhraseInfo>,
}

impl FieldPhraseList {
    pub fn new(
        field_term_stack: &mut FieldTermStack,
        field_query: &FieldQuery,
        phrase_limit: i32,
    ) -> FieldPhraseList {
        let field = field_term_stack.field_name.clone();
        let mut field_phrase_list = FieldPhraseList {
            phrase_list: vec![],
        };

        let mut curr_map: Option<&QueryPhraseMap> = None;
        let mut next_map: Option<&QueryPhraseMap>;

        while !field_term_stack.is_empty()
            && field_phrase_list.phrase_list.len() < phrase_limit as usize
        {
            let mut phrase_candidate: Vec<TermInfo> = vec![];

            let first = field_term_stack.pop().unwrap();
            match field_query.get_field_term_map(&field, &first.text) {
                Some(map) => {
                    curr_map = Some(map);
                    phrase_candidate.push(first.clone());
                }
                None => for i in 0..first.next.len() {
                    curr_map = field_query.get_field_term_map(&field, &first.next[i].text);
                    if curr_map.is_some() {
                        phrase_candidate.push(first.next[i].clone());
                        break;
                    }
                },
            }

            // if not found, discard top TermInfo from stack, then try next element
            if curr_map.is_none() {
                continue;
            }

            loop {
                next_map = None;
                let curr_map_unwrap = curr_map.unwrap();
                let first = field_term_stack.pop();

                if first.is_some() {
                    let first_unwrap = first.unwrap();

                    next_map = curr_map_unwrap.sub_map.get(&first_unwrap.text);
                    let mut i = 0;
                    while next_map.is_none() && i < first_unwrap.next.len() {
                        next_map = curr_map_unwrap.sub_map.get(&first_unwrap.next[i].text);
                        i += 1;
                    }

                    match next_map {
                        Some(_) => {
                            if i > 0 {
                                phrase_candidate.push(first_unwrap.next[i - 1].clone());
                            } else {
                                phrase_candidate.push(first_unwrap);
                                curr_map = next_map;
                            }
                        }
                        None => field_term_stack.push(first_unwrap),
                    }
                }

                if next_map.is_none() {
                    if curr_map_unwrap.is_valid_term_or_phrase(&phrase_candidate) {
                        field_phrase_list.add_if_no_overlap(WeightedPhraseInfo::new(
                            phrase_candidate,
                            curr_map_unwrap.boost,
                            Some(curr_map_unwrap.term_or_phrase_number),
                        ));
                        break;
                    } else {
                        let mut i = phrase_candidate.len() - 1;
                        while i > 0 {
                            field_term_stack.push(phrase_candidate[i].clone());
                            curr_map = field_query.search_phrase(&field, &phrase_candidate);
                            if curr_map.is_some() {
                                let curr_map_unwrap = curr_map.unwrap();
                                field_phrase_list.add_if_no_overlap(WeightedPhraseInfo::new(
                                    phrase_candidate,
                                    curr_map_unwrap.boost,
                                    Some(curr_map_unwrap.term_or_phrase_number),
                                ));
                                break;
                            }

                            i -= 1;
                        }
                    }

                    break;
                }
            }
        }

        field_phrase_list
    }

    pub fn merge_new(to_merge: Vec<FieldPhraseList>) -> FieldPhraseList {
        // Merge all overlapping WeightedPhraseInfos
        // Step 1.  Sort by startOffset, endOffset, and boost, in that order.
        assert!(
            !to_merge.is_empty(),
            "toMerge must contain at least one FieldPhraseList."
        );

        let mut sequence: Vec<usize> = vec![];
        {
            let mut current_index: Vec<usize> = vec![0; to_merge.len()];
            let mut all_infos: Vec<&Vec<WeightedPhraseInfo>> = Vec::with_capacity(to_merge.len());

            for field_phrase_list in &to_merge {
                all_infos.push(&field_phrase_list.phrase_list);
            }

            loop {
                let mut min_offset = to_merge.len();
                for i in 0..to_merge.len() {
                    if current_index[i] >= all_infos[i].len() {
                        continue;
                    }

                    if min_offset == to_merge.len() {
                        min_offset = i;
                    }

                    if all_infos[i][current_index[i]].start_offset()
                        < all_infos[min_offset][current_index[min_offset]].start_offset()
                        || (all_infos[i][current_index[i]].start_offset()
                            == all_infos[min_offset][current_index[min_offset]].start_offset()
                            && all_infos[i][current_index[i]].end_offset()
                                < all_infos[min_offset][current_index[min_offset]].end_offset())
                        || (all_infos[i][current_index[i]].start_offset()
                            == all_infos[min_offset][current_index[min_offset]].start_offset()
                            && all_infos[i][current_index[i]].end_offset()
                                == all_infos[min_offset][current_index[min_offset]].end_offset()
                            && all_infos[i][current_index[i]].boost
                                < all_infos[min_offset][current_index[min_offset]].boost)
                    {
                        min_offset = i;
                    }
                }

                if min_offset != to_merge.len() {
                    sequence.push(min_offset);
                    current_index[min_offset] += 1;
                } else {
                    break;
                }
            }
        }

        let mut weighted_phrase_infos: Vec<WeightedPhraseInfo> = vec![];
        let mut iter_list = Vec::with_capacity(to_merge.len());
        let mut to_merge_iter = to_merge.into_iter();

        while !to_merge_iter.is_empty() {
            iter_list.push(to_merge_iter.next().unwrap().phrase_list.into_iter());
        }

        for index in sequence {
            if let Some(w) = iter_list[index].next() {
                weighted_phrase_infos.push(w)
            }
        }

        // Step 2.  Walk the sorted list merging overlaps

        let mut phrase_list: Vec<WeightedPhraseInfo> = vec![];
        let mut iter = weighted_phrase_infos.into_iter();

        if !iter.is_empty() {
            let mut work: Vec<WeightedPhraseInfo> = vec![];
            let first = iter.next().unwrap();
            let mut work_end_offset = first.end_offset();

            work.push(first);
            while !iter.is_empty() {
                let curr = iter.next().unwrap();

                if curr.start_offset() <= work_end_offset {
                    work_end_offset = max(work_end_offset, curr.end_offset());
                    work.push(curr);
                } else {
                    work_end_offset = curr.end_offset();

                    if work.len() == 1 {
                        phrase_list.push(work.pop().unwrap());
                        work.push(curr);
                    } else {
                        let merge_work = work;
                        work = vec![];
                        work.push(curr);

                        phrase_list.push(WeightedPhraseInfo::merge_new(merge_work));
                    }
                }
            }

            if work.len() == 1 {
                phrase_list.push(work.pop().unwrap());
            } else {
                phrase_list.push(WeightedPhraseInfo::merge_new(work));
            }
        }

        FieldPhraseList { phrase_list }
    }

    fn add_if_no_overlap(&mut self, wpi: WeightedPhraseInfo) {
        for exist_wpi in &mut self.phrase_list {
            if exist_wpi.is_offset_overlap(&wpi) {
                // WeightedPhraseInfo.addIfNoOverlap() dumps the second part of, for example,
                // hyphenated words (social-economics). The result is that all
                // informations in TermInfo are lost and not available for further operations.
                exist_wpi.terms_infos.clone_from(&wpi.terms_infos);
                if exist_wpi.end_offset() < wpi.end_offset() {
                    let i = exist_wpi.terms_offsets.len() - 1;
                    exist_wpi.terms_offsets[i].end_offset = wpi.end_offset();
                }

                return;
            }
        }

        self.phrase_list.push(wpi);
    }
}

///
// Finds fragment boundaries: pluggable into {@link BaseFragmentsBuilder}
//
pub trait BoundaryScanner {
    ///
    // Scan backward to find end offset.
    // @param buffer scanned object
    // @param start start offset to begin
    // @return the found start offset
    //
    fn find_start_offset(&self, buffer: &str, start: i32) -> i32;

    ///
    // Scan forward to find start offset.
    // @param buffer scanned object
    // @param start start offset to begin
    // @return the found end offset
    //
    fn find_end_offset(&self, buffer: &str, start: i32) -> i32;
}

///
// Simple boundary scanner implementation that divides fragments
// based on a set of separator characters.
//

// DEFAULT_MAX_SCAN: i32 = 20;
pub const DEFAULT_MAX_SCAN: i32 = 50;
// vec!['.', ',', '!', '?', ' ', '\t', '\n'];
pub const DEFAULT_BOUNDARY_CHARS: &str = " \t\n,，|!！?？;；.。:：";

pub struct SimpleBoundaryScanner {
    max_scan: i32,
    boundary_chars: Vec<char>,
}

impl SimpleBoundaryScanner {
    pub fn new(max_scan: Option<i32>, boundary_chars: Option<&Vec<char>>) -> SimpleBoundaryScanner {
        SimpleBoundaryScanner {
            max_scan: max_scan.unwrap_or(DEFAULT_MAX_SCAN),
            boundary_chars: match boundary_chars {
                Some(x) => x.clone(),
                None => SimpleBoundaryScanner::boundary_chars_to_vec(DEFAULT_BOUNDARY_CHARS),
            },
        }
    }

    pub fn boundary_chars_to_vec(boundary_chars: &str) -> Vec<char> {
        boundary_chars.chars().collect()
    }
}

impl BoundaryScanner for SimpleBoundaryScanner {
    fn find_start_offset(&self, buffer: &str, start: i32) -> i32 {
        if start > buffer.chars().count() as i32 || start < 1 {
            return start;
        }

        let mut offset = start;
        let mut count = self.max_scan;
        let chars: Vec<char> = buffer.chars().collect();

        while offset > 0 && count > 0 {
            if self.boundary_chars.contains(&chars[(offset - 1) as usize]) {
                return offset;
            }

            count -= 1;
            offset -= 1;
        }

        if offset == 0 {
            return 0;
        }

        start
    }

    fn find_end_offset(&self, buffer: &str, start: i32) -> i32 {
        if start > buffer.chars().count() as i32 || start < 0 {
            return start;
        }

        let mut offset = start;
        let mut count = self.max_scan;
        let chars: Vec<char> = buffer.chars().collect();

        while offset < buffer.len() as i32 && count > 0 {
            if self.boundary_chars.contains(&chars[offset as usize]) {
                return offset;
            }

            count -= 1;
            offset += 1;
        }

        start
    }
}

///
// {@link org.apache.lucene.search.vectorhighlight.FragmentsBuilder} is an interface for fragments
// (snippets) builder classes. A {@link org.apache.lucene.search.vectorhighlight.FragmentsBuilder}
// class can be plugged in to {@link org.apache.lucene.search.vectorhighlight.
// FastVectorHighlighter}.
///
pub trait FragmentsBuilder {
    ///
    // create multiple fragments.
    //
    // @param reader IndexReader of the index
    // @param docId document id to be highlighted
    // @param fieldName field of the document to be highlighted
    // @param fieldFragList FieldFragList object
    // @param maxNumFragments maximum number of fragments
    // @param preTags pre-tags to be used to highlight terms
    // @param postTags post-tags to be used to highlight terms
    // @param encoder an encoder that generates encoded text
    // @return created fragments or null when no fragments created.
    //         size of the array can be less than maxNumFragments
    // @throws IOException If there is a low-level I/O error
    ///
    #[allow(too_many_arguments)]
    fn create_fragments(
        &self,
        reader: &IndexReader,
        doc_id: DocId,
        field_name: &str,
        field_frag_list: &mut FieldFragList,
        pre_tags: Option<&[String]>,
        post_tags: Option<&[String]>,
        max_num_fragments: Option<i32>,
        encoder: Option<&Encoder>,
        score_order: Option<bool>,
    ) -> Result<Vec<String>>;
}

///
// FragListBuilder is an interface for FieldFragList builder classes.
// A FragListBuilder class can be plugged in to Highlighter.
///
pub trait FragListBuilder {
    ///
    // create a FieldFragList.
    //
    // @param fieldPhraseList FieldPhraseList object
    // @param fragCharSize the length (number of chars) of a fragment
    // @return the created FieldFragList object
    ///
    fn create_field_frag_list(
        &self,
        field_phrase_list: &FieldPhraseList,
        frag_char_size: i32,
    ) -> Result<Box<FieldFragList>>;
}
