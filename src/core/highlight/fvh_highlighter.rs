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

use core::codec::Codec;
use core::highlight::frag_list_builder::SimpleFragListBuilder;
use core::highlight::fragments_builder::BaseFragmentsBuilder;
use core::highlight::{
    Encoder, FieldFragList, FieldPhraseList, FieldQuery, FieldTermStack, FragListBuilder,
    FragmentsBuilder,
};

use core::index::reader::{IndexReader, LeafReaderContext};
use core::search::query::Query;
use core::util::DocId;
use error::Result;

use std::i32;

const DEFAULT_PHRASE_HIGHLIGHT: bool = true;
const DEFAULT_FIELD_MATCH: bool = true;
const DEFAULT_PHRASE_LIMIT: i32 = 256;

pub struct FastVectorHighlighter {
    pub phrase_highlight: bool,
    pub field_match: bool,
    frag_list_builder: Box<dyn FragListBuilder>,
    fragments_builder: BaseFragmentsBuilder,
    pub phrase_limit: i32,
}

impl Default for FastVectorHighlighter {
    fn default() -> Self {
        Self::new(None, None, None, None, None)
    }
}

impl FastVectorHighlighter {
    pub fn new(
        phrase_highlight: Option<bool>,
        field_match: Option<bool>,
        frag_list_builder: Option<Box<dyn FragListBuilder>>,
        fragments_builder: Option<BaseFragmentsBuilder>,
        phrase_limit: Option<i32>,
    ) -> FastVectorHighlighter {
        FastVectorHighlighter {
            phrase_highlight: match phrase_highlight {
                Some(x) => x,
                None => DEFAULT_PHRASE_HIGHLIGHT,
            },
            field_match: match field_match {
                Some(x) => x,
                None => DEFAULT_FIELD_MATCH,
            },
            frag_list_builder: match frag_list_builder {
                Some(x) => x,
                None => Box::new(SimpleFragListBuilder::new(None)),
            },
            fragments_builder: match fragments_builder {
                Some(x) => x,
                None => BaseFragmentsBuilder::new(None, None, None),
            },
            phrase_limit: match phrase_limit {
                Some(x) => x,
                None => DEFAULT_PHRASE_LIMIT,
            },
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn get_best_fragments<C: Codec>(
        &mut self,
        field_query: &mut FieldQuery,
        reader: &LeafReaderContext<'_, C>,
        doc_id: DocId,
        field_name: &str,
        stored_field: &str,
        frag_char_size: i32,
        max_num_fragments: Option<i32>,
        frag_list_builder: Option<&dyn FragListBuilder>,
        fragments_builder: Option<&BaseFragmentsBuilder>,
        pre_tags: Option<&[String]>,
        post_tags: Option<&[String]>,
        encoder: Option<&dyn Encoder>,
        score_order: Option<bool>,
    ) -> Result<Vec<String>> {
        let frag_list_builder = match frag_list_builder {
            Some(builder) => builder,
            None => self.frag_list_builder.as_ref(),
        };

        let mut field_frag_list = self.get_field_frag_list(
            frag_list_builder,
            field_query,
            reader,
            doc_id,
            field_name,
            frag_char_size,
        )?;

        let fragments_builder = match fragments_builder {
            Some(builder) => builder,
            None => &self.fragments_builder,
        };

        fragments_builder.create_fragments(
            reader.parent,
            doc_id,
            stored_field,
            field_frag_list.as_mut(),
            pre_tags,
            post_tags,
            max_num_fragments,
            encoder,
            score_order,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn get_best_fragments_with_tags_fields<C: Codec>(
        &mut self,
        field_query: &mut FieldQuery,
        reader: &LeafReaderContext<'_, C>,
        doc_id: DocId,
        stored_field: &str,
        matched_fields: &[String],
        frag_char_size: i32,
        max_num_fragments: Option<i32>,
        frag_list_builder: &dyn FragListBuilder,
        fragments_builder: &BaseFragmentsBuilder,
        pre_tags: Option<&[String]>,
        post_tags: Option<&[String]>,
        encoder: Option<&dyn Encoder>,
        score_ordered: Option<bool>,
    ) -> Result<Vec<String>> {
        let mut field_frag_list = self.get_fields_frag_list(
            frag_list_builder,
            field_query,
            reader,
            doc_id,
            matched_fields,
            frag_char_size,
        )?;

        fragments_builder.create_fragments(
            reader.parent,
            doc_id,
            stored_field,
            field_frag_list.as_mut(),
            pre_tags,
            post_tags,
            max_num_fragments,
            encoder,
            score_ordered,
        )
    }

    pub fn get_field_query<C: Codec>(
        &self,
        query: &dyn Query<C>,
        reader: Option<&dyn IndexReader<Codec = C>>,
    ) -> Result<FieldQuery> {
        FieldQuery::new(query, reader, self.phrase_highlight, self.field_match)
    }

    fn get_field_frag_list<C: Codec>(
        &self,
        frag_list_builder: &dyn FragListBuilder,
        field_query: &FieldQuery,
        reader: &LeafReaderContext<'_, C>,
        doc_id: DocId,
        field: &str,
        frag_char_size: i32,
    ) -> Result<Box<dyn FieldFragList>> {
        let mut field_term_stack = FieldTermStack::new(reader, doc_id, field, field_query)?;
        let field_phrase_list =
            FieldPhraseList::new(&mut field_term_stack, field_query, self.phrase_limit);

        frag_list_builder.create_field_frag_list(&field_phrase_list, frag_char_size)
    }

    fn get_fields_frag_list<C: Codec>(
        &self,
        frag_list_builder: &dyn FragListBuilder,
        field_query: &FieldQuery,
        reader: &LeafReaderContext<'_, C>,
        doc_id: DocId,
        matched_fields: &[String],
        frag_char_size: i32,
    ) -> Result<Box<dyn FieldFragList>> {
        assert!(!matched_fields.is_empty());

        let mut to_merge: Vec<FieldPhraseList> = Vec::with_capacity(matched_fields.len());
        for field in matched_fields {
            let mut stack = FieldTermStack::new(reader, doc_id, field, field_query)?;
            to_merge.push(FieldPhraseList::new(
                &mut stack,
                field_query,
                self.phrase_limit,
            ));
        }

        let field_phrase_list = FieldPhraseList::merge_new(to_merge);

        frag_list_builder.create_field_frag_list(&field_phrase_list, frag_char_size)
    }
}
