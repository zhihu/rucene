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

use error::{ErrorKind::IllegalArgument, Result};
use std::option::Option::{None, Some};
use std::result::Result::Ok;
use std::str::Chars;
use std::vec::Vec;

use core::codec::Codec;
use core::doc::Term;
use core::search::query::{BooleanQuery, BoostQuery, PhraseQuery, Query, TermQuery};

/// A query that parses a query string and runs it.
///
/// See: [`Query String Syntax`](https://www.elastic.co/guide/en/elasticsearch/reference/6.3/query-dsl-query-string-query.html#query-string-syntax) for more detail.
/// NOTE: currently this builder only support some of the syntax than elastic search do.
pub struct QueryStringQueryBuilder {
    query_string: String,
    fields: Vec<(String, f32)>,
    #[allow(dead_code)]
    min_should_match: i32,
    #[allow(dead_code)]
    boost: f32,
}

impl QueryStringQueryBuilder {
    pub fn new(
        query_string: String,
        fields: Vec<(String, f32)>,
        min_should_match: i32,
        boost: f32,
    ) -> QueryStringQueryBuilder {
        QueryStringQueryBuilder {
            query_string,
            fields,
            min_should_match,
            boost,
        }
    }

    pub fn build<C: Codec>(&self) -> Result<Box<dyn Query<C>>> {
        match self.parse_query(&mut self.query_string.chars(), None) {
            Ok(Some(q)) => Ok(q),
            Ok(None) => bail!(IllegalArgument("empty query string!".into())),
            Err(e) => Err(e),
        }
    }

    fn parse_query<C: Codec>(
        &self,
        chars: &mut Chars,
        end_char: Option<char>,
    ) -> Result<Option<Box<dyn Query<C>>>> {
        let mut musts = Vec::new();
        let mut shoulds = Vec::new();
        let mut is_option = true;
        while let Some(ch) = chars.next() {
            match ch {
                '+' => is_option = false,
                '|' => is_option = true,
                '(' => {
                    if let Ok(Some(query)) = self.parse_query(chars, Some(')')) {
                        if is_option {
                            shoulds.push(query);
                        } else {
                            musts.push(query);
                        }
                    }
                }
                '"' => {
                    let mut term_chars = Vec::new();
                    while let Some(ch) = chars.next() {
                        if ch == '"' {
                            break;
                        }
                        term_chars.push(ch);
                    }

                    if let Some(ch) = chars.next() {
                        if ch == '^' || ch == '~' {
                            term_chars.push(ch);
                            while let Some(ch) = chars.next() {
                                if ch == ' ' {
                                    break;
                                }
                                term_chars.push(ch);
                            }
                        }
                    }

                    if !term_chars.is_empty() {
                        let term: String = term_chars.iter().cloned().collect();
                        let query = self.build_field_query(term);
                        match query {
                            Ok(q) => {
                                if is_option {
                                    shoulds.push(q);
                                } else {
                                    musts.push(q);
                                }
                            }
                            Err(e) => {
                                return Err(e);
                            }
                        }
                    }
                    is_option = true;
                }
                ' ' => is_option = true,
                ')' => {
                    if end_char.is_none() || end_char.unwrap() != ')' {
                        bail!(IllegalArgument("parenthesis not match!".into()));
                    }
                    break;
                }
                _ => {
                    let mut term_chars = Vec::new();
                    term_chars.push(ch);
                    let mut should_return = false;
                    while let Some(c) = chars.next() {
                        if c == ' ' {
                            break;
                        }
                        if c == ')' {
                            if end_char.is_none() || end_char.unwrap() != ')' {
                                bail!(IllegalArgument("parenthesis not match!".into()));
                            }
                            should_return = true;
                            break;
                        }
                        term_chars.push(c);
                    }
                    if !term_chars.is_empty() {
                        let term: String = term_chars.iter().cloned().collect();
                        let query_res = self.build_field_query(term);
                        match query_res {
                            Ok(q) => {
                                if is_option {
                                    shoulds.push(q);
                                } else {
                                    musts.push(q);
                                }
                            }
                            Err(e) => {
                                return Err(e);
                            }
                        }
                    }
                    is_option = true;
                    if should_return {
                        break;
                    }
                }
            }
        }
        let query: Box<dyn Query<C>> = if musts.len() + shoulds.len() == 1 {
            if !musts.is_empty() {
                musts.remove(0)
            } else {
                shoulds.remove(0)
            }
        } else {
            BooleanQuery::build(musts, shoulds, vec![], vec![], self.min_should_match)?
        };
        Ok(Some(query))
    }

    fn term_query<C: Codec>(&self, term: String, field: String, boost: f32) -> Box<dyn Query<C>> {
        Box::new(TermQuery::new(Term::new(field, term.into()), boost, None))
    }

    fn build_field_query<C: Codec>(&self, term_boost: String) -> Result<Box<dyn Query<C>>> {
        let mut queries = if term_boost.find('~').is_some() {
            self.field_phrase_query(&term_boost)?
        } else {
            self.field_term_query(term_boost)?
        };

        let res = if queries.len() == 1 {
            queries.remove(0)
        } else {
            BooleanQuery::build(Vec::new(), queries, vec![], vec![], self.min_should_match)?
        };
        Ok(res)
    }

    fn field_term_query<C: Codec>(&self, query: String) -> Result<Vec<Box<dyn Query<C>>>> {
        let (term, boost) = if let Some(i) = query.find('^') {
            let (t, b) = query.split_at(i as usize);
            let boost_str: String = b.chars().skip(1).collect();
            let boost = boost_str.parse::<f32>()?;
            (t.to_string(), boost)
        } else {
            (query, 1f32)
        };
        let term = if term.starts_with('"') {
            term.chars().skip(1).take(term.len() - 2).collect()
        } else {
            term
        };
        let mut queries = Vec::new();
        for fb in &self.fields {
            queries.push(self.term_query(term.clone(), fb.0.clone(), fb.1 * boost));
        }
        Ok(queries)
    }

    fn field_phrase_query<C: Codec>(&self, query: &str) -> Result<Vec<Box<dyn Query<C>>>> {
        if let Some(idx) = query.find('~') {
            let (t, s) = query.split_at(idx);
            let slop_str: String = s.chars().skip(1).collect();
            let slop = slop_str.parse::<i32>()?;
            let term_strs: Vec<&str> = t.split_whitespace().collect();
            if term_strs.len() < 2 {
                bail!(IllegalArgument(
                    "phrase query terms size must not small than 2".into()
                ));
            }
            let mut queries = Vec::with_capacity(self.fields.len());
            for fb in &self.fields {
                let terms: Vec<Term> = term_strs
                    .iter()
                    .map(|term| Term::new(fb.0.clone(), term.as_bytes().to_vec()))
                    .collect();
                queries.push(BoostQuery::build(
                    Box::new(PhraseQuery::build(terms, slop, None, None)?),
                    fb.1,
                ))
            }

            Ok(queries)
        } else {
            bail!(IllegalArgument(format!(
                "invalid query string '{}' for phrase query",
                &query
            )));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::codec::tests::TestCodec;

    #[test]
    fn test_query_string_query() {
        let term = String::from("test");
        let field = String::from("title");
        let q: Box<dyn Query<TestCodec>> =
            QueryStringQueryBuilder::new(term.clone(), vec![(field, 1.0)], 0, 1.0)
                .build()
                .unwrap();
        let term_str: String = q.to_string();
        assert_eq!(
            term_str,
            String::from("TermQuery(field: title, term: test, boost: 1)")
        );

        let term = String::from("(test^0.2 | 测试^2)");
        let field = String::from("title");
        let q: Box<dyn Query<TestCodec>> =
            QueryStringQueryBuilder::new(term.clone(), vec![(field, 1.0)], 0, 2.0)
                .build()
                .unwrap();
        let term_str: String = q.to_string();
        assert_eq!(
            term_str,
            String::from(
                "BooleanQuery(must: [], should: [TermQuery(field: title, term: test, boost: 0.2), \
                 TermQuery(field: title, term: 测试, boost: 2)], filters: [], must_not: [], \
                 match: 1)",
            )
        );

        let term = String::from("test^0.2 \"测试\"^2");
        let field = String::from("title");
        let q: Box<dyn Query<TestCodec>> =
            QueryStringQueryBuilder::new(term.clone(), vec![(field, 1.0)], 0, 2.0)
                .build()
                .unwrap();
        let term_str: String = q.to_string();
        assert_eq!(
            term_str,
            String::from(
                "BooleanQuery(must: [], should: [TermQuery(field: title, term: test, boost: 0.2), \
                 TermQuery(field: title, term: 测试, boost: 2)], filters: [], must_not: [], \
                 match: 1)",
            )
        );

        let field = String::from("title");
        let q: Box<dyn Query<TestCodec>> =
            QueryStringQueryBuilder::new(String::from("+test"), vec![(field, 1.0)], 0, 1.0)
                .build()
                .unwrap();
        let term_str: String = q.to_string();
        assert_eq!(
            term_str,
            String::from("TermQuery(field: title, term: test, boost: 1)")
        );

        let query_string = String::from("test search");
        let field = String::from("title");
        let q: Box<dyn Query<TestCodec>> =
            QueryStringQueryBuilder::new(query_string.clone(), vec![(field, 1.0)], 0, 1.0)
                .build()
                .unwrap();
        let term_str: String = q.to_string();
        assert_eq!(
            term_str,
            String::from(
                "BooleanQuery(must: [], should: [TermQuery(field: title, term: test, boost: 1), \
                 TermQuery(field: title, term: search, boost: 1)], filters: [], must_not: [], \
                 match: 1)",
            )
        );

        let query_string = String::from("test +search");
        let field = String::from("title");
        let q: Box<dyn Query<TestCodec>> =
            QueryStringQueryBuilder::new(query_string.clone(), vec![(field, 1.0)], 0, 1.0)
                .build()
                .unwrap();
        let term_str: String = q.to_string();
        assert_eq!(
            term_str,
            String::from(
                "BooleanQuery(must: [TermQuery(field: title, term: search, boost: 1)], should: \
                 [TermQuery(field: title, term: test, boost: 1)], filters: [], must_not: [], \
                 match: 0)",
            )
        );

        let query_string = String::from("test +(search 搜索)");
        let field = String::from("title");
        let q: Box<dyn Query<TestCodec>> =
            QueryStringQueryBuilder::new(query_string.clone(), vec![(field, 1.0)], 0, 1.0)
                .build()
                .unwrap();
        let term_str: String = q.to_string();
        assert_eq!(
            term_str,
            String::from(
                "BooleanQuery(must: [BooleanQuery(must: [], should: [TermQuery(field: title, \
                 term: search, boost: 1), TermQuery(field: title, term: 搜索, boost: 1)], \
                 filters: [], must_not: [], match: 1)], should: [TermQuery(field: title, term: \
                 test, boost: 1)], filters: [], must_not: [], match: 0)",
            )
        );

        let query_string = String::from("test +search");
        let q: Box<dyn Query<TestCodec>> = QueryStringQueryBuilder::new(
            query_string.clone(),
            vec![("title".to_string(), 1.0), ("content".to_string(), 1.0)],
            0,
            1.0,
        )
        .build()
        .unwrap();
        let term_str: String = q.to_string();
        assert_eq!(
            term_str,
            String::from(
                "BooleanQuery(must: [BooleanQuery(must: [], should: [TermQuery(field: title, \
                 term: search, boost: 1), TermQuery(field: content, term: search, boost: 1)], \
                 filters: [], must_not: [], match: 1)], should: [BooleanQuery(must: [], should: \
                 [TermQuery(field: title, term: test, boost: 1), TermQuery(field: content, term: \
                 test, boost: 1)], filters: [], must_not: [], match: 1)], filters: [], must_not: \
                 [], match: 0)",
            )
        );

        let query_string = String::from(
            "从 +(市场定位 (+市场 +定位)) 分析 +b2b +((电子商务 电商^0.8) (+电子 +商务)) +网站",
        );
        let field = String::from("title");
        let q: Box<dyn Query<TestCodec>> =
            QueryStringQueryBuilder::new(query_string.clone(), vec![(field, 1.0)], 0, 1.0)
                .build()
                .unwrap();
        let term_str: String = q.to_string();
        assert_eq!(
            term_str,
            String::from(
                "BooleanQuery(must: [BooleanQuery(must: [], should: [TermQuery(field: title, \
                 term: 市场定位, boost: 1), BooleanQuery(must: [TermQuery(field: title, term: \
                 市场, boost: 1), TermQuery(field: title, term: 定位, boost: 1)], should: [], \
                 filters: [], must_not: [], match: 0)], filters: [], must_not: [], match: 1), \
                 TermQuery(field: title, term: b2b, boost: 1), BooleanQuery(must: [], should: \
                 [BooleanQuery(must: [], should: [TermQuery(field: title, term: 电子商务, boost: \
                 1), TermQuery(field: title, term: 电商, boost: 0.8)], filters: [], must_not: [], \
                 match: 1), BooleanQuery(must: [TermQuery(field: title, term: 电子, boost: 1), \
                 TermQuery(field: title, term: 商务, boost: 1)], should: [], filters: [], \
                 must_not: [], match: 0)], filters: [], must_not: [], match: 1), TermQuery(field: \
                 title, term: 网站, boost: 1)], should: [TermQuery(field: title, term: 从, boost: \
                 1), TermQuery(field: title, term: 分析, boost: 1)], filters: [], must_not: [], \
                 match: 0)",
            )
        );
    }
}
