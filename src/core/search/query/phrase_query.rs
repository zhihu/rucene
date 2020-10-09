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

use std::boxed::Box;
use std::fmt;

use core::codec::Codec;
use core::codec::PostingIteratorFlags;
use core::codec::{TermIterator, Terms};
use core::doc::Term;
use core::index::reader::LeafReaderContext;
use core::search::explanation::Explanation;
use core::search::query::{Query, TermQuery, Weight};
use core::search::scorer::{ExactPhraseScorer, PostingsAndFreq, Scorer, SloppyPhraseScorer};
use core::search::searcher::SearchPlanBuilder;
use core::search::similarity::{SimWeight, Similarity};
use core::search::statistics::{CollectionStatistics, TermStatistics};
use core::search::DocIterator;
use core::util::{DocId, KeyedContext};
use error::{ErrorKind, Result};

pub const PHRASE: &str = "phrase";

/// A Query that matches documents containing a particular sequence of terms.
///
/// A PhraseQuery is built by QueryParser for input like `"new york"`.
///
/// This query may be combined with other terms or queries with a {@link BooleanQuery}.
///
/// *NOTE*:
/// All terms in the phrase must match, even those at the same position. If you
/// have terms at the same position, perhaps synonyms, you probably want `MultiPhraseQuery`
/// instead which only requires one term at a position to match.
///
/// Also, Leading holes don't have any particular meaning for this query
/// and will be ignored.
#[derive(Clone, Debug)]
pub struct PhraseQuery {
    field: String,
    terms: Vec<Term>,
    positions: Vec<i32>,
    slop: i32,
    ctx: Option<KeyedContext>,
    ctxs: Option<Vec<KeyedContext>>,
}

impl PhraseQuery {
    pub fn new<T: Into<Option<Vec<KeyedContext>>>, S: Into<Option<KeyedContext>>>(
        terms: Vec<Term>,
        positions: Vec<i32>,
        slop: i32,
        ctx: S,
        ctxs: T,
    ) -> Result<PhraseQuery> {
        let ctx = ctx.into();
        let ctxs = ctxs.into();
        debug_assert_eq!(
            terms.len(),
            positions.len(),
            "Must have as many terms as positions"
        );
        debug_assert_eq!(
            terms.len(),
            ctxs.as_ref().map(Vec::len).unwrap_or_else(|| terms.len()),
            "Must have as many terms as positions"
        );
        assert!(slop >= 0, format!("Slop must be >= 0, got {}", slop));
        if terms.len() < 2 {
            bail!(ErrorKind::IllegalArgument(
                "phrase query terms should not be less than 2!".into()
            ));
        }
        for i in 1..terms.len() {
            debug_assert_eq!(
                terms[i - 1].field,
                terms[i].field,
                "All terms should have the same field"
            );
        }
        for pos in &positions {
            debug_assert!(*pos >= 0, format!("Positions must be >= 0, got {}", pos));
        }
        for i in 1..positions.len() {
            debug_assert!(
                positions[i - 1] <= positions[i],
                format!(
                    "Positions should not go backwards, got {} before {}",
                    positions[i - 1],
                    positions[i]
                )
            );
        }
        // normalize positions
        let mut positions = positions;
        let first = positions[0];
        for pos in &mut positions {
            *pos -= first;
        }

        let field = terms[0].field.clone();

        Ok(PhraseQuery {
            field,
            terms,
            positions,
            slop,
            ctx,
            ctxs,
        })
    }

    pub fn build<T: Into<Option<Vec<KeyedContext>>>, S: Into<Option<KeyedContext>>>(
        terms: Vec<Term>,
        slop: i32,
        ctx: S,
        ctxs: T,
    ) -> Result<PhraseQuery> {
        let positions = Self::increment_positions(terms.len());
        Self::new(terms, positions, slop, ctx, ctxs)
    }

    fn increment_positions(length: usize) -> Vec<i32> {
        (0..length as i32).collect()
    }
}

impl<C: Codec> Query<C> for PhraseQuery {
    fn create_weight(
        &self,
        searcher: &dyn SearchPlanBuilder<C>,
        needs_scores: bool,
    ) -> Result<Box<dyn Weight<C>>> {
        debug_assert!(
            self.positions.len() >= 2,
            "PhraseWeight does not support less than 2 terms, call rewrite first"
        );
        debug_assert_eq!(
            self.positions[0], 0,
            "PhraseWeight requires that the first position is 0, call rewrite first"
        );

        let max_doc = i64::from(searcher.max_doc());
        let mut term_stats: Vec<TermStatistics> = Vec::with_capacity(self.terms.len());

        for i in 0..self.terms.len() {
            if needs_scores {
                term_stats.push(searcher.term_statistics(&self.terms[i])?);
            } else {
                term_stats.push(TermStatistics::new(
                    self.terms[i].bytes.clone(),
                    max_doc,
                    -1,
                ));
            };
        }

        let collection_stats = if needs_scores {
            if let Some(stat) = searcher.collections_statistics(&self.field) {
                stat.clone()
            } else {
                CollectionStatistics::new(self.field.clone(), 0, max_doc, -1, -1, -1)
            }
        } else {
            CollectionStatistics::new(self.field.clone(), 0, max_doc, -1, -1, -1)
        };

        let similarity = searcher.similarity(&self.field, needs_scores);

        let sim_weight =
            similarity.compute_weight(&collection_stats, &term_stats, self.ctx.as_ref(), 1.0f32);

        Ok(Box::new(PhraseWeight::new(
            self.field.clone(),
            self.terms.clone(),
            self.positions.clone(),
            self.slop,
            similarity,
            sim_weight,
            needs_scores,
        )))
    }

    fn extract_terms(&self) -> Vec<TermQuery> {
        let mut term_query_list: Vec<TermQuery> = vec![];
        let ctxs = self
            .ctxs
            .as_ref()
            .map(|ctxs| Clone::clone(ctxs).into_iter().map(Some).collect())
            .unwrap_or_else(|| vec![None; self.terms.len()]);

        for (term, ctx) in self.terms.iter().zip(ctxs.into_iter()) {
            term_query_list.push(TermQuery::new(term.clone(), 1.0f32, ctx));
        }

        term_query_list
    }

    fn as_any(&self) -> &dyn (::std::any::Any) {
        self
    }
}

impl fmt::Display for PhraseQuery {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "PhraseQuery(field: {}, terms: {:?}, positions: {:?}, slop: {})",
            &self.field, &self.terms, &self.positions, self.slop,
        )
    }
}

pub static TERM_POSNS_SEEK_OPS_PER_DOC: i32 = 128;
pub static TERM_OPS_PER_POS: i32 = 7;

struct PhraseWeight<C: Codec> {
    field: String,
    terms: Vec<Term>,
    positions: Vec<i32>,
    slop: i32,
    similarity: Box<dyn Similarity<C>>,
    sim_weight: Box<dyn SimWeight<C>>,
    needs_scores: bool,
}

impl<C: Codec> PhraseWeight<C> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        field: String,
        terms: Vec<Term>,
        positions: Vec<i32>,
        slop: i32,
        similarity: Box<dyn Similarity<C>>,
        sim_weight: Box<dyn SimWeight<C>>,
        needs_scores: bool,
    ) -> PhraseWeight<C> {
        PhraseWeight {
            field,
            terms,
            positions,
            slop,
            similarity,
            sim_weight,
            needs_scores,
        }
    }

    fn term_positions_cost(&self, term_iter: &mut impl TermIterator) -> Result<f32> {
        let doc_freq = term_iter.doc_freq()?;
        debug_assert!(doc_freq > 0);
        let total_term_freq = term_iter.total_term_freq()?; // -1 when not available
        let exp_occurrences_in_matching_doc = if total_term_freq < i64::from(doc_freq) {
            1.0f32
        } else {
            total_term_freq as f32 / doc_freq as f32
        };

        Ok(TERM_POSNS_SEEK_OPS_PER_DOC as f32
            + exp_occurrences_in_matching_doc * TERM_OPS_PER_POS as f32)
    }
}

impl<C: Codec> Weight<C> for PhraseWeight<C> {
    fn create_scorer(&self, reader: &LeafReaderContext<'_, C>) -> Result<Option<Box<dyn Scorer>>> {
        debug_assert!(!self.terms.len() >= 2);

        let mut postings_freqs = Vec::with_capacity(self.terms.len());
        let mut term_iter = if let Some(field_terms) = reader.reader.terms(&self.field)? {
            debug_assert!(
                field_terms.has_positions()?,
                format!(
                    "field {} was indexed without position data; cannot run PhraseQuery \
                     (phrase={:?})",
                    self.field, self.terms
                )
            );
            field_terms.iterator()?
        } else {
            return Ok(None);
        };

        let mut total_match_cost = 0f32;
        for i in 0..self.terms.len() {
            term_iter.seek_exact(self.terms[i].bytes.as_ref())?;
            total_match_cost += self.term_positions_cost(&mut term_iter)?;

            postings_freqs.push(PostingsAndFreq::new(
                term_iter.postings_with_flags(PostingIteratorFlags::POSITIONS)?,
                self.positions[i],
                &self.terms[i],
            ));
        }

        let sim_scorer = self.sim_weight.sim_scorer(reader.reader)?;
        let scorer: Box<dyn Scorer> = if self.slop == 0 {
            // sort by increasing docFreq order
            // optimize exact case

            postings_freqs.sort();
            Box::new(ExactPhraseScorer::new(
                postings_freqs,
                sim_scorer,
                self.needs_scores,
                total_match_cost,
            ))
        } else {
            Box::new(SloppyPhraseScorer::new(
                postings_freqs,
                self.slop,
                sim_scorer,
                self.needs_scores,
                total_match_cost,
            ))
        };
        Ok(Some(scorer))
    }

    fn query_type(&self) -> &'static str {
        PHRASE
    }

    fn normalize(&mut self, norm: f32, boost: f32) {
        self.sim_weight.normalize(norm, boost)
    }

    fn value_for_normalization(&self) -> f32 {
        self.sim_weight.get_value_for_normalization()
    }

    fn needs_scores(&self) -> bool {
        self.needs_scores
    }

    fn explain(&self, reader: &LeafReaderContext<'_, C>, doc: DocId) -> Result<Explanation> {
        debug_assert!(!self.terms.len() >= 2);

        let mut matched = true;
        let mut postings_freqs = Vec::with_capacity(self.terms.len());
        let mut term_iter = if let Some(field_terms) = reader.reader.terms(&self.field)? {
            debug_assert!(
                field_terms.has_positions()?,
                format!(
                    "field {} was indexed without position data; cannot run PhraseQuery \
                     (phrase={:?})",
                    self.field, self.terms
                )
            );
            Some(field_terms.iterator()?)
        } else {
            matched = false;
            None
        };

        let mut total_match_cost = 0f32;
        for i in 0..self.terms.len() {
            if let Some(ref mut term_iter) = term_iter {
                term_iter.seek_exact(self.terms[i].bytes.as_ref())?;
                total_match_cost += self.term_positions_cost(term_iter)?;

                postings_freqs.push(PostingsAndFreq::new(
                    term_iter.postings_with_flags(PostingIteratorFlags::POSITIONS)?,
                    self.positions[i],
                    &self.terms[i],
                ));
            }
        }

        if matched {
            let sim_scorer = self.sim_weight.sim_scorer(reader.reader)?;
            if self.slop == 0 {
                postings_freqs.sort();
                let mut scorer = ExactPhraseScorer::new(
                    postings_freqs,
                    sim_scorer,
                    self.needs_scores,
                    total_match_cost,
                );

                if scorer.advance(doc)? == doc {
                    let freq = scorer.freq() as f32;
                    let freq_expl =
                        Explanation::new(true, freq, format!("phraseFreq={}", freq), vec![]);
                    let score_expl = self.sim_weight.explain(reader.reader, doc, freq_expl)?;

                    return Ok(Explanation::new(
                        true,
                        score_expl.value(),
                        format!("weight({} in {}), result of:", self, doc),
                        vec![score_expl],
                    ));
                }
            } else {
                let mut scorer = SloppyPhraseScorer::new(
                    postings_freqs,
                    self.slop,
                    sim_scorer,
                    self.needs_scores,
                    total_match_cost,
                );

                if scorer.advance(doc)? == doc {
                    let freq = scorer.sloppy_freq();
                    let freq_expl =
                        Explanation::new(true, freq, format!("phraseFreq={}", freq), vec![]);
                    let score_expl = self.sim_weight.explain(reader.reader, doc, freq_expl)?;

                    return Ok(Explanation::new(
                        true,
                        score_expl.value(),
                        format!("weight({} in {}), result of:", self, doc),
                        vec![score_expl],
                    ));
                }
            }
        }

        Ok(Explanation::new(
            false,
            0.0f32,
            "no matching term".to_string(),
            vec![],
        ))
    }
}

impl<C: Codec> fmt::Display for PhraseWeight<C> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "PhraseWeight(field: {}, terms: {:?}, positions: {:?}, similarity: {}, need_score: {})",
            &self.field, &self.terms, &self.positions, &self.similarity, self.needs_scores
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use core::analysis::WhitespaceTokenizer;
    use core::doc::{Field, FieldType, Fieldable, IndexOptions, Term};
    use core::index::writer::{IndexWriter, IndexWriterConfig};
    use core::search::collector::TopDocsCollector;
    use core::search::{DefaultIndexSearcher, IndexSearcher};
    use core::store::directory::FSDirectory;

    use std::fs;
    use std::io;
    use std::path::Path;
    use std::sync::Arc;

    fn indexed_text_field_type() -> FieldType {
        let mut field_type = FieldType::default();
        field_type.index_options = IndexOptions::DocsAndFreqsAndPositionsAndOffsets;
        field_type.store_term_vectors = true;
        field_type.store_term_vector_offsets = true;
        field_type.store_term_vector_positions = true;
        field_type
    }

    fn new_index_text_field(field_name: String, text: String) -> Field {
        let token_stream = WhitespaceTokenizer::new(Box::new(StringReader::new(text)));
        Field::new(
            field_name,
            indexed_text_field_type(),
            None,
            Some(Box::new(token_stream)),
        )
    }

    struct StringReader {
        text: String,
        index: usize,
    }

    impl StringReader {
        fn new(text: String) -> Self {
            StringReader { text, index: 0 }
        }
    }

    impl io::Read for StringReader {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            let remain = buf.len().min(self.text.len() - self.index);
            if remain > 0 {
                buf[..remain]
                    .copy_from_slice(&self.text.as_bytes()[self.index..self.index + remain]);
                self.index += remain;
            }
            Ok(remain)
        }
    }

    #[test]
    fn phrase_query() {
        // create index directory
        let path = "/tmp/test_rucene";
        let dir_path = Path::new(path);
        if dir_path.exists() {
            fs::remove_dir_all(&dir_path).unwrap();
            fs::create_dir(&dir_path).unwrap();
        }

        // create index writer
        let config = Arc::new(IndexWriterConfig::default());
        let directory = Arc::new(FSDirectory::with_path(&dir_path).unwrap());
        let writer = IndexWriter::new(directory, config).unwrap();

        {
            let mut doc: Vec<Box<dyn Fieldable>> = vec![];
            // add indexed text field
            let text = "The quick brown fox jumps over a lazy dog";
            doc.push(Box::new(new_index_text_field("title".into(), text.into())));
            // add the document
            writer.add_document(doc).unwrap();
        }

        {
            let mut doc: Vec<Box<dyn Fieldable>> = vec![];
            // add indexed text field
            let text = "The quick fox jumps over a lazy dog";
            doc.push(Box::new(new_index_text_field("title".into(), text.into())));
            // add the document
            writer.add_document(doc).unwrap();
        }

        {
            let mut doc: Vec<Box<dyn Fieldable>> = vec![];
            // add indexed text field
            let text = "The fox jumps quick over a lazy dog";
            doc.push(Box::new(new_index_text_field("title".into(), text.into())));
            // add the document
            writer.add_document(doc).unwrap();
        }
        // flush to disk
        writer.commit().unwrap();

        // new index search
        let reader = writer.get_reader(true, false).unwrap();
        let index_searcher = DefaultIndexSearcher::new(Arc::new(reader), None);

        {
            // search slop 0
            let query: PhraseQuery = PhraseQuery::new(
                vec![
                    Term::new("title".into(), "quick".as_bytes().to_vec()),
                    Term::new("title".into(), "fox".as_bytes().to_vec()),
                ],
                vec![0, 1],
                0,
                None,
                None,
            )
            .unwrap();
            let mut collector = TopDocsCollector::new(10);
            index_searcher.search(&query, &mut collector).unwrap();
            let top_docs = collector.top_docs();
            assert_eq!(top_docs.total_hits(), 1);
        }

        {
            // search slop 1
            let query: PhraseQuery = PhraseQuery::new(
                vec![
                    Term::new("title".into(), "quick".as_bytes().to_vec()),
                    Term::new("title".into(), "fox".as_bytes().to_vec()),
                ],
                vec![0, 1],
                1,
                None,
                None,
            )
            .unwrap();
            let mut collector = TopDocsCollector::new(10);
            index_searcher.search(&query, &mut collector).unwrap();
            let top_docs = collector.top_docs();
            assert_eq!(top_docs.total_hits(), 2);
        }

        {
            // search slop 2
            let query: PhraseQuery = PhraseQuery::new(
                vec![
                    Term::new("title".into(), "quick".as_bytes().to_vec()),
                    Term::new("title".into(), "fox".as_bytes().to_vec()),
                ],
                vec![0, 1],
                2,
                None,
                None,
            )
            .unwrap();
            let mut collector = TopDocsCollector::new(10);
            index_searcher.search(&query, &mut collector).unwrap();
            let top_docs = collector.top_docs();
            assert_eq!(top_docs.total_hits(), 2);
        }

        {
            // search slop 3
            let query: PhraseQuery = PhraseQuery::new(
                vec![
                    Term::new("title".into(), "quick".as_bytes().to_vec()),
                    Term::new("title".into(), "fox".as_bytes().to_vec()),
                ],
                vec![0, 1],
                3,
                None,
                None,
            )
            .unwrap();
            let mut collector = TopDocsCollector::new(10);
            index_searcher.search(&query, &mut collector).unwrap();
            let top_docs = collector.top_docs();
            assert_eq!(top_docs.total_hits(), 3);
        }
    }
}
