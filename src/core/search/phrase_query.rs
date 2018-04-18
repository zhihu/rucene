use error::*;
use std::boxed::Box;
use std::fmt;
use std::sync::Arc;

use core::index::LeafReader;
use core::index::Term;
use core::index::TermContext;
use core::index::{POSTINGS_FREQS, POSTINGS_NONE};
use core::search::bm25_similarity::BM25Similarity;
use core::search::searcher::IndexSearcher;
use core::search::statistics::*;
use core::search::term_scorer::TermScorer;
use core::search::DocIterator;
use core::search::Query;
use core::search::Scorer;
use core::search::Similarity;
use core::search::SimilarityEnum;
use core::search::Weight;

#[derive(Clone, Debug)]
pub struct PhraseQuery {
    pub terms: Vec<Term>,
    pub boost: f32,
}

impl PhraseQuery {
    pub fn new(terms: Vec<Term>, boost: f32) -> PhraseQuery {
        PhraseQuery { terms, boost }
    }
}

impl Query for PhraseQuery {
    fn create_weight(&self, searcher: &IndexSearcher, needs_scores: bool) -> Result<Box<Weight>> {
        /*
        let reader = searcher.reader.as_ref();
        let mut term_context = TermContext::new(reader);
        term_context.build(reader, &self.term)?;
        let max_doc = i64::from(reader.max_doc());
        let (term_stats, collection_stats) = if needs_scores {
            (
                searcher.term_statistics(self.term.clone(), &term_context),
                searcher.collections_statistics(self.term.field.clone())?,
            )
        } else {
            (
                TermStatistics::new(self.term.bytes.clone(), max_doc, -1),
                CollectionStatistics::new(self.term.field.clone(), max_doc, -1, -1, -1),
            )
        };

        match searcher.similarity() {
            SimilarityEnum::BM25 { k1, b } => {
                let similarity = BM25Similarity::new(k1, b);
                let sim_weight = similarity.compute_weight(&collection_stats, &term_stats);
                Ok(Box::new(TermWeight::new(
                    self.term.clone(),
                    self.boost,
                    similarity,
                    sim_weight,
                    needs_scores,
                )))
            }
        }
        */
        unimplemented!();
    }

    fn extract_terms(&self) -> Vec<PhraseQuery> {
        self.terms.iter().map(|t| Self::new(t, self.boost)).collect()
    }
}

impl fmt::Display for PhraseQuery {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "PhraseQuery(boost: {})",
            self.boost
        )
        /*
        write!(
            f,
            "PhraseQuery(field: {}, term: {}, boost: {})",
            &self.term.field(),
            &self.term.text().unwrap(),
            self.boost
        )
        */
    }
}

pub struct PhraseWeight<T: Similarity> {
    term: Phrase,
    boost: f32,
    similarity: T,
    sim_weight: Arc<T::Weight>,
    needs_scores: bool,
}

impl<T: Similarity> PhraseWeight<T> {
    pub fn new(
        term: Phrase,
        boost: f32,
        similarity: T,
        sim_weight: T::Weight,
        needs_scores: bool,
    ) -> PhraseWeight<T> {
        PhraseWeight {
            term,
            boost,
            similarity,
            sim_weight: Arc::new(sim_weight),
            needs_scores,
        }
    }

    fn create_doc_iterator(&self, reader: &LeafReader, flags: i32) -> Result<Box<DocIterator>> {
        reader.docs(&self.term, flags)
    }
}

impl<T: Similarity> Weight for PhraseWeight<T> {
    fn create_scorer(&self, reader: &LeafReader) -> Result<Box<Scorer>> {
        let _norms = reader.norm_values(&self.term.field);
        let sim_scorer = self.similarity.sim_scorer(self.sim_weight.clone(), reader)?;

        let flags = if self.needs_scores {
            POSTINGS_FREQS
        } else {
            POSTINGS_NONE
        };

        Ok(Box::new(PhraseScorer::new(
            sim_scorer,
            self.create_doc_iterator(reader, i32::from(flags))?,
            self.boost,
        )))
    }
}
