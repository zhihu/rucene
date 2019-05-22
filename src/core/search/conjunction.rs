use core::search::{two_phase_next, DocIterator, Scorer, NO_MORE_DOCS};
use core::util::DocId;
use error::Result;

pub struct ConjunctionScorer<T: Scorer> {
    lead1: T,
    lead2: T,
    others: Vec<T>,
    support_two_phase: bool,
    two_phase_match_cost: f32,
}

impl<T: Scorer> ConjunctionScorer<T> {
    pub fn new(mut children: Vec<T>) -> ConjunctionScorer<T> {
        assert!(children.len() >= 2);

        // Sort the scores the first time to allow the least cost DocIterator to
        // lead the matching.
        let support_two_phase = children.iter().any(|scorer| scorer.support_two_phase());
        let two_phase_match_cost = if support_two_phase {
            children.iter().map(|s| s.match_cost()).sum()
        } else {
            0f32
        };
        children.sort_by(|a, b| a.cost().cmp(&b.cost()));

        let others = children.drain(2..).collect();

        let lead2 = children.remove(1);
        let lead1 = children.remove(0);

        ConjunctionScorer {
            lead1,
            lead2,
            others,
            support_two_phase,
            two_phase_match_cost,
        }
    }

    fn skip_to_approx(&mut self, target: DocId) -> Result<DocId> {
        let mut doc = target;

        'advanceHead: loop {
            debug_assert_eq!(self.lead1.doc_id(), doc);

            // find agreement between the two iterators with the lower costs
            // we special case them because they do not need the
            // 'other.doc_id() < target' check that the 'others' iterators need
            let next2 = self.lead2.approximate_advance(doc)?;

            if next2 != doc {
                doc = self.lead1.approximate_advance(next2)?;
                if next2 != doc {
                    continue;
                }
            }

            if doc == NO_MORE_DOCS {
                return Ok(doc);
            }

            // then find agreement with other iterators
            for other in &mut self.others {
                // other.doc may already be equal to doc if we "continued advanceHead"
                // on the previous iteration and the advance on the lead scorer exactly matched.
                if other.doc_id() < doc {
                    let next = other.approximate_advance(doc)?;

                    if next > doc {
                        // iterator beyond the current doc - advance lead
                        // and continue to the new highest doc.
                        doc = self.lead1.approximate_advance(next)?;
                        continue 'advanceHead;
                    }
                }
            }
            return Ok(doc);
        }
    }
}

impl<T: Scorer> Scorer for ConjunctionScorer<T> {
    fn score(&mut self) -> Result<f32> {
        let mut score: f32 = self.lead1.score()?;
        score += self.lead2.score()?;
        for scorer in &mut self.others {
            score += scorer.score()?;
        }

        Ok(score)
    }
    fn support_two_phase(&self) -> bool {
        self.support_two_phase
    }
}

impl<T: Scorer> DocIterator for ConjunctionScorer<T> {
    fn doc_id(&self) -> DocId {
        self.lead1.doc_id()
    }

    fn next(&mut self) -> Result<DocId> {
        self.approximate_next()?;
        two_phase_next(self)
    }

    fn advance(&mut self, target: DocId) -> Result<DocId> {
        self.approximate_advance(target)?;
        two_phase_next(self)
    }

    fn cost(&self) -> usize {
        self.lead1.cost()
    }

    fn matches(&mut self) -> Result<bool> {
        if !self.support_two_phase {
            Ok(true)
        } else if !self.lead1.matches()? || !self.lead2.matches()? {
            Ok(false)
        } else {
            let mut res = true;
            for s in &mut self.others {
                if !s.matches()? {
                    res = false;
                    break;
                }
            }
            Ok(res)
        }
    }

    fn match_cost(&self) -> f32 {
        self.two_phase_match_cost
    }

    fn approximate_next(&mut self) -> Result<DocId> {
        let doc = self.lead1.approximate_next()?;
        self.skip_to_approx(doc)
    }

    fn approximate_advance(&mut self, target: DocId) -> Result<DocId> {
        let doc = self.lead1.approximate_advance(target)?;
        self.skip_to_approx(doc)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use core::search::tests::*;

    #[test]
    fn test_mock_doc_iterator_next() {
        let mut it = MockDocIterator::new(vec![1, 2, 3, 4, 5]);

        assert_eq!(it.doc_id(), -1);
        assert_eq!(it.next().unwrap(), 1);
        assert_eq!(it.doc_id(), 1);

        assert_eq!(it.next().unwrap(), 2);
        assert_eq!(it.doc_id(), 2);

        assert_eq!(it.next().unwrap(), 3);
        assert_eq!(it.doc_id(), 3);

        assert_eq!(it.next().unwrap(), 4);
        assert_eq!(it.doc_id(), 4);

        assert_eq!(it.next().unwrap(), 5);
        assert_eq!(it.doc_id(), 5);

        assert_eq!(it.next().unwrap(), NO_MORE_DOCS);
        assert_eq!(it.doc_id(), NO_MORE_DOCS);

        it = MockDocIterator::new(vec![1, 2, 3, 4, 5]);
        assert_eq!(it.advance(4).unwrap(), 4);
    }

    #[test]
    fn test_conjunction_iterator_next() {
        let mut iterator = create_conjunction_scorer();

        assert_eq!(iterator.next().unwrap(), 2);
        assert_eq!(iterator.doc_id(), 2);

        assert_eq!(iterator.next().unwrap(), 5);
        assert_eq!(iterator.doc_id(), 5);

        assert_eq!(iterator.next().unwrap(), NO_MORE_DOCS);
        assert_eq!(iterator.doc_id(), NO_MORE_DOCS);

        let mut two_phase = create_conjunction_two_phase_scorer();
        assert_eq!(two_phase.approximate_next().unwrap(), 2);
        assert_eq!(two_phase.doc_id(), 2);
        assert!(!two_phase.matches().unwrap());

        assert_eq!(two_phase.approximate_next().unwrap(), 3);
        assert_eq!(two_phase.doc_id(), 3);
        assert!(two_phase.matches().unwrap());

        assert_eq!(two_phase.next().unwrap(), 7);
        assert_eq!(two_phase.doc_id(), 7);
        assert!(two_phase.matches().unwrap());
    }

    #[test]
    fn test_conjunction_iterator_advance() {
        {
            let mut iterator = create_conjunction_scorer();
            assert!(!iterator.support_two_phase());
            assert_eq!(iterator.advance(1).unwrap(), 2);
            assert_eq!(iterator.doc_id(), 2);
        }

        {
            let mut iterator = create_conjunction_scorer();
            assert_eq!(iterator.advance(2).unwrap(), 2);
            assert_eq!(iterator.doc_id(), 2);

            assert_eq!(iterator.advance(5).unwrap(), 5);
            assert_eq!(iterator.doc_id(), 5);

            assert_eq!(iterator.advance(7).unwrap(), NO_MORE_DOCS);
            assert_eq!(iterator.doc_id(), NO_MORE_DOCS);
        }
        {
            let mut two_phase = create_conjunction_two_phase_scorer();
            assert!(two_phase.support_two_phase());
            assert_eq!(two_phase.approximate_advance(2).unwrap(), 2);
            assert_eq!(two_phase.doc_id(), 2);
            assert!(!two_phase.matches().unwrap());

            assert_eq!(two_phase.approximate_advance(5).unwrap(), 5);
            assert_eq!(two_phase.doc_id(), 5);
            assert!(!two_phase.matches().unwrap());
        }
        {
            let mut two_phase = create_conjunction_two_phase_scorer();
            assert_eq!(two_phase.advance(2).unwrap(), 3);
            assert_eq!(two_phase.doc_id(), 3);
            assert!(two_phase.matches().unwrap());

            assert_eq!(two_phase.advance(5).unwrap(), 7);
            assert_eq!(two_phase.doc_id(), 7);
            assert!(two_phase.matches().unwrap());
        }
        {
            let mut two_phase = create_conjunction_two_phase_scorer();
            assert_eq!(two_phase.advance(7).unwrap(), 7);
            assert_eq!(two_phase.doc_id(), 7);
            assert!(two_phase.matches().unwrap());
        }
    }

    #[test]
    fn test_conjunction_scorer() {
        let mut scorer = create_conjunction_scorer();
        assert_eq!(scorer.doc_id(), -1);
        assert!((scorer.score().unwrap() + 3.0) < ::std::f32::EPSILON);

        scorer.next().unwrap();
        assert_eq!(scorer.doc_id(), 2);
        assert!((scorer.score().unwrap() - 6.0) < ::std::f32::EPSILON);

        scorer.next().unwrap();
        assert_eq!(scorer.doc_id(), 5);
        assert!((scorer.score().unwrap() - 15.0) < ::std::f32::EPSILON);

        scorer.next().unwrap();
        assert_eq!(scorer.doc_id(), NO_MORE_DOCS);
    }

    fn create_conjunction_scorer() -> ConjunctionScorer<MockSimpleScorer<MockDocIterator>> {
        let s1 = create_mock_scorer(vec![1, 2, 3, 4, 5]);
        let s2 = create_mock_scorer(vec![2, 5]);
        let s3 = create_mock_scorer(vec![2, 3, 4, 5]);

        ConjunctionScorer::new(vec![s1, s2, s3])
    }

    fn create_conjunction_two_phase_scorer() -> ConjunctionScorer<Box<dyn Scorer>> {
        let s1 = create_mock_scorer(vec![1, 2, 3, 4, 5, 6, 7, 8]);
        let s2 = create_mock_scorer(vec![2, 3, 5, 7, 8]);
        let s3 = create_mock_two_phase_scorer(vec![1, 2, 3, 4, 5, 6, 7], vec![1, 4, 5]);
        let s4 = create_mock_two_phase_scorer(vec![1, 2, 3, 4, 5, 6, 7], vec![2, 4]);

        let scorers: Vec<Box<dyn Scorer>> =
            vec![Box::new(s1), Box::new(s2), Box::new(s3), Box::new(s4)];
        ConjunctionScorer::new(scorers)
    }
}
