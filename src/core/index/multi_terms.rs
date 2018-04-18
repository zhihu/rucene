use core::index::reader_slice::ReaderSlice;
use core::index::term::{TermIterator, Terms, TermsRef};
use error::Result;

pub struct MultiTerms {
    subs: Vec<TermsRef>,
    #[allow(dead_code)]
    sub_slices: Vec<ReaderSlice>,
    has_freqs: bool,
    has_offsets: bool,
    has_positions: bool,
    has_payloads: bool,
}

impl MultiTerms {
    pub fn new(subs: Vec<TermsRef>, sub_slices: Vec<ReaderSlice>) -> Result<MultiTerms> {
        let mut has_freqs = true;
        let mut has_offsets = true;
        let mut has_positions = true;
        let mut has_payloads = true;

        for i in &subs {
            has_freqs &= i.has_freqs()?;
            has_offsets &= i.has_offsets()?;
            has_positions &= i.has_positions()?;
            has_payloads |= i.has_payloads()?;
        }

        Ok(MultiTerms {
            subs,
            sub_slices,
            has_freqs,
            has_offsets,
            has_positions,
            has_payloads,
        })
    }
}

impl Terms for MultiTerms {
    fn iterator(&self) -> Result<Box<TermIterator>> {
        unimplemented!();
    }

    fn size(&self) -> Result<i64> {
        Ok(-1)
    }

    fn sum_total_term_freq(&self) -> Result<i64> {
        let mut sum = 0i64;
        for terms in &self.subs {
            let v = terms.sum_total_term_freq()?;
            if v == -1 {
                return Ok(-1i64);
            }
            sum += v
        }
        Ok(sum)
    }

    fn sum_doc_freq(&self) -> Result<i64> {
        let mut sum = 0i64;
        for terms in &self.subs {
            let v = terms.sum_doc_freq()?;
            if v == -1 {
                return Ok(-1i64);
            }
            sum += v
        }
        Ok(sum)
    }

    fn doc_count(&self) -> Result<i32> {
        let mut sum = 0;
        for terms in &self.subs {
            let v = terms.doc_count()?;
            if v == -1 {
                return Ok(-1);
            }
            sum += v
        }
        Ok(sum)
    }

    fn has_freqs(&self) -> Result<bool> {
        Ok(self.has_freqs)
    }

    fn has_offsets(&self) -> Result<bool> {
        Ok(self.has_offsets)
    }

    fn has_positions(&self) -> Result<bool> {
        Ok(self.has_positions)
    }

    fn has_payloads(&self) -> Result<bool> {
        Ok(self.has_payloads)
    }

    fn min(&self) -> Result<Vec<u8>> {
        unimplemented!();
    }

    fn max(&self) -> Result<Vec<u8>> {
        unimplemented!();
    }

    fn stats(&self) -> Result<String> {
        unimplemented!();
    }
}
