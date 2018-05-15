use core::highlight::{FieldFragList, FieldPhraseList, FragListBuilder, SimpleFieldFragList,
                      WeightedPhraseInfo};
use error::*;

use std::cmp::max;
use std::i32;

pub struct BaseFragListBuilder {
    margin: i32,
    min_frag_char_size: i32,
}

impl BaseFragListBuilder {
    pub fn new(margin: Option<i32>) -> BaseFragListBuilder {
        const MARGIN_DEFAULT: i32 = 6;
        let margin = margin.unwrap_or(MARGIN_DEFAULT);

        let min_frag_char_size_factor: i32 = 3;
        BaseFragListBuilder {
            margin,
            min_frag_char_size: max(1, margin * min_frag_char_size_factor),
        }
    }

    pub fn create_field_frag_list(
        &self,
        field_frag_list: &mut FieldFragList,
        field_phrase_list: &FieldPhraseList,
        frag_char_size: i32,
    ) -> Result<()> {
        if frag_char_size < self.min_frag_char_size {
            panic!(
                "fragCharSize({}) is too small. It must be {} or higher.",
                frag_char_size, self.min_frag_char_size
            );
        }

        let mut wpil: Vec<WeightedPhraseInfo> = vec![];
        let mut start_offset = 0;
        let mut curr = 0;
        let phrase_len = field_phrase_list.phrase_list.len();
        while curr < phrase_len {
            let phrase_info = &field_phrase_list.phrase_list[curr];
            // if the phrase violates the border of previous fragment, discard it and try next
            // phrase
            if phrase_info.start_offset() < start_offset {
                curr += 1;
                continue;
            }

            wpil.clear();
            let curr_phrase_start_offset = phrase_info.start_offset();
            let mut curr_phrase_end_offset = phrase_info.end_offset();
            let mut span_start = max(curr_phrase_start_offset - self.margin, start_offset);
            let mut span_end = max(curr_phrase_end_offset, span_start + frag_char_size);

            curr += 1;
            if BaseFragListBuilder::accept_phrase(
                phrase_info,
                curr_phrase_end_offset - curr_phrase_start_offset,
                frag_char_size,
            ) {
                wpil.push(phrase_info.clone());
            }

            loop {
                // pull until we crossed the current spanEnd
                if curr >= phrase_len {
                    break;
                }

                let phrase_info = &field_phrase_list.phrase_list[curr];

                if phrase_info.end_offset() <= span_end {
                    curr_phrase_end_offset = phrase_info.end_offset();
                    curr += 1;
                    if BaseFragListBuilder::accept_phrase(
                        phrase_info,
                        curr_phrase_end_offset - curr_phrase_start_offset,
                        frag_char_size,
                    ) {
                        wpil.push(phrase_info.clone());
                    }
                } else {
                    break;
                }
            }

            if wpil.is_empty() {
                continue;
            }

            let match_len = curr_phrase_end_offset - curr_phrase_start_offset;
            // now recalculate the start and end position to "center" the result
            // matchLen can be > fragCharSize prevent IAOOB here
            let new_margin = max(0, (frag_char_size - match_len) / 2);
            span_start = curr_phrase_start_offset - new_margin;
            if span_start < start_offset {
                span_start = start_offset;
            }
            // whatever is bigger here we grow this out
            span_end = span_start + max(match_len, frag_char_size);
            start_offset = span_end;
            field_frag_list.add(span_start, span_end, &wpil);
        }

        Ok(())
    }

    #[inline]
    fn accept_phrase(info: &WeightedPhraseInfo, match_length: i32, frag_char_size: i32) -> bool {
        info.terms_offsets.len() <= 1 || match_length <= frag_char_size
    }
}

pub struct SimpleFragListBuilder {
    base_builder: BaseFragListBuilder,
}

impl SimpleFragListBuilder {
    pub fn new(margin: Option<i32>) -> SimpleFragListBuilder {
        SimpleFragListBuilder {
            base_builder: BaseFragListBuilder::new(margin),
        }
    }
}

impl FragListBuilder for SimpleFragListBuilder {
    fn create_field_frag_list(
        &self,
        field_phrase_list: &FieldPhraseList,
        frag_char_size: i32,
    ) -> Result<Box<FieldFragList>> {
        let mut field_frag_list = SimpleFieldFragList::default();
        self.base_builder.create_field_frag_list(
            &mut field_frag_list,
            field_phrase_list,
            frag_char_size,
        )?;

        Ok(Box::new(field_frag_list))
    }
}

pub struct SingleFragListBuilder {}

impl Default for SingleFragListBuilder {
    fn default() -> SingleFragListBuilder {
        SingleFragListBuilder {}
    }
}

impl FragListBuilder for SingleFragListBuilder {
    fn create_field_frag_list(
        &self,
        field_phrase_list: &FieldPhraseList,
        _frag_char_size: i32,
    ) -> Result<Box<FieldFragList>> {
        let mut ffl = SimpleFieldFragList::default();

        if !field_phrase_list.phrase_list.is_empty() {
            ffl.add(0, i32::MAX, field_phrase_list.phrase_list.as_ref());
        }

        Ok(Box::new(ffl))
    }
}
