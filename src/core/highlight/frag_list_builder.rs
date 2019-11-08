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

use core::highlight::{
    FieldFragList, FieldPhraseList, FragListBuilder, SimpleFieldFragList, WeightedPhraseInfo,
};
use error::Result;

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
        field_frag_list: &mut dyn FieldFragList,
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

    pub fn get_best_field_frag_list(
        &self,
        field_frag_list: &mut dyn FieldFragList,
        field_phrase_list: &FieldPhraseList,
        frag_char_size: i32,
    ) -> Result<()> {
        if frag_char_size < self.min_frag_char_size {
            panic!(
                "fragCharSize({}) is too small. It must be {} or higher.",
                frag_char_size, self.min_frag_char_size
            );
        }

        let phrase_len = field_phrase_list.phrase_list.len();
        if phrase_len > 0 {
            let frag_window_size = (frag_char_size - self.margin).max(self.min_frag_char_size);
            let mut score_map = vec![vec![0.0f32; phrase_len]; phrase_len];
            let mut best_first = 0;
            let mut best_last = 0;
            let mut best_score = field_phrase_list.phrase_list[0].boost;

            score_map[0][0] = field_phrase_list.phrase_list[0].boost;
            for i in 1..phrase_len {
                score_map[i][i] = field_phrase_list.phrase_list[i].boost;

                let mut j = i - 1;
                while j as i32 >= 0 {
                    let phrase_i = &field_phrase_list.phrase_list[i];
                    let phrase_j = &field_phrase_list.phrase_list[j];
                    score_map[i][j] = phrase_i.boost + score_map[i - 1][j];
                    if phrase_i.end_offset() - phrase_j.start_offset() > frag_window_size {
                        break;
                    }

                    if score_map[i][j] > best_score {
                        best_score = score_map[i][j];
                        best_first = j;
                        best_last = i;
                    }

                    j -= 1;
                }
            }

            let mut wpil: Vec<WeightedPhraseInfo> = vec![];
            for i in best_first..best_last + 1 {
                wpil.push(field_phrase_list.phrase_list[i].clone());
            }

            let wpil_len = wpil.len();
            if wpil_len > 0 {
                let mut start = wpil[0].start_offset();
                let mut end = wpil[wpil_len - 1].end_offset();

                if end - start < frag_char_size {
                    start = 0.max(start - (frag_char_size - (end - start)) / 2);
                    end = start + frag_char_size;
                }

                field_frag_list.add(start, end, &wpil);
            }
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
    ) -> Result<Box<dyn FieldFragList>> {
        let mut field_frag_list = SimpleFieldFragList::default();
        self.base_builder.get_best_field_frag_list(
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
    ) -> Result<Box<dyn FieldFragList>> {
        let mut ffl = SimpleFieldFragList::default();

        if !field_phrase_list.phrase_list.is_empty() {
            ffl.add(0, i32::MAX, field_phrase_list.phrase_list.as_ref());
        }

        Ok(Box::new(ffl))
    }
}
