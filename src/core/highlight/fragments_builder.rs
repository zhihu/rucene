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

use std::borrow::Borrow;
use std::cmp::min;
use std::collections::HashMap;

use core::codec::Codec;
use core::doc::{Fieldable, StoredField};
use core::highlight::{
    BoundaryScanner, DefaultEncoder, Encoder, FieldFragList, FragmentsBuilder,
    SimpleBoundaryScanner, SubInfo, Toffs, WeightedFragInfo,
};
use core::index::reader::IndexReader;
use core::util::DocId;

use error::Result;

pub struct BaseFragmentsBuilder {
    pre_tags: Vec<String>,
    post_tags: Vec<String>,
    multi_valued_separator: char,
    boundary_scanner: Box<dyn BoundaryScanner>,
    pub discrete_multi_value_highlighting: bool,
}

impl BaseFragmentsBuilder {
    pub fn new(
        pre_tags: Option<&[String]>,
        post_tags: Option<&[String]>,
        boundary_scanner: Option<Box<dyn BoundaryScanner>>,
    ) -> BaseFragmentsBuilder {
        BaseFragmentsBuilder {
            pre_tags: pre_tags.map_or(vec!["<b>".to_owned()], |x| x.to_vec()),
            post_tags: post_tags.map_or(vec!["</b>".to_owned()], |x| x.to_vec()),
            multi_valued_separator: ' ',
            boundary_scanner: boundary_scanner
                .unwrap_or_else(|| Box::new(SimpleBoundaryScanner::new(None, None))),
            discrete_multi_value_highlighting: false,
        }
    }

    fn fields<C: Codec>(
        &self,
        reader: &dyn IndexReader<Codec = C>,
        doc_id: DocId,
        field_name: &str,
    ) -> Result<Vec<StoredField>> {
        let fields = [field_name.to_string()];
        // let mut visitor = DocumentStoredFieldVisitor::new(&fields);

        let document = reader.document(doc_id, &fields)?;
        Ok(document.fields)
    }

    fn discrete_multi_value_highlighting(
        &self,
        frag_infos: &mut Vec<WeightedFragInfo>,
        fields: &[StoredField],
    ) -> Vec<WeightedFragInfo> {
        let mut field_name_to_frag_infos: HashMap<String, Vec<WeightedFragInfo>> = HashMap::new();
        for field in fields {
            field_name_to_frag_infos.insert(String::from(field.field.name()), vec![]);
        }

        'fragInfos: for frag_info in frag_infos {
            let mut field_start;
            let mut field_end = 0i32;

            for field in fields {
                let string_value = format!("{}", field.field.field_data().unwrap());
                if string_value.is_empty() {
                    field_end += 1;
                    continue;
                }

                field_start = field_end;
                // + 1 for going to next field with same name.
                field_end += string_value.chars().count() as i32 + 1;

                if frag_info.start_offset >= field_start
                    && frag_info.end_offset >= field_start
                    && frag_info.start_offset <= field_end
                    && frag_info.end_offset <= field_end
                {
                    if let Some(ref mut x) = field_name_to_frag_infos.get_mut(field.field.name()) {
                        x.push(frag_info.clone())
                    }

                    continue 'fragInfos;
                }

                if frag_info.sub_infos.is_empty() {
                    continue 'fragInfos;
                }

                if frag_info.start_offset >= field_end
                    || frag_info.sub_infos[0].term_offsets[0].start_offset >= field_end
                {
                    continue;
                }

                let frag_start =
                    if frag_info.start_offset > field_start && frag_info.start_offset < field_end {
                        frag_info.start_offset
                    } else {
                        field_start
                    };

                let frag_end =
                    if frag_info.end_offset > field_start && frag_info.end_offset < field_end {
                        frag_info.end_offset
                    } else {
                        field_end
                    };

                let mut sub_infos: Vec<SubInfo> = vec![];
                let mut boost = 0f32;

                for sub_info in &mut frag_info.sub_infos {
                    let mut toffs_list: Vec<Toffs> = vec![];

                    for toffs in &mut sub_info.term_offsets {
                        if toffs.start_offset >= field_end {
                            // We've gone past this value so its not worth iterating any more.
                            break;
                        }

                        let starts_after_field = toffs.start_offset >= field_start;
                        let ends_before_field = toffs.end_offset < field_end;
                        if starts_after_field && ends_before_field {
                            toffs_list.push(toffs.clone());
                            // sub_info.terms_offsets.remove_item(toffs);
                            toffs.end_offset = toffs.start_offset - 1;
                        } else if starts_after_field {
                            // The Toffs starts within this value but ends after this value
                            // so we clamp the returned Toffs to this value and leave the
                            // Toffs in the iterator for the next value of this field.
                            //
                            toffs_list.push(Toffs::new(toffs.start_offset, field_end - 1));
                        } else if ends_before_field {
                            // The Toffs starts before this value but ends in this value
                            // which means we're really continuing from where we left off
                            // above. Since we use the remainder of the offset we can remove
                            // it from the iterator.
                            //
                            toffs_list.push(Toffs::new(field_start, toffs.end_offset));
                            // sub_info.terms_offsets.remove_item(toffs);
                            toffs.end_offset = toffs.start_offset - 1;
                        } else {
                            // The Toffs spans the whole value so we clamp on both sides.
                            // This is basically a combination of both arms of the loop
                            // above.
                            //
                            toffs_list.push(Toffs::new(field_start, field_end - 1));
                        }
                    }

                    if !toffs_list.is_empty() {
                        sub_infos.push(SubInfo::new(
                            sub_info.text.clone(),
                            toffs_list,
                            sub_info.seqnum,
                            sub_info.boost,
                        ));
                        boost += sub_info.boost;
                    }

                    let mut i = sub_info.term_offsets.len();
                    while i > 0 {
                        if sub_info.term_offsets[i - 1].start_offset - 1
                            == sub_info.term_offsets[i - 1].end_offset
                        {
                            sub_info.term_offsets.remove(i - 1);
                        }

                        i -= 1;
                    }
                }

                let mut i = frag_info.sub_infos.len();
                while i > 0 {
                    if frag_info.sub_infos[i - 1].term_offsets.is_empty() {
                        frag_info.sub_infos.remove(i - 1);
                    }

                    i -= 1;
                }

                let weighted_frag_info =
                    WeightedFragInfo::new(sub_infos, boost, frag_start, frag_end);
                if let Some(ref mut x) = field_name_to_frag_infos.get_mut(field.field.name()) {
                    x.push(weighted_frag_info)
                }
            }
        }

        let mut result: Vec<_> = field_name_to_frag_infos
            .into_iter()
            .flat_map(|(_k, v)| v)
            .collect();
        result.sort_by(|a, b| a.start_offset.cmp(&b.start_offset));

        result
    }

    fn score_order_weighted_frag_info_list(&self, frag_infos: &mut Vec<WeightedFragInfo>) {
        frag_infos.sort_by(WeightedFragInfo::order_by_boost_and_offset);
    }

    #[allow(clippy::too_many_arguments)]
    fn make_fragment(
        &self,
        buffer: &mut String,
        index: &mut Vec<i32>,
        values: &[StoredField],
        frag_info: &WeightedFragInfo,
        pre_tags: &[String],
        post_tags: &[String],
        encoder: &dyn Encoder,
    ) -> Result<String> {
        let mut fragment = String::from("");
        let s = frag_info.start_offset;
        let mut modified_start_offset = vec![s];
        let src = self.get_fragment_source_mso(
            buffer,
            index,
            values,
            s,
            frag_info.end_offset,
            &mut modified_start_offset,
        )?;

        let src_chars: Vec<char> = src.chars().collect();
        let src_len = src_chars.len() as i32;
        let mut original: String;
        let mut src_index = 0;
        for sub_info in &frag_info.sub_infos {
            for to in &sub_info.term_offsets {
                let offset_delta = if to.end_offset - modified_start_offset[0] > src_len {
                    to.end_offset - modified_start_offset[0] - src_len
                } else {
                    0
                };

                if src_index > (to.start_offset - offset_delta - modified_start_offset[0])
                    || (to.start_offset - offset_delta - modified_start_offset[0]) >= src_len
                {
                    continue;
                }

                original = src_chars[src_index as usize
                    ..(to.start_offset - offset_delta - modified_start_offset[0]) as usize]
                    .iter()
                    .collect();
                fragment.push_str(encoder.encode_text(original.as_str()).borrow());

                fragment.push_str(Self::tag(pre_tags, sub_info.seqnum));

                original = src_chars[(to.start_offset - offset_delta - modified_start_offset[0])
                    as usize
                    ..(to.end_offset - offset_delta - modified_start_offset[0]) as usize]
                    .iter()
                    .collect();
                fragment.push_str(encoder.encode_text(original.as_str()).borrow());

                fragment.push_str(Self::tag(post_tags, sub_info.seqnum));

                src_index = to.end_offset - offset_delta - modified_start_offset[0];
            }
        }

        original = src_chars[src_index as usize..src.chars().count()]
            .iter()
            .collect();
        fragment.push_str(encoder.encode_text(original.as_str()).borrow());

        Ok(fragment)
    }

    fn get_fragment_source_mso(
        &self,
        buffer: &mut String,
        index: &mut Vec<i32>,
        values: &[StoredField],
        start_offset: i32,
        end_offset: i32,
        modified_start_offset: &mut Vec<i32>,
    ) -> Result<String> {
        while (buffer.chars().count() as i32) < end_offset && index[0] < (values.len() as i32) {
            let i = index[0];
            buffer.push_str(format!("{}", values[i as usize].field.field_data().unwrap()).as_str());
            index[0] += 1;
            buffer.push(self.multi_valued_separator);
        }

        let mut buffer_len = buffer.chars().count() as i32;
        // we added the multi value char to the last buffer, ignore it
        if values[index[0] as usize - 1].field.field_type().tokenized() {
            buffer_len -= 1;
        }

        let mut start_offset = start_offset;
        let eo = if buffer_len < end_offset {
            if start_offset - (end_offset - buffer_len - 1) <= 0 {
                start_offset = 0;
            }

            buffer_len
        } else {
            self.boundary_scanner.find_end_offset(buffer, end_offset)
        };

        modified_start_offset[0] = self
            .boundary_scanner
            .find_start_offset(buffer, start_offset);

        let buffer_chars: Vec<char> = buffer.chars().collect();
        if buffer_chars.len() < eo as usize || modified_start_offset[0] > eo {
            bail!(
                "get fragmets source error, source len: {}, highlight required slice [{}..{}], \
                 so={} eo={} buffer={}",
                buffer_chars.len(),
                modified_start_offset[0],
                eo,
                start_offset,
                end_offset,
                buffer,
            );
        }
        let ret: String = buffer_chars[modified_start_offset[0] as usize..eo as usize]
            .iter()
            .collect();

        Ok(ret)
    }

    #[inline]
    fn tag(tags: &[String], num: i32) -> &str {
        let num = num as usize % tags.len();
        tags[num].as_ref()
    }
}

impl FragmentsBuilder for BaseFragmentsBuilder {
    fn create_fragments<C: Codec>(
        &self,
        reader: &dyn IndexReader<Codec = C>,
        doc_id: DocId,
        field_name: &str,
        field_frag_list: &mut dyn FieldFragList,
        pre_tags: Option<&[String]>,
        post_tags: Option<&[String]>,
        max_num_fragments: Option<i32>,
        encoder: Option<&dyn Encoder>,
        score_ordered: Option<bool>,
    ) -> Result<Vec<String>> {
        let pre_tags = match pre_tags {
            Some(x) => x,
            None => &self.pre_tags,
        };
        let post_tags = match post_tags {
            Some(x) => x,
            None => &self.post_tags,
        };
        let max_num_fragments = match max_num_fragments {
            Some(x) => x,
            None => 1,
        };
        let null_decoder = DefaultEncoder::default();
        let encoder = match encoder {
            Some(x) => x,
            None => &null_decoder,
        };
        let score_ordered = match score_ordered {
            Some(x) => x,
            None => false,
        };

        assert!(
            max_num_fragments > 0,
            "maxNumFragments({}) must be positive number.",
            max_num_fragments
        );

        let values = self.fields(reader, doc_id, field_name)?;
        if values.is_empty() {
            Ok(vec![])
        } else {
            let mut discrete_frag_infos;

            let frag_infos = if self.discrete_multi_value_highlighting && values.len() > 1 {
                discrete_frag_infos =
                    self.discrete_multi_value_highlighting(field_frag_list.frag_infos(), &values);
                &mut discrete_frag_infos
            } else {
                field_frag_list.frag_infos()
            };

            if score_ordered {
                self.score_order_weighted_frag_info_list(frag_infos);
            }

            let limit_fragments = min(max_num_fragments as usize, frag_infos.len());
            let mut fragments: Vec<String> = Vec::with_capacity(limit_fragments);

            let mut buffer = String::from("");
            let mut next_value_index = vec![0];

            for frag_info in frag_infos.iter().take(limit_fragments as usize) {
                fragments.push(self.make_fragment(
                    &mut buffer,
                    &mut next_value_index,
                    &values,
                    frag_info,
                    pre_tags,
                    post_tags,
                    encoder,
                )?);
            }

            Ok(fragments)
        }
    }
}
