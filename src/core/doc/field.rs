use core::analysis::TokenStream;
use core::attribute::{BytesTermAttribute, PayloadAttribute, TermToBytesRefAttribute};
use core::attribute::{CharTermAttribute, OffsetAttribute, PositionIncrementAttribute};
use core::doc::FieldType;
use core::index::{Fieldable, IndexOptions};
use core::util::byte_ref::BytesRef;
use core::util::{Numeric, VariantValue};

use error::{ErrorKind, Result};

use std::cmp::Ordering;
use std::collections::HashSet;
use std::hash::Hash;
use std::mem;

#[derive(Debug)]
pub struct Field {
    name: String,
    field_type: FieldType,
    fields_data: Option<VariantValue>,
    boost: f32,
    token_stream: Option<Box<TokenStream>>,
}

impl Field {
    pub fn new(
        name: String,
        field_type: FieldType,
        fields_data: Option<VariantValue>,
        token_stream: Option<Box<TokenStream>>,
    ) -> Field {
        Field {
            field_type,
            name,
            fields_data,
            boost: 1.0_f32,
            token_stream,
        }
    }

    pub fn new_bytes(name: String, bytes: Vec<u8>, field_type: FieldType) -> Self {
        Field {
            name,
            fields_data: Some(VariantValue::Binary(bytes)),
            field_type,
            token_stream: None,
            boost: 1.0,
        }
    }

    pub fn set_boost(&mut self, boost: f32) {
        self.boost = boost;
    }

    pub fn set_fields_data(&mut self, data: Option<VariantValue>) {
        self.fields_data = data;
    }
}

impl Fieldable for Field {
    fn name(&self) -> &str {
        &self.name
    }

    fn field_type(&self) -> &FieldType {
        &self.field_type
    }

    fn boost(&self) -> f32 {
        self.boost
    }

    fn fields_data(&self) -> Option<&VariantValue> {
        self.fields_data.as_ref()
    }

    // TODO currently this function should only be called once per doc field
    fn token_stream(&mut self) -> Result<Box<TokenStream>> {
        debug_assert_ne!(self.field_type.index_options, IndexOptions::Null);

        if !self.field_type.tokenized {
            if let Some(ref field_data) = self.fields_data {
                match field_data {
                    VariantValue::VString(s) => {
                        return Ok(Box::new(StringTokenStream::new(s.clone())));
                    }
                    VariantValue::Binary(b) => {
                        return Ok(Box::new(BinaryTokenStream::new(BytesRef::new(b.as_ref()))));
                    }
                    _ => {
                        bail!(ErrorKind::IllegalArgument(
                            "Non-Tokenized Fields must have a String value".into()
                        ));
                    }
                }
            }
        }

        if self.token_stream.is_some() {
            let stream = mem::replace(&mut self.token_stream, None);
            Ok(stream.unwrap())
        } else {
            // TODO currently not support analyzer
            unimplemented!()
        }
    }

    fn binary_value(&self) -> Option<&[u8]> {
        self.fields_data.as_ref().and_then(|f| f.get_binary())
    }

    fn string_value(&self) -> Option<&str> {
        self.fields_data.as_ref().and_then(|f| f.get_string())
    }

    fn numeric_value(&self) -> Option<Numeric> {
        self.fields_data.as_ref().and_then(|f| f.get_numeric())
    }
}

impl Clone for Field {
    fn clone(&self) -> Self {
        Field {
            name: self.name.clone(),
            field_type: self.field_type.clone(),
            fields_data: self.fields_data.clone(),
            boost: self.boost,
            token_stream: None,
            // TODO, no used
        }
    }
}

#[derive(Debug)]
struct StringTokenStream {
    term_attribute: CharTermAttribute,
    offset_attribute: OffsetAttribute,
    position_attribute: PositionIncrementAttribute,
    payload_attribute: PayloadAttribute,
    used: bool,
    value: String,
}

impl StringTokenStream {
    fn new(value: String) -> Self {
        StringTokenStream {
            term_attribute: CharTermAttribute::new(),
            offset_attribute: OffsetAttribute::new(),
            position_attribute: PositionIncrementAttribute::new(),
            payload_attribute: PayloadAttribute::new(Vec::with_capacity(0)),
            used: true,
            value,
        }
    }
}

impl TokenStream for StringTokenStream {
    fn increment_token(&mut self) -> Result<bool> {
        if self.used {
            return Ok(false);
        }
        self.clear_attributes();

        self.term_attribute.append(&self.value);
        self.offset_attribute.set_offset(0, self.value.len())?;
        self.used = true;
        Ok(true)
    }

    fn end(&mut self) -> Result<()> {
        self.end_attributes();
        let final_offset = self.value.len();
        self.offset_attribute.set_offset(final_offset, final_offset)
    }

    fn reset(&mut self) -> Result<()> {
        self.used = false;
        Ok(())
    }

    fn offset_attribute_mut(&mut self) -> &mut OffsetAttribute {
        &mut self.offset_attribute
    }

    fn offset_attribute(&self) -> &OffsetAttribute {
        &self.offset_attribute
    }

    fn position_attribute_mut(&mut self) -> &mut PositionIncrementAttribute {
        &mut self.position_attribute
    }

    fn term_bytes_attribute_mut(&mut self) -> &mut TermToBytesRefAttribute {
        &mut self.term_attribute
    }

    fn term_bytes_attribute(&self) -> &TermToBytesRefAttribute {
        &self.term_attribute
    }
}

#[derive(Debug)]
pub struct BinaryTokenStream {
    term_attribute: BytesTermAttribute,
    offset_attribute: OffsetAttribute,
    position_attribute: PositionIncrementAttribute,
    payload_attribute: PayloadAttribute,
    used: bool,
    value: BytesRef,
}

impl BinaryTokenStream {
    pub fn new(value: BytesRef) -> Self {
        BinaryTokenStream {
            term_attribute: BytesTermAttribute::new(),
            offset_attribute: OffsetAttribute::new(),
            position_attribute: PositionIncrementAttribute::new(),
            payload_attribute: PayloadAttribute::new(Vec::with_capacity(0)),
            used: true,
            value,
        }
    }
}

impl TokenStream for BinaryTokenStream {
    fn increment_token(&mut self) -> Result<bool> {
        if self.used {
            return Ok(false);
        }
        self.clear_attributes();

        self.term_attribute.set_bytes(self.value.bytes());
        self.used = true;
        Ok(true)
    }

    fn end(&mut self) -> Result<()> {
        self.end_attributes();
        Ok(())
    }

    fn reset(&mut self) -> Result<()> {
        self.used = false;
        Ok(())
    }

    fn offset_attribute_mut(&mut self) -> &mut OffsetAttribute {
        &mut self.offset_attribute
    }

    fn offset_attribute(&self) -> &OffsetAttribute {
        &self.offset_attribute
    }

    fn position_attribute_mut(&mut self) -> &mut PositionIncrementAttribute {
        &mut self.position_attribute
    }

    fn term_bytes_attribute_mut(&mut self) -> &mut TermToBytesRefAttribute {
        &mut self.term_attribute
    }

    fn term_bytes_attribute(&self) -> &TermToBytesRefAttribute {
        &self.term_attribute
    }
}

pub const MAX_WORD_LEN: usize = 128;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Word {
    value: String,
    begin: usize,
    length: usize,
}

impl Word {
    pub fn new(value: &str, begin: usize, length: usize) -> Word {
        let mut value = value.to_string();
        let length = if length > MAX_WORD_LEN {
            value = value.chars().take(MAX_WORD_LEN).collect();
            MAX_WORD_LEN
        } else {
            length
        };

        Word {
            value,
            begin,
            length,
        }
    }
}

#[derive(Debug)]
pub struct WordTokenStream {
    term_attribute: CharTermAttribute,
    offset_attribute: OffsetAttribute,
    position_attribute: PositionIncrementAttribute,
    payload_attribute: PayloadAttribute,
    values: Vec<Word>,
    current: usize,
}

impl WordTokenStream {
    pub fn new(values: Vec<Word>) -> WordTokenStream {
        let mut elements = values;
        let set: HashSet<_> = elements.drain(..).collect();
        elements.extend(set.into_iter());

        elements.sort_by(|a, b| {
            let cmp = a.begin.cmp(&b.begin);
            if cmp == Ordering::Equal {
                a.length.cmp(&b.length)
            } else {
                cmp
            }
        });

        WordTokenStream {
            term_attribute: CharTermAttribute::new(),
            offset_attribute: OffsetAttribute::new(),
            position_attribute: PositionIncrementAttribute::new(),
            payload_attribute: PayloadAttribute::new(Vec::with_capacity(0)),
            values: elements,
            current: 0,
        }
    }
}

impl TokenStream for WordTokenStream {
    fn increment_token(&mut self) -> Result<bool> {
        if self.current == self.values.len() {
            return Ok(false);
        }

        self.clear_attributes();

        let word: &Word = &self.values[self.current];
        self.term_attribute.append(&word.value);
        self.offset_attribute
            .set_offset(word.begin, word.begin + word.length)?;
        self.current += 1;
        Ok(true)
    }

    fn end(&mut self) -> Result<()> {
        self.end_attributes();
        if let Some(word) = self.values.last() {
            let final_offset = word.begin + word.length;
            // self.offset_attribute.set_offset(final_offset, final_offset)
            self.offset_attribute.set_offset(0, 0)
        } else {
            self.offset_attribute.set_offset(0, 0)
        }
    }

    fn reset(&mut self) -> Result<()> {
        self.current = 0;
        Ok(())
    }

    fn offset_attribute_mut(&mut self) -> &mut OffsetAttribute {
        &mut self.offset_attribute
    }

    fn offset_attribute(&self) -> &OffsetAttribute {
        &self.offset_attribute
    }

    fn position_attribute_mut(&mut self) -> &mut PositionIncrementAttribute {
        &mut self.position_attribute
    }

    fn term_bytes_attribute_mut(&mut self) -> &mut TermToBytesRefAttribute {
        &mut self.term_attribute
    }

    fn term_bytes_attribute(&self) -> &TermToBytesRefAttribute {
        &self.term_attribute
    }
}
