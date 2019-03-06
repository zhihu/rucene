use core::analysis::TokenStream;
use core::doc::FieldType;
use core::util::{Numeric, VariantValue};

use error::Result;

pub trait Fieldable {
    fn name(&self) -> &str;
    fn field_type(&self) -> &FieldType;
    fn boost(&self) -> f32;
    fn fields_data(&self) -> Option<&VariantValue>;
    fn token_stream(&mut self) -> Result<Box<TokenStream>>;
    fn binary_value(&self) -> Option<&[u8]>;
    fn string_value(&self) -> Option<&str>;
    fn numeric_value(&self) -> Option<Numeric>;
}
