use core::analysis::TokenStream;
use core::doc::FieldType;
use core::util::{Numeric, VariantValue};

use error::Result;

pub trait Fieldable {
    fn name(&self) -> &str;
    fn field_type(&self) -> &FieldType;
    fn boost(&self) -> f32;
    fn fields_data(&self) -> Option<&VariantValue>;
    fn token_stream(&mut self) -> Result<Box<dyn TokenStream>>;
    fn binary_value(&self) -> Option<&[u8]>;
    fn string_value(&self) -> Option<&str>;
    fn numeric_value(&self) -> Option<Numeric>;
}

impl<T: Fieldable + ?Sized> Fieldable for Box<T> {
    fn name(&self) -> &str {
        (**self).name()
    }
    fn field_type(&self) -> &FieldType {
        (**self).field_type()
    }
    fn boost(&self) -> f32 {
        (**self).boost()
    }
    fn fields_data(&self) -> Option<&VariantValue> {
        (**self).fields_data()
    }
    fn token_stream(&mut self) -> Result<Box<dyn TokenStream>> {
        (**self).token_stream()
    }
    fn binary_value(&self) -> Option<&[u8]> {
        (**self).binary_value()
    }
    fn string_value(&self) -> Option<&str> {
        (**self).string_value()
    }
    fn numeric_value(&self) -> Option<Numeric> {
        (**self).numeric_value()
    }
}
