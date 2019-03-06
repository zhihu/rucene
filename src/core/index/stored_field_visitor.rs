use core::index::FieldInfo;

use error::Result;

pub enum Status {
    Yes,
    No,
    Stop,
}

pub trait StoredFieldVisitor {
    fn binary_field(&mut self, field_info: &FieldInfo, value: Vec<u8>) -> Result<()>;
    fn string_field(&mut self, field_info: &FieldInfo, value: Vec<u8>) -> Result<()>;
    fn int_field(&mut self, field_info: &FieldInfo, value: i32) -> Result<()>;
    fn long_field(&mut self, field_info: &FieldInfo, value: i64) -> Result<()>;
    fn float_field(&mut self, field_info: &FieldInfo, value: f32) -> Result<()>;
    fn double_field(&mut self, field_info: &FieldInfo, value: f64) -> Result<()>;

    fn needs_field(&self, field_info: &FieldInfo) -> Status;
}
