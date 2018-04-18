use core::index::FieldInfo;

pub enum Status {
    YES,
    NO,
    STOP,
}

pub trait StoredFieldVisitor {
    fn binary_field(&mut self, field_info: &FieldInfo, value: &[u8]);
    fn string_field(&mut self, field_info: &FieldInfo, value: &[u8]);
    fn int_field(&mut self, field_info: &FieldInfo, value: i32);
    fn long_field(&mut self, field_info: &FieldInfo, value: i64);
    fn float_field(&mut self, field_info: &FieldInfo, value: f32);
    fn double_field(&mut self, field_info: &FieldInfo, value: f64);

    fn needs_field(&self, field_info: &FieldInfo) -> Status;
}
