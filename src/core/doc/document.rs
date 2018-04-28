use core::doc::stored_field::StoredField;
use core::index::Fieldable;

#[derive(Debug)]
pub struct Document {
    pub fields: Vec<StoredField>,
}

impl Document {
    pub fn new(fields: Vec<StoredField>) -> Document {
        Document { fields }
    }
}

impl Document {
    pub fn add(&mut self, field: StoredField) {
        self.fields.push(field);
    }
    pub fn remove_field(&mut self, name: &str) {
        self.fields.retain(|ref v| v.field.name() != name);
    }
}
