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
        for i in 0..self.fields.len() {
            if self.fields[i].field.name().eq(name) {
                self.fields.remove(i);
                return;
            }
        }
    }

    pub fn remove_fields(&mut self, name: &str) {
        for i in 0..self.fields.len() {
            if self.fields[i].field.name().eq(name) {
                self.fields.remove(i);
            }
        }
    }
}
