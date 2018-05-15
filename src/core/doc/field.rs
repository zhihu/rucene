use core::doc::FieldType;
use core::index::Fieldable;
use core::util::VariantValue;

#[derive(Clone, Debug)]
pub struct Field {
    name: String,
    field_type: FieldType,
    fields_data: VariantValue,
    boost: f32,
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

    fn fields_data(&self) -> &VariantValue {
        &self.fields_data
    }
}

impl Field {
    pub fn new(name: &str, field_type: FieldType, fields_data: VariantValue) -> Field {
        Field {
            field_type,
            name: name.to_string(),
            fields_data,
            boost: 1.0_f32,
        }
    }

    pub fn set_boost(&mut self, boost: f32) {
        self.boost = boost;
    }
}
