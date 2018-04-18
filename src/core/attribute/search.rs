pub struct BoostAttribute {
    boost: f32,
}

impl BoostAttribute {
    pub fn new() -> BoostAttribute {
        BoostAttribute::from(1.0)
    }
    pub fn from(boost: f32) -> BoostAttribute {
        BoostAttribute { boost }
    }
    pub fn clear(&mut self) {
        self.boost = 1.0
    }
    pub fn set_boost(&mut self, boost: f32) {
        self.boost = boost
    }
    pub fn get_boost(&self) -> f32 {
        self.boost
    }
}

impl Default for BoostAttribute {
    fn default() -> Self {
        BoostAttribute::from(1.0)
    }
}

pub struct MaxNonCompetitiveBoostAttribute {}
