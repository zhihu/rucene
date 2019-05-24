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

pub struct BoostAttribute {
    boost: f32,
}

impl BoostAttribute {
    #[inline]
    pub fn new() -> BoostAttribute {
        BoostAttribute::from(1.0)
    }
    #[inline]
    pub fn from(boost: f32) -> BoostAttribute {
        BoostAttribute { boost }
    }
    #[inline]
    pub fn clear(&mut self) {
        self.boost = 1.0
    }
    #[inline]
    pub fn set_boost(&mut self, boost: f32) {
        self.boost = boost
    }
    #[inline]
    pub fn get_boost(&self) -> f32 {
        self.boost
    }
}

impl Default for BoostAttribute {
    #[inline]
    fn default() -> Self {
        BoostAttribute::from(1.0)
    }
}

pub struct MaxNonCompetitiveBoostAttribute {}
