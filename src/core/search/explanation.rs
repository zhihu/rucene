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

#[derive(Serialize, Deserialize)]
pub struct Explanation {
    is_match: bool,
    value: f32,
    description: String,
    details: Vec<Explanation>,
}

impl Explanation {
    pub fn new(
        is_match: bool,
        value: f32,
        description: String,
        details: Vec<Explanation>,
    ) -> Explanation {
        let value = if !is_match { 0.0f32 } else { value };

        Explanation {
            is_match,
            value,
            description,
            details,
        }
    }

    pub fn is_match(&self) -> bool {
        self.is_match
    }

    pub fn value(&self) -> f32 {
        self.value
    }

    pub fn description(&self) -> String {
        self.description.clone()
    }

    pub fn summary(&self) -> String {
        format!("{} = {}", self.value, self.description)
    }

    pub fn details(&self) -> &[Explanation] {
        self.details.as_ref()
    }

    pub fn to_string(&self, depth: i32) -> String {
        let mut buffer = String::from("");

        for _i in 0..depth {
            buffer.push_str("  ");
        }

        buffer.push_str(&self.summary());
        buffer.push_str("\n");

        for detail in &self.details {
            buffer.push_str(&detail.to_string(depth + 1))
        }

        buffer
    }
}

impl Clone for Explanation {
    fn clone(&self) -> Self {
        let mut details: Vec<Explanation> = vec![];
        for detail in &self.details {
            details.push(detail.clone());
        }
        Explanation {
            is_match: self.is_match,
            value: self.value(),
            description: self.description(),
            details,
        }
    }
}
