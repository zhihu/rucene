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

use core::codec::Terms;

use error::Result;

/// Flex API for access to fields and terms
pub trait Fields {
    type Terms: Terms;
    fn fields(&self) -> Vec<String>;
    fn terms(&self, field: &str) -> Result<Option<Self::Terms>>;
    fn size(&self) -> usize;
    fn terms_freq(&self, _field: &str) -> usize {
        unimplemented!()
    }
}
