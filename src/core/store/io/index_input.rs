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

use core::store::io::{DataInput, RandomAccessInput};

use error::Result;

pub trait IndexInput: DataInput + Send + Sync {
    fn clone(&self) -> Result<Box<dyn IndexInput>>;

    fn file_pointer(&self) -> i64;
    fn seek(&mut self, pos: i64) -> Result<()>;
    fn len(&self) -> u64;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn name(&self) -> &str;

    fn random_access_slice(&self, _offset: i64, _length: i64)
        -> Result<Box<dyn RandomAccessInput>>;

    fn slice(&self, _description: &str, _offset: i64, _length: i64) -> Result<Box<dyn IndexInput>> {
        unimplemented!();
    }

    unsafe fn get_and_advance(&mut self, _length: usize) -> *const u8 {
        unimplemented!()
    }

    fn is_buffered(&self) -> bool {
        false
    }
}
