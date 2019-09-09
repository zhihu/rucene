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

use error::Result;

/// Random Access Index API.
///
/// Unlike `IndexInput`, this has no concept of file position, all reads
/// are absolute. However, like IndexInput, it is only intended for use by a single thread.
pub trait RandomAccessInput: Send + Sync {
    fn read_byte(&self, pos: u64) -> Result<u8>;
    fn read_short(&self, pos: u64) -> Result<i16>;
    fn read_int(&self, pos: u64) -> Result<i32>;
    fn read_long(&self, pos: u64) -> Result<i64>;
}
