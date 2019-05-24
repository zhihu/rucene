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

mod blocktree_reader;
pub use self::blocktree_reader::*;

mod blocktree_writer;
pub use self::blocktree_writer::*;

mod term_iter_frame;

const MAX_LONGS_SIZE: usize = 3;
