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

use std::ops::Drop;
use std::time;

/// Tool to measure time of code block (usually a func) to execute.
/// Typical use:
/// let _a = ComputeTime::new(file!(), "search_req");
#[derive(Debug)]
pub struct ComputeTime {
    file: &'static str,
    func: &'static str,
    instant: time::Instant,
}

impl ComputeTime {
    pub fn new(file: &'static str, msg: &'static str) -> Self {
        ComputeTime {
            file,
            func: msg,
            instant: time::Instant::now(),
        }
    }

    #[inline]
    pub fn elapsed_ms_since(when: time::Instant) -> u64 {
        let elapsed = when.elapsed();
        elapsed.as_secs() * 1000 + u64::from(elapsed.subsec_nanos()) / 1_000_000
    }
}

impl Drop for ComputeTime {
    fn drop(&mut self) {
        let elapsed = self.instant.elapsed();
        let ms = elapsed.as_secs() * 1000 + u64::from(elapsed.subsec_nanos()) / 1_000_000;
        info!("[{}:{}] {}ms", self.file, self.func, ms);
    }
}
