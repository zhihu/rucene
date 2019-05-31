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

use error::{ErrorKind::IllegalArgument, Result};
use std::cmp::Ordering;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize)]
pub enum IndexOptions {
    Null,
    Docs,
    DocsAndFreqs,
    DocsAndFreqsAndPositions,
    DocsAndFreqsAndPositionsAndOffsets,
}

impl Default for IndexOptions {
    fn default() -> IndexOptions {
        IndexOptions::Null
    }
}

impl IndexOptions {
    pub fn from(options: &str) -> Result<IndexOptions> {
        let res = match options {
            "offsets" => IndexOptions::DocsAndFreqsAndPositionsAndOffsets,
            "positions" => IndexOptions::DocsAndFreqsAndPositions,
            "freqs" => IndexOptions::DocsAndFreqs,
            "docs" => IndexOptions::Docs,
            _ => {
                bail!(IllegalArgument(format!(
                    "failed to parse index option [{}]",
                    options
                )));
            }
        };
        Ok(res)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            IndexOptions::DocsAndFreqsAndPositionsAndOffsets => "offsets",
            IndexOptions::DocsAndFreqs => "freqs",
            IndexOptions::DocsAndFreqsAndPositions => "positions",
            IndexOptions::Docs => "docs",
            _ => unreachable!(),
        }
    }

    pub fn has_docs(self) -> bool {
        match self {
            IndexOptions::Null => false,
            _ => true,
        }
    }

    pub fn has_freqs(self) -> bool {
        match self {
            IndexOptions::DocsAndFreqs => true,
            IndexOptions::DocsAndFreqsAndPositions => true,
            IndexOptions::DocsAndFreqsAndPositionsAndOffsets => true,
            _ => false,
        }
    }

    pub fn has_positions(self) -> bool {
        match self {
            IndexOptions::DocsAndFreqsAndPositions => true,
            IndexOptions::DocsAndFreqsAndPositionsAndOffsets => true,
            _ => false,
        }
    }

    pub fn has_offsets(self) -> bool {
        match self {
            IndexOptions::DocsAndFreqsAndPositionsAndOffsets => true,
            _ => false,
        }
    }

    pub fn value(self) -> i32 {
        match self {
            IndexOptions::Null => 0,
            IndexOptions::Docs => 1,
            IndexOptions::DocsAndFreqs => 2,
            IndexOptions::DocsAndFreqsAndPositions => 3,
            IndexOptions::DocsAndFreqsAndPositionsAndOffsets => 4,
        }
    }
}

impl Ord for IndexOptions {
    fn cmp(&self, other: &Self) -> Ordering {
        self.value().cmp(&other.value())
    }
}

impl PartialOrd for IndexOptions {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
