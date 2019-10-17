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

extern crate crc;

use core::store::io::{ChecksumIndexInput, DataInput, IndexInput, RandomAccessInput};

use error::ErrorKind::IllegalArgument;
use error::Result;

use crc::{crc32, Hasher32};
use std::io::Read;

/// Simple implementation of `ChecksumIndexInput` that wraps
/// another input and delegates calls.
pub struct BufferedChecksumIndexInput {
    index_input: Box<dyn IndexInput>,
    digest: crc32::Digest,
    name: String,
}

impl BufferedChecksumIndexInput {
    pub fn new(index_input: Box<dyn IndexInput>) -> BufferedChecksumIndexInput {
        let digest = crc32::Digest::new_with_initial(crc32::IEEE, 0u32);
        let name = String::from(index_input.name());
        BufferedChecksumIndexInput {
            index_input,
            digest,
            name,
        }
    }
}

impl ChecksumIndexInput for BufferedChecksumIndexInput {
    fn checksum(&self) -> i64 {
        i64::from(self.digest.sum32())
    }
}

impl DataInput for BufferedChecksumIndexInput {}

impl Read for BufferedChecksumIndexInput {
    fn read(&mut self, buf: &mut [u8]) -> ::std::io::Result<usize> {
        let length = self.index_input.read(buf)?;
        self.digest.write(&buf[0..length]);
        Ok(length)
    }
}

impl IndexInput for BufferedChecksumIndexInput {
    fn clone(&self) -> Result<Box<dyn IndexInput>> {
        Ok(Box::new(Self {
            index_input: self.index_input.clone()?,
            digest: crc32::Digest::new_with_initial(crc32::IEEE, self.digest.sum32()),
            name: self.name.clone(),
        }))
    }
    fn file_pointer(&self) -> i64 {
        self.index_input.file_pointer()
    }

    fn seek(&mut self, pos: i64) -> Result<()> {
        let curr_pos = self.file_pointer();
        let to_skip = pos - curr_pos;
        if to_skip < 0 {
            bail!(IllegalArgument(format!(
                "Can't seek backwards: {} => {}",
                curr_pos, pos
            )));
        }
        self.skip_bytes(to_skip as usize)
    }

    fn len(&self) -> u64 {
        self.index_input.len()
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn random_access_slice(
        &self,
        _offset: i64,
        _length: i64,
    ) -> Result<Box<dyn RandomAccessInput>> {
        unimplemented!()
    }
}
