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

use core::store::io::{DataInput, DataOutput};
use error::Result;
use std::cmp::min;
use std::vec::Vec;

use core::util::fst::{Output, OutputFactory};

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct ByteSequenceOutput {
    bytes: Vec<u8>,
}

impl Into<Vec<u8>> for ByteSequenceOutput {
    fn into(self) -> Vec<u8> {
        self.bytes
    }
}

impl ByteSequenceOutput {
    pub fn new(bytes: Vec<u8>) -> ByteSequenceOutput {
        ByteSequenceOutput { bytes }
    }

    pub fn empty() -> ByteSequenceOutput {
        ByteSequenceOutput {
            bytes: Vec::with_capacity(0),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    fn starts_with(&self, other: &ByteSequenceOutput) -> bool {
        self.bytes.starts_with(&other.bytes)
    }

    #[inline]
    pub fn inner(&self) -> &[u8] {
        &self.bytes
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.bytes.len() == 0
    }
}

impl Clone for ByteSequenceOutput {
    fn clone(&self) -> Self {
        if self.bytes.is_empty() {
            ByteSequenceOutput {
                bytes: Vec::with_capacity(1),
            }
        } else {
            ByteSequenceOutput::new(self.bytes.clone())
        }
    }
}

impl Output for ByteSequenceOutput {
    type Value = Vec<u8>;

    fn prefix(&self, other: &ByteSequenceOutput) -> ByteSequenceOutput {
        let mut pos1 = 0;
        let mut pos2 = 0;
        let stop = min(self.bytes.len(), other.bytes.len());

        while pos1 < stop {
            if self.bytes[pos1] != other.bytes[pos2] {
                break;
            }

            pos1 += 1;
            pos2 += 1;
        }

        if pos1 == 0 {
            // No common prefix
            ByteSequenceOutput::empty()
        } else if pos1 == self.bytes.len() {
            // self.bytes is a prefix of other
            self.clone()
        } else if pos2 == other.bytes.len() {
            // other is a prefix of prefix
            other.clone()
        } else {
            ByteSequenceOutput::new(self.bytes[0..pos1].to_vec())
        }
    }

    fn cat(&self, other: &ByteSequenceOutput) -> ByteSequenceOutput {
        if self.is_empty() {
            other.clone()
        } else if other.is_empty() {
            self.clone()
        } else {
            let total_length = self.bytes.len() + other.bytes.len();
            let mut result: Vec<u8> = Vec::with_capacity(total_length);
            result.extend(&self.bytes);
            result.extend(&other.bytes);
            ByteSequenceOutput::new(result)
        }
    }

    fn concat(&mut self, other: &ByteSequenceOutput) {
        self.bytes.extend(&other.bytes);
    }

    fn subtract(&self, other: &ByteSequenceOutput) -> ByteSequenceOutput {
        if other.is_empty() {
            return self.clone();
        }
        debug_assert!(self.starts_with(other));

        if self.bytes.len() == other.bytes.len() {
            return ByteSequenceOutput::empty();
        }

        debug_assert!(other.bytes.len() < self.bytes.len());

        ByteSequenceOutput::new(self.bytes[other.bytes.len()..].to_vec())
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    #[inline]
    fn value(&self) -> Vec<u8> {
        self.bytes.clone()
    }
}

#[derive(Copy, Clone, Default)]
pub struct ByteSequenceOutputFactory {}

#[allow(dead_code)]
impl ByteSequenceOutputFactory {
    pub fn new() -> ByteSequenceOutputFactory {
        ByteSequenceOutputFactory {}
    }
}

impl OutputFactory for ByteSequenceOutputFactory {
    type Value = ByteSequenceOutput;

    fn empty(&self) -> Self::Value {
        ByteSequenceOutput::empty()
    }

    fn common(
        &self,
        o1: &<Self as OutputFactory>::Value,
        o2: &<Self as OutputFactory>::Value,
    ) -> <Self as OutputFactory>::Value {
        let mut res = Vec::new();
        for i in 0..min(o1.len(), o2.len()) {
            if o1.bytes[i] == o2.bytes[i] {
                res.push(o1.bytes[i]);
            } else {
                break;
            }
        }
        if res.is_empty() {
            self.empty()
        } else {
            ByteSequenceOutput::new(res)
        }
    }

    fn subtract(
        &self,
        o1: &<Self as OutputFactory>::Value,
        o2: &<Self as OutputFactory>::Value,
    ) -> <Self as OutputFactory>::Value {
        if o2.is_empty() {
            o1.clone()
        } else {
            debug_assert!(o1.starts_with(o2));
            if o1.len() == o2.len() {
                self.empty()
            } else {
                ByteSequenceOutput::new((&o1.bytes[o2.len()..]).to_vec())
            }
        }
    }

    fn add(
        &self,
        prefix: &<Self as OutputFactory>::Value,
        output: &<Self as OutputFactory>::Value,
    ) -> <Self as OutputFactory>::Value {
        if prefix.is_empty() {
            output.clone()
        } else if output.is_empty() {
            prefix.clone()
        } else {
            let mut result = vec![0u8; prefix.len() + output.len()];
            result[0..prefix.len()].copy_from_slice(&prefix.bytes);
            result[prefix.len()..].copy_from_slice(&output.bytes);
            ByteSequenceOutput::new(result)
        }
    }

    fn read<T: DataInput + ?Sized>(&self, data_in: &mut T) -> Result<ByteSequenceOutput> {
        let len = data_in.read_vint()?;
        if len != 0 {
            let len = len as usize;
            let mut buffer: Vec<u8> = vec![0u8; len];
            data_in.read_exact(&mut buffer)?;

            Ok(ByteSequenceOutput::new(buffer))
        } else {
            Ok(self.empty())
        }
    }

    fn write<T: DataOutput + ?Sized>(
        &self,
        output: &ByteSequenceOutput,
        data_out: &mut T,
    ) -> Result<()> {
        data_out.write_vint(output.bytes.len() as i32)?;
        data_out.write_bytes(&output.bytes, 0, output.bytes.len())
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    use core::util::fst::tests::*;

    #[test]
    fn test_prefix() {
        {
            let output1 = ByteSequenceOutput::new(vec![1, 2, 3, 4, 5]);
            let output2 = ByteSequenceOutput::new(vec![1, 2, 4, 5, 6]);
            let result = output1.prefix(&output2);
            assert_eq!(result.bytes, vec![1, 2]);
        }
        {
            let output1 = ByteSequenceOutput::new(vec![]);
            let output2 = ByteSequenceOutput::new(vec![1, 2, 4, 5, 6]);
            let result = output1.prefix(&output2);
            assert!(result.bytes.is_empty());
        }
    }

    #[test]
    fn test_cat() {
        {
            let output1 = ByteSequenceOutput::new(vec![1, 2, 3]);
            let output2 = ByteSequenceOutput::new(vec![4, 5]);
            let result = output1.cat(&output2);
            assert_eq!(result.bytes, vec![1, 2, 3, 4, 5]);
        }
        {
            let output1 = ByteSequenceOutput::new(vec![]);
            let output2 = ByteSequenceOutput::new(vec![4, 5]);
            let result = output1.cat(&output2);
            assert_eq!(result.bytes, vec![4, 5]);
        }
    }

    #[test]
    fn test_subtract() {
        {
            let output1 = ByteSequenceOutput::new(vec![1, 2, 3, 4, 5]);
            let output2 = ByteSequenceOutput::new(vec![1, 2]);
            let result = output1.subtract(&output2);
            assert_eq!(result.bytes, vec![3, 4, 5])
        }
        {
            let output1 = ByteSequenceOutput::new(vec![1, 2, 3, 4, 5]);
            let output2 = ByteSequenceOutput::new(vec![]);
            let result = output1.subtract(&output2);
            assert_eq!(result.bytes, vec![1, 2, 3, 4, 5])
        }
    }

    #[test]
    fn test_read_write() {
        let mut io = TestBufferedDataIO::default();
        let mut output = ByteSequenceOutput::new(vec![1, 2, 3, 4, 5]);
        let output_factory = ByteSequenceOutputFactory::new();
        output_factory.write(&output, &mut io).unwrap();
        assert_eq!(io.bytes, vec![5, 1, 2, 3, 4, 5]);

        output = output_factory.read(&mut io).unwrap();
        assert_eq!(output.bytes, vec![1, 2, 3, 4, 5]);
    }
}
