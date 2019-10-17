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

use core::store::io::DataOutput;

use std::io::Write;

const MIN_UTF8_SIZE_TO_ENABLE_DOUBLE_PASS_ENCODING: usize = 65536;

/// a `IndexOutput` that can be used to build a bytes array.
pub struct GrowableByteArrayDataOutput {
    pub bytes: Vec<u8>,
    length: usize,
    /* scratch for utf8 encoding of small strings
     * _scratch_bytes: Vec<u8>, */
}

impl GrowableByteArrayDataOutput {
    pub fn new(cp: usize) -> GrowableByteArrayDataOutput {
        GrowableByteArrayDataOutput {
            bytes: vec![0u8; cp + MIN_UTF8_SIZE_TO_ENABLE_DOUBLE_PASS_ENCODING],
            length: 0,
            //_scratch_bytes: vec![0; cp + MIN_UTF8_SIZE_TO_ENABLE_DOUBLE_PASS_ENCODING],
        }
    }

    pub fn position(&self) -> usize {
        self.length
    }

    pub fn reset(&mut self) {
        self.length = 0;
    }
}

impl Write for GrowableByteArrayDataOutput {
    fn write(&mut self, buf: &[u8]) -> ::std::io::Result<usize> {
        let buf_len = buf.len();
        let new_len = self.length + buf_len;
        if self.bytes.len() < new_len {
            self.bytes.resize(new_len, 0u8);
        }
        self.bytes[self.length..new_len].copy_from_slice(buf);
        self.length += buf_len;
        Ok(buf_len)
    }

    fn flush(&mut self) -> ::std::io::Result<()> {
        Ok(())
    }
}

impl DataOutput for GrowableByteArrayDataOutput {}
