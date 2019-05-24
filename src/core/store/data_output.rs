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

use core::store::DataInput;
use core::util::bit_util::ZigZagEncoding;
use error::ErrorKind::IllegalArgument;
use error::Result;

use std::collections::{HashMap, HashSet};
use std::io::Write;

pub trait DataOutput: Write {
    fn write_byte(&mut self, b: u8) -> Result<()> {
        let buf = [b; 1];
        self.write_all(&buf)?;
        Ok(())
    }

    fn write_bytes(&mut self, b: &[u8], offset: usize, length: usize) -> Result<()> {
        let end = offset + length;
        if b.len() < end {
            bail!(IllegalArgument("b.len() < end".to_owned()));
        }
        let blob = &b[offset..end];
        self.write_all(blob)?;
        Ok(())
    }

    fn write_short(&mut self, i: i16) -> Result<()> {
        self.write_byte((i >> 8) as u8)?;
        self.write_byte(i as u8)
    }

    fn write_int(&mut self, i: i32) -> Result<()> {
        self.write_byte((i >> 24) as u8)?;
        self.write_byte((i >> 16) as u8)?;
        self.write_byte((i >> 8) as u8)?;
        self.write_byte(i as u8)
    }

    fn write_vint(&mut self, i: i32) -> Result<()> {
        let mut i = i as u32;
        while (i & !0x7f_u32) != 0 {
            self.write_byte(((i & 0x7f) | 0x80) as u8)?;
            i >>= 7;
        }
        self.write_byte(i as u8)
    }

    fn write_zint(&mut self, i: i32) -> Result<()> {
        self.write_vint(i.encode())
    }

    fn write_long(&mut self, i: i64) -> Result<()> {
        self.write_int((i >> 32) as i32)?;
        self.write_int(i as i32)
    }

    fn _write_signed_vlong(&mut self, i: i64) -> Result<()> {
        let mut i = i as u64;
        while (i & !0x7f_u64) != 0 {
            self.write_byte(((i & 0x7f_u64) | 0x80_u64) as u8)?;
            i >>= 7;
        }
        self.write_byte(i as u8)
    }

    fn write_vlong(&mut self, i: i64) -> Result<()> {
        if i < 0 {
            bail!(IllegalArgument("Can't write negative vLong".to_owned()));
        }
        self._write_signed_vlong(i)
    }

    fn write_zlong(&mut self, i: i64) -> Result<()> {
        self._write_signed_vlong(i.encode())
    }

    fn write_string(&mut self, s: &str) -> Result<()> {
        let s = s.as_bytes();
        self.write_vint(s.len() as i32)?;
        self.write_bytes(s, 0, s.len())?;
        Ok(())
    }

    fn write_map_of_strings(&mut self, map: &HashMap<String, String>) -> Result<()> {
        self.write_vint(map.len() as i32)?;

        let mut keys: Vec<&String> = map.keys().collect();
        keys.sort();
        for k in keys {
            self.write_string(k)?;
            self.write_string(map.get(k).unwrap())?;
        }
        Ok(())
    }

    fn write_set_of_strings(&mut self, set: &HashSet<String>) -> Result<()> {
        self.write_vint(set.len() as i32)?;

        let mut keys: Vec<&String> = set.iter().collect();
        keys.sort();
        for k in keys {
            self.write_string(k)?;
        }
        Ok(())
    }

    fn copy_bytes<I: DataInput + ?Sized>(&mut self, from: &mut I, len: usize) -> Result<()> {
        const COPY_BUFFER_SIZE: usize = 16384;
        let mut left = len as i64;
        let mut copy_buffer = [0u8; COPY_BUFFER_SIZE];
        while left > 0 {
            let to_copy = if left as usize > COPY_BUFFER_SIZE {
                COPY_BUFFER_SIZE
            } else {
                left as usize
            };
            from.read_bytes(&mut copy_buffer, 0, to_copy)?;
            self.write_bytes(&copy_buffer, 0, to_copy)?;
            left -= to_copy as i64;
        }
        Ok(())
    }
}

// a implement that can use Vec<u8> as a data output
impl DataOutput for Vec<u8> {}
