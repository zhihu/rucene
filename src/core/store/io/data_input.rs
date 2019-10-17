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

use core::util::ZigZagEncoding;
use error::ErrorKind::*;
use error::Result;

use std::collections::{HashMap, HashSet};
use std::io::{self, Read};
use std::mem;

pub trait DataInput: Read {
    fn read_byte(&mut self) -> Result<u8> {
        let mut buffer = [0u8; 1];
        if self.read(&mut buffer)? != 1 {
            bail!(UnexpectedEOF(
                "Reached EOF when a single byte is expected".to_owned()
            ))
        } else {
            Ok(buffer[0])
        }
    }

    fn read_bytes(&mut self, b: &mut [u8], offset: usize, length: usize) -> Result<()> {
        let end = offset + length;
        if b.len() < end {
            let msg = format!(
                "Buffer too small: wring [{}, {}) to [0, {})",
                offset,
                end,
                b.len(),
            );
            bail!(IllegalArgument(msg));
        }

        let mut blob = &mut b[offset..end];

        if self.read(&mut blob)? != length {
            bail!(UnexpectedEOF(format!(
                "Reached EOF when {} bytes are expected",
                length
            )))
        } else {
            Ok(())
        }
    }

    fn read_short(&mut self) -> Result<i16> {
        let mut r = 0i16;
        let bytes: &mut [u8] = unsafe {
            let p = &mut r as *mut i16 as *mut u8;
            std::slice::from_raw_parts_mut(p, mem::size_of::<i16>())
        };
        self.read_exact(bytes)?;
        Ok(r.to_be())
    }

    fn read_int(&mut self) -> Result<i32> {
        let mut r = 0i32;
        let bytes: &mut [u8] = unsafe {
            let p = &mut r as *mut i32 as *mut u8;
            std::slice::from_raw_parts_mut(p, mem::size_of::<i32>())
        };
        self.read_exact(bytes)?;
        Ok(r.to_be())
    }

    fn read_vint(&mut self) -> Result<i32> {
        let mut b = (self.read_byte()?) as i8;
        if b >= 0 {
            return Ok(i32::from(b));
        }

        let mut i = i32::from(b) & 0x7f;
        b = self.read_byte()? as i8;
        i |= (i32::from(b) & 0x7f) << 7;
        if b >= 0 {
            return Ok(i);
        }

        b = self.read_byte()? as i8;
        i |= (i32::from(b) & 0x7f) << 14;
        if b >= 0 {
            return Ok(i);
        }

        b = self.read_byte()? as i8;
        i |= (i32::from(b) & 0x7f) << 21;
        if b >= 0 {
            return Ok(i);
        }

        b = self.read_byte()? as i8;
        i |= (i32::from(b) & 0x0f) << 28;

        if (b as u8 & 0xf0) != 0 {
            bail!(IllegalState("Invalid vInt detected".to_owned()));
        }

        Ok(i)
    }

    fn read_zint(&mut self) -> Result<i32> {
        Ok(self.read_vint()?.decode())
    }

    fn read_long(&mut self) -> Result<i64> {
        let mut r = 0i64;
        let bytes: &mut [u8] = unsafe {
            let p: *mut u8 = &mut r as *mut i64 as *mut u8;
            std::slice::from_raw_parts_mut(p, mem::size_of::<i64>())
        };
        self.read_exact(bytes)?;
        Ok(r.to_be())
    }

    fn read_vlong(&mut self) -> Result<i64> {
        self.read_vlong_ex(false)
    }

    fn read_vlong_ex(&mut self, negative_allowed: bool) -> Result<i64> {
        const ERR_MSG: &str = "Invalid vLong detected";

        let mut b = self.read_byte()? as i8;
        if b >= 0 {
            return Ok(i64::from(b));
        }

        let mut i = i64::from(b) & 0x7f_i64;

        b = self.read_byte()? as i8;
        i |= (i64::from(b) & 0x7f_i64) << 7;
        if b >= 0 {
            return Ok(i);
        }

        b = self.read_byte()? as i8;
        i |= (i64::from(b) & 0x7f_i64) << 14;
        if b >= 0 {
            return Ok(i);
        }

        b = self.read_byte()? as i8;
        i |= (i64::from(b) & 0x7f_i64) << 21;
        if b >= 0 {
            return Ok(i);
        }

        b = self.read_byte()? as i8;
        i |= (i64::from(b) & 0x7f_i64) << 28;
        if b >= 0 {
            return Ok(i);
        }

        b = self.read_byte()? as i8;
        i |= (i64::from(b) & 0x7f_i64) << 35;
        if b >= 0 {
            return Ok(i);
        }

        b = self.read_byte()? as i8;
        i |= (i64::from(b) & 0x7f_i64) << 42;
        if b >= 0 {
            return Ok(i);
        }

        b = self.read_byte()? as i8;
        i |= (i64::from(b) & 0x7f_i64) << 49;
        if b >= 0 {
            return Ok(i);
        }

        b = self.read_byte()? as i8;
        i |= (i64::from(b) & 0x7f_i64) << 56;
        if b >= 0 {
            return Ok(i);
        }

        if negative_allowed {
            b = self.read_byte()? as i8;
            if b == 0 || b == 1 {
                i |= (i64::from(b) & 0x7f_i64) << 63;
                return Ok(i);
            } else {
                bail!(IllegalState(ERR_MSG.to_owned()));
            }
        }
        bail!(IllegalState(ERR_MSG.to_owned()))
    }

    fn read_zlong(&mut self) -> Result<i64> {
        Ok(self.read_vlong_ex(true)?.decode())
    }

    fn read_string(&mut self) -> Result<String> {
        const ERR_MSG: &str = "Invalid String detected";
        let length = self.read_vint()?;
        if length < 0 {
            bail!(IllegalState(ERR_MSG.to_owned()));
        }

        let length = length as usize;

        let mut buffer = Vec::with_capacity(length);

        unsafe {
            buffer.set_len(length);
        };

        self.read_exact(&mut buffer)?;
        Ok(String::from_utf8(buffer)?)
    }

    fn read_map_of_strings(&mut self) -> Result<HashMap<String, String>> {
        let count = self.read_vint()?;
        if count < 0 {
            bail!(IllegalState("Invalid StringMap detected".to_owned()));
        }

        let mut map = HashMap::new();
        if count != 0 {
            for _ in 0..count {
                let k = self.read_string()?;
                let v = self.read_string()?;
                map.insert(k, v);
            }
        }

        Ok(map)
    }

    fn read_set_of_strings(&mut self) -> Result<HashSet<String>> {
        let count = self.read_vint()?;
        if count < 0 {
            bail!(IllegalState("Invalid StringSet detected".to_owned()));
        }

        let mut hash_set = HashSet::new();
        if count != 0 {
            for _ in 0..count {
                hash_set.insert(self.read_string()?);
            }
        }

        Ok(hash_set)
    }

    fn skip_bytes(&mut self, count: usize) -> Result<()> {
        const SKIP_BUFFER_SIZE: usize = 1024;
        let mut skip_buffer = [0u8; SKIP_BUFFER_SIZE];
        let mut skipped = 0;

        while skipped < count {
            let step = ::std::cmp::min(SKIP_BUFFER_SIZE, count - skipped);
            self.read_bytes(&mut skip_buffer, 0, step)?;
            skipped += step;
        }
        Ok(())
    }
}

impl<'a> DataInput for &'a [u8] {
    fn read_byte(&mut self) -> Result<u8> {
        if self.len() < 1 {
            bail!(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "failed to fill whole buffer"
            ));
        }
        let b = self[0];
        *self = &self[1..];
        Ok(b)
    }

    fn skip_bytes(&mut self, count: usize) -> Result<()> {
        if self.len() < count {
            bail!(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "failed to fill whole buffer"
            ));
        }

        *self = &self[count..];
        Ok(())
    }
}
