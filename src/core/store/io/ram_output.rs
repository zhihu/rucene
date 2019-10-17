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

use core::store::io::{ByteArrayDataOutput, DataOutput, IndexOutput};

use error::{ErrorKind, Result};

use std::io::{self, Write};

use flate2::Crc;

use core::util::fst::BytesStore;

const CHUNK_SIZE: usize = 8192;

/// A memory-resident `IndexOutput` implementation.
/// Use `BytesStore` to represent in memory output store
pub struct RAMOutputStream {
    name: String,
    pub store: BytesStore,
    crc: Option<Crc>,
}

impl RAMOutputStream {
    pub fn new(checksum: bool) -> Self {
        Self::with_chunk_size(CHUNK_SIZE, checksum)
    }

    pub fn from_store(store: BytesStore) -> Self {
        RAMOutputStream {
            name: "noname".into(),
            store,
            crc: None,
        }
    }

    pub fn with_chunk_size(chunk_size: usize, checksum: bool) -> Self {
        let store = BytesStore::with_block_bits(chunk_size.trailing_zeros() as usize);
        let crc = if checksum { Some(Crc::new()) } else { None };

        RAMOutputStream {
            name: "noname".into(),
            store,
            crc,
        }
    }

    pub fn write_to(&self, out: &mut impl DataOutput) -> Result<()> {
        // self.flush();
        self.store.write_to(out)
    }

    pub fn write_to_buf(&self, out: &mut [u8]) -> Result<()> {
        let length = out.len();
        let mut output = ByteArrayDataOutput::new(out, 0, length);
        self.write_to(&mut output)
    }

    pub fn reset(&mut self) {
        self.store.truncate(0);
        if let Some(ref mut crc) = self.crc {
            crc.reset();
        }
    }
}

impl Write for RAMOutputStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let size = self.store.write(buf)?;
        if size > 0 {
            if let Some(ref mut crc) = self.crc {
                crc.update(&buf[0..size]);
            }
        }
        Ok(size)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.store.flush()
    }
}

impl DataOutput for RAMOutputStream {}

impl IndexOutput for RAMOutputStream {
    fn name(&self) -> &str {
        &self.name
    }

    fn file_pointer(&self) -> i64 {
        self.store.get_position() as i64
    }

    fn checksum(&self) -> Result<i64> {
        if let Some(ref crc) = self.crc {
            Ok((crc.sum() as i64) & 0xffff_ffffi64)
        } else {
            bail!(ErrorKind::IllegalState(
                "internal RAMOutputStream created with checksum disabled".into()
            ))
        }
    }
}
