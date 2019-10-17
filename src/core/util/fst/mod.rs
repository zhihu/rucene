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

mod bytes_output;

pub use self::bytes_output::*;

mod bytes_store;

pub use self::bytes_store::*;

mod fst_builder;

pub use self::fst_builder::*;

mod fst_iteartor;

pub use self::fst_iteartor::*;

mod fst_reader;

pub use self::fst_reader::*;

use std::fmt::Debug;
use std::hash::Hash;
use std::io;

use core::store::io::{DataInput, DataOutput};
use error::Result;

pub trait Output: Clone + Eq + Hash + Debug {
    type Value;

    fn prefix(&self, other: &Self) -> Self;

    fn cat(&self, other: &Self) -> Self;

    fn concat(&mut self, other: &Self);

    fn subtract(&self, other: &Self) -> Self;

    fn is_empty(&self) -> bool;

    fn value(&self) -> Self::Value;
}

pub trait OutputFactory: Clone {
    type Value: Output;

    /// Return an empty Output
    fn empty(&self) -> Self::Value;

    fn common(&self, o1: &Self::Value, o2: &Self::Value) -> Self::Value;

    fn subtract(&self, o1: &Self::Value, o2: &Self::Value) -> Self::Value;

    fn add(&self, prefix: &Self::Value, output: &Self::Value) -> Self::Value;

    fn merge(&self, _o1: &Self::Value, _o2: &Self::Value) -> Self::Value {
        panic!("UnsupportedOperation");
    }

    /// Decode an output value previously written with `write`
    fn read<T: DataInput + ?Sized>(&self, data_in: &mut T) -> Result<Self::Value>;

    /// Encode an output value into a `DataOutput`
    fn write<T: DataOutput + ?Sized>(&self, output: &Self::Value, data_out: &mut T) -> Result<()>;

    /// Encode an final node output value into a `DataOutput`.
    /// By default this just calls `write`.
    fn write_final_output<T: DataOutput + ?Sized>(
        &self,
        output: &Self::Value,
        data_out: &mut T,
    ) -> Result<()> {
        self.write(output, data_out)
    }

    /// Decode an output value previously written with `write_final_output`.
    /// By default this just calls `read`.
    fn read_final_output<T: DataInput + ?Sized>(&self, data_in: &mut T) -> Result<Self::Value> {
        self.read(data_in)
    }

    /// Skip the output previously written with `write_final_output`,
    /// defaults to just calling `read_final_output` and discarding the result.
    fn skip_final_output<T: DataInput + ?Sized>(&self, data_in: &mut T) -> Result<()> {
        self.skip_output(data_in)
    }

    /// Skip the output; defaults to just calling `read`
    /// and discarding the result.
    fn skip_output<T: DataInput + ?Sized>(&self, data_in: &mut T) -> Result<()> {
        self.read(data_in).map(|_| ())
    }
}

pub trait BytesReader: DataInput {
    /// Get current read position
    fn position(&self) -> usize;

    /// Set current read position.
    fn set_position(&mut self, pos: usize);

    /// Returns true if this reader uses reversed bytes
    /// under-the-hood.
    fn reversed(&self) -> bool;
}

pub struct DirectionalBytesReader {
    bytes: *const [u8],
    pos: usize,
    pub reversed: bool,
}

impl<'a> DirectionalBytesReader {
    pub fn new(bytes: &[u8], reversed: bool) -> DirectionalBytesReader {
        DirectionalBytesReader {
            bytes: bytes as *const [u8],
            pos: 0,
            reversed,
        }
    }

    fn bytes_slice(&self) -> &[u8] {
        unsafe { &(*self.bytes) }
    }
}

impl BytesReader for DirectionalBytesReader {
    fn position(&self) -> usize {
        self.pos
    }

    fn set_position(&mut self, pos: usize) {
        self.pos = pos
    }

    fn reversed(&self) -> bool {
        self.reversed
    }
}

impl io::Read for DirectionalBytesReader {
    fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
        let mut len = b.len();
        if self.reversed {
            if len > self.pos {
                len = self.pos
            }
            for v in b.iter_mut().take(len) {
                *v = self.bytes_slice()[self.pos];
                self.pos -= 1;
            }
        } else {
            let available = self.bytes_slice().len() - self.pos;
            if available < len {
                len = available;
            }
            b[..len].copy_from_slice(&self.bytes_slice()[self.pos..len]);

            self.pos += len;
        }

        Ok(len)
    }
}

impl DataInput for DirectionalBytesReader {
    fn skip_bytes(&mut self, count: usize) -> Result<()> {
        if self.reversed {
            self.pos -= count;
        } else {
            self.pos += count;
        }

        Ok(())
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use std::io;
    use std::io::{Read, Write};

    #[derive(Default)]
    pub struct TestBufferedDataIO {
        pub bytes: Vec<u8>,
        read: usize,
    }

    impl DataOutput for TestBufferedDataIO {
        fn write_byte(&mut self, v: u8) -> Result<()> {
            self.bytes.push(v);

            Ok(())
        }

        fn write_bytes(&mut self, b: &[u8], offset: usize, len: usize) -> Result<()> {
            self.bytes.extend_from_slice(&b[offset..(offset + len)]);
            Ok(())
        }
    }

    impl Write for TestBufferedDataIO {
        fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
            unimplemented!()
        }

        fn flush(&mut self) -> io::Result<()> {
            unimplemented!()
        }
    }

    impl DataInput for TestBufferedDataIO {
        fn read_byte(&mut self) -> Result<u8> {
            let v = self.bytes[self.read];
            self.read += 1;

            Ok(v)
        }

        fn read_bytes(&mut self, b: &mut [u8], offset: usize, len: usize) -> Result<()> {
            let mut to = offset;
            let stop = offset + len;

            while to < stop {
                b[to] = self.read_byte()?;
                to += 1;
            }

            Ok(())
        }
    }

    impl Read for TestBufferedDataIO {
        fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
            let len = b.len().min(self.bytes.len() - self.read);
            if len > 0 {
                b[..len].copy_from_slice(&self.bytes[self.read..self.read + len]);
                self.read += len;
            }
            Ok(len)
        }
    }
}
