use std::io;

use core::store::{DataInput, DataOutput};
use error::*;

// pub mod builder;
pub mod bytes_output;
pub mod bytes_store;
pub mod fst_reader;

pub trait Output: Clone {
    type Value;

    fn prefix(&self, other: &Self) -> Self;

    fn cat(&self, other: &Self) -> Self;

    fn subtract(&self, other: &Self) -> Self;

    fn is_empty(&self) -> bool;

    fn value(&self) -> Self::Value;
}

pub trait OutputFactory {
    type Value: Output;

    /// Return an empty Output
    ///
    fn empty(&self) -> Self::Value;

    /// Decode an output value previously written with `write`
    ///
    fn read<T: DataInput + ?Sized>(&self, data_in: &mut T) -> Result<Self::Value>;

    /// Encode an output value into a `DataOutput`
    ///
    fn write<T: DataOutput + ?Sized>(&self, output: &Self::Value, data_out: &mut T) -> Result<()>;

    /// Encode an final node output value into a `DataOutput`.
    /// By default this just calls `write`.
    ///
    fn write_final_output<T: DataOutput + ?Sized>(
        &self,
        output: &Self::Value,
        data_out: &mut T,
    ) -> Result<()> {
        self.write(output, data_out)
    }

    /// Decode an output value previously written with `write_final_output`.
    /// By default this just calls `read`.
    ///
    fn read_final_output<T: DataInput + ?Sized>(&self, data_in: &mut T) -> Result<Self::Value> {
        self.read(data_in)
    }

    /// Skip the output previously written with `write_final_output`,
    /// defaults to just calling `read_final_output` and discarding the result.
    ///
    fn skip_final_output<T: DataInput + ?Sized>(&self, data_in: &mut T) -> Result<()> {
        self.skip_output(data_in)
    }

    /// Skip the output; defaults to just calling `read`
    /// and discarding the result.
    ///
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

pub struct DirectionalBytesReader<'a> {
    bytes: &'a [u8],
    pos: usize,
    pub reversed: bool,
}

impl<'a> DirectionalBytesReader<'a> {
    pub fn new(bytes: &'a [u8], reversed: bool) -> DirectionalBytesReader {
        DirectionalBytesReader {
            bytes,
            pos: 0,
            reversed,
        }
    }
}

impl<'a> BytesReader for DirectionalBytesReader<'a> {
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

impl<'a> io::Read for DirectionalBytesReader<'a> {
    fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
        let mut len = b.len();
        if self.reversed {
            if len > self.pos {
                len = self.pos
            }
            for v in b.iter_mut().take(len) {
                *v = self.bytes[self.pos];
                self.pos -= 1;
            }
        } else {
            let available = self.bytes.len() - self.pos;
            if available < len {
                len = available;
            }
            b[..len].clone_from_slice(&self.bytes[self.pos..len]);

            self.pos += len;
        }

        Ok(len)
    }
}

impl<'a> DataInput for DirectionalBytesReader<'a> {
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
            let left = b.len();

            for b in b.iter_mut().take(left) {
                if let Ok(byte) = self.read_byte() {
                    *b = byte;
                }
            }

            Ok(b.len() - left)
        }
    }
}

pub use self::bytes_output::{ByteSequenceOutput, ByteSequenceOutputFactory};
pub use self::fst_reader::{Arc, FST};
