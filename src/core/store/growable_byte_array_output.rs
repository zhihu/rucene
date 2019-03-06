use core::store::DataOutput;
use std::io::Write;

const MIN_UTF8_SIZE_TO_ENABLE_DOUBLE_PASS_ENCODING: usize = 65536;

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
