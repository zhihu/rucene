use core::codec::lucene54::CompressedBinaryTermIterator;
use core::codec::lucene54::{BinaryEntry, ReverseTermsIndexRef};
use core::index::term::SeekStatus;
use core::index::term::TermIterator;
use core::store::IndexInput;
use core::util::packed::MonotonicBlockPackedReaderRef;
use core::util::DocId;
use core::util::LongValues;
use error::Result;

use std::ops::Deref;
use std::sync::{Arc, Mutex};

pub trait BinaryDocValues: Send {
    fn get(&mut self, doc_id: DocId) -> Result<&[u8]>;
}

pub type BinaryDocValuesRef = Arc<Mutex<Box<BinaryDocValues>>>;

pub trait LongBinaryDocValues: BinaryDocValues {
    fn get64(&mut self, doc_id: i64) -> Result<&[u8]>;
}

pub struct FixedBinaryDocValues {
    data: Mutex<Box<IndexInput>>,
    buffer: Vec<u8>,
}

impl FixedBinaryDocValues {
    pub fn new(data: Box<IndexInput>, buffer_len: usize) -> Self {
        FixedBinaryDocValues {
            data: Mutex::new(data),
            buffer: vec![0u8; buffer_len],
        }
    }
}

impl LongBinaryDocValues for FixedBinaryDocValues {
    fn get64(&mut self, id: i64) -> Result<&[u8]> {
        let length = self.buffer.len();
        let mut data = self.data.lock()?;
        data.seek(id * length as i64)?;
        data.read_bytes(&mut self.buffer, 0, length)?;
        Ok(&self.buffer)
    }
}

impl BinaryDocValues for FixedBinaryDocValues {
    fn get(&mut self, doc_id: DocId) -> Result<&[u8]> {
        FixedBinaryDocValues::get64(self, i64::from(doc_id))
    }
}

pub struct VariableBinaryDocValues {
    addresses: Box<LongValues>,
    data: Mutex<Box<IndexInput>>,
    buffer: Vec<u8>,
}

impl VariableBinaryDocValues {
    pub fn new(addresses: Box<LongValues>, data: Box<IndexInput>, length: usize) -> Self {
        let buffer = vec![0u8; length];
        VariableBinaryDocValues {
            addresses,
            data: Mutex::new(data),
            buffer,
        }
    }
}

impl LongBinaryDocValues for VariableBinaryDocValues {
    fn get64(&mut self, id: i64) -> Result<&[u8]> {
        let start_address = self.addresses.get64(id)?;
        let end_address = self.addresses.get64(id + 1)?;
        let length = (end_address - start_address) as usize;
        let mut data = self.data.lock()?;
        data.seek(start_address)?;
        data.read_bytes(&mut self.buffer, 0, length)?;
        Ok(&self.buffer[0..length])
    }
}

impl BinaryDocValues for VariableBinaryDocValues {
    fn get(&mut self, doc_id: DocId) -> Result<&[u8]> {
        VariableBinaryDocValues::get64(self, i64::from(doc_id))
    }
}

pub struct CompressedBinaryDocValues {
    num_values: i64,
    num_index_values: i64,
    num_reverse_index_values: i64,
    max_term_length: i32,
    data: Mutex<Box<IndexInput>>,
    term_iterator: CompressedBinaryTermIterator,
    term_buffer: Vec<u8>,
    reverse_index: ReverseTermsIndexRef,
    addresses: MonotonicBlockPackedReaderRef,
}

impl CompressedBinaryDocValues {
    pub fn new(
        bytes: &BinaryEntry,
        addresses: MonotonicBlockPackedReaderRef,
        reverse_index: ReverseTermsIndexRef,
        data: Mutex<Box<IndexInput>>,
    ) -> Result<CompressedBinaryDocValues> {
        let max_term_length = bytes.max_length;
        let num_reverse_index_values = reverse_index.lock()?.term_addresses.size() as i64;
        let num_values = bytes.count;
        let num_index_values = addresses.lock()?.size() as i64;

        let data_copy = IndexInput::clone(data.lock()?.as_ref())?;
        let term_iterator = CompressedBinaryTermIterator::new(
            data_copy,
            max_term_length as usize,
            num_reverse_index_values,
            Arc::clone(&reverse_index),
            Arc::clone(&addresses),
            num_values,
            num_index_values,
        )?;

        let dv = CompressedBinaryDocValues {
            num_values,
            num_index_values,
            num_reverse_index_values,
            max_term_length,
            data,
            term_iterator,
            term_buffer: Vec::new(),
            reverse_index,
            addresses,
        };
        Ok(dv)
    }

    pub fn lookup_term(&mut self, key: &[u8]) -> Result<i64> {
        match self.term_iterator.seek_ceil(key)? {
            SeekStatus::Found => self.term_iterator.ord(),
            SeekStatus::NotFound => {
                let val = -self.term_iterator.ord()? - 1;
                Ok(val)
            }
            _ => Ok(-self.num_values - 1),
        }
    }

    pub fn get_term_iterator(&self) -> Result<CompressedBinaryTermIterator> {
        let data = IndexInput::clone(self.data.lock()?.deref().as_ref())?;
        CompressedBinaryTermIterator::new(
            data,
            self.max_term_length as usize,
            self.num_reverse_index_values,
            Arc::clone(&self.reverse_index),
            Arc::clone(&self.addresses),
            self.num_values,
            self.num_index_values,
        )
    }
}

impl LongBinaryDocValues for CompressedBinaryDocValues {
    fn get64(&mut self, id: i64) -> Result<&[u8]> {
        self.term_iterator.seek_exact_ord(id)?;
        self.term_buffer = self.term_iterator.term()?;
        Ok(self.term_buffer.as_ref())
    }
}

impl BinaryDocValues for CompressedBinaryDocValues {
    fn get(&mut self, doc_id: DocId) -> Result<&[u8]> {
        CompressedBinaryDocValues::get64(self, i64::from(doc_id))
    }
}

pub enum BoxedBinaryDocValuesEnum {
    General(Box<LongBinaryDocValues>),
    Compressed(Box<CompressedBinaryDocValues>),
}
