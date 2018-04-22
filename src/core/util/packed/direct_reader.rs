use core::index::NumericDocValues;
use core::store::RandomAccessInput;
use core::util::LongValues;
use error::ErrorKind::IllegalArgument;
use error::ErrorKind::RuntimeError;
use error::Result;

use core::util::DocId;
use std::sync::Arc;

pub struct DirectReader;
impl DirectReader {
    pub fn get_instance(
        slice: Arc<Box<RandomAccessInput>>,
        bits_per_value: i32,
        offset: i64,
    ) -> Result<Box<LongValues>> {
        let reader: Box<LongValues> = match bits_per_value {
            1 => Box::new(DirectPackedReader1::new(Arc::clone(&slice), offset)),
            2 => Box::new(DirectPackedReader2::new(slice, offset)),
            4 => Box::new(DirectPackedReader4::new(slice, offset)),
            8 => Box::new(DirectPackedReader8::new(slice, offset)),
            12 => Box::new(DirectPackedReader12::new(slice, offset)),
            16 => Box::new(DirectPackedReader16::new(slice, offset)),
            20 => Box::new(DirectPackedReader20::new(slice, offset)),
            24 => Box::new(DirectPackedReader24::new(slice, offset)),
            28 => Box::new(DirectPackedReader28::new(slice, offset)),
            32 => Box::new(DirectPackedReader32::new(slice, offset)),
            40 => Box::new(DirectPackedReader40::new(slice, offset)),
            48 => Box::new(DirectPackedReader48::new(slice, offset)),
            56 => Box::new(DirectPackedReader56::new(slice, offset)),
            64 => Box::new(DirectPackedReader64::new(slice, offset)),
            _ => {
                bail!(IllegalArgument(format!(
                    "unsupported bits_per_value: {}",
                    bits_per_value
                )));
            }
        };
        Ok(reader)
    }
}

// ================ Begin Reader 1 ================
struct DirectPackedReader1 {
    random_access_input: Arc<Box<RandomAccessInput>>,
    offset: i64,
}

impl DirectPackedReader1 {
    fn new(random_access_input: Arc<Box<RandomAccessInput>>, offset: i64) -> DirectPackedReader1 {
        DirectPackedReader1 {
            random_access_input,
            offset,
        }
    }
}

impl LongValues for DirectPackedReader1 {
    fn get64(&mut self, index: i64) -> Result<i64> {
        if index < 0 {
            bail!(IllegalArgument(format!(
                "negative index encountered: {}",
                index
            )));
        }

        let shift = 7 - (index as i32 & 0x7);
        let byte_dance = self.random_access_input
            .read_byte(self.offset + (index >> 3))?;

        Ok(i64::from((byte_dance >> shift) & 0x1))
    }
}

impl NumericDocValues for DirectPackedReader1 {
    fn get(&mut self, doc_id: DocId) -> Result<i64> {
        LongValues::get64(self, i64::from(doc_id))
    }
}

// ================ Begin Reader 2 ================
struct DirectPackedReader2 {
    random_access_input: Arc<Box<RandomAccessInput>>,
    offset: i64,
}

impl DirectPackedReader2 {
    fn new(random_access_input: Arc<Box<RandomAccessInput>>, offset: i64) -> DirectPackedReader2 {
        DirectPackedReader2 {
            random_access_input,
            offset,
        }
    }
}

impl LongValues for DirectPackedReader2 {
    fn get64(&mut self, index: i64) -> Result<i64> {
        if index < 0 {
            bail!(IllegalArgument(format!(
                "negative index encountered: {}",
                index
            )));
        }

        let shift = (3 - (index as i32 & 0x3)) << 1;

        let byte_dance = match self.random_access_input
            .read_byte(self.offset + (index >> 2))
        {
            Ok(byte_dance) => byte_dance,
            Err(ref e) => bail!(RuntimeError(format!("{:?}", e))),
        };

        Ok(i64::from((byte_dance >> shift) & 0x3))
    }
}

impl NumericDocValues for DirectPackedReader2 {
    fn get(&mut self, doc_id: DocId) -> Result<i64> {
        LongValues::get64(self, i64::from(doc_id))
    }
}

// ================ Begin Reader 4 ================
struct DirectPackedReader4 {
    random_access_input: Arc<Box<RandomAccessInput>>,
    offset: i64,
}

impl DirectPackedReader4 {
    fn new(random_access_input: Arc<Box<RandomAccessInput>>, offset: i64) -> DirectPackedReader4 {
        DirectPackedReader4 {
            random_access_input,
            offset,
        }
    }
}

impl LongValues for DirectPackedReader4 {
    fn get64(&mut self, index: i64) -> Result<i64> {
        if index < 0 {
            bail!(IllegalArgument(format!(
                "negative index encountered: {}",
                index
            )));
        }

        let shift = (((index + 1) & 0x1) as i32) << 2;

        let byte_dance = match self.random_access_input
            .read_byte(self.offset + (index >> 1))
        {
            Ok(byte_dance) => byte_dance,
            Err(ref e) => bail!(RuntimeError(format!("{:?}", e))),
        };

        Ok(i64::from((byte_dance >> shift) & 0xF))
    }
}

impl NumericDocValues for DirectPackedReader4 {
    fn get(&mut self, doc_id: DocId) -> Result<i64> {
        LongValues::get64(self, i64::from(doc_id))
    }
}

// ================ Begin Reader 8 ================
struct DirectPackedReader8 {
    random_access_input: Arc<Box<RandomAccessInput>>,
    offset: i64,
}

impl DirectPackedReader8 {
    fn new(random_access_input: Arc<Box<RandomAccessInput>>, offset: i64) -> DirectPackedReader8 {
        DirectPackedReader8 {
            random_access_input,
            offset,
        }
    }
}

impl LongValues for DirectPackedReader8 {
    fn get64(&mut self, index: i64) -> Result<i64> {
        if index < 0 {
            bail!(IllegalArgument(format!(
                "negative index encountered: {}",
                index
            )));
        }

        let byte_dance = match self.random_access_input.read_byte(self.offset + index) {
            Ok(byte_dance) => byte_dance,
            Err(ref e) => bail!(RuntimeError(format!("{:?}", e))),
        };

        Ok(i64::from(byte_dance))
    }
}

impl NumericDocValues for DirectPackedReader8 {
    fn get(&mut self, doc_id: DocId) -> Result<i64> {
        LongValues::get64(self, i64::from(doc_id))
    }
}

// ================ Begin Reader 12 ================
struct DirectPackedReader12 {
    random_access_input: Arc<Box<RandomAccessInput>>,
    offset: i64,
}

impl DirectPackedReader12 {
    fn new(random_access_input: Arc<Box<RandomAccessInput>>, offset: i64) -> DirectPackedReader12 {
        DirectPackedReader12 {
            random_access_input,
            offset,
        }
    }
}

impl LongValues for DirectPackedReader12 {
    fn get64(&mut self, index: i64) -> Result<i64> {
        if index < 0 {
            bail!(IllegalArgument(format!(
                "negative index encountered: {}",
                index
            )));
        }

        let offset = (index * 3) >> 1;
        let shift = ((index + 1) & 0x1) << 2;

        let word = match self.random_access_input.read_short(self.offset + offset) {
            Ok(w) => w as u16,
            Err(ref e) => bail!(RuntimeError(format!("{:?}", e))),
        };

        Ok(i64::from((word >> shift) & 0xFFF))
    }
}

impl NumericDocValues for DirectPackedReader12 {
    fn get(&mut self, doc_id: DocId) -> Result<i64> {
        LongValues::get64(self, i64::from(doc_id))
    }
}

// ================ Begin Reader 16 ================
struct DirectPackedReader16 {
    random_access_input: Arc<Box<RandomAccessInput>>,
    offset: i64,
}

impl DirectPackedReader16 {
    fn new(random_access_input: Arc<Box<RandomAccessInput>>, offset: i64) -> DirectPackedReader16 {
        DirectPackedReader16 {
            random_access_input,
            offset,
        }
    }
}

impl LongValues for DirectPackedReader16 {
    fn get64(&mut self, index: i64) -> Result<i64> {
        if index < 0 {
            bail!(IllegalArgument(format!(
                "negative index encountered: {}",
                index
            )));
        }

        let word = match self.random_access_input
            .read_short(self.offset + (index << 1))
        {
            Ok(w) => w as u16,
            Err(ref e) => bail!(RuntimeError(format!("{:?}", e))),
        };

        Ok(i64::from(word))
    }
}

impl NumericDocValues for DirectPackedReader16 {
    fn get(&mut self, doc_id: DocId) -> Result<i64> {
        LongValues::get64(self, i64::from(doc_id))
    }
}

// ================ Begin Reader 20 ================
struct DirectPackedReader20 {
    random_access_input: Arc<Box<RandomAccessInput>>,
    offset: i64,
}

impl DirectPackedReader20 {
    fn new(random_access_input: Arc<Box<RandomAccessInput>>, offset: i64) -> DirectPackedReader20 {
        DirectPackedReader20 {
            random_access_input,
            offset,
        }
    }
}

impl LongValues for DirectPackedReader20 {
    fn get64(&mut self, index: i64) -> Result<i64> {
        if index < 0 {
            bail!(IllegalArgument(format!(
                "negative index encountered: {}",
                index
            )));
        }

        let offset = (index * 5) >> 1;

        let dword = match self.random_access_input.read_int(self.offset + offset) {
            Ok(dw) => (dw as u32) >> 8,
            Err(ref e) => bail!(RuntimeError(format!("{:?}", e))),
        };

        let shift = ((index + 1) & 0x1) << 2;

        Ok(i64::from((dword >> shift) & 0xFFFFF))
    }
}

impl NumericDocValues for DirectPackedReader20 {
    fn get(&mut self, doc_id: DocId) -> Result<i64> {
        LongValues::get64(self, i64::from(doc_id))
    }
}

// ================ Begin Reader 24 ================
struct DirectPackedReader24 {
    random_access_input: Arc<Box<RandomAccessInput>>,
    offset: i64,
}

impl DirectPackedReader24 {
    fn new(random_access_input: Arc<Box<RandomAccessInput>>, offset: i64) -> DirectPackedReader24 {
        DirectPackedReader24 {
            random_access_input,
            offset,
        }
    }
}

impl LongValues for DirectPackedReader24 {
    fn get64(&mut self, index: i64) -> Result<i64> {
        if index < 0 {
            bail!(IllegalArgument(format!(
                "negative index encountered: {}",
                index
            )));
        }

        match self.random_access_input.read_int(self.offset + 3 * index) {
            Ok(v) => Ok(i64::from(v as u32 >> 8)),
            Err(ref e) => bail!(RuntimeError(format!("{:?}", e))),
        }
    }
}

impl NumericDocValues for DirectPackedReader24 {
    fn get(&mut self, doc_id: DocId) -> Result<i64> {
        LongValues::get64(self, i64::from(doc_id))
    }
}

// ================ Begin Reader 28 ================
struct DirectPackedReader28 {
    random_access_input: Arc<Box<RandomAccessInput>>,
    offset: i64,
}

impl DirectPackedReader28 {
    fn new(random_access_input: Arc<Box<RandomAccessInput>>, offset: i64) -> DirectPackedReader28 {
        DirectPackedReader28 {
            random_access_input,
            offset,
        }
    }
}

impl LongValues for DirectPackedReader28 {
    fn get64(&mut self, index: i64) -> Result<i64> {
        if index < 0 {
            bail!(IllegalArgument(format!(
                "negative index encountered: {}",
                index
            )));
        }

        let offset = (index * 7) >> 1;
        let shift = ((index + 1) & 0x1) << 2;
        match self.random_access_input.read_int(self.offset + offset) {
            Ok(v) => Ok(i64::from((v as u32 >> shift) & 0x0FFF_FFFF)),
            Err(ref e) => bail!(RuntimeError(format!("{:?}", e))),
        }
    }
}

impl NumericDocValues for DirectPackedReader28 {
    fn get(&mut self, doc_id: DocId) -> Result<i64> {
        LongValues::get64(self, i64::from(doc_id))
    }
}

// ================ Begin Reader 32 ================
struct DirectPackedReader32 {
    random_access_input: Arc<Box<RandomAccessInput>>,
    offset: i64,
}

impl DirectPackedReader32 {
    fn new(random_access_input: Arc<Box<RandomAccessInput>>, offset: i64) -> DirectPackedReader32 {
        DirectPackedReader32 {
            random_access_input,
            offset,
        }
    }
}

impl LongValues for DirectPackedReader32 {
    fn get64(&mut self, index: i64) -> Result<i64> {
        if index < 0 {
            bail!(IllegalArgument(format!(
                "negative index encountered: {}",
                index
            )));
        }

        match self.random_access_input
            .read_int(self.offset + (index << 2))
        {
            Ok(v) => Ok(i64::from(v as u32)),
            Err(ref e) => bail!(RuntimeError(format!("{:?}", e))),
        }
    }
}

impl NumericDocValues for DirectPackedReader32 {
    fn get(&mut self, doc_id: DocId) -> Result<i64> {
        LongValues::get64(self, i64::from(doc_id))
    }
}

// ================ Begin Reader 40 ================
struct DirectPackedReader40 {
    random_access_input: Arc<Box<RandomAccessInput>>,
    offset: i64,
}

impl DirectPackedReader40 {
    fn new(random_access_input: Arc<Box<RandomAccessInput>>, offset: i64) -> DirectPackedReader40 {
        DirectPackedReader40 {
            random_access_input,
            offset,
        }
    }
}

impl LongValues for DirectPackedReader40 {
    fn get64(&mut self, index: i64) -> Result<i64> {
        if index < 0 {
            bail!(IllegalArgument(format!(
                "negative index encountered: {}",
                index
            )));
        }

        match self.random_access_input.read_long(self.offset + index * 5) {
            Ok(w) => Ok(((w as u64) >> 24) as i64),
            Err(ref e) => bail!(RuntimeError(format!("{:?}", e))),
        }
    }
}

impl NumericDocValues for DirectPackedReader40 {
    fn get(&mut self, doc_id: DocId) -> Result<i64> {
        LongValues::get64(self, i64::from(doc_id))
    }
}

// ================ Begin Reader 48 ================
struct DirectPackedReader48 {
    random_access_input: Arc<Box<RandomAccessInput>>,
    offset: i64,
}

impl DirectPackedReader48 {
    fn new(random_access_input: Arc<Box<RandomAccessInput>>, offset: i64) -> DirectPackedReader48 {
        DirectPackedReader48 {
            random_access_input,
            offset,
        }
    }
}

impl LongValues for DirectPackedReader48 {
    fn get64(&mut self, index: i64) -> Result<i64> {
        if index < 0 {
            bail!(IllegalArgument(format!(
                "negative index encountered: {}",
                index
            )));
        }

        match self.random_access_input.read_long(self.offset + index * 6) {
            Ok(w) => Ok(((w as u64) >> 16) as i64),
            Err(ref e) => bail!(RuntimeError(format!("{:?}", e))),
        }
    }
}

impl NumericDocValues for DirectPackedReader48 {
    fn get(&mut self, doc_id: DocId) -> Result<i64> {
        LongValues::get64(self, i64::from(doc_id))
    }
}

// ================ Begin Reader 56 ================
struct DirectPackedReader56 {
    random_access_input: Arc<Box<RandomAccessInput>>,
    offset: i64,
}

impl DirectPackedReader56 {
    fn new(random_access_input: Arc<Box<RandomAccessInput>>, offset: i64) -> DirectPackedReader56 {
        DirectPackedReader56 {
            random_access_input,
            offset,
        }
    }
}

impl LongValues for DirectPackedReader56 {
    fn get64(&mut self, index: i64) -> Result<i64> {
        if index < 0 {
            bail!(IllegalArgument(format!(
                "negative index encountered: {}",
                index
            )));
        }

        match self.random_access_input.read_long(self.offset + 7 * index) {
            Ok(v) => Ok(((v as u64) >> 8) as i64),
            Err(ref e) => bail!(RuntimeError(format!("{:?}", e))),
        }
    }
}

impl NumericDocValues for DirectPackedReader56 {
    fn get(&mut self, doc_id: DocId) -> Result<i64> {
        LongValues::get64(self, i64::from(doc_id))
    }
}

// ================ Begin Reader 64 ================
struct DirectPackedReader64 {
    random_access_input: Arc<Box<RandomAccessInput>>,
    offset: i64,
}

impl DirectPackedReader64 {
    fn new(random_access_input: Arc<Box<RandomAccessInput>>, offset: i64) -> DirectPackedReader64 {
        DirectPackedReader64 {
            random_access_input,
            offset,
        }
    }
}

impl LongValues for DirectPackedReader64 {
    fn get64(&mut self, index: i64) -> Result<i64> {
        if index < 0 {
            bail!(IllegalArgument(format!(
                "negative index encountered: {}",
                index
            )));
        }

        match self.random_access_input
            .read_long(self.offset + (index << 3))
        {
            Ok(v) => Ok(v),
            Err(ref e) => bail!(RuntimeError(format!("{:?}", e))),
        }
    }
}

impl NumericDocValues for DirectPackedReader64 {
    fn get(&mut self, doc_id: DocId) -> Result<i64> {
        LongValues::get64(self, i64::from(doc_id))
    }
}
