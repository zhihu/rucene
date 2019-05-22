use core::index::{NumericDocValues, NumericDocValuesContext};
use core::store::RandomAccessInput;
use core::util::{LongValues, LongValuesContext};
use error::ErrorKind::IllegalArgument;
use error::ErrorKind::RuntimeError;
use error::Result;

use core::util::DocId;
use std::sync::Arc;

pub struct DirectReader;
impl DirectReader {
    pub fn get_instance(
        slice: Arc<RandomAccessInput>,
        bits_per_value: i32,
        offset: i64,
    ) -> Result<DirectPackedReader> {
        match bits_per_value {
            1 => Ok(DirectPackedReader(DirectPackedReaderEnum::Bit1(
                DirectPackedReader1::new(slice, offset),
            ))),
            2 => Ok(DirectPackedReader(DirectPackedReaderEnum::Bit2(
                DirectPackedReader2::new(slice, offset),
            ))),
            4 => Ok(DirectPackedReader(DirectPackedReaderEnum::Bit4(
                DirectPackedReader4::new(slice, offset),
            ))),
            8 => Ok(DirectPackedReader(DirectPackedReaderEnum::Bit8(
                DirectPackedReader8::new(slice, offset),
            ))),
            12 => Ok(DirectPackedReader(DirectPackedReaderEnum::Bit12(
                DirectPackedReader12::new(slice, offset),
            ))),
            16 => Ok(DirectPackedReader(DirectPackedReaderEnum::Bit16(
                DirectPackedReader16::new(slice, offset),
            ))),
            20 => Ok(DirectPackedReader(DirectPackedReaderEnum::Bit20(
                DirectPackedReader20::new(slice, offset),
            ))),
            24 => Ok(DirectPackedReader(DirectPackedReaderEnum::Bit24(
                DirectPackedReader24::new(slice, offset),
            ))),
            28 => Ok(DirectPackedReader(DirectPackedReaderEnum::Bit28(
                DirectPackedReader28::new(slice, offset),
            ))),
            32 => Ok(DirectPackedReader(DirectPackedReaderEnum::Bit32(
                DirectPackedReader32::new(slice, offset),
            ))),
            40 => Ok(DirectPackedReader(DirectPackedReaderEnum::Bit40(
                DirectPackedReader40::new(slice, offset),
            ))),
            48 => Ok(DirectPackedReader(DirectPackedReaderEnum::Bit48(
                DirectPackedReader48::new(slice, offset),
            ))),
            56 => Ok(DirectPackedReader(DirectPackedReaderEnum::Bit56(
                DirectPackedReader56::new(slice, offset),
            ))),
            64 => Ok(DirectPackedReader(DirectPackedReaderEnum::Bit64(
                DirectPackedReader64::new(slice, offset),
            ))),
            _ => bail!(IllegalArgument(format!(
                "unsupported bits_per_value: {}",
                bits_per_value
            ))),
        }
    }
}

pub struct DirectPackedReader(DirectPackedReaderEnum);

impl LongValues for DirectPackedReader {
    fn get64_with_ctx(
        &self,
        ctx: LongValuesContext,
        index: i64,
    ) -> Result<(i64, LongValuesContext)> {
        match &self.0 {
            DirectPackedReaderEnum::Bit1(r) => r.get64_with_ctx(ctx, index),
            DirectPackedReaderEnum::Bit2(r) => r.get64_with_ctx(ctx, index),
            DirectPackedReaderEnum::Bit4(r) => r.get64_with_ctx(ctx, index),
            DirectPackedReaderEnum::Bit8(r) => r.get64_with_ctx(ctx, index),
            DirectPackedReaderEnum::Bit12(r) => r.get64_with_ctx(ctx, index),
            DirectPackedReaderEnum::Bit16(r) => r.get64_with_ctx(ctx, index),
            DirectPackedReaderEnum::Bit20(r) => r.get64_with_ctx(ctx, index),
            DirectPackedReaderEnum::Bit24(r) => r.get64_with_ctx(ctx, index),
            DirectPackedReaderEnum::Bit28(r) => r.get64_with_ctx(ctx, index),
            DirectPackedReaderEnum::Bit32(r) => r.get64_with_ctx(ctx, index),
            DirectPackedReaderEnum::Bit40(r) => r.get64_with_ctx(ctx, index),
            DirectPackedReaderEnum::Bit48(r) => r.get64_with_ctx(ctx, index),
            DirectPackedReaderEnum::Bit56(r) => r.get64_with_ctx(ctx, index),
            DirectPackedReaderEnum::Bit64(r) => r.get64_with_ctx(ctx, index),
        }
    }
}

impl NumericDocValues for DirectPackedReader {
    fn get_with_ctx(
        &self,
        ctx: NumericDocValuesContext,
        doc_id: DocId,
    ) -> Result<(i64, NumericDocValuesContext)> {
        match &self.0 {
            DirectPackedReaderEnum::Bit1(r) => r.get64_with_ctx(ctx, i64::from(doc_id)),
            DirectPackedReaderEnum::Bit2(r) => r.get64_with_ctx(ctx, i64::from(doc_id)),
            DirectPackedReaderEnum::Bit4(r) => r.get64_with_ctx(ctx, i64::from(doc_id)),
            DirectPackedReaderEnum::Bit8(r) => r.get64_with_ctx(ctx, i64::from(doc_id)),
            DirectPackedReaderEnum::Bit12(r) => r.get64_with_ctx(ctx, i64::from(doc_id)),
            DirectPackedReaderEnum::Bit16(r) => r.get64_with_ctx(ctx, i64::from(doc_id)),
            DirectPackedReaderEnum::Bit20(r) => r.get64_with_ctx(ctx, i64::from(doc_id)),
            DirectPackedReaderEnum::Bit24(r) => r.get64_with_ctx(ctx, i64::from(doc_id)),
            DirectPackedReaderEnum::Bit28(r) => r.get64_with_ctx(ctx, i64::from(doc_id)),
            DirectPackedReaderEnum::Bit32(r) => r.get64_with_ctx(ctx, i64::from(doc_id)),
            DirectPackedReaderEnum::Bit40(r) => r.get64_with_ctx(ctx, i64::from(doc_id)),
            DirectPackedReaderEnum::Bit48(r) => r.get64_with_ctx(ctx, i64::from(doc_id)),
            DirectPackedReaderEnum::Bit56(r) => r.get64_with_ctx(ctx, i64::from(doc_id)),
            DirectPackedReaderEnum::Bit64(r) => r.get64_with_ctx(ctx, i64::from(doc_id)),
        }
    }
}

enum DirectPackedReaderEnum {
    Bit1(DirectPackedReader1),
    Bit2(DirectPackedReader2),
    Bit4(DirectPackedReader4),
    Bit8(DirectPackedReader8),
    Bit12(DirectPackedReader12),
    Bit16(DirectPackedReader16),
    Bit20(DirectPackedReader20),
    Bit24(DirectPackedReader24),
    Bit28(DirectPackedReader28),
    Bit32(DirectPackedReader32),
    Bit40(DirectPackedReader40),
    Bit48(DirectPackedReader48),
    Bit56(DirectPackedReader56),
    Bit64(DirectPackedReader64),
}

// ================ Begin Reader 1 ================
struct DirectPackedReader1 {
    random_access_input: Arc<RandomAccessInput>,
    offset: i64,
}

impl DirectPackedReader1 {
    fn new(random_access_input: Arc<RandomAccessInput>, offset: i64) -> DirectPackedReader1 {
        DirectPackedReader1 {
            random_access_input,
            offset,
        }
    }
}

impl LongValues for DirectPackedReader1 {
    fn get64_with_ctx(
        &self,
        ctx: LongValuesContext,
        index: i64,
    ) -> Result<(i64, LongValuesContext)> {
        if index < 0 {
            bail!(IllegalArgument(format!(
                "negative index encountered: {}",
                index
            )));
        }

        let shift = 7 - (index as i32 & 0x7);
        let byte_dance = self
            .random_access_input
            .read_byte(self.offset + (index >> 3))?;

        Ok((i64::from((byte_dance >> shift) & 0x1), ctx))
    }
}

impl NumericDocValues for DirectPackedReader1 {
    fn get_with_ctx(
        &self,
        ctx: NumericDocValuesContext,
        doc_id: DocId,
    ) -> Result<(i64, NumericDocValuesContext)> {
        LongValues::get64_with_ctx(self, ctx, i64::from(doc_id))
    }
}

// ================ Begin Reader 2 ================
struct DirectPackedReader2 {
    random_access_input: Arc<RandomAccessInput>,
    offset: i64,
}

impl DirectPackedReader2 {
    fn new(random_access_input: Arc<RandomAccessInput>, offset: i64) -> DirectPackedReader2 {
        DirectPackedReader2 {
            random_access_input,
            offset,
        }
    }
}

impl LongValues for DirectPackedReader2 {
    fn get64_with_ctx(
        &self,
        ctx: LongValuesContext,
        index: i64,
    ) -> Result<(i64, LongValuesContext)> {
        if index < 0 {
            bail!(IllegalArgument(format!(
                "negative index encountered: {}",
                index
            )));
        }

        let shift = (3 - (index as i32 & 0x3)) << 1;

        let byte_dance = self
            .random_access_input
            .read_byte(self.offset + (index >> 2))?;

        Ok((i64::from((byte_dance >> shift) & 0x3), ctx))
    }
}

impl NumericDocValues for DirectPackedReader2 {
    fn get_with_ctx(
        &self,
        ctx: NumericDocValuesContext,
        doc_id: DocId,
    ) -> Result<(i64, NumericDocValuesContext)> {
        LongValues::get64_with_ctx(self, ctx, i64::from(doc_id))
    }
}

// ================ Begin Reader 4 ================
struct DirectPackedReader4 {
    random_access_input: Arc<RandomAccessInput>,
    offset: i64,
}

impl DirectPackedReader4 {
    fn new(random_access_input: Arc<RandomAccessInput>, offset: i64) -> DirectPackedReader4 {
        DirectPackedReader4 {
            random_access_input,
            offset,
        }
    }
}

impl LongValues for DirectPackedReader4 {
    fn get64_with_ctx(
        &self,
        ctx: LongValuesContext,
        index: i64,
    ) -> Result<(i64, LongValuesContext)> {
        if index < 0 {
            bail!(IllegalArgument(format!(
                "negative index encountered: {}",
                index
            )));
        }

        let shift = (((index + 1) & 0x1) as i32) << 2;

        let byte_dance = match self
            .random_access_input
            .read_byte(self.offset + (index >> 1))
        {
            Ok(byte_dance) => byte_dance,
            Err(ref e) => bail!(RuntimeError(format!("{:?}", e))),
        };

        Ok((i64::from((byte_dance >> shift) & 0xF), ctx))
    }
}

impl NumericDocValues for DirectPackedReader4 {
    fn get_with_ctx(
        &self,
        ctx: NumericDocValuesContext,
        doc_id: DocId,
    ) -> Result<(i64, NumericDocValuesContext)> {
        LongValues::get64_with_ctx(self, ctx, i64::from(doc_id))
    }
}

// ================ Begin Reader 8 ================
struct DirectPackedReader8 {
    random_access_input: Arc<RandomAccessInput>,
    offset: i64,
}

impl DirectPackedReader8 {
    fn new(random_access_input: Arc<RandomAccessInput>, offset: i64) -> DirectPackedReader8 {
        DirectPackedReader8 {
            random_access_input,
            offset,
        }
    }
}

impl LongValues for DirectPackedReader8 {
    fn get64_with_ctx(
        &self,
        ctx: LongValuesContext,
        index: i64,
    ) -> Result<(i64, LongValuesContext)> {
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

        Ok((i64::from(byte_dance), ctx))
    }
}

impl NumericDocValues for DirectPackedReader8 {
    fn get_with_ctx(
        &self,
        ctx: NumericDocValuesContext,
        doc_id: DocId,
    ) -> Result<(i64, NumericDocValuesContext)> {
        LongValues::get64_with_ctx(self, ctx, i64::from(doc_id))
    }
}

// ================ Begin Reader 12 ================
struct DirectPackedReader12 {
    random_access_input: Arc<RandomAccessInput>,
    offset: i64,
}

impl DirectPackedReader12 {
    fn new(random_access_input: Arc<RandomAccessInput>, offset: i64) -> DirectPackedReader12 {
        DirectPackedReader12 {
            random_access_input,
            offset,
        }
    }
}

impl LongValues for DirectPackedReader12 {
    fn get64_with_ctx(
        &self,
        ctx: LongValuesContext,
        index: i64,
    ) -> Result<(i64, LongValuesContext)> {
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

        Ok((i64::from((word >> shift) & 0xFFF), ctx))
    }
}

impl NumericDocValues for DirectPackedReader12 {
    fn get_with_ctx(
        &self,
        ctx: NumericDocValuesContext,
        doc_id: DocId,
    ) -> Result<(i64, NumericDocValuesContext)> {
        LongValues::get64_with_ctx(self, ctx, i64::from(doc_id))
    }
}

// ================ Begin Reader 16 ================
struct DirectPackedReader16 {
    random_access_input: Arc<RandomAccessInput>,
    offset: i64,
}

impl DirectPackedReader16 {
    fn new(random_access_input: Arc<RandomAccessInput>, offset: i64) -> DirectPackedReader16 {
        DirectPackedReader16 {
            random_access_input,
            offset,
        }
    }
}

impl LongValues for DirectPackedReader16 {
    fn get64_with_ctx(
        &self,
        ctx: LongValuesContext,
        index: i64,
    ) -> Result<(i64, LongValuesContext)> {
        if index < 0 {
            bail!(IllegalArgument(format!(
                "negative index encountered: {}",
                index
            )));
        }

        let word = match self
            .random_access_input
            .read_short(self.offset + (index << 1))
        {
            Ok(w) => w as u16,
            Err(ref e) => bail!(RuntimeError(format!("{:?}", e))),
        };

        Ok((i64::from(word), ctx))
    }
}

impl NumericDocValues for DirectPackedReader16 {
    fn get_with_ctx(
        &self,
        ctx: NumericDocValuesContext,
        doc_id: DocId,
    ) -> Result<(i64, NumericDocValuesContext)> {
        LongValues::get64_with_ctx(self, ctx, i64::from(doc_id))
    }
}

// ================ Begin Reader 20 ================
struct DirectPackedReader20 {
    random_access_input: Arc<RandomAccessInput>,
    offset: i64,
}

impl DirectPackedReader20 {
    fn new(random_access_input: Arc<RandomAccessInput>, offset: i64) -> DirectPackedReader20 {
        DirectPackedReader20 {
            random_access_input,
            offset,
        }
    }
}

impl LongValues for DirectPackedReader20 {
    fn get64_with_ctx(
        &self,
        ctx: LongValuesContext,
        index: i64,
    ) -> Result<(i64, LongValuesContext)> {
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

        Ok((i64::from((dword >> shift) & 0xFFFFF), ctx))
    }
}

impl NumericDocValues for DirectPackedReader20 {
    fn get_with_ctx(
        &self,
        ctx: NumericDocValuesContext,
        doc_id: DocId,
    ) -> Result<(i64, NumericDocValuesContext)> {
        LongValues::get64_with_ctx(self, ctx, i64::from(doc_id))
    }
}

// ================ Begin Reader 24 ================
struct DirectPackedReader24 {
    random_access_input: Arc<RandomAccessInput>,
    offset: i64,
}

impl DirectPackedReader24 {
    fn new(random_access_input: Arc<RandomAccessInput>, offset: i64) -> DirectPackedReader24 {
        DirectPackedReader24 {
            random_access_input,
            offset,
        }
    }
}

impl LongValues for DirectPackedReader24 {
    fn get64_with_ctx(
        &self,
        ctx: LongValuesContext,
        index: i64,
    ) -> Result<(i64, LongValuesContext)> {
        if index < 0 {
            bail!(IllegalArgument(format!(
                "negative index encountered: {}",
                index
            )));
        }

        match self.random_access_input.read_int(self.offset + 3 * index) {
            Ok(v) => Ok((i64::from(v as u32 >> 8), ctx)),
            Err(ref e) => bail!(RuntimeError(format!("{:?}", e))),
        }
    }
}

impl NumericDocValues for DirectPackedReader24 {
    fn get_with_ctx(
        &self,
        ctx: NumericDocValuesContext,
        doc_id: DocId,
    ) -> Result<(i64, NumericDocValuesContext)> {
        LongValues::get64_with_ctx(self, ctx, i64::from(doc_id))
    }
}

// ================ Begin Reader 28 ================
struct DirectPackedReader28 {
    random_access_input: Arc<RandomAccessInput>,
    offset: i64,
}

impl DirectPackedReader28 {
    fn new(random_access_input: Arc<RandomAccessInput>, offset: i64) -> DirectPackedReader28 {
        DirectPackedReader28 {
            random_access_input,
            offset,
        }
    }
}

impl LongValues for DirectPackedReader28 {
    fn get64_with_ctx(
        &self,
        ctx: LongValuesContext,
        index: i64,
    ) -> Result<(i64, LongValuesContext)> {
        if index < 0 {
            bail!(IllegalArgument(format!(
                "negative index encountered: {}",
                index
            )));
        }

        let offset = (index * 7) >> 1;
        let shift = ((index + 1) & 0x1) << 2;
        match self.random_access_input.read_int(self.offset + offset) {
            Ok(v) => Ok((i64::from((v as u32 >> shift) & 0x0FFF_FFFF), ctx)),
            Err(ref e) => bail!(RuntimeError(format!("{:?}", e))),
        }
    }
}

impl NumericDocValues for DirectPackedReader28 {
    fn get_with_ctx(
        &self,
        ctx: NumericDocValuesContext,
        doc_id: DocId,
    ) -> Result<(i64, NumericDocValuesContext)> {
        LongValues::get64_with_ctx(self, ctx, i64::from(doc_id))
    }
}

// ================ Begin Reader 32 ================
struct DirectPackedReader32 {
    random_access_input: Arc<RandomAccessInput>,
    offset: i64,
}

impl DirectPackedReader32 {
    fn new(random_access_input: Arc<RandomAccessInput>, offset: i64) -> DirectPackedReader32 {
        DirectPackedReader32 {
            random_access_input,
            offset,
        }
    }
}

impl LongValues for DirectPackedReader32 {
    fn get64_with_ctx(
        &self,
        ctx: LongValuesContext,
        index: i64,
    ) -> Result<(i64, LongValuesContext)> {
        if index < 0 {
            bail!(IllegalArgument(format!(
                "negative index encountered: {}",
                index
            )));
        }

        match self
            .random_access_input
            .read_int(self.offset + (index << 2))
        {
            Ok(v) => Ok((i64::from(v as u32), ctx)),
            Err(ref e) => bail!(RuntimeError(format!("{:?}", e))),
        }
    }
}

impl NumericDocValues for DirectPackedReader32 {
    fn get_with_ctx(
        &self,
        ctx: NumericDocValuesContext,
        doc_id: DocId,
    ) -> Result<(i64, NumericDocValuesContext)> {
        LongValues::get64_with_ctx(self, ctx, i64::from(doc_id))
    }
}

// ================ Begin Reader 40 ================
struct DirectPackedReader40 {
    random_access_input: Arc<RandomAccessInput>,
    offset: i64,
}

impl DirectPackedReader40 {
    fn new(random_access_input: Arc<RandomAccessInput>, offset: i64) -> DirectPackedReader40 {
        DirectPackedReader40 {
            random_access_input,
            offset,
        }
    }
}

impl LongValues for DirectPackedReader40 {
    fn get64_with_ctx(
        &self,
        ctx: LongValuesContext,
        index: i64,
    ) -> Result<(i64, LongValuesContext)> {
        if index < 0 {
            bail!(IllegalArgument(format!(
                "negative index encountered: {}",
                index
            )));
        }

        match self.random_access_input.read_long(self.offset + index * 5) {
            Ok(w) => Ok((((w as u64) >> 24) as i64, ctx)),
            Err(ref e) => bail!(RuntimeError(format!("{:?}", e))),
        }
    }
}

impl NumericDocValues for DirectPackedReader40 {
    fn get_with_ctx(
        &self,
        ctx: NumericDocValuesContext,
        doc_id: DocId,
    ) -> Result<(i64, NumericDocValuesContext)> {
        LongValues::get64_with_ctx(self, ctx, i64::from(doc_id))
    }
}

// ================ Begin Reader 48 ================
struct DirectPackedReader48 {
    random_access_input: Arc<RandomAccessInput>,
    offset: i64,
}

impl DirectPackedReader48 {
    fn new(random_access_input: Arc<RandomAccessInput>, offset: i64) -> DirectPackedReader48 {
        DirectPackedReader48 {
            random_access_input,
            offset,
        }
    }
}

impl LongValues for DirectPackedReader48 {
    fn get64_with_ctx(
        &self,
        ctx: LongValuesContext,
        index: i64,
    ) -> Result<(i64, LongValuesContext)> {
        if index < 0 {
            bail!(IllegalArgument(format!(
                "negative index encountered: {}",
                index
            )));
        }

        match self.random_access_input.read_long(self.offset + index * 6) {
            Ok(w) => Ok((((w as u64) >> 16) as i64, ctx)),
            Err(ref e) => bail!(RuntimeError(format!("{:?}", e))),
        }
    }
}

impl NumericDocValues for DirectPackedReader48 {
    fn get_with_ctx(
        &self,
        ctx: NumericDocValuesContext,
        doc_id: DocId,
    ) -> Result<(i64, NumericDocValuesContext)> {
        LongValues::get64_with_ctx(self, ctx, i64::from(doc_id))
    }
}

// ================ Begin Reader 56 ================
struct DirectPackedReader56 {
    random_access_input: Arc<RandomAccessInput>,
    offset: i64,
}

impl DirectPackedReader56 {
    fn new(random_access_input: Arc<RandomAccessInput>, offset: i64) -> DirectPackedReader56 {
        DirectPackedReader56 {
            random_access_input,
            offset,
        }
    }
}

impl LongValues for DirectPackedReader56 {
    fn get64_with_ctx(
        &self,
        ctx: LongValuesContext,
        index: i64,
    ) -> Result<(i64, LongValuesContext)> {
        if index < 0 {
            bail!(IllegalArgument(format!(
                "negative index encountered: {}",
                index
            )));
        }

        match self.random_access_input.read_long(self.offset + 7 * index) {
            Ok(v) => Ok((((v as u64) >> 8) as i64, ctx)),
            Err(ref e) => bail!(RuntimeError(format!("{:?}", e))),
        }
    }
}

impl NumericDocValues for DirectPackedReader56 {
    fn get_with_ctx(
        &self,
        ctx: NumericDocValuesContext,
        doc_id: DocId,
    ) -> Result<(i64, NumericDocValuesContext)> {
        LongValues::get64_with_ctx(self, ctx, i64::from(doc_id))
    }
}

// ================ Begin Reader 64 ================
struct DirectPackedReader64 {
    random_access_input: Arc<RandomAccessInput>,
    offset: i64,
}

impl DirectPackedReader64 {
    fn new(random_access_input: Arc<RandomAccessInput>, offset: i64) -> DirectPackedReader64 {
        DirectPackedReader64 {
            random_access_input,
            offset,
        }
    }
}

impl LongValues for DirectPackedReader64 {
    fn get64_with_ctx(
        &self,
        ctx: LongValuesContext,
        index: i64,
    ) -> Result<(i64, LongValuesContext)> {
        if index < 0 {
            bail!(IllegalArgument(format!(
                "negative index encountered: {}",
                index
            )));
        }

        match self
            .random_access_input
            .read_long(self.offset + (index << 3))
        {
            Ok(v) => Ok((v, ctx)),
            Err(ref e) => bail!(RuntimeError(format!("{:?}", e))),
        }
    }
}

impl NumericDocValues for DirectPackedReader64 {
    fn get_with_ctx(
        &self,
        ctx: NumericDocValuesContext,
        doc_id: DocId,
    ) -> Result<(i64, NumericDocValuesContext)> {
        LongValues::get64_with_ctx(self, ctx, i64::from(doc_id))
    }
}
