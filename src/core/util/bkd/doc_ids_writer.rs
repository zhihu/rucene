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

use error::{ErrorKind::UnsupportedOperation, Result};

use core::codec::points::IntersectVisitor;
use core::store::io::{DataOutput, IndexInput};
use core::util::bit_util::UnsignedShift;
use core::util::DocId;

use std::borrow::Cow;

pub struct DocIdsWriter;

impl DocIdsWriter {
    /// Read {@code count} integers into {@code docIDs}.
    pub fn read_ints(
        input: &mut dyn IndexInput,
        count: usize,
        doc_ids: &mut [DocId],
    ) -> Result<()> {
        let bpv = input.read_byte()?;

        match bpv {
            0 => DocIdsWriter::read_delta_vints(input, count, doc_ids),
            32 => DocIdsWriter::read_ints32(input, count, doc_ids),
            24 => DocIdsWriter::read_ints24(input, count, doc_ids),
            _ => bail!(UnsupportedOperation(Cow::Owned(format!(
                "Unsupported number of bits per value: {}",
                bpv
            )))),
        }
    }

    fn read_delta_vints(
        input: &mut dyn IndexInput,
        count: usize,
        doc_ids: &mut [DocId],
    ) -> Result<()> {
        let mut doc = 0;
        for id in doc_ids.iter_mut().take(count) {
            doc += input.read_vint()?;
            *id = doc;
        }

        Ok(())
    }

    pub fn read_ints32(
        input: &mut dyn IndexInput,
        count: usize,
        doc_ids: &mut [DocId],
    ) -> Result<()> {
        for id in doc_ids.iter_mut().take(count) {
            *id = input.read_int()?;
        }

        Ok(())
    }

    fn read_ints24(input: &mut dyn IndexInput, count: usize, doc_ids: &mut [DocId]) -> Result<()> {
        let mut i = 0usize;

        while i + 7 < count {
            let l1 = input.read_long()?;
            let l2 = input.read_long()?;
            let l3 = input.read_long()?;

            doc_ids[i] = l1.unsigned_shift(40) as i32;
            doc_ids[i + 1] = (l1.unsigned_shift(16) & 0xFF_FFFF) as i32;
            doc_ids[i + 2] = (((l1 & 0xFFFF) << 8) | l2.unsigned_shift(56)) as i32;
            doc_ids[i + 3] = (l2.unsigned_shift(32) & 0xFF_FFFF) as i32;
            doc_ids[i + 4] = (l2.unsigned_shift(8) & 0xFF_FFFF) as i32;
            doc_ids[i + 5] = (((l2 & 0xFF) << 16) | l3.unsigned_shift(48)) as i32;
            doc_ids[i + 6] = (l3.unsigned_shift(24) & 0xFF_FFFF) as i32;
            doc_ids[i + 7] = (l3 & 0xFF_FFFF) as i32;

            i += 8;
        }

        for id in doc_ids.iter_mut().take(count).skip(i) {
            let l1 = u32::from(input.read_short()? as u16);
            let l2 = u32::from(input.read_byte()?);
            *id = ((l1 << 8) | l2) as i32;
        }

        Ok(())
    }

    /// Read {@code count} integers and feed the result directly to {@link
    /// IntersectVisitor#visit(int)}.
    pub fn read_ints_with_visitor(
        input: &mut dyn IndexInput,
        count: usize,
        visitor: &mut impl IntersectVisitor,
    ) -> Result<()> {
        let bpv = input.read_byte()?;

        match bpv {
            0 => DocIdsWriter::read_delta_vints_with_visitor(input, count, visitor),
            32 => DocIdsWriter::read_ints32_with_visitor(input, count, visitor),
            24 => DocIdsWriter::read_ints24_with_visitor(input, count, visitor),
            _ => bail!(UnsupportedOperation(Cow::Owned(format!(
                "Unsupported number of bits per value: {}",
                bpv
            )))),
        }
    }

    fn read_delta_vints_with_visitor(
        input: &mut dyn IndexInput,
        count: usize,
        visitor: &mut impl IntersectVisitor,
    ) -> Result<()> {
        let mut doc = 0;
        for _ in 0..count {
            doc += input.read_vint()?;
            visitor.visit(doc)?;
        }

        Ok(())
    }

    pub fn read_ints32_with_visitor(
        input: &mut dyn IndexInput,
        count: usize,
        visitor: &mut impl IntersectVisitor,
    ) -> Result<()> {
        for _ in 0..count {
            visitor.visit(input.read_vint()?)?;
        }

        Ok(())
    }

    fn read_ints24_with_visitor(
        input: &mut dyn IndexInput,
        count: usize,
        visitor: &mut impl IntersectVisitor,
    ) -> Result<()> {
        let mut i = 0;

        while i + 7 < count {
            let l1 = input.read_long()?;
            let l2 = input.read_long()?;
            let l3 = input.read_long()?;

            visitor.visit(l1.unsigned_shift(40) as i32)?;
            visitor.visit((l1.unsigned_shift(16) & 0xFF_FFFF) as i32)?;
            visitor.visit((((l1 & 0xFFFF) << 8) | l2.unsigned_shift(56)) as i32)?;
            visitor.visit((l2.unsigned_shift(32) & 0xFF_FFFF) as i32)?;
            visitor.visit((l2.unsigned_shift(8) & 0xFF_FFFF) as i32)?;
            visitor.visit((((l2 & 0xFF) << 16) | l3.unsigned_shift(48)) as i32)?;
            visitor.visit((l3.unsigned_shift(24) & 0xFF_FFFF) as i32)?;
            visitor.visit((l3 & 0xFF_FFFF) as i32)?;

            i += 8;
        }

        for _ in i..count {
            let l1 = u32::from(input.read_short()? as u16);
            let l2 = u32::from(input.read_byte()?);
            visitor.visit(((l1 << 8) | l2) as i32)?;
        }

        Ok(())
    }

    pub fn write_doc_ids(
        out: &mut impl DataOutput,
        doc_ids: &[DocId],
        start: usize,
        count: usize,
    ) -> Result<()> {
        // docs can be sorted either when all docs in a block have the same value
        // or when a segment is sorted
        let mut sorted = true;
        for i in 1..count {
            if doc_ids[start + i - 1] > doc_ids[start + i] {
                sorted = false;
                break;
            }
        }

        if sorted {
            out.write_byte(0)?;
            let mut previous = 0;
            for doc in &doc_ids[start..start + count] {
                let d = *doc;
                out.write_vint(d - previous)?;
                previous = d;
            }
        } else {
            let mut max = 0;
            for i in 0..count {
                max |= doc_ids[start + i] as u32 as u64;
            }

            if max <= 0x00FF_FFFF {
                out.write_byte(24 as u8)?;
                for i in 0..count {
                    out.write_short((doc_ids[start + i].unsigned_shift(8)) as i16)?;
                    out.write_byte(doc_ids[start + i] as u8)?;
                }
            } else {
                out.write_byte(32 as u8)?;
                for i in 0..count {
                    out.write_int(doc_ids[start + i])?;
                }
            }
        }

        Ok(())
    }
}
