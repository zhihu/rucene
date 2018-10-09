use core::index::{NumericDocValues, NumericDocValuesContext};
use core::util::packed::packed_misc::{check_block_size, copy_by_buf, num_blocks};
use core::util::packed::packed_misc::{GrowableWriter, Mutable};
use core::util::LongValues;
use std::cmp::min;

use error::Result;

const MIN_BLOCK_SIZE: usize = 1 << 6;
const MAX_BLOCK_SIZE: usize = 1 << 30;

pub struct PagedMutableBase {
    pub size: usize,
    pub page_shift: usize,
    pub page_mask: usize,
    pub sub_mutables: Vec<Box<Mutable>>,
    pub bits_per_value: i32,
}

impl PagedMutableBase {
    pub fn new(bits_per_value: i32, size: usize, page_size: usize) -> Self {
        let page_shift = check_block_size(page_size, MIN_BLOCK_SIZE, MAX_BLOCK_SIZE);
        let num_pages = num_blocks(size, page_size);
        PagedMutableBase {
            size,
            page_shift,
            page_mask: page_size - 1,
            sub_mutables: Vec::with_capacity(num_pages),
            bits_per_value,
        }
    }

    fn page_size(&self) -> usize {
        self.page_mask + 1
    }

    fn last_page_size(&self, size: usize) -> usize {
        let sz = self.index_in_page(size);
        if sz == 0 {
            self.page_size()
        } else {
            sz
        }
    }

    fn index_in_page(&self, index: usize) -> usize {
        index & self.page_mask
    }

    fn page_index(&self, index: usize) -> usize {
        index >> self.page_shift
    }
}

pub trait PagedMutable: LongValues + Sized {
    fn paged_mutable_base(&self) -> &PagedMutableBase;

    fn paged_mutable_base_mut(&mut self) -> &mut PagedMutableBase;

    fn fill_pages(&mut self) {
        let num_pages = num_blocks(
            self.paged_mutable_base().size,
            self.paged_mutable_base().page_size() as usize,
        );
        for i in 0..num_pages {
            // do not allocate for more entries than necessary on the last page
            let value_count = if i == num_pages - 1 {
                self.paged_mutable_base()
                    .last_page_size(self.paged_mutable_base().size)
            } else {
                self.paged_mutable_base().page_size()
            };
            let new_tbl = self.new_mutable(value_count, self.paged_mutable_base().bits_per_value);
            self.paged_mutable_base_mut().sub_mutables.push(new_tbl);
        }
    }

    fn new_mutable(&self, value_count: usize, bits_per_value: i32) -> Box<Mutable>;

    fn set(&mut self, index: usize, value: i64) {
        debug_assert!(index < self.paged_mutable_base().size);
        let page_index = self.paged_mutable_base().page_index(index);
        let index_in_page = self.paged_mutable_base().index_in_page(index);
        self.paged_mutable_base_mut().sub_mutables[page_index].set(index_in_page, value)
    }

    fn new_unfilled_copy(&self, new_size: usize) -> Self;

    /// Create a new copy of size `new_size` based on the content of
    /// this buffer. This method is much more efficient than creating a new
    /// instance and copying values one by one.
    fn resize(&self, new_size: usize) -> Self {
        let mut copy = self.new_unfilled_copy(new_size);
        let num_common_pages = min(
            copy.paged_mutable_base().sub_mutables.capacity(),
            self.paged_mutable_base().sub_mutables.len(),
        );
        let mut copy_buffer = [0i64; 1024];
        for i in 0..copy.paged_mutable_base().sub_mutables.capacity() {
            let value_count = if i == copy.paged_mutable_base().sub_mutables.capacity() - 1 {
                self.paged_mutable_base().last_page_size(new_size)
            } else {
                self.paged_mutable_base().page_size()
            };
            let bpv = if i < num_common_pages {
                self.paged_mutable_base().sub_mutables[i].get_bits_per_value()
            } else {
                self.paged_mutable_base().bits_per_value
            };

            copy.paged_mutable_base_mut()
                .sub_mutables
                .push(self.new_mutable(value_count as usize, bpv));

            if i < num_common_pages {
                let copy_length = min(
                    value_count as usize,
                    self.paged_mutable_base().sub_mutables[i].size(),
                );
                copy_by_buf(
                    self.paged_mutable_base().sub_mutables[i].as_ref(),
                    0,
                    copy.paged_mutable_base_mut().sub_mutables[i].as_mut(),
                    0,
                    copy_length,
                    &mut copy_buffer,
                );
            }
        }
        copy
    }

    fn grow_by_size(&self, min_size: usize) -> Self {
        let mut extra = min_size >> 3;
        if extra < 3 {
            extra = 3;
        }
        let new_size = min_size + extra;
        self.resize(new_size)
    }

    fn grow(&self) -> Self {
        self.grow_by_size(self.paged_mutable_base().size + 1)
    }
}

/// A `PagedGrowableWriter`. This class slices data into fixed-size blocks
/// which have independent numbers of bits per value and grow on-demand.
/// You should use this class instead of the `PackedLongValues` related ones only when
/// you need random write-access. Otherwise this class will likely be slower and
/// less memory-efficient.
///
pub struct PagedGrowableWriter {
    base: PagedMutableBase,
    acceptable_overhead_ratio: f32,
}

impl PagedGrowableWriter {
    pub fn new(
        size: usize,
        page_size: usize,
        start_bits_per_value: i32,
        acceptable_overhead_ratio: f32,
    ) -> Self {
        Self::new_with_fillpages(
            size,
            page_size,
            start_bits_per_value,
            acceptable_overhead_ratio,
            true,
        )
    }

    pub fn new_with_fillpages(
        size: usize,
        page_size: usize,
        start_bits_per_value: i32,
        acceptable_overhead_ratio: f32,
        fill_pages: bool,
    ) -> Self {
        let base = PagedMutableBase::new(start_bits_per_value, size, page_size);
        let mut ret = PagedGrowableWriter {
            base,
            acceptable_overhead_ratio,
        };
        if fill_pages {
            ret.fill_pages();
        }
        return ret;
    }

    pub fn size(&self) -> usize {
        self.base.size
    }
}

impl PagedMutable for PagedGrowableWriter {
    fn paged_mutable_base(&self) -> &PagedMutableBase {
        &self.base
    }

    fn paged_mutable_base_mut(&mut self) -> &mut PagedMutableBase {
        &mut self.base
    }

    fn new_mutable(&self, value_count: usize, bits_per_value: i32) -> Box<Mutable> {
        Box::new(GrowableWriter::new(
            bits_per_value,
            value_count,
            self.acceptable_overhead_ratio,
        ))
    }

    fn new_unfilled_copy(&self, new_size: usize) -> Self {
        PagedGrowableWriter::new_with_fillpages(
            new_size,
            self.paged_mutable_base().page_size() as usize,
            self.paged_mutable_base().bits_per_value,
            self.acceptable_overhead_ratio,
            false,
        )
    }
}

impl LongValues for PagedGrowableWriter {
    fn get64_with_ctx(
        &self,
        _ctx: Option<[u8; 64]>,
        index: i64,
    ) -> Result<(i64, Option<[u8; 64]>)> {
        debug_assert!(index >= 0 && index < self.base.size as i64);
        let page_index = self.base.page_index(index as usize);
        let index_in_page = self.base.index_in_page(index as usize);
        Ok((self.base.sub_mutables[page_index].get(index_in_page), None))
    }
}

impl NumericDocValues for PagedGrowableWriter {
    fn get_with_ctx(
        &self,
        _ctx: NumericDocValuesContext,
        doc_id: i32,
    ) -> Result<(i64, NumericDocValuesContext)> {
        debug_assert!(doc_id >= 0 && (doc_id as usize) < self.base.size);
        let page_index = self.base.page_index(doc_id as usize);
        let index_in_page = self.base.index_in_page(doc_id as usize);
        Ok((self.base.sub_mutables[page_index].get(index_in_page), None))
    }
}
