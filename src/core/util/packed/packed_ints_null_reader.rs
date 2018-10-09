use core::util::packed_misc::{Mutable, Reader};

pub struct PackedIntsNullReader {
    value_count: usize,
}

impl PackedIntsNullReader {
    pub fn new(value_count: usize) -> Self {
        PackedIntsNullReader { value_count }
    }
}

impl Reader for PackedIntsNullReader {
    fn get(&self, _doc_id: usize) -> i64 {
        0
    }

    // FIXME: usize-> docId
    fn bulk_get(&self, index: usize, output: &mut [i64], len: usize) -> usize {
        assert!(index < self.value_count);
        let len = ::std::cmp::min(len, self.value_count - index);
        unsafe {
            let slice = output.as_mut_ptr();
            ::std::ptr::write_bytes(slice, 0, len);
        }
        len
    }
    fn size(&self) -> usize {
        self.value_count
    }

    fn as_mutable(&self) -> &Mutable {
        panic!("mutable not implemented")
    }

    fn as_mutable_mut(&mut self) -> &mut Mutable {
        panic!("mutable not implemented")
    }
}
