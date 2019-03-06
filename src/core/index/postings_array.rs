use core::util::array::over_size;

const BYTES_PER_POSTING: usize = 3 * 4; // 3 * sizeof(i32)

pub trait PostingsArray: Default {
    fn parallel_array(&self) -> &ParallelPostingsArray;

    fn parallel_array_mut(&mut self) -> &mut ParallelPostingsArray;

    fn bytes_per_posting(&self) -> usize;

    fn grow(&mut self);

    fn clear(&mut self);
}

pub struct ParallelPostingsArray {
    pub size: usize,
    pub text_starts: Vec<u32>,
    pub int_starts: Vec<u32>,
    pub byte_starts: Vec<u32>,
}

impl Default for ParallelPostingsArray {
    fn default() -> Self {
        ParallelPostingsArray::new(2)
    }
}

impl ParallelPostingsArray {
    pub fn new(size: usize) -> Self {
        ParallelPostingsArray {
            size,
            text_starts: vec![0u32; size],
            int_starts: vec![0u32; size],
            byte_starts: vec![0u32; size],
        }
    }
}

impl PostingsArray for ParallelPostingsArray {
    fn parallel_array(&self) -> &ParallelPostingsArray {
        self
    }

    fn parallel_array_mut(&mut self) -> &mut ParallelPostingsArray {
        self
    }

    fn bytes_per_posting(&self) -> usize {
        BYTES_PER_POSTING
    }

    fn grow(&mut self) {
        self.size = over_size(self.size + 1);
        let new_size = self.size;
        self.text_starts.resize(new_size, 0u32);
        self.int_starts.resize(new_size, 0u32);
        self.byte_starts.resize(new_size, 0u32);
    }

    fn clear(&mut self) {
        self.size = 0;
        self.text_starts = Vec::with_capacity(0);
        self.int_starts = Vec::with_capacity(0);
        self.byte_starts = Vec::with_capacity(0);
    }
}
