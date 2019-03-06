#[derive(Copy, Clone)]
pub struct ReaderSlice {
    pub start: i32,
    pub length: i32,
    pub reader_index: usize,
}

impl ReaderSlice {
    pub fn new(start: i32, length: i32, reader_index: usize) -> ReaderSlice {
        ReaderSlice {
            start,
            length,
            reader_index,
        }
    }
}
