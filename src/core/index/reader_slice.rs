#[derive(Copy, Clone)]
pub struct ReaderSlice {
    #[allow(dead_code)]
    start: i32,
    #[allow(dead_code)]
    length: i32,
    #[allow(dead_code)]
    reader_index: i32,
}

impl ReaderSlice {
    pub fn new(start: i32, length: i32, reader_index: i32) -> ReaderSlice {
        ReaderSlice {
            start,
            length,
            reader_index,
        }
    }
}
