pub fn fill_slice<T: Copy>(array: &mut [T], value: T) {
    for i in array {
        *i = value;
    }
}

pub fn over_size(size: usize) -> usize {
    let mut size = size;
    let mut extra = size >> 3;
    if extra < 3 {
        // for very small arrays, where constant overhead of
        // realloc is presumably relatively high, we grow
        // faster
        extra = 3;
    }
    size += extra;
    size
}
