pub trait ZigZagEncoding {
    fn encode(&self) -> Self;
    fn decode(&self) -> Self;
}

impl ZigZagEncoding for i32 {
    fn encode(&self) -> i32 {
        (*self >> 31) ^ (self << 1)
    }
    fn decode(&self) -> i32 {
        (*self as u32 >> 1) as i32 ^ -(self & 1)
    }
}

impl ZigZagEncoding for i64 {
    fn encode(&self) -> i64 {
        (*self >> 63) ^ (self << 1)
    }
    fn decode(&self) -> i64 {
        ((*self as u64 >> 1) as i64 ^ -(self & 1))
    }
}

pub trait UnsignedShift: Sized {
    fn unsigned_shift(&self, by: usize) -> Self;
}

impl UnsignedShift for i8 {
    fn unsigned_shift(&self, by: usize) -> Self {
        (*self as u8 >> by) as i8
    }
}

impl UnsignedShift for i16 {
    fn unsigned_shift(&self, by: usize) -> Self {
        (*self as u16 >> by) as i16
    }
}

impl UnsignedShift for i32 {
    fn unsigned_shift(&self, by: usize) -> Self {
        (*self as u32 >> by) as i32
    }
}

impl UnsignedShift for i64 {
    fn unsigned_shift(&self, by: usize) -> Self {
        (*self as u64 >> by) as i64
    }
}

impl UnsignedShift for isize {
    fn unsigned_shift(&self, by: usize) -> Self {
        (*self as usize >> by) as isize
    }
}

pub fn bcompare(a: &[u8], b: &[u8]) -> i32 {
    let alen = a.len();
    let blen = b.len();
    let min_len = ::std::cmp::min(alen, blen);
    for i in 0..min_len {
        if a[i] != b[i] {
            return if a[i] < b[i] { -1 } else { 1 };
        }
    }

    alen as i32 - blen as i32
}

// The pop methods used to rely on bit-manipulation tricks for speed but it
// turns out that it is faster to use the Long.bitCount method (which is an
// intrinsic since Java 6u18) in a naive loop, see LUCENE-2221

/// Returns the number of set bits in an array of longs.
pub fn pop_array(arr: &[i64], word_offset: usize, num_words: usize) -> usize {
    let mut pop_count = 0usize;
    for a in arr.iter().skip(word_offset).take(num_words) {
        pop_count += a.count_ones() as usize;
    }
    pop_count
}
