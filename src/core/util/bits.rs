use core::store::IndexInput;
use core::store::RandomAccessInput;
use error::Result;
use std::sync::Arc;

pub type BitsContext = Option<[u8; 64]>;

pub trait Bits: Send + Sync {
    fn get_with_ctx(&self, ctx: BitsContext, index: usize) -> Result<(bool, BitsContext)>;
    fn get(&self, index: usize) -> Result<bool> {
        self.get_with_ctx(None, index).map(|x| x.0)
    }
    fn id(&self) -> i32 {
        0
    }
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub type BitsRef = Arc<Bits>;

pub struct MatchAllBits {
    len: usize,
}

impl MatchAllBits {
    pub fn new(len: usize) -> Self {
        MatchAllBits { len }
    }
}

impl Bits for MatchAllBits {
    fn get_with_ctx(&self, ctx: BitsContext, _index: usize) -> Result<(bool, BitsContext)> {
        Ok((true, ctx))
    }

    fn id(&self) -> i32 {
        1
    }

    fn len(&self) -> usize {
        self.len
    }
}

pub struct MatchNoBits {
    len: usize,
}

impl MatchNoBits {
    pub fn new(len: usize) -> Self {
        MatchNoBits { len }
    }
}

impl Bits for MatchNoBits {
    fn get_with_ctx(&self, ctx: BitsContext, _index: usize) -> Result<(bool, BitsContext)> {
        Ok((false, ctx))
    }

    fn len(&self) -> usize {
        self.len
    }
}

pub struct LiveBits {
    input: Box<RandomAccessInput>,
    count: usize,
}

impl LiveBits {
    pub fn new(data: &IndexInput, offset: i64, count: usize) -> Result<LiveBits> {
        let length = (count + 7) >> 3;
        let input = data.random_access_slice(offset, length as i64)?;
        Ok(LiveBits { input, count })
    }
}

impl Bits for LiveBits {
    fn get_with_ctx(&self, ctx: BitsContext, index: usize) -> Result<(bool, BitsContext)> {
        let bitset = self.input.read_byte((index >> 3) as i64)?;
        Ok(((bitset & (1u8 << (index & 0x7))) != 0, ctx))
    }

    fn len(&self) -> usize {
        self.count
    }
}

pub struct FixedBits {
    num_bits: usize,
    num_words: usize,
    bits: Arc<Vec<i64>>,
}

impl FixedBits {
    pub fn new(bits: Arc<Vec<i64>>, num_bits: usize) -> FixedBits {
        let num_words = FixedBits::bits_2_words(num_bits);
        FixedBits {
            num_bits,
            num_words,
            bits,
        }
    }

    pub fn bits_2_words(num_bits: usize) -> usize {
        if num_bits == 0 {
            0
        } else {
            ((num_bits - 1) >> 6) + 1
        }
    }

    pub fn cardinality(&self) -> usize {
        let mut set_bits = 0;
        for i in 0..self.num_words {
            set_bits += self.bits[i].count_ones() as usize;
        }

        set_bits
    }

    pub fn length(&self) -> usize {
        self.num_bits
    }
}

impl Bits for FixedBits {
    fn get_with_ctx(&self, ctx: BitsContext, index: usize) -> Result<(bool, BitsContext)> {
        assert!(index < self.num_bits);
        let i = index >> 6;

        let bit_mask = 1i64 << (index % 64) as i64;
        Ok((self.bits[i] & bit_mask != 0, ctx))
    }

    fn len(&self) -> usize {
        self.num_bits
    }
}
