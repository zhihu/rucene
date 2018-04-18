use core::store::IndexInput;
use core::store::RandomAccessInput;
use error::Result;
use std::sync::{Arc, Mutex};

pub trait ImmutableBits: Send + Sync {
    fn get(&self, index: usize) -> Result<bool>;
    fn id(&self) -> i32 {
        0
    }
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub trait MutableBits: Send + Sync {
    fn get(&mut self, index: usize) -> Result<bool>;
    fn id(&self) -> i32 {
        0
    }
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub enum Bits {
    Immutable(Box<ImmutableBits>),
    Mutable(Mutex<Box<MutableBits>>),
}

impl Bits {
    pub fn new(b: Box<ImmutableBits>) -> Self {
        Bits::Immutable(b)
    }

    pub fn new_mut(b: Box<MutableBits>) -> Self {
        Bits::Mutable(Mutex::new(b))
    }

    pub fn get(&self, index: usize) -> Result<bool> {
        match *self {
            Bits::Immutable(ref b) => b.get(index),
            Bits::Mutable(ref b) => b.lock()?.get(index),
        }
    }

    pub fn id(&self) -> i32 {
        match *self {
            Bits::Immutable(ref b) => b.id(),
            Bits::Mutable(ref b) => {
                let b = b.lock().unwrap();
                b.id()
            }
        }
    }

    pub fn len(&self) -> usize {
        match *self {
            Bits::Immutable(ref b) => b.len(),
            Bits::Mutable(ref b) => {
                let b = b.lock().unwrap();
                b.len()
            }
        }
    }

    pub fn is_empty(&self) -> bool {
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

impl ImmutableBits for MatchAllBits {
    fn get(&self, _index: usize) -> Result<bool> {
        Ok(true)
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

impl ImmutableBits for MatchNoBits {
    fn get(&self, _index: usize) -> Result<bool> {
        Ok(false)
    }

    fn len(&self) -> usize {
        self.len
    }
}

pub struct LiveBits {
    input: Mutex<Box<RandomAccessInput>>,
    count: usize,
}

impl LiveBits {
    pub fn new(data: &IndexInput, offset: i64, count: usize) -> Result<LiveBits> {
        let length = (count + 7) >> 3;
        let input = data.random_access_slice(offset, length as i64)?;
        let input = Mutex::new(input);
        Ok(LiveBits { input, count })
    }
}

impl ImmutableBits for LiveBits {
    fn get(&self, index: usize) -> Result<bool> {
        let input = self.input.lock()?;
        let bitset = input.read_byte((index >> 3) as i64)?;
        Ok((bitset & (1u8 << (index & 0x7))) != 0)
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

impl ImmutableBits for FixedBits {
    fn get(&self, index: usize) -> Result<bool> {
        assert!(index < self.num_bits);
        let i = index >> 6;

        let bit_mask = 1i64 << (index % 64) as i64;
        Ok(self.bits[i] & bit_mask != 0)
    }

    fn len(&self) -> usize {
        self.num_bits
    }
}
