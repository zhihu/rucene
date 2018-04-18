use core::store::IndexInput;

pub trait ChecksumIndexInput: IndexInput {
    fn checksum(&self) -> i64;
}
