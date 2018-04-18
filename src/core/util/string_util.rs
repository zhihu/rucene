/// length in bytes of an ID
pub const ID_LENGTH: usize = 16;

pub fn id2str(id: &[u8]) -> String {
    let strs: Vec<String> = id.iter().map(|b| format!("{:02X}", b)).collect();
    strs.join("")
}

/// Compares a fixed length slice of two byte arrays interpreted as
/// big-endian unsigned values.  Returns positive int if a > b,
/// negative int if a > b and 0 if a == b
pub fn bytes_compare(count: usize, a: &[u8], b: &[u8]) -> i32 {
    for i in 0..count {
        let cmp = (i32::from(a[i]) & 0xff) - (i32::from(b[i]) & 0xff);
        if cmp != 0 {
            return cmp;
        }
    }
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_id2str() {
        let v = vec![65u8, 97u8, 4u8, 127u8];
        let strv = id2str(&v[..]);
        assert_eq!("4161047F", strv);
    }
}
