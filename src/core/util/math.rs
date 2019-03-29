use error::ErrorKind::IllegalState;
use error::Result;

use core::util::bit_util::UnsignedShift;

pub fn log(mut x: i64, base: i32) -> i32 {
    debug_assert!(base > 1);

    let base = i64::from(base);
    let mut ret = 0;
    while x >= base {
        x /= base;
        ret += 1;
    }

    ret
}

pub fn long_to_int_exact(val: i64) -> Result<i32> {
    let ans = val as i32;
    if i64::from(ans) != val {
        bail!(IllegalState("integer overflow".to_owned()));
    }
    Ok(ans)
}

// see http://en.wikipedia.org/wiki/Binary_GCD_algorithm#Iterative_version_in_C.2B.2B_using_ctz_.28count_trailing_zeros.29
pub fn gcd(a: i64, b: i64) -> i64 {
    debug_assert_ne!(a, i64::min_value());
    debug_assert_ne!(b, i64::min_value());
    let mut a = a.abs();
    let mut b = b.abs();

    if a == 0 {
        return b;
    } else if b == 0 {
        return a;
    }

    let common_trailing_zeros = (a | b).trailing_zeros();
    a = a.unsigned_shift(a.trailing_zeros() as usize);

    loop {
        b = b.unsigned_shift(b.trailing_zeros() as usize);
        if a == b {
            break;
        } else if a > b || a == i64::min_value() {
            // MIN_VALUE is treated as 2^64
            let tmp = a;
            a = b;
            b = tmp;
        }

        if a == 1 {
            break;
        }

        b -= a;
    }

    a << common_trailing_zeros
}
