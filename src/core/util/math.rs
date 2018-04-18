use error::ErrorKind::IllegalArgument;
use error::ErrorKind::IllegalState;
use error::Result;

pub fn log(x: i64, base: i32) -> Result<i32> {
    if base <= 1 {
        bail!(IllegalArgument("base must be > 1".to_owned()));
    }

    let base = i64::from(base);
    let mut ret = 0;
    let mut x = x;
    while x >= base {
        x /= base;
        ret += 1;
    }

    Ok(ret)
}

pub fn long_to_int_exact(val: i64) -> Result<i32> {
    let ans = val as i32;
    if i64::from(ans) != val {
        bail!(IllegalState("integer overflow".to_owned()));
    }
    Ok(ans)
}
