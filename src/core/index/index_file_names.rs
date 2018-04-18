pub fn segment_file_name(name: &str, suffix: &str, ext: &str) -> String {
    let ext_len = ext.len();
    let sfx_len = suffix.len();

    if ext_len > 0 && sfx_len > 0 {
        format!("{}_{}.{}", name, suffix, ext)
    } else if ext_len > 0 {
        format!("{}.{}", name, ext)
    } else if sfx_len > 0 {
        format!("{}_{}", name, suffix)
    } else {
        String::from(name)
    }
}

pub fn matches_extension(filename: &str, ext: &str) -> bool {
    filename.ends_with(ext)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_segment_file_name() {
        let seg_name = segment_file_name("524", "", "dvm");
        assert_eq!(seg_name, "524.dvm");
    }
    #[test]
    fn test_matches_extension() {
        let filename = "123.dvd";
        assert_eq!(true, matches_extension(filename, "dvd"));
        assert_ne!(true, matches_extension(filename, "dvm"));
    }
}
