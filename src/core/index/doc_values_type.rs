#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize)]
pub enum DocValuesType {
    /// No doc values for this field.
    Null,
    /// A per-document Number
    Numeric,
    /// A per-document [u8].  Values may be larger than
    /// 32766 bytes, but different codecs may enforce their own limits.
    Binary,
    /// A pre-sorted [u8]. Fields with this type only store distinct byte values
    /// and store an additional offset pointer per document to dereference the shared
    /// [u8]. The stored [u8] is presorted and allows access via document id,
    /// ordinal and by-value.  Values must be `<= 32766` bytes.
    Sorted,
    /// A pre-sorted [Number]. Fields with this type store numeric values in sorted
    /// order according to `i64::cmp`.
    SortedNumeric,
    /// A pre-sorted Set<[u8]>. Fields with this type only store distinct byte values
    /// and store additional offset pointers per document to dereference the shared
    /// byte[]s. The stored byte[] is presorted and allows access via document id,
    /// ordinal and by-value.  Values must be `<= 32766` bytes.
    SortedSet,
}

impl DocValuesType {
    pub fn null(&self) -> bool {
        match *self {
            DocValuesType::Null => true,
            _ => false,
        }
    }

    pub fn is_numeric(&self) -> bool {
        match *self {
            DocValuesType::Numeric | DocValuesType::SortedNumeric => true,
            _ => false,
        }
    }

    // whether the doc values type can store string
    pub fn support_string(&self) -> bool {
        match *self {
            DocValuesType::Binary | DocValuesType::Sorted | DocValuesType::SortedSet => true,
            _ => false,
        }
    }
}

impl Default for DocValuesType {
    fn default() -> DocValuesType {
        DocValuesType::Null
    }
}
