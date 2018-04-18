use error::*;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize)]
pub enum IndexOptions {
    Null,
    Docs,
    DocsAndFreqs,
    DocsAndFreqsAndPositions,
    DocsAndFreqsAndPositionsAndOffsets,
}

impl Default for IndexOptions {
    fn default() -> IndexOptions {
        IndexOptions::Null
    }
}

impl IndexOptions {
    pub fn from(options: &str) -> Result<IndexOptions> {
        let res = match options {
            "offsets" => IndexOptions::DocsAndFreqsAndPositionsAndOffsets,
            "positions" => IndexOptions::DocsAndFreqsAndPositions,
            "freqs" => IndexOptions::DocsAndFreqs,
            "docs" => IndexOptions::Docs,
            _ => {
                bail!("failed to parse index option [{}]", options);
            }
        };
        Ok(res)
    }

    pub fn has_docs(&self) -> bool {
        match *self {
            IndexOptions::Null => false,
            _ => true,
        }
    }

    pub fn has_freqs(&self) -> bool {
        match *self {
            IndexOptions::DocsAndFreqs => true,
            IndexOptions::DocsAndFreqsAndPositions => true,
            IndexOptions::DocsAndFreqsAndPositionsAndOffsets => true,
            _ => false,
        }
    }

    pub fn has_positions(&self) -> bool {
        match *self {
            IndexOptions::DocsAndFreqsAndPositions => true,
            IndexOptions::DocsAndFreqsAndPositionsAndOffsets => true,
            _ => false,
        }
    }

    pub fn has_offsets(&self) -> bool {
        match *self {
            IndexOptions::DocsAndFreqsAndPositionsAndOffsets => true,
            _ => false,
        }
    }
}
