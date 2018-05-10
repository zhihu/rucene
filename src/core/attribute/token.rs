use error::*;

#[derive(Debug, Default)]
pub struct OffsetAttribute {
    start_offset: i32,
    end_offset: i32,
}

impl OffsetAttribute {
    pub fn new() -> OffsetAttribute {
        OffsetAttribute {
            start_offset: 0,
            end_offset: 0,
        }
    }

    pub fn start_offset(&self) -> i32 {
        self.start_offset
    }

    pub fn set_offsets(&mut self, start_offset: i32, end_offset: i32) -> Result<()> {
        // TODO: we could assert that this is set-once, ie,
        // current values are -1?  Very few token filters should
        // change offsets once set by the tokenizer... and
        // tokenizer should call clearAtts before re-using
        // OffsetAtt

        if start_offset < 0 || end_offset < start_offset {
            bail!(
                "startOffset must be non-negative, and endOffset must be >= startOffset; got \
                 startOffset={}, endOffset={}",
                start_offset,
                end_offset
            )
        }

        self.start_offset = start_offset;
        self.end_offset = end_offset;
        Ok(())
    }

    pub fn end_offset(&self) -> i32 {
        self.end_offset
    }

    pub fn clear(&mut self) {
        // TODO: we could use -1 as default here?  Then we can
        // assert in setOffset...
        self.start_offset = 0;
        self.end_offset = 0;
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PositionIncrementAttribute {
    position_increment: i32,
}

impl Default for PositionIncrementAttribute {
    fn default() -> Self {
        PositionIncrementAttribute::new()
    }
}

impl PositionIncrementAttribute {
    #[inline]
    pub fn new() -> PositionIncrementAttribute {
        PositionIncrementAttribute {
            position_increment: 1,
        }
    }

    pub fn set_position_increment(&mut self, position_increment: i32) -> Result<()> {
        if position_increment < 0 {
            bail!(
                "Position increment must be zero or greater; got {}",
                position_increment
            )
        }
        self.position_increment = position_increment;
        Ok(())
    }

    pub fn get_position_increment(&self) -> i32 {
        self.position_increment
    }

    pub fn clear(&mut self) {
        self.position_increment = 1
    }

    pub fn end(&mut self) {
        self.position_increment = 0
    }
}

pub struct PayloadAttribute {
    payload: Vec<u8>,
}

impl PayloadAttribute {
    pub fn new(payload: &[u8]) -> PayloadAttribute {
        PayloadAttribute {
            payload: payload.to_vec(),
        }
    }

    pub fn from(payload: Vec<u8>) -> PayloadAttribute {
        PayloadAttribute { payload }
    }

    pub fn get_payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn set_payload(&mut self, payload: Vec<u8>) {
        self.payload = payload
    }

    pub fn clear(&mut self) {
        self.payload.clear()
    }
}
