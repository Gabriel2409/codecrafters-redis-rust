use std::fmt::Display;

use crate::{Error, Result};
#[derive(Debug, Clone)]
pub struct Stream {
    last_id: StreamId,
}

impl Stream {
    pub fn new() -> Self {
        Self {
            last_id: StreamId::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct StreamId {
    timestamp_ms: u64,
    seq_number: u64,
}
impl StreamId {
    pub fn new() -> Self {
        Self {
            timestamp_ms: 0,
            seq_number: 0,
        }
    }
}

impl TryFrom<&str> for StreamId {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self> {
        let (ts, seq) = match value.split_once("-") {
            None => (value, None),
            Some((ts, seq)) => (ts, Some(seq)),
        };
        if ts.len() != 13 {
            Err(Error::CantConvertToMsTimestamp(ts.to_string()))?;
        }
        let timestamp_ms = ts.parse::<u64>()?;
        let seq_number = match seq {
            None => 0,
            Some(seq) => seq.parse::<u64>()?,
        };
        Ok(Self {
            timestamp_ms,
            seq_number,
        })
    }
}

impl Display for StreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.timestamp_ms, self.seq_number)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_stream_id() -> Result<()> {
        let initial_input = "1526985054069-3";
        let stream_id = StreamId::try_from(initial_input)?;
        assert_eq!(
            stream_id,
            StreamId {
                timestamp_ms: 1526985054069,
                seq_number: 3
            }
        );
        assert_eq!(stream_id.to_string(), initial_input.to_string());

        Ok(())
    }
}
