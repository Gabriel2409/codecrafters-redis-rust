use std::{
    collections::{HashMap, VecDeque},
    fmt::Display,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{Error, Result};
#[derive(Debug, Clone)]
pub struct Stream {
    last_stream_id: StreamId,
    entries: VecDeque<StreamEntry>,
}

impl Stream {
    pub fn new() -> Self {
        Self {
            last_stream_id: StreamId::default(),
            entries: VecDeque::from([]),
        }
    }

    /// Generates a new stream id compatible with the stream
    pub fn next_stream_id(&self) -> StreamId {
        let since_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time should not go backward");
        let current_timestamp_in_ms =
            since_epoch.as_secs() * 1000 + since_epoch.subsec_nanos() as u64 / 1000000;

        // We force timestamp_ms to be at least equal to last timestamp in stream
        let (timestamp_ms, seq_number) = {
            if current_timestamp_in_ms > self.last_stream_id.timestamp_ms {
                (current_timestamp_in_ms, 0)
            } else {
                (
                    self.last_stream_id.timestamp_ms,
                    self.last_stream_id.timestamp_ms + 1,
                )
            }
        };

        StreamId {
            timestamp_ms,
            seq_number,
        }
    }

    pub fn xadd(
        &mut self,
        store: HashMap<String, String>,
        stream_id: Option<StreamId>,
    ) -> Result<()> {
        let stream_id = match stream_id {
            None => self.next_stream_id(),
            Some(stream_id) => {
                if stream_id <= self.last_stream_id {
                    Err(Error::InvalidStreamId)?
                } else {
                    stream_id
                }
            }
        };
        let entry = StreamEntry::build(stream_id, store);
        self.entries.push_back(entry);

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct StreamId {
    timestamp_ms: u64,
    seq_number: u64,
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

#[derive(Debug, Clone)]
struct StreamEntry {
    stream_id: StreamId,
    store: HashMap<String, String>,
}
impl StreamEntry {
    pub fn build(stream_id: StreamId, store: HashMap<String, String>) -> Self {
        Self { stream_id, store }
    }
}

impl Display for StreamEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{ id:{}", self.stream_id)?;
        Ok(())
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

    #[test]
    fn test_partial_stream_id() -> Result<()> {
        let initial_input = "1526985054069";
        let stream_id = StreamId::try_from(initial_input)?;
        assert_eq!(
            stream_id,
            StreamId {
                timestamp_ms: 1526985054069,
                seq_number: 0
            }
        );
        assert_eq!(stream_id.to_string(), format!("{}-0", initial_input));

        Ok(())
    }

    #[test]
    fn test_stream_entry() -> Result<()> {
        let initial_input = "1526985054069";
        let stream_id = StreamId::try_from(initial_input)?;

        Ok(())
    }
}
