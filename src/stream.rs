use std::{
    collections::{HashMap, VecDeque},
    fmt::Display,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{Error, Result};
#[derive(Debug, Clone)]
pub struct Stream {
    pub entries: VecDeque<StreamEntry>,
}

impl Stream {
    pub fn new() -> Self {
        Self {
            entries: VecDeque::from([]),
        }
    }

    pub fn get_last_stream_id(&self) -> StreamId {
        self.entries.back().map(|s| s.stream_id).unwrap_or_default()
    }

    /// Generates a new stream id compatible with the stream
    pub fn next_stream_id(&self) -> StreamId {
        let since_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time should not go backward");

        let last_stream_id = self.get_last_stream_id();

        let current_timestamp_in_ms =
            since_epoch.as_secs() * 1000 + since_epoch.subsec_nanos() as u64 / 1000000;
        // We force timestamp_ms to be at least equal to last timestamp in stream
        let (timestamp_ms, seq_number) = {
            if current_timestamp_in_ms > last_stream_id.timestamp_ms {
                (current_timestamp_in_ms, 0)
            } else {
                (last_stream_id.timestamp_ms, last_stream_id.timestamp_ms + 1)
            }
        };

        StreamId {
            timestamp_ms,
            seq_number,
        }
    }

    /// Creates a stream id from a string
    pub fn create_stream_id(&self, value: &str) -> Result<StreamId> {
        match value {
            "*" => Ok(self.next_stream_id()),
            value => {
                let (ts, seq) = match value.split_once("-") {
                    None => (value, None),
                    Some((ts, seq)) => (ts, Some(seq)),
                };
                if ts.len() > 13 {
                    Err(Error::CantConvertToMsTimestamp(ts.to_string()))?;
                }

                let timestamp_ms = ts.parse::<u64>()?;
                let seq_number = match seq {
                    None => 0,
                    Some("*") => {
                        let last_stream_id = self.get_last_stream_id();

                        if last_stream_id.timestamp_ms == timestamp_ms {
                            last_stream_id.seq_number + 1
                        } else if timestamp_ms == 0 {
                            1
                        } else {
                            0
                        }
                    }
                    Some(seq) => seq.parse::<u64>()?,
                };
                Ok(StreamId {
                    timestamp_ms,
                    seq_number,
                })
            }
        }
    }

    pub fn xadd(
        &mut self,
        store: HashMap<String, String>,
        stream_id: Option<StreamId>,
    ) -> Result<StreamId> {
        let stream_id = match stream_id {
            None => self.next_stream_id(),
            Some(stream_id) => {
                if stream_id <= self.get_last_stream_id() {
                    Err(Error::InvalidStreamId {
                        should_be_greater_than: self.get_last_stream_id().to_string(),
                        got: stream_id.to_string(),
                    })?
                } else {
                    stream_id
                }
            }
        };
        let entry = StreamEntry::build(stream_id, store);
        self.entries.push_back(entry);

        Ok(stream_id)
    }

    pub fn xrange(
        &mut self,
        stream_id_start: &str,
        stream_id_end: &str,
    ) -> Result<Vec<(String, HashMap<String, String>)>> {
        if self.entries.is_empty() {
            return Ok(vec![]);
        }

        let start_index = {
            match stream_id_start {
                "-" => Some(0),
                stream_id_start => {
                    let stream_id_start = self.create_stream_id(stream_id_start)?;
                    self.entries
                        .iter()
                        .position(|x| x.stream_id >= stream_id_start)
                }
            }
        };
        let end_index = match stream_id_end {
            "+" => self.entries.len(),
            stream_id_end => {
                let stream_id_end = self.create_stream_id(stream_id_end)?;
                dbg!(stream_id_end);
                self.entries
                    .iter()
                    .position(|x| x.stream_id > stream_id_end)
                    .unwrap_or(self.entries.len())
            }
        };
        match start_index {
            None => Ok(vec![]),
            Some(start_index) => {
                let mut v = Vec::new();
                // NOTE: really not optimized with vecdeque
                for i in start_index..end_index {
                    let entry = &self.entries[i];
                    v.push((entry.stream_id.to_string(), entry.store.clone()));
                }
                Ok(v)
            }
        }
    }

    pub fn xread(
        &mut self,
        stream_id_start: &str,
    ) -> Result<Vec<(String, HashMap<String, String>)>> {
        if self.entries.is_empty() {
            return Ok(vec![]);
        }

        let start_index = {
            match stream_id_start {
                "-" => Some(0),
                stream_id_start => {
                    let stream_id_start = self.create_stream_id(stream_id_start)?;
                    self.entries
                        .iter()
                        // Diff with xrange
                        .position(|x| x.stream_id > stream_id_start)
                }
            }
        };
        match start_index {
            None => Ok(vec![]),
            Some(start_index) => {
                let mut v = Vec::new();
                // NOTE: really not optimized with vecdeque
                for i in start_index..self.entries.len() {
                    let entry = &self.entries[i];
                    v.push((entry.stream_id.to_string(), entry.store.clone()));
                }
                Ok(v)
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct StreamId {
    timestamp_ms: u64,
    seq_number: u64,
}

impl Display for StreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.timestamp_ms, self.seq_number)
    }
}

#[derive(Debug, Clone)]
pub struct StreamEntry {
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
        for (key, val) in self.store.iter() {
            write!(f, ", {}:{}", key, val)?;
        }
        write!(f, " }}")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_stream_id() -> Result<()> {
        let stream = Stream::new();
        let initial_input = "1526985054069-3";
        let stream_id = stream.create_stream_id(initial_input)?;
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
        let stream = Stream::new();
        let initial_input = "1526985054069";
        let stream_id = stream.create_stream_id(initial_input)?;
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
    fn test_xadd() -> Result<()> {
        let mut stream = Stream::new();

        let stream_id = stream.create_stream_id("1526985054069-87")?;
        let mut store = HashMap::new();
        store.insert("key1".to_string(), "value1".to_string());
        stream.xadd(store.clone(), Some(stream_id))?;
        assert_eq!(stream.entries.len(), 1);

        let same_insert = stream.xadd(store.clone(), Some(stream_id));
        assert!(same_insert.is_err());
        assert_eq!(stream.entries.len(), 1);

        let prev_seq_stream_id = stream.create_stream_id("1526985054069-86")?;
        let prev_seq_insert = stream.xadd(store.clone(), Some(prev_seq_stream_id));
        assert!(prev_seq_insert.is_err());
        assert_eq!(stream.entries.len(), 1);

        let prev_timestamp_stream_id = stream.create_stream_id("1526985054068-87")?;
        let prev_timestamp_insert = stream.xadd(store.clone(), Some(prev_timestamp_stream_id));
        assert!(prev_timestamp_insert.is_err());
        assert_eq!(stream.entries.len(), 1);

        let next_seq_stream_id = stream.create_stream_id("1526985054069-88")?;
        stream.xadd(store.clone(), Some(next_seq_stream_id))?;
        assert_eq!(stream.entries.len(), 2);

        let next_timestamp_stream_id = stream.create_stream_id("1526985054070-87")?;
        stream.xadd(store.clone(), Some(next_timestamp_stream_id))?;
        assert_eq!(stream.entries.len(), 3);

        let returned_id = stream.xadd(store.clone(), None)?;
        assert!(returned_id > stream_id);
        assert_eq!(stream.entries.len(), 4);

        dbg!(stream);

        Ok(())
    }
}
