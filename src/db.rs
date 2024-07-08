use mio::net::TcpStream;

use crate::Result;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::rc::Rc;
use std::time::{Duration, Instant};

use crate::parser::RedisValue;

#[derive(Debug, Clone)]
pub enum ConnectionState {
    Ready,
    BeforePing,
    BeforeReplConf1,
    BeforeReplConf2,
    BeforePsync,
    BeforeRdbFile,
}

#[derive(Debug, Clone)]
struct DbValue {
    value: String,
    expires_at: Option<Instant>,
}

impl DbValue {
    fn new(value: String, expires_in: Option<Duration>) -> Self {
        let expires_at = expires_in.map(|dur| Instant::now() + dur);
        Self { value, expires_at }
    }

    fn is_expired(&self) -> bool {
        if let Some(expires_at) = self.expires_at {
            Instant::now() >= expires_at
        } else {
            false
        }
    }
}

#[derive(Debug, Clone)]
pub struct DbInfo {
    pub role: String,
    pub port: u16,

    pub master_replid: String,
    pub master_repl_offset: u64,
}

impl DbInfo {
    pub fn build(role: &str, port: u16) -> Self {
        let master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string();
        let master_repl_offset = 0;

        Self {
            role: role.to_string(),
            port,
            master_replid,
            master_repl_offset,
        }
    }
}

impl std::fmt::Display for DbInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "role:{}\r\n", self.role)?;
        write!(f, "master_replid:{}\r\n", self.master_replid)?;
        write!(f, "master_repl_offset:{}\r\n", self.master_repl_offset)?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct InnerRedisDb {
    store: HashMap<String, DbValue>,
}

impl InnerRedisDb {
    pub fn build() -> Self {
        Self {
            store: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RedisDb {
    pub info: DbInfo,
    pub state: ConnectionState,
    inner: Rc<RefCell<InnerRedisDb>>,
    pub replica_streams: Vec<Rc<RefCell<TcpStream>>>,
    pub processed_bytes: usize,
}

impl RedisDb {
    pub fn build(info: DbInfo, state: ConnectionState) -> Self {
        Self {
            info,
            state,
            inner: Rc::new(RefCell::new(InnerRedisDb::build())),
            replica_streams: Vec::new(),
            processed_bytes: 0,
        }
    }

    pub fn set(&self, key: String, value: String, px: Option<u64>) {
        let expires_in = px.map(Duration::from_millis);
        let db_value = DbValue::new(value, expires_in);
        self.inner.borrow_mut().store.insert(key, db_value);
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let db_value = self.inner.borrow().store.get(key).cloned();
        match db_value {
            None => None,
            Some(db_value) => {
                if db_value.is_expired() {
                    self.inner.borrow_mut().store.remove(key);
                    None
                } else {
                    Some(db_value.value)
                }
            }
        }
    }

    pub fn is_replica(&self) -> bool {
        self.info.role == "slave"
    }

    pub fn set_replica_stream(&mut self, replica_stream: TcpStream) {
        self.replica_streams
            .push(Rc::new(RefCell::new(replica_stream)));
    }

    /// Starts the handshake process: A replica sends a ping to the master
    /// Note that the response is handled in the main loop
    pub fn send_ping_to_master(&self, stream: &mut TcpStream) -> Result<()> {
        // let port = self.inner.borrow().info.port;

        let redis_value = RedisValue::array_of_bulkstrings_from("PING");
        stream.write_all(redis_value.to_string().as_bytes())?;
        Ok(())
    }

    pub fn send_to_replica(&self, redis_value: RedisValue) -> Result<()> {
        for stream in self.replica_streams.iter() {
            stream
                .borrow_mut()
                .write_all(redis_value.to_string().as_bytes())?;
        }

        Ok(())
    }
}
