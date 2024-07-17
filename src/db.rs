use mio::net::TcpStream;
use mio::Token;

use crate::rdb::{Rdb, ValueTypeEncoding};
use crate::replica::Replica;
use crate::stream::Stream;
use crate::token::TokenTrack;
use crate::{Error, Result};
use std::cell::RefCell;
use std::collections::HashMap;
use std::io::Write;
use std::rc::Rc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::parser::RedisValue;

#[derive(Debug, Clone)]
pub enum ConnectionState {
    Ready,
    Waiting(Instant, Duration, u64, u64),
    BeforePing,
    BeforeReplConf1,
    BeforeReplConf2,
    BeforePsync,
    BeforeRdbFile,
}

#[derive(Debug, Clone)]
pub struct DbValue {
    pub value: ValueType,
    pub expires_at: Option<Instant>,
}

// TODO: rename
#[derive(Debug, Clone)]
pub enum ValueType {
    String(String),
    Stream(Stream),
}

impl DbValue {
    fn new(value: ValueType, expires_in: Option<Duration>) -> Self {
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
    pub dir: String,
    pub dbfilename: String,
}

impl DbInfo {
    pub fn build(role: &str, port: u16, dir: &str, dbfilename: &str) -> Self {
        let master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string();
        let master_repl_offset = 0;

        Self {
            role: role.to_string(),
            port,
            master_replid,
            master_repl_offset,
            dir: dir.to_string(),
            dbfilename: dbfilename.to_string(),
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

#[derive(Debug)]
pub struct RedisDb {
    pub info: DbInfo,
    pub state: ConnectionState,
    inner: Rc<RefCell<InnerRedisDb>>,
    pub replicas: Vec<Replica>,
    pub waiting_connection: Option<Rc<RefCell<TcpStream>>>,
    pub processed_bytes: usize,
    pub token_track: TokenTrack,
}

impl RedisDb {
    pub fn build(info: DbInfo, state: ConnectionState) -> Self {
        Self {
            info,
            state,
            inner: Rc::new(RefCell::new(InnerRedisDb::build())),
            replicas: Vec::new(),
            processed_bytes: 0,
            waiting_connection: None,
            token_track: TokenTrack::new(),
        }
    }

    pub fn set(&self, key: String, value: ValueType, px: Option<u64>) {
        let expires_in = px.map(Duration::from_millis);
        let db_value = DbValue::new(value, expires_in);
        self.inner.borrow_mut().store.insert(key, db_value);
    }

    pub fn get(&self, key: &str) -> Option<ValueType> {
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

    pub fn xadd(
        &self,
        key: &str,
        stream_id: &str,
        store: HashMap<String, String>,
    ) -> Result<String> {
        let db_value = self
            .inner
            .borrow_mut()
            .store
            .entry(key.to_string())
            .or_insert(DbValue::new(ValueType::Stream(Stream::new()), None));
        match db_value.value {
            ValueType::Stream(mut stream) => {
                let stream_id = match stream_id {
                    "*" => None,
                    x => Some(x.try_into()?),
                };
                let returned_stream_id = stream.xadd(store, stream_id)?;
                Ok(returned_stream_id.to_string())
            }
            _ => Err(Error::WrongTypeOperation)?,
        }
    }

    pub fn keys(&self, pat: &str) -> Vec<String> {
        self.inner
            .borrow()
            .store
            .keys()
            .map(|x| x.to_string())
            .collect::<Vec<_>>()
    }

    pub fn is_replica(&self) -> bool {
        self.info.role == "slave"
    }

    pub fn register_replica(&mut self, replica_stream: TcpStream, replica_token: Token) {
        self.replicas
            .push(Replica::new(replica_stream, replica_token));
    }

    pub fn get_nb_uptodate_replicas(&self) -> usize {
        self.replicas.iter().filter(|r| r.up_to_date).count()
    }
    pub fn mark_replicas_as_outdated(&mut self) {
        for replica in self.replicas.iter_mut() {
            replica.up_to_date = false;
        }
    }

    pub fn mark_replica_as_uptodate(&mut self, token: Token) {
        self.replicas
            .iter_mut()
            .find(|replica| replica.token == token)
            .expect("Replica should exist")
            .up_to_date = true;
    }

    pub fn set_waiting_connection(&mut self, waiting_connection: TcpStream) {
        self.waiting_connection = Some(Rc::new(RefCell::new(waiting_connection)));
    }

    /// Starts the handshake process: A replica sends a ping to the master
    /// Note that the response is handled in the main loop
    pub fn send_ping_to_master(&self, stream: &mut TcpStream) -> Result<()> {
        // let port = self.inner.borrow().info.port;

        let redis_value = RedisValue::array_of_bulkstrings_from("PING");
        stream.write_all(redis_value.to_string().as_bytes())?;
        Ok(())
    }

    pub fn send_to_replicas(&self, redis_value: RedisValue, ignore_up_to_date: bool) -> Result<()> {
        for replica in self.replicas.iter() {
            if replica.up_to_date && ignore_up_to_date {
                continue;
            }
            replica
                .stream
                .borrow_mut()
                .write_all(redis_value.to_string().as_bytes())?;
        }

        Ok(())
    }

    pub fn load_rdb(&self, rdb: &Rdb) {
        let db_section = rdb
            .database_sections
            .iter()
            .find(|x| x.db_number.length == 0);
        match db_section {
            None => {}
            Some(db_section) => {
                for field in &db_section.fields_with_expiry {
                    let unix_timestamp_ms_expire = field.get_unix_timestamp_expiration_ms();

                    let value = match field.value_type {
                        ValueTypeEncoding::String => ValueType::String(field.value.field.clone()),
                        _ => todo!("Only string implemented with rdb"),
                    };

                    match unix_timestamp_ms_expire {
                        None => {
                            self.set(field.key.field.clone(), value, None);
                        }
                        Some(unix_timestamp_ms_expire) => {
                            let since_epoch = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .expect("time should not go backward");

                            let current_timestamp_in_ms = since_epoch.as_secs() * 1000
                                + since_epoch.subsec_nanos() as u64 / 1000000;

                            if current_timestamp_in_ms < unix_timestamp_ms_expire {
                                let px = unix_timestamp_ms_expire - current_timestamp_in_ms;
                                self.set(field.key.field.clone(), value, Some(px));
                            }
                        }
                    }
                }
            }
        }
    }

    // adds to the underlying stream and returns the id as String
    // pub fn xadd(
    //     &self,
    //     key: &str,
    //     stream_id: &str,
    //     store: &HashMap<String, String>,
    // ) -> Result<String> {
    //     let val = self.get(key);
    //     match val {
    //         None => self.set(key.to_string(), ValueType::Stream(Stream::new()), None),
    //         Some(ValueType::Stream(_)) => {}
    //         Some(_) => Err(Error::WrongTypeOperation)?,
    //     }
    //     Ok("AA".to_string())
    // }
}
