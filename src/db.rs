use mio::net::TcpStream;

use crate::Result;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::{Duration, Instant};

use crate::parser::RedisValue;

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
    role: String,
    port: u16,

    master_replid: String,
    master_repl_offset: u64,
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
    info: DbInfo, // Example of another field
    store: HashMap<String, DbValue>,
}

impl InnerRedisDb {
    pub fn build(info: DbInfo) -> Self {
        Self {
            info,
            store: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RedisDb {
    inner: Rc<RefCell<InnerRedisDb>>,
    pub master_stream: Rc<RefCell<Option<TcpStream>>>,
    // TODO: make a vec
    pub replica_stream: Rc<RefCell<Option<TcpStream>>>,
}

impl RedisDb {
    pub fn build(info: DbInfo) -> Self {
        Self {
            inner: Rc::new(RefCell::new(InnerRedisDb::build(info))),
            master_stream: Rc::new(RefCell::new(None)),
            replica_stream: Rc::new(RefCell::new(None)),
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

    pub fn is_master(&self) -> bool {
        self.inner.borrow().info.role == "master"
    }
    pub fn info(&self) -> String {
        self.inner.borrow().info.to_string()
    }

    pub fn master_replid(&self) -> String {
        self.inner.borrow().info.master_replid.clone()
    }

    pub fn set_master_stream(&mut self, master_stream: Option<TcpStream>) {
        self.master_stream = Rc::new(RefCell::new(master_stream));
    }

    pub fn set_replica_stream(&mut self, replica_stream: TcpStream) {
        self.replica_stream = Rc::new(RefCell::new(Some(replica_stream)));
    }

    pub fn send_handshake_to_master(&self) -> Result<()> {
        let port = self.inner.borrow().info.port;

        let mut master_stream_ref = self.master_stream.borrow_mut();

        if let Some(ref mut stream) = *master_stream_ref {
            dbg!(&stream);
            // Responses from server are small so we don't need a large buffer
            let mut buf = [0; 256];

            let redis_value = RedisValue::array_of_bulkstrings_from("PING");
            stream.write_all(redis_value.to_string().as_bytes())?;

            loop {
                match stream.read(&mut buf) {
                    Ok(bytes_read) => {
                        let response = String::from_utf8_lossy(&buf[..bytes_read]);
                        println!("{}", response);
                        break;
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(200));
                    }
                    Err(e) => {
                        // If it was any other kind of error, something went
                        // wrong and we terminate with an error.
                        Err(e)?
                    }
                }
            }

            buf.fill(0);

            let redis_value =
                RedisValue::array_of_bulkstrings_from(&format!("REPLCONF listening-port {}", port));
            stream.write_all(redis_value.to_string().as_bytes())?;
            loop {
                match stream.read(&mut buf) {
                    Ok(bytes_read) => {
                        let response = String::from_utf8_lossy(&buf[..bytes_read]);
                        println!("{}", response);
                        break;
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(200));
                    }
                    Err(e) => {
                        // If it was any other kind of error, something went
                        // wrong and we terminate with an error.
                        Err(e)?
                    }
                }
            }

            buf.fill(0);

            let redis_value = RedisValue::array_of_bulkstrings_from("REPLCONF capa psync2");
            stream.write_all(redis_value.to_string().as_bytes())?;
            loop {
                match stream.read(&mut buf) {
                    Ok(bytes_read) => {
                        let response = String::from_utf8_lossy(&buf[..bytes_read]);
                        println!("{}", response);
                        break;
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(200));
                    }
                    Err(e) => {
                        // If it was any other kind of error, something went
                        // wrong and we terminate with an error.
                        Err(e)?
                    }
                }
            }

            buf.fill(0);
            let redis_value = RedisValue::array_of_bulkstrings_from("PSYNC ? -1");
            stream.write_all(redis_value.to_string().as_bytes())?;
            loop {
                match stream.read(&mut buf) {
                    Ok(bytes_read) => {
                        let response = String::from_utf8_lossy(&buf[..bytes_read]);
                        println!("{}", response);
                        break;
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(200));
                    }
                    Err(e) => {
                        // If it was any other kind of error, something went
                        // wrong and we terminate with an error.
                        Err(e)?
                    }
                }
            }

            // TODO: actually parse length and then read the full rdb file
            // std::thread::sleep(Duration::from_millis(1000));
            buf.fill(0);
            loop {
                match stream.read(&mut buf) {
                    Ok(bytes_read) => {
                        let response = String::from_utf8_lossy(&buf[..bytes_read]);
                        println!("{}", response);
                        break;
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(200));
                    }
                    Err(e) => {
                        // If it was any other kind of error, something went
                        // wrong and we terminate with an error.
                        Err(e)?
                    }
                }
            }
        }
        Ok(())
    }

    pub fn send_to_replica(&self, redis_value: RedisValue) -> Result<()> {
        if let Some(ref mut stream) = *self.replica_stream.borrow_mut() {
            stream.write_all(redis_value.to_string().as_bytes())?;
        }
        Ok(())
    }
}
