use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::{Duration, Instant};

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

    /// Replicas need to refer to master
    replicaof_host: Option<String>,
    replicaof_port: Option<String>,

    master_replid: String,
    master_repl_offset: u64,
}

impl DbInfo {
    pub fn build(role: &str, replicaof_host: Option<&str>, replicaof_port: Option<&str>) -> Self {
        let master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string();
        let master_repl_offset = 0;

        Self {
            role: role.to_string(),
            replicaof_host: replicaof_host.map(|s| s.to_string()),
            replicaof_port: replicaof_port.map(|s| s.to_string()),
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
}

impl RedisDb {
    pub fn build(info: DbInfo) -> Self {
        Self {
            inner: Rc::new(RefCell::new(InnerRedisDb::build(info))),
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

    pub fn info(&self) -> String {
        self.inner.borrow().info.to_string()
    }
}
