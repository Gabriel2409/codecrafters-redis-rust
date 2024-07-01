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
pub struct RedisDb {
    store: Rc<RefCell<HashMap<String, DbValue>>>,
}

impl RedisDb {
    pub fn build() -> Self {
        Self {
            store: Rc::new(RefCell::new(HashMap::new())),
        }
    }

    pub fn set(&self, key: String, value: String, px: Option<u64>) {
        let expires_in = px.map(Duration::from_millis);
        let db_value = DbValue::new(value, expires_in);
        self.store.borrow_mut().insert(key, db_value);
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let db_value = self.store.borrow().get(key).cloned();
        match db_value {
            None => None,
            Some(db_value) => {
                if db_value.is_expired() {
                    self.store.borrow_mut().remove(key);
                    None
                } else {
                    Some(db_value.value)
                }
            }
        }
    }
}
