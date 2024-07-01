use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

#[derive(Debug, Clone)]
pub struct RedisDb {
    store: Rc<RefCell<HashMap<String, String>>>,
}

impl RedisDb {
    pub fn new() -> Self {
        Self {
            store: Rc::new(RefCell::new(HashMap::new())),
        }
    }

    pub fn set(&self, key: String, value: String) {
        self.store.borrow_mut().insert(key, value);
    }

    pub fn get(&self, key: &str) -> Option<String> {
        self.store.borrow().get(key).cloned()
    }
}
