use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
pub struct Store {
    map: Arc<Mutex<HashMap<String, String>>>,
}

impl Store {
    pub fn new() -> Self {
        Store {
            map: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn set(&self, key: String, value: String) {
        self.map.lock().unwrap().insert(key, value);
    }

    pub fn get(&self, key: &str) -> Option<String> {
        self.map.lock().unwrap().get(key).cloned()
    }
}
