use std::{
    alloc::System,
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

#[derive(Clone, Debug)]
struct Value {
    data: String,
    expiration: Option<SystemTime>,
}

impl Value {
    fn has_expired(&self) -> bool {
        match self.expiration {
            None => false,
            Some(expiration) => expiration <= SystemTime::now(),
        }
    }
}

pub struct Store {
    map: Arc<Mutex<HashMap<String, Value>>>,
}

impl Store {
    pub fn new() -> Self {
        Store {
            map: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn set(&self, key: String, value: String, expire_in: Option<Duration>) {
        let expiration = expire_in
            .map(|expire_in| SystemTime::now().checked_add(expire_in))
            .flatten();

        self.map.lock().unwrap().insert(
            key,
            Value {
                data: value,
                expiration,
            },
        );
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let mut map = self.map.lock().unwrap();

        match map.get(key).cloned() {
            None => None,
            Some(value) => {
                if value.has_expired() {
                    map.remove(key);
                    None
                } else {
                    Some(value.data)
                }
            }
        }
    }
}
