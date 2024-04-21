use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

#[derive(Clone, Debug)]
pub enum Value {
    String(String),
}

impl Value {
    pub fn type_string(&self) -> &str {
        "string"
    }

    pub fn to_string(&self) -> String {
        let Self::String(s) = self; 
        s.clone()
    }
}

#[derive(Clone, Debug)]
struct ValueWrapper {
    value: Value,
    expiration: Option<SystemTime>,
}

impl ValueWrapper {
    fn has_expired(&self) -> bool {
        match self.expiration {
            None => false,
            Some(expiration) => expiration <= SystemTime::now(),
        }
    }
}

pub struct Store {
    map: Arc<Mutex<HashMap<String, ValueWrapper>>>,
}

impl Store {
    pub fn new() -> Self {
        Store {
            map: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn set(&self, key: String, value: Value, expire_in: Option<Duration>) {
        let expiration = expire_in
            .map(|expire_in| SystemTime::now().checked_add(expire_in))
            .flatten();

        self.map
            .lock()
            .unwrap()
            .insert(key, ValueWrapper { value, expiration });
    }

    pub fn get(&self, key: &str) -> Option<Value> {
        let mut map = self.map.lock().unwrap();

        match map.get(key).cloned() {
            None => None,
            Some(value) => {
                if value.has_expired() {
                    map.remove(key);
                    None
                } else {
                    Some(value.value)
                }
            }
        }
    }

    pub fn data(&self) -> HashMap<String, Value> {
        let mut map = self.map.lock().unwrap();

        *map = map
            .iter()
            .filter(|&(_, v)| !v.has_expired())
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        map.iter()
            .map(|(k, v)| (k.clone(), v.value.clone()))
            .collect()
    }
}
