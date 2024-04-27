use crate::stream::{Entry, EntryId, Stream};
use crate::value::Value;
use anyhow::Result;
use crossbeam_channel::Receiver;
use std::{
    collections::HashMap,
    ops::Bound,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

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
    streams: Arc<Mutex<HashMap<String, Stream>>>,
}

impl Store {
    pub fn new() -> Self {
        Store {
            map: Arc::new(Mutex::new(HashMap::new())),
            streams: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn get_type(&self, key: String) -> String {
        match self.get(key.as_str()) {
            Some(v) => return v.type_string(),
            None => {
                let streams = self.streams.lock().unwrap();

                if streams.contains_key(key.as_str()) {
                    return "stream".into();
                }
            }
        }

        "none".into()
    }

    pub fn set(&self, key: String, value: Value, expire_in: Option<Duration>) {
        let expiration = expire_in.and_then(|expire_in| SystemTime::now().checked_add(expire_in));

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

    pub fn get_stream_range(
        &self,
        stream: String,
        start: Bound<EntryId>,
        end: Bound<EntryId>,
    ) -> Result<Vec<(EntryId, Vec<Entry>)>> {
        let streams = self.streams.lock().unwrap();
        let stream = streams.get(&stream);

        match stream {
            None => Ok(Vec::new()),
            Some(stream) => stream.range(start, end),
        }
    }

    pub fn get_stream_curr_max_id(&self, stream: String) -> EntryId {
        let mut streams = self.streams.lock().unwrap();

        let stream = streams.entry(stream).or_insert(Stream::new());
        stream.max_entry_id()
    }

    /// The `entry_id` arg might be wildcard. The returned `EntryId` is the
    /// actually inserted id.
    pub fn stream_set(
        &mut self,
        stream: String,
        entry_id: String,
        kvs: Vec<(String, String)>,
    ) -> Result<EntryId> {
        let mut streams = self.streams.lock().unwrap();

        let stream = streams.entry(stream).or_insert(Stream::new());
        let entry_id = EntryId::create(entry_id, &stream.max_entry_id())?;

        let entries = kvs
            .into_iter()
            .map(|(key, value)| Entry { key, value })
            .collect();

        stream.append(entry_id.clone(), entries)?;

        Ok(entry_id)
    }

    pub fn stream_subscribe(&mut self, stream: String, entry_id: EntryId) -> Receiver<()> {
        let mut streams = self.streams.lock().unwrap();
        let stream = streams.entry(stream).or_insert(Stream::new());
        stream.subscribe_entries_after(entry_id)
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
