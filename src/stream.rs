use anyhow::{bail, Result};
use std::{collections::BTreeMap, fmt::Display};

const NOT_INCREASING_ERR_MSG: &str =
    "ERR The ID specified in XADD is equal or smaller than the target stream top item";

const MIN_ID_ERR_MSG: &str = "ERR The ID specified in XADD must be greater than 0-0";

// Derived PartialEq and Eq is exactly what we want: compare `ms` and then `seq`
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct EntryId {
    ms: u64,
    seq: u64,
}

impl Display for EntryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}

impl EntryId {
    /// Handles wildcard
    pub fn create(s: String, curr_max: &Self) -> Result<Self> {
        let vs = s.split('-').collect::<Vec<_>>();
        if vs.len() != 2 {
            bail!("Expect 'ms-seq' format");
        }

        let ms = vs[0].to_string().parse()?;
        let seq = match vs[1].to_string().parse::<u64>() {
            Ok(ms) => ms,
            Err(_) => {
                assert_eq!(vs[1], "*");

                if ms == curr_max.ms {
                    curr_max.seq + 1
                } else {
                    if ms == 0 {
                        // 0-0 is not allowed
                        1
                    } else {
                        0
                    }
                }
            }
        };
        Ok(Self { ms, seq })
    }
}

#[derive(Clone, Debug)]
pub struct Entry {
    pub key: String,
    pub value: String,
}

#[derive(Debug)]
pub struct Stream {
    entries: BTreeMap<EntryId, Entry>,
}

impl Stream {
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }

    pub fn append(&mut self, entry_id: EntryId, entry: Entry) -> Result<()> {
        // Validate entry id is strictly increasing
        if entry_id <= (EntryId { ms: 0, seq: 0 }) {
            bail!(MIN_ID_ERR_MSG);
        }

        if entry_id <= self.max_entry_id() {
            bail!(NOT_INCREASING_ERR_MSG);
        }

        self.entries.insert(entry_id, entry);
        Ok(())
    }

    pub fn max_entry_id(&self) -> EntryId {
        self.entries
            .iter()
            .max_by_key(|e| e.0)
            .map(|v| v.0.clone())
            .unwrap_or(EntryId { ms: 0, seq: 0 })
    }
}
